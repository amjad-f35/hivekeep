//! The consumer half of the image cache, run by the privileged worker
//! (which owns every mount on the node), never by the keeper.
//!
//! [`setup_bundle_rootfs`] finalizes each layer (materializing recorded
//! whiteouts, once per layer node-wide), mounts an overlay at
//! `<bundle>/rootfs` with the cached layers as read-only lowerdirs and the
//! bundle's `upper/` and `work/` as the actor's private writable side, and
//! creates the spec's extra dirs through the mount. The mount lives in the
//! calling process's mount namespace, which is where the sandbox resolves
//! it, so no mount propagation is needed.
//!
//! Overlays are built with the new mount API (`fsopen`, one `fsconfig`
//! `lowerdir+` per layer) rather than `mount(2)`, whose single-page option
//! string digest-derived layer paths outgrow at about 34 layers. Minimum
//! kernel: Linux 6.7, or 6.6.23 where `lowerdir+` was backported.

use std::ffi::CString;
use std::fs;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use crate::confined;
use crate::error::{Error, Result};
use crate::implicitdirs::{apply_dir_fixups, resolve_implicit_dir_fixups};
use crate::spec::{
    create_extra_dirs, image_volume_mount_path, overlay_lower_dirs, read_spec, ImageVolumeOverlay,
    LAYER_FS_DIR_NAME,
};
use crate::store::LAYER_FINALIZED_MARKER_NAME;
use crate::unpack::{read_whiteouts, validate_tar_name};

const FSOPEN_CLOEXEC: libc::c_uint = 0x1;
const FSCONFIG_SET_FLAG: libc::c_uint = 0;
const FSCONFIG_SET_STRING: libc::c_uint = 1;
const FSCONFIG_CMD_CREATE: libc::c_uint = 6;
const FSMOUNT_CLOEXEC: libc::c_uint = 0x1;
const MOVE_MOUNT_F_EMPTY_PATH: libc::c_uint = 0x4;
const MOUNT_ATTR_RDONLY: u64 = 0x1;
const AT_RECURSIVE: libc::c_uint = 0x8000;

#[repr(C)]
struct MountAttr {
    attr_set: u64,
    attr_clr: u64,
    propagation: u64,
    userns_fd: u64,
}

fn cpath(p: &Path) -> io::Result<CString> {
    CString::new(p.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))
}

fn cstr(s: &str) -> io::Result<CString> {
    CString::new(s).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "string contains NUL"))
}

fn check(r: libc::c_long) -> io::Result<libc::c_long> {
    if r < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(r)
    }
}

/// Lazily detaches the mount at `p`; nothing mounted is not an error.
fn detach(p: &Path) -> io::Result<()> {
    let c = cpath(p)?;
    // SAFETY: a valid NUL-terminated path.
    let r = unsafe { libc::umount2(c.as_ptr(), libc::MNT_DETACH) };
    if r == 0 {
        return Ok(());
    }
    let e = io::Error::last_os_error();
    match e.raw_os_error() {
        Some(libc::EINVAL) | Some(libc::ENOENT) => Ok(()),
        _ => Err(e),
    }
}

/// Composes the bundle's rootfs from cached layers per its overlay spec.
/// A bundle without a spec is left untouched.
pub fn setup_bundle_rootfs(bundle: &Path) -> Result<()> {
    let Some(spec) = read_spec(bundle)? else {
        return Ok(());
    };
    for layer in &spec.layers {
        finalize_layer(Path::new(layer))
            .map_err(|e| e.context(format!("while finalizing layer {layer:?}")))?;
    }
    let rootfs = bundle.join("rootfs");
    let upper = bundle.join("upper");
    let work = bundle.join("work");
    for d in [&rootfs, &upper, &work] {
        mkdir_all_mode(d, 0o700).map_err(|e| Error::io(format!("while creating {d:?}"), e))?;
    }
    // A stale mount from an earlier incarnation of this bundle path.
    let _ = detach(&rootfs);
    if spec.layers.is_empty() {
        return create_extra_dirs(&rootfs, &spec.extra_dirs);
    }
    mount_overlay(
        &rootfs,
        &overlay_lower_dirs(&spec.layers),
        Some((&upper, &work)),
    )
    .map_err(|e| Error::msg(format!("while mounting overlay rootfs at {rootfs:?}: {e}")))?;
    create_extra_dirs(&rootfs, &spec.extra_dirs)?;
    // The chmod/chown copy up into this bundle's upper, never the pool.
    let layers: Vec<PathBuf> = spec.layers.iter().map(PathBuf::from).collect();
    let fixups = resolve_implicit_dir_fixups(&layers)
        .map_err(|e| e.context("while resolving implicit dir metadata"))?;
    apply_dir_fixups(&rootfs, &fixups)
        .map_err(|e| e.context("while repairing implicit dir metadata"))?;
    setup_image_volumes(bundle, &spec.image_volumes)
        .map_err(|e| e.context("while setting up image volumes"))
}

fn mkdir_all_mode(d: &Path, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new().recursive(true).mode(mode).create(d)
}

/// Exposes each image volume read-only at its bundle-local mount point.
fn setup_image_volumes(bundle: &Path, volumes: &[ImageVolumeOverlay]) -> Result<()> {
    for vol in volumes {
        for layer in &vol.layers {
            finalize_layer(Path::new(layer)).map_err(|e| {
                e.context(format!(
                    "while finalizing layer {layer:?} of image volume {:?}",
                    vol.name
                ))
            })?;
        }
        let mountpoint = image_volume_mount_path(bundle, &vol.name);
        mkdir_all_mode(&mountpoint, 0o700).map_err(|e| {
            Error::io(
                format!("while creating image volume mount point {mountpoint:?}"),
                e,
            )
        })?;
        let _ = detach(&mountpoint);
        mount_image_volume(&mountpoint, &vol.layers)
            .map_err(|e| Error::msg(format!("while mounting image volume {:?}: {e}", vol.name)))?;
    }
    Ok(())
}

/// Attaches `layers` read-only at `mountpoint`. The kernel rejects a single
/// lowerdir without an upper, so one layer is bind-mounted instead, made
/// read-only and private on every mount the recursive bind brought along.
fn mount_image_volume(mountpoint: &Path, layers: &[String]) -> io::Result<()> {
    if layers.len() == 1 {
        let fs_dir = Path::new(&layers[0]).join(LAYER_FS_DIR_NAME);
        let (src, dst) = (cpath(&fs_dir)?, cpath(mountpoint)?);
        // SAFETY: valid NUL-terminated paths; null fstype and data.
        let r = unsafe {
            libc::mount(
                src.as_ptr(),
                dst.as_ptr(),
                std::ptr::null(),
                libc::MS_BIND | libc::MS_REC,
                std::ptr::null(),
            )
        };
        if r != 0 {
            return Err(io::Error::other(format!(
                "while binding {fs_dir:?}: {}",
                io::Error::last_os_error()
            )));
        }
        let attr = MountAttr {
            attr_set: MOUNT_ATTR_RDONLY,
            attr_clr: 0,
            propagation: libc::MS_PRIVATE,
            userns_fd: 0,
        };
        // SAFETY: a valid path, a properly sized mount_attr.
        check(unsafe {
            libc::syscall(
                libc::SYS_mount_setattr,
                libc::AT_FDCWD,
                dst.as_ptr(),
                AT_RECURSIVE,
                &attr as *const MountAttr,
                std::mem::size_of::<MountAttr>(),
            )
        })
        .map_err(|e| io::Error::other(format!("while making {mountpoint:?} read-only: {e}")))?;
        return Ok(());
    }
    mount_overlay(mountpoint, &overlay_lower_dirs(layers), None)
}

/// Attaches an overlay of `lowers` (top-most first) at `mountpoint`, with
/// `upper` and its workdir when given (without one, overlayfs is
/// read-only). Volatile: the upper holds one activation's copy-ups and is
/// wiped between activations, so its syncs (including the one at unmount)
/// buy nothing.
fn mount_overlay(
    mountpoint: &Path,
    lowers: &[PathBuf],
    upper: Option<(&Path, &Path)>,
) -> io::Result<()> {
    let fstype = cstr("overlay")?;
    // SAFETY: a valid NUL-terminated fs name.
    let fd = check(unsafe { libc::syscall(libc::SYS_fsopen, fstype.as_ptr(), FSOPEN_CLOEXEC) })
        .map_err(|e| io::Error::other(format!("while opening overlay fs context: {e}")))?;
    // SAFETY: fsopen returned a new fd we own.
    let fs_fd = unsafe { OwnedFd::from_raw_fd(fd as libc::c_int) };
    let raw = fs_fd.as_raw_fd();

    let set = |key: &str, val: &Path| -> io::Result<()> {
        let (k, v) = (cstr(key)?, cpath(val)?);
        // SAFETY: a valid fs context fd and NUL-terminated key and value.
        check(unsafe {
            libc::syscall(
                libc::SYS_fsconfig,
                raw,
                FSCONFIG_SET_STRING,
                k.as_ptr(),
                v.as_ptr(),
                0,
            )
        })
        .map(|_| ())
        .map_err(|e| {
            io::Error::other(format!(
                "while setting overlay {key}={val:?}: {e}{}",
                fs_context_log(raw)
            ))
        })
    };
    for lower in lowers {
        set("lowerdir+", lower)?;
    }
    if let Some((up, work)) = upper {
        set("upperdir", up)?;
        set("workdir", work)?;
    }
    let volatile = cstr("volatile")?;
    // SAFETY: a valid fs context fd and NUL-terminated key.
    check(unsafe {
        libc::syscall(
            libc::SYS_fsconfig,
            raw,
            FSCONFIG_SET_FLAG,
            volatile.as_ptr(),
            std::ptr::null::<libc::c_char>(),
            0,
        )
    })
    .map_err(|e| {
        io::Error::other(format!(
            "while setting overlay volatile: {e}{}",
            fs_context_log(raw)
        ))
    })?;
    // SAFETY: a valid fs context fd.
    check(unsafe {
        libc::syscall(
            libc::SYS_fsconfig,
            raw,
            FSCONFIG_CMD_CREATE,
            std::ptr::null::<libc::c_char>(),
            std::ptr::null::<libc::c_char>(),
            0,
        )
    })
    .map_err(|e| {
        io::Error::other(format!(
            "while creating overlay superblock: {e}{}",
            fs_context_log(raw)
        ))
    })?;
    // SAFETY: a valid, configured fs context fd.
    let mfd = check(unsafe { libc::syscall(libc::SYS_fsmount, raw, FSMOUNT_CLOEXEC, 0) }).map_err(
        |e| {
            io::Error::other(format!(
                "while creating overlay mount object: {e}{}",
                fs_context_log(raw)
            ))
        },
    )?;
    // SAFETY: fsmount returned a new fd we own.
    let mount_fd = unsafe { OwnedFd::from_raw_fd(mfd as libc::c_int) };
    let (empty, dst) = (cstr("")?, cpath(mountpoint)?);
    // SAFETY: a valid mount fd, empty path with F_EMPTY_PATH, valid target.
    check(unsafe {
        libc::syscall(
            libc::SYS_move_mount,
            mount_fd.as_raw_fd(),
            empty.as_ptr(),
            libc::AT_FDCWD,
            dst.as_ptr(),
            MOVE_MOUNT_F_EMPTY_PATH,
        )
    })
    .map_err(|e| io::Error::other(format!("while attaching overlay at {mountpoint:?}: {e}")))?;
    Ok(())
}

/// Drains the messages the kernel queued on an fs context (which option it
/// rejected and why), for appending to an error.
fn fs_context_log(fd: libc::c_int) -> String {
    let mut msgs = Vec::new();
    let mut buf = [0u8; 1024];
    for _ in 0..8 {
        // SAFETY: a valid fd and a buffer of the given length.
        let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
        if n <= 0 {
            break;
        }
        msgs.push(
            String::from_utf8_lossy(&buf[..n as usize])
                .trim()
                .to_string(),
        );
    }
    if msgs.is_empty() {
        String::new()
    } else {
        format!(" (kernel: {})", msgs.join("; "))
    }
}

/// Materializes a layer's recorded whiteout state: 0:0 character devices
/// for whiteouts and `trusted.overlay.opaque=y` on opaque dirs. Needs
/// `CAP_MKNOD` and `CAP_SYS_ADMIN`, which the keeper drops. Idempotent and
/// safe under concurrent workers sharing the pool: existing devices are
/// success and the marker is written last. Recorded paths are re-validated,
/// so a crafted metadata file cannot escape the layer tree.
pub fn finalize_layer(layer_dir: &Path) -> Result<()> {
    let marker = layer_dir.join(LAYER_FINALIZED_MARKER_NAME);
    if marker.exists() {
        return Ok(());
    }
    let wh = read_whiteouts(layer_dir)?;
    let fs_dir = layer_dir.join(LAYER_FS_DIR_NAME);
    let root = confined::open_root(&fs_dir)
        .map_err(|e| Error::io(format!("while opening layer fs {fs_dir:?}"), e))?;
    for p in &wh.whiteouts {
        let rel = match validate_tar_name(p) {
            Err(e) => return Err(Error::msg(format!("invalid whiteout path: {e}"))),
            Ok(None) => continue,
            Ok(Some(r)) => PathBuf::from(r),
        };
        if let Some(parent) = confined::parent_of(&rel) {
            confined::mkdir_all(&root, parent, 0o755)
                .map_err(|e| Error::io(format!("while creating whiteout {rel:?}"), e))?;
        }
        match confined::mknod(&root, &rel, libc::S_IFCHR, 0, 0, 0) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(Error::io(
                    format!("while creating whiteout {rel:?}: mknodat"),
                    e,
                ))
            }
        }
    }
    for p in &wh.opaques {
        let rel = match validate_tar_name(p) {
            Err(e) => return Err(Error::msg(format!("invalid opaque dir path: {e}"))),
            Ok(None) => continue,
            Ok(Some(r)) => PathBuf::from(r),
        };
        confined::mkdir_all(&root, &rel, 0o755)
            .map_err(|e| Error::io(format!("while marking {rel:?} opaque"), e))?;
        // A real descriptor, opened relative to the parent: `Dir::open_dir`
        // handles are O_PATH on Linux, and fsetxattr rejects those (EBADF).
        let dir = confined::at_parent(&root, &rel, |parent, base| {
            // SAFETY: a valid directory fd and NUL-terminated name.
            let fd = unsafe {
                libc::openat(
                    parent,
                    base.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: `fd` was just opened and is owned by no one else.
            Ok(unsafe { OwnedFd::from_raw_fd(fd) })
        })
        .map_err(|e| Error::io(format!("while marking {rel:?} opaque"), e))?;
        let (name, val) = (
            cstr("trusted.overlay.opaque").map_err(|e| Error::io("xattr", e))?,
            b"y",
        );
        // SAFETY: a valid directory fd, NUL-terminated name, value buffer.
        let r = unsafe {
            libc::fsetxattr(
                dir.as_raw_fd(),
                name.as_ptr(),
                val.as_ptr().cast(),
                val.len(),
                0,
            )
        };
        if r != 0 {
            return Err(Error::io(
                format!("while marking {rel:?} opaque: fsetxattr"),
                io::Error::last_os_error(),
            ));
        }
    }
    fs::write(&marker, b"").map_err(|e| Error::io("while writing finalized marker", e))?;
    Ok(())
}

/// Lazily detaches every mount at or below `dir` in this mount namespace,
/// deepest first: the teardown counterpart of [`setup_bundle_rootfs`].
/// Missing mounts are not an error.
pub fn unmount_all_under(dir: &Path) -> Result<()> {
    let f = fs::File::open("/proc/self/mountinfo")
        .map_err(|e| Error::io("while opening mountinfo", e))?;
    let mut points = crate::mountinfo::mount_points_in(f, dir)
        .map_err(|e| Error::io("while reading mountinfo", e))?;
    points.sort_by_key(|p| std::cmp::Reverse(p.len()));
    let mut errs = Vec::new();
    for p in points {
        if let Err(e) = detach(Path::new(&p)) {
            errs.push(Error::io(format!("while unmounting {p:?}"), e));
        }
    }
    match Error::join(errs) {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{write_spec, OverlaySpec};
    use crate::unpack::{WhiteoutSet, LAYER_WHITEOUTS_FILE_NAME};
    use std::collections::BTreeMap;
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};

    /// Builds a layer dir (`fs/` tree and metadata) as unpack would.
    fn write_layer(dir: &Path, files: &[(&str, &str)], wh: Option<WhiteoutSet>) {
        let fs_dir = dir.join(LAYER_FS_DIR_NAME);
        fs::create_dir_all(&fs_dir).unwrap();
        for (name, body) in files {
            let p = fs_dir.join(name);
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            fs::write(p, body).unwrap();
        }
        if let Some(mut wh) = wh {
            wh.version = 1;
            fs::write(
                dir.join(LAYER_WHITEOUTS_FILE_NAME),
                serde_json::to_vec(&wh).unwrap(),
            )
            .unwrap();
        }
    }

    fn strs(v: &[&Path]) -> Vec<String> {
        v.iter().map(|p| p.to_string_lossy().into_owned()).collect()
    }

    fn is_root() -> bool {
        // SAFETY: no preconditions.
        unsafe { libc::geteuid() == 0 }
    }

    #[test]
    #[ignore = "needs root (CAP_MKNOD, CAP_SYS_ADMIN for trusted.* xattrs)"]
    fn finalize_layer_materializes_whiteouts() {
        let dir = tempfile::tempdir().unwrap();
        write_layer(
            dir.path(),
            &[("kept.txt", "kept")],
            Some(WhiteoutSet {
                whiteouts: vec!["removed.txt".into(), "sub/dir/removed-deep.txt".into()],
                opaques: vec!["opaque-dir".into()],
                ..Default::default()
            }),
        );
        finalize_layer(dir.path()).unwrap();
        for p in ["removed.txt", "sub/dir/removed-deep.txt"] {
            let md = fs::symlink_metadata(dir.path().join("fs").join(p)).unwrap();
            assert!(md.file_type().is_char_device(), "{p}");
            assert_eq!(md.rdev(), 0, "{p}");
        }
        let path = cpath(&dir.path().join("fs/opaque-dir")).unwrap();
        let name = cstr("trusted.overlay.opaque").unwrap();
        let mut val = [0u8; 8];
        // SAFETY: valid path, name and buffer.
        let n = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                val.as_mut_ptr().cast(),
                val.len(),
            )
        };
        assert_eq!(&val[..n.max(0) as usize], b"y");
        assert!(dir.path().join(LAYER_FINALIZED_MARKER_NAME).exists());
        finalize_layer(dir.path()).unwrap();
    }

    // A crafted metadata file fails validation before any mknod.
    #[test]
    fn finalize_layer_rejects_escaping_paths() {
        let dir = tempfile::tempdir().unwrap();
        write_layer(
            dir.path(),
            &[],
            Some(WhiteoutSet {
                whiteouts: vec!["../escape".into()],
                ..Default::default()
            }),
        );
        assert!(finalize_layer(dir.path()).is_err());
        let dir = tempfile::tempdir().unwrap();
        write_layer(
            dir.path(),
            &[],
            Some(WhiteoutSet {
                opaques: vec!["a/../../escape".into()],
                ..Default::default()
            }),
        );
        assert!(finalize_layer(dir.path()).is_err());
    }

    #[test]
    fn finalize_layer_no_whiteouts() {
        let dir = tempfile::tempdir().unwrap();
        write_layer(dir.path(), &[("f", "x")], None);
        finalize_layer(dir.path()).unwrap();
        assert!(dir.path().join(LAYER_FINALIZED_MARKER_NAME).exists());
    }

    #[test]
    fn setup_bundle_rootfs_no_spec_is_noop() {
        let bundle = tempfile::tempdir().unwrap();
        setup_bundle_rootfs(bundle.path()).unwrap();
        assert_eq!(fs::read_dir(bundle.path()).unwrap().count(), 0);
    }

    // A zero-layer spec composes without a mount.
    #[test]
    fn setup_bundle_rootfs_zero_layers() {
        let bundle = tempfile::tempdir().unwrap();
        write_spec(
            bundle.path(),
            &mut OverlaySpec {
                extra_dirs: vec!["/run/hivekeep".into()],
                ..Default::default()
            },
        )
        .unwrap();
        setup_bundle_rootfs(bundle.path()).unwrap();
        assert!(bundle.path().join("rootfs/run/hivekeep").is_dir());
        for d in ["upper", "work"] {
            assert!(bundle.path().join(d).is_dir(), "{d}");
        }
    }

    #[test]
    #[ignore = "needs root (mount/unmount)"]
    fn setup_bundle_rootfs_implicit_dir_metadata_repair() {
        assert!(is_root());
        let base = tempfile::tempdir().unwrap();
        write_layer(base.path(), &[("secret/keep.txt", "k")], None);
        fs::set_permissions(
            base.path().join("fs/secret"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let top = tempfile::tempdir().unwrap();
        write_layer(
            top.path(),
            &[("secret/new.txt", "n")],
            Some(WhiteoutSet {
                implicit_dirs: vec!["secret".into()],
                ..Default::default()
            }),
        );
        let bundle = tempfile::tempdir().unwrap();
        write_spec(
            bundle.path(),
            &mut OverlaySpec {
                layers: strs(&[base.path(), top.path()]),
                ..Default::default()
            },
        )
        .unwrap();
        setup_bundle_rootfs(bundle.path()).unwrap();
        let mode = fs::symlink_metadata(bundle.path().join("rootfs/secret"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        let top_mode = fs::symlink_metadata(top.path().join("fs/secret"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        let copied_up = bundle.path().join("upper/secret").exists();
        unmount_all_under(bundle.path()).unwrap();
        assert_eq!(mode, 0o700);
        assert_eq!(top_mode, 0o755, "shared top layer tree was modified");
        assert!(copied_up, "repair did not copy up into the bundle upper");
    }

    #[test]
    #[ignore = "needs root (mount/unmount)"]
    fn setup_bundle_rootfs_mount_and_unmount() {
        assert!(is_root());
        let layer = tempfile::tempdir().unwrap();
        write_layer(layer.path(), &[("from-layer.txt", "hello")], None);
        let bundle = tempfile::tempdir().unwrap();
        write_spec(
            bundle.path(),
            &mut OverlaySpec {
                layers: strs(&[layer.path()]),
                extra_dirs: vec!["/run/hivekeep".into()],
                ..Default::default()
            },
        )
        .unwrap();
        setup_bundle_rootfs(bundle.path()).unwrap();
        let rootfs = bundle.path().join("rootfs");
        assert_eq!(
            fs::read_to_string(rootfs.join("from-layer.txt")).unwrap(),
            "hello"
        );
        assert!(rootfs.join("run/hivekeep").is_dir());
        fs::write(rootfs.join("written.txt"), "w").unwrap();
        assert!(bundle.path().join("upper/written.txt").exists());
        assert!(
            !layer.path().join("fs/written.txt").exists(),
            "write leaked into the shared layer"
        );
        unmount_all_under(bundle.path()).unwrap();
        assert!(!rootfs.join("from-layer.txt").exists());
    }

    // A lowerdir chain whose joined paths exceed one page mounts fine.
    #[test]
    #[ignore = "needs root (mount/unmount)"]
    fn setup_bundle_rootfs_many_layers() {
        assert!(is_root());
        let tmp = tempfile::tempdir().unwrap();
        let pool = tmp.path().join("sha256");
        const LOWER_LEN: usize = 255;
        let base_len = pool
            .join("0".repeat(64))
            .join(LAYER_FS_DIR_NAME)
            .as_os_str()
            .len();
        assert!(base_len <= LOWER_LEN, "temp dir too long to pad");
        let pad = LOWER_LEN - base_len;
        // SAFETY: no preconditions.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let n = page / (LOWER_LEN + 1) + 1;
        let mut layers = Vec::new();
        for i in 0..n {
            let l = pool.join(format!("{i:064}{}", "p".repeat(pad)));
            write_layer(&l, &[(&format!("from-layer-{i:02}.txt"), "x")], None);
            layers.push(l.to_string_lossy().into_owned());
        }
        let bundle = tempfile::tempdir().unwrap();
        write_spec(
            bundle.path(),
            &mut OverlaySpec {
                layers,
                ..Default::default()
            },
        )
        .unwrap();
        setup_bundle_rootfs(bundle.path()).unwrap();
        let ok = [0, n - 1].iter().all(|i| {
            bundle
                .path()
                .join(format!("rootfs/from-layer-{i:02}.txt"))
                .exists()
        });
        unmount_all_under(bundle.path()).unwrap();
        assert!(ok, "bottom or top layer missing from the merged rootfs");
    }

    // Image volumes reach the same content through both arms: one layer
    // binds, several overlay; neither is writable.
    #[test]
    #[ignore = "needs root (mount/unmount)"]
    fn setup_bundle_rootfs_image_volumes() {
        assert!(is_root());
        for n in [1usize, 3] {
            let dirs: Vec<tempfile::TempDir> =
                (0..n).map(|_| tempfile::tempdir().unwrap()).collect();
            for (i, d) in dirs.iter().enumerate() {
                let mut files = BTreeMap::new();
                files.insert(format!("layer{i}.txt"), "content".to_string());
                files.insert("shadowed.txt".to_string(), format!("from-layer-{i}"));
                let files: Vec<(&str, &str)> = files
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                write_layer(d.path(), &files, None);
            }
            let layers: Vec<String> = dirs
                .iter()
                .map(|d| d.path().to_string_lossy().into_owned())
                .collect();
            let bundle = tempfile::tempdir().unwrap();
            write_spec(
                bundle.path(),
                &mut OverlaySpec {
                    layers: vec![layers[0].clone()],
                    image_volumes: vec![ImageVolumeOverlay {
                        name: "agent".into(),
                        layers: layers.clone(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )
            .unwrap();
            setup_bundle_rootfs(bundle.path()).unwrap();
            let mnt = bundle.path().join("volumes/agent");
            let visible = (0..n).all(|i| mnt.join(format!("layer{i}.txt")).exists());
            let shadowed = fs::read_to_string(mnt.join("shadowed.txt")).unwrap_or_default();
            let write = fs::write(mnt.join("nope.txt"), "x");
            unmount_all_under(bundle.path()).unwrap();
            assert!(visible, "{n} layers: a layer is not visible");
            assert_eq!(shadowed, format!("from-layer-{}", n - 1));
            let err = write.expect_err("write succeeded through a read-only image volume");
            assert_eq!(err.raw_os_error(), Some(libc::EROFS));
            assert!(!dirs[n - 1].path().join("fs/nope.txt").exists());
        }
    }
}

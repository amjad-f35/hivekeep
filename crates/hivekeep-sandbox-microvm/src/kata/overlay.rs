//! The host side of each container's rootfs: an overlay of the OCI bundle
//! (lower) and host directories (upper/work), merged by the host kernel and
//! served to the guest over the one virtio-fs share, plus the binds that put
//! volumes into that share.
//!
//! The guest runs its containers on the shared tree directly and mounts no
//! overlay itself, so rootfs writes cost host disk rather than guest RAM and
//! ride the snapshot's rootfs-upper tar across suspend and resume.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::actor_cgroup::ActorLeaf;
use crate::child::Process;
use crate::kata::paths::upper_work_dirs;

/// Creates the per-container upper layout under `upper_base`, which must
/// exist: `<name>` and the workdir are 0700, the upperdir 0755 because
/// overlayfs presents its mode as the container's `/`.
pub fn mkdir_upper_work_dirs(upper_base: &Path, name: &str) -> io::Result<(PathBuf, PathBuf)> {
    use std::os::unix::fs::DirBuilderExt;
    let (upper, work) = upper_work_dirs(upper_base, name);
    for (p, mode) in [
        (upper.parent().unwrap().to_path_buf(), 0o700),
        (upper.clone(), 0o755),
        (work.clone(), 0o700),
    ] {
        match std::fs::DirBuilder::new().mode(mode).create(&p) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("creating {}: {e}", p.display()),
                ))
            }
        }
    }
    Ok((upper, work))
}

/// Creates the `/proc`, `/sys` and `/dev` mountpoints in a container rootfs.
/// Neither layer of that rootfs is trusted, so each is created relative to
/// the rootfs directory without following a symlink planted at the name; an
/// existing entry, an in-rootfs symlink included, is left alone.
pub fn ensure_oci_mountpoints(rootfs: &Path) -> io::Result<()> {
    use nix::errno::Errno;
    use nix::fcntl::OFlag;
    use nix::sys::stat::{mkdirat, Mode};
    let root = nix::fcntl::open(
        rootfs,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )?;
    for d in ["proc", "sys", "dev"] {
        match mkdirat(&root, d, Mode::from_bits_truncate(0o755)) {
            Ok(()) | Err(Errno::EEXIST) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// The virtiofsd command line.
pub fn virtiofsd_args(socket: &Path, shared: &Path) -> Vec<String> {
    vec![
        format!("--socket-path={}", socket.display()),
        format!("--shared-dir={}", shared.display()),
        "--cache=auto".into(),
        "--thread-pool-size=1".into(),
        "--announce-submounts".into(),
        "--migration-mode".into(),
        "find-paths".into(),
        // The default (abort) makes a snapshot unrestorable when the guest
        // still references an inode with no findable path, such as a
        // live-rewritten trust bundle; guest-error answers EIO instead.
        "--migration-on-error".into(),
        "guest-error".into(),
    ]
}

/// How long virtiofsd may take to bind its socket.
pub const VIRTIOFSD_SOCKET_TIMEOUT: Duration = Duration::from_secs(10);
/// How often the socket is looked for. virtiofsd binds in milliseconds, so
/// the interval decides what the wait costs.
pub const SOCKET_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Waits until `path` exists, `cancel` fires, or `timeout` passes.
pub async fn wait_for_socket(
    path: &Path,
    timeout: Duration,
    cancel: &CancellationToken,
) -> io::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut tick = tokio::time::interval(SOCKET_POLL_INTERVAL);
    loop {
        if path.exists() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "virtiofsd socket {} did not appear within {timeout:?}",
                    path.display()
                ),
            ));
        }
        tokio::select! {
            _ = cancel.cancelled() => return Err(io::Error::new(io::ErrorKind::Interrupted, "context canceled")),
            _ = tick.tick() => {}
        }
    }
}

/// What [`start_virtiofsd`] runs.
pub struct VirtiofsdOptions<'a> {
    /// The executable; `virtiofsd` from PATH when empty.
    pub binary: &'a str,
    /// The vhost-user socket the VMM connects to.
    pub socket: PathBuf,
    /// The directory served.
    pub shared: PathBuf,
    /// Where its output goes.
    pub log: Option<PathBuf>,
    /// The cgroup it starts in.
    pub leaf: Option<&'a ActorLeaf>,
}

/// Starts virtiofsd in find-paths migration mode and waits for its socket.
/// The process outlives the caller (the VMM demand-pages from it); the caller
/// owns it.
pub async fn start_virtiofsd(
    o: VirtiofsdOptions<'_>,
    cancel: &CancellationToken,
) -> io::Result<Arc<Process>> {
    let bin = if o.binary.is_empty() {
        "virtiofsd"
    } else {
        o.binary
    };
    let _ = std::fs::remove_file(&o.socket);
    let mut cmd = tokio::process::Command::new(bin);
    cmd.args(virtiofsd_args(&o.socket, &o.shared));
    if let Some(log) = &o.log {
        let f = std::fs::File::create(log)?;
        cmd.stdout(f.try_clone()?).stderr(f);
    } else {
        cmd.stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
    }
    cmd.stdin(std::process::Stdio::null());
    if let Some(leaf) = o.leaf {
        leaf.configure(&mut cmd)?;
    }
    let p = Process::start(cmd)
        .map_err(|e| io::Error::new(e.kind(), format!("starting virtiofsd: {e}")))?;
    if let Err(e) = wait_for_socket(&o.socket, VIRTIOFSD_SOCKET_TIMEOUT, cancel).await {
        p.stop().await;
        return Err(e);
    }
    Ok(p)
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use crate::kata::paths::{shared_dir, shared_volume_dir};
    use nix::mount::{mount, umount2, MntFlags, MsFlags};

    const MOUNT_ATTR_RDONLY: u64 = 0x1;
    const MOUNT_ATTR_NOSUID: u64 = 0x2;
    const MOUNT_ATTR_NODEV: u64 = 0x4;
    const AT_RECURSIVE: libc::c_uint = 0x8000;

    #[repr(C)]
    struct MountAttr {
        attr_set: u64,
        attr_clr: u64,
        propagation: u64,
        userns_fd: u64,
    }

    fn mount_setattr(dst: &Path, attr_set: u64, propagation: u64) -> io::Result<()> {
        let c = std::ffi::CString::new(dst.as_os_str().as_encoded_bytes())
            .map_err(|_| io::Error::other("path holds a NUL"))?;
        let attr = MountAttr {
            attr_set,
            attr_clr: 0,
            propagation,
            userns_fd: 0,
        };
        // SAFETY: a valid path and attribute struct of the size passed.
        let rc = unsafe {
            libc::syscall(
                libc::SYS_mount_setattr,
                libc::AT_FDCWD,
                c.as_ptr(),
                AT_RECURSIVE,
                &attr as *const MountAttr,
                std::mem::size_of::<MountAttr>(),
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Drops the mount at `dst`, lazily if it is busy; nothing mounted there
    /// is fine.
    pub fn unmount(dst: &Path) {
        match umount2(dst, MntFlags::empty()) {
            Ok(()) | Err(nix::errno::Errno::EINVAL) | Err(nix::errno::Errno::ENOENT) => {}
            Err(_) => {
                let _ = umount2(dst, MntFlags::MNT_DETACH);
            }
        }
    }

    /// Makes the mount at `dst` and every mount beneath it read-only, keeping
    /// their other flags, and private so a later mount under the source does
    /// not propagate in writable.
    pub fn remount_read_only(dst: &Path) -> io::Result<()> {
        mount_setattr(dst, MOUNT_ATTR_RDONLY, libc::MS_PRIVATE)
    }

    /// Adds nosuid and nodev to the mount at `dst` and every mount beneath
    /// it: nothing on the host should honor a setuid bit or device node in
    /// workload-written or snapshot-restored files.
    pub fn set_nosuid_nodev(dst: &Path) -> io::Result<()> {
        mount_setattr(dst, MOUNT_ATTR_NOSUID | MOUNT_ATTR_NODEV, 0).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("setting nosuid,nodev on {}: {e}", dst.display()),
            )
        })
    }

    /// Bind-mounts `src` at `<shared dir>/<rel>`, so the one virtiofsd serves
    /// it as a subtree of the share. This is how every other host directory
    /// reaches the guest: a bind costs one mount, where a share of its own
    /// would cost a virtiofsd, a socket and a snapshot device forever.
    /// Stage binds before virtiofsd starts.
    pub fn bind_into_share(src: &Path, id: &str, rel: &str) -> io::Result<()> {
        if rel.is_empty() {
            return Err(io::Error::other("empty share-relative path"));
        }
        let dst = shared_dir(id).join(rel);
        unmount(&dst);
        std::fs::create_dir_all(&dst).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("creating share subdir {}: {e}", dst.display()),
            )
        })?;
        // rbind: only it carries mounts nested beneath the source.
        mount(
            Some(src),
            &dst,
            None::<&str>,
            MsFlags::MS_BIND | MsFlags::MS_REC,
            None::<&str>,
        )
        .map_err(|e| {
            io::Error::other(format!(
                "bind-mounting {} into the shared tree at {}: {e}",
                src.display(),
                dst.display()
            ))
        })?;
        set_nosuid_nodev(&dst)
    }

    /// Binds one composed image volume read-only at
    /// `<cid>/volumes/<name>` in the share.
    pub fn stage_image_volume(src: &Path, id: &str, cid: &str, volume: &str) -> io::Result<()> {
        if cid.is_empty() || volume.is_empty() {
            return Err(io::Error::other("empty container id or volume name"));
        }
        bind_into_share(src, id, &format!("{cid}/volumes/{volume}")).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("while binding image volume {volume:?} into the shared tree: {e}"),
            )
        })?;
        let dst = shared_volume_dir(id, cid, volume);
        remount_read_only(&dst).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("remounting image volume {} read-only: {e}", dst.display()),
            )
        })
    }

    /// Mounts overlay(lower = the bundle rootfs, upper/work = the Actor's
    /// host dirs for `name`) at `<shared dir of restore_id>/<cid>/rootfs`.
    ///
    /// `metacopy` and `index` are pinned off: both store handles to lower
    /// inodes in the upper, which the snapshot tar preserves but a restore
    /// (with a freshly unpacked lower) cannot honor. `volatile` skips syncs
    /// of an upper that is tarred and thrown away, and the workdir is wiped
    /// first because volatile refuses a dirty one.
    pub fn stage_merged_rootfs(
        bundle_rootfs: &Path,
        upper_base: &Path,
        name: &str,
        restore_id: &str,
        cid: &str,
    ) -> io::Result<()> {
        if name.is_empty() || cid.is_empty() {
            return Err(io::Error::other("empty container name or id"));
        }
        let dst = shared_dir(restore_id).join(cid).join("rootfs");
        let (_, work) = upper_work_dirs(upper_base, name);
        unmount(&dst);
        match std::fs::remove_dir_all(&work) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("clearing overlay workdir {}: {e}", work.display()),
                ))
            }
        }
        std::fs::create_dir_all(&dst)
            .map_err(|e| io::Error::new(e.kind(), format!("creating {}: {e}", dst.display())))?;
        let (upper, work) = mkdir_upper_work_dirs(upper_base, name)?;
        let opts = format!(
            "lowerdir={},upperdir={},workdir={},metacopy=off,index=off,volatile",
            bundle_rootfs.display(),
            upper.display(),
            work.display()
        );
        mount(
            Some("overlay"),
            &dst,
            Some("overlay"),
            MsFlags::MS_NOSUID | MsFlags::MS_NODEV,
            Some(opts.as_str()),
        )
        .map_err(|e| {
            io::Error::other(format!(
                "mounting merged rootfs overlay at {}: {e}",
                dst.display()
            ))
        })?;
        // Created in the merged tree, so they land in the upper and the
        // layout find-paths re-opens is the same on every node.
        ensure_oci_mountpoints(&dst).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("creating OCI mountpoints under {}: {e}", dst.display()),
            )
        })
    }

    /// Binds a directory read-only into the share (the system-info volumes,
    /// written only by the keeper on the host side).
    pub fn bind_read_only(src: &Path, dst: &Path) -> io::Result<()> {
        unmount(dst);
        std::fs::create_dir_all(dst)?;
        mount(Some(src), dst, None::<&str>, MsFlags::MS_BIND, None::<&str>).map_err(|e| {
            io::Error::other(format!(
                "bind-mounting {} at {}: {e}",
                src.display(),
                dst.display()
            ))
        })?;
        remount_read_only(dst)
    }

    /// Self-binds `path` and makes it rshared (unless it already is a shared
    /// mount point), so mounts created beneath it propagate to slave binds.
    pub fn ensure_shared_propagation(path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)?;
        if let Ok(info) = std::fs::read_to_string("/proc/self/mountinfo") {
            let p = path.to_string_lossy();
            if info.lines().any(|l| {
                let f: Vec<&str> = l.split_whitespace().collect();
                f.len() >= 7 && f[4] == p && l.contains("shared:")
            }) {
                return Ok(());
            }
        }
        mount(
            Some(path),
            path,
            None::<&str>,
            MsFlags::MS_BIND,
            None::<&str>,
        )
        .map_err(|e| io::Error::other(format!("self-binding {}: {e}", path.display())))?;
        mount(
            None::<&str>,
            path,
            None::<&str>,
            MsFlags::MS_SHARED | MsFlags::MS_REC,
            None::<&str>,
        )
        .map_err(|e| io::Error::other(format!("marking {} rshared: {e}", path.display())))
    }
}

#[cfg(target_os = "linux")]
pub use linux::*;

#[cfg(not(target_os = "linux"))]
mod other {
    use super::*;

    fn unsupported() -> io::Error {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "mounts are only supported on Linux",
        )
    }

    /// Nothing is mounted off Linux.
    pub fn unmount(_dst: &Path) {}
    /// Unsupported off Linux.
    pub fn remount_read_only(_dst: &Path) -> io::Result<()> {
        Err(unsupported())
    }
    /// Unsupported off Linux.
    pub fn set_nosuid_nodev(_dst: &Path) -> io::Result<()> {
        Err(unsupported())
    }
    /// Unsupported off Linux.
    pub fn bind_into_share(_src: &Path, _id: &str, _rel: &str) -> io::Result<()> {
        Err(unsupported())
    }
    /// Unsupported off Linux.
    pub fn stage_image_volume(_src: &Path, _id: &str, _cid: &str, _volume: &str) -> io::Result<()> {
        Err(unsupported())
    }
    /// Unsupported off Linux.
    pub fn stage_merged_rootfs(
        _b: &Path,
        _u: &Path,
        _n: &str,
        _r: &str,
        _c: &str,
    ) -> io::Result<()> {
        Err(unsupported())
    }
    /// Unsupported off Linux.
    pub fn bind_read_only(_src: &Path, _dst: &Path) -> io::Result<()> {
        Err(unsupported())
    }
    /// Unsupported off Linux.
    pub fn ensure_shared_propagation(_path: &Path) -> io::Result<()> {
        Err(unsupported())
    }
}

#[cfg(not(target_os = "linux"))]
pub use other::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mountpoints_do_not_follow_planted_symlinks() {
        let d = tempfile::tempdir().unwrap();
        let outside = d.path().join("outside");
        let rootfs = d.path().join("rootfs");
        std::fs::create_dir_all(rootfs.join("realsys")).unwrap();
        std::os::unix::fs::symlink(outside.join("proc"), rootfs.join("proc")).unwrap();
        std::os::unix::fs::symlink("realsys", rootfs.join("sys")).unwrap();
        ensure_oci_mountpoints(&rootfs).unwrap();
        assert!(
            std::fs::symlink_metadata(&outside).is_err(),
            "the symlink was followed out of the rootfs"
        );
        assert!(rootfs.join("dev").is_dir());
        assert_eq!(
            std::fs::read_link(rootfs.join("sys")).unwrap(),
            Path::new("realsys")
        );
    }

    #[test]
    fn virtiofsd_flags() {
        let args = virtiofsd_args(
            Path::new("/run/vm/virtiofsd.sock"),
            Path::new("/run/shared"),
        );
        assert!(args.iter().any(|a| a == "--cache=auto"));
        // The host owns the overlay, so the guest needs no xattr passthrough.
        assert!(!args.iter().any(|a| a == "--xattr"));
        let i = args
            .iter()
            .position(|a| a == "--migration-on-error")
            .unwrap();
        assert_eq!(args[i + 1], "guest-error");
    }

    #[test]
    fn unmounting_nothing_is_fine() {
        let d = tempfile::tempdir().unwrap();
        unmount(&d.path().join("nonexistent"));
    }

    #[tokio::test]
    async fn waits_for_a_present_socket() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("vfsd.sock");
        std::fs::write(&p, "").unwrap();
        wait_for_socket(&p, Duration::from_secs(1), &CancellationToken::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn waiting_for_a_socket_times_out() {
        let d = tempfile::tempdir().unwrap();
        let err = wait_for_socket(
            &d.path().join("never.sock"),
            Duration::from_millis(20),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(!err.to_string().is_empty());
    }

    #[tokio::test]
    async fn waiting_for_a_socket_honours_cancellation() {
        let d = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            c.cancel();
        });
        let err = wait_for_socket(
            &d.path().join("never.sock"),
            Duration::from_secs(5),
            &cancel,
        )
        .await
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Interrupted);
    }

    #[cfg(target_os = "linux")]
    fn statfs_flags(p: &Path) -> u64 {
        let c = std::ffi::CString::new(p.as_os_str().as_encoded_bytes()).unwrap();
        // SAFETY: zeroed out struct for statvfs to fill.
        let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: valid path and out pointer.
        assert_eq!(unsafe { libc::statvfs(c.as_ptr(), &mut st) }, 0);
        st.f_flag as u64
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "needs root: bind mounts"]
    fn remount_read_only_covers_submounts_and_later_mounts() {
        use nix::mount::{mount, umount2, MntFlags, MsFlags};
        let d = tempfile::tempdir().unwrap();
        let (src, dst) = (d.path().join("src"), d.path().join("dst"));
        let (sub, later) = (src.join("sub"), src.join("later"));
        for p in [&src, &dst, &sub, &later] {
            std::fs::create_dir(p).unwrap();
        }
        mount(
            Some(&src),
            &src,
            None::<&str>,
            MsFlags::MS_BIND,
            None::<&str>,
        )
        .unwrap();
        mount(
            None::<&str>,
            &src,
            None::<&str>,
            MsFlags::MS_SHARED,
            None::<&str>,
        )
        .unwrap();
        mount(
            Some("tmpfs"),
            &sub,
            Some("tmpfs"),
            MsFlags::empty(),
            None::<&str>,
        )
        .unwrap();
        mount(
            Some(&src),
            &dst,
            None::<&str>,
            MsFlags::MS_BIND | MsFlags::MS_REC,
            None::<&str>,
        )
        .unwrap();
        set_nosuid_nodev(&dst).unwrap();
        for f in [dst.join("test.txt"), dst.join("sub/test.txt")] {
            std::fs::write(&f, "write test").unwrap();
        }
        remount_read_only(&dst).unwrap();
        mount(
            Some("tmpfs"),
            &later,
            Some("tmpfs"),
            MsFlags::empty(),
            None::<&str>,
        )
        .unwrap();
        for f in [
            dst.join("test.txt"),
            dst.join("sub/test.txt"),
            dst.join("later/test.txt"),
        ] {
            let err = std::fs::write(&f, "should fail").unwrap_err();
            assert_eq!(err.raw_os_error(), Some(libc::EROFS), "{}", f.display());
        }
        for p in [dst.clone(), dst.join("sub")] {
            let fl = statfs_flags(&p);
            assert!(
                fl & libc::ST_NOSUID != 0 && fl & libc::ST_NODEV != 0,
                "{}",
                p.display()
            );
        }
        unmount(&dst);
        let _ = umount2(&sub, MntFlags::MNT_DETACH);
        let _ = umount2(&later, MntFlags::MNT_DETACH);
        let _ = umount2(&src, MntFlags::MNT_DETACH);
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "needs root: bind mounts"]
    fn nosuid_nodev_keeps_read_only() {
        use nix::mount::{mount, umount2, MntFlags, MsFlags};
        let (src, dst) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        std::fs::create_dir(src.path().join("sub")).unwrap();
        mount(
            Some("tmpfs"),
            &src.path().join("sub"),
            Some("tmpfs"),
            MsFlags::empty(),
            None::<&str>,
        )
        .unwrap();
        mount(
            Some(src.path()),
            dst.path(),
            None::<&str>,
            MsFlags::MS_BIND | MsFlags::MS_REC,
            None::<&str>,
        )
        .unwrap();
        mount(
            None::<&str>,
            dst.path(),
            None::<&str>,
            MsFlags::MS_BIND | MsFlags::MS_REMOUNT | MsFlags::MS_RDONLY,
            None::<&str>,
        )
        .unwrap();
        set_nosuid_nodev(dst.path()).unwrap();
        for (p, want_ro) in [
            (dst.path().to_path_buf(), true),
            (dst.path().join("sub"), false),
        ] {
            let fl = statfs_flags(&p);
            assert!(fl & libc::ST_NOSUID != 0 && fl & libc::ST_NODEV != 0);
            assert_eq!(fl & libc::ST_RDONLY != 0, want_ro, "{}", p.display());
        }
        let _ = umount2(dst.path(), MntFlags::MNT_DETACH);
        let _ = umount2(&src.path().join("sub"), MntFlags::MNT_DETACH);
    }
}

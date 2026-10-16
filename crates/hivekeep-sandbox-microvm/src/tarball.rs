//! Tar archives of host directories for snapshots: durable-dir volumes and
//! container rootfs uppers.
//!
//! Archives keep modes, ownership, modification times, symlinks, hardlinks,
//! FIFOs, overlay whiteouts and the xattrs overlayfs and workloads rely on
//! (`trusted.overlay.*`, `user.*`, as PAX records). Sockets and other device
//! nodes are skipped: they carry no data, and a workload recreates its
//! sockets on start.
//!
//! Both directions treat the tree as untrusted. Extraction resolves every
//! entry component by component without following symlinks, so an entry
//! cannot write outside the destination through `..`, an absolute name, or a
//! symlink planted earlier in the same archive.

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, BufWriter, Read};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Component, Path, PathBuf};

use nix::errno::Errno;
use nix::fcntl::{openat, OFlag};
use nix::sys::stat::{mkdirat, Mode};
use nix::unistd::{unlinkat, UnlinkatFlags};
use tokio_util::sync::CancellationToken;

/// The PAX record prefix carrying an xattr.
const PAX_XATTR: &str = "SCHILY.xattr.";

fn other(msg: impl Into<String>) -> io::Error {
    io::Error::other(msg.into())
}

/// Whether an xattr rides in the archive: overlay metadata (opaque markers,
/// redirects) and workload `user.*` data. Everything else stays behind.
fn keep_xattr(name: &str) -> bool {
    name.starts_with("trusted.overlay.") || name.starts_with("user.")
}

/// Archives the contents of `src` into `tar_path`. Entry names are relative to
/// `src`; with `include_root`, a `./` entry carries `src`'s own metadata,
/// which [`extract`] applies to its destination.
pub fn create(
    tar_path: &Path,
    src: &Path,
    include_root: bool,
    cancel: &CancellationToken,
) -> io::Result<()> {
    let f = File::create(tar_path)
        .map_err(|e| other(format!("creating tar {}: {e}", tar_path.display())))?;
    let mut b = tar::Builder::new(BufWriter::with_capacity(64 << 10, f));
    b.follow_symlinks(false);
    let mut linked: HashMap<(u64, u64), PathBuf> = HashMap::new();
    if include_root {
        let md = std::fs::symlink_metadata(src)?;
        let mut h = header_for(&md, tar::EntryType::Directory);
        append_xattrs(&mut b, src)?;
        b.append_data(&mut h, ".", io::empty())?;
    }
    walk(src, Path::new(""), &mut b, &mut linked, cancel)?;
    let w = b.into_inner()?;
    w.into_inner().map_err(|e| e.into_error())?;
    Ok(())
}

/// Like [`create`] with the root entry: the usual shape for snapshots.
pub fn create_with_root(tar_path: &Path, src: &Path, cancel: &CancellationToken) -> io::Result<()> {
    create(tar_path, src, true, cancel)
}

fn header_for(md: &std::fs::Metadata, kind: tar::EntryType) -> tar::Header {
    let mut h = tar::Header::new_gnu();
    h.set_entry_type(kind);
    h.set_mode(md.mode() & 0o7777);
    h.set_uid(md.uid() as u64);
    h.set_gid(md.gid() as u64);
    h.set_mtime(md.mtime().max(0) as u64);
    h.set_size(0);
    h
}

fn walk(
    root: &Path,
    rel: &Path,
    b: &mut tar::Builder<BufWriter<File>>,
    linked: &mut HashMap<(u64, u64), PathBuf>,
    cancel: &CancellationToken,
) -> io::Result<()> {
    let dir = root.join(rel);
    let mut names: Vec<_> = std::fs::read_dir(&dir)?
        .map(|e| e.map(|e| e.file_name()))
        .collect::<io::Result<_>>()?;
    // Lexical order keeps archives of identical trees byte-comparable.
    names.sort();
    for name in names {
        if cancel.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "archiving cancelled",
            ));
        }
        let rel = rel.join(&name);
        let path = root.join(&rel);
        let md = std::fs::symlink_metadata(&path)?;
        let ft = md.file_type();
        if ft.is_socket() {
            tracing::warn!(path = %path.display(), root = %root.display(), "Skipping socket while archiving directory");
            continue;
        }
        if ft.is_symlink() {
            let target = std::fs::read_link(&path)?;
            let mut h = header_for(&md, tar::EntryType::Symlink);
            b.append_link(&mut h, &rel, target)?;
        } else if ft.is_dir() {
            let mut h = header_for(&md, tar::EntryType::Directory);
            append_xattrs(b, &path)?;
            b.append_data(&mut h, &rel, io::empty())?;
            walk(root, &rel, b, linked, cancel)?;
        } else if ft.is_file() {
            if md.nlink() > 1 {
                let key = (md.dev(), md.ino());
                if let Some(first) = linked.get(&key) {
                    let mut h = header_for(&md, tar::EntryType::Link);
                    b.append_link(&mut h, &rel, first)?;
                    continue;
                }
                linked.insert(key, rel.clone());
            }
            let f = open_regular(root, &rel)?;
            let mut h = header_for(&md, tar::EntryType::Regular);
            h.set_size(f.metadata()?.len());
            append_xattrs(b, &path)?;
            b.append_data(&mut h, &rel, f)?;
        } else if ft.is_fifo() {
            let mut h = header_for(&md, tar::EntryType::Fifo);
            b.append_data(&mut h, &rel, io::empty())?;
        } else if ft.is_char_device() && md.rdev() == 0 {
            // An overlay whiteout (0/0 character device).
            let mut h = header_for(&md, tar::EntryType::Char);
            h.set_device_major(0)?;
            h.set_device_minor(0)?;
            b.append_data(&mut h, &rel, io::empty())?;
        } else {
            tracing::warn!(path = %path.display(), root = %root.display(), "Skipping device node while archiving directory");
        }
    }
    Ok(())
}

/// Opens `rel` under `root` for reading without following a symlink at any
/// component, and fails unless it is still a regular file.
fn open_regular(root: &Path, rel: &Path) -> io::Result<File> {
    let rootfd = open_dir(root)?;
    let (parent, name) = walk_parents(rootfd, rel, false)?;
    let fd = openat(
        parent.as_fd(),
        name.as_os_str(),
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    let f = File::from(fd);
    if !f.metadata()?.is_file() {
        return Err(other(format!(
            "{} is no longer a regular file",
            rel.display()
        )));
    }
    Ok(f)
}

#[cfg(target_os = "linux")]
fn list_xattrs(path: &Path) -> io::Result<BTreeMap<String, Vec<u8>>> {
    use std::ffi::CString;
    let c = CString::new(path.as_os_str().as_bytes()).map_err(|_| other("path holds a NUL"))?;
    let mut out = BTreeMap::new();
    // SAFETY: probing the size with a null buffer.
    let n = unsafe { libc::llistxattr(c.as_ptr(), std::ptr::null_mut(), 0) };
    if n < 0 {
        let e = io::Error::last_os_error();
        return match e.raw_os_error() {
            Some(libc::ENOTSUP) => Ok(out),
            _ => Err(e),
        };
    }
    let mut names = vec![0u8; n as usize];
    // SAFETY: names has room for n bytes.
    let n = unsafe { libc::llistxattr(c.as_ptr(), names.as_mut_ptr().cast(), names.len()) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    for name in names[..n as usize]
        .split(|b| *b == 0)
        .filter(|n| !n.is_empty())
    {
        let Ok(s) = std::str::from_utf8(name) else {
            continue;
        };
        if !keep_xattr(s) {
            continue;
        }
        let cn = CString::new(name).map_err(|_| other("xattr name holds a NUL"))?;
        // SAFETY: size probe.
        let len = unsafe { libc::lgetxattr(c.as_ptr(), cn.as_ptr(), std::ptr::null_mut(), 0) };
        if len < 0 {
            continue;
        }
        let mut v = vec![0u8; len as usize];
        // SAFETY: v has room for len bytes.
        let len =
            unsafe { libc::lgetxattr(c.as_ptr(), cn.as_ptr(), v.as_mut_ptr().cast(), v.len()) };
        if len < 0 {
            continue;
        }
        v.truncate(len as usize);
        out.insert(s.to_string(), v);
    }
    Ok(out)
}

#[cfg(not(target_os = "linux"))]
fn list_xattrs(_path: &Path) -> io::Result<BTreeMap<String, Vec<u8>>> {
    Ok(BTreeMap::new())
}

fn append_xattrs(b: &mut tar::Builder<BufWriter<File>>, path: &Path) -> io::Result<()> {
    let x = list_xattrs(path)?;
    if x.is_empty() {
        return Ok(());
    }
    let recs: Vec<(String, Vec<u8>)> = x
        .into_iter()
        .map(|(k, v)| (format!("{PAX_XATTR}{k}"), v))
        .collect();
    b.append_pax_extensions(recs.iter().map(|(k, v)| (k.as_str(), v.as_slice())))
}

fn open_dir(p: &Path) -> io::Result<OwnedFd> {
    nix::fcntl::open(
        p,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(|e| other(format!("opening {}: {e}", p.display())))
}

fn open_subdir(parent: BorrowedFd<'_>, name: &OsStr) -> nix::Result<OwnedFd> {
    openat(
        parent,
        name,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
}

/// Normalizes an entry name: `None` for the root entry, an error for an
/// absolute name or one with `..`.
fn clean_name(raw: &[u8]) -> io::Result<Option<PathBuf>> {
    let p = Path::new(OsStr::from_bytes(raw));
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::Normal(n) => out.push(n),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(other(format!(
                    "refusing entry {:?}",
                    String::from_utf8_lossy(raw)
                )))
            }
        }
    }
    Ok((!out.as_os_str().is_empty()).then_some(out))
}

/// Opens every parent directory of `rel` under `root` without following
/// symlinks, optionally creating missing ones, and returns the innermost with
/// the final name.
fn walk_parents(root: OwnedFd, rel: &Path, create: bool) -> io::Result<(OwnedFd, PathBuf)> {
    let mut comps: Vec<_> = rel.components().collect();
    let last = comps.pop().ok_or_else(|| other("empty entry name"))?;
    let mut cur = root;
    for c in comps {
        let name = c.as_os_str();
        let next = match open_subdir(cur.as_fd(), name) {
            Err(Errno::ENOENT) if create => {
                mkdirat(cur.as_fd(), name, Mode::from_bits_truncate(0o700))
                    .map_err(io::Error::from)?;
                open_subdir(cur.as_fd(), name)
            }
            other => other,
        }
        .map_err(|e| {
            other(format!(
                "resolving {}: {} is not a directory inside the destination ({e})",
                rel.display(),
                Path::new(name).display()
            ))
        })?;
        cur = next;
    }
    Ok((cur, PathBuf::from(last.as_os_str())))
}

/// Removes whatever sits at `name` in `dir` (whose full path is `full`),
/// unless it is a directory and `keep_dir` is set. A directory that must go
/// is removed whole; `remove_dir_all` does not follow symlinks.
fn clear(dir: BorrowedFd<'_>, name: &OsStr, full: &Path, keep_dir: bool) -> io::Result<bool> {
    match nix::sys::stat::fstatat(dir, name, nix::fcntl::AtFlags::AT_SYMLINK_NOFOLLOW) {
        Err(Errno::ENOENT) => Ok(false),
        Err(e) => Err(e.into()),
        Ok(st) => {
            let is_dir = (st.st_mode & libc::S_IFMT) == libc::S_IFDIR;
            if is_dir && keep_dir {
                return Ok(true);
            }
            if is_dir {
                std::fs::remove_dir_all(full)?;
            } else {
                unlinkat(dir, name, UnlinkatFlags::NoRemoveDir)?;
            }
            Ok(false)
        }
    }
}

#[cfg(target_os = "linux")]
fn mknod(
    dir: BorrowedFd<'_>,
    name: &OsStr,
    kind: nix::sys::stat::SFlag,
    mode: u32,
) -> io::Result<()> {
    nix::sys::stat::mknodat(dir, name, kind, Mode::from_bits_truncate(mode as _), 0)
        .map_err(io::Error::from)
}

#[cfg(not(target_os = "linux"))]
fn mknod(
    _dir: BorrowedFd<'_>,
    _name: &OsStr,
    _kind: nix::sys::stat::SFlag,
    _mode: u32,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "special files are only restored on Linux",
    ))
}

/// Metadata applied after everything is written.
struct Meta {
    mode: u32,
    mtime: i64,
    uid: u64,
    gid: u64,
    xattrs: Vec<(String, Vec<u8>)>,
}

fn meta_of(e: &mut tar::Entry<'_, impl Read>) -> io::Result<Meta> {
    let h = e.header();
    let mut m = Meta {
        mode: h.mode()? & 0o7777,
        mtime: h.mtime().unwrap_or(0) as i64,
        uid: h.uid().unwrap_or(0),
        gid: h.gid().unwrap_or(0),
        xattrs: vec![],
    };
    if let Some(exts) = e.pax_extensions()? {
        for x in exts.flatten() {
            if let Ok(k) = x.key() {
                if let Some(name) = k.strip_prefix(PAX_XATTR) {
                    if keep_xattr(name) {
                        m.xattrs.push((name.to_string(), x.value_bytes().to_vec()));
                    }
                }
            }
        }
    }
    Ok(m)
}

fn is_root() -> bool {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() == 0 }
}

/// Applies ownership, xattrs, mode and times to an open file or directory.
fn apply_meta(fd: BorrowedFd<'_>, m: &Meta, mode: bool) -> io::Result<()> {
    if is_root() {
        // SAFETY: fchown on an open descriptor.
        if unsafe { libc::fchown(fd.as_raw_fd(), m.uid as libc::uid_t, m.gid as libc::gid_t) } != 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    #[cfg(target_os = "linux")]
    for (k, v) in &m.xattrs {
        let k = std::ffi::CString::new(k.as_str()).map_err(|_| other("xattr name holds a NUL"))?;
        // SAFETY: valid name and value buffers.
        if unsafe { libc::fsetxattr(fd.as_raw_fd(), k.as_ptr(), v.as_ptr().cast(), v.len(), 0) }
            != 0
        {
            let e = io::Error::last_os_error();
            // trusted.* needs CAP_SYS_ADMIN; without it there is no overlay to
            // feed either.
            if e.raw_os_error() != Some(libc::EPERM) {
                return Err(e);
            }
        }
    }
    if mode {
        // SAFETY: fchmod on an open descriptor.
        if unsafe { libc::fchmod(fd.as_raw_fd(), m.mode as libc::mode_t) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    let ts = nix::sys::time::TimeSpec::new(m.mtime, 0);
    nix::sys::stat::futimens(fd, &ts, &ts).map_err(io::Error::from)
}

/// Unpacks `tar_path` into `dst`, which must exist. An entry that collides
/// with an existing path replaces it, except that an existing directory is
/// kept for a directory entry. Directory modes and times are applied once
/// every child exists, so a read-only directory can still be filled.
pub fn extract(tar_path: &Path, dst: &Path) -> io::Result<()> {
    let f = File::open(tar_path)
        .map_err(|e| other(format!("opening tar {}: {e}", tar_path.display())))?;
    let mut a = tar::Archive::new(io::BufReader::with_capacity(64 << 10, f));
    let mut dirs: Vec<(Option<PathBuf>, Meta)> = Vec::new();
    for e in a.entries()? {
        let mut e = e.map_err(|e| other(format!("reading tar {}: {e}", tar_path.display())))?;
        let raw = e.path_bytes().into_owned();
        let name = clean_name(&raw).map_err(|err| {
            other(format!(
                "invalid entry in tar {}: {err}",
                tar_path.display()
            ))
        })?;
        let kind = e.header().entry_type();
        let meta = meta_of(&mut e)?;
        let Some(name) = name else {
            if kind.is_dir() {
                dirs.push((None, meta));
            }
            continue;
        };
        let (parent, leaf) = walk_parents(open_dir(dst)?, &name, true)?;
        let leaf = leaf.as_os_str();
        let full = dst.join(&name);
        match kind {
            tar::EntryType::Directory => {
                if !clear(parent.as_fd(), leaf, &full, true)? {
                    mkdirat(
                        parent.as_fd(),
                        leaf,
                        Mode::from_bits_truncate(((meta.mode | 0o700) & 0o7777) as _),
                    )
                    .map_err(io::Error::from)?;
                }
                dirs.push((Some(name), meta));
            }
            tar::EntryType::Regular | tar::EntryType::Continuous => {
                clear(parent.as_fd(), leaf, &full, false)?;
                let fd = openat(
                    parent.as_fd(),
                    leaf,
                    OFlag::O_WRONLY
                        | OFlag::O_CREAT
                        | OFlag::O_EXCL
                        | OFlag::O_NOFOLLOW
                        | OFlag::O_CLOEXEC,
                    Mode::from_bits_truncate(0o600),
                )
                .map_err(io::Error::from)?;
                let mut out = File::from(fd);
                io::copy(&mut e, &mut out)?;
                apply_meta(out.as_fd(), &meta, true)?;
            }
            tar::EntryType::Symlink => {
                let target = e
                    .link_name_bytes()
                    .ok_or_else(|| other("symlink without a target"))?
                    .into_owned();
                clear(parent.as_fd(), leaf, &full, false)?;
                nix::unistd::symlinkat(OsStr::from_bytes(&target), parent.as_fd(), leaf)
                    .map_err(io::Error::from)?;
                if is_root() {
                    let _ = nix::unistd::fchownat(
                        parent.as_fd(),
                        leaf,
                        Some(nix::unistd::Uid::from_raw(meta.uid as u32)),
                        Some(nix::unistd::Gid::from_raw(meta.gid as u32)),
                        nix::fcntl::AtFlags::AT_SYMLINK_NOFOLLOW,
                    );
                }
            }
            tar::EntryType::Link => {
                let target = e
                    .link_name_bytes()
                    .ok_or_else(|| other("hardlink without a target"))?
                    .into_owned();
                let target = clean_name(&target)?.ok_or_else(|| other("hardlink to the root"))?;
                let (tparent, tleaf) = walk_parents(open_dir(dst)?, &target, false)?;
                clear(parent.as_fd(), leaf, &full, false)?;
                nix::unistd::linkat(
                    tparent.as_fd(),
                    tleaf.as_os_str(),
                    parent.as_fd(),
                    leaf,
                    nix::fcntl::AtFlags::empty(),
                )
                .map_err(io::Error::from)?;
            }
            tar::EntryType::Fifo => {
                clear(parent.as_fd(), leaf, &full, false)?;
                mknod(
                    parent.as_fd(),
                    leaf,
                    nix::sys::stat::SFlag::S_IFIFO,
                    meta.mode,
                )?;
            }
            tar::EntryType::Char
                if e.header().device_major()?.unwrap_or(0) == 0
                    && e.header().device_minor()?.unwrap_or(0) == 0 =>
            {
                clear(parent.as_fd(), leaf, &full, false)?;
                mknod(
                    parent.as_fd(),
                    leaf,
                    nix::sys::stat::SFlag::S_IFCHR,
                    meta.mode,
                )
                .map_err(|e| other(format!("creating whiteout {}: {e}", name.display())))?;
            }
            other_kind => {
                return Err(other(format!(
                    "unsupported entry {} of type {other_kind:?} in tar {}",
                    name.display(),
                    tar_path.display()
                )))
            }
        }
    }
    // Deepest first, so a parent's times are set after its children change.
    dirs.sort_by_key(|(p, _)| std::cmp::Reverse(p.as_ref().map_or(0, |p| p.components().count())));
    for (name, meta) in dirs {
        let fd = match &name {
            None => open_dir(dst)?,
            Some(n) => {
                let (parent, leaf) = walk_parents(open_dir(dst)?, n, false)?;
                open_subdir(parent.as_fd(), leaf.as_os_str()).map_err(io::Error::from)?
            }
        };
        apply_meta(fd.as_fd(), &meta, true)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn round_trips_a_tree() {
        let src = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(src.path().join("a/b")).unwrap();
        std::fs::write(src.path().join("a/b/file"), "content").unwrap();
        std::fs::hard_link(src.path().join("a/b/file"), src.path().join("a/link")).unwrap();
        std::os::unix::fs::symlink("b/file", src.path().join("a/sym")).unwrap();
        std::fs::set_permissions(src.path(), std::fs::Permissions::from_mode(0o750)).unwrap();
        let _sock = std::os::unix::net::UnixListener::bind(src.path().join("agent.sock")).unwrap();
        let out = tempfile::tempdir().unwrap();
        let tar = out.path().join("t.tar");
        create_with_root(&tar, src.path(), &CancellationToken::new()).unwrap();
        let dst = tempfile::tempdir().unwrap();
        extract(&tar, dst.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(dst.path().join("a/b/file")).unwrap(),
            "content"
        );
        assert_eq!(
            std::fs::read_to_string(dst.path().join("a/link")).unwrap(),
            "content"
        );
        let a = std::fs::metadata(dst.path().join("a/b/file")).unwrap();
        let b = std::fs::metadata(dst.path().join("a/link")).unwrap();
        assert_eq!(a.ino(), b.ino(), "the hardlink came back as a copy");
        assert_eq!(
            std::fs::read_link(dst.path().join("a/sym")).unwrap(),
            Path::new("b/file")
        );
        assert!(
            !dst.path().join("agent.sock").exists(),
            "a socket was archived"
        );
        assert_eq!(
            std::fs::metadata(dst.path()).unwrap().permissions().mode() & 0o777,
            0o750
        );
    }

    #[test]
    fn refuses_to_write_through_a_planted_symlink() {
        let victim = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(victim.path(), src.path().join("data")).unwrap();
        let out = tempfile::tempdir().unwrap();
        let first = out.path().join("first.tar");
        create(&first, src.path(), false, &CancellationToken::new()).unwrap();
        // A second archive writes "data/file": through the symlink the first
        // one plants.
        let src2 = tempfile::tempdir().unwrap();
        std::fs::create_dir(src2.path().join("data")).unwrap();
        std::fs::write(src2.path().join("data/file"), "x").unwrap();
        let second = out.path().join("second.tar");
        create(&second, src2.path(), false, &CancellationToken::new()).unwrap();
        let dst = tempfile::tempdir().unwrap();
        extract(&first, dst.path()).unwrap();
        // The directory entry replaces the symlink instead of following it.
        extract(&second, dst.path()).unwrap();
        assert!(std::fs::read_dir(victim.path()).unwrap().next().is_none());
        assert!(std::fs::symlink_metadata(dst.path().join("data"))
            .unwrap()
            .is_dir());
    }

    #[test]
    fn rejects_escaping_names() {
        for bad in [&b"../x"[..], b"/abs", b"a/../../x"] {
            assert!(
                clean_name(bad).is_err(),
                "{:?}",
                String::from_utf8_lossy(bad)
            );
        }
        assert_eq!(clean_name(b"./").unwrap(), None);
        assert_eq!(clean_name(b"./a/b").unwrap(), Some(PathBuf::from("a/b")));
    }
}

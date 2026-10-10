//! Filesystem access confined to a directory tree.
//!
//! Untrusted trees (image layers, archived workload data, a mounted rootfs)
//! are only ever written through a [`cap_std::fs::Dir`]: path resolution
//! refuses `..` past the root, absolute paths, and symlinks pointing out of
//! the tree, so a crafted entry cannot redirect a write to the host. The
//! helpers here add what `Dir` lacks: lexical name cleaning, times, and
//! metadata calls made relative to an opened parent directory.

use std::ffi::{CString, OsStr};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use cap_std::ambient_authority;
use cap_std::fs::{
    Dir, DirBuilder, DirBuilderExt, OpenOptions, OpenOptionsExt, Permissions, PermissionsExt,
};

/// Opens `path` as a confined root.
pub fn open_root(path: &Path) -> io::Result<Dir> {
    Dir::open_ambient_dir(path, ambient_authority())
}

/// Lexically cleans a slash-separated name the way a path cleaner does:
/// drops empty and `.` components, resolves `..` against earlier ones, and
/// keeps a leading `/`. Returns `.` for an empty result.
pub fn clean(name: &[u8]) -> Vec<u8> {
    let rooted = name.first() == Some(&b'/');
    let mut parts: Vec<&[u8]> = Vec::new();
    for c in name.split(|&b| b == b'/') {
        match c {
            b"" | b"." => {}
            b".." => {
                if parts.last().is_some_and(|l| *l != b"..") {
                    parts.pop();
                } else if !rooted {
                    parts.push(c);
                }
            }
            c => parts.push(c),
        }
    }
    let mut out = Vec::new();
    if rooted {
        out.push(b'/');
    }
    out.extend_from_slice(&parts.join(&b'/'));
    if out.is_empty() {
        out.push(b'.');
    }
    out
}

/// Whether a cleaned name stays within its root: relative, non-empty, and
/// free of `..` components.
pub fn is_local(cleaned: &[u8]) -> bool {
    !cleaned.is_empty() && cleaned[0] != b'/' && !cleaned.split(|&b| b == b'/').any(|c| c == b"..")
}

/// Validates an archive entry name. Returns `None` for names that denote
/// the root itself (`""`, `.`, `/`), the cleaned relative path otherwise,
/// and an error for names escaping the root.
pub fn validate_name(name: &[u8]) -> Result<Option<PathBuf>, String> {
    if name.is_empty() {
        return Ok(None);
    }
    let cleaned = clean(name);
    let rel: &[u8] = cleaned.strip_prefix(b"/").unwrap_or(&cleaned);
    if rel.is_empty() || rel == b"." {
        return Ok(None);
    }
    if !is_local(rel) {
        return Err(format!(
            "not a local path: {:?}",
            String::from_utf8_lossy(name)
        ));
    }
    Ok(Some(PathBuf::from(OsStr::from_bytes(rel))))
}

/// The parent of a relative path, `None` at the top level.
pub fn parent_of(rel: &Path) -> Option<&Path> {
    rel.parent().filter(|p| !p.as_os_str().is_empty())
}

/// Every ancestor of `rel`, nearest first, excluding the root.
pub fn ancestors(rel: &Path) -> impl Iterator<Item = &Path> {
    rel.ancestors()
        .skip(1)
        .filter(|p| !p.as_os_str().is_empty())
}

/// Splits `rel` into its parent directory (`.` at the top level) and final
/// component.
pub fn split(rel: &Path) -> (&Path, &OsStr) {
    let base = rel.file_name().unwrap_or(rel.as_os_str());
    (parent_of(rel).unwrap_or(Path::new(".")), base)
}

/// Whether something (anything, symlinks included) exists at `rel`.
pub fn lexists(root: &Dir, rel: &Path) -> io::Result<bool> {
    match root.symlink_metadata(rel) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Removes whatever is at `rel`: a directory tree, or a single entry. A
/// symlink is removed itself, never followed.
pub fn remove_all(root: &Dir, rel: &Path) -> io::Result<()> {
    let md = match root.symlink_metadata(rel) {
        Ok(md) => md,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    if md.is_dir() {
        root.remove_dir_all(rel)
    } else {
        root.remove_file(rel)
    }
}

/// Creates `rel` and its missing parents with `mode` (umask applies).
pub fn mkdir_all(root: &Dir, rel: &Path, mode: u32) -> io::Result<()> {
    let mut b = DirBuilder::new();
    b.recursive(true).mode(mode);
    root.create_dir_with(rel, &b)
}

/// Creates the single directory `rel` with `mode` (umask applies).
pub fn mkdir(root: &Dir, rel: &Path, mode: u32) -> io::Result<()> {
    let mut b = DirBuilder::new();
    b.mode(mode);
    root.create_dir_with(rel, &b)
}

/// Creates (truncating) the regular file `rel` with `mode` (umask applies).
pub fn create_file(root: &Dir, rel: &Path, mode: u32) -> io::Result<std::fs::File> {
    let mut o = OpenOptions::new();
    o.read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .mode(mode);
    Ok(root.open_with(rel, &o)?.into_std())
}

/// Sets `rel`'s permission bits.
pub fn chmod(root: &Dir, rel: &Path, mode: u32) -> io::Result<()> {
    root.set_permissions(rel, Permissions::from_mode(mode))
}

/// Sets a directory's access and modification times.
///
/// Goes through `utimensat` on the parent rather than a handle to the
/// directory itself: directory handles from `Dir::open_dir` are `O_PATH` on
/// Linux, and setting times through one fails with EBADF.
pub fn set_dir_times(root: &Dir, rel: &Path, t: SystemTime) -> io::Result<()> {
    if rel.file_name().is_some() {
        return lutimes(root, rel, t);
    }
    let ts = timespec(t);
    let times = [ts, ts];
    let dot = CString::new(".").expect("no NUL");
    // SAFETY: a valid directory fd, NUL-terminated name and two timespecs.
    check(unsafe { libc::utimensat(root.as_raw_fd(), dot.as_ptr(), times.as_ptr(), 0) })
}

fn timespec(t: SystemTime) -> libc::timespec {
    let d = t.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    libc::timespec {
        tv_sec: d.as_secs() as libc::time_t,
        tv_nsec: d.subsec_nanos() as _,
    }
}

/// Sets an open file's access and modification times.
pub fn set_file_times(f: &std::fs::File, t: SystemTime) -> io::Result<()> {
    f.set_times(std::fs::FileTimes::new().set_accessed(t).set_modified(t))
}

fn cstr(name: &OsStr) -> io::Result<CString> {
    CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name contains NUL"))
}

fn check(r: libc::c_int) -> io::Result<()> {
    if r == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Runs `f` with the file descriptor of `rel`'s parent, opened through
/// `root`, and `rel`'s final component. The final component is a single
/// name, so a call relative to the parent cannot walk anywhere else.
pub fn at_parent<T>(
    root: &Dir,
    rel: &Path,
    f: impl FnOnce(libc::c_int, &CString) -> io::Result<T>,
) -> io::Result<T> {
    let (parent, base) = split(rel);
    let dir = root.open_dir(parent)?;
    let name = cstr(base)?;
    f(dir.as_raw_fd(), &name)
}

/// Changes `rel`'s owner without following a final symlink.
pub fn lchown(root: &Dir, rel: &Path, uid: u32, gid: u32) -> io::Result<()> {
    at_parent(root, rel, |fd, name| {
        // SAFETY: a valid directory fd and NUL-terminated name.
        check(unsafe { libc::fchownat(fd, name.as_ptr(), uid, gid, libc::AT_SYMLINK_NOFOLLOW) })
    })
}

/// Sets `rel`'s times without following a final symlink.
pub fn lutimes(root: &Dir, rel: &Path, t: SystemTime) -> io::Result<()> {
    let ts = timespec(t);
    let times = [ts, ts];
    at_parent(root, rel, |fd, name| {
        // SAFETY: a valid directory fd, NUL-terminated name and two timespecs.
        check(unsafe {
            libc::utimensat(fd, name.as_ptr(), times.as_ptr(), libc::AT_SYMLINK_NOFOLLOW)
        })
    })
}

/// Creates a FIFO at `rel`.
pub fn mkfifo(root: &Dir, rel: &Path, mode: u32) -> io::Result<()> {
    at_parent(root, rel, |fd, name| {
        // SAFETY: a valid directory fd and NUL-terminated name.
        check(unsafe { libc::mkfifoat(fd, name.as_ptr(), mode as libc::mode_t) })
    })
}

/// Creates a device node at `rel`. `kind` is `S_IFCHR` or `S_IFBLK`.
pub fn mknod(
    root: &Dir,
    rel: &Path,
    kind: libc::mode_t,
    mode: u32,
    major: u32,
    minor: u32,
) -> io::Result<()> {
    let dev = make_dev(major, minor);
    at_parent(root, rel, |fd, name| {
        // SAFETY: a valid directory fd and NUL-terminated name.
        check(unsafe { libc::mknodat(fd, name.as_ptr(), kind | mode as libc::mode_t, dev) })
    })
}

#[cfg(target_os = "linux")]
fn make_dev(major: u32, minor: u32) -> libc::dev_t {
    libc::makedev(major, minor)
}

#[cfg(not(target_os = "linux"))]
fn make_dev(major: u32, minor: u32) -> libc::dev_t {
    ((major << 24) | (minor & 0xff_ffff)) as libc::dev_t
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_matches_lexical_semantics() {
        for (input, want) in [
            ("", "."),
            (".", "."),
            ("etc/", "etc"),
            ("/etc/passwd", "/etc/passwd"),
            ("//etc/passwd", "/etc/passwd"),
            ("../etc", "../etc"),
            ("a/../../escape", "../escape"),
            ("./a/./b", "a/b"),
            ("/../x", "/x"),
        ] {
            assert_eq!(
                String::from_utf8(clean(input.as_bytes())).unwrap(),
                want,
                "{input}"
            );
        }
    }

    #[test]
    fn helpers_stay_in_root() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), tmp.path().join("evil")).unwrap();
        let root = open_root(tmp.path()).unwrap();
        assert!(mkdir_all(&root, Path::new("evil/x"), 0o755).is_err());
        assert!(create_file(&root, Path::new("evil/f"), 0o644).is_err());
        assert!(std::fs::read_dir(outside.path()).unwrap().next().is_none());
        mkdir_all(&root, Path::new("a/b"), 0o755).unwrap();
        assert!(lexists(&root, Path::new("a/b")).unwrap());
        remove_all(&root, Path::new("evil")).unwrap();
        assert!(outside.path().exists());
    }
}

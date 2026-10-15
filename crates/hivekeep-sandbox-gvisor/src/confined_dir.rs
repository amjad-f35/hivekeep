//! Filesystem access confined beneath a directory.
//!
//! Paths are walked one component at a time with `openat` and `O_NOFOLLOW`
//! from a held directory descriptor. Symlinks are expanded by hand and may
//! point anywhere inside the directory, but never above it and never at an
//! absolute target, so a tree written by someone else (an Actor's image, a
//! driver directory) cannot redirect an access to elsewhere in the worker's
//! mount namespace, and a swap of a component mid-walk cannot either.

use std::collections::VecDeque;
use std::ffi::{CString, OsStr, OsString};
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::Path;

/// How many symlinks one walk may expand, as the kernel allows.
const MAX_SYMLINKS: usize = 40;

/// A directory that accesses cannot leave.
#[derive(Debug)]
pub struct ConfinedDir {
    fd: OwnedFd,
}

fn cstr(s: &OsStr) -> io::Result<CString> {
    CString::new(s.as_bytes()).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))
}

fn cvt(rc: libc::c_int) -> io::Result<libc::c_int> {
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(rc)
    }
}

fn escapes(rel: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!("path {} escapes from its parent", rel.display()),
    )
}

fn open_dir_at(dir: RawFd, name: &OsStr) -> io::Result<OwnedFd> {
    let c = cstr(name)?;
    // SAFETY: `c` is NUL terminated and `dir` is a live descriptor.
    let fd = cvt(unsafe {
        libc::openat(
            dir,
            c.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    })?;
    // SAFETY: a fresh descriptor this function owns.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// `lstat` of `name` in `dir`.
fn lstat_at(dir: RawFd, name: &OsStr) -> io::Result<libc::stat> {
    let c = cstr(name)?;
    let mut st = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `c` is NUL terminated, `st` is writable.
    cvt(unsafe { libc::fstatat(dir, c.as_ptr(), st.as_mut_ptr(), libc::AT_SYMLINK_NOFOLLOW) })?;
    // SAFETY: fstatat succeeded, so it filled `st`.
    Ok(unsafe { st.assume_init() })
}

fn readlink_at(dir: RawFd, name: &OsStr) -> io::Result<OsString> {
    let c = cstr(name)?;
    let mut buf = vec![0u8; libc::PATH_MAX as usize];
    // SAFETY: `buf` is writable for its length.
    let n = unsafe { libc::readlinkat(dir, c.as_ptr(), buf.as_mut_ptr().cast(), buf.len()) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    buf.truncate(n as usize);
    Ok(OsString::from_vec(buf))
}

fn is_kind(st: &libc::stat, kind: libc::mode_t) -> bool {
    st.st_mode & libc::S_IFMT == kind
}

/// Where a walk ended: the directory holding the final component (with the
/// directories above it, so `..` can climb back), and that component, which
/// is not a symlink when the walk followed it. `None` is the root itself.
struct Walked {
    dirs: Vec<OwnedFd>,
    last: Option<OsString>,
}

impl Walked {
    fn dir(&self) -> RawFd {
        self.dirs
            .last()
            .expect("the root is always held")
            .as_raw_fd()
    }

    fn last(&self, rel: &Path) -> io::Result<&OsStr> {
        self.last.as_deref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} names the directory itself", rel.display()),
            )
        })
    }
}

impl ConfinedDir {
    /// Opens `dir`, which itself is resolved normally.
    pub fn open(dir: &Path) -> io::Result<Self> {
        let c = cstr(dir.as_os_str())?;
        // SAFETY: `c` is NUL terminated.
        let fd = cvt(unsafe {
            libc::open(
                c.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        })?;
        // SAFETY: a fresh descriptor this function owns.
        Ok(ConfinedDir {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }

    /// Walks `rel`. Intermediate symlinks are always expanded (inside the
    /// directory); the final one only when `follow_last` is set. A missing
    /// final component is fine, so callers can create it.
    fn walk(&self, rel: &Path, follow_last: bool) -> io::Result<Walked> {
        if rel.is_absolute() {
            return Err(escapes(rel));
        }
        let mut queue: VecDeque<OsString> = split(rel.as_os_str());
        let mut dirs = vec![self.fd.try_clone()?];
        let mut links = 0;
        while let Some(comp) = queue.pop_front() {
            let top = dirs.last().expect("the root is always held").as_raw_fd();
            if comp == ".." {
                if dirs.len() == 1 {
                    return Err(escapes(rel));
                }
                dirs.pop();
                continue;
            }
            let is_last = queue.is_empty();
            if is_last && !follow_last {
                return Ok(Walked {
                    dirs,
                    last: Some(comp),
                });
            }
            let st = match lstat_at(top, &comp) {
                Err(e) if e.kind() == io::ErrorKind::NotFound && is_last => {
                    return Ok(Walked {
                        dirs,
                        last: Some(comp),
                    })
                }
                r => r?,
            };
            if is_kind(&st, libc::S_IFLNK) {
                links += 1;
                if links > MAX_SYMLINKS {
                    return Err(io::Error::other(format!(
                        "too many symlinks resolving {}",
                        rel.display()
                    )));
                }
                let target = readlink_at(top, &comp)?;
                if Path::new(&target).is_absolute() {
                    return Err(escapes(rel));
                }
                for c in split(&target).into_iter().rev() {
                    queue.push_front(c);
                }
                continue;
            }
            if is_last {
                return Ok(Walked {
                    dirs,
                    last: Some(comp),
                });
            }
            if !is_kind(&st, libc::S_IFDIR) {
                return Err(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    format!("{}: not a directory", rel.display()),
                ));
            }
            dirs.push(open_dir_at(top, &comp)?);
        }
        Ok(Walked { dirs, last: None })
    }

    /// `stat` of `rel`, following symlinks that stay inside.
    pub fn stat(&self, rel: &Path) -> io::Result<libc::stat> {
        let w = self.walk(rel, true)?;
        match &w.last {
            Some(name) => lstat_at(w.dir(), name),
            None => {
                let mut st = std::mem::MaybeUninit::<libc::stat>::uninit();
                // SAFETY: `st` is writable and the descriptor is live.
                cvt(unsafe { libc::fstat(w.dir(), st.as_mut_ptr()) })?;
                // SAFETY: fstat succeeded.
                Ok(unsafe { st.assume_init() })
            }
        }
    }

    /// Opens `rel` for reading, following symlinks that stay inside.
    pub fn open_file(&self, rel: &Path) -> io::Result<File> {
        let w = self.walk(rel, true)?;
        let c = cstr(w.last(rel)?)?;
        // SAFETY: `c` is NUL terminated and the descriptor is live.
        let fd = cvt(unsafe {
            libc::openat(
                w.dir(),
                c.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        })?;
        // SAFETY: a fresh descriptor this function owns.
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    /// `mkdir -p rel` with `mode`.
    pub fn create_dir_all(&self, rel: &Path, mode: libc::mode_t) -> io::Result<()> {
        let comps = split(rel.as_os_str());
        let mut prefix = std::path::PathBuf::new();
        for c in comps {
            prefix.push(&c);
            if c == ".." {
                continue;
            }
            let w = self.walk(&prefix, true)?;
            let Some(name) = &w.last else { continue };
            let cname = cstr(name)?;
            // SAFETY: `cname` is NUL terminated and the descriptor is live.
            match cvt(unsafe { libc::mkdirat(w.dir(), cname.as_ptr(), mode) }) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    let st = lstat_at(w.dir(), name)?;
                    if !is_kind(&st, libc::S_IFDIR) {
                        return Err(io::Error::new(
                            io::ErrorKind::NotADirectory,
                            format!("{}: not a directory", prefix.display()),
                        ));
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Removes the file or symlink `rel` (not what it points at).
    pub fn remove_file(&self, rel: &Path) -> io::Result<()> {
        let w = self.walk(rel, false)?;
        let c = cstr(w.last(rel)?)?;
        // SAFETY: `c` is NUL terminated and the descriptor is live.
        cvt(unsafe { libc::unlinkat(w.dir(), c.as_ptr(), 0) }).map(drop)
    }

    /// Creates the symlink `rel` pointing at `target`.
    pub fn symlink(&self, target: &Path, rel: &Path) -> io::Result<()> {
        let w = self.walk(rel, false)?;
        let c = cstr(w.last(rel)?)?;
        let t = cstr(target.as_os_str())?;
        // SAFETY: both are NUL terminated and the descriptor is live.
        cvt(unsafe { libc::symlinkat(t.as_ptr(), w.dir(), c.as_ptr()) }).map(drop)
    }
}

/// The components of a relative path, without empty and `.` ones.
fn split(p: &OsStr) -> VecDeque<OsString> {
    p.as_bytes()
        .split(|b| *b == b'/')
        .filter(|c| !c.is_empty() && *c != b".")
        .map(|c| OsString::from_vec(c.to_vec()))
        .collect()
}

/// Whether a stat is of a device node.
pub fn is_device(st: &libc::stat) -> bool {
    is_kind(st, libc::S_IFCHR) || is_kind(st, libc::S_IFBLK)
}

/// Whether a stat is of a regular file.
pub fn is_regular(st: &libc::stat) -> bool {
    is_kind(st, libc::S_IFREG)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::os::unix::fs::symlink;

    #[test]
    fn follows_symlinks_that_stay_inside() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("a/b")).unwrap();
        std::fs::write(d.path().join("a/b/f"), b"hi").unwrap();
        symlink("b/f", d.path().join("a/rel")).unwrap();
        symlink("a/b", d.path().join("dirlink")).unwrap();
        let c = ConfinedDir::open(d.path()).unwrap();
        for p in ["a/b/f", "a/rel", "dirlink/f", "a/b/../b/f"] {
            let mut s = String::new();
            c.open_file(Path::new(p))
                .unwrap()
                .read_to_string(&mut s)
                .unwrap();
            assert_eq!(s, "hi", "{p}");
        }
    }

    #[test]
    fn refuses_to_leave() {
        let d = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), b"x").unwrap();
        symlink(outside.path(), d.path().join("abs")).unwrap();
        symlink("../../secret", d.path().join("rel")).unwrap();
        let c = ConfinedDir::open(d.path()).unwrap();
        for p in ["abs/secret", "rel", "../x", "/etc/passwd"] {
            assert!(c.stat(Path::new(p)).is_err(), "{p}");
        }
        assert!(c.create_dir_all(Path::new("abs/new"), 0o755).is_err());
        assert!(!outside.path().join("new").exists());
    }

    #[test]
    fn creates_dirs_and_links() {
        let d = tempfile::tempdir().unwrap();
        let c = ConfinedDir::open(d.path()).unwrap();
        c.create_dir_all(Path::new("x/y/z"), 0o755).unwrap();
        c.create_dir_all(Path::new("x/y/z"), 0o755).unwrap();
        c.symlink(Path::new("target"), Path::new("x/y/z/l"))
            .unwrap();
        assert_eq!(
            std::fs::read_link(d.path().join("x/y/z/l")).unwrap(),
            Path::new("target")
        );
        c.remove_file(Path::new("x/y/z/l")).unwrap();
        assert!(c.remove_file(Path::new("x/y/z/l")).is_err());
    }
}

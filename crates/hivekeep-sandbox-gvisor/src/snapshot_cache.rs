//! Releasing the page cache and disk space of snapshot files a restore has
//! finished with.

use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// A file found under a restore directory, pinned by identity so a file
/// replaced since is never touched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct File {
    /// Where it was found.
    pub path: PathBuf,
    dev: u64,
    ino: u64,
    size: u64,
    /// Hard links when found.
    pub nlink: u64,
}

impl File {
    fn of(path: PathBuf, m: &std::fs::Metadata) -> Self {
        File {
            path,
            dev: m.dev(),
            ino: m.ino(),
            size: m.len(),
            nlink: m.nlink(),
        }
    }

    fn same_as(&self, m: &std::fs::Metadata) -> bool {
        m.dev() == self.dev && m.ino() == self.ino && m.len() == self.size
    }
}

/// Delays between eviction retries while pages are still dirty.
pub const RETRY_DELAYS: [Duration; 3] = [
    Duration::from_millis(500),
    Duration::from_millis(1500),
    Duration::from_millis(3000),
];

/// The regular files under `dir`, recursively, whose names match.
pub fn find_files(dir: &Path, matches: impl Fn(&str) -> bool) -> Vec<File> {
    let mut out = Vec::new();
    if !dir.as_os_str().is_empty() {
        walk(dir, &matches, &mut out);
    }
    out
}

fn walk(dir: &Path, matches: &impl Fn(&str) -> bool, out: &mut Vec<File>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let Ok(ft) = e.file_type() else { continue };
        if ft.is_dir() {
            walk(&e.path(), matches, out);
        } else if ft.is_file() && matches(&e.file_name().to_string_lossy()) {
            if let Ok(m) = e.metadata() {
                out.push(File::of(e.path(), &m));
            }
        }
    }
}

/// Unlinks `f` if it is still the file that was found. Reports whether it did.
pub fn drop_file(f: &File) -> bool {
    match std::fs::metadata(&f.path) {
        Ok(m) if f.same_as(&m) => match std::fs::remove_file(&f.path) {
            Ok(()) => true,
            Err(e) => {
                if e.kind() != io::ErrorKind::NotFound {
                    tracing::warn!(path = %f.path.display(), err = %e, "could not drop staged snapshot file");
                }
                false
            }
        },
        _ => false,
    }
}

/// Unlinks every file still the one found. Reports whether any went.
pub fn drop_files(files: &[File]) -> bool {
    let mut any = false;
    for f in files {
        any |= drop_file(f);
    }
    any
}

/// Evicts the files' clean pages from the host page cache. Reports whether
/// any pages could not be evicted and are worth another try.
#[cfg(target_os = "linux")]
pub fn evict_files(files: &[File]) -> bool {
    use std::os::fd::AsRawFd;
    let mut remaining = false;
    for f in files {
        let Ok(file) = std::fs::File::open(&f.path) else {
            continue;
        };
        match file.metadata() {
            Ok(m) if f.same_as(&m) => {}
            _ => continue,
        }
        // SAFETY: the fd is open for the call's duration.
        let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        if rc != 0 {
            remaining = true;
        }
    }
    remaining
}

/// No host page cache to manage.
#[cfg(not(target_os = "linux"))]
pub fn evict_files(_files: &[File]) -> bool {
    false
}

/// [`evict_files`], retried on [`RETRY_DELAYS`] while pages remain. Blocking.
pub fn evict_with_retries(files: &[File]) {
    if !evict_files(files) {
        return;
    }
    for d in RETRY_DELAYS {
        std::thread::sleep(d);
        if !evict_files(files) {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_and_drops_only_unchanged_files() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir(d.path().join("fs")).unwrap();
        for n in ["a.img", "fs/b.img", "c.json"] {
            std::fs::write(d.path().join(n), b"data").unwrap();
        }
        let mut found = find_files(d.path(), |n| n.ends_with(".img"));
        found.sort_by(|a, b| a.path.cmp(&b.path));
        assert_eq!(found.len(), 2);
        // A replaced file is left alone.
        std::fs::remove_file(d.path().join("a.img")).unwrap();
        std::fs::write(d.path().join("a.img"), b"other data").unwrap();
        assert!(drop_files(&found));
        assert!(d.path().join("a.img").exists());
        assert!(!d.path().join("fs/b.img").exists());
        assert!(!drop_files(&found[1..]));
        assert!(find_files(Path::new(""), |_| true).is_empty());
        let _ = evict_files(&found);
    }
}

//! Files that readers never observe partly written.
//!
//! The data goes to a temporary file beside the target, which is then
//! renamed over it. [`write`] does not fsync, so after a node crash the
//! target may be empty or hold its old contents; [`write_sync`] is for a
//! file that must survive one.

use std::fs;
use std::io::{self, Write};
use std::path::Path;

/// Replaces `path` with `data`, mode `perm`, atomically but not durably.
pub fn write(path: &Path, data: &[u8], perm: u32) -> io::Result<()> {
    write_impl(path, data, perm, false)
}

/// Replaces `path` with `data`, mode `perm`, atomically and durably: the
/// data is synced before the rename and the directory after it.
pub fn write_sync(path: &Path, data: &[u8], perm: u32) -> io::Result<()> {
    write_impl(path, data, perm, true)
}

fn write_impl(path: &Path, data: &[u8], perm: u32, sync: bool) -> io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let base = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut tmp = tempfile_in(dir, &format!(".{base}.tmp-"))?;
    (|| {
        tmp.file.write_all(data)?;
        set_mode(&tmp.file, perm)?;
        if sync {
            tmp.file.sync_all()?;
        }
        fs::rename(&tmp.path, path)?;
        tmp.persisted = true;
        if sync {
            fs::File::open(dir)?.sync_all()?;
        }
        Ok(())
    })()
}

/// A temporary file removed on drop unless persisted.
pub(crate) struct TempFile {
    pub(crate) file: fs::File,
    pub(crate) path: std::path::PathBuf,
    pub(crate) persisted: bool,
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if !self.persisted {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Creates a new file in `dir` named `prefix` plus a random suffix.
pub(crate) fn tempfile_in(dir: &Path, prefix: &str) -> io::Result<TempFile> {
    for _ in 0..64 {
        let path = dir.join(format!("{prefix}{}", crate::rand::suffix()));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => {
                return Ok(TempFile {
                    file,
                    path,
                    persisted: false,
                })
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not create a unique temporary file",
    ))
}

/// Creates a new directory in `dir` named `prefix` plus a random suffix,
/// mode 0700.
pub fn tempdir_in(dir: &Path, prefix: &str) -> io::Result<std::path::PathBuf> {
    for _ in 0..64 {
        let path = dir.join(format!("{prefix}{}", crate::rand::suffix()));
        match create_dir_mode(&path, 0o700) {
            Ok(()) => return Ok(path),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not create a unique temporary directory",
    ))
}

/// Creates one directory with `mode` (subject to the umask).
pub fn create_dir_mode(path: &Path, mode: u32) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new().mode(mode).create(path)
    }
    #[cfg(not(unix))]
    {
        let _ = mode;
        fs::create_dir(path)
    }
}

/// Creates a directory and its parents with `mode` (subject to the umask).
pub fn create_dir_all_mode(path: &Path, mode: u32) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(mode)
            .create(path)
    }
    #[cfg(not(unix))]
    {
        let _ = mode;
        fs::create_dir_all(path)
    }
}

/// Sets the permission bits of an open file.
pub fn set_mode(file: &fs::File, mode: u32) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(mode))
    }
    #[cfg(not(unix))]
    {
        let _ = (file, mode);
        Ok(())
    }
}

/// Sets the permission bits of a path.
pub fn chmod(path: &Path, mode: u32) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode))
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
        Ok(())
    }
}

/// The permission bits of `meta`.
pub fn mode_of(meta: &fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o7777
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        0o644
    }
}

#[cfg(test)]
mod behavior_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_replaces_and_sets_mode() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("state.json");
        fs::write(&target, "old").unwrap();
        write(&target, b"new", 0o644).unwrap();
        assert_eq!(fs::read_to_string(&target).unwrap(), "new");
        assert_eq!(mode_of(&fs::metadata(&target).unwrap()), 0o644);
        let names: Vec<_> = fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(names.len(), 1, "no temp files left behind");
    }

    #[test]
    fn write_sync_creates_and_replaces() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.json");
        write_sync(&path, b"v1", 0o600).unwrap();
        write_sync(&path, b"v2", 0o600).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "v2");
        assert_eq!(mode_of(&fs::metadata(&path).unwrap()), 0o600);
    }

    #[test]
    fn write_into_missing_dir_fails_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing").join("f");
        assert!(write(&path, b"x", 0o600).is_err());
    }
}

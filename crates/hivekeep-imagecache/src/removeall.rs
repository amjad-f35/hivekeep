//! Removing unpacked image trees.
//!
//! Unpacked layers keep the image's (possibly read-only) directory modes,
//! which a root process without `CAP_DAC_OVERRIDE` cannot delete from. The
//! cache owns these files, so it makes every directory owner-writable first;
//! chmod on owned files needs no capability.

use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// Removes `path` and everything under it, first making each directory
/// owner-accessible top-down so the walk can descend and unlink children.
pub fn remove_all_writable(path: &Path) -> io::Result<()> {
    let md = match fs::symlink_metadata(path) {
        Ok(md) => md,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    if !md.is_dir() {
        return fs::remove_file(path);
    }
    make_writable(path);
    fs::remove_dir_all(path).or_else(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            Ok(())
        } else {
            Err(e)
        }
    })
}

/// Best effort: errors are left for the removal to surface.
fn make_writable(dir: &Path) {
    let _ = fs::set_permissions(dir, fs::Permissions::from_mode(0o700));
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            make_writable(&e.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_read_only_trees() {
        let tmp = tempfile::tempdir().unwrap();
        let ro = tmp.path().join("t/ro");
        fs::create_dir_all(&ro).unwrap();
        fs::write(ro.join("f"), b"x").unwrap();
        fs::set_permissions(&ro, fs::Permissions::from_mode(0o500)).unwrap();
        fs::set_permissions(tmp.path().join("t"), fs::Permissions::from_mode(0o000)).unwrap();
        remove_all_writable(&tmp.path().join("t")).unwrap();
        assert!(!tmp.path().join("t").exists());
        remove_all_writable(&tmp.path().join("missing")).unwrap();
    }
}

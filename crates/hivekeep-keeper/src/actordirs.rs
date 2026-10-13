//! Each actor's on-node directory trees: emptying them between activations
//! and reclaiming them once the actor leaves the node.
//!
//! External volume mount points are never removed recursively: if an
//! unmount failed, a recursive delete would delete the volume's contents.

use std::fs;
use std::io;
use std::path::Path;

use walkdir::WalkDir;

use crate::atomicfile::{chmod, create_dir_all_mode};
use crate::paths::{NodePaths, SNAPSHOT_STATE_FILE_NAME};

/// An actor-directory operation that failed, with what it was doing.
#[derive(Debug, thiserror::Error)]
#[error("{what}: {source}")]
pub struct DirError {
    what: String,
    #[source]
    source: io::Error,
}

impl DirError {
    fn new(what: impl Into<String>, source: io::Error) -> Self {
        DirError {
            what: what.into(),
            source,
        }
    }
}

impl From<DirError> for tonic::Status {
    fn from(e: DirError) -> Self {
        tonic::Status::internal(e.to_string())
    }
}

/// Removes `path` and everything below it like `remove_dir_all`, after
/// making every directory in it writable. A bundle's upper dir can hold
/// copied-up image directories that keep the image's read-only modes,
/// which even root cannot unlink entries from without the write bit when
/// the filesystem enforces it. A missing path is not an error.
pub fn remove_all_writable(path: &Path) -> io::Result<()> {
    for entry in WalkDir::new(path).into_iter().flatten() {
        if entry.file_type().is_dir() {
            let _ = chmod(entry.path(), 0o700);
        }
    }
    remove_all(path)
}

/// `remove_dir_all` for a directory or file, where a missing path is not an
/// error.
pub fn remove_all(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
        Ok(m) if m.is_dir() => match fs::remove_dir_all(path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            r => r,
        },
        Ok(_) => match fs::remove_file(path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            r => r,
        },
    }
}

/// Empties `dir` with `remove` and recreates it with `mode`. `what` names
/// the directory in errors.
pub fn reset_dir(
    what: &str,
    dir: &Path,
    remove: fn(&Path) -> io::Result<()>,
    mode: u32,
) -> Result<(), DirError> {
    remove(dir).map_err(|e| DirError::new(format!("while deleting {what}"), e))?;
    create_dir_all_mode(dir, mode)
        .map_err(|e| DirError::new(format!("while creating {what}"), e))?;
    // The umask may have narrowed the mode.
    chmod(dir, mode).map_err(|e| DirError::new(format!("while setting mode of {what}"), e))
}

/// Removes each empty mount point under the actor's external volumes dir,
/// refusing to delete through a populated one.
fn remove_empty_volume_dirs(paths: &NodePaths, actor_uid: &str) -> Result<(), DirError> {
    let dir = paths.external_volume_mounts_dir(actor_uid);
    let entries = match fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(DirError::new("while reading volumes dir", e)),
    };
    for entry in entries {
        let entry = entry.map_err(|e| DirError::new("while reading volumes dir", e))?;
        match fs::remove_dir(entry.path()) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(DirError::new("while removing volume dir", e)),
        }
    }
    Ok(())
}

/// Empties the actor's directories and leaves them in place for its next
/// activation. Use [`remove_actor_dirs`] when the actor will not return.
pub fn reset_actor_dirs(paths: &NodePaths, actor_uid: &str) -> Result<(), DirError> {
    reset_actor_dirs_keeping_volumes(paths, actor_uid)?;
    remove_empty_volume_dirs(paths, actor_uid)
}

/// [`reset_actor_dirs`] for Run and Restore, which leave the external
/// volume mount points alone: a failed earlier attempt can leave a volume
/// published there, and publishing it again is a no-op.
pub fn reset_actor_dirs_keeping_volumes(
    paths: &NodePaths,
    actor_uid: &str,
) -> Result<(), DirError> {
    reset_dir(
        "bundle dir",
        &paths.oci_bundle_dir(actor_uid),
        remove_all_writable,
        0o700,
    )?;
    reset_dir(
        "checkpoint-state dir",
        &paths.checkpoint_state_dir(actor_uid),
        remove_all,
        0o700,
    )?;
    reset_dir(
        "restore-state dir",
        &paths.restore_state_dir(actor_uid),
        remove_all,
        0o700,
    )?;
    reset_dir(
        "durable-dir volumes mount dir",
        &paths.durable_dir_volume_mounts_dir(actor_uid),
        remove_all,
        0o755,
    )?;
    // World-readable: bind-mounted read-only into the actor.
    reset_dir(
        "system-info volume roots dir",
        &paths.system_info_volume_roots_dir(actor_uid),
        remove_all,
        0o755,
    )?;
    create_dir_all_mode(&paths.external_volume_mounts_dir(actor_uid), 0o755)
        .map_err(|e| DirError::new("while creating volumes dir", e))
}

/// Removes the actor's snapshot tree and everything under its actor dir
/// except the snapshot state file, refusing to proceed while an external
/// volume mount point is still populated.
pub fn remove_actor_files_except_state(paths: &NodePaths, actor_uid: &str) -> Result<(), DirError> {
    remove_empty_volume_dirs(paths, actor_uid)?;
    match fs::remove_dir(paths.external_volume_mounts_dir(actor_uid)) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(DirError::new("while removing volumes dir", e)),
    }
    remove_all(&paths.actor_snapshot_dir(actor_uid))
        .map_err(|e| DirError::new("while deleting actor snapshot dir", e))?;
    let actor_dir = paths.actor_dir(actor_uid);
    let entries = match fs::read_dir(&actor_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(DirError::new("while reading actor dir", e)),
    };
    for entry in entries {
        let entry = entry.map_err(|e| DirError::new("while reading actor dir", e))?;
        if entry.file_name() == SNAPSHOT_STATE_FILE_NAME {
            continue;
        }
        remove_all_writable(&entry.path())
            .map_err(|e| DirError::new("while deleting actor dir", e))?;
    }
    Ok(())
}

/// Reclaims both of the actor's directory trees, roots included. It
/// refuses to proceed while a volume mount point is populated, so a failed
/// unmount never becomes a deletion of the volume's contents.
pub fn remove_actor_dirs(paths: &NodePaths, actor_uid: &str) -> Result<(), DirError> {
    remove_actor_files_except_state(paths, actor_uid)?;
    match fs::remove_file(paths.actor_snapshot_state_file(actor_uid)) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(DirError::new(
                "while deleting the actor's snapshot state",
                e,
            ))
        }
    }
    match fs::remove_dir(paths.actor_dir(actor_uid)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(DirError::new("while deleting actor dir", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (tempfile::TempDir, NodePaths) {
        let dir = tempfile::tempdir().unwrap();
        let paths = NodePaths::new(dir.path());
        (dir, paths)
    }

    #[test]
    fn reset_recreates_dirs_and_keeps_records() {
        let (_d, p) = setup();
        let uid = "a1";
        reset_actor_dirs(&p, uid).unwrap();
        fs::write(p.checkpoint_state_dir(uid).join("img"), b"x").unwrap();
        fs::write(p.actor_sandbox_assets_file(uid), b"{}").unwrap();
        reset_actor_dirs(&p, uid).unwrap();
        assert!(p.checkpoint_state_dir(uid).is_dir());
        assert!(!p.checkpoint_state_dir(uid).join("img").exists());
        assert!(p.actor_sandbox_assets_file(uid).exists());
    }

    #[test]
    fn remove_refuses_populated_volume() {
        let (_d, p) = setup();
        let uid = "a1";
        reset_actor_dirs(&p, uid).unwrap();
        let vol = p.external_volume_host_path(uid, "data");
        fs::create_dir_all(&vol).unwrap();
        fs::write(vol.join("precious"), b"keep").unwrap();
        assert!(remove_actor_dirs(&p, uid).is_err());
        assert!(vol.join("precious").exists());
    }

    #[test]
    fn remove_except_state_keeps_state_file() {
        let (_d, p) = setup();
        let uid = "a1";
        reset_actor_dirs(&p, uid).unwrap();
        fs::create_dir_all(p.local_snapshot_dir(uid, "s1")).unwrap();
        fs::write(p.actor_snapshot_state_file(uid), b"{}").unwrap();
        remove_actor_files_except_state(&p, uid).unwrap();
        assert!(p.actor_snapshot_state_file(uid).exists());
        assert!(!p.actor_snapshot_dir(uid).exists());
        assert!(!p.oci_bundle_dir(uid).exists());
        remove_actor_dirs(&p, uid).unwrap();
        assert!(!p.actor_dir(uid).exists());
    }

    #[cfg(unix)]
    #[test]
    fn remove_all_writable_handles_read_only_dirs() {
        let (_d, p) = setup();
        let dir = p.base().join("ro");
        fs::create_dir_all(dir.join("inner")).unwrap();
        fs::write(dir.join("inner/f"), b"x").unwrap();
        chmod(&dir.join("inner"), 0o500).unwrap();
        remove_all_writable(&dir).unwrap();
        assert!(!dir.exists());
    }
}

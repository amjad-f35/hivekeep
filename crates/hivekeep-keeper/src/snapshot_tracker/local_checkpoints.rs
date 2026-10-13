//! An actor's node-local snapshots on disk.

use std::fs;
use std::io;
use std::path::Path;

use walkdir::WalkDir;

use crate::paths::NodePaths;
use crate::sparsefile::allocated_bytes;

/// Removes every local snapshot of the actor except those named in `keep`,
/// and the local checkpoints dir itself once nothing is kept. A missing
/// directory is not an error, so retries are safe. Every snapshot is
/// attempted, so one undeletable snapshot does not strand the others.
pub fn prune_local_checkpoints(
    paths: &NodePaths,
    actor_uid: &str,
    keep: &[String],
) -> Result<(), String> {
    prune_dir(&paths.local_checkpoints_dir(actor_uid), keep)
}

fn prune_dir(dir: &Path, keep: &[String]) -> Result<(), String> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(format!(
                "while listing local checkpoints in {}: {e}",
                dir.display()
            ))
        }
    };
    let mut errs = Vec::new();
    let mut kept = false;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if keep.contains(&name) {
            kept = true;
            continue;
        }
        let path = entry.path();
        match crate::actordirs::remove_all(&path) {
            Ok(()) => tracing::info!(path = %path.display(), "pruned local checkpoint"),
            Err(e) => errs.push(format!(
                "while pruning local checkpoint {}: {e}",
                path.display()
            )),
        }
    }
    if !errs.is_empty() {
        return Err(errs.join("; "));
    }
    if !kept {
        match fs::remove_dir(dir) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(format!(
                    "while removing local checkpoints dir {}: {e}",
                    dir.display()
                ))
            }
        }
    }
    Ok(())
}

/// Removes one local snapshot. A missing snapshot is not an error.
pub fn remove_local_checkpoint(paths: &NodePaths, actor_uid: &str, name: &str) -> io::Result<()> {
    let path = paths.local_snapshot_dir(actor_uid, name);
    crate::actordirs::remove_all(&path)?;
    tracing::info!(path = %path.display(), "removed local checkpoint");
    Ok(())
}

/// The disk space allocated to one local snapshot's files. Allocated
/// blocks track what an upload sends, where apparent sizes would count a
/// sparse memory image at its full size. Missing files count nothing; on
/// error it returns what it measured so far.
pub fn local_snapshot_size(
    paths: &NodePaths,
    actor_uid: &str,
    name: &str,
) -> (i64, Option<String>) {
    let mut size = 0;
    for entry in WalkDir::new(paths.local_snapshot_dir(actor_uid, name)) {
        match entry {
            Ok(e) if e.file_type().is_file() => match e.metadata() {
                Ok(m) => size += allocated_bytes(&m),
                Err(err) if is_not_found(&err) => {}
                Err(err) => return (size, Some(err.to_string())),
            },
            Ok(_) => {}
            Err(err) if is_not_found(&err) => {}
            Err(err) => return (size, Some(err.to_string())),
        }
    }
    (size, None)
}

fn is_not_found(e: &walkdir::Error) -> bool {
    e.io_error()
        .is_some_and(|e| e.kind() == io::ErrorKind::NotFound)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prune_keeps_named_and_removes_dir_when_empty() {
        let d = tempfile::tempdir().unwrap();
        let p = NodePaths::new(d.path());
        for s in ["s1", "s2", "s3"] {
            fs::create_dir_all(p.local_snapshot_dir("u", s)).unwrap();
            fs::write(p.local_snapshot_dir("u", s).join("mem"), b"x").unwrap();
        }
        prune_local_checkpoints(&p, "u", &["s2".to_string()]).unwrap();
        assert!(!p.local_snapshot_dir("u", "s1").exists());
        assert!(p.local_snapshot_dir("u", "s2").exists());
        prune_local_checkpoints(&p, "u", &[]).unwrap();
        assert!(!p.local_checkpoints_dir("u").exists());
        prune_local_checkpoints(&p, "u", &[]).unwrap();
    }

    #[test]
    fn size_counts_nested_files_and_tolerates_missing() {
        let d = tempfile::tempdir().unwrap();
        let p = NodePaths::new(d.path());
        assert_eq!(local_snapshot_size(&p, "u", "gone"), (0, None));
        let dir = p.local_snapshot_dir("u", "s1");
        fs::create_dir_all(dir.join("fs")).unwrap();
        fs::write(dir.join("fs/img"), vec![1u8; 8192]).unwrap();
        let (size, err) = local_snapshot_size(&p, "u", "s1");
        assert!(err.is_none());
        assert!(size >= 8192);
        remove_local_checkpoint(&p, "u", "s1").unwrap();
        assert!(!dir.exists());
    }
}

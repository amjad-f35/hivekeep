//! The completion marker of a checkpoint, so a retry after the caller timed
//! out (or the keeper restarted) can reuse a checkpoint that finished.

use std::io;
use std::path::Path;

use hivekeep_proto::worker::v1 as pb;
use serde::{Deserialize, Serialize};

/// Written into the checkpoint directory once every file is listed and the
/// workload is torn down. Cleared with the directory on the next checkpoint.
pub const CHECKPOINT_DONE_FILE: &str = ".checkpoint-done.json";

#[derive(Serialize, Deserialize)]
struct Record {
    fidelity: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    snapshot_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    data_snapshot_files: Vec<String>,
}

/// Records `resp` as the finished checkpoint in `dir`.
pub fn record_checkpoint(
    dir: &Path,
    fidelity: i32,
    resp: &pb::CheckpointWorkloadResponse,
) -> io::Result<()> {
    let b = serde_json::to_vec(&Record {
        fidelity,
        snapshot_files: resp.snapshot_files.clone(),
        data_snapshot_files: resp.data_snapshot_files.clone(),
    })?;
    let tmp = dir.join(format!("{CHECKPOINT_DONE_FILE}.tmp"));
    std::fs::write(&tmp, b)?;
    std::fs::rename(tmp, dir.join(CHECKPOINT_DONE_FILE))
}

/// The recorded response in `dir`, if it is for `fidelity`, consistent, and
/// every file it lists is still a regular file on disk.
pub fn reuse_checkpoint(dir: &Path, fidelity: i32) -> Option<pb::CheckpointWorkloadResponse> {
    let b = std::fs::read(dir.join(CHECKPOINT_DONE_FILE)).ok()?;
    let rec: Record = serde_json::from_slice(&b).ok()?;
    if rec.fidelity != fidelity {
        return None;
    }
    // The data files are the VOLUMES subset: all of them on VOLUMES, a strict
    // subset otherwise.
    if fidelity == pb::SnapshotFidelity::Volumes as i32 {
        if rec.snapshot_files.len() != rec.data_snapshot_files.len()
            || rec
                .snapshot_files
                .iter()
                .any(|f| !rec.data_snapshot_files.contains(f))
        {
            return None;
        }
    } else if rec.snapshot_files.is_empty()
        || rec.data_snapshot_files.len() >= rec.snapshot_files.len()
    {
        return None;
    }
    if rec
        .data_snapshot_files
        .iter()
        .any(|f| !rec.snapshot_files.contains(f))
    {
        return None;
    }
    if !rec.snapshot_files.iter().all(|f| {
        std::fs::metadata(dir.join(f))
            .map(|m| m.is_file())
            .unwrap_or(false)
    }) {
        return None;
    }
    Some(pb::CheckpointWorkloadResponse {
        snapshot_files: rec.snapshot_files,
        data_snapshot_files: rec.data_snapshot_files,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MEMORY: i32 = pb::SnapshotFidelity::Memory as i32;
    const VOLUMES: i32 = pb::SnapshotFidelity::Volumes as i32;

    fn resp(all: &[&str], data: &[&str]) -> pb::CheckpointWorkloadResponse {
        pb::CheckpointWorkloadResponse {
            snapshot_files: all.iter().map(|s| s.to_string()).collect(),
            data_snapshot_files: data.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn round_trip_and_staleness() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir(d.path().join("fs")).unwrap();
        for f in ["checkpoint.img", "fs/pages.img"] {
            std::fs::write(d.path().join(f), b"x").unwrap();
        }
        let r = resp(&["checkpoint.img", "fs/pages.img"], &["fs/pages.img"]);
        assert!(reuse_checkpoint(d.path(), MEMORY).is_none());
        record_checkpoint(d.path(), MEMORY, &r).unwrap();
        assert_eq!(reuse_checkpoint(d.path(), MEMORY), Some(r));
        // Another fidelity, or a file gone, is not reusable.
        assert!(reuse_checkpoint(d.path(), VOLUMES).is_none());
        std::fs::remove_file(d.path().join("checkpoint.img")).unwrap();
        assert!(reuse_checkpoint(d.path(), MEMORY).is_none());
    }

    #[test]
    fn volumes_records_must_be_all_data() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("a"), b"x").unwrap();
        record_checkpoint(d.path(), VOLUMES, &resp(&["a"], &[])).unwrap();
        assert!(reuse_checkpoint(d.path(), VOLUMES).is_none());
        record_checkpoint(d.path(), VOLUMES, &resp(&["a"], &["a"])).unwrap();
        assert!(reuse_checkpoint(d.path(), VOLUMES).is_some());
        // A MEMORY record whose data files are not a strict subset is bogus.
        record_checkpoint(d.path(), MEMORY, &resp(&["a"], &["a"])).unwrap();
        assert!(reuse_checkpoint(d.path(), MEMORY).is_none());
    }
}

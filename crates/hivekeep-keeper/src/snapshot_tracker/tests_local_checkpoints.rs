//! Pruning and measuring an actor's node-local snapshots.

use std::fs;
use std::path::Path;

use super::test_support_core::UID;
use super::*;

fn write_snapshot_dir(dir: &Path, name: &str) {
    let p = dir.join(name);
    fs::create_dir_all(&p).unwrap();
    fs::write(p.join("memory.img"), b"x").unwrap();
}

/// Writes `n` bytes that compress poorly, so all of them are allocated.
fn write_dense_file(path: &Path, n: usize) {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    let data: Vec<u8> = (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect();
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, data).unwrap();
}

fn node() -> (tempfile::TempDir, NodePaths) {
    let d = tempfile::tempdir().unwrap();
    let p = NodePaths::new(d.path());
    (d, p)
}

/// Every file counts by its allocated blocks, nested ones included, so a
/// sparse image counts only its data; a missing snapshot counts nothing.
#[test]
fn local_snapshot_size_counts_allocated_blocks() {
    let (_d, p) = node();
    assert_eq!(local_snapshot_size(&p, UID, "s1"), (0, None));

    let dir = p.local_snapshot_dir(UID, "s1");
    const DENSE: i64 = 96 << 10;
    write_dense_file(&dir.join("pages.img"), 64 << 10);
    write_dense_file(&dir.join("fs").join("multitar.img"), 32 << 10);
    // A sparse image as large as a guest's memory, holding no data.
    let sparse = fs::File::create(dir.join("memory.img")).unwrap();
    sparse.set_len(1 << 30).unwrap();
    drop(sparse);

    let (got, err) = local_snapshot_size(&p, UID, "s1");
    assert!(err.is_none(), "{err:?}");
    assert!(
        (DENSE..=2 * DENSE).contains(&got),
        "size {got}, want about {DENSE}: holes must not count"
    );
}

#[test]
fn prune_removes_every_snapshot() {
    let (_d, p) = node();
    let dir = p.local_checkpoints_dir(UID);
    for s in ["pause-1", "pause-2", "pause-3"] {
        write_snapshot_dir(&dir, s);
    }
    prune_local_checkpoints(&p, UID, &[]).unwrap();
    assert!(!dir.exists(), "the dir itself is removed");
}

#[test]
fn prune_keeps_specified_snapshots() {
    for (name, keep, removed) in [
        ("one", vec!["pause-2"], vec!["pause-1", "pause-3"]),
        (
            "the latest and one still uploading",
            vec!["pause-3", "pause-1"],
            vec!["pause-2"],
        ),
    ] {
        let (_d, p) = node();
        let dir = p.local_checkpoints_dir(UID);
        for s in ["pause-1", "pause-2", "pause-3"] {
            write_snapshot_dir(&dir, s);
        }
        let keep: Vec<String> = keep.into_iter().map(String::from).collect();
        prune_local_checkpoints(&p, UID, &keep).unwrap();
        for r in removed {
            assert!(!dir.join(r).exists(), "{name}: {r} still exists");
        }
        for k in &keep {
            assert!(
                dir.join(k).join("memory.img").exists(),
                "{name}: {k} missing"
            );
        }
    }
}

#[test]
fn prune_missing_dir_is_noop() {
    let (_d, p) = node();
    prune_local_checkpoints(&p, "absent", &[]).unwrap();
}

/// An undeletable snapshot is reported without stranding the ones that
/// could be removed.
#[cfg(unix)]
#[test]
fn prune_reports_failure_and_still_removes_the_rest() {
    if unsafe { libc::geteuid() } == 0 {
        return; // root ignores directory modes
    }
    let (_d, p) = node();
    let dir = p.local_checkpoints_dir(UID);
    write_snapshot_dir(&dir, "pause-1");
    write_snapshot_dir(&dir, "pause-2");
    write_snapshot_dir(&dir, "pause-stuck");
    let stuck = dir.join("pause-stuck");
    crate::atomicfile::chmod(&stuck, 0o500).unwrap();
    let res = prune_local_checkpoints(&p, UID, &[]);
    crate::atomicfile::chmod(&stuck, 0o700).unwrap();
    let err = res.expect_err("the undeletable snapshot is reported");
    assert!(err.contains("pause-stuck"), "{err}");
    for s in ["pause-1", "pause-2"] {
        assert!(!dir.join(s).exists(), "{s} still exists");
    }
}

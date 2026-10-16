//! The data files of a staged snapshot, and releasing them: an eager restore
//! has read guest memory in full, so the staged image and the tars already
//! extracted can go, and their page cache with them.

use std::path::Path;

use hivekeep_sandbox_gvisor::snapshot_cache;

use crate::ch::MEM_RESTORE_EAGER;

/// The VMM's guest memory image in a snapshot.
pub const MEMORY_RANGES_FILE: &str = "memory-ranges";

/// Whether `name` is a snapshot data file: the tars, and the memory image
/// when `include_memory`.
pub fn is_snapshot_data_file(name: &str, include_memory: bool) -> bool {
    (include_memory && name == MEMORY_RANGES_FILE) || name.ends_with(".tar")
}

/// Drops the staged memory image and extracted tars from `restore_dir`.
pub fn drop_staged_memory_image(restore_dir: &Path) {
    let targets = snapshot_cache::find_files(restore_dir, |n| is_snapshot_data_file(n, true));
    if !targets.is_empty() && snapshot_cache::drop_files(&targets) {
        tracing::info!("Dropped the staged memory image (an eager restore needs no merge base)");
    }
}

/// [`drop_staged_memory_image`] after an eager restore of a snapshot that is
/// not preserved.
pub fn maybe_drop_staged_memory_image(restore_dir: &Path, mem_mode: &str, preserve: bool) {
    if mem_mode == MEM_RESTORE_EAGER && !preserve {
        drop_staged_memory_image(restore_dir);
    }
}

/// Pins the snapshot's data files now (while the caller holds the Actor's
/// lock) and evicts their clean page cache in the background.
pub fn drop_actor_snapshot_cache_async(restore_dir: &Path, include_memory: bool) {
    let targets =
        snapshot_cache::find_files(restore_dir, |n| is_snapshot_data_file(n, include_memory));
    if targets.is_empty() {
        return;
    }
    std::thread::spawn(move || snapshot_cache::evict_with_retries(&targets));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ch::MEM_RESTORE_ON_DEMAND;

    fn staged(files: &[&str]) -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        for f in files {
            std::fs::write(d.path().join(f), "staged data").unwrap();
        }
        d
    }

    #[test]
    fn drops_the_staged_image_and_tars() {
        let names = [
            MEMORY_RANGES_FILE,
            "rootfs-upper-app.tar",
            "durable-dir-data.tar",
        ];
        let d = staged(&names);
        std::fs::write(d.path().join("config.json"), "{}").unwrap();
        drop_staged_memory_image(d.path());
        for n in names {
            assert!(!d.path().join(n).exists(), "{n} survived");
        }
        assert!(d.path().join("config.json").exists());
    }

    #[test]
    fn drops_only_for_an_eager_unpreserved_restore() {
        for (name, mode, preserve, removed) in [
            (
                "eager staging dir removes memory-ranges",
                MEM_RESTORE_EAGER,
                false,
                true,
            ),
            (
                "eager preserved dir keeps memory-ranges",
                MEM_RESTORE_EAGER,
                true,
                false,
            ),
            (
                "on-demand staging dir keeps memory-ranges",
                MEM_RESTORE_ON_DEMAND,
                false,
                false,
            ),
        ] {
            let d = staged(&[MEMORY_RANGES_FILE]);
            maybe_drop_staged_memory_image(d.path(), mode, preserve);
            assert_eq!(
                !d.path().join(MEMORY_RANGES_FILE).exists(),
                removed,
                "{name}"
            );
        }
    }

    #[test]
    fn a_preserved_dir_keeps_its_contents() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(
            d.path().join(MEMORY_RANGES_FILE),
            "preserved snapshot memory",
        )
        .unwrap();
        maybe_drop_staged_memory_image(d.path(), MEM_RESTORE_EAGER, true);
        assert_eq!(
            std::fs::read_to_string(d.path().join(MEMORY_RANGES_FILE)).unwrap(),
            "preserved snapshot memory"
        );
    }

    #[test]
    fn picks_the_data_files() {
        let d = staged(&[MEMORY_RANGES_FILE, "rootfs-upper-app.tar", "config.json"]);
        assert_eq!(
            snapshot_cache::find_files(d.path(), |n| is_snapshot_data_file(n, true)).len(),
            2
        );
        let on_demand = snapshot_cache::find_files(d.path(), |n| is_snapshot_data_file(n, false));
        assert_eq!(on_demand.len(), 1);
        assert_eq!(
            on_demand[0].path.file_name().unwrap(),
            "rootfs-upper-app.tar"
        );
    }
}

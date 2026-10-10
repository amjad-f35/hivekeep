//! Size accounting and last-use recording, the facts eviction runs on.
//! Both live in the filesystem (a per-layer `size` file, the record's
//! mtime), so they survive restarts without in-memory state.

use std::fs;
use std::io;
use std::path::Path;

use crate::digest::Digest;
use crate::error::{Error, IoContext, Result};
use crate::spec::LAYER_FS_DIR_NAME;
use crate::store::{now, write_private, Store, LAYER_SIZE_FILE_NAME};

impl Store {
    /// Refreshes the record's mtime (the persisted last use). Best effort:
    /// a failed touch only costs LRU accuracy.
    pub(crate) fn touch_record(&self, digest: &Digest) {
        let t = now();
        if let Err(err) = filetime::set_file_times(self.record_path(digest), t, t) {
            if err.kind() != io::ErrorKind::NotFound {
                tracing::warn!(%digest, %err, "Failed to touch image record");
            }
        }
    }

    /// The layer's recorded size, backfilling the size file for layers
    /// unpacked before sizes were recorded. The backfill write bumps the
    /// directory's mtime and deliberately leaves it bumped: restoring it
    /// could rewind a concurrent reuse touch and defeat the freshness veto.
    pub(crate) fn layer_size(&self, layer_dir: &Path) -> Result<i64> {
        if let Some(n) = recorded_layer_size(layer_dir)? {
            return Ok(n);
        }
        let total = walk_layer_size(layer_dir);
        let _ = write_private(
            &layer_dir.join(LAYER_SIZE_FILE_NAME),
            format!("{total}\n").as_bytes(),
        );
        Ok(total)
    }

    /// [`Store::layer_size`] without the backfill, for dry runs: a dry run
    /// mutates nothing, derived metadata included.
    pub(crate) fn layer_size_read_only(&self, layer_dir: &Path) -> Result<i64> {
        if let Some(n) = recorded_layer_size(layer_dir)? {
            return Ok(n);
        }
        Ok(walk_layer_size(layer_dir))
    }

    /// The sum of recorded sizes of every layer in the pool (the accounting
    /// behind the cache's byte cap, distinct from volume usage).
    pub fn cache_size(&self) -> Result<i64> {
        let mut total = 0;
        for e in fs::read_dir(self.layers_dir()).ctx("while listing layer pool")? {
            let e = e.ctx("while listing layer pool")?;
            let name = e.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') || !e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            match self.layer_size(&e.path()) {
                Ok(n) => total += n,
                Err(err) => tracing::warn!(layer = %name, %err, "Failed to size layer"),
            }
        }
        Ok(total)
    }
}

/// The layer's size file; `None` when absent or corrupt (the caller walks
/// the tree instead).
pub(crate) fn recorded_layer_size(layer_dir: &Path) -> Result<Option<i64>> {
    match fs::read_to_string(layer_dir.join(LAYER_SIZE_FILE_NAME)) {
        Ok(s) => Ok(s.trim().parse::<i64>().ok()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::io("while reading layer size", e)),
    }
}

/// Sums regular-file sizes under the layer's tree. Images ship unreadable
/// directories and the cache lacks `CAP_DAC_READ_SEARCH`, so those are
/// skipped and under-counted rather than aborting.
pub(crate) fn walk_layer_size(layer_dir: &Path) -> i64 {
    walkdir::WalkDir::new(layer_dir.join(LAYER_FS_DIR_NAME))
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len() as i64)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::{backdate, fixture_store, must_ensure, one_file_image};
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    #[tokio::test]
    async fn layer_size_recorded_and_backfilled() {
        let reg = crate::testutil::registry::TestRegistry::start().await;
        let r = one_file_image(&reg, "test/sized:latest", "data", &"x".repeat(4096));
        let (_t, store) = fixture_store(Default::default());
        let img = must_ensure(&store, &r).await;
        let layer = &img.layer_dirs[0];
        let size_path = layer.join(LAYER_SIZE_FILE_NAME);
        assert!(size_path.exists(), "size file not written at unpack");
        assert!(store.layer_size(layer).unwrap() >= 4096);

        // Backfill: a layer from before size files.
        fs::remove_file(&size_path).unwrap();
        backdate(layer, Duration::from_secs(3600));
        let before = fs::metadata(layer).unwrap().modified().unwrap();
        let refilled = store.layer_size(layer).unwrap();
        assert!(refilled >= 4096, "{refilled}");
        assert!(size_path.exists(), "backfill did not rewrite the size file");
        let after = fs::metadata(layer).unwrap().modified().unwrap();
        assert!(after > before, "backfill must bump the layer dir mtime");

        // A corrupt size file heals the same way.
        fs::write(&size_path, "not-a-number\n").unwrap();
        assert!(store.layer_size(layer).unwrap() >= 4096);
        assert_ne!(
            fs::read_to_string(&size_path).unwrap().trim(),
            "not-a-number"
        );

        assert!(store.cache_size().unwrap() >= refilled);
    }

    #[tokio::test]
    async fn ensure_image_hit_touches_record() {
        let reg = crate::testutil::registry::TestRegistry::start().await;
        let r = one_file_image(&reg, "test/touch:latest", "f", "hi");
        let (_t, store) = fixture_store(Default::default());
        let img = must_ensure(&store, &r).await;
        let rec = store.record_path(&img.digest);
        backdate(&rec, Duration::from_secs(3600));
        let stale = fs::metadata(&rec).unwrap().modified().unwrap();
        must_ensure(&store, &r).await;
        assert!(fs::metadata(&rec).unwrap().modified().unwrap() > stale);
    }

    #[test]
    fn layer_size_backfill_skips_unreadable_dirs() {
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipped: root can list mode-0111 dirs");
            return;
        }
        let (_t, store) = fixture_store(Default::default());
        let dir = store.layers_dir().join("11".repeat(32));
        let fs_dir = dir.join(LAYER_FS_DIR_NAME);
        let readable = fs_dir.join("usr");
        let inner = fs_dir.join("opt/secret");
        fs::create_dir_all(&readable).unwrap();
        fs::create_dir_all(&inner).unwrap();
        fs::write(readable.join("blob"), vec![0u8; 4096]).unwrap();
        fs::write(inner.join("blob"), vec![0u8; 8192]).unwrap();
        fs::set_permissions(&inner, fs::Permissions::from_mode(0o111)).unwrap();
        let got = store.layer_size(&dir).unwrap();
        assert!(got >= 4096, "{got}");
        assert!(dir.join(LAYER_SIZE_FILE_NAME).exists());
        assert!(store.cache_size().unwrap() >= 4096);
        fs::set_permissions(&inner, fs::Permissions::from_mode(0o700)).unwrap();
    }
}

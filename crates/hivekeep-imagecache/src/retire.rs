//! Two-phase layer deletion. Eviction renames a layer dir aside (one
//! rename under the pin lock, the only step contending with pulls); the
//! slow removal of the renamed tree happens afterwards, outside every lock.
//! A crash in between leaves a `.rm-*` dir for the startup sweep. Nothing
//! here needs privileges: rename, chmod and unlink suffice even on
//! read-only trees.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};
use crate::store::Store;

/// Prefix of a layer dir renamed aside by eviction. It shares the
/// dot-hidden namespace with `.tmp-`, so diffID names never collide.
pub const RETIRED_PREFIX: &str = ".rm-";

/// Logged when a retirement is vetoed by freshness (fixed for greps).
pub const LOG_LAYER_RETIRE_VETOED: &str = "Image cache layer retirement vetoed: recently used";
/// Logged when a retirement is vetoed by a pull's pin.
pub const LOG_LAYER_RETIRE_PINNED: &str = "Image cache layer retirement vetoed: in use by a pull";

/// What a retirement did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireStatus {
    /// No dir under the layer's name and no pull pinning it.
    Gone,
    /// Kept: pinned, fresh, or the rename failed.
    Vetoed,
    /// Renamed aside; the caller removes it.
    Retired,
}

/// Whether `name` is a well-formed sha256 layer dir name.
pub fn is_layer_dir_name(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// `Retired` when `dir` exists and is older than `cutoff`, `Gone` when
/// missing, `Vetoed` otherwise; with the mtime when read.
pub(crate) fn retirable(
    dir: &Path,
    cutoff: SystemTime,
) -> (RetireStatus, Option<SystemTime>, Option<io::Error>) {
    match fs::metadata(dir) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => (RetireStatus::Gone, None, None),
        Err(e) => (RetireStatus::Vetoed, None, Some(e)),
        Ok(md) => {
            let m = md.modified().unwrap_or(UNIX_EPOCH);
            if m > cutoff {
                (RetireStatus::Vetoed, Some(m), None)
            } else {
                (RetireStatus::Retired, Some(m), None)
            }
        }
    }
}

impl Store {
    /// Evicts a layer by renaming it aside, returning the new path for the
    /// caller to delete. A layer pinned by a pull or touched after `cutoff`
    /// is vetoed. A fresh layer is ruled out before the pin lock (which
    /// every pull takes), and the logging happens outside it.
    pub(crate) fn retire_layer(
        &self,
        hex: &str,
        cutoff: SystemTime,
    ) -> Result<(Option<PathBuf>, RetireStatus)> {
        if !is_layer_dir_name(hex) {
            return Err(Error::msg(format!("not a layer dir name: {hex:?}")));
        }
        let dir = self.layers_dir().join(hex);
        let (st, mtime, err) = retirable(&dir, cutoff);
        if st == RetireStatus::Vetoed {
            return match err {
                Some(e) => Err(Error::io(format!("while checking layer {hex}"), e)),
                None => {
                    tracing::info!(diffid = hex, last_used = ?mtime, "{LOG_LAYER_RETIRE_VETOED}");
                    Ok((None, st))
                }
            };
        }
        let (dst, st, pins) = self.rename_aside_unless_pinned(&dir, hex, cutoff)?;
        if pins > 0 {
            tracing::info!(diffid = hex, pulls = pins, "{LOG_LAYER_RETIRE_PINNED}");
        }
        Ok((dst, st))
    }

    /// The critical section: the pin check, a re-stat and the rename run as
    /// one unit under the pin lock. Either a pin lands first and the
    /// retirement is vetoed, or the rename lands first and the pull finds
    /// the dir missing and unpacks it again.
    fn rename_aside_unless_pinned(
        &self,
        dir: &Path,
        hex: &str,
        cutoff: SystemTime,
    ) -> Result<(Option<PathBuf>, RetireStatus, usize)> {
        let pins = self.pins.lock();
        if let Some(&n) = pins.get(hex) {
            if n > 0 {
                return Ok((None, RetireStatus::Vetoed, n));
            }
        }
        let (st, _, err) = retirable(dir, cutoff);
        if let Some(e) = err {
            return Err(Error::io(format!("while checking layer {hex}"), e));
        }
        if st != RetireStatus::Retired {
            return Ok((None, st, 0));
        }
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dst = self
            .layers_dir()
            .join(format!("{RETIRED_PREFIX}{}-{nanos}", &hex[..12]));
        fs::rename(dir, &dst).map_err(|e| Error::io(format!("while retiring layer {hex}"), e))?;
        Ok((Some(dst), RetireStatus::Retired, 0))
    }

    /// What [`Store::retire_layer`] would do, without renaming. Sizing is
    /// read-only.
    pub(crate) fn dry_run_retire(&self, hex: &str, cutoff: SystemTime) -> (i64, RetireStatus) {
        if self.pinned(hex) {
            return (0, RetireStatus::Vetoed);
        }
        let dir = self.layers_dir().join(hex);
        let (st, _, _) = retirable(&dir, cutoff);
        if st != RetireStatus::Retired {
            return (0, st);
        }
        (
            self.layer_size_read_only(&dir).unwrap_or(0),
            RetireStatus::Retired,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::LAYER_FS_DIR_NAME;
    use crate::store::tests::{backdate, fixture_store, one_file_image};
    use crate::store::StoreOptions;
    use crate::testutil::image::{mem_layer, TarEntry};
    use crate::testutil::registry::TestRegistry;
    use hivekeep_node::context::Ctx;
    use std::sync::Arc;
    use std::time::Duration;

    fn ago(d: Duration) -> SystemTime {
        SystemTime::now() - d
    }

    #[test]
    fn retire_layer_statuses() {
        let (_t, store) = fixture_store(StoreOptions::default());
        let hex = "ab".repeat(32);
        assert!(store.retire_layer("../escape", SystemTime::now()).is_err());
        assert_eq!(
            store.retire_layer(&hex, SystemTime::now()).unwrap().1,
            RetireStatus::Gone
        );

        // Absent but pinned: still unpacking.
        let pin = store.pin_layers(vec![hex.clone()]);
        assert_eq!(
            store.retire_layer(&hex, SystemTime::now()).unwrap().1,
            RetireStatus::Vetoed
        );
        assert_eq!(
            store.dry_run_retire(&hex, SystemTime::now()).1,
            RetireStatus::Vetoed
        );
        drop(pin);

        let dir = store.layers_dir().join(&hex);
        fs::create_dir_all(dir.join(LAYER_FS_DIR_NAME)).unwrap();
        // Fresh dir, cutoff in the past.
        assert_eq!(
            store
                .retire_layer(&hex, ago(Duration::from_secs(60)))
                .unwrap()
                .1,
            RetireStatus::Vetoed
        );
        assert!(dir.exists());

        backdate(&dir, Duration::from_secs(7200));
        let (retired, st) = store
            .retire_layer(&hex, ago(Duration::from_secs(60)))
            .unwrap();
        assert_eq!(st, RetireStatus::Retired);
        assert!(!dir.exists());
        let retired = retired.unwrap();
        assert!(retired
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(RETIRED_PREFIX));
        assert!(retired.exists());
    }

    #[test]
    fn new_sweeps_retired_dirs() {
        let (t, store) = fixture_store(StoreOptions::default());
        let retired = store
            .layers_dir()
            .join(format!("{RETIRED_PREFIX}deadbeef-1"));
        fs::create_dir_all(retired.join("fs/ro")).unwrap();
        fs::write(retired.join("fs/ro/f"), "x").unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(retired.join("fs/ro"), fs::Permissions::from_mode(0o500)).unwrap();
        Store::open(t.path(), StoreOptions::default()).unwrap();
        assert!(!retired.exists(), "retired dir not swept at startup");
    }

    #[test]
    fn sweep_leaves_non_prefixed_entries() {
        let (t, store) = fixture_store(StoreOptions::default());
        let keep = [
            store.layers_dir().join("cd".repeat(32)),
            store.layers_dir().join("lost+found"),
        ];
        for d in &keep {
            fs::create_dir_all(d).unwrap();
        }
        let stray = store.layers_dir().join("README");
        fs::write(&stray, "x").unwrap();
        Store::open(t.path(), StoreOptions::default()).unwrap();
        for p in keep.iter().chain([&stray]) {
            assert!(p.exists(), "startup sweep removed {p:?}");
        }
    }

    // Retirement racing pulls of an image using the same layer: neither
    // side may ever error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn retire_layer_vs_ensure_image_race() {
        let reg = TestRegistry::start().await;
        let r = one_file_image(&reg, "test/retire-race:latest", "f", "hi");
        let (_t, store) = fixture_store(StoreOptions::default());
        let img = store.ensure_image(&Ctx::background(), &r).await.unwrap();
        let hex = img.layer_dirs[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();

        let (s1, r1) = (store.clone(), r.clone());
        let puller = tokio::spawn(async move {
            for _ in 0..25 {
                let img = s1.ensure_image(&Ctx::background(), &r1).await?;
                // Best effort: the layer may already be retired.
                let past = filetime::FileTime::from_system_time(ago(Duration::from_secs(7200)));
                let _ = filetime::set_file_times(&img.layer_dirs[0], past, past);
            }
            Ok::<_, Error>(())
        });
        let (s2, h2) = (store.clone(), hex.clone());
        let retirer = tokio::task::spawn_blocking(move || {
            for _ in 0..200 {
                s2.retire_layer(&h2, ago(Duration::from_secs(60)))?;
            }
            Ok::<_, Error>(())
        });
        puller.await.unwrap().unwrap();
        retirer.await.unwrap().unwrap();
        let img = store.ensure_image(&Ctx::background(), &r).await.unwrap();
        assert!(img.layer_dirs[0].join(LAYER_FS_DIR_NAME).exists());
    }

    #[test]
    fn retire_layer_vetoed_while_pinned() {
        let (_t, store) = fixture_store(StoreOptions::default());
        let hex = "ab".repeat(32);
        let dir = store.layers_dir().join(&hex);
        fs::create_dir_all(dir.join(LAYER_FS_DIR_NAME)).unwrap();
        backdate(&dir, Duration::from_secs(3 * 3600));
        let (a, b) = (
            store.pin_layers(vec![hex.clone()]),
            store.pin_layers(vec![hex.clone()]),
        );
        drop(a);
        assert_eq!(
            store.retire_layer(&hex, SystemTime::now()).unwrap().1,
            RetireStatus::Vetoed
        );
        assert!(dir.join(LAYER_FS_DIR_NAME).exists());
        drop(b);
        assert!(!store.pinned(&hex));
        assert_eq!(
            store.retire_layer(&hex, SystemTime::now()).unwrap().1,
            RetireStatus::Retired
        );
    }

    // A fresh layer is ruled out before the pin lock is taken.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retire_layer_preflight_skips_pin_lock() {
        let (_t, store) = fixture_store(StoreOptions::default());
        let fresh = mem_layer(&[TarEntry::file("f", "fresh")]);
        let diff_id = fresh.diff_id.clone();
        store
            .ensure_layer(&Ctx::background(), &diff_id, Arc::new(fresh))
            .await
            .unwrap();
        let cutoff = ago(Duration::from_secs(3600));
        let guard = store.pins.lock();
        let s = store.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(s.retire_layer(&diff_id.hex, cutoff).map(|r| r.1));
        });
        let got = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("retire_layer took the pin lock for a fresh layer");
        drop(guard);
        assert_eq!(got.unwrap(), RetireStatus::Vetoed);
    }
}

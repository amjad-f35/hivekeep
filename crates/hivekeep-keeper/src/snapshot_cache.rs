//! Restores' reads of snapshot files, through the node's snapshot cache
//! when the control plane declared the snapshot shared.
//!
//! A shared snapshot (a tag's, which every actor created from it restores)
//! is immutable once published, so its files are a node-level resource:
//! concurrent restores share one download, and later restores read local
//! disk. Whether a snapshot is shared is the control plane's call; the
//! keeper only maps it to a serving mode per sandbox class.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use hivekeep_proto::keeper::v1 as pb;
use tonic::Status;

use crate::assets::{redact_url, MANIFEST_NAME};
use crate::context::Ctx;
use crate::filecache::{self, CacheError, Key};
use crate::snapshot_transfer::SnapshotTransfer;
use crate::worker_jwt::TransferActor;

/// How a restore may materialize snapshot files from the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheMode {
    /// Download afresh, bypassing the cache.
    Off,
    /// Hard-link the cached copy: free hits, but the consumer must treat
    /// the file as read-only.
    Link,
    /// A private, hole-preserving copy the consumer may change, drop from
    /// the page cache, or keep open without pinning the cache entry.
    Copy,
}

/// How a sandbox class's restores may use the cache. The gVisor worker
/// reads restore state and drops it once paged in, so links are safe and
/// free. The micro-VM worker keeps its memory file open for the VM's life
/// and merges deltas into it, so a link would pin the entry and turn every
/// suspend into a full copy; it gets copies.
pub fn cache_mode_for(sandbox_class: &str) -> CacheMode {
    match sandbox_class {
        hivekeep_core::sandbox_class::GVISOR => CacheMode::Link,
        hivekeep_core::sandbox_class::MICROVM => CacheMode::Copy,
        _ => CacheMode::Off,
    }
}

/// `class_mode` when the control plane declared the snapshot shared, else
/// Off. Cacheability is never inferred from the URI.
pub fn snapshot_cache_mode(object: Option<&pb::ObjectStorage>, class_mode: CacheMode) -> CacheMode {
    match object {
        Some(o) if o.sharing == pb::SnapshotSharing::Shared as i32 => class_mode,
        _ => CacheMode::Off,
    }
}

/// Opens the snapshot cache at `dir`, creating it, and sweeps the debris a
/// crash may have left. A sweep that fails is logged, not fatal: debris
/// wastes space but no lookup sees it.
pub async fn open_snapshot_cache(
    dir: &Path,
    opts: filecache::Options,
) -> Result<filecache::Store, String> {
    let store = filecache::Store::open(dir, opts)
        .map_err(|e| format!("while opening snapshot cache at {}: {e}", dir.display()))?;
    let stats = match store.sweep_debris().await {
        Ok(s) => s,
        Err((s, err)) => {
            tracing::warn!(%err, "Snapshot cache debris sweep incomplete");
            s
        }
    };
    tracing::info!(
        dir = %dir.display(),
        min_age = ?store.min_age(),
        fetch_timeout = ?store.fetch_timeout(),
        tmp_removed = stats.tmp_removed,
        retired_removed = stats.retired_removed,
        "Snapshot cache open"
    );
    Ok(store)
}

/// Stages snapshot files for restores.
pub struct SnapshotDownloader {
    transfer: Arc<SnapshotTransfer>,
    cache: Option<filecache::Store>,
    cross_mount_warned: AtomicBool,
}

impl SnapshotDownloader {
    /// Downloads through `transfer`, via `cache` when one is given.
    pub fn new(transfer: Arc<SnapshotTransfer>, cache: Option<filecache::Store>) -> Self {
        SnapshotDownloader {
            transfer,
            cache,
            cross_mount_warned: AtomicBool::new(false),
        }
    }

    /// The transfer underneath.
    pub fn transfer(&self) -> &Arc<SnapshotTransfer> {
        &self.transfer
    }

    /// Stages `files` of one snapshot into `dst_dir`: from the cache when
    /// `mode` allows and a cache is configured, else downloaded directly.
    pub async fn download(
        &self,
        ctx: &Ctx,
        actor: &TransferActor,
        uri: &str,
        dst_dir: &Path,
        files: &[String],
        mode: CacheMode,
    ) -> Result<(), Status> {
        let res = match (&self.cache, mode) {
            (Some(cache), CacheMode::Link | CacheMode::Copy) => {
                self.download_cached(ctx, cache, actor, uri, dst_dir, files, mode)
                    .await
            }
            _ => {
                self.transfer
                    .fetch_snapshot_files(ctx, actor, uri, dst_dir, files)
                    .await
            }
        };
        res.map_err(|e| crate::status::wrap("while downloading snapshot files", e))
    }

    /// The snapshot's manifest, through the cache when `mode` allows: a
    /// shared snapshot's manifest is as immutable as its files. It is read
    /// from a scratch link that is dropped again.
    pub async fn fetch_manifest(
        &self,
        ctx: &Ctx,
        actor: &TransferActor,
        uri: &str,
        mode: CacheMode,
    ) -> Result<Vec<u8>, Status> {
        let Some(cache) = &self.cache else {
            return self.transfer.fetch_manifest(ctx, actor, uri).await;
        };
        if mode == CacheMode::Off {
            return self.transfer.fetch_manifest(ctx, actor, uri).await;
        }
        let dir = self.transfer.snapshot_scratch()?;
        let local = dir.join(MANIFEST_NAME);
        let res = async {
            self.fetch_file(
                ctx,
                cache,
                actor,
                uri,
                MANIFEST_NAME,
                &local,
                CacheMode::Link,
            )
            .await?;
            std::fs::read(&local)
                .map_err(|e| Status::internal(format!("while reading cached manifest: {e}")))
        }
        .await;
        let _ = crate::actordirs::remove_all(&dir);
        res
    }

    #[allow(clippy::too_many_arguments)]
    async fn download_cached(
        &self,
        ctx: &Ctx,
        cache: &filecache::Store,
        actor: &TransferActor,
        uri: &str,
        dst_dir: &Path,
        files: &[String],
        mode: CacheMode,
    ) -> Result<(), Status> {
        hivekeep_core::snapshot::validate_snapshot_file_names(files)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        // Subdirectories once each before the parallel fetches.
        let mut made = std::collections::HashSet::new();
        for f in files {
            if let Some((dir, _)) = f.rsplit_once('/') {
                if made.insert(dir.to_string()) {
                    mkdir_no_follow(&dst_dir.join(dir)).map_err(|e| {
                        Status::internal(format!("while creating {dir} in restore directory: {e}"))
                    })?;
                }
            }
        }
        let futs = files.iter().map(|f| {
            let dst: PathBuf = dst_dir.join(f);
            async move {
                self.fetch_file(ctx, cache, actor, uri, f, &dst, mode)
                    .await
                    .map_err(|e| crate::status::wrap(format!("while downloading {f}"), e))
            }
        });
        futures::future::try_join_all(futs).await.map(|_| ())
    }

    /// Stages one file at `local` through the cache. A Link hit is a hard
    /// link, so `local` stays valid whatever the cache evicts later; a Copy
    /// hit is a private copy. A cache on another mount than the restore dir
    /// serves copies.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_file(
        &self,
        ctx: &Ctx,
        cache: &filecache::Store,
        actor: &TransferActor,
        uri: &str,
        file: &str,
        local: &Path,
        mode: CacheMode,
    ) -> Result<(), Status> {
        // Keyed by address without the query: a signed URL's query is a
        // credential, and keys are logged.
        let u = url::Url::parse(uri).map_err(|e| {
            Status::invalid_argument(format!("while parsing the snapshot URI: {e}"))
        })?;
        let base = redact_url(&u);
        let object = format!("{}/{}", base.trim_end_matches('/'), file);
        let key = Key::uri(&[object]);
        let fetcher = || {
            let transfer = self.transfer.clone();
            let actor = actor.clone();
            let uri = uri.to_string();
            let file = file.to_string();
            filecache::fetcher(move |ctx: Ctx, dst: PathBuf| async move {
                fetch_file_to(&transfer, &ctx, &actor, &uri, &file, &dst).await
            })
        };
        if mode == CacheMode::Copy {
            return cache
                .get_file_copy_to(ctx, &key, local, fetcher())
                .await
                .map_err(Status::from);
        }
        match cache.get_file_to(ctx, &key, local, fetcher()).await {
            Ok(()) => Ok(()),
            Err(CacheError::CrossDevice(_)) => {
                if !self.cross_mount_warned.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        cache = %cache.root().display(),
                        "Snapshot cache is on a different filesystem than the restore dirs; serving every hit as a copy instead of a hard link"
                    );
                }
                tracing::debug!(snapshot_uri = %base, file, "Snapshot cache hit served as a copy across mounts");
                cache
                    .get_file_copy_to(ctx, &key, local, fetcher())
                    .await
                    .map_err(Status::from)
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// Downloads one snapshot file to `dst`: the store writes a directory of
/// files, so it fetches into a scratch dir beside `dst` and moves the file.
async fn fetch_file_to(
    transfer: &SnapshotTransfer,
    ctx: &Ctx,
    actor: &TransferActor,
    uri: &str,
    file: &str,
    dst: &Path,
) -> Result<(), Status> {
    let parent = dst.parent().unwrap_or_else(|| Path::new("/"));
    let scratch = crate::atomicfile::tempdir_in(parent, ".fetch-")
        .map_err(|e| Status::internal(format!("while creating a snapshot fetch dir: {e}")))?;
    let res = async {
        transfer
            .fetch_snapshot_files(ctx, actor, uri, &scratch, &[file.to_string()])
            .await?;
        std::fs::rename(scratch.join(file), dst)
            .map_err(|e| Status::internal(format!("while placing {file}: {e}")))
    }
    .await;
    let _ = crate::actordirs::remove_all(&scratch);
    res
}

/// Creates `dir` unless it already is a directory; anything else in its
/// place, a symlink above all, is an error.
fn mkdir_no_follow(dir: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            crate::atomicfile::create_dir_mode(dir, 0o700)
        }
        Err(e) => Err(e),
        Ok(m) if m.is_dir() => Ok(()),
        Ok(_) => Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("{} exists and is not a directory", dir.display()),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objectstore::{LocalObjectStore, ObjectStore, UploadSnapshot};
    use crate::paths::NodePaths;
    use crate::worker_jwt::tests::FakeWorkers;
    use crate::worker_jwt::WorkerActorJwts;

    const URI: &str = "gs://bucket/hives/h/tags/t1";

    #[test]
    fn modes() {
        assert_eq!(cache_mode_for("gvisor"), CacheMode::Link);
        assert_eq!(cache_mode_for("microvm"), CacheMode::Copy);
        assert_eq!(cache_mode_for("other"), CacheMode::Off);
        let shared = pb::ObjectStorage {
            snapshot_uri: URI.into(),
            sharing: pb::SnapshotSharing::Shared as i32,
        };
        let private = pb::ObjectStorage {
            snapshot_uri: URI.into(),
            sharing: pb::SnapshotSharing::Private as i32,
        };
        assert_eq!(
            snapshot_cache_mode(Some(&shared), CacheMode::Link),
            CacheMode::Link
        );
        assert_eq!(
            snapshot_cache_mode(Some(&private), CacheMode::Link),
            CacheMode::Off
        );
        assert_eq!(snapshot_cache_mode(None, CacheMode::Copy), CacheMode::Off);
    }

    #[tokio::test]
    async fn cached_downloads_share_one_fetch_and_link_out() {
        let d = tempfile::tempdir().unwrap();
        let paths = Arc::new(NodePaths::new(d.path().join("node")));
        let objects = Arc::new(LocalObjectStore::new(d.path().join("objects")));
        let jwts = Arc::new(WorkerActorJwts::new(
            Arc::new(FakeWorkers::default()),
            Arc::new(crate::clock::SystemClock),
        ));
        let transfer = Arc::new(SnapshotTransfer::new(
            objects.clone(),
            jwts,
            paths.clone(),
            "b".into(),
        ));
        let src = d.path().join("src");
        std::fs::create_dir_all(src.join("fs")).unwrap();
        std::fs::write(src.join("mem"), b"m").unwrap();
        std::fs::write(src.join("fs/img"), b"i").unwrap();
        objects
            .upload_snapshot(
                &Ctx::background(),
                UploadSnapshot {
                    snapshot_uri: URI.into(),
                    local_path: src,
                    files: vec!["mem".into(), "fs/img".into()],
                    actor_jwt: String::new(),
                },
            )
            .await
            .unwrap();
        let cache =
            filecache::Store::open(paths.snapshot_cache_dir(), filecache::Options::default())
                .unwrap();
        let dl = SnapshotDownloader::new(transfer, Some(cache.clone()));
        let actor = TransferActor {
            hive: "h".into(),
            name: "a".into(),
            uid: "u".into(),
        };
        let files = vec!["mem".to_string(), "fs/img".to_string()];
        for i in 0..2 {
            let dst = paths.base().join(format!("restore{i}"));
            std::fs::create_dir_all(&dst).unwrap();
            dl.download(
                &Ctx::background(),
                &actor,
                URI,
                &dst,
                &files,
                CacheMode::Link,
            )
            .await
            .unwrap();
            assert_eq!(std::fs::read(dst.join("fs/img")).unwrap(), b"i");
        }
        assert!(cache.total_bytes().await.unwrap() > 0);
    }
}

#[cfg(test)]
mod behavior_tests;

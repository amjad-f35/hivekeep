//! Keeps each local worker's default runtime staged under the worker's
//! directory, so its assets stay cached while no actor runs them and the
//! first Run after a config change finds them on disk.
//!
//! Best-effort: a failure is retried a few times with backoff, then left
//! to the on-demand fetch at Run.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tonic::Status;

use crate::assets::SandboxAssetsRecord;
use crate::context::Ctx;
use crate::runtime_index::RuntimeIndex;
use crate::workqueue::{RateLimiter, WorkQueue};

/// Bounds one prewarm attempt.
pub const DEFAULT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10 * 60);
/// Retries before a prewarm is given up.
const MAX_RETRIES: u32 = 8;

/// Stages a runtime's assets.
#[async_trait]
pub trait AssetFetcher: Send + Sync + 'static {
    /// Stages into `dst_dir`, creating it.
    async fn ensure(
        &self,
        ctx: &Ctx,
        rec: &SandboxAssetsRecord,
        dst_dir: &Path,
    ) -> Result<BTreeMap<String, String>, Status>;
    /// Stages into the existing `dst_dir`.
    async fn stage(
        &self,
        ctx: &Ctx,
        rec: &SandboxAssetsRecord,
        dst_dir: &Path,
    ) -> Result<BTreeMap<String, String>, Status>;
}

#[async_trait]
impl AssetFetcher for crate::sandbox_assets::AssetStager {
    async fn ensure(
        &self,
        ctx: &Ctx,
        rec: &SandboxAssetsRecord,
        dst_dir: &Path,
    ) -> Result<BTreeMap<String, String>, Status> {
        crate::sandbox_assets::AssetStager::ensure(self, ctx, rec, dst_dir).await
    }
    async fn stage(
        &self,
        ctx: &Ctx,
        rec: &SandboxAssetsRecord,
        dst_dir: &Path,
    ) -> Result<BTreeMap<String, String>, Status> {
        crate::sandbox_assets::AssetStager::stage(self, ctx, rec, dst_dir).await
    }
}

/// Queues runtimes to stage into worker directories.
pub trait Prewarm: Send + Sync + 'static {
    /// Stages the runtime `hash` into `dir` in the background.
    fn enqueue(&self, hash: &str, dir: &Path);
}

/// The prewarm queue and its worker.
pub struct RuntimePrewarmer {
    index: Arc<RuntimeIndex>,
    assets: Arc<dyn AssetFetcher>,
    queue: WorkQueue,
    retries: RateLimiter,
    /// Directories by queue key, which is `hash` + NUL + `dir`.
    items: Mutex<std::collections::HashMap<String, (String, PathBuf)>>,
    attempt_timeout: Duration,
}

impl RuntimePrewarmer {
    /// A prewarmer resolving runtimes in `index`.
    pub fn new(index: Arc<RuntimeIndex>, assets: Arc<dyn AssetFetcher>) -> Self {
        RuntimePrewarmer {
            index,
            assets,
            queue: WorkQueue::new(),
            retries: RateLimiter::new(Duration::from_secs(1), Duration::from_secs(5 * 60)),
            items: Mutex::new(Default::default()),
            attempt_timeout: DEFAULT_ATTEMPT_TIMEOUT,
        }
    }

    /// Runs the queue until `ctx` ends.
    pub async fn run(self: Arc<Self>, ctx: Ctx) {
        let q = self.queue.clone();
        let stop = ctx.clone();
        tokio::spawn(async move {
            stop.done().await;
            q.shut_down();
        });
        while let Some(key) = self.queue.get().await {
            self.process(&ctx, &key).await;
            self.queue.done(&key);
        }
    }

    async fn process(&self, ctx: &Ctx, key: &str) {
        let Some((hash, dir)) = self.items.lock().get(key).cloned() else {
            return;
        };
        let Some(rt) = self.index.lookup(&hash) else {
            tracing::error!(runtime_hash = %hash, "Sandbox runtime to prewarm is not in the index");
            self.retries.forget(key);
            self.items.lock().remove(key);
            return;
        };
        let res = self.prewarm(ctx, &hash, &dir, &rt.rec).await;
        match res {
            Ok(()) => {
                self.retries.forget(key);
                self.items.lock().remove(key);
            }
            Err(_) if ctx.is_done() => {}
            Err(err) => {
                let n = self.retries.num_requeues(key);
                if n < MAX_RETRIES {
                    tracing::warn!(runtime_hash = %hash, config = %rt.config, version = %rt.version, retries = n, %err, "Sandbox runtime prewarm failed; will retry");
                    let d = self.retries.when(key);
                    self.queue.add_after(key, d);
                } else {
                    tracing::warn!(runtime_hash = %hash, config = %rt.config, version = %rt.version, %err, "Sandbox runtime prewarm failed; giving up");
                    self.retries.forget(key);
                    self.items.lock().remove(key);
                }
            }
        }
    }

    async fn prewarm(
        &self,
        ctx: &Ctx,
        hash: &str,
        dir: &Path,
        rec: &SandboxAssetsRecord,
    ) -> Result<(), Status> {
        let ctx = ctx.with_timeout(self.attempt_timeout);
        // The syncer removes a directory the worker no longer advertises,
        // which supersedes the prewarm.
        if dir_gone(dir)? {
            tracing::debug!(runtime_hash = hash, dir = %dir.display(), "Sandbox runtime prewarm superseded; its directory is gone");
            return Ok(());
        }
        let started = std::time::Instant::now();
        if let Err(e) = self.assets.stage(&ctx, rec, dir).await {
            if dir_gone(dir).unwrap_or(false) {
                return Ok(());
            }
            return Err(e);
        }
        tracing::info!(runtime_hash = hash, assets = rec.assets.len(), dir = %dir.display(), duration_ms = started.elapsed().as_millis() as u64, "Sandbox runtime prewarmed");
        Ok(())
    }
}

impl Prewarm for RuntimePrewarmer {
    fn enqueue(&self, hash: &str, dir: &Path) {
        let key = format!("{hash}\0{}", dir.display());
        self.items
            .lock()
            .insert(key.clone(), (hash.to_string(), dir.to_path_buf()));
        self.queue.add(&key);
    }
}

fn dir_gone(dir: &Path) -> Result<bool, Status> {
    match std::fs::metadata(dir) {
        Ok(_) => Ok(false),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(Status::internal(format!(
            "while checking the prewarm directory: {e}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets::AssetEntry;
    use crate::runtime_index::{AdvertisedClass, HashedVersion};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Fake {
        calls: AtomicUsize,
        fail_first: AtomicUsize,
    }

    #[async_trait]
    impl AssetFetcher for Fake {
        async fn ensure(
            &self,
            ctx: &Ctx,
            rec: &SandboxAssetsRecord,
            dst: &Path,
        ) -> Result<BTreeMap<String, String>, Status> {
            self.stage(ctx, rec, dst).await
        }
        async fn stage(
            &self,
            _: &Ctx,
            _: &SandboxAssetsRecord,
            dst: &Path,
        ) -> Result<BTreeMap<String, String>, Status> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_first.load(Ordering::SeqCst) > 0 {
                self.fail_first.fetch_sub(1, Ordering::SeqCst);
                return Err(Status::unavailable("flaky"));
            }
            std::fs::write(dst.join("kernel"), b"k").unwrap();
            Ok(BTreeMap::new())
        }
    }

    fn indexed() -> (Arc<RuntimeIndex>, String) {
        let x = Arc::new(RuntimeIndex::in_memory());
        let mut rec = SandboxAssetsRecord::default();
        rec.assets.insert("kernel".into(), AssetEntry::default());
        let hash = rec.bundle_hash();
        x.register(
            "pod",
            &[AdvertisedClass {
                class: "microvm".into(),
                config: "c".into(),
                hashed: vec![HashedVersion {
                    name: "v".into(),
                    is_default: true,
                    rec,
                    hash: hash.clone(),
                }],
            }],
        )
        .unwrap();
        (x, hash)
    }

    #[tokio::test(start_paused = true)]
    async fn stages_and_retries() {
        let d = tempfile::tempdir().unwrap();
        let (x, hash) = indexed();
        let fake = Arc::new(Fake {
            calls: AtomicUsize::new(0),
            fail_first: AtomicUsize::new(1),
        });
        let p = Arc::new(RuntimePrewarmer::new(x, fake.clone()));
        let ctx = Ctx::background();
        tokio::spawn(p.clone().run(ctx.clone()));
        p.enqueue(&hash, d.path());
        for _ in 0..200 {
            if d.path().join("kernel").exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(d.path().join("kernel").exists());
        assert_eq!(fake.calls.load(Ordering::SeqCst), 2);
        ctx.cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn a_gone_directory_supersedes_the_prewarm() {
        let d = tempfile::tempdir().unwrap();
        let (x, hash) = indexed();
        let fake = Arc::new(Fake {
            calls: AtomicUsize::new(0),
            fail_first: AtomicUsize::new(0),
        });
        let p = Arc::new(RuntimePrewarmer::new(x, fake.clone()));
        let ctx = Ctx::background();
        tokio::spawn(p.clone().run(ctx.clone()));
        p.enqueue(&hash, &d.path().join("gone"));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
        ctx.cancel();
    }
}

#[cfg(test)]
mod behavior_tests;

//! Keeps a sandbox runtimes file in each local worker's directory: the
//! runtimes of the enabled versions of its pool's sandbox config, rewritten
//! when the config changes, and removed with the pod.
//!
//! The worker registers the runtimes it reads from that file, so the keeper
//! indexes every runtime before writing it: a hash a worker registers
//! always resolves here.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::context::Ctx;
use crate::paths::NodePaths;
use crate::prewarm::{AssetFetcher, Prewarm};
use crate::runtime_index::{
    AdvertisedClass, HashedVersion, RuntimeIndex, ATTR_SANDBOX_BUNDLE_HASH,
};
use crate::sandbox_versions::{
    hash_versions, node_arch, plan_config, write_runtimes_file, HostProbe, Runtime, RuntimesClass,
    RuntimesFile, SandboxPlan, SyncError, ATTR_ARCHITECTURE, ATTR_CPU_FEATURES, ATTR_CPU_MODEL,
    ATTR_CPU_VENDOR, PROBE_FETCH_TIMEOUT,
};
use crate::workqueue::{RateLimiter, WorkQueue};

pub use hivekeep_node::cluster_view::{
    ClusterChangeNotifier, ClusterChanges, ClusterView, StaticClusterView, WorkerPod,
    WORKER_POOL_LABEL,
};

/// Each worker's staged default runtimes live under this subdirectory.
pub const DEFAULT_RUNTIME_DIRS_NAME: &str = "default-runtime";
/// A worker directory younger than this is not treated as an orphan: its
/// pod may not be visible yet.
const ORPHAN_MIN_AGE: Duration = Duration::from_secs(60);
/// Syncs at once.
const SYNC_WORKERS: usize = 2;
/// Retries of a failing sync.
const SYNC_MAX_RETRIES: u32 = 8;
/// Upper bound of the random delay before syncs caused by a WorkerPool or
/// SandboxConfig change. Every keeper sees such a change at once and each
/// rewritten file makes its worker register again, so the rewrites are
/// spread out; repeated changes inside the window collapse into one queue
/// entry. The changed config's runtimes are indexed at once, so an Actor
/// placed on the new default inside the window still resolves.
pub const SYNC_MAX_JITTER: Duration = Duration::from_secs(30);

/// The syncer.
pub struct WorkerSandboxSyncer {
    cluster: Arc<dyn ClusterView>,
    paths: Arc<NodePaths>,
    index: Arc<RuntimeIndex>,
    prewarm: Option<Arc<dyn Prewarm>>,
    assets: Arc<dyn AssetFetcher>,
    probe: Arc<dyn HostProbe>,
    queue: WorkQueue,
    retries: RateLimiter,
    min_age: Duration,
    max_jitter: Duration,
    host_cpu: Mutex<Option<(String, String)>>,
    cpu_features: Mutex<HashMap<String, String>>,
    microvm_assets: Mutex<HashSet<String>>,
}

impl WorkerSandboxSyncer {
    /// A syncer writing under `paths`' workers dir.
    pub fn new(
        cluster: Arc<dyn ClusterView>,
        paths: Arc<NodePaths>,
        index: Arc<RuntimeIndex>,
        prewarm: Option<Arc<dyn Prewarm>>,
        assets: Arc<dyn AssetFetcher>,
        probe: Arc<dyn HostProbe>,
    ) -> Self {
        WorkerSandboxSyncer {
            cluster,
            paths,
            index,
            prewarm,
            assets,
            probe,
            queue: WorkQueue::new(),
            retries: RateLimiter::new(Duration::from_secs(1), Duration::from_secs(5 * 60)),
            min_age: ORPHAN_MIN_AGE,
            max_jitter: SYNC_MAX_JITTER,
            host_cpu: Mutex::new(None),
            cpu_features: Mutex::new(HashMap::new()),
            microvm_assets: Mutex::new(HashSet::new()),
        }
    }

    /// Sets the bound of the random delay before fleet-wide syncs.
    pub fn with_max_jitter(mut self, max: Duration) -> Self {
        self.max_jitter = max;
        self
    }

    /// Resyncs every pod and worker directory each `period` and whenever
    /// the cluster view reports a change, and runs the sync workers, until
    /// `ctx` ends.
    pub async fn run(self: Arc<Self>, ctx: Ctx, period: Duration) {
        for _ in 0..SYNC_WORKERS {
            let this = self.clone();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                while let Some(uid) = this.queue.get().await {
                    this.process(&ctx, &uid).await;
                    this.queue.done(&uid);
                }
            });
        }
        let mut changes = self.cluster.changes();
        let mut seen = changes
            .as_mut()
            .map(|rx| *rx.borrow_and_update())
            .unwrap_or_default();
        self.enqueue_all().await;
        let mut next_resync = tokio::time::Instant::now() + period;
        loop {
            let changed = async {
                match changes.as_mut() {
                    Some(rx) => rx.changed().await.is_ok(),
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                _ = ctx.done() => {
                    self.queue.shut_down();
                    return;
                }
                _ = tokio::time::sleep_until(next_resync) => {
                    self.enqueue_all().await;
                    next_resync = tokio::time::Instant::now() + period;
                }
                open = changed => {
                    let Some(rx) = changes.as_mut().filter(|_| open) else {
                        // The view stopped reporting; the period still runs.
                        changes = None;
                        continue;
                    };
                    let now = *rx.borrow_and_update();
                    self.on_cluster_change(seen, now).await;
                    seen = now;
                }
            }
        }
    }

    /// Reacts to the view's changes since `seen`: a fleet-wide change
    /// indexes the new runtimes and queues every pod after a random delay;
    /// a local one queues every pod and directory at once.
    pub async fn on_cluster_change(&self, seen: ClusterChanges, now: ClusterChanges) {
        if now.fleet != seen.fleet {
            self.enqueue_fleet_change().await;
        }
        if now.local != seen.local {
            self.enqueue_all().await;
        }
    }

    /// Queues every worker pod after a random delay below the max jitter,
    /// registering each pool's planned runtimes in the index first.
    async fn enqueue_fleet_change(&self) {
        let pods = match self.cluster.worker_pods().await {
            Ok(p) => p,
            Err(err) => {
                tracing::warn!(%err, "Worker pod list failed; the sandbox config change is picked up by the next resync");
                return;
            }
        };
        let mut planned = HashSet::new();
        for pod in &pods {
            if !self.max_jitter.is_zero() {
                if let Some(pool) = &pod.pool {
                    if planned.insert((pod.namespace.clone(), pool.clone())) {
                        self.register_ahead(pod).await;
                    }
                }
            }
            match jitter(self.max_jitter) {
                d if d.is_zero() => self.queue.add(&pod.uid),
                d => self.queue.add_after(&pod.uid, d),
            }
        }
    }

    /// Makes the runtimes of the pod's current plan resolvable before its
    /// delayed sync writes the file. Only the hashes are registered; what
    /// the worker advertises changes with its sync. A pod that does not plan
    /// is left to its sync, which reports and retries it.
    async fn register_ahead(&self, pod: &WorkerPod) {
        let Ok(plan) = self.plan(pod).await else {
            return;
        };
        if let Err(err) = self
            .index
            .register_runtimes(&plan.config, &hash_versions(&plan))
        {
            tracing::warn!(pool = ?pod.pool, %err, "Could not index a changed pool's runtimes ahead of its sync");
        }
    }

    /// Queues every worker pod and every worker directory.
    pub async fn enqueue_all(&self) {
        match self.cluster.worker_pods().await {
            Ok(pods) => {
                for p in pods {
                    self.queue.add(&p.uid);
                }
            }
            Err(err) => {
                tracing::warn!(%err, "Worker pod list failed; workers on this node cannot update their sandbox runtimes until it recovers")
            }
        }
        let Ok(entries) = std::fs::read_dir(self.paths.workers_dir()) else {
            return;
        };
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if !e.file_type().is_ok_and(|t| t.is_dir())
                || !hivekeep_core::names::check_uuid(&name).is_empty()
            {
                continue;
            }
            let age = e
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.elapsed().ok())
                .unwrap_or_default();
            if age < self.min_age {
                self.queue.add_after(&name, self.min_age - age);
            } else {
                self.queue.add(&name);
            }
        }
    }

    async fn process(&self, ctx: &Ctx, uid: &str) {
        let pod = match self.cluster.worker_pods().await {
            Ok(pods) => pods.into_iter().find(|p| p.uid == uid),
            Err(err) => {
                tracing::warn!(pod_uid = uid, %err, "Could not list worker pods");
                self.queue.add_after(uid, self.retries.when(uid));
                return;
            }
        };
        let res = match &pod {
            None => self.remove_dir(uid),
            Some(p) if p.deleting => Ok(()),
            Some(p) => self.sync_pod(ctx, p).await,
        };
        self.finish(ctx, uid, res);
    }

    /// Settles one sync: forgets `uid` on success or a permanent failure,
    /// gives up on a pool whose config stays unresolved after
    /// [`SYNC_MAX_RETRIES`], and otherwise retries with backoff for as long
    /// as it fails. A sync cut short by shutdown is not a failure. A sync
    /// that fails before it writes leaves the existing file in place.
    fn finish(&self, ctx: &Ctx, uid: &str, res: Result<(), SyncError>) {
        let retries = self.retries.num_requeues(uid);
        match res {
            Ok(()) => self.retries.forget(uid),
            Err(err @ SyncError::Permanent(_)) => {
                tracing::error!(pod_uid = uid, %err, "Worker sandbox runtimes cannot be synced; not retrying until the pool's sandbox config changes");
                self.retries.forget(uid);
            }
            Err(_) if ctx.is_done() => {}
            Err(SyncError::Incomplete(err)) => {
                tracing::warn!(pod_uid = uid, retries, %err, "Worker sandbox runtimes file is missing a restorable version; will retry");
                self.queue.add_after(uid, self.retries.when(uid));
            }
            Err(SyncError::Unresolved(err)) if retries >= SYNC_MAX_RETRIES => {
                tracing::warn!(pod_uid = uid, retries, %err, "Worker sandbox runtimes sync failed; giving up until the pool's sandbox config changes");
                self.retries.forget(uid);
            }
            Err(SyncError::Unresolved(err)) | Err(SyncError::Other(err)) => {
                tracing::warn!(pod_uid = uid, retries, %err, "Worker sandbox runtimes sync failed; will retry");
                self.queue.add_after(uid, self.retries.when(uid));
            }
        }
    }

    /// The pod's plan: its pool's resolved sandbox config, planned.
    async fn plan(&self, pod: &WorkerPod) -> Result<SandboxPlan, SyncError> {
        let pool = pod.pool.clone().filter(|p| !p.is_empty()).ok_or_else(|| {
            SyncError::Permanent(format!(
                "worker pod {}/{} has no {WORKER_POOL_LABEL} label",
                pod.namespace, pod.name
            ))
        })?;
        let cfg_name = self
            .cluster
            .pool_sandbox_config(&pod.namespace, &pool)
            .await
            .map_err(|e| {
                SyncError::Unresolved(format!(
                    "while getting WorkerPool {}/{pool}: {e}",
                    pod.namespace
                ))
            })?
            .filter(|c| !c.is_empty())
            .ok_or_else(|| {
                SyncError::Unresolved(format!(
                    "WorkerPool {}/{pool} has not resolved a SandboxConfig",
                    pod.namespace
                ))
            })?;
        let cfg = self
            .cluster
            .sandbox_config(&cfg_name)
            .await
            .map_err(|e| {
                SyncError::Unresolved(format!("while getting SandboxConfig {cfg_name:?}: {e}"))
            })?
            .ok_or_else(|| {
                SyncError::Unresolved(format!("SandboxConfig {cfg_name:?} not found"))
            })?;
        plan_config(&cfg)
    }

    /// Indexes, writes and registers what the pod's pool runs, and stages
    /// its default runtime for prewarming.
    pub async fn sync_pod(&self, ctx: &Ctx, pod: &WorkerPod) -> Result<(), SyncError> {
        let plan = self.plan(pod).await?;
        let hashed = hash_versions(&plan);
        self.index
            .register_runtimes(&plan.config, &hashed)
            .map_err(SyncError::Other)?;
        let (want, build_err) = match self.build_file(ctx, &plan, &hashed).await {
            Ok(f) => (f, None),
            Err((Some(f), e @ SyncError::Incomplete(_))) => (f, Some(e)),
            Err((_, e)) => return Err(e),
        };
        self.write(&pod.uid, &plan.config, &want)?;
        let written = written_versions(&hashed, &want);
        self.index
            .register(
                &pod.uid,
                &[AdvertisedClass {
                    class: plan.sandbox_class.clone(),
                    config: plan.config.clone(),
                    hashed: written.clone(),
                }],
            )
            .map_err(SyncError::Other)?;
        if let Some(prewarm) = &self.prewarm {
            let defaults: Vec<String> = written
                .iter()
                .filter(|v| v.is_default)
                .map(|v| v.hash.clone())
                .collect();
            self.sync_default_runtime_dirs(&pod.uid, &defaults)
                .map_err(SyncError::Other)?;
            for hash in defaults {
                prewarm.enqueue(&hash, &self.default_runtime_dir(&pod.uid, &hash));
            }
        }
        match build_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// The runtimes file for a plan: the default, which must probe, then
    /// every other version that does. Versions failing a permanent probe
    /// are left out; others leave the file incomplete, to retry.
    async fn build_file(
        &self,
        ctx: &Ctx,
        plan: &SandboxPlan,
        hashed: &[HashedVersion],
    ) -> Result<RuntimesFile, (Option<RuntimesFile>, SyncError)> {
        let mut class = RuntimesClass {
            sandbox_class: plan.sandbox_class.clone(),
            ..Default::default()
        };
        let mut seen = HashSet::new();
        let mut incomplete: Vec<String> = Vec::new();
        for v in hashed {
            if seen.contains(&v.hash) {
                continue;
            }
            match self.version_attrs(ctx, &plan.sandbox_class, v).await {
                Ok(attrs) => {
                    seen.insert(v.hash.clone());
                    if v.is_default {
                        class.default_runtime = Runtime { version: attrs };
                    } else {
                        class.restorable_runtimes.push(Runtime { version: attrs });
                    }
                }
                Err(e) => {
                    let msg = format!("SandboxConfig {:?}, version {:?}: {e}", plan.config, v.name);
                    if v.is_default {
                        let e = match e {
                            SyncError::Permanent(_) => SyncError::Permanent(msg),
                            _ => SyncError::Other(msg),
                        };
                        return Err((None, e));
                    }
                    if matches!(e, SyncError::Permanent(_)) {
                        tracing::warn!(err = %msg, "Leaving a sandbox version out of the worker sandbox runtimes file");
                    } else {
                        incomplete.push(msg);
                    }
                }
            }
        }
        let file = RuntimesFile {
            sandbox_classes: vec![class],
        };
        if !incomplete.is_empty() {
            return Err((Some(file), SyncError::Incomplete(incomplete.join("; "))));
        }
        Ok(file)
    }

    async fn version_attrs(
        &self,
        ctx: &Ctx,
        class: &str,
        v: &HashedVersion,
    ) -> Result<std::collections::BTreeMap<String, String>, SyncError> {
        let mut attrs = std::collections::BTreeMap::new();
        attrs.insert(ATTR_ARCHITECTURE.to_string(), node_arch().to_string());
        attrs.insert(ATTR_SANDBOX_BUNDLE_HASH.to_string(), v.hash.clone());
        match class {
            hivekeep_core::sandbox_class::MICROVM => {
                let (vendor, model) = self.host_cpu_identity().map_err(|e| {
                    SyncError::Other(format!("while probing host CPU identity: {e}"))
                })?;
                self.ensure_microvm_assets(ctx, v).await?;
                attrs.insert(ATTR_CPU_VENDOR.to_string(), vendor);
                attrs.insert(ATTR_CPU_MODEL.to_string(), model);
            }
            hivekeep_core::sandbox_class::GVISOR => {
                attrs.insert(
                    ATTR_CPU_FEATURES.to_string(),
                    self.gvisor_cpu_features(ctx, v).await?,
                );
            }
            other => {
                return Err(SyncError::Permanent(format!(
                    "unsupported sandbox class {other:?}"
                )))
            }
        }
        Ok(attrs)
    }

    fn host_cpu_identity(&self) -> Result<(String, String), String> {
        let mut c = self.host_cpu.lock();
        if c.is_none() {
            *c = Some(self.probe.cpu_identity()?);
        }
        Ok(c.clone().expect("set above"))
    }

    /// Fetches a micro-VM version's assets once, so a version whose assets
    /// cannot be fetched is never advertised.
    async fn ensure_microvm_assets(&self, ctx: &Ctx, v: &HashedVersion) -> Result<(), SyncError> {
        if !v.is_default && self.microvm_assets.lock().contains(&v.hash) {
            return Ok(());
        }
        let scratch = self.scratch("microvm-assets-")?;
        let ctx = ctx.with_timeout(PROBE_FETCH_TIMEOUT);
        let res = self.assets.ensure(&ctx, &v.rec, &scratch).await;
        let _ = crate::actordirs::remove_all(&scratch);
        res.map_err(|e| SyncError::Other(e.message().to_string()))?;
        self.microvm_assets.lock().insert(v.hash.clone());
        Ok(())
    }

    /// The CPU features a gVisor version's sandbox exposes, probed once per
    /// release digest.
    async fn gvisor_cpu_features(&self, ctx: &Ctx, v: &HashedVersion) -> Result<String, SyncError> {
        let digest = v
            .rec
            .assets
            .get(crate::assets::GVISOR_ASSET_NAME)
            .map(|a| a.sha256.clone())
            .filter(|d| !d.is_empty())
            .ok_or_else(|| SyncError::Permanent("gvisor version has no \"gvisor\" asset".into()))?;
        if let Some(m) = self.cpu_features.lock().get(&digest) {
            return Ok(m.clone());
        }
        let scratch = self.scratch("cpu-features-")?;
        let ctx = ctx.with_timeout(PROBE_FETCH_TIMEOUT);
        let res = async {
            let paths = self
                .assets
                .ensure(&ctx, &v.rec, &scratch)
                .await
                .map_err(|e| SyncError::Other(e.message().to_string()))?;
            let runsc = crate::assets::runsc_path_for(&paths);
            let names = self.probe.cpu_features(Path::new(&runsc)).await?;
            self.probe
                .encode_cpu_features(node_arch(), &names)
                .map_err(SyncError::Permanent)
        }
        .await;
        let _ = crate::actordirs::remove_all(&scratch);
        let mask = res?;
        self.cpu_features.lock().insert(digest, mask.clone());
        Ok(mask)
    }

    fn scratch(&self, prefix: &str) -> Result<PathBuf, SyncError> {
        let dir = self.paths.sandbox_asset_scratch_dir();
        crate::atomicfile::create_dir_all_mode(&dir, 0o700)
            .map_err(|e| SyncError::Other(e.to_string()))?;
        crate::atomicfile::tempdir_in(&dir, prefix)
            .map_err(|e| SyncError::Other(format!("while creating scratch dir: {e}")))
    }

    fn write(&self, uid: &str, config: &str, want: &RuntimesFile) -> Result<(), SyncError> {
        crate::atomicfile::create_dir_all_mode(&self.paths.worker_dir(uid), 0o700)
            .map_err(|e| SyncError::Other(format!("while creating worker directory: {e}")))?;
        if write_runtimes_file(&self.paths.worker_sandbox_runtimes_file(uid), want)
            .map_err(SyncError::Other)?
        {
            for c in &want.sandbox_classes {
                tracing::info!(
                    pod_uid = uid, sandbox_config = config, sandbox_class = %c.sandbox_class,
                    default_bundle_hash = %c.default_runtime.version.get(ATTR_SANDBOX_BUNDLE_HASH).cloned().unwrap_or_default(),
                    restorable_runtimes = c.restorable_runtimes.len(),
                    "Wrote worker sandbox runtimes"
                );
            }
        }
        Ok(())
    }

    /// Where a worker's default runtime `hash` is staged.
    pub fn default_runtime_dir(&self, uid: &str, hash: &str) -> PathBuf {
        self.paths
            .worker_dir(uid)
            .join(DEFAULT_RUNTIME_DIRS_NAME)
            .join(hash.replace(':', "-"))
    }

    /// Leaves exactly the default runtime dirs of `defaults`.
    fn sync_default_runtime_dirs(&self, uid: &str, defaults: &[String]) -> Result<(), String> {
        let parent = self.paths.worker_dir(uid).join(DEFAULT_RUNTIME_DIRS_NAME);
        crate::atomicfile::create_dir_all_mode(&parent, 0o700)
            .map_err(|e| format!("while creating the worker's default runtime dir: {e}"))?;
        let want: HashSet<String> = defaults
            .iter()
            .filter_map(|h| {
                self.default_runtime_dir(uid, h)
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
            })
            .collect();
        for e in std::fs::read_dir(&parent)
            .map_err(|e| e.to_string())?
            .flatten()
        {
            let name = e.file_name().to_string_lossy().into_owned();
            if !want.contains(&name) {
                crate::actordirs::remove_all(&e.path())
                    .map_err(|e| format!("while removing a retired default runtime dir: {e}"))?;
            }
        }
        for name in want {
            match crate::atomicfile::create_dir_mode(&parent.join(name), 0o700) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(format!("while creating a default runtime dir: {e}")),
            }
        }
        Ok(())
    }

    /// Removes the directory of a pod that is gone, and forgets what it
    /// advertised.
    fn remove_dir(&self, uid: &str) -> Result<(), SyncError> {
        let dir = self.paths.worker_dir(uid);
        if dir.exists() {
            tracing::info!(
                pod_uid = uid,
                "Removing the directory of a worker pod that is gone"
            );
        }
        crate::actordirs::remove_all(&dir)
            .map_err(|e| SyncError::Other(format!("while removing worker directory: {e}")))?;
        self.index.forget(uid);
        Ok(())
    }
}

/// The versions the file advertises.
/// A random delay below `max` (zero for a zero `max`).
fn jitter(max: Duration) -> Duration {
    use std::hash::BuildHasher;
    let nanos = max.as_nanos().min(u128::from(u64::MAX)) as u64;
    if nanos == 0 {
        return Duration::ZERO;
    }
    // Each RandomState is freshly keyed, so this is a cheap random draw.
    Duration::from_nanos(std::collections::hash_map::RandomState::new().hash_one(()) % nanos)
}

fn written_versions(hashed: &[HashedVersion], file: &RuntimesFile) -> Vec<HashedVersion> {
    let mut written = HashSet::new();
    for c in &file.sandbox_classes {
        if let Some(h) = c.default_runtime.version.get(ATTR_SANDBOX_BUNDLE_HASH) {
            written.insert(h.clone());
        }
        for r in &c.restorable_runtimes {
            if let Some(h) = r.version.get(ATTR_SANDBOX_BUNDLE_HASH) {
                written.insert(h.clone());
            }
        }
    }
    hashed
        .iter()
        .filter(|v| written.contains(&v.hash))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets::SandboxAssetsRecord;
    use crate::sandbox_versions::SandboxConfig;
    use crate::sandbox_versions::{read_runtimes_file, AssetFile, SandboxVersion, VersionState};
    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tonic::Status;

    const POD: &str = "422938ba-8860-4983-a25d-d6bcb0a69d4e";

    struct FakeAssets;

    #[async_trait]
    impl AssetFetcher for FakeAssets {
        async fn ensure(
            &self,
            ctx: &Ctx,
            rec: &SandboxAssetsRecord,
            dst: &Path,
        ) -> Result<BTreeMap<String, String>, Status> {
            std::fs::create_dir_all(dst).unwrap();
            self.stage(ctx, rec, dst).await
        }
        async fn stage(
            &self,
            _: &Ctx,
            rec: &SandboxAssetsRecord,
            dst: &Path,
        ) -> Result<BTreeMap<String, String>, Status> {
            if rec.assets.values().any(|a| a.url.contains("broken")) {
                return Err(Status::unavailable("cannot fetch"));
            }
            Ok(rec
                .assets
                .keys()
                .map(|k| {
                    (
                        k.clone(),
                        dst.join(k).join("runsc").to_string_lossy().into_owned(),
                    )
                })
                .collect())
        }
    }

    struct FakeProbe {
        features: AtomicUsize,
    }

    #[async_trait]
    impl HostProbe for FakeProbe {
        fn cpu_identity(&self) -> Result<(String, String), String> {
            Ok(("GenuineIntel".into(), "Xeon".into()))
        }
        async fn cpu_features(&self, _: &Path) -> Result<Vec<String>, SyncError> {
            self.features.fetch_add(1, Ordering::SeqCst);
            Ok(vec!["sse".into(), "avx".into()])
        }
        fn encode_cpu_features(&self, _: &str, names: &[String]) -> Result<String, String> {
            Ok(names.join("|"))
        }
    }

    struct Recorder(Mutex<Vec<(String, PathBuf)>>);
    impl Prewarm for Recorder {
        fn enqueue(&self, hash: &str, dir: &Path) {
            self.0.lock().push((hash.into(), dir.into()));
        }
    }

    fn view(versions: Vec<(&str, &str)>, default: &str) -> StaticClusterView {
        let versions = versions
            .into_iter()
            .map(|(name, url)| {
                let mut files = BTreeMap::new();
                files.insert(
                    "gvisor".to_string(),
                    AssetFile {
                        url: url.into(),
                        sha256: hex::encode(sha2::Sha256::digest(name.as_bytes())),
                    },
                );
                let mut assets = HashMap::new();
                assets.insert(node_arch().to_string(), files);
                SandboxVersion {
                    name: name.into(),
                    state: VersionState::Enabled,
                    assets,
                }
            })
            .collect();
        let mut v = StaticClusterView::default();
        v.pods.insert(POD.into(), ("ns".into(), "pool".into()));
        v.pools.insert("ns/pool".into(), "cfg".into());
        v.configs.push(SandboxConfig {
            name: "cfg".into(),
            generation: 1,
            sandbox_class: "gvisor".into(),
            default_version: default.into(),
            versions,
        });
        v
    }

    use sha2::Digest;

    fn syncer(
        d: &Path,
        view: StaticClusterView,
    ) -> (
        Arc<WorkerSandboxSyncer>,
        Arc<RuntimeIndex>,
        Arc<Recorder>,
        Arc<FakeProbe>,
    ) {
        let paths = Arc::new(NodePaths::new(d));
        let index = Arc::new(RuntimeIndex::in_memory());
        let rec = Arc::new(Recorder(Mutex::new(vec![])));
        let probe = Arc::new(FakeProbe {
            features: AtomicUsize::new(0),
        });
        let s = Arc::new(WorkerSandboxSyncer::new(
            Arc::new(view),
            paths,
            index.clone(),
            Some(rec.clone()),
            Arc::new(FakeAssets),
            probe.clone(),
        ));
        (s, index, rec, probe)
    }

    fn pod() -> WorkerPod {
        WorkerPod {
            uid: POD.into(),
            namespace: "ns".into(),
            name: "worker-0".into(),
            pool: Some("pool".into()),
            deleting: false,
        }
    }

    #[tokio::test]
    async fn writes_indexes_and_prewarms() {
        let d = tempfile::tempdir().unwrap();
        let (s, index, rec, probe) = syncer(
            d.path(),
            view(
                vec![("v1", "gs://g/1.tar.zst"), ("v2", "gs://g/2.tar.zst")],
                "v2",
            ),
        );
        s.sync_pod(&Ctx::background(), &pod()).await.unwrap();
        let file = read_runtimes_file(&s.paths.worker_sandbox_runtimes_file(POD)).unwrap();
        let class = &file.sandbox_classes[0];
        assert_eq!(class.sandbox_class, "gvisor");
        assert_eq!(class.restorable_runtimes.len(), 1);
        assert_eq!(class.default_runtime.version[ATTR_CPU_FEATURES], "sse|avx");
        let default_hash = class.default_runtime.version[ATTR_SANDBOX_BUNDLE_HASH].clone();
        assert!(index.lookup(&default_hash).is_some());
        assert_eq!(
            index.worker(POD).unwrap()["gvisor"].default_hash,
            default_hash
        );
        assert_eq!(rec.0.lock().len(), 1);
        assert!(s.default_runtime_dir(POD, &default_hash).is_dir());
        // Probes are cached per release.
        s.sync_pod(&Ctx::background(), &pod()).await.unwrap();
        assert_eq!(probe.features.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_unfetchable_old_version_leaves_the_file_incomplete() {
        let d = tempfile::tempdir().unwrap();
        let (s, _index, _rec, _probe) = syncer(
            d.path(),
            view(vec![("v1", "gs://broken/1"), ("v2", "gs://g/2")], "v2"),
        );
        let err = s.sync_pod(&Ctx::background(), &pod()).await.unwrap_err();
        assert!(matches!(err, SyncError::Incomplete(_)));
        let file = read_runtimes_file(&s.paths.worker_sandbox_runtimes_file(POD)).unwrap();
        assert!(file.sandbox_classes[0].restorable_runtimes.is_empty());
    }

    #[tokio::test]
    async fn an_unresolved_pool_and_a_gone_pod() {
        let d = tempfile::tempdir().unwrap();
        let mut v = view(vec![("v1", "gs://g/1")], "v1");
        v.pools.clear();
        let (s, index, _rec, _probe) = syncer(d.path(), v);
        assert!(matches!(
            s.sync_pod(&Ctx::background(), &pod()).await,
            Err(SyncError::Unresolved(_))
        ));
        std::fs::create_dir_all(s.paths.worker_dir("gone")).unwrap();
        s.remove_dir("gone").unwrap();
        assert!(!s.paths.worker_dir("gone").exists());
        assert!(index.worker("gone").is_none());
    }
}

#[cfg(test)]
mod behavior_tests;

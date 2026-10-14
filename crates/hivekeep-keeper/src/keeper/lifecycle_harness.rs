//! A node for lifecycle tests: a scriptable fake worker, a recording object
//! store over a local directory, the real asset stager fed by a fake
//! anonymous fetcher, a fake image cache, and the real tracker, transfer
//! and system-info refresher, all rooted in one temp directory.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::FutureExt;
use hivekeep_core::resource::ActorRef;
use hivekeep_core::validation::rules::Operation;
use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::worker::v1 as wpb;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, watch};
use tonic::Status;

use super::{Keeper, KeeperDeps};
use crate::assets::{AssetEntry, SandboxAssetsRecord, GVISOR_ASSET_NAME, MANIFEST_NAME};
use crate::context::Ctx;
use crate::filecache;
use crate::keeper_metrics::KeeperInstruments;
use crate::objectstore::{
    FetchAsset, FetchSnapshot, LocalObjectStore, ObjectStore, UploadSnapshot,
};
use crate::oci::{Image, ImageCache, ImageConfig};
use crate::paths::NodePaths;
use crate::runtime_index::{
    AdvertisedClass, HashedVersion, RuntimeIndex, ATTR_SANDBOX_BUNDLE_HASH,
};
use crate::sandbox_assets::{AnonymousFetcher, AssetStager};
use crate::snapshot_cache::SnapshotDownloader;
use crate::snapshot_tracker::{self, Options, Reporter, SnapshotRef, Store, Tracker};
use crate::snapshot_transfer::SnapshotTransfer;
use crate::system_info::SystemInfoRefresher;
use crate::trustbundle::Source;
use crate::volumes::{MountVolumeRequest, NoVolumeDrivers, VolumeManager, VolumePlugin};
use crate::worker_dialer::{WorkerApi, WorkerConnector};
use crate::worker_jwt::tests::FakeWorkers;
use crate::worker_jwt::WorkerActorJwts;

pub(super) const HIVE: &str = "hk-demo";
pub(super) const ACTOR: &str = "counter";
/// The actor `run_plugin_actor` runs.
pub(super) const UID: &str = "01234567-89ab-cdef-0123-456789abcdef";
pub(super) const POD: &str = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f";
/// The image the fake registry serves.
pub(super) const IMAGE: &str =
    "registry.test/actor@sha256:7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730";
/// A pinned reference that passes validation but the registry lacks.
pub(super) const MISSING_IMAGE: &str =
    "registry.test/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/// An uploaded snapshot of another actor, as tests stage one directly.
pub(super) const PAUSED_SNAPSHOT_URI: &str =
    "gs://bucket/root/hives/hk-demo/actors/3d7f1b62-8a04-4c19-b5e6-2f9c7a1d0e38/snapshots/snap-1";

/// An async callback a fake runs inside an RPC.
pub(super) type Hook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;
/// A callback whose result fails the RPC.
pub(super) type FallibleHook =
    Arc<dyn Fn() -> BoxFuture<'static, Result<(), Status>> + Send + Sync>;

pub(super) fn hook<F, Fut>(f: F) -> Hook
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Arc::new(move || f().boxed())
}

pub(super) fn fallible_hook<F, Fut>(f: F) -> FallibleHook
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Status>> + Send + 'static,
{
    Arc::new(move || f().boxed())
}

/// A one-way flag tasks can wait on.
#[derive(Clone)]
pub(super) struct Latch(Arc<watch::Sender<bool>>);

impl Latch {
    pub(super) fn new() -> Self {
        Latch(Arc::new(watch::channel(false).0))
    }
    pub(super) fn open(&self) {
        self.0.send_replace(true);
    }
    pub(super) async fn wait(&self) {
        let mut rx = self.0.subscribe();
        let _ = rx.wait_for(|v| *v).await;
    }
}

/// A hook that, the first time it runs, opens `started` and waits for
/// `release`.
pub(super) fn block_once(started: &Latch, release: &Latch) -> Hook {
    let fired = Arc::new(AtomicBool::new(false));
    let (started, release) = (started.clone(), release.clone());
    hook(move || {
        let first = !fired.swap(true, Ordering::SeqCst);
        let (started, release) = (started.clone(), release.clone());
        async move {
            if first {
                started.open();
                release.wait().await;
            }
        }
    })
}

/// Awaits `f`, failing the test after `d`.
pub(super) async fn within<T>(d: Duration, what: &str, f: impl Future<Output = T>) -> T {
    tokio::time::timeout(d, f)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

/// Polls `cond` until it holds, failing the test after ten seconds.
pub(super) async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    within(Duration::from_secs(10), what, async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
}

/// Asserts `h` has not finished within `d`.
pub(super) async fn assert_pending<T>(h: &mut tokio::task::JoinHandle<T>, d: Duration, what: &str) {
    if tokio::time::timeout(d, h).await.is_ok() {
        panic!("{what} returned, want it to wait");
    }
}

/// Runs `f` as a gRPC client sees it: it ends early with the caller's
/// context, and a reply that arrives after the context ended is lost.
async fn as_rpc<T>(ctx: &Ctx, f: impl Future<Output = Result<T, Status>>) -> Result<T, Status> {
    let r = match ctx.run(f).await {
        Ok(r) => r,
        Err(e) => return Err(Status::from(e)),
    };
    if let Some(e) = ctx.err() {
        return Err(Status::from(e));
    }
    r
}

/// A fake worker. It writes the files a real checkpoint would leave in the
/// checkpoint dir and reads back what a restore was handed. Like a real
/// worker it takes every actor directory from the request.
#[derive(Default)]
pub(super) struct FakeWorker {
    /// Written at checkpoint and reported back as the snapshot's files.
    pub(super) snapshot_files: Mutex<BTreeMap<String, String>>,
    /// The file contents the latest RestoreWorkload found staged.
    pub(super) restored: Mutex<BTreeMap<String, String>>,
    /// Fails every RestoreWorkload after it recorded the request.
    pub(super) restore_err: Mutex<Option<Status>>,
    /// The ActorDirs each RPC arrived with, by RPC name.
    pub(super) actor_dirs: Mutex<HashMap<String, wpb::ActorDirs>>,
    /// The latest RestoreWorkload's preserve_restore_dir.
    pub(super) preserve_restore_dir: AtomicBool,
    /// The epoch RunWorkload and RestoreWorkload report.
    pub(super) worker_epoch: AtomicI64,
    /// The fidelity each checkpoint or restore arrived with.
    pub(super) fidelities: Mutex<HashMap<String, i32>>,
    /// Runs inside CheckpointWorkload after the files are written.
    pub(super) on_checkpoint: Mutex<Option<Hook>>,
    /// Runs inside TerminateWorkload, which fails with its error.
    pub(super) on_terminate: Mutex<Option<FallibleHook>>,
    /// Runs inside RestoreWorkload after it recorded the request.
    pub(super) on_restore: Mutex<Option<Hook>>,
    /// Makes a checkpoint stop the workload, as a real worker's does: until
    /// a Run or Restore starts it again, CheckpointWorkload answers with the
    /// stopping checkpoint's response while the checkpoint dir still holds
    /// its files, and fails otherwise.
    pub(super) stops_workload: AtomicBool,
    stopped_by: Mutex<Option<wpb::CheckpointWorkloadResponse>>,
    /// TerminateWorkload calls.
    pub(super) terminations: AtomicI32,
}

impl FakeWorker {
    pub(super) fn with_files(files: &[(&str, &str)]) -> Arc<Self> {
        let w = FakeWorker::default();
        *w.snapshot_files.lock() = files
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Arc::new(w)
    }

    fn record_dirs(&self, rpc: &str, d: Option<wpb::ActorDirs>) {
        self.actor_dirs
            .lock()
            .insert(rpc.into(), d.unwrap_or_default());
    }

    pub(super) fn dirs(&self, rpc: &str) -> Option<wpb::ActorDirs> {
        self.actor_dirs.lock().get(rpc).cloned()
    }

    pub(super) fn restored(&self, name: &str) -> Option<String> {
        self.restored.lock().get(name).cloned()
    }

    pub(super) fn preserved(&self) -> bool {
        self.preserve_restore_dir.load(Ordering::SeqCst)
    }

    pub(super) fn set_on_checkpoint(&self, h: Hook) {
        *self.on_checkpoint.lock() = Some(h);
    }

    pub(super) fn set_on_terminate(&self, h: FallibleHook) {
        *self.on_terminate.lock() = Some(h);
    }
}

#[async_trait]
impl WorkerApi for FakeWorker {
    async fn run_workload(
        &self,
        ctx: &Ctx,
        req: wpb::RunWorkloadRequest,
    ) -> Result<wpb::RunWorkloadResponse, Status> {
        as_rpc(ctx, async {
            self.record_dirs("RunWorkload", req.actor_dirs);
            *self.stopped_by.lock() = None;
            Ok(wpb::RunWorkloadResponse {
                worker_epoch: self.worker_epoch.load(Ordering::SeqCst),
            })
        })
        .await
    }

    async fn checkpoint_workload(
        &self,
        ctx: &Ctx,
        req: wpb::CheckpointWorkloadRequest,
    ) -> Result<wpb::CheckpointWorkloadResponse, Status> {
        as_rpc(ctx, async {
            self.record_dirs("CheckpointWorkload", req.actor_dirs.clone());
            self.fidelities
                .lock()
                .insert("CheckpointWorkload".into(), req.fidelity);
            let dir = PathBuf::from(req.actor_dirs.unwrap_or_default().checkpoint_dir);
            let stopped = self.stopped_by.lock().clone();
            if let Some(resp) = stopped {
                for name in &resp.snapshot_files {
                    if let Err(e) = std::fs::metadata(dir.join(name)) {
                        return Err(Status::internal(format!("no workload to checkpoint: {e}")));
                    }
                }
                return Ok(resp);
            }
            let files = self.snapshot_files.lock().clone();
            for (name, body) in &files {
                std::fs::write(dir.join(name), body)
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
            let h = self.on_checkpoint.lock().clone();
            if let Some(h) = h {
                h().await;
            }
            let resp = wpb::CheckpointWorkloadResponse {
                snapshot_files: files.keys().cloned().collect(),
                data_snapshot_files: vec![],
            };
            if self.stops_workload.load(Ordering::SeqCst) {
                *self.stopped_by.lock() = Some(resp.clone());
            }
            Ok(resp)
        })
        .await
    }

    async fn restore_workload(
        &self,
        ctx: &Ctx,
        req: wpb::RestoreWorkloadRequest,
    ) -> Result<wpb::RestoreWorkloadResponse, Status> {
        as_rpc(ctx, async {
            self.record_dirs("RestoreWorkload", req.actor_dirs.clone());
            self.fidelities
                .lock()
                .insert("RestoreWorkload".into(), req.fidelity);
            self.preserve_restore_dir
                .store(req.preserve_restore_dir, Ordering::SeqCst);
            let h = self.on_restore.lock().clone();
            if let Some(h) = h {
                h().await;
            }
            if let Some(e) = self.restore_err.lock().clone() {
                return Err(e);
            }
            *self.stopped_by.lock() = None;
            let dir = PathBuf::from(req.actor_dirs.unwrap_or_default().restore_dir);
            let mut restored = BTreeMap::new();
            let names: Vec<String> = self.snapshot_files.lock().keys().cloned().collect();
            for name in names {
                let body = std::fs::read_to_string(dir.join(&name))
                    .map_err(|e| Status::internal(format!("reading {name}: {e}")))?;
                restored.insert(name, body);
            }
            *self.restored.lock() = restored;
            Ok(wpb::RestoreWorkloadResponse {
                worker_epoch: self.worker_epoch.load(Ordering::SeqCst),
            })
        })
        .await
    }

    async fn terminate_workload(
        &self,
        ctx: &Ctx,
        req: wpb::TerminateWorkloadRequest,
    ) -> Result<(), Status> {
        as_rpc(ctx, async {
            self.terminations.fetch_add(1, Ordering::SeqCst);
            self.record_dirs("TerminateWorkload", req.actor_dirs);
            let h = self.on_terminate.lock().clone();
            if let Some(h) = h {
                h().await?;
            }
            Ok(())
        })
        .await
    }

    async fn get_active_workload_stats(
        &self,
        _: &Ctx,
    ) -> Result<wpb::GetActiveWorkloadStatsResponse, Status> {
        Ok(Default::default())
    }
}

/// Dials the fake worker. Its socket is a plain file whose presence stands
/// for a live worker process.
pub(super) struct Connector {
    pub(super) worker: Mutex<Option<Arc<FakeWorker>>>,
    pub(super) socket: Mutex<PathBuf>,
}

#[async_trait]
impl WorkerConnector for Connector {
    async fn connect(&self, _: &str) -> Result<Arc<dyn WorkerApi>, Status> {
        match self.worker.lock().clone() {
            Some(w) => Ok(w),
            None => Err(Status::unavailable("no worker is listening")),
        }
    }
    fn socket_path(&self, _: &str) -> PathBuf {
        self.socket.lock().clone()
    }
}

/// Serves a public object from memory, counting reads.
#[derive(Default)]
pub(super) struct FakeGcs {
    pub(super) data: Mutex<Vec<u8>>,
    pub(super) err: Mutex<Option<String>>,
    pub(super) gets: AtomicUsize,
    pub(super) delay: Mutex<Duration>,
}

#[async_trait]
impl AnonymousFetcher for FakeGcs {
    async fn fetch(&self, _: &Ctx, _: &url::Url, path: &Path, _: i64) -> Result<(), String> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let delay = *self.delay.lock();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if let Some(e) = self.err.lock().clone() {
            return Err(e);
        }
        let data = self.data.lock().clone();
        std::fs::write(path, data).map_err(|e| e.to_string())
    }
}

/// An object store over a local directory that can fail or pause uploads.
pub(super) struct RecordingStore {
    pub(super) inner: LocalObjectStore,
    root: PathBuf,
    pub(super) put_err: Mutex<Option<String>>,
    pub(super) on_put: Mutex<Option<Hook>>,
}

impl RecordingStore {
    fn new(root: PathBuf) -> Self {
        RecordingStore {
            inner: LocalObjectStore::new(root.clone()),
            root,
            put_err: Mutex::new(None),
            on_put: Mutex::new(None),
        }
    }

    /// Every object held, relative to the store's root.
    pub(super) fn keys(&self) -> Vec<String> {
        let mut out: Vec<String> = walkdir::WalkDir::new(&self.root)
            .into_iter()
            .flatten()
            .filter(|e| e.file_type().is_file())
            .filter_map(|e| {
                e.path()
                    .strip_prefix(&self.root)
                    .ok()
                    .map(|p| p.to_string_lossy().into_owned())
            })
            .filter(|k| !k.split('/').any(|s| s.starts_with('.')))
            .collect();
        out.sort();
        out
    }

    /// Where the manifest of the snapshot at `uri` is kept.
    pub(super) fn manifest_path(&self, uri: &str) -> PathBuf {
        self.inner.dir_for(uri).unwrap().join(MANIFEST_NAME)
    }

    pub(super) fn has_manifest(&self, uri: &str) -> bool {
        self.manifest_path(uri).is_file()
    }

    pub(super) fn fail_puts(&self, why: &str) {
        *self.put_err.lock() = Some(why.into());
    }
}

#[async_trait]
impl ObjectStore for RecordingStore {
    async fn fetch_snapshot(&self, ctx: &Ctx, req: FetchSnapshot) -> Result<(), Status> {
        self.inner.fetch_snapshot(ctx, req).await
    }
    async fn upload_snapshot(&self, ctx: &Ctx, req: UploadSnapshot) -> Result<(), Status> {
        let h = self.on_put.lock().clone();
        if let Some(h) = h {
            h().await;
        }
        if let Some(e) = self.put_err.lock().clone() {
            return Err(Status::internal(e));
        }
        self.inner.upload_snapshot(ctx, req).await
    }
    async fn fetch_asset(&self, _: &Ctx, _: FetchAsset) -> Result<(), Status> {
        Err(Status::unimplemented("this store serves no assets"))
    }
}

/// An image cache that serves [`IMAGE`] and nothing else.
pub(super) struct FakeImages {
    layer: PathBuf,
}

#[async_trait]
impl ImageCache for FakeImages {
    async fn ensure_image(&self, _: &Ctx, reference: &str) -> Result<Image, Status> {
        if reference != IMAGE {
            return Err(Status::not_found(format!("manifest unknown: {reference}")));
        }
        Ok(Image {
            digest: IMAGE.rsplit('@').next().unwrap().into(),
            config: ImageConfig {
                cmd: vec!["/bin/app".into()],
                ..Default::default()
            },
            layer_dirs: vec![self.layer.to_string_lossy().into_owned()],
        })
    }
}

/// A volume driver that can fail mounts.
#[derive(Default)]
pub(super) struct FakeVolumes {
    pub(super) mount_err: Mutex<Option<Status>>,
}

#[async_trait]
impl VolumePlugin for FakeVolumes {
    async fn mount_volume(&self, _: &Ctx, _: MountVolumeRequest) -> Result<(), Status> {
        match self.mount_err.lock().clone() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
    async fn unmount_volume(&self, _: &Ctx, _: &str, _: &str) -> Result<(), Status> {
        Ok(())
    }
}

/// A reporter the control plane always acknowledges.
pub(super) struct AckReporter;

#[async_trait]
impl Reporter for AckReporter {
    async fn report_actor_snapshot_state(
        &self,
        _: &Ctx,
        _: &str,
        _: &ActorRef,
        _: &pb::ActorSnapshotState,
    ) -> Result<(), Status> {
        Ok(())
    }
}

/// A tracker store whose uploads report on `started` and then wait for
/// `release` before uploading through `inner`. With `canceled`, an upload
/// whose context ends reports there and still waits for `release`, as an
/// object-store call that is finishing would. With `objects`, every upload
/// after the first gated one fails.
pub(super) struct GatingStore {
    pub(super) inner: Arc<SnapshotTransfer>,
    pub(super) objects: Option<Arc<RecordingStore>>,
    pub(super) started: mpsc::Sender<()>,
    pub(super) release: Latch,
    pub(super) canceled: Option<mpsc::Sender<()>>,
}

#[async_trait]
impl Store for GatingStore {
    async fn upload_local_snapshot(
        &self,
        ctx: &Ctx,
        actor_uid: &str,
        r: &SnapshotRef,
    ) -> Result<bool, Status> {
        let _ = self.started.send(()).await;
        tokio::select! {
            _ = self.release.wait() => {}
            e = ctx.done() => {
                if let Some(c) = &self.canceled {
                    let _ = c.send(()).await;
                    self.release.wait().await;
                }
                return Err(Status::from(e));
            }
        }
        let res = self.inner.upload_local_snapshot(ctx, actor_uid, r).await;
        if let Some(o) = &self.objects {
            let _ = std::fs::remove_file(o.manifest_path(r.uri()));
            o.fail_puts("promote re-uploaded a snapshot already uploaded in the background");
        }
        res
    }
}

/// A tracker store whose uploads report on `started` and then block until
/// their context ends.
pub(super) struct BlockingStore {
    pub(super) started: mpsc::Sender<()>,
}

#[async_trait]
impl Store for BlockingStore {
    async fn upload_local_snapshot(
        &self,
        ctx: &Ctx,
        _: &str,
        _: &SnapshotRef,
    ) -> Result<bool, Status> {
        let _ = self.started.send(()).await;
        Err(Status::from(ctx.done().await))
    }
}

/// Makes a tracker store from the transfer and the object store.
pub(super) type MakeStore =
    Box<dyn FnOnce(Arc<SnapshotTransfer>, Arc<RecordingStore>) -> Arc<dyn Store>>;

/// Which store the tracker uploads with.
pub(super) enum StoreKind {
    /// None: nothing is uploaded or detached in the background.
    None,
    /// The snapshot transfer.
    Transfer,
    /// A store made from the transfer and the object store.
    Custom(MakeStore),
}

/// How [`Node::new`] builds the node.
pub(super) struct NodeOpts {
    pub(super) boot_id: String,
    pub(super) store: StoreKind,
    pub(super) tracker: Options,
    pub(super) start_tracker: bool,
    pub(super) checkpoint_timeout: Duration,
    /// Serves shared snapshots through a cache with no min age.
    pub(super) snapshot_cache: bool,
    /// The trust bundle the egress CA name resolves to.
    pub(super) egress_ca: Arc<Mutex<Option<String>>>,
    /// Pins the separate snapshots root.
    pub(super) snapshots_dir: Option<PathBuf>,
}

impl Default for NodeOpts {
    fn default() -> Self {
        NodeOpts {
            boot_id: String::new(),
            store: StoreKind::None,
            tracker: Options::default(),
            start_tracker: false,
            checkpoint_timeout: super::DEFAULT_CHECKPOINT_TIMEOUT,
            snapshot_cache: false,
            egress_ca: Arc::new(Mutex::new(None)),
            snapshots_dir: None,
        }
    }
}

/// A gVisor release tarball whose runsc holds `runsc`.
pub(super) fn release_tarball(runsc: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut b = tar::Builder::new(&mut buf);
        for (name, data) in [
            ("runsc", runsc.as_bytes()),
            ("gvisor-bin/containerd-shim", b"shim bytes"),
        ] {
            let mut h = tar::Header::new_gnu();
            h.set_size(data.len() as u64);
            h.set_mode(0o755);
            b.append_data(&mut h, name, data).unwrap();
        }
        b.finish().unwrap();
    }
    buf
}

/// The record of a gVisor runtime whose only asset is `tarball`.
pub(super) fn sandbox_record(tarball: &[u8]) -> SandboxAssetsRecord {
    let mut rec = SandboxAssetsRecord {
        sandbox_class: "gvisor".into(),
        ..Default::default()
    };
    rec.assets.insert(
        GVISOR_ASSET_NAME.into(),
        AssetEntry {
            url: "gs://test-bucket/gvisor.tar".into(),
            sha256: hex::encode(Sha256::digest(tarball)),
        },
    );
    rec
}

/// The runtime the control plane names for an actor placed on a worker
/// advertising `rec`.
pub(super) fn runtime_for(rec: &SandboxAssetsRecord) -> pb::ActorSandboxRuntime {
    pb::ActorSandboxRuntime {
        sandbox_class: rec.sandbox_class.clone(),
        version: Some(pb::VersionedSandboxCompat {
            schema_version: "1".into(),
            attributes: vec![
                pb::AttributeEntry {
                    key: "architecture".into(),
                    value: std::env::consts::ARCH.into(),
                },
                pb::AttributeEntry {
                    key: ATTR_SANDBOX_BUNDLE_HASH.into(),
                    value: rec.bundle_hash(),
                },
            ],
        }),
    }
}

/// The snapshot the control plane names for `uid`'s checkpoint `name`.
pub(super) fn lifecycle_snapshot(
    uid: &str,
    name: &str,
    generation: i32,
    fidelity: pb::SnapshotFidelity,
) -> pb::Snapshot {
    pb::Snapshot {
        snapshot_uuid: name.into(),
        generation,
        fidelity: fidelity as i32,
        object: Some(pb::ObjectStorage {
            snapshot_uri: format!("gs://test-bucket/hives/{HIVE}/actors/{uid}/snapshots/{name}"),
            sharing: 0,
        }),
    }
}

/// Writes a snapshot directory: `contents`, then `rec` as its manifest.
pub(super) fn write_local_snapshot(
    dir: &Path,
    rec: &SandboxAssetsRecord,
    contents: &[(&str, &str)],
) {
    std::fs::create_dir_all(dir).unwrap();
    for (name, body) in contents {
        std::fs::write(dir.join(name), body).unwrap();
    }
    std::fs::write(dir.join(MANIFEST_NAME), rec.to_json()).unwrap();
}

/// Fails unless `dir` is gone or empty.
pub(super) fn assert_dir_gone(what: &str, dir: &Path) {
    match std::fs::read_dir(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => panic!("reading {what} {}: {e}", dir.display()),
        Ok(entries) => {
            let left: Vec<_> = entries
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            assert!(
                left.is_empty(),
                "{what} {} still holds {left:?}",
                dir.display()
            );
        }
    }
}

pub(super) fn exists(p: &Path) -> bool {
    std::fs::symlink_metadata(p).is_ok()
}

/// A node: the keeper and everything it drives.
pub(super) struct Node {
    pub(super) dir: tempfile::TempDir,
    pub(super) paths: Arc<NodePaths>,
    pub(super) keeper: Arc<Keeper>,
    pub(super) worker: Arc<FakeWorker>,
    pub(super) connector: Arc<Connector>,
    pub(super) objects: Arc<RecordingStore>,
    pub(super) transfer: Arc<SnapshotTransfer>,
    pub(super) gcs: Arc<FakeGcs>,
    pub(super) runtimes: Arc<RuntimeIndex>,
    pub(super) refresher: Arc<SystemInfoRefresher>,
    pub(super) volumes: Arc<FakeVolumes>,
    pub(super) registry: prometheus::Registry,
    pub(super) snapshot_cache: Option<filecache::Store>,
    pub(super) tracker_ctx: Ctx,
    egress_ca: Arc<Mutex<Option<String>>>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.tracker_ctx.cancel();
    }
}

impl Node {
    /// A node whose worker writes `files` at checkpoint, its socket live.
    pub(super) fn new(files: &[(&str, &str)], opts: NodeOpts) -> Node {
        Self::with_worker(FakeWorker::with_files(files), opts)
    }

    pub(super) fn with_worker(worker: Arc<FakeWorker>, opts: NodeOpts) -> Node {
        let dir = tempfile::tempdir().unwrap();
        let mut np = NodePaths::new(dir.path().join("node"));
        if let Some(s) = &opts.snapshots_dir {
            np = np.with_snapshots_dir(s.clone());
        }
        let paths = Arc::new(np);
        let objects = Arc::new(RecordingStore::new(dir.path().join("objects")));
        let jwts = Arc::new(WorkerActorJwts::new(
            Arc::new(FakeWorkers::default()),
            Arc::new(crate::clock::SystemClock),
        ));
        let transfer = Arc::new(SnapshotTransfer::new(
            objects.clone(),
            jwts,
            paths.clone(),
            opts.boot_id.clone(),
        ));
        let snapshot_cache = opts.snapshot_cache.then(|| {
            filecache::Store::open(
                dir.path().join("snapshot-cache"),
                filecache::Options {
                    min_age: Duration::ZERO,
                    ..Default::default()
                },
            )
            .unwrap()
        });
        let downloader = Arc::new(SnapshotDownloader::new(
            transfer.clone(),
            snapshot_cache.clone(),
        ));
        let gcs = Arc::new(FakeGcs::default());
        let asset_cache = filecache::Store::open(
            dir.path().join("asset-cache"),
            filecache::Options::default(),
        )
        .unwrap();
        let stager = AssetStager::new(asset_cache, None, Some(gcs.clone()), "test");
        let store: Option<Arc<dyn Store>> = match opts.store {
            StoreKind::None => None,
            StoreKind::Transfer => Some(transfer.clone()),
            StoreKind::Custom(f) => Some(f(transfer.clone(), objects.clone())),
        };
        let tracker_ctx = Ctx::background();
        let tracker = Tracker::new(
            tracker_ctx.clone(),
            paths.clone(),
            store,
            Arc::new(crate::clock::SystemClock),
            opts.tracker,
        );
        let egress = opts.egress_ca.clone();
        let refresher = Arc::new(SystemInfoRefresher::new(Source::new(
            Arc::new(move |_: &str| Ok(egress.lock().clone())),
            Vec::new(),
        )));
        let volumes = Arc::new(FakeVolumes::default());
        let mut plugins: HashMap<String, Arc<dyn VolumePlugin>> = HashMap::new();
        plugins.insert("mock-driver".into(), volumes.clone());
        let socket = dir.path().join("worker.sock");
        std::fs::write(&socket, b"").unwrap();
        let connector = Arc::new(Connector {
            worker: Mutex::new(Some(worker.clone())),
            socket: Mutex::new(socket),
        });
        let layer = dir.path().join("layers/app");
        std::fs::create_dir_all(layer.join("fs/bin")).unwrap();
        std::fs::write(layer.join("fs/bin/app"), b"app").unwrap();
        let registry = prometheus::Registry::new();
        let runtimes = Arc::new(RuntimeIndex::in_memory());
        let deps = KeeperDeps {
            paths: paths.clone(),
            workers: connector.clone(),
            images: Arc::new(FakeImages { layer }),
            assets: Arc::new(stager),
            transfer: transfer.clone(),
            downloader,
            runtimes: runtimes.clone(),
            instruments: Some(KeeperInstruments::new(&registry).unwrap()),
            volumes: Arc::new(VolumeManager::new(
                paths.clone(),
                plugins,
                Arc::new(NoVolumeDrivers),
            )),
            system_info: refresher.clone(),
            snapshots: tracker,
            validation: Operation::create(),
        };
        let keeper = Arc::new(Keeper::new(deps).with_checkpoint_timeout(opts.checkpoint_timeout));
        Node {
            dir,
            paths,
            keeper,
            worker,
            connector,
            objects,
            transfer,
            gcs,
            runtimes,
            refresher,
            volumes,
            registry,
            snapshot_cache,
            tracker_ctx,
            egress_ca: opts.egress_ca,
        }
    }

    /// The bundle the egress CA name resolves to; `None` is absent.
    pub(super) fn egress_ca(&self) -> &Arc<Mutex<Option<String>>> {
        &self.egress_ca
    }

    pub(super) fn tracker(&self) -> &Tracker {
        self.keeper.snapshots()
    }

    /// Starts the tracker's workers.
    pub(super) async fn start_tracker(&self) {
        self.tracker().start().await;
    }

    /// Stops the tracker's workers and waits for them, as a test cleanup.
    pub(super) async fn stop_tracker(&self) {
        self.tracker_ctx.cancel();
        let ctx = Ctx::background().with_timeout(Duration::from_secs(60));
        self.tracker()
            .wait(&ctx)
            .await
            .expect("tracker workers stop");
    }

    /// Makes the runtime `rec` describes one a worker on this node
    /// advertises, replacing what that worker advertised before.
    pub(super) fn advertise(&self, rec: &SandboxAssetsRecord) {
        self.runtimes
            .register(
                "test-worker-pod",
                &[AdvertisedClass {
                    class: rec.sandbox_class.clone(),
                    config: "test-config".into(),
                    hashed: vec![HashedVersion {
                        name: "v1".into(),
                        is_default: true,
                        rec: rec.clone(),
                        hash: rec.bundle_hash(),
                    }],
                }],
            )
            .unwrap();
    }

    /// Serves `tarball` as the release.
    pub(super) fn serve_release(&self, tarball: &[u8]) {
        *self.gcs.data.lock() = tarball.to_vec();
    }

    /// Takes the worker's socket away, as a deleted pod does.
    pub(super) fn remove_socket(&self) {
        *self.connector.socket.lock() = self.dir.path().join("gone/worker.sock");
    }

    /// Where an actor's staged runsc sits.
    pub(super) fn staged_runsc(&self, uid: &str) -> PathBuf {
        self.paths
            .sandbox_assets_dir(uid)
            .join(GVISOR_ASSET_NAME)
            .join("runsc")
    }

    /// Uploads a snapshot from `src`, as an earlier checkpoint would have.
    pub(super) async fn upload(&self, uri: &str, src: &Path, rec: &SandboxAssetsRecord) {
        let actor = crate::worker_jwt::TransferActor {
            hive: HIVE.into(),
            name: "bot".into(),
            uid: "9f1e0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a".into(),
        };
        self.transfer
            .upload_snapshot(
                &Ctx::background(),
                &actor,
                &hivekeep_core::snapshot::SnapshotUri::parse(uri).unwrap(),
                src,
                rec,
            )
            .await
            .unwrap();
    }

    /// The number of checkpoint durations recorded for `phase`.
    pub(super) fn checkpoint_samples(&self, phase: &str) -> u64 {
        histogram_samples(
            &self.registry,
            "hivekeep_actor_checkpoint_duration_seconds",
            phase,
        )
    }

    /// A fresh tracker over this node's state, as after a restart.
    pub(super) fn new_tracker(&self, opts: Options) -> Tracker {
        Tracker::new(
            self.tracker_ctx.child(),
            self.paths.clone(),
            None,
            Arc::new(crate::clock::SystemClock),
            opts,
        )
    }
}

/// The samples of `metric` with `snapshot_phase` = `phase`.
pub(super) fn histogram_samples(reg: &prometheus::Registry, metric: &str, phase: &str) -> u64 {
    reg.gather()
        .iter()
        .filter(|f| f.name() == metric)
        .flat_map(|f| f.get_metric().iter())
        .filter(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "hk_snapshot_phase" && l.value() == phase)
        })
        .map(|m| m.get_histogram().get_sample_count())
        .sum()
}

/// An actor running on a node's fake worker.
pub(super) struct PluginActor {
    pub(super) node: Node,
    pub(super) rec: SandboxAssetsRecord,
    pub(super) spec: pb::WorkloadSpec,
}

/// Runs [`UID`] on a node whose worker writes `checkpoint.img`.
pub(super) async fn run_plugin_actor(opts: NodeOpts) -> PluginActor {
    let start = opts.start_tracker;
    let node = Node::new(&[("checkpoint.img", "guest-memory")], opts);
    if start {
        node.start_tracker().await;
    }
    let tarball = release_tarball("runsc bytes");
    node.serve_release(&tarball);
    let rec = sandbox_record(&tarball);
    node.advertise(&rec);
    let a = PluginActor {
        node,
        rec,
        spec: pb::WorkloadSpec {
            containers: vec![pb::Container {
                name: "app".into(),
                image: IMAGE.into(),
                command: vec!["/bin/app".into()],
                ..Default::default()
            }],
            volumes: vec![],
        },
    };
    a.k()
        .run(&Ctx::background(), a.run_request())
        .await
        .expect("Run");
    a
}

impl PluginActor {
    pub(super) fn k(&self) -> &Arc<Keeper> {
        &self.node.keeper
    }

    pub(super) fn paths(&self) -> &NodePaths {
        &self.node.paths
    }

    pub(super) fn worker(&self) -> &Arc<FakeWorker> {
        &self.node.worker
    }

    pub(super) fn snapshot(&self, name: &str, generation: i32) -> pb::Snapshot {
        lifecycle_snapshot(UID, name, generation, pb::SnapshotFidelity::Memory)
    }

    pub(super) fn run_request(&self) -> pb::RunRequest {
        pb::RunRequest {
            hive: HIVE.into(),
            actor_name: ACTOR.into(),
            actor_uid: UID.into(),
            actor_template_hive: "default".into(),
            actor_template_name: "counter".into(),
            worker_pod_uid: POD.into(),
            sandbox_runtime: Some(runtime_for(&self.rec)),
            spec: Some(self.spec.clone()),
            ..Default::default()
        }
    }

    pub(super) fn checkpoint_request(
        &self,
        snap: &pb::Snapshot,
        mode: pb::UploadMode,
    ) -> pb::CheckpointRequest {
        pb::CheckpointRequest {
            hive: HIVE.into(),
            actor_name: ACTOR.into(),
            actor_uid: UID.into(),
            actor_template_hive: "default".into(),
            actor_template_name: "counter".into(),
            worker_pod_uid: POD.into(),
            spec: Some(self.spec.clone()),
            snapshot: Some(snap.clone()),
            upload_config: Some(pb::UploadConfig { mode: mode as i32 }),
        }
    }

    pub(super) async fn checkpoint(
        &self,
        snap: &pb::Snapshot,
        mode: pb::UploadMode,
    ) -> Result<pb::CheckpointResponse, Status> {
        self.k()
            .checkpoint(&Ctx::background(), self.checkpoint_request(snap, mode))
            .await
    }

    /// Checkpoints the actor as `name`, deferred, and returns the snapshot.
    pub(super) async fn checkpointed(&self, name: &str, generation: i32) -> pb::Snapshot {
        let snap = self.snapshot(name, generation);
        self.checkpoint(&snap, pb::UploadMode::Deferred)
            .await
            .expect("Checkpoint");
        snap
    }

    pub(super) fn restore_request(&self, snap: &pb::Snapshot) -> pb::RestoreRequest {
        pb::RestoreRequest {
            hive: HIVE.into(),
            actor_name: ACTOR.into(),
            actor_uid: UID.into(),
            actor_template_hive: "default".into(),
            actor_template_name: "counter".into(),
            worker_pod_uid: POD.into(),
            sandbox_runtime: Some(runtime_for(&self.rec)),
            spec: Some(self.spec.clone()),
            snapshot: Some(snap.clone()),
            ..Default::default()
        }
    }

    /// Resumes the actor from its local snapshot `name`, which it must
    /// restore in place.
    pub(super) async fn restore_local(&self, name: &str, generation: i32) {
        self.k()
            .restore(
                &Ctx::background(),
                self.restore_request(&self.snapshot(name, generation)),
            )
            .await
            .expect("Restore");
        assert!(
            self.worker().preserved(),
            "Restore of {name} read object storage, want its local copy restored in place"
        );
    }

    pub(super) fn promote_request(&self, snap: &pb::Snapshot) -> pb::PromoteSnapshotRequest {
        pb::PromoteSnapshotRequest {
            hive: HIVE.into(),
            actor_name: ACTOR.into(),
            actor_uid: UID.into(),
            actor_template_hive: "default".into(),
            actor_template_name: "counter".into(),
            snapshot: Some(snap.clone()),
        }
    }

    pub(super) async fn promote(&self, snap: &pb::Snapshot) -> pb::ActorSnapshotState {
        self.k()
            .promote_snapshot(&Ctx::background(), self.promote_request(snap))
            .await
            .expect("PromoteSnapshot")
            .state
            .unwrap_or_default()
    }

    pub(super) fn terminate_request(&self) -> pb::TerminateRequest {
        pb::TerminateRequest {
            hive: HIVE.into(),
            actor_name: ACTOR.into(),
            actor_uid: UID.into(),
            actor_template_hive: "default".into(),
            actor_template_name: "counter".into(),
            worker_pod_uid: POD.into(),
            spec: Some(self.spec.clone()),
        }
    }

    pub(super) fn detach_request(&self) -> pb::DetachActorRequest {
        pb::DetachActorRequest {
            hive: HIVE.into(),
            actor_name: ACTOR.into(),
            actor_uid: UID.into(),
        }
    }

    /// Whether the object store holds the manifest of snapshot `name`.
    pub(super) fn uploaded(&self, name: &str) -> bool {
        self.node
            .objects
            .has_manifest(&self.snapshot(name, 1).object.unwrap().snapshot_uri)
    }

    /// Gives the spec the external volumes `ids`, served by the node's fake
    /// driver.
    pub(super) fn with_external_volumes(&mut self, ids: &[&str]) {
        self.spec.volumes = ids
            .iter()
            .map(|id| pb::Volume {
                name: format!("ext-{id}"),
                external: Some(pb::ExternalVolumeSource {
                    storage_volume_id: id.to_string(),
                    volume_type: "mock-driver".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect();
    }
}

/// The uuid of `r`'s snapshot, or "".
pub(super) fn uuid(r: Option<&pb::SnapshotReport>) -> String {
    r.and_then(|r| r.snapshot.as_ref())
        .map(|s| s.snapshot_uuid.clone())
        .unwrap_or_default()
}

/// The snapshot of `r`.
pub(super) fn snap_of(r: Option<&pb::SnapshotReport>) -> Option<pb::Snapshot> {
    r.and_then(|r| r.snapshot.clone())
}

/// A tracker store gated on its own channels, made from the transfer.
pub(super) fn gating(
    objects: bool,
    canceled: Option<mpsc::Sender<()>>,
) -> (StoreKind, mpsc::Receiver<()>, Latch) {
    let (tx, rx) = mpsc::channel(1);
    let release = Latch::new();
    let r2 = release.clone();
    let kind = StoreKind::Custom(Box::new(move |transfer, objs| {
        Arc::new(GatingStore {
            inner: transfer,
            objects: objects.then_some(objs),
            started: tx,
            release: r2,
            canceled,
        }) as Arc<dyn Store>
    }));
    (kind, rx, release)
}

/// Tracker options with background uploads due after `upload` and
/// detaches after an hour.
pub(super) fn upload_after(upload: Duration) -> Options {
    Options {
        upload_delay: upload,
        detach_delay: Duration::from_secs(3600),
        ..Default::default()
    }
}

pub(super) fn reporting() -> Options {
    Options {
        reporter: Some(Arc::new(AckReporter)),
        report_startup_jitter: Duration::ZERO,
        ..Default::default()
    }
}

pub(super) use snapshot_tracker::prune_local_checkpoints;

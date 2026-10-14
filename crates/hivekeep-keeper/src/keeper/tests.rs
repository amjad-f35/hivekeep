//! Lifecycle tests against a fake worker and a local object store.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_core::validation::rules::Operation;
use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::worker::v1 as wpb;
use parking_lot::Mutex;
use tonic::{Code, Status};

use super::*;
use crate::assets::{AssetEntry, SandboxAssetsRecord, MANIFEST_NAME};
use crate::objectstore::LocalObjectStore;
use crate::oci::NoImageCache;
use crate::runtime_index::{AdvertisedClass, HashedVersion, ATTR_SANDBOX_BUNDLE_HASH};
use crate::snapshot_tracker::{self, Options};
use crate::system_info::SystemInfoRefresher;
use crate::trustbundle::Source;
use crate::volumes::{NoVolumeDrivers, VolumeManager};
use crate::worker_dialer::{WorkerApi, WorkerConnector};
use crate::worker_jwt::tests::FakeWorkers;
use crate::worker_jwt::WorkerActorJwts;

const UID: &str = "1b4e28ba-2fa1-11d2-883f-0016d3cca427";
const POD: &str = "422938ba-8860-4983-a25d-d6bcb0a69d4e";

#[derive(Default)]
struct FakeWorker {
    calls: Mutex<Vec<String>>,
    restores: Mutex<Vec<wpb::RestoreWorkloadRequest>>,
    restore_errs: Mutex<VecDeque<Status>>,
    terminate_err: Mutex<Option<Status>>,
}

#[async_trait]
impl WorkerApi for FakeWorker {
    async fn run_workload(
        &self,
        _: &Ctx,
        req: wpb::RunWorkloadRequest,
    ) -> Result<wpb::RunWorkloadResponse, Status> {
        self.calls.lock().push(format!("run:{}", req.runsc_path));
        Ok(wpb::RunWorkloadResponse { worker_epoch: 3 })
    }
    async fn checkpoint_workload(
        &self,
        _: &Ctx,
        req: wpb::CheckpointWorkloadRequest,
    ) -> Result<wpb::CheckpointWorkloadResponse, Status> {
        self.calls.lock().push("checkpoint".into());
        let dir = PathBuf::from(req.actor_dirs.unwrap().checkpoint_dir);
        std::fs::create_dir_all(dir.join("fs")).unwrap();
        std::fs::write(dir.join("mem"), b"guest memory").unwrap();
        std::fs::write(dir.join("fs/img"), b"rootfs").unwrap();
        Ok(wpb::CheckpointWorkloadResponse {
            snapshot_files: vec!["mem".into(), "fs/img".into()],
            data_snapshot_files: vec!["fs/img".into()],
        })
    }
    async fn restore_workload(
        &self,
        _: &Ctx,
        req: wpb::RestoreWorkloadRequest,
    ) -> Result<wpb::RestoreWorkloadResponse, Status> {
        self.calls.lock().push("restore".into());
        self.restores.lock().push(req);
        if let Some(e) = self.restore_errs.lock().pop_front() {
            return Err(e);
        }
        Ok(wpb::RestoreWorkloadResponse { worker_epoch: 4 })
    }
    async fn terminate_workload(
        &self,
        _: &Ctx,
        _: wpb::TerminateWorkloadRequest,
    ) -> Result<(), Status> {
        self.calls.lock().push("terminate".into());
        match self.terminate_err.lock().clone() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
    async fn get_active_workload_stats(
        &self,
        _: &Ctx,
    ) -> Result<wpb::GetActiveWorkloadStatsResponse, Status> {
        Ok(Default::default())
    }
}

struct Connector {
    worker: Arc<FakeWorker>,
    paths: Arc<NodePaths>,
}

#[async_trait]
impl WorkerConnector for Connector {
    async fn connect(&self, _: &str) -> Result<Arc<dyn WorkerApi>, Status> {
        Ok(self.worker.clone())
    }
    fn socket_path(&self, pod: &str) -> PathBuf {
        self.paths.worker_socket(pod)
    }
}

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
        let mut out = BTreeMap::new();
        for name in rec.assets.keys() {
            std::fs::create_dir_all(dst.join(name)).unwrap();
            let p = dst.join(name).join("runsc");
            std::fs::write(&p, b"bin").unwrap();
            out.insert(name.clone(), p.to_string_lossy().into_owned());
        }
        Ok(out)
    }
}

struct Env {
    _dir: tempfile::TempDir,
    paths: Arc<NodePaths>,
    keeper: Arc<Keeper>,
    worker: Arc<FakeWorker>,
    objects: Arc<LocalObjectStore>,
    runtime: pb::ActorSandboxRuntime,
}

fn runtime_record() -> SandboxAssetsRecord {
    let mut rec = SandboxAssetsRecord {
        sandbox_class: "gvisor".into(),
        ..Default::default()
    };
    rec.assets.insert(
        "gvisor".into(),
        AssetEntry {
            url: "gs://gvisor/release.tar.zst".into(),
            sha256: "ab".repeat(32),
        },
    );
    rec
}

fn env() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let paths = Arc::new(NodePaths::new(dir.path().join("node")));
    let objects = Arc::new(LocalObjectStore::new(dir.path().join("objects")));
    let jwts = Arc::new(WorkerActorJwts::new(
        Arc::new(FakeWorkers::default()),
        Arc::new(crate::clock::SystemClock),
    ));
    let transfer = Arc::new(SnapshotTransfer::new(
        objects.clone(),
        jwts,
        paths.clone(),
        "boot-1".into(),
    ));
    let downloader = Arc::new(SnapshotDownloader::new(transfer.clone(), None));
    let rec = runtime_record();
    let hash = rec.bundle_hash();
    let runtimes = Arc::new(RuntimeIndex::in_memory());
    runtimes
        .register(
            POD,
            &[AdvertisedClass {
                class: "gvisor".into(),
                config: "cfg".into(),
                hashed: vec![HashedVersion {
                    name: "v1".into(),
                    is_default: true,
                    rec,
                    hash: hash.clone(),
                }],
            }],
        )
        .unwrap();
    let tracker = snapshot_tracker::Tracker::new(
        Ctx::background(),
        paths.clone(),
        Some(transfer.clone() as Arc<dyn snapshot_tracker::Store>),
        Arc::new(crate::clock::SystemClock),
        Options::default(),
    );
    let worker = Arc::new(FakeWorker::default());
    let deps = KeeperDeps {
        paths: paths.clone(),
        workers: Arc::new(Connector {
            worker: worker.clone(),
            paths: paths.clone(),
        }),
        images: Arc::new(NoImageCache),
        assets: Arc::new(FakeAssets),
        transfer,
        downloader,
        runtimes,
        instruments: None,
        volumes: Arc::new(VolumeManager::new(
            paths.clone(),
            HashMap::new(),
            Arc::new(NoVolumeDrivers),
        )),
        system_info: Arc::new(SystemInfoRefresher::new(Source::new(
            Arc::new(|_: &str| Ok(None)),
            Vec::new(),
        ))),
        snapshots: tracker,
        validation: Operation::create(),
    };
    let runtime = pb::ActorSandboxRuntime {
        sandbox_class: "gvisor".into(),
        version: Some(pb::VersionedSandboxCompat {
            schema_version: "1".into(),
            attributes: vec![pb::AttributeEntry {
                key: ATTR_SANDBOX_BUNDLE_HASH.into(),
                value: hash,
            }],
        }),
    };
    Env {
        _dir: dir,
        paths,
        keeper: Arc::new(Keeper::new(deps)),
        worker,
        objects,
        runtime,
    }
}

fn spec() -> pb::WorkloadSpec {
    pb::WorkloadSpec {
        containers: vec![],
        volumes: vec![pb::Volume {
            name: "data".into(),
            durable_dir: Some(pb::DurableDirVolumeSource {}),
            ..Default::default()
        }],
    }
}

fn snapshot(uuid: &str, generation: i32) -> pb::Snapshot {
    pb::Snapshot {
        snapshot_uuid: uuid.into(),
        generation,
        fidelity: pb::SnapshotFidelity::Memory as i32,
        object: Some(pb::ObjectStorage {
            snapshot_uri: format!("gs://bucket/hives/demo/actors/{UID}/snapshots/{uuid}"),
            sharing: 0,
        }),
    }
}

fn run_req(e: &Env) -> pb::RunRequest {
    pb::RunRequest {
        worker_pod_uid: POD.into(),
        hive: "demo".into(),
        actor_name: "counter".into(),
        actor_uid: UID.into(),
        spec: Some(spec()),
        sandbox_runtime: Some(e.runtime.clone()),
        ..Default::default()
    }
}

fn checkpoint_req(uuid: &str, generation: i32, mode: pb::UploadMode) -> pb::CheckpointRequest {
    pb::CheckpointRequest {
        worker_pod_uid: POD.into(),
        hive: "demo".into(),
        actor_name: "counter".into(),
        actor_uid: UID.into(),
        spec: Some(spec()),
        snapshot: Some(snapshot(uuid, generation)),
        upload_config: Some(pb::UploadConfig { mode: mode as i32 }),
        ..Default::default()
    }
}

fn restore_req(e: &Env, uuid: &str, generation: i32) -> pb::RestoreRequest {
    pb::RestoreRequest {
        worker_pod_uid: POD.into(),
        hive: "demo".into(),
        actor_name: "counter".into(),
        actor_uid: UID.into(),
        spec: Some(spec()),
        snapshot: Some(snapshot(uuid, generation)),
        sandbox_runtime: Some(e.runtime.clone()),
        ..Default::default()
    }
}

fn uuid_of(r: &Option<pb::SnapshotReport>) -> String {
    r.as_ref()
        .and_then(|r| r.snapshot.as_ref())
        .map(|s| s.snapshot_uuid.clone())
        .unwrap_or_default()
}

#[tokio::test]
async fn run_validates_and_resolves_the_runtime_first() {
    let e = env();
    let mut bad = run_req(&e);
    bad.actor_uid = "not-a-uuid".into();
    assert_eq!(
        e.keeper
            .run(&Ctx::background(), bad)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let mut unknown = run_req(&e);
    unknown
        .sandbox_runtime
        .as_mut()
        .unwrap()
        .version
        .as_mut()
        .unwrap()
        .attributes[0]
        .value = "sha256:nope".into();
    assert_eq!(
        e.keeper
            .run(&Ctx::background(), unknown)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert!(
        !e.paths.actor_dir(UID).exists(),
        "an invalid request changes nothing"
    );
}

#[tokio::test]
async fn run_stages_assets_and_starts_the_workload() {
    let e = env();
    let resp = e.keeper.run(&Ctx::background(), run_req(&e)).await.unwrap();
    assert_eq!(resp.worker_epoch, 3);
    let runsc = e.paths.sandbox_assets_dir(UID).join("gvisor/runsc");
    assert_eq!(
        *e.worker.calls.lock(),
        vec![format!("run:{}", runsc.display())]
    );
    assert!(runsc.exists());
    assert!(!e.paths.sandbox_assets_staging_dir(UID).exists());
    assert!(e.paths.actor_sandbox_assets_file(UID).exists());
    assert!(e.paths.durable_dir_volume_mount_point(UID, "data").is_dir());
}

#[tokio::test]
async fn deferred_checkpoint_keeps_a_local_snapshot_and_restores_from_it() {
    let e = env();
    let ctx = Ctx::background();
    e.keeper.run(&ctx, run_req(&e)).await.unwrap();
    let resp = e
        .keeper
        .checkpoint(&ctx, checkpoint_req("s1", 1, pb::UploadMode::Deferred))
        .await
        .unwrap();
    let st = resp.state.unwrap();
    assert_eq!(uuid_of(&st.latest_local), "s1");
    assert!(st.latest_durable.is_none());
    let local = e.paths.local_snapshot_dir(UID, "s1");
    assert_eq!(std::fs::read(local.join("mem")).unwrap(), b"guest memory");
    let manifest =
        SandboxAssetsRecord::parse(&std::fs::read(local.join(MANIFEST_NAME)).unwrap()).unwrap();
    assert_eq!(manifest.boot_id, "boot-1");
    assert_eq!(manifest.fidelity, "memory");
    assert_eq!(manifest.data_snapshot_files, vec!["fs/img"]);
    assert_eq!(
        std::fs::read_dir(e.paths.checkpoint_state_dir(UID))
            .unwrap()
            .count(),
        0,
        "the checkpoint dir is reset"
    );

    // A retry answers from the committed snapshot without the worker.
    let again = e
        .keeper
        .checkpoint(&ctx, checkpoint_req("s1", 1, pb::UploadMode::Deferred))
        .await
        .unwrap();
    assert_eq!(uuid_of(&again.state.unwrap().latest_local), "s1");
    assert_eq!(
        e.worker
            .calls
            .lock()
            .iter()
            .filter(|c| *c == "checkpoint")
            .count(),
        1
    );
    // A stale generation is refused.
    let err = e
        .keeper
        .checkpoint(&ctx, checkpoint_req("s0", 0, pb::UploadMode::Deferred))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let resp = e
        .keeper
        .restore(&ctx, restore_req(&e, "s1", 1))
        .await
        .unwrap();
    assert_eq!(resp.worker_epoch, 4);
    let r = e.worker.restores.lock()[0].clone();
    assert!(r.preserve_restore_dir, "restored in place");
    assert_eq!(r.actor_dirs.unwrap().restore_dir, local.to_string_lossy());
}

#[tokio::test]
async fn immediate_checkpoint_uploads_and_restores_from_object_storage() {
    let e = env();
    let ctx = Ctx::background();
    e.keeper.run(&ctx, run_req(&e)).await.unwrap();
    let st = e
        .keeper
        .checkpoint(&ctx, checkpoint_req("s1", 1, pb::UploadMode::Immediate))
        .await
        .unwrap()
        .state
        .unwrap();
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert_eq!(uuid_of(&st.latest_local), "s1", "a local copy is kept too");
    let remote = e
        .objects
        .dir_for(&snapshot("s1", 1).object.unwrap().snapshot_uri)
        .unwrap();
    assert!(remote.join("mem").exists() && remote.join(MANIFEST_NAME).exists());
    let uploaded =
        SandboxAssetsRecord::parse(&std::fs::read(remote.join(MANIFEST_NAME)).unwrap()).unwrap();
    assert!(uploaded.boot_id.is_empty());

    // Without the local copy, the restore reads object storage.
    std::fs::remove_dir_all(e.paths.local_snapshot_dir(UID, "s1")).unwrap();
    e.keeper
        .restore(&ctx, restore_req(&e, "s1", 1))
        .await
        .unwrap();
    let r = e.worker.restores.lock()[0].clone();
    assert!(!r.preserve_restore_dir);
    let restore_dir = e.paths.restore_state_dir(UID);
    assert_eq!(
        r.actor_dirs.unwrap().restore_dir,
        restore_dir.to_string_lossy()
    );
    assert_eq!(
        std::fs::read(restore_dir.join("fs/img")).unwrap(),
        b"rootfs"
    );
}

#[tokio::test]
async fn restore_without_any_copy_is_not_found_and_cleans_up() {
    let e = env();
    let err = e
        .keeper
        .restore(&Ctx::background(), restore_req(&e, "s9", 9))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    assert!(!e.paths.restore_state_dir(UID).exists());
    assert!(!e.paths.sandbox_assets_dir(UID).exists());
}

#[tokio::test]
async fn repeated_retryable_restore_failures_crash_the_actor() {
    let e = env();
    let ctx = Ctx::background();
    e.keeper.run(&ctx, run_req(&e)).await.unwrap();
    e.keeper
        .checkpoint(&ctx, checkpoint_req("s1", 1, pb::UploadMode::Deferred))
        .await
        .unwrap();
    for _ in 0..3 {
        e.worker
            .restore_errs
            .lock()
            .push_back(Status::unavailable("guest not ready"));
    }
    assert_eq!(
        e.keeper
            .restore(&ctx, restore_req(&e, "s1", 1))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    assert_eq!(
        e.keeper
            .restore(&ctx, restore_req(&e, "s1", 1))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    assert_eq!(
        e.keeper
            .restore(&ctx, restore_req(&e, "s1", 1))
            .await
            .unwrap_err()
            .code(),
        Code::Internal
    );
}

#[tokio::test]
async fn promote_uploads_a_local_snapshot_and_detach_releases_it() {
    let e = env();
    let ctx = Ctx::background();
    e.keeper.run(&ctx, run_req(&e)).await.unwrap();
    e.keeper
        .checkpoint(&ctx, checkpoint_req("s1", 1, pb::UploadMode::Deferred))
        .await
        .unwrap();
    let st = e
        .keeper
        .promote_snapshot(
            &ctx,
            pb::PromoteSnapshotRequest {
                hive: "demo".into(),
                actor_name: "counter".into(),
                actor_uid: UID.into(),
                snapshot: Some(snapshot("s1", 1)),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .state
        .unwrap();
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert!(st.latest_durable.unwrap().durable_complete_time.is_some());
    let st = e
        .keeper
        .detach_actor(
            &ctx,
            pb::DetachActorRequest {
                hive: "demo".into(),
                actor_name: "counter".into(),
                actor_uid: UID.into(),
            },
        )
        .await
        .unwrap()
        .state
        .unwrap();
    assert!(st.latest_local.is_none());
    assert!(!e.paths.actor_snapshot_dir(UID).exists());
}

#[tokio::test]
async fn terminate_tears_down_and_reclaims() {
    let e = env();
    let ctx = Ctx::background();
    e.keeper.run(&ctx, run_req(&e)).await.unwrap();
    e.keeper
        .checkpoint(&ctx, checkpoint_req("s1", 1, pb::UploadMode::Deferred))
        .await
        .unwrap();
    // The worker is alive: its socket exists.
    std::fs::create_dir_all(e.paths.worker_dir(POD)).unwrap();
    std::fs::write(e.paths.worker_socket(POD), b"").unwrap();
    *e.worker.terminate_err.lock() = Some(Status::not_found("no workload"));
    let req = pb::TerminateRequest {
        worker_pod_uid: POD.into(),
        hive: "demo".into(),
        actor_name: "counter".into(),
        actor_uid: UID.into(),
        spec: Some(spec()),
        ..Default::default()
    };
    e.keeper.terminate(&ctx, req.clone()).await.unwrap();
    assert!(e.worker.calls.lock().contains(&"terminate".to_string()));
    assert!(!e.paths.actor_dir(UID).exists());
    assert!(!e.paths.actor_snapshot_dir(UID).exists());
    // A retry, and one naming no worker, succeed too.
    e.keeper.terminate(&ctx, req.clone()).await.unwrap();
    let mut no_worker = req;
    no_worker.worker_pod_uid.clear();
    e.keeper.terminate(&ctx, no_worker).await.unwrap();
}

#[tokio::test]
async fn terminate_fails_when_a_live_worker_refuses() {
    let e = env();
    let ctx = Ctx::background();
    e.keeper.run(&ctx, run_req(&e)).await.unwrap();
    std::fs::create_dir_all(e.paths.worker_dir(POD)).unwrap();
    std::fs::write(e.paths.worker_socket(POD), b"").unwrap();
    *e.worker.terminate_err.lock() = Some(Status::internal("busy"));
    let req = pb::TerminateRequest {
        worker_pod_uid: POD.into(),
        hive: "demo".into(),
        actor_name: "counter".into(),
        actor_uid: UID.into(),
        ..Default::default()
    };
    assert_eq!(
        e.keeper.terminate(&ctx, req).await.unwrap_err().code(),
        Code::Internal
    );
    assert!(
        e.paths.actor_dir(UID).exists(),
        "a live sandbox's directories stay"
    );
}

#[test]
fn restore_failure_streaks() {
    assert!(ends_restore_failure_streak::<()>(&Ok(())));
    assert!(ends_restore_failure_streak::<()>(&Err(Status::internal(
        "x"
    ))));
    assert!(!ends_restore_failure_streak::<()>(&Err(
        Status::unavailable("x")
    )));
    assert!(!ends_restore_failure_streak::<()>(&Err(
        Status::deadline_exceeded("x")
    )));
}

#[test]
fn local_snapshot_checks() {
    let d = tempfile::tempdir().unwrap();
    assert!(matches!(
        read_local_snapshot(d.path(), "b"),
        Err(LocalSnapshotError::Missing)
    ));
    let rec = SandboxAssetsRecord {
        snapshot_files: vec!["mem".into()],
        boot_id: "b".into(),
        ..Default::default()
    };
    std::fs::write(d.path().join(MANIFEST_NAME), rec.to_json()).unwrap();
    assert!(
        matches!(
            read_local_snapshot(d.path(), "b"),
            Err(LocalSnapshotError::Unusable(_))
        ),
        "a listed file is missing"
    );
    std::fs::write(d.path().join("mem"), b"m").unwrap();
    assert!(read_local_snapshot(d.path(), "b").is_ok());
    assert!(matches!(
        read_local_snapshot(d.path(), "other-boot"),
        Err(LocalSnapshotError::Unusable(_))
    ));
}

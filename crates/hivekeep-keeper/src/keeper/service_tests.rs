//! Keeper RPC boundaries, metrics and request logging, against a fake
//! worker and a recording object store.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_core::validation::rules::Operation;
use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::keeper::v1::keeper_server::Keeper as _;
use hivekeep_proto::worker::v1 as wpb;
use prometheus::Registry;
use tonic::{Code, Request, Status};

use super::*;
use crate::assets::{AssetEntry, SandboxAssetsRecord};
use crate::keeper_metrics::behavior_tests::{family, phase_sums, CHECKPOINT_METRIC};
use crate::labels::phase;
use crate::oci::NoImageCache;
use crate::runtime_index::{AdvertisedClass, HashedVersion, ATTR_SANDBOX_BUNDLE_HASH};
use crate::service_stubs::{Minter, RecordingStore};
use crate::snapshot_tracker::{self, Options};
use crate::system_info::SystemInfoRefresher;
use crate::trustbundle::Source;
use crate::volumes::{NoVolumeDrivers, VolumeManager};
use crate::worker_dialer::{WorkerApi, WorkerConnector};
use crate::worker_jwt::WorkerActorJwts;

const UID: &str = "123e4567-e89b-12d3-a456-426614174000";
const POD: &str = "422938ba-8860-4983-a25d-d6bcb0a69d4e";

/// A worker whose checkpoint writes two files.
struct Worker;

#[async_trait]
impl WorkerApi for Worker {
    async fn run_workload(
        &self,
        _: &Ctx,
        _: wpb::RunWorkloadRequest,
    ) -> Result<wpb::RunWorkloadResponse, Status> {
        Ok(wpb::RunWorkloadResponse { worker_epoch: 1 })
    }
    async fn checkpoint_workload(
        &self,
        _: &Ctx,
        req: wpb::CheckpointWorkloadRequest,
    ) -> Result<wpb::CheckpointWorkloadResponse, Status> {
        let dir = PathBuf::from(req.actor_dirs.unwrap().checkpoint_dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("mem"), b"guest memory").unwrap();
        std::fs::write(dir.join("data.tar"), b"data").unwrap();
        Ok(wpb::CheckpointWorkloadResponse {
            snapshot_files: vec!["mem".into(), "data.tar".into()],
            data_snapshot_files: vec!["data.tar".into()],
        })
    }
    async fn restore_workload(
        &self,
        _: &Ctx,
        _: wpb::RestoreWorkloadRequest,
    ) -> Result<wpb::RestoreWorkloadResponse, Status> {
        Err(Status::unavailable("asset fetch failed"))
    }
    async fn terminate_workload(
        &self,
        _: &Ctx,
        _: wpb::TerminateWorkloadRequest,
    ) -> Result<(), Status> {
        Ok(())
    }
    async fn get_active_workload_stats(
        &self,
        _: &Ctx,
    ) -> Result<wpb::GetActiveWorkloadStatsResponse, Status> {
        Ok(Default::default())
    }
}

struct Connector(Arc<NodePaths>);

#[async_trait]
impl WorkerConnector for Connector {
    async fn connect(&self, _: &str) -> Result<Arc<dyn WorkerApi>, Status> {
        Ok(Arc::new(Worker))
    }
    fn socket_path(&self, pod: &str) -> PathBuf {
        self.0.worker_socket(pod)
    }
}

/// Stages each asset as a small file.
struct Assets;

#[async_trait]
impl AssetFetcher for Assets {
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
    keeper: Arc<Keeper>,
    registry: Registry,
    runtime: pb::ActorSandboxRuntime,
}

fn env(put_err: Option<Status>) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let paths = Arc::new(NodePaths::new(dir.path().join("node")));
    let mut store = RecordingStore::new(&dir.path().join("objects"));
    store.put_err = put_err;
    let jwts = Arc::new(WorkerActorJwts::new(
        Arc::new(Minter::new(
            "jwt",
            chrono::Utc::now() + chrono::Duration::hours(1),
        )),
        Arc::new(crate::clock::SystemClock),
    ));
    let transfer = Arc::new(SnapshotTransfer::new(
        Arc::new(store),
        jwts,
        paths.clone(),
        "boot-1".into(),
    ));
    let downloader = Arc::new(SnapshotDownloader::new(transfer.clone(), None));
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
    let registry = Registry::new();
    let deps = KeeperDeps {
        paths: paths.clone(),
        workers: Arc::new(Connector(paths.clone())),
        images: Arc::new(NoImageCache),
        assets: Arc::new(Assets),
        transfer,
        downloader,
        runtimes,
        instruments: Some(KeeperInstruments::new(&registry).unwrap()),
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
        keeper: Arc::new(Keeper::new(deps)),
        registry,
        runtime,
    }
}

fn snapshot(uuid: &str) -> pb::Snapshot {
    pb::Snapshot {
        snapshot_uuid: uuid.into(),
        generation: 1,
        fidelity: pb::SnapshotFidelity::Memory as i32,
        object: Some(pb::ObjectStorage {
            snapshot_uri: format!("gs://bucket/hives/demo/actors/{UID}/snapshots/{uuid}"),
            sharing: 0,
        }),
    }
}

fn data_spec() -> pb::WorkloadSpec {
    pb::WorkloadSpec {
        containers: vec![],
        volumes: vec![pb::Volume {
            name: "data".into(),
            durable_dir: Some(pb::DurableDirVolumeSource {}),
            ..Default::default()
        }],
    }
}

/// A local snapshot file must be a regular file reached without symlinks.
#[cfg(unix)]
#[test]
fn check_local_snapshot_files_rejects_symlink() {
    let d = tempfile::tempdir().unwrap();
    let snap = d.path().join("local-checkpoint/pause-1");
    std::fs::create_dir_all(&snap).unwrap();
    std::fs::write(snap.join("config.json"), b"{}").unwrap();
    let outside = d.path().join("outside");
    std::fs::write(&outside, b"secret").unwrap();
    std::os::unix::fs::symlink(&outside, snap.join("checkpoint.img")).unwrap();
    assert!(check_local_snapshot_files(&snap, &["config.json".into()]).is_ok());
    assert!(
        check_local_snapshot_files(&snap, &["config.json".into(), "checkpoint.img".into()])
            .is_err(),
        "accepted a symlink"
    );
    assert!(check_local_snapshot_files(&snap, &["missing".into()]).is_err());
}

/// Each RPC validates its path inputs before touching anything: a
/// traversal in the worker pod UID is InvalidArgument.
#[tokio::test]
async fn rpc_boundaries_reject() {
    let e = env(None);
    let bad = "../escape";
    let spec = pb::WorkloadSpec {
        containers: vec![pb::Container {
            name: "worker".into(),
            image: "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
            ..Default::default()
        }],
        volumes: vec![],
    };
    let ctx = Ctx::background();
    let k = &e.keeper;
    let run = k
        .run(
            &ctx,
            pb::RunRequest {
                hive: "demo".into(),
                actor_name: "counter-1".into(),
                actor_uid: UID.into(),
                worker_pod_uid: bad.into(),
                spec: Some(spec.clone()),
                sandbox_runtime: Some(e.runtime.clone()),
                ..Default::default()
            },
        )
        .await;
    let checkpoint = k
        .checkpoint(
            &ctx,
            pb::CheckpointRequest {
                hive: "demo".into(),
                actor_name: "counter-1".into(),
                actor_uid: UID.into(),
                worker_pod_uid: bad.into(),
                spec: Some(spec.clone()),
                snapshot: Some(snapshot("snap-1")),
                upload_config: Some(pb::UploadConfig {
                    mode: pb::UploadMode::Immediate as i32,
                }),
                ..Default::default()
            },
        )
        .await;
    let restore = k
        .restore(
            &ctx,
            pb::RestoreRequest {
                hive: "demo".into(),
                actor_name: "counter-1".into(),
                actor_uid: UID.into(),
                worker_pod_uid: bad.into(),
                spec: Some(spec.clone()),
                snapshot: Some(snapshot("snap-1")),
                sandbox_runtime: Some(e.runtime.clone()),
                ..Default::default()
            },
        )
        .await;
    let terminate = k
        .terminate(
            &ctx,
            pb::TerminateRequest {
                hive: "demo".into(),
                actor_name: "counter-1".into(),
                actor_uid: UID.into(),
                actor_template_hive: "default".into(),
                actor_template_name: "template".into(),
                worker_pod_uid: bad.into(),
                spec: Some(spec),
            },
        )
        .await;
    for (rpc, res) in [
        ("Run", run.err()),
        ("Checkpoint", checkpoint.err()),
        ("Restore", restore.err()),
        ("Terminate", terminate.err()),
    ] {
        let err = res.unwrap_or_else(|| panic!("{rpc} accepted an invalid worker pod UID"));
        assert_eq!(err.code(), Code::InvalidArgument, "{rpc}: {err:?}");
    }
}

/// The node cleanup a Checkpoint does once its snapshot commits lands in
/// the cleanup phase; a failed upload returns before it; the phases fit
/// in the total.
#[tokio::test]
async fn checkpoint_records_cleanup() {
    for (name, put_err, want_cleanup) in [
        ("succeeded", None, true),
        (
            "upload failed",
            Some(Status::unavailable("object store unavailable")),
            false,
        ),
    ] {
        let failing = put_err.is_some();
        let e = env(put_err);
        let ctx = Ctx::background();
        e.keeper
            .run(
                &ctx,
                pb::RunRequest {
                    worker_pod_uid: POD.into(),
                    hive: "demo".into(),
                    actor_name: "counter".into(),
                    actor_uid: UID.into(),
                    spec: Some(data_spec()),
                    sandbox_runtime: Some(e.runtime.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let res = e
            .keeper
            .checkpoint(
                &ctx,
                pb::CheckpointRequest {
                    worker_pod_uid: POD.into(),
                    hive: "demo".into(),
                    actor_name: "counter".into(),
                    actor_uid: UID.into(),
                    spec: Some(data_spec()),
                    snapshot: Some(snapshot("suspend-snap-1")),
                    upload_config: Some(pb::UploadConfig {
                        mode: pb::UploadMode::Immediate as i32,
                    }),
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(res.is_err(), failing, "{name}: {res:?}");
        let sums = phase_sums(&family(&e.registry, CHECKPOINT_METRIC));
        let cleanup = sums.get(phase::CLEANUP).copied().unwrap_or(0.0);
        assert_eq!(cleanup > 0.0, want_cleanup, "{name}: cleanup = {cleanup}s");
        let parts: f64 = sums
            .iter()
            .filter(|(k, _)| k.as_str() != phase::TOTAL)
            .map(|(_, v)| v)
            .sum();
        assert!(
            parts <= sums[phase::TOTAL],
            "{name}: phases {parts}s exceed the total"
        );
    }
}

/// A shared buffer the log subscriber writes into.
#[derive(Clone, Default)]
struct LogBuf(Arc<parking_lot::Mutex<Vec<u8>>>);

impl std::io::Write for LogBuf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = LogBuf;
    fn make_writer(&'a self) -> LogBuf {
        self.clone()
    }
}

/// Run and Restore with an environment variable value go through the
/// service, and nothing the keeper logs, at any level, holds the value.
#[tokio::test]
async fn request_log_masks_env_var_value() {
    const SECRET: &str = "sk-secret-env-value";
    let buf = LogBuf::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buf.clone())
        .with_max_level(tracing::Level::TRACE)
        .json()
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let e = env(None);
    let svc = KeeperService::new(e.keeper.clone());
    let spec = pb::WorkloadSpec {
        containers: vec![pb::Container {
            name: "main".into(),
            env: vec![pb::EnvVar {
                name: "API_KEY".into(),
                value: SECRET.into(),
            }],
            ..Default::default()
        }],
        volumes: vec![],
    };
    let _ = svc
        .run(Request::new(pb::RunRequest {
            worker_pod_uid: POD.into(),
            hive: "demo".into(),
            actor_name: "counter".into(),
            actor_uid: UID.into(),
            spec: Some(spec.clone()),
            sandbox_runtime: Some(e.runtime.clone()),
            ..Default::default()
        }))
        .await;
    let err = svc
        .restore(Request::new(pb::RestoreRequest {
            worker_pod_uid: POD.into(),
            hive: "demo".into(),
            actor_name: "counter".into(),
            actor_uid: UID.into(),
            spec: Some(spec),
            snapshot: Some(snapshot("snap-1")),
            sandbox_runtime: Some(e.runtime.clone()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_ne!(err.code(), Code::Ok);

    let log = String::from_utf8(buf.0.lock().clone()).unwrap();
    assert!(
        !log.contains(SECRET),
        "the log holds the env var value:\n{log}"
    );
    for rpc in ["Run", "Restore"] {
        assert!(
            log.lines()
                .any(|l| l.contains(&format!("\"rpc\":\"{rpc}\""))),
            "no log line for {rpc}:\n{log}"
        );
    }
}

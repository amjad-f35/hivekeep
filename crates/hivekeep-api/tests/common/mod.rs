//! A full control plane over the in-memory store, a fake keeper served on a
//! local port, an in-memory object store and a mock volume plugin.

#![allow(dead_code)]

pub mod fault;
pub mod logs;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hivekeep_api::authz::{AllowAll, Authorizer};
use hivekeep_api::context::{request_as, Principal};
use hivekeep_api::control::{ControlService, Identity};
use hivekeep_api::keeper_auth::testing as certs;
use hivekeep_api::keeper_dialer::StaticKeeperDialer;
use hivekeep_api::metrics::Metrics;
use hivekeep_api::object_store::MemoryObjectStore;
use hivekeep_api::scheduling::{Scheduler, WorkerSource};
use hivekeep_api::volumes::{CreatedVolume, StaticVolumes, StorageClass, VolumePlugin};
use hivekeep_api::worker_cache::WorkerCache;
use hivekeep_api::worker_service::WorkerServiceImpl;
use hivekeep_api::worker_workflow::WorkerWorkflow;
use hivekeep_api::workflow::{ActorWorkflow, Settings};
use hivekeep_core::preview::{PreviewGates, PREVIEW_HEADER};
use hivekeep_core::resource::ActorTemplateRef;
use hivekeep_core::snapshot::SnapshotUri;
use hivekeep_identity::ca::{Ca, CaPool};
use hivekeep_identity::jwt::authority::{JwtAlgorithm, JwtPool};
use hivekeep_identity::key::KeyAlgorithm;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::api::v1::control_server::Control;
use hivekeep_proto::api::v1::worker_service_server::WorkerService;
use hivekeep_proto::keeper::v1 as kp;
use hivekeep_proto::keeper::v1::keeper_server::{Keeper, KeeperServer};
#[allow(unused_imports)]
use hivekeep_store::{
    ActorStore, ActorTemplateStore, MemoryStore, Precondition, Store, TagStore, WorkerStore,
};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Response, Status};

pub const HIVE: &str = "test-hive";
pub const LOCATION: &str = "gs://fake-fake-fake";
pub const ISSUER: &str = "https://idp.example.test";
pub const NODE: &str = "node1";
pub const KEEPER_NS: &str = "hivekeep-system";
pub const KEEPER_SA: &str = "hivekeep-keeper";
pub const IMAGE: &str =
    "main@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
pub const MOCK_DRIVER: &str = "hivekeep.io/mock";
/// The epoch every test worker registers.
pub const REPORTED_EPOCH: i64 = 1;
pub const SNAPSHOT_OBJECTS: [&str; 2] = ["manifest.json", "memory.zst"];

/// What the fake keeper saw and how it should fail.
#[derive(Default)]
pub struct KeeperState {
    pub run: Option<kp::RunRequest>,
    pub fail_run: Option<Status>,
    pub checkpoint: Option<kp::CheckpointRequest>,
    pub fail_checkpoint: Option<Status>,
    pub keep_checkpoint_local: bool,
    pub checkpointed: HashMap<String, kp::Snapshot>,
    pub restore: Option<kp::RestoreRequest>,
    pub fail_restore: Option<Status>,
    pub restore_epoch: Option<i64>,
    pub promote: Option<kp::PromoteSnapshotRequest>,
    pub fail_promote: Option<Status>,
    pub detach: Option<kp::DetachActorRequest>,
    pub fail_detach: Option<Status>,
    pub terminate: Option<kp::TerminateRequest>,
    pub terminate_calls: usize,
    pub fail_terminate: Option<Status>,
    pub terminate_delay: Option<Duration>,
    pub checkpoint_calls: usize,
    pub restore_calls: usize,
    /// Runs inside the next Checkpoint, before it answers.
    pub on_checkpoint: Option<fault::Hook>,
    /// Runs inside the next Restore, before it answers.
    pub on_restore: Option<fault::Hook>,
}

/// A keeper that records requests and writes snapshots to the object store.
#[derive(Clone)]
pub struct FakeKeeper {
    pub state: Arc<Mutex<KeeperState>>,
    pub objects: Arc<TestObjects>,
}

impl FakeKeeper {
    fn write(&self, uri: &str) {
        if !uri.is_empty() {
            self.objects.write_snapshot(uri, &SNAPSHOT_OBJECTS);
        }
    }
}

#[async_trait]
impl Keeper for FakeKeeper {
    async fn run(&self, r: Request<kp::RunRequest>) -> Result<Response<kp::RunResponse>, Status> {
        let mut s = self.state.lock();
        s.run = Some(r.into_inner());
        if let Some(e) = s.fail_run.clone() {
            return Err(e);
        }
        Ok(Response::new(kp::RunResponse {
            worker_epoch: REPORTED_EPOCH,
        }))
    }

    async fn checkpoint(
        &self,
        r: Request<kp::CheckpointRequest>,
    ) -> Result<Response<kp::CheckpointResponse>, Status> {
        let req = r.into_inner();
        let hook = self.state.lock().on_checkpoint.take();
        if let Some(h) = hook {
            h().await;
        }
        let mut s = self.state.lock();
        s.checkpoint_calls += 1;
        s.checkpoint = Some(req.clone());
        if let Some(e) = s.fail_checkpoint.clone() {
            return Err(e);
        }
        let snap = req.snapshot.clone().unwrap_or_default();
        s.checkpointed.insert(req.actor_uid.clone(), snap.clone());
        let immediate = req.upload_config.map(|u| u.mode) == Some(kp::UploadMode::Immediate as i32);
        if immediate {
            self.write(
                snap.object
                    .as_ref()
                    .map(|o| o.snapshot_uri.as_str())
                    .unwrap_or(""),
            );
        }
        if s.keep_checkpoint_local {
            let report = kp::SnapshotReport {
                snapshot: Some(snap),
                ..Default::default()
            };
            let state = kp::ActorSnapshotState {
                latest_local: Some(report.clone()),
                latest_durable: immediate.then_some(report),
                ..Default::default()
            };
            return Ok(Response::new(kp::CheckpointResponse { state: Some(state) }));
        }
        Ok(Response::new(kp::CheckpointResponse::default()))
    }

    async fn restore(
        &self,
        r: Request<kp::RestoreRequest>,
    ) -> Result<Response<kp::RestoreResponse>, Status> {
        let hook = self.state.lock().on_restore.take();
        if let Some(h) = hook {
            h().await;
        }
        let mut s = self.state.lock();
        s.restore_calls += 1;
        s.restore = Some(r.into_inner());
        if let Some(e) = s.fail_restore.clone() {
            return Err(e);
        }
        Ok(Response::new(kp::RestoreResponse {
            worker_epoch: s.restore_epoch.unwrap_or(REPORTED_EPOCH),
        }))
    }

    async fn promote_snapshot(
        &self,
        r: Request<kp::PromoteSnapshotRequest>,
    ) -> Result<Response<kp::PromoteSnapshotResponse>, Status> {
        let req = r.into_inner();
        let mut s = self.state.lock();
        s.promote = Some(req.clone());
        if let Some(e) = s.fail_promote.clone() {
            return Err(e);
        }
        let snap = req.snapshot.unwrap_or_default();
        self.write(
            snap.object
                .as_ref()
                .map(|o| o.snapshot_uri.as_str())
                .unwrap_or(""),
        );
        let report = kp::SnapshotReport {
            snapshot: Some(snap),
            ..Default::default()
        };
        Ok(Response::new(kp::PromoteSnapshotResponse {
            state: Some(kp::ActorSnapshotState {
                latest_local: Some(report.clone()),
                latest_durable: Some(report),
                ..Default::default()
            }),
        }))
    }

    async fn detach_actor(
        &self,
        r: Request<kp::DetachActorRequest>,
    ) -> Result<Response<kp::DetachActorResponse>, Status> {
        let req = r.into_inner();
        let mut s = self.state.lock();
        s.detach = Some(req.clone());
        if let Some(e) = s.fail_detach.clone() {
            return Err(e);
        }
        let Some(snap) = s.checkpointed.get(&req.actor_uid).cloned() else {
            return Ok(Response::new(kp::DetachActorResponse {
                state: Some(Default::default()),
            }));
        };
        self.write(
            snap.object
                .as_ref()
                .map(|o| o.snapshot_uri.as_str())
                .unwrap_or(""),
        );
        Ok(Response::new(kp::DetachActorResponse {
            state: Some(kp::ActorSnapshotState {
                latest_durable: Some(kp::SnapshotReport {
                    snapshot: Some(snap),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }))
    }

    async fn terminate(
        &self,
        r: Request<kp::TerminateRequest>,
    ) -> Result<Response<kp::TerminateResponse>, Status> {
        let delay = self.state.lock().terminate_delay;
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        let mut s = self.state.lock();
        s.terminate = Some(r.into_inner());
        s.terminate_calls += 1;
        if let Some(e) = s.fail_terminate.clone() {
            return Err(e);
        }
        Ok(Response::new(kp::TerminateResponse {}))
    }
}

/// The in-memory object store with injectable failures.
#[derive(Default)]
pub struct TestObjects {
    pub inner: MemoryObjectStore,
    pub fail_cleanup: Mutex<bool>,
    pub fail_copy: Mutex<bool>,
    pub cleanups: Mutex<Vec<String>>,
}

impl TestObjects {
    pub fn write_snapshot(&self, uri: &str, names: &[&str]) {
        self.inner.write_snapshot(uri, names)
    }
    pub fn list(&self, prefix: &str) -> Vec<String> {
        self.inner.list(prefix)
    }
    pub fn put(&self, uri: impl Into<String>, data: impl Into<Vec<u8>>) {
        self.inner.put(uri, data)
    }
}

#[async_trait]
impl hivekeep_api::object_store::ObjectStore for TestObjects {
    async fn cleanup_snapshot(
        &self,
        prefix: &str,
    ) -> Result<(), hivekeep_api::object_store::ObjectStoreError> {
        self.cleanups.lock().push(prefix.to_string());
        if *self.fail_cleanup.lock() {
            return Err(hivekeep_api::object_store::ObjectStoreError(
                "injected cleanup failure".into(),
            ));
        }
        self.inner.cleanup_snapshot(prefix).await
    }
    async fn copy_snapshot(
        &self,
        src: &str,
        dst: &str,
    ) -> Result<(), hivekeep_api::object_store::ObjectStoreError> {
        if *self.fail_copy.lock() {
            return Err(hivekeep_api::object_store::ObjectStoreError(
                "injected copy failure".into(),
            ));
        }
        self.inner.copy_snapshot(src, dst).await
    }
}

/// A volume plugin that hands out ids and records calls.
#[derive(Default)]
pub struct MockVolumes {
    pub created: Mutex<Vec<String>>,
    pub attached: Mutex<Vec<(String, String)>>,
    pub detached: Mutex<Vec<(String, String)>>,
    pub deleted: Mutex<Vec<String>>,
    pub fail_create: Mutex<Option<Status>>,
    pub fail_attach: Mutex<Option<Status>>,
    /// Attaches of volume ids containing one of these substrings fail.
    pub fail_attach_matching: Mutex<Vec<String>>,
    pub fail_detach: Mutex<Option<Status>>,
    pub fail_delete: Mutex<Option<Status>>,
}

#[async_trait]
impl VolumePlugin for MockVolumes {
    async fn create_volume(
        &self,
        name: &str,
        _: i64,
        _: &HashMap<String, String>,
    ) -> Result<CreatedVolume, Status> {
        if let Some(e) = self.fail_create.lock().clone() {
            return Err(e);
        }
        self.created.lock().push(name.into());
        Ok(CreatedVolume {
            volume_id: format!("vol-{name}"),
            volume_context: [("ctx".to_string(), "v".to_string())].into(),
        })
    }
    async fn attach_volume(&self, id: &str, node: &str) -> Result<HashMap<String, String>, Status> {
        if let Some(e) = self.fail_attach.lock().clone() {
            return Err(e);
        }
        if self
            .fail_attach_matching
            .lock()
            .iter()
            .any(|m| id.contains(m.as_str()))
        {
            return Err(Status::unavailable(format!("attach {id} unavailable")));
        }
        self.attached.lock().push((id.into(), node.into()));
        Ok([("devicePath".to_string(), format!("/dev/{id}"))].into())
    }
    async fn detach_volume(&self, id: &str, node: &str) -> Result<(), Status> {
        if let Some(e) = self.fail_detach.lock().clone() {
            return Err(e);
        }
        self.detached.lock().push((id.into(), node.into()));
        Ok(())
    }
    async fn delete_volume(&self, id: &str) -> Result<(), Status> {
        if let Some(e) = self.fail_delete.lock().clone() {
            return Err(e);
        }
        self.deleted.lock().push(id.into());
        Ok(())
    }
}

pub struct Options {
    pub gates: PreviewGates,
    pub preview_client: bool,
    pub authz: Option<Arc<dyn Authorizer>>,
    /// Use the policy authorizer over this harness's store with these
    /// bootstrap owners and system principals.
    pub policy_authz: Option<(Vec<String>, hivekeep_api::authz::SystemPrincipals)>,
    pub egress_gateway: String,
    pub no_object_store: bool,
    pub snapshot_location: Option<String>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            gates: PreviewGates::none(),
            preview_client: false,
            authz: None,
            policy_authz: None,
            egress_gateway: String::new(),
            no_object_store: false,
            snapshot_location: None,
        }
    }
}

pub struct Harness {
    pub store: Arc<dyn Store>,
    pub memory: Arc<MemoryStore>,
    pub cache: Arc<WorkerCache>,
    pub control: ControlService,
    pub workers: WorkerServiceImpl,
    pub worker_workflow: Arc<WorkerWorkflow>,
    pub keeper: FakeKeeper,
    pub dialer: Arc<StaticKeeperDialer>,
    pub objects: Arc<TestObjects>,
    pub faults: Arc<fault::FaultStore>,
    pub volumes: Arc<MockVolumes>,
    pub metrics: Arc<Metrics>,
    pub pod_ca: CaPool,
    pub actor_ca: CaPool,
    pub jwt: Arc<JwtPool>,
    pub preview_client: bool,
    pub keeper_addr: String,
    stop: CancellationToken,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// Serves `keeper` on a fresh local port, returning `http://addr`.
pub async fn serve_keeper(keeper: FakeKeeper, stop: CancellationToken) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(KeeperServer::new(keeper))
            .serve_with_incoming_shutdown(incoming, stop.cancelled())
            .await;
    });
    format!("http://{addr}")
}

impl Harness {
    pub async fn new() -> Harness {
        Self::with(Options::default()).await
    }

    pub async fn preview() -> Harness {
        Self::with(Options {
            gates: PreviewGates::all(),
            preview_client: true,
            ..Default::default()
        })
        .await
    }

    pub async fn with(o: Options) -> Harness {
        let stop = CancellationToken::new();
        let memory = Arc::new(MemoryStore::new());
        let faults = Arc::new(fault::FaultStore::new(memory.clone()));
        let store: Arc<dyn Store> = faults.clone();
        let cache = WorkerCache::new(store.clone(), Duration::from_secs(300));
        cache.start(stop.clone()).await.unwrap();
        let objects = Arc::new(TestObjects::default());
        let keeper = FakeKeeper {
            state: Arc::new(Mutex::new(KeeperState::default())),
            objects: objects.clone(),
        };
        let keeper_addr = serve_keeper(keeper.clone(), stop.clone()).await;
        let dialer = Arc::new(StaticKeeperDialer::new());
        dialer.set(NODE, keeper_addr.clone());
        let mock = Arc::new(MockVolumes::default());
        let volumes = Arc::new(StaticVolumes::new());
        volumes.add_plugin(MOCK_DRIVER, mock.clone());
        for sc in ["standard", "fast"] {
            volumes.add_storage_class(
                sc,
                StorageClass {
                    provisioner: MOCK_DRIVER.into(),
                    parameters: HashMap::new(),
                },
            );
        }
        let metrics = Arc::new(Metrics::new());
        let settings = Settings {
            egress_gateway_address: o.egress_gateway.clone(),
            snapshot_location: o
                .snapshot_location
                .clone()
                .unwrap_or_else(|| LOCATION.into()),
            gates: o.gates.clone(),
        };
        let workflow = Arc::new(ActorWorkflow::new(
            store.clone(),
            cache.clone(),
            Scheduler::new(cache.clone() as Arc<dyn WorkerSource>),
            dialer.clone(),
            volumes,
            if o.no_object_store {
                None
            } else {
                Some(objects.clone() as Arc<dyn hivekeep_api::object_store::ObjectStore>)
            },
            metrics.clone(),
            settings,
        ));
        let worker_workflow = Arc::new(WorkerWorkflow::new(
            store.clone(),
            Some(dialer.clone()),
            metrics.clone(),
            true,
        ));
        let jwt = Arc::new(JwtPool::generate(JwtAlgorithm::Es256, Some("1")).unwrap());
        let actor_ca = CaPool::new(
            vec![Ca::generate("1", KeyAlgorithm::EcdsaP256, Duration::from_secs(86400)).unwrap()],
            "1",
        );
        let identity = Identity {
            jwt_issuer: ISSUER.into(),
            jwt_signer: jwt.clone(),
            ca: Arc::new(actor_ca.clone()),
        };
        let authz: Arc<dyn Authorizer> =
            match (o.authz, o.policy_authz) {
                (Some(a), _) => a,
                (None, Some((owners, sys))) => Arc::new(
                    hivekeep_api::authz::PolicyAuthorizer::new(store.clone(), owners, sys),
                ),
                (None, None) => Arc::new(AllowAll),
            };
        let control = ControlService::new(
            store.clone(),
            workflow,
            worker_workflow.clone(),
            authz,
            identity.clone(),
        );
        let workers = WorkerServiceImpl::new(
            control.clone(),
            certs::spiffe_id(KEEPER_NS, KEEPER_SA),
            identity,
        );
        let h = Harness {
            store,
            memory,
            cache,
            control,
            workers,
            worker_workflow,
            keeper,
            dialer,
            objects,
            faults,
            volumes: mock,
            metrics,
            pod_ca: certs::pod_ca(),
            actor_ca,
            jwt,
            preview_client: o.preview_client,
            keeper_addr,
            stop,
        };
        h.c().create_hive(h.req(hive(HIVE))).await.unwrap();
        h
    }

    /// The Control API.
    pub fn c(&self) -> &dyn Control {
        &self.control
    }

    /// The WorkerService.
    pub fn ws(&self) -> &dyn WorkerService {
        &self.workers
    }

    /// A request from an ordinary user.
    pub fn req<T>(&self, msg: T) -> Request<T> {
        let mut r = request_as(msg, Some(Principal::jwt("tester", ISSUER)), None);
        if self.preview_client {
            r.metadata_mut()
                .insert(PREVIEW_HEADER, "true".parse().unwrap());
        }
        r
    }

    /// A request from a user without preview opt-in.
    pub fn req_no_preview<T>(&self, msg: T) -> Request<T> {
        request_as(msg, Some(Principal::jwt("tester", ISSUER)), None)
    }

    /// A request from the keeper on `node`.
    pub fn keeper_req<T>(&self, node: &str, msg: T) -> Request<T> {
        let chain = certs::pod_cert(&self.pod_ca, KEEPER_NS, KEEPER_SA, node);
        request_as(
            msg,
            Some(Principal::mtls(certs::spiffe_id(KEEPER_NS, KEEPER_SA))),
            Some(chain),
        )
    }

    pub fn ks(&self) -> parking_lot::MutexGuard<'_, KeeperState> {
        self.keeper.state.lock()
    }

    pub fn reset_keeper(&self) {
        *self.keeper.state.lock() = KeeperState::default();
    }

    /// Creates template `name` with one container, a selector on
    /// `pool=<pool>` and 1 CPU / 512 MiB limits.
    pub async fn create_template_with(
        &self,
        name: &str,
        pool: &str,
        containers: Vec<pb::Container>,
        volumes: Vec<pb::Volume>,
    ) -> pb::ActorTemplate {
        self.c()
            .create_actor_template(self.req(pb::CreateActorTemplateRequest {
                actor_template: Some(pb::ActorTemplate {
                    metadata: Some(md(HIVE, name)),
                    containers,
                    volumes,
                    worker_selector: Some(pb::Selector {
                        match_labels: [("pool".to_string(), pool.to_string())].into(),
                    }),
                    resources: Some(pb::ResourceRequirements {
                        limits: Some(pb::Resources {
                            cpu_millicores: 1000,
                            memory_bytes: 512 << 20,
                        }),
                    }),
                    ..Default::default()
                }),
            }))
            .await
            .unwrap()
            .into_inner()
    }

    /// A template "tmpl1" with no golden snapshot.
    pub async fn create_template(&self) -> pb::ActorTemplate {
        self.create_template_with("tmpl1", "pool1", vec![container()], vec![])
            .await
    }

    /// A template "tmpl1" with a golden Tag recorded, as the reconciler
    /// would leave it.
    pub async fn create_template_with_golden(&self) -> pb::ActorTemplate {
        let t = self.create_template().await;
        self.record_golden(&t).await
    }

    pub async fn record_golden(&self, t: &pb::ActorTemplate) -> pb::ActorTemplate {
        let uid = t.metadata.as_ref().unwrap().uid.clone();
        let uri = golden_uri(&uid);
        self.objects.write_snapshot(&uri, &SNAPSHOT_OBJECTS);
        let tag = self
            .store
            .create_tag(&pb::Tag {
                metadata: Some(md(HIVE, &uid)),
                source_actor: Some(oref(HIVE, &uid)),
                scope: pb::TagScope::Published as i32,
                status: Some(pb::TagStatus {
                    snapshot: Some(durable_snapshot(
                        1,
                        pb::SnapshotOwner::Tag,
                        pb::SnapshotFidelity::Memory,
                        &uid,
                        &uid,
                        &uri,
                    )),
                    actor_template_uid: uid.clone(),
                }),
            })
            .await
            .unwrap();
        let tr = ActorTemplateRef::new(HIVE, t.metadata.as_ref().unwrap().name.clone());
        self.store
            .update_actor_template(&tr, Precondition::of(t), &mut |t| {
                t.status = Some(pb::ActorTemplateStatus {
                    golden_snapshot_status: Some(pb::GoldenSnapshotStatus {
                        golden_tag: Some(oref(HIVE, &tag.metadata.as_ref().unwrap().name)),
                        ..Default::default()
                    }),
                });
                Ok(())
            })
            .await
            .unwrap()
    }

    pub async fn create_actor(&self, name: &str, template: &str) -> pb::Actor {
        self.c()
            .create_actor(self.req(pb::CreateActorRequest {
                actor: Some(pb::Actor {
                    metadata: Some(md(HIVE, name)),
                    actor_template: Some(oref(HIVE, template)),
                    ..Default::default()
                }),
            }))
            .await
            .unwrap()
            .into_inner()
    }

    pub async fn get_actor(&self, name: &str) -> pb::Actor {
        self.store
            .get_actor(&hivekeep_core::resource::ActorRef::new(HIVE, name))
            .await
            .unwrap()
    }

    pub async fn resume(&self, name: &str) -> Result<pb::ResumeActorResponse, Status> {
        self.c()
            .resume_actor(self.req(pb::ResumeActorRequest {
                actor: Some(oref(HIVE, name)),
            }))
            .await
            .map(|r| r.into_inner())
    }

    pub async fn suspend(&self, name: &str, mode: pb::DurabilityMode) -> Result<pb::Actor, Status> {
        self.c()
            .suspend_actor(self.req(pb::SuspendActorRequest {
                actor: Some(oref(HIVE, name)),
                options: Some(pb::SuspendOptions { mode: mode as i32 }),
            }))
            .await
            .map(|r| r.into_inner().actor.unwrap())
    }

    pub async fn revert(&self, name: &str) -> Result<pb::Actor, Status> {
        self.c()
            .revert_actor(self.req(pb::RevertActorRequest {
                actor: Some(oref(HIVE, name)),
            }))
            .await
            .map(|r| r.into_inner().actor.unwrap())
    }

    pub async fn delete(&self, name: &str) -> Result<pb::Actor, Status> {
        self.c()
            .delete_actor(self.req(pb::DeleteActorRequest {
                actor: Some(oref(HIVE, name)),
                options: None,
            }))
            .await
            .map(|r| r.into_inner())
    }

    /// Registers worker `name` on `node` in `pool` with `slots` actor slots
    /// and a gvisor runtime, marks it available, and waits for the cache.
    pub async fn create_worker(
        &self,
        name: &str,
        node: &str,
        pool: &str,
        slots: i32,
    ) -> pb::Worker {
        let pod_uid = uuid::Uuid::new_v4().to_string();
        self.c()
            .create_worker(self.req(pb::CreateWorkerRequest {
                worker: Some(pb::Worker {
                    metadata: Some(pb::ResourceMetadata {
                        name: name.into(),
                        ..Default::default()
                    }),
                    worker_namespace: "workers".into(),
                    worker_pool: pool.into(),
                    worker_pod: name.into(),
                    worker_pod_uid: pod_uid,
                    node_name: node.into(),
                    labels: [("pool".to_string(), pool.to_string())].into(),
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();
        self.c()
            .mark_worker_available(self.req(pb::MarkWorkerAvailableRequest {
                worker: Some(wref(name)),
            }))
            .await
            .unwrap();
        self.register_worker(name, node, slots).await;
        self.wait_for_cache(name, |w| {
            w.status
                .as_ref()
                .is_some_and(|s| s.registered_epoch > 0 && !s.sandbox_classes.is_empty())
        })
        .await;
        self.store.get_worker(name).await.unwrap()
    }

    /// Acquires an epoch, registers it, and reports a gvisor runtime.
    pub async fn register_worker(&self, name: &str, node: &str, slots: i32) -> i64 {
        let epoch = self
            .ws()
            .acquire_worker_epoch(self.keeper_req(
                node,
                pb::AcquireWorkerEpochRequest {
                    worker: Some(wref(name)),
                },
            ))
            .await
            .unwrap()
            .into_inner()
            .epoch;
        self.ws()
            .register_worker(self.keeper_req(
                node,
                pb::RegisterWorkerRequest {
                    worker: Some(wref(name)),
                    epoch,
                    ips: vec!["10.0.0.1".into()],
                    capacity: Some(pb::WorkerResources {
                        cpu_millicores: 8000,
                        memory_bytes: 16 << 30,
                        actors: slots,
                    }),
                },
            ))
            .await
            .unwrap();
        self.ws()
            .set_worker_sandbox_runtimes(self.keeper_req(
                node,
                pb::SetWorkerSandboxRuntimesRequest {
                    worker: Some(wref(name)),
                    epoch,
                    sandbox_classes: vec![gvisor_class("v1", "bundle-a")],
                },
            ))
            .await
            .unwrap();
        epoch
    }

    pub async fn wait_for_cache(&self, name: &str, cond: impl Fn(&pb::Worker) -> bool) {
        for _ in 0..500 {
            if self.cache.worker(name).is_some_and(|w| cond(&w)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("worker {name} never reached the expected state in the cache");
    }

    /// Waits until the cache shows `name` hosting nothing.
    pub async fn wait_worker_free(&self, name: &str) {
        self.wait_for_cache(name, |w| {
            w.status
                .as_ref()
                .and_then(|s| s.allocated)
                .map(|a| a.actors)
                .unwrap_or(0)
                == 0
        })
        .await
    }

    /// A RUNNING Actor `name` from "tmpl1" on a fresh worker "w1".
    pub async fn running_actor(&self, name: &str) -> pb::Actor {
        if self
            .store
            .get_actor_template(&ActorTemplateRef::new(HIVE, "tmpl1"))
            .await
            .is_err()
        {
            self.create_template().await;
        }
        if self.store.get_worker("w1").await.is_err() {
            self.create_worker("w1", NODE, "pool1", 4).await;
        }
        self.create_actor(name, "tmpl1").await;
        let r = self.resume(name).await.unwrap();
        assert!(r.resumed);
        r.actor.unwrap()
    }

    pub async fn set_actor_status(
        &self,
        name: &str,
        f: impl Fn(&mut pb::ActorStatus) + Send + Sync,
    ) -> pb::Actor {
        let a = self.get_actor(name).await;
        self.store
            .update_actor(
                &hivekeep_core::resource::ActorRef::new(HIVE, name),
                Precondition::of(&a),
                &mut |a| {
                    f(a.status.get_or_insert_with(Default::default));
                    Ok(())
                },
            )
            .await
            .unwrap()
    }
}

pub fn md(hive: &str, name: &str) -> pb::ResourceMetadata {
    pb::ResourceMetadata {
        hive: hive.into(),
        name: name.into(),
        ..Default::default()
    }
}

pub fn oref(hive: &str, name: &str) -> pb::ObjectRef {
    pb::ObjectRef {
        hive: hive.into(),
        name: name.into(),
    }
}

pub fn wref(name: &str) -> pb::ObjectRef {
    pb::ObjectRef {
        hive: String::new(),
        name: name.into(),
    }
}

pub fn hive(name: &str) -> pb::CreateHiveRequest {
    pb::CreateHiveRequest {
        hive: Some(pb::Hive {
            metadata: Some(pb::ResourceMetadata {
                name: name.into(),
                ..Default::default()
            }),
        }),
    }
}

pub fn container() -> pb::Container {
    pb::Container {
        name: "main".into(),
        image: IMAGE.into(),
        command: vec!["/main".into()],
        ..Default::default()
    }
}

pub fn gvisor_class(schema: &str, bundle: &str) -> pb::WorkerSandboxClass {
    pb::WorkerSandboxClass {
        sandbox_class: "gvisor".into(),
        default_runtime: Some(pb::WorkerSandboxRuntime {
            version: Some(compat(schema, bundle)),
            actor_overhead: None,
        }),
        restorable_runtimes: vec![],
    }
}

pub fn compat(schema: &str, bundle: &str) -> pb::VersionedSandboxCompat {
    pb::VersionedSandboxCompat {
        schema_version: schema.into(),
        attributes: vec![pb::AttributeEntry {
            key: "sandbox_bundle_hash".into(),
            value: bundle.into(),
        }],
    }
}

pub fn golden_uri(uid: &str) -> String {
    SnapshotUri::for_tag(LOCATION, HIVE, uid)
        .unwrap()
        .as_str()
        .to_string()
}

pub fn durable_snapshot(
    gen: i32,
    owner: pb::SnapshotOwner,
    fidelity: pb::SnapshotFidelity,
    template_uid: &str,
    uuid: &str,
    uri: &str,
) -> pb::Snapshot {
    pb::Snapshot {
        uuid: uuid.into(),
        generation: gen,
        owner: owner as i32,
        fidelity: fidelity as i32,
        actor_template_uid: template_uid.into(),
        durable_snapshot: Some(pb::SnapshotStorage {
            state: pb::SnapshotStorageState::Completed as i32,
            object: Some(pb::ObjectSnapshot {
                snapshot_uri: uri.into(),
            }),
        }),
        ..Default::default()
    }
}

pub fn state(a: &pb::Actor) -> pb::ActorState {
    pb::ActorState::try_from(a.status.as_ref().map(|s| s.state).unwrap_or(0)).unwrap()
}

pub fn latest_durable(a: &pb::Actor) -> Option<&pb::Snapshot> {
    a.status
        .as_ref()?
        .snapshots
        .iter()
        .filter(|s| {
            s.durable_snapshot
                .as_ref()
                .is_some_and(|d| d.state == pb::SnapshotStorageState::Completed as i32)
        })
        .max_by_key(|s| s.generation)
}

pub fn latest_local(a: &pb::Actor) -> Option<&pb::Snapshot> {
    a.status
        .as_ref()?
        .snapshots
        .iter()
        .filter(|s| !s.locality.is_empty())
        .max_by_key(|s| s.generation)
}

pub fn uri_of(s: &pb::Snapshot) -> String {
    s.durable_snapshot
        .as_ref()
        .and_then(|d| d.object.as_ref())
        .map(|o| o.snapshot_uri.clone())
        .unwrap_or_default()
}

#[track_caller]
pub fn assert_code<T: std::fmt::Debug>(r: Result<T, Status>, code: Code, contains: &str) {
    match r {
        Ok(v) => panic!("expected {code:?}, got Ok({v:?})"),
        Err(s) => {
            assert_eq!(s.code(), code, "message: {}", s.message());
            assert!(
                s.message().contains(contains),
                "message {:?} lacks {contains:?}",
                s.message()
            );
        }
    }
}

/// Sum of the samples of `metric` whose labels contain every `label=value`.
pub fn metric_count(h: &Harness, metric: &str, labels: &[(&str, &str)]) -> f64 {
    h.metrics
        .encode()
        .lines()
        .filter(|l| {
            l.starts_with(metric)
                && (l.as_bytes().get(metric.len()) == Some(&b'{')
                    || l.as_bytes().get(metric.len()) == Some(&b' '))
        })
        .filter(|l| {
            labels
                .iter()
                .all(|(k, v)| l.contains(&format!("{k}=\"{v}\"")))
        })
        .filter_map(|l| l.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()))
        .sum()
}

/// A hook future from an async block.
pub fn hook<F: std::future::Future<Output = ()> + Send + 'static>(f: F) -> fault::Hook {
    Box::new(move || Box::pin(f))
}

impl Harness {
    /// Ensures template "tmpl1" and worker "w1" on NODE exist.
    pub async fn ensure_base(&self) {
        if self
            .store
            .get_actor_template(&ActorTemplateRef::new(HIVE, "tmpl1"))
            .await
            .is_err()
        {
            self.create_template().await;
        }
        if self.store.get_worker("w1").await.is_err() {
            self.create_worker("w1", NODE, "pool1", 4).await;
        }
    }

    /// An Actor `name` bound to "w1" and left in `state` with the given
    /// snapshots, as a workflow would leave it.
    pub async fn seed_on_worker(
        &self,
        name: &str,
        state: pb::ActorState,
        snapshots: Vec<pb::Snapshot>,
    ) -> pb::Actor {
        self.ensure_base().await;
        let a = self.create_actor(name, "tmpl1").await;
        let uid = a.metadata.as_ref().unwrap().uid.clone();
        let w = self.store.get_worker("w1").await.unwrap();
        self.store
            .bind_actor_to_worker(
                "w1",
                &pb::ActorAssignment {
                    actor: Some(oref(HIVE, name)),
                    actor_uid: uid,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
        let mut wa =
            hivekeep_api::workflow::resume::worker_assignment_from(&w, &Default::default());
        wa.worker_epoch = REPORTED_EPOCH;
        let gen = snapshots.iter().map(|s| s.generation).max().unwrap_or(0);
        self.set_actor_status(name, move |s| {
            s.state = state as i32;
            s.assigned_node = NODE.into();
            s.worker_assignment = Some(wa.clone());
            s.snapshots = snapshots.clone();
            s.last_assigned_generation = gen;
        })
        .await
    }

    /// The URI of snapshot `name` under the Actor's own prefix.
    pub fn actor_uri(&self, actor: &pb::Actor, name: &str) -> String {
        actor_uri_at(LOCATION, actor, name)
    }

    pub fn crash_count(&self) -> f64 {
        metric_count(self, "hivekeep_actor_crashes_total", &[])
    }
}

pub fn actor_uri_at(location: &str, actor: &pb::Actor, name: &str) -> String {
    let md = actor.metadata.as_ref().unwrap();
    SnapshotUri::for_actor(location, &md.hive, &md.uid, name)
        .unwrap()
        .as_str()
        .to_string()
}

/// An Actor-owned snapshot record.
pub fn actor_snapshot(
    gen: i32,
    name: &str,
    uri: &str,
    state: pb::SnapshotStorageState,
    locality: &str,
) -> pb::Snapshot {
    let mut s = durable_snapshot(
        gen,
        pb::SnapshotOwner::Actor,
        pb::SnapshotFidelity::Memory,
        "",
        name,
        uri,
    );
    s.durable_snapshot.as_mut().unwrap().state = state as i32;
    s.locality = locality.into();
    s
}

pub fn ar(name: &str) -> hivekeep_core::resource::ActorRef {
    hivekeep_core::resource::ActorRef::new(HIVE, name)
}

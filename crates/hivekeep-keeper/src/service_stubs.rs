//! Test servers: a Keeper and a WorkerSupport service whose RPCs answer
//! from a shared behaviour, for tests of what wraps a service (TLS,
//! interceptors, draining) rather than of the service itself.

use std::net::SocketAddr;
use std::sync::Arc;

use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::keeper::v1::{keeper_server, worker_support_server};
use tokio::sync::Notify;
use tonic::{Request, Response, Status};

/// What every stub RPC does.
#[derive(Clone, Default)]
pub(crate) struct StubBehaviour {
    /// When set, each call waits for this before answering.
    pub(crate) block: Option<Arc<Notify>>,
}

impl StubBehaviour {
    async fn answer<R: Default>(&self) -> Result<Response<R>, Status> {
        if let Some(n) = &self.block {
            n.notified().await;
        }
        Ok(Response::new(R::default()))
    }
}

/// A Keeper service answering every RPC with an empty response.
#[derive(Clone, Default)]
pub(crate) struct StubKeeper(pub(crate) StubBehaviour);

#[tonic::async_trait]
impl keeper_server::Keeper for StubKeeper {
    async fn run(&self, _: Request<pb::RunRequest>) -> Result<Response<pb::RunResponse>, Status> {
        self.0.answer().await
    }
    async fn checkpoint(
        &self,
        _: Request<pb::CheckpointRequest>,
    ) -> Result<Response<pb::CheckpointResponse>, Status> {
        self.0.answer().await
    }
    async fn restore(
        &self,
        _: Request<pb::RestoreRequest>,
    ) -> Result<Response<pb::RestoreResponse>, Status> {
        self.0.answer().await
    }
    async fn promote_snapshot(
        &self,
        _: Request<pb::PromoteSnapshotRequest>,
    ) -> Result<Response<pb::PromoteSnapshotResponse>, Status> {
        self.0.answer().await
    }
    async fn detach_actor(
        &self,
        _: Request<pb::DetachActorRequest>,
    ) -> Result<Response<pb::DetachActorResponse>, Status> {
        self.0.answer().await
    }
    async fn terminate(
        &self,
        _: Request<pb::TerminateRequest>,
    ) -> Result<Response<pb::TerminateResponse>, Status> {
        self.0.answer().await
    }
}

/// A WorkerSupport service answering every RPC with an empty response.
#[derive(Clone, Default)]
pub(crate) struct StubWorkerSupport(pub(crate) StubBehaviour);

#[tonic::async_trait]
impl worker_support_server::WorkerSupport for StubWorkerSupport {
    async fn mint_actor_certificate(
        &self,
        _: Request<pb::MintActorCertificateRequest>,
    ) -> Result<Response<pb::MintActorCertificateResponse>, Status> {
        self.0.answer().await
    }
    async fn acquire_worker_epoch(
        &self,
        _: Request<pb::AcquireWorkerEpochRequest>,
    ) -> Result<Response<pb::AcquireWorkerEpochResponse>, Status> {
        self.0.answer().await
    }
    async fn register_worker(
        &self,
        _: Request<pb::RegisterWorkerRequest>,
    ) -> Result<Response<pb::RegisterWorkerResponse>, Status> {
        self.0.answer().await
    }
    async fn set_worker_sandbox_runtimes(
        &self,
        _: Request<pb::SetWorkerSandboxRuntimesRequest>,
    ) -> Result<Response<pb::SetWorkerSandboxRuntimesResponse>, Status> {
        self.0.answer().await
    }
    async fn request_actor_suspend(
        &self,
        _: Request<pb::RequestActorSuspendRequest>,
    ) -> Result<Response<pb::RequestActorSuspendResponse>, Status> {
        self.0.answer().await
    }
    async fn get_actor_egress_policy(
        &self,
        _: Request<pb::GetActorEgressPolicyRequest>,
    ) -> Result<Response<pb::GetActorEgressPolicyResponse>, Status> {
        self.0.answer().await
    }
    async fn report_actor_crashed(
        &self,
        _: Request<pb::ReportActorCrashedRequest>,
    ) -> Result<Response<pb::ReportActorCrashedResponse>, Status> {
        self.0.answer().await
    }
}

/// A loopback TCP listener and its address.
pub(crate) async fn loopback() -> (tokio_stream::wrappers::TcpListenerStream, SocketAddr) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    (tokio_stream::wrappers::TcpListenerStream::new(l), addr)
}

/// A control plane that mints a fixed actor JWT, or fails, and records
/// what it was asked to mint. Its other calls are unimplemented.
pub(crate) struct Minter {
    pub(crate) jwt: parking_lot::Mutex<String>,
    pub(crate) expiry: chrono::DateTime<chrono::Utc>,
    pub(crate) err: Option<Status>,
    pub(crate) got: parking_lot::Mutex<Vec<hivekeep_proto::api::v1::MintWorkerActorJwtRequest>>,
}

impl Minter {
    /// A minter of `jwt`, valid until `expiry`.
    pub(crate) fn new(jwt: &str, expiry: chrono::DateTime<chrono::Utc>) -> Self {
        Minter {
            jwt: parking_lot::Mutex::new(jwt.into()),
            expiry,
            err: None,
            got: Default::default(),
        }
    }

    /// A minter failing with `err`.
    pub(crate) fn failing(err: Status) -> Self {
        Minter {
            err: Some(err),
            ..Minter::new("", chrono::Utc::now())
        }
    }
}

#[tonic::async_trait]
impl crate::control::WorkerServiceApi for Minter {
    async fn acquire_worker_epoch(
        &self,
        _: &crate::context::Ctx,
        _: hivekeep_proto::api::v1::AcquireWorkerEpochRequest,
    ) -> Result<hivekeep_proto::api::v1::AcquireWorkerEpochResponse, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn register_worker(
        &self,
        _: &crate::context::Ctx,
        _: hivekeep_proto::api::v1::RegisterWorkerRequest,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn set_worker_sandbox_runtimes(
        &self,
        _: &crate::context::Ctx,
        _: hivekeep_proto::api::v1::SetWorkerSandboxRuntimesRequest,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn mint_worker_actor_certificate(
        &self,
        _: &crate::context::Ctx,
        _: hivekeep_proto::api::v1::MintWorkerActorCertificateRequest,
    ) -> Result<hivekeep_proto::api::v1::MintWorkerActorCertificateResponse, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn mint_worker_actor_jwt(
        &self,
        _: &crate::context::Ctx,
        req: hivekeep_proto::api::v1::MintWorkerActorJwtRequest,
    ) -> Result<hivekeep_proto::api::v1::MintWorkerActorJwtResponse, Status> {
        self.got.lock().push(req);
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        Ok(hivekeep_proto::api::v1::MintWorkerActorJwtResponse {
            actor_jwt: self.jwt.lock().clone(),
            expire_time: Some(crate::snapshot_tracker::timestamp(self.expiry)),
        })
    }
    async fn request_actor_suspend(
        &self,
        _: &crate::context::Ctx,
        _: hivekeep_proto::api::v1::RequestActorSuspendRequest,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn report_actor_snapshot_state(
        &self,
        _: &crate::context::Ctx,
        _: hivekeep_proto::api::v1::ReportActorSnapshotStateRequest,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn report_actor_crashed(
        &self,
        _: &crate::context::Ctx,
        _: hivekeep_proto::api::v1::ReportActorCrashedRequest,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("stub"))
    }
}

/// An object store over a [`LocalObjectStore`](crate::objectstore::LocalObjectStore)
/// that records every transfer request and can fail uploads.
pub(crate) struct RecordingStore {
    pub(crate) inner: crate::objectstore::LocalObjectStore,
    pub(crate) put_err: Option<Status>,
    pub(crate) fetches: parking_lot::Mutex<Vec<crate::objectstore::FetchSnapshot>>,
    pub(crate) uploads: parking_lot::Mutex<Vec<crate::objectstore::UploadSnapshot>>,
}

impl RecordingStore {
    /// A store keeping objects under `root`.
    pub(crate) fn new(root: &std::path::Path) -> Self {
        RecordingStore {
            inner: crate::objectstore::LocalObjectStore::new(root),
            put_err: None,
            fetches: Default::default(),
            uploads: Default::default(),
        }
    }

    /// The object names stored under `uri`, sorted.
    pub(crate) fn keys(&self, uri: &str) -> Vec<String> {
        let Ok(dir) = self.inner.dir_for(uri) else {
            return Vec::new();
        };
        let mut out: Vec<String> = walkdir::WalkDir::new(&dir)
            .into_iter()
            .flatten()
            .filter(|e| e.file_type().is_file())
            .map(|e| {
                e.path()
                    .strip_prefix(&dir)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        out.sort();
        out
    }
}

#[tonic::async_trait]
impl crate::objectstore::ObjectStore for RecordingStore {
    async fn fetch_snapshot(
        &self,
        ctx: &crate::context::Ctx,
        req: crate::objectstore::FetchSnapshot,
    ) -> Result<(), Status> {
        self.fetches.lock().push(req.clone());
        self.inner.fetch_snapshot(ctx, req).await
    }
    async fn upload_snapshot(
        &self,
        ctx: &crate::context::Ctx,
        req: crate::objectstore::UploadSnapshot,
    ) -> Result<(), Status> {
        self.uploads.lock().push(req.clone());
        if let Some(e) = &self.put_err {
            return Err(e.clone());
        }
        self.inner.upload_snapshot(ctx, req).await
    }
    async fn fetch_asset(
        &self,
        ctx: &crate::context::Ctx,
        req: crate::objectstore::FetchAsset,
    ) -> Result<(), Status> {
        self.inner.fetch_asset(ctx, req).await
    }
}

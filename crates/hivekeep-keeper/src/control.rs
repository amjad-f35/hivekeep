//! The control plane as the keeper calls it.
//!
//! The keeper reaches `hivekeep-api` for two reasons: as the node's broker
//! for its workers (registration, certificates, suspend and crash reports,
//! egress policies) and on its own behalf (actor JWTs for snapshot
//! transfers, snapshot state reports). Both go through these traits, so
//! tests can stand in a fake control plane.

use async_trait::async_trait;
use hivekeep_proto::api::v1 as api;
use tonic::transport::Channel;
use tonic::Status;

use crate::context::Ctx;

/// The control plane's WorkerService.
#[async_trait]
pub trait WorkerServiceApi: Send + Sync + 'static {
    /// Starts a new epoch for a worker.
    async fn acquire_worker_epoch(
        &self,
        ctx: &Ctx,
        req: api::AcquireWorkerEpochRequest,
    ) -> Result<api::AcquireWorkerEpochResponse, Status>;
    /// Registers a worker's addresses and capacity.
    async fn register_worker(
        &self,
        ctx: &Ctx,
        req: api::RegisterWorkerRequest,
    ) -> Result<(), Status>;
    /// Records the sandbox runtimes a worker offers.
    async fn set_worker_sandbox_runtimes(
        &self,
        ctx: &Ctx,
        req: api::SetWorkerSandboxRuntimesRequest,
    ) -> Result<(), Status>;
    /// Mints an actor's certificate for the worker running it.
    async fn mint_worker_actor_certificate(
        &self,
        ctx: &Ctx,
        req: api::MintWorkerActorCertificateRequest,
    ) -> Result<api::MintWorkerActorCertificateResponse, Status>;
    /// Mints the JWT a snapshot transfer of an actor carries.
    async fn mint_worker_actor_jwt(
        &self,
        ctx: &Ctx,
        req: api::MintWorkerActorJwtRequest,
    ) -> Result<api::MintWorkerActorJwtResponse, Status>;
    /// Asks to suspend an actor.
    async fn request_actor_suspend(
        &self,
        ctx: &Ctx,
        req: api::RequestActorSuspendRequest,
    ) -> Result<(), Status>;
    /// Reports what the keeper holds of an actor's snapshots.
    async fn report_actor_snapshot_state(
        &self,
        ctx: &Ctx,
        req: api::ReportActorSnapshotStateRequest,
    ) -> Result<(), Status>;
    /// Reports an actor's crash.
    async fn report_actor_crashed(
        &self,
        ctx: &Ctx,
        req: api::ReportActorCrashedRequest,
    ) -> Result<(), Status>;
}

/// The part of the control plane's public API the keeper reads.
#[async_trait]
pub trait ControlApi: Send + Sync + 'static {
    /// An actor's egress policy.
    async fn get_actor_egress_policy(
        &self,
        ctx: &Ctx,
        req: api::GetActorEgressPolicyRequest,
    ) -> Result<api::EgressPolicy, Status>;
}

/// Both services over one gRPC channel. The keeper acts for the system,
/// not a user, so every request opts in to the preview features the
/// server enables.
#[derive(Clone)]
pub struct GrpcControlPlane {
    channel: Channel,
}

impl GrpcControlPlane {
    /// A client over `channel`.
    pub fn new(channel: Channel) -> Self {
        GrpcControlPlane { channel }
    }
}

/// Wraps `msg` for a call under `ctx`: its deadline becomes the call's
/// timeout, and the preview opt-in header is set.
fn request<T>(ctx: &Ctx, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    if let Some(d) = ctx.deadline() {
        req.set_timeout(d.saturating_duration_since(tokio::time::Instant::now()));
    }
    req.metadata_mut().insert(
        hivekeep_core::preview::PREVIEW_HEADER,
        tonic::metadata::MetadataValue::from_static("true"),
    );
    req
}

/// Runs a call under `ctx`.
async fn call<T, F>(ctx: &Ctx, f: F) -> Result<T, Status>
where
    F: std::future::Future<Output = Result<tonic::Response<T>, Status>>,
{
    match ctx.run(f).await {
        Ok(r) => r.map(|r| r.into_inner()),
        Err(c) => Err(c.into()),
    }
}

#[async_trait]
impl WorkerServiceApi for GrpcControlPlane {
    async fn acquire_worker_epoch(
        &self,
        ctx: &Ctx,
        req: api::AcquireWorkerEpochRequest,
    ) -> Result<api::AcquireWorkerEpochResponse, Status> {
        let mut c = api::worker_service_client::WorkerServiceClient::new(self.channel.clone());
        call(ctx, c.acquire_worker_epoch(request(ctx, req))).await
    }

    async fn register_worker(
        &self,
        ctx: &Ctx,
        req: api::RegisterWorkerRequest,
    ) -> Result<(), Status> {
        let mut c = api::worker_service_client::WorkerServiceClient::new(self.channel.clone());
        call(ctx, c.register_worker(request(ctx, req)))
            .await
            .map(|_| ())
    }

    async fn set_worker_sandbox_runtimes(
        &self,
        ctx: &Ctx,
        req: api::SetWorkerSandboxRuntimesRequest,
    ) -> Result<(), Status> {
        let mut c = api::worker_service_client::WorkerServiceClient::new(self.channel.clone());
        call(ctx, c.set_worker_sandbox_runtimes(request(ctx, req)))
            .await
            .map(|_| ())
    }

    async fn mint_worker_actor_certificate(
        &self,
        ctx: &Ctx,
        req: api::MintWorkerActorCertificateRequest,
    ) -> Result<api::MintWorkerActorCertificateResponse, Status> {
        let mut c = api::worker_service_client::WorkerServiceClient::new(self.channel.clone());
        call(ctx, c.mint_worker_actor_certificate(request(ctx, req))).await
    }

    async fn mint_worker_actor_jwt(
        &self,
        ctx: &Ctx,
        req: api::MintWorkerActorJwtRequest,
    ) -> Result<api::MintWorkerActorJwtResponse, Status> {
        let mut c = api::worker_service_client::WorkerServiceClient::new(self.channel.clone());
        call(ctx, c.mint_worker_actor_jwt(request(ctx, req))).await
    }

    async fn request_actor_suspend(
        &self,
        ctx: &Ctx,
        req: api::RequestActorSuspendRequest,
    ) -> Result<(), Status> {
        let mut c = api::worker_service_client::WorkerServiceClient::new(self.channel.clone());
        call(ctx, c.request_actor_suspend(request(ctx, req)))
            .await
            .map(|_| ())
    }

    async fn report_actor_snapshot_state(
        &self,
        ctx: &Ctx,
        req: api::ReportActorSnapshotStateRequest,
    ) -> Result<(), Status> {
        let mut c = api::worker_service_client::WorkerServiceClient::new(self.channel.clone());
        call(ctx, c.report_actor_snapshot_state(request(ctx, req)))
            .await
            .map(|_| ())
    }

    async fn report_actor_crashed(
        &self,
        ctx: &Ctx,
        req: api::ReportActorCrashedRequest,
    ) -> Result<(), Status> {
        let mut c = api::worker_service_client::WorkerServiceClient::new(self.channel.clone());
        call(ctx, c.report_actor_crashed(request(ctx, req)))
            .await
            .map(|_| ())
    }
}

#[async_trait]
impl ControlApi for GrpcControlPlane {
    async fn get_actor_egress_policy(
        &self,
        ctx: &Ctx,
        req: api::GetActorEgressPolicyRequest,
    ) -> Result<api::EgressPolicy, Status> {
        let mut c = api::control_client::ControlClient::new(self.channel.clone());
        call(ctx, c.get_actor_egress_policy(request(ctx, req))).await
    }
}

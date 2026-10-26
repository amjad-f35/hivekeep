//! The capacity relay: forwards fake-workersync's capacity reports to
//! hivekeep-api under this pod's keeper identity.
//!
//! It stands where the keeper's WorkerSupport stands in production: the
//! worker knows its capacity and the keeper relays it, because hivekeep-api
//! accepts a report only from the keeper on the Worker's node. The relay
//! decides nothing; fake-workersync names the Worker and its capacity, and
//! hivekeep-api checks the Worker is on this node.
//!
//! It serves WorkerService so the request and hivekeep-api's status pass
//! through unchanged; every other WorkerService call is Unimplemented, since
//! forwarding, say, a certificate mint would hand fake-workersync an
//! identity it does not hold.

use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_proto::api::v1 as api;
use hivekeep_proto::api::v1::worker_service_client::WorkerServiceClient;
use hivekeep_proto::api::v1::worker_service_server::WorkerService;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use super::epochs::WorkerEpochs;
use crate::tls::server::ServerTls;
use crate::tls::TlsError;

/// The part of hivekeep-api's WorkerService the relay forwards to.
#[async_trait]
pub trait CapacityClient: Send + Sync + 'static {
    /// AcquireWorkerEpoch.
    async fn acquire_worker_epoch(
        &self,
        req: api::AcquireWorkerEpochRequest,
    ) -> Result<api::AcquireWorkerEpochResponse, Status>;
    /// RegisterWorker.
    async fn register_worker(
        &self,
        req: api::RegisterWorkerRequest,
    ) -> Result<api::RegisterWorkerResponse, Status>;
    /// SetWorkerSandboxRuntimes.
    async fn set_worker_sandbox_runtimes(
        &self,
        req: api::SetWorkerSandboxRuntimesRequest,
    ) -> Result<api::SetWorkerSandboxRuntimesResponse, Status>;
}

#[async_trait]
impl CapacityClient for WorkerServiceClient<Channel> {
    async fn acquire_worker_epoch(
        &self,
        req: api::AcquireWorkerEpochRequest,
    ) -> Result<api::AcquireWorkerEpochResponse, Status> {
        WorkerServiceClient::acquire_worker_epoch(&mut self.clone(), req)
            .await
            .map(Response::into_inner)
    }

    async fn register_worker(
        &self,
        req: api::RegisterWorkerRequest,
    ) -> Result<api::RegisterWorkerResponse, Status> {
        WorkerServiceClient::register_worker(&mut self.clone(), req)
            .await
            .map(Response::into_inner)
    }

    async fn set_worker_sandbox_runtimes(
        &self,
        req: api::SetWorkerSandboxRuntimesRequest,
    ) -> Result<api::SetWorkerSandboxRuntimesResponse, Status> {
        WorkerServiceClient::set_worker_sandbox_runtimes(&mut self.clone(), req)
            .await
            .map(Response::into_inner)
    }
}

/// Forwards capacity reports.
pub struct Relay {
    client: Arc<dyn CapacityClient>,
    /// Records the epoch of each registration hivekeep-api accepts.
    epochs: Arc<WorkerEpochs>,
}

fn worker_name(r: &Option<api::ObjectRef>) -> &str {
    r.as_ref().map(|r| r.name.as_str()).unwrap_or("")
}

impl Relay {
    /// A relay forwarding to `client` and recording epochs in `epochs`.
    pub fn new(client: Arc<dyn CapacityClient>, epochs: Arc<WorkerEpochs>) -> Relay {
        Relay { client, epochs }
    }
}

#[async_trait]
impl WorkerService for Relay {
    async fn acquire_worker_epoch(
        &self,
        request: Request<api::AcquireWorkerEpochRequest>,
    ) -> Result<Response<api::AcquireWorkerEpochResponse>, Status> {
        let req = request.into_inner();
        let worker = worker_name(&req.worker).to_string();
        self.client
            .acquire_worker_epoch(req)
            .await
            .map(Response::new)
            .inspect_err(|err| {
                tracing::warn!(worker, %err, "Epoch acquire rejected");
            })
    }

    async fn register_worker(
        &self,
        request: Request<api::RegisterWorkerRequest>,
    ) -> Result<Response<api::RegisterWorkerResponse>, Status> {
        let req = request.into_inner();
        let worker = worker_name(&req.worker).to_string();
        let epoch = req.epoch;
        let capacity = req.capacity;
        let resp = self.client.register_worker(req).await.inspect_err(|err| {
            tracing::warn!(worker, %err, "Capacity report rejected");
        })?;
        self.epochs.set(&worker, epoch);
        tracing::debug!(worker, ?capacity, "Relayed capacity report");
        Ok(Response::new(resp))
    }

    async fn set_worker_sandbox_runtimes(
        &self,
        request: Request<api::SetWorkerSandboxRuntimesRequest>,
    ) -> Result<Response<api::SetWorkerSandboxRuntimesResponse>, Status> {
        let req = request.into_inner();
        let worker = worker_name(&req.worker).to_string();
        self.client
            .set_worker_sandbox_runtimes(req)
            .await
            .map(Response::new)
            .inspect_err(|err| tracing::warn!(worker, %err, "Sandbox runtimes report rejected"))
    }

    async fn mint_worker_actor_certificate(
        &self,
        _request: Request<api::MintWorkerActorCertificateRequest>,
    ) -> Result<Response<api::MintWorkerActorCertificateResponse>, Status> {
        Err(Status::unimplemented(
            "the capacity relay forwards capacity reports only",
        ))
    }

    async fn mint_worker_actor_jwt(
        &self,
        _request: Request<api::MintWorkerActorJwtRequest>,
    ) -> Result<Response<api::MintWorkerActorJwtResponse>, Status> {
        Err(Status::unimplemented(
            "the capacity relay forwards capacity reports only",
        ))
    }

    async fn request_actor_suspend(
        &self,
        _request: Request<api::RequestActorSuspendRequest>,
    ) -> Result<Response<api::RequestActorSuspendResponse>, Status> {
        Err(Status::unimplemented(
            "the capacity relay forwards capacity reports only",
        ))
    }

    async fn report_actor_snapshot_state(
        &self,
        _request: Request<api::ReportActorSnapshotStateRequest>,
    ) -> Result<Response<api::ReportActorSnapshotStateResponse>, Status> {
        Err(Status::unimplemented(
            "the capacity relay forwards capacity reports only",
        ))
    }

    async fn report_actor_crashed(
        &self,
        _request: Request<api::ReportActorCrashedRequest>,
    ) -> Result<Response<api::ReportActorCrashedResponse>, Status> {
        Err(Status::unimplemented(
            "the capacity relay forwards capacity reports only",
        ))
    }
}

/// The relay's TLS: the pod-identity certificate, and only a client whose
/// certificate chains to the pod-identity trust bundle and carries
/// `allowed`: anything else could set any Worker's capacity. The trust
/// bundle is read at each handshake, so a CA rotation is picked up without
/// a restart.
pub fn relay_tls(
    serving_bundle: &str,
    client_ca: &str,
    allowed: &str,
) -> Result<ServerTls, TlsError> {
    ServerTls::new(serving_bundle, client_ca, Some(allowed))
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;

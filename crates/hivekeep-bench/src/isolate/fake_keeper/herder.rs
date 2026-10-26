//! The herder: a keeper that answers every call with success after its
//! delay, without running or saving any workload.
//!
//! The one side effect it keeps is a placeholder object under each snapshot
//! URI a call uploads to before it returns: hivekeep-api copies an Actor's
//! snapshot when it creates a Tag, golden Tags included, and refuses to copy
//! an empty one.
//!
//! The one call back it keeps is the Actor certificate mint: before the
//! worker starts a workload with tunnelled egress, it has the keeper mint
//! the Actor's certificate from hivekeep-api, so in production every Run
//! and Restore that names an egress gateway costs hivekeep-api a mint.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hivekeep_core::snapshot::SnapshotUri;
use hivekeep_proto::api::v1 as api;
use hivekeep_proto::api::v1::worker_service_client::WorkerServiceClient;
use hivekeep_proto::keeper::v1 as kpb;
use hivekeep_proto::keeper::v1::keeper_server::Keeper;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use super::config::Delays;
use super::epochs::WorkerEpochs;
use super::storage::{bucket_prefix, ObjectWriter};

/// The one object written under each snapshot URI.
pub const PLACEHOLDER_OBJECT: &str = "fake-dataplane-placeholder";

/// The part of hivekeep-api's WorkerService the herder uses.
#[async_trait]
pub trait ActorCertMinter: Send + Sync + 'static {
    /// Mints an Actor certificate for the worker hosting it.
    async fn mint_worker_actor_certificate(
        &self,
        req: api::MintWorkerActorCertificateRequest,
    ) -> Result<api::MintWorkerActorCertificateResponse, Status>;
}

#[async_trait]
impl ActorCertMinter for WorkerServiceClient<Channel> {
    async fn mint_worker_actor_certificate(
        &self,
        req: api::MintWorkerActorCertificateRequest,
    ) -> Result<api::MintWorkerActorCertificateResponse, Status> {
        WorkerServiceClient::mint_worker_actor_certificate(&mut self.clone(), req)
            .await
            .map(Response::into_inner)
    }
}

/// A CSR for a fresh P-256 key. hivekeep-api takes only the public key from
/// it, so one serves every mint; the worker makes a key per activation, but
/// that cost is the data plane's, not hivekeep-api's.
pub fn new_actor_csr() -> Result<Vec<u8>, String> {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
        .map_err(|e| format!("while generating the actor key: {e}"))?;
    let params = rcgen::CertificateParams::new(Vec::<String>::new()).map_err(|e| e.to_string())?;
    let csr = params
        .serialize_request(&key)
        .map_err(|e| format!("while creating the actor CSR: {e}"))?;
    Ok(csr.der().to_vec())
}

/// Answers every keeper call with success after its delay.
pub struct Herder {
    /// Per-call delays.
    pub delays: Delays,
    /// Where placeholders are written.
    pub storage: Arc<dyn ObjectWriter>,
    /// Mints Actor certificates under this pod's keeper identity; `None`
    /// leaves the mint out.
    pub minter: Option<Arc<dyn ActorCertMinter>>,
    /// The CSR sent with every mint.
    pub csr: Vec<u8>,
    /// Where Run and Restore read the epoch they report; the relay records
    /// it when the Worker registers. Workers are named by their pod UID.
    pub epochs: Arc<WorkerEpochs>,
}

/// Sleeps until `start + d`.
async fn wait(start: Instant, d: Duration) {
    tokio::time::sleep_until((start + d).into()).await;
}

impl Herder {
    /// A herder with no minter and fresh epochs.
    pub fn new(delays: Delays, storage: Arc<dyn ObjectWriter>) -> Herder {
        Herder {
            delays,
            storage,
            minter: None,
            csr: Vec::new(),
            epochs: Arc::new(WorkerEpochs::new()),
        }
    }

    /// Mints the Actor's certificate when the call names an egress gateway,
    /// as the worker does before it starts the workload. Its time is
    /// hivekeep-api's, so it adds to the call's delay rather than running
    /// inside it; a failure fails the call, as tunnelled egress fails
    /// closed.
    async fn mint(
        &self,
        worker_pod_uid: &str,
        hive: &str,
        name: &str,
        uid: &str,
        gateway: Option<&kpb::EgressGateway>,
    ) -> Result<(), Status> {
        let (Some(minter), Some(_)) = (&self.minter, gateway) else {
            return Ok(());
        };
        minter
            .mint_worker_actor_certificate(api::MintWorkerActorCertificateRequest {
                actor: Some(api::ObjectRef {
                    hive: hive.into(),
                    name: name.into(),
                }),
                actor_uid: uid.into(),
                certificate_signing_request: self.csr.clone(),
                // Workers are global-scoped and named by their pod UID.
                worker: Some(api::ObjectRef {
                    hive: String::new(),
                    name: worker_pod_uid.into(),
                }),
            })
            .await
            .map(|_| ())
            .map_err(|e| {
                Status::new(
                    e.code(),
                    format!(
                        "while minting the certificate of actor {hive}/{name}: {}",
                        e.message()
                    ),
                )
            })
    }

    /// Writes the placeholder under `snapshot_uri`. It runs inside the
    /// call's delay; a write slower than a nonzero delay is logged, since
    /// the call then takes longer than configured. With no delay every
    /// write outlasts it, so there is nothing to report.
    pub async fn write_placeholder(
        &self,
        start: Instant,
        d: Duration,
        snapshot_uri: &str,
    ) -> Result<(), Status> {
        let uri = SnapshotUri::parse(snapshot_uri).map_err(|e| {
            Status::invalid_argument(format!("while parsing snapshot URI {snapshot_uri:?}: {e}"))
        })?;
        let (bucket, prefix) = bucket_prefix(&uri.prefix()).map_err(Status::invalid_argument)?;
        self.storage
            .put_object(
                &bucket,
                &format!("{prefix}{PLACEHOLDER_OBJECT}"),
                Vec::new(),
            )
            .await
            .map_err(|e| {
                Status::unavailable(format!(
                    "while writing the placeholder under {snapshot_uri}: {e}"
                ))
            })?;
        let elapsed = start.elapsed();
        if !d.is_zero() && elapsed > d {
            tracing::warn!(snapshot_uri, ?elapsed, delay = ?d, "Placeholder write outlasted the configured delay");
        }
        Ok(())
    }

    /// Run: mint, then the delay; reports the Worker's registered epoch.
    pub async fn run(&self, req: kpb::RunRequest) -> Result<kpb::RunResponse, Status> {
        self.mint(
            &req.worker_pod_uid,
            &req.hive,
            &req.actor_name,
            &req.actor_uid,
            req.egress_gateway.as_ref(),
        )
        .await?;
        wait(Instant::now(), self.delays.run).await;
        Ok(kpb::RunResponse {
            worker_epoch: self.epochs.get(&req.worker_pod_uid),
        })
    }

    /// Restore: as Run.
    pub async fn restore(&self, req: kpb::RestoreRequest) -> Result<kpb::RestoreResponse, Status> {
        self.mint(
            &req.worker_pod_uid,
            &req.hive,
            &req.actor_name,
            &req.actor_uid,
            req.egress_gateway.as_ref(),
        )
        .await?;
        wait(Instant::now(), self.delays.restore).await;
        Ok(kpb::RestoreResponse {
            worker_epoch: self.epochs.get(&req.worker_pod_uid),
        })
    }

    /// Checkpoint. An immediate upload puts the snapshot in object storage
    /// before Checkpoint returns. Deferred uploads are not modelled: the
    /// Actor's PromoteSnapshot writes the placeholder instead.
    pub async fn checkpoint(
        &self,
        req: kpb::CheckpointRequest,
    ) -> Result<kpb::CheckpointResponse, Status> {
        let start = Instant::now();
        let immediate =
            req.upload_config.as_ref().map(|c| c.mode()) == Some(kpb::UploadMode::Immediate);
        if immediate {
            let uri = snapshot_uri(req.snapshot.as_ref());
            self.write_placeholder(start, self.delays.checkpoint, uri)
                .await?;
        }
        wait(start, self.delays.checkpoint).await;
        Ok(kpb::CheckpointResponse::default())
    }

    /// PromoteSnapshot: writes the placeholder, then reports the snapshot
    /// durable and still kept on the node, as the keeper does.
    pub async fn promote_snapshot(
        &self,
        req: kpb::PromoteSnapshotRequest,
    ) -> Result<kpb::PromoteSnapshotResponse, Status> {
        let start = Instant::now();
        self.write_placeholder(
            start,
            self.delays.promote_snapshot,
            snapshot_uri(req.snapshot.as_ref()),
        )
        .await?;
        wait(start, self.delays.promote_snapshot).await;
        let report = kpb::SnapshotReport {
            snapshot: req.snapshot,
            ..Default::default()
        };
        Ok(kpb::PromoteSnapshotResponse {
            state: Some(kpb::ActorSnapshotState {
                latest_local: Some(report.clone()),
                latest_durable: Some(report),
                ..Default::default()
            }),
        })
    }

    /// Terminate: the delay only.
    pub async fn terminate(
        &self,
        _req: kpb::TerminateRequest,
    ) -> Result<kpb::TerminateResponse, Status> {
        wait(Instant::now(), self.delays.terminate).await;
        Ok(kpb::TerminateResponse::default())
    }
}

fn snapshot_uri(s: Option<&kpb::Snapshot>) -> &str {
    s.and_then(|s| s.object.as_ref())
        .map(|o| o.snapshot_uri.as_str())
        .unwrap_or("")
}

#[async_trait]
impl Keeper for Herder {
    async fn run(
        &self,
        request: Request<kpb::RunRequest>,
    ) -> Result<Response<kpb::RunResponse>, Status> {
        Herder::run(self, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn checkpoint(
        &self,
        request: Request<kpb::CheckpointRequest>,
    ) -> Result<Response<kpb::CheckpointResponse>, Status> {
        Herder::checkpoint(self, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn restore(
        &self,
        request: Request<kpb::RestoreRequest>,
    ) -> Result<Response<kpb::RestoreResponse>, Status> {
        Herder::restore(self, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn promote_snapshot(
        &self,
        request: Request<kpb::PromoteSnapshotRequest>,
    ) -> Result<Response<kpb::PromoteSnapshotResponse>, Status> {
        Herder::promote_snapshot(self, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn detach_actor(
        &self,
        _request: Request<kpb::DetachActorRequest>,
    ) -> Result<Response<kpb::DetachActorResponse>, Status> {
        Err(Status::unimplemented(
            "the fake keeper does not detach actors",
        ))
    }

    async fn terminate(
        &self,
        request: Request<kpb::TerminateRequest>,
    ) -> Result<Response<kpb::TerminateResponse>, Status> {
        Herder::terminate(self, request.into_inner())
            .await
            .map(Response::new)
    }
}

#[cfg(test)]
#[path = "herder_tests.rs"]
mod tests;

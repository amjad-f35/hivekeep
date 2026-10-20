//! The Worker half of the hivekeep-api Control API, as the syncer uses it.

use async_trait::async_trait;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::api::v1::control_client::ControlClient;
use tonic::Status;

/// The Worker registry behind hivekeep-api.
#[async_trait]
pub trait WorkerRegistry: Send + Sync + 'static {
    /// Reads a Worker.
    async fn get_worker(&self, name: &str) -> Result<pb::Worker, Status>;
    /// Registers a Worker (born UNAVAILABLE).
    async fn create_worker(&self, worker: pb::Worker) -> Result<pb::Worker, Status>;
    /// Replaces a Worker, preconditioned on its uid and version.
    async fn update_worker(&self, worker: pb::Worker) -> Result<pb::Worker, Status>;
    /// Deregisters a Worker, releasing its Actors.
    async fn delete_worker(&self, name: &str) -> Result<pb::Worker, Status>;
    /// Moves a Worker to DRAINING (idempotent).
    async fn drain_worker(&self, name: &str) -> Result<Option<pb::Worker>, Status>;
    /// Marks a Worker ACTIVE.
    async fn mark_worker_available(&self, name: &str) -> Result<Option<pb::Worker>, Status>;
    /// Marks a Worker UNAVAILABLE.
    async fn mark_worker_unavailable(&self, name: &str) -> Result<Option<pb::Worker>, Status>;
    /// One page of Workers.
    async fn list_workers(
        &self,
        page_size: i32,
        page_token: &str,
    ) -> Result<pb::ListWorkersResponse, Status>;
}

/// The controller acts for the system, not a user, so it always opts in
/// to the preview features the server enables.
fn opt_in<T>(msg: T) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut().insert(
        hivekeep_core::preview::PREVIEW_HEADER,
        tonic::metadata::MetadataValue::from_static("true"),
    );
    r
}

fn obj(name: &str) -> Option<pb::ObjectRef> {
    Some(pb::ObjectRef {
        name: name.into(),
        ..Default::default()
    })
}

#[async_trait]
impl WorkerRegistry for ControlClient<tonic::transport::Channel> {
    async fn get_worker(&self, name: &str) -> Result<pb::Worker, Status> {
        let req = pb::GetWorkerRequest { worker: obj(name) };
        Ok(ControlClient::get_worker(&mut self.clone(), opt_in(req))
            .await?
            .into_inner())
    }
    async fn create_worker(&self, worker: pb::Worker) -> Result<pb::Worker, Status> {
        let req = pb::CreateWorkerRequest {
            worker: Some(worker),
        };
        Ok(ControlClient::create_worker(&mut self.clone(), opt_in(req))
            .await?
            .into_inner())
    }
    async fn update_worker(&self, worker: pb::Worker) -> Result<pb::Worker, Status> {
        let req = pb::UpdateWorkerRequest {
            worker: Some(worker),
        };
        Ok(ControlClient::update_worker(&mut self.clone(), opt_in(req))
            .await?
            .into_inner())
    }
    async fn delete_worker(&self, name: &str) -> Result<pb::Worker, Status> {
        let req = pb::DeleteWorkerRequest {
            worker: obj(name),
            options: None,
        };
        Ok(ControlClient::delete_worker(&mut self.clone(), opt_in(req))
            .await?
            .into_inner())
    }
    async fn drain_worker(&self, name: &str) -> Result<Option<pb::Worker>, Status> {
        let req = pb::DrainWorkerRequest { worker: obj(name) };
        Ok(ControlClient::drain_worker(&mut self.clone(), opt_in(req))
            .await?
            .into_inner()
            .worker)
    }
    async fn mark_worker_available(&self, name: &str) -> Result<Option<pb::Worker>, Status> {
        let req = pb::MarkWorkerAvailableRequest { worker: obj(name) };
        Ok(
            ControlClient::mark_worker_available(&mut self.clone(), opt_in(req))
                .await?
                .into_inner()
                .worker,
        )
    }
    async fn mark_worker_unavailable(&self, name: &str) -> Result<Option<pb::Worker>, Status> {
        let req = pb::MarkWorkerUnavailableRequest { worker: obj(name) };
        Ok(
            ControlClient::mark_worker_unavailable(&mut self.clone(), opt_in(req))
                .await?
                .into_inner()
                .worker,
        )
    }
    async fn list_workers(
        &self,
        page_size: i32,
        page_token: &str,
    ) -> Result<pb::ListWorkersResponse, Status> {
        let req = pb::ListWorkersRequest {
            options: Some(pb::ListOptions {
                page_size,
                page_token: page_token.into(),
            }),
        };
        Ok(ControlClient::list_workers(&mut self.clone(), opt_in(req))
            .await?
            .into_inner())
    }
}

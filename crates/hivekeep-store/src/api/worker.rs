use async_trait::async_trait;
use hivekeep_proto::api::v1 as pb;

use super::Mutate;
use crate::{DeletePreconditions, ListOptions, ListResponse, Precondition, Result, WorkerWatch};

/// Workers. Every write publishes a [`WorkerEvent`](crate::WorkerEvent) to
/// active watches. A Worker's epochs (`status.issued_epoch`,
/// `registered_epoch`, `observed_epoch`) are advanced with
/// [`update_worker`](Self::update_worker) like any other status field.
#[async_trait]
pub trait WorkerStore: Send + Sync {
    /// Registers a Worker.
    async fn create_worker(&self, worker: &pb::Worker) -> Result<pb::Worker>;

    /// Fetches a Worker by name.
    async fn get_worker(&self, name: &str) -> Result<pb::Worker>;

    /// Lists Workers in name order.
    async fn list_workers(&self, opts: &ListOptions) -> Result<ListResponse<pb::Worker>>;

    /// Guarded read-modify-write of a Worker, serialized against every other
    /// write to it.
    async fn update_worker(
        &self,
        name: &str,
        precondition: Precondition,
        mutate: Mutate<'_, pb::Worker>,
    ) -> Result<pb::Worker>;

    /// Starts deleting a Worker.
    async fn mark_worker_for_deletion(
        &self,
        name: &str,
        precondition: DeletePreconditions,
        mutate: Option<Mutate<'_, pb::Worker>>,
    ) -> Result<pb::Worker>;

    /// Removes a Worker and every assignment it holds.
    async fn delete_worker(
        &self,
        name: &str,
        precondition: DeletePreconditions,
    ) -> Result<pb::Worker>;

    /// Subscribes to Worker changes made after this call returns.
    async fn watch_workers(&self) -> Result<WorkerWatch>;
}

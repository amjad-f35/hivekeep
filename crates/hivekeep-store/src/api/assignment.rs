use async_trait::async_trait;
use hivekeep_proto::api::v1 as pb;

use super::Admit;
use crate::{ListOptions, ListResponse, Result};

/// Which Actors each Worker hosts. An assignment and the Worker's
/// `status.allocated` total always change in the same write, and an Actor is
/// hosted by at most one Worker.
#[async_trait]
pub trait WorkerAssignmentStore: Send + Sync {
    /// Binds an Actor to a Worker and adds it to the Worker's allocation.
    /// Rebinding an Actor already on this Worker replaces its assignment
    /// (keeping the assignment's uid and create time) and swaps its
    /// reservation. Returns the stored assignment.
    ///
    /// `admit` decides whether the Worker takes the Actor; it sees the Worker
    /// as it stands, locked, with any previous reservation of this Actor
    /// already taken off. Its error refuses the bind and is returned
    /// unchanged.
    ///
    /// [`NotFound`](crate::StoreError::NotFound) if the Worker is gone;
    /// [`FailedPrecondition`](crate::StoreError::FailedPrecondition) if the
    /// Actor is hosted by another Worker.
    async fn bind_actor_to_worker(
        &self,
        worker: &str,
        assignment: &pb::ActorAssignment,
        admit: Option<Admit<'_>>,
    ) -> Result<pb::ActorAssignment>;

    /// Removes an Actor's assignment from a Worker and returns the Worker as
    /// it now stands, or `None` (writing nothing) if the Worker was not
    /// hosting it.
    async fn release_actor_from_worker(
        &self,
        worker: &str,
        actor_uid: &str,
    ) -> Result<Option<pb::Worker>>;

    /// A Worker's assignment for `actor_uid`.
    async fn get_worker_assignment(
        &self,
        worker: &str,
        actor_uid: &str,
    ) -> Result<pb::ActorAssignment>;

    /// A page of a Worker's assignments, in Actor uid order.
    async fn list_worker_assignments(
        &self,
        worker: &str,
        opts: &ListOptions,
    ) -> Result<ListResponse<pb::ActorAssignment>>;

    /// The name of the Worker hosting `actor_uid`.
    async fn find_worker_hosting_actor(&self, actor_uid: &str) -> Result<String>;
}

use async_trait::async_trait;
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;

use super::Mutate;
use crate::{DeletePreconditions, ListOptions, ListResponse, Precondition, Result};

/// Actors.
#[async_trait]
pub trait ActorStore: Send + Sync {
    /// Stores a new Actor.
    /// [`AlreadyExists`](crate::StoreError::AlreadyExists) if the key is
    /// taken, [`FailedPrecondition`](crate::StoreError::FailedPrecondition)
    /// if its Hive does not exist,
    /// [`InvalidArgument`](crate::StoreError::InvalidArgument) if a snapshot
    /// URI is malformed.
    async fn create_actor(&self, actor: &pb::Actor) -> Result<pb::Actor>;

    /// Fetches an Actor.
    async fn get_actor(&self, actor: &ActorRef) -> Result<pb::Actor>;

    /// Lists the Actors of `hive` in name order, or of every Hive in
    /// `(hive, name)` order when `hive` is empty.
    async fn list_actors(&self, hive: &str, opts: &ListOptions) -> Result<ListResponse<pb::Actor>>;

    /// Guarded read-modify-write of an Actor.
    async fn update_actor(
        &self,
        actor: &ActorRef,
        precondition: Precondition,
        mutate: Mutate<'_, pb::Actor>,
    ) -> Result<pb::Actor>;

    /// Starts deleting an Actor: sets `delete_time` if unset and applies
    /// `mutate`, if any, in the same write.
    async fn mark_actor_for_deletion(
        &self,
        actor: &ActorRef,
        precondition: DeletePreconditions,
        mutate: Option<Mutate<'_, pb::Actor>>,
    ) -> Result<pb::Actor>;

    /// Removes an Actor, and its egress policy, whatever its state.
    async fn delete_actor(
        &self,
        actor: &ActorRef,
        precondition: DeletePreconditions,
    ) -> Result<pb::Actor>;
}

use async_trait::async_trait;
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;

use super::Mutate;
use crate::{DeletePreconditions, Precondition, Result};

/// The 1:1 egress policy sub-resource of an Actor. It lives and dies with
/// its Actor: deleting the Actor deletes the policy, and a recreated Actor
/// starts without one.
#[async_trait]
pub trait EgressPolicyStore: Send + Sync {
    /// Creates the policy of an existing Actor.
    /// [`FailedPrecondition`](crate::StoreError::FailedPrecondition) if the
    /// Actor does not exist.
    async fn create_egress_policy(
        &self,
        actor: &ActorRef,
        policy: &pb::EgressPolicy,
    ) -> Result<pb::EgressPolicy>;

    /// Fetches an Actor's policy.
    async fn get_egress_policy(&self, actor: &ActorRef) -> Result<pb::EgressPolicy>;

    /// Guarded read-modify-write of an Actor's policy. The mutation cannot
    /// change any metadata.
    async fn update_egress_policy(
        &self,
        actor: &ActorRef,
        precondition: Precondition,
        mutate: Mutate<'_, pb::EgressPolicy>,
    ) -> Result<pb::EgressPolicy>;

    /// Removes an Actor's policy and returns it with `delete_time` set.
    async fn delete_egress_policy(
        &self,
        actor: &ActorRef,
        precondition: DeletePreconditions,
    ) -> Result<pb::EgressPolicy>;
}

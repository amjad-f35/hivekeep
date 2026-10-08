use async_trait::async_trait;
use hivekeep_proto::api::v1 as pb;

use super::Mutate;
use crate::{DeletePreconditions, Precondition, Result};

/// Role bindings: the deployment-wide singleton and the 1:1 policy of each
/// Hive. Deleting a Hive deletes its policy.
#[async_trait]
pub trait AccessPolicyStore: Send + Sync {
    /// Creates the global policy.
    async fn create_global_access_policy(
        &self,
        policy: &pb::AccessPolicy,
    ) -> Result<pb::AccessPolicy>;

    /// Fetches the global policy.
    async fn get_global_access_policy(&self) -> Result<pb::AccessPolicy>;

    /// Guarded read-modify-write of the global policy.
    async fn update_global_access_policy(
        &self,
        precondition: Precondition,
        mutate: Mutate<'_, pb::AccessPolicy>,
    ) -> Result<pb::AccessPolicy>;

    /// Creates the policy of an existing Hive.
    /// [`FailedPrecondition`](crate::StoreError::FailedPrecondition) if the
    /// Hive does not exist.
    async fn create_hive_access_policy(
        &self,
        hive: &str,
        policy: &pb::AccessPolicy,
    ) -> Result<pb::AccessPolicy>;

    /// Fetches a Hive's policy.
    async fn get_hive_access_policy(&self, hive: &str) -> Result<pb::AccessPolicy>;

    /// Guarded read-modify-write of a Hive's policy.
    async fn update_hive_access_policy(
        &self,
        hive: &str,
        precondition: Precondition,
        mutate: Mutate<'_, pb::AccessPolicy>,
    ) -> Result<pb::AccessPolicy>;

    /// Removes a Hive's policy and returns it with `delete_time` set.
    async fn delete_hive_access_policy(
        &self,
        hive: &str,
        precondition: DeletePreconditions,
    ) -> Result<pb::AccessPolicy>;
}

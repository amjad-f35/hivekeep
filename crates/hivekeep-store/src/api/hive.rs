use async_trait::async_trait;
use hivekeep_proto::api::v1 as pb;

use crate::{DeletePreconditions, ListOptions, ListResponse, Result};

/// Hives: the global isolation boundary every hive-scoped resource lives in.
#[async_trait]
pub trait HiveStore: Send + Sync {
    /// Stores a new Hive. [`AlreadyExists`](crate::StoreError::AlreadyExists)
    /// if the name is taken.
    async fn create_hive(&self, hive: &pb::Hive) -> Result<pb::Hive>;

    /// Fetches a Hive by name.
    async fn get_hive(&self, name: &str) -> Result<pb::Hive>;

    /// Lists Hives in name order.
    async fn list_hives(&self, opts: &ListOptions) -> Result<ListResponse<pb::Hive>>;

    /// Removes an empty Hive, along with its access policy, and returns it
    /// with `delete_time` set.
    /// [`FailedPrecondition`](crate::StoreError::FailedPrecondition) while it
    /// still holds Actors, ActorTemplates or Tags.
    async fn delete_hive(&self, name: &str, precondition: DeletePreconditions) -> Result<pb::Hive>;
}

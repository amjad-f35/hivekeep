use async_trait::async_trait;
use hivekeep_core::resource::TagRef;
use hivekeep_proto::api::v1 as pb;

use super::Mutate;
use crate::{DeletePreconditions, ListOptions, ListResponse, Precondition, Result};

/// Tags: stable, hive-owned names for one snapshot.
#[async_trait]
pub trait TagStore: Send + Sync {
    /// Stores a new Tag, typically with a PENDING snapshot (or none yet).
    /// [`AlreadyExists`](crate::StoreError::AlreadyExists) if the name is
    /// taken, including by the caller's own unfinished attempt;
    /// [`FailedPrecondition`](crate::StoreError::FailedPrecondition) if its
    /// Hive does not exist.
    async fn create_tag(&self, tag: &pb::Tag) -> Result<pb::Tag>;

    /// Fetches a Tag.
    async fn get_tag(&self, tag: &TagRef) -> Result<pb::Tag>;

    /// Lists Tags of `hive`, or of every Hive when empty.
    async fn list_tags(&self, hive: &str, opts: &ListOptions) -> Result<ListResponse<pb::Tag>>;

    /// Guarded read-modify-write of a Tag.
    ///
    /// `status.snapshot` is immutable once set, except for its durable copy
    /// moving from PENDING to COMPLETED; `status.actor_template_uid` and the
    /// name are immutable. Breaking either is
    /// [`ImmutableField`](crate::StoreError::ImmutableField).
    async fn update_tag(
        &self,
        tag: &TagRef,
        precondition: Precondition,
        mutate: Mutate<'_, pb::Tag>,
    ) -> Result<pb::Tag>;

    /// Starts deleting a Tag.
    async fn mark_tag_for_deletion(
        &self,
        tag: &TagRef,
        precondition: DeletePreconditions,
        mutate: Option<Mutate<'_, pb::Tag>>,
    ) -> Result<pb::Tag>;

    /// Removes a Tag.
    async fn delete_tag(&self, tag: &TagRef, precondition: DeletePreconditions) -> Result<pb::Tag>;
}

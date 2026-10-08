use async_trait::async_trait;
use hivekeep_core::resource::ActorTemplateRef;
use hivekeep_proto::api::v1 as pb;

use super::Mutate;
use crate::{DeletePreconditions, ListOptions, ListResponse, Precondition, Result};

/// ActorTemplates.
#[async_trait]
pub trait ActorTemplateStore: Send + Sync {
    /// Stores a new template.
    /// [`FailedPrecondition`](crate::StoreError::FailedPrecondition) if its
    /// Hive does not exist.
    async fn create_actor_template(
        &self,
        template: &pb::ActorTemplate,
    ) -> Result<pb::ActorTemplate>;

    /// Fetches a template.
    async fn get_actor_template(&self, template: &ActorTemplateRef) -> Result<pb::ActorTemplate>;

    /// Lists templates of `hive`, or of every Hive when empty.
    async fn list_actor_templates(
        &self,
        hive: &str,
        opts: &ListOptions,
    ) -> Result<ListResponse<pb::ActorTemplate>>;

    /// Guarded read-modify-write of a template.
    /// [`ImmutableField`](crate::StoreError::ImmutableField) if the mutation
    /// renames it.
    async fn update_actor_template(
        &self,
        template: &ActorTemplateRef,
        precondition: Precondition,
        mutate: Mutate<'_, pb::ActorTemplate>,
    ) -> Result<pb::ActorTemplate>;

    /// Starts deleting a template.
    async fn mark_actor_template_for_deletion(
        &self,
        template: &ActorTemplateRef,
        precondition: DeletePreconditions,
        mutate: Option<Mutate<'_, pb::ActorTemplate>>,
    ) -> Result<pb::ActorTemplate>;

    /// Removes a template.
    async fn delete_actor_template(
        &self,
        template: &ActorTemplateRef,
        precondition: DeletePreconditions,
    ) -> Result<pb::ActorTemplate>;
}

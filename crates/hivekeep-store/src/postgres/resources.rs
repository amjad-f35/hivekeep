//! Hive-scoped resources and access policies.

use async_trait::async_trait;
use hivekeep_core::resource::{ActorRef, ActorTemplateRef, TagRef};
use hivekeep_proto::api::v1 as pb;

use super::codec::db;
use super::table::{self, *};
use super::PgStore;
use crate::api::*;
use crate::page_token::ListKind;
use crate::resource::{now, Resource};
use crate::{DeletePreconditions, ListOptions, ListResponse, Precondition, Result};

#[async_trait]
impl HiveStore for PgStore {
    async fn create_hive(&self, hive: &pb::Hive) -> Result<pb::Hive> {
        let mut c = self.conn().await?;
        table::insert(&mut c, HIVES, &[hive.name()], hive).await
    }

    async fn get_hive(&self, name: &str) -> Result<pb::Hive> {
        table::get(&mut *self.conn().await?, HIVES, &[name], false).await
    }

    async fn list_hives(&self, opts: &ListOptions) -> Result<ListResponse<pb::Hive>> {
        table::list(&mut *self.conn().await?, HIVES, "", opts, ListKind::Hive).await
    }

    async fn delete_hive(&self, name: &str, precondition: DeletePreconditions) -> Result<pb::Hive> {
        // The foreign keys refuse the delete while the Hive holds Actors,
        // ActorTemplates or Tags, and take its access policy with it.
        let mut c = self.conn().await?;
        let mut removed: pb::Hive = table::delete(&mut c, HIVES, &[name], &precondition).await?;
        removed.metadata_mut().delete_time = Some(now());
        Ok(removed)
    }
}

#[async_trait]
impl ActorStore for PgStore {
    async fn create_actor(&self, actor: &pb::Actor) -> Result<pb::Actor> {
        let mut c = self.conn().await?;
        table::insert(&mut c, ACTORS, &[actor.hive(), actor.name()], actor).await
    }

    async fn get_actor(&self, actor: &ActorRef) -> Result<pb::Actor> {
        table::get(
            &mut *self.conn().await?,
            ACTORS,
            &[&actor.hive, &actor.name],
            false,
        )
        .await
    }

    async fn list_actors(&self, hive: &str, opts: &ListOptions) -> Result<ListResponse<pb::Actor>> {
        table::list(
            &mut *self.conn().await?,
            ACTORS,
            hive,
            opts,
            ListKind::Actor,
        )
        .await
    }

    async fn update_actor(
        &self,
        actor: &ActorRef,
        precondition: Precondition,
        mutate: Mutate<'_, pb::Actor>,
    ) -> Result<pb::Actor> {
        precondition.validate()?;
        let mut c = self.conn().await?;
        table::update(
            &mut c,
            ACTORS,
            &[&actor.hive, &actor.name],
            &precondition.into(),
            Some(mutate),
            false,
        )
        .await
    }

    async fn mark_actor_for_deletion(
        &self,
        actor: &ActorRef,
        precondition: DeletePreconditions,
        mutate: Option<Mutate<'_, pb::Actor>>,
    ) -> Result<pb::Actor> {
        let mut c = self.conn().await?;
        table::update(
            &mut c,
            ACTORS,
            &[&actor.hive, &actor.name],
            &precondition,
            mutate,
            true,
        )
        .await
    }

    async fn delete_actor(
        &self,
        actor: &ActorRef,
        precondition: DeletePreconditions,
    ) -> Result<pb::Actor> {
        let mut c = self.conn().await?;
        table::delete(&mut c, ACTORS, &[&actor.hive, &actor.name], &precondition).await
    }
}

#[async_trait]
impl EgressPolicyStore for PgStore {
    async fn create_egress_policy(
        &self,
        actor: &ActorRef,
        policy: &pb::EgressPolicy,
    ) -> Result<pb::EgressPolicy> {
        let mut c = self.conn().await?;
        table::insert(&mut c, EGRESS_POLICIES, &[&actor.hive, &actor.name], policy).await
    }

    async fn get_egress_policy(&self, actor: &ActorRef) -> Result<pb::EgressPolicy> {
        table::get(
            &mut *self.conn().await?,
            EGRESS_POLICIES,
            &[&actor.hive, &actor.name],
            false,
        )
        .await
    }

    async fn update_egress_policy(
        &self,
        actor: &ActorRef,
        precondition: Precondition,
        mutate: Mutate<'_, pb::EgressPolicy>,
    ) -> Result<pb::EgressPolicy> {
        precondition.validate()?;
        let mut c = self.conn().await?;
        let keys = [actor.hive.as_str(), actor.name.as_str()];
        table::update(
            &mut c,
            EGRESS_POLICIES,
            &keys,
            &precondition.into(),
            Some(mutate),
            false,
        )
        .await
    }

    async fn delete_egress_policy(
        &self,
        actor: &ActorRef,
        precondition: DeletePreconditions,
    ) -> Result<pb::EgressPolicy> {
        let mut c = self.conn().await?;
        let mut removed: pb::EgressPolicy = table::delete(
            &mut c,
            EGRESS_POLICIES,
            &[&actor.hive, &actor.name],
            &precondition,
        )
        .await?;
        removed.metadata_mut().delete_time = Some(now());
        Ok(removed)
    }
}

#[async_trait]
impl AccessPolicyStore for PgStore {
    async fn create_global_access_policy(
        &self,
        policy: &pb::AccessPolicy,
    ) -> Result<pb::AccessPolicy> {
        let mut c = self.conn().await?;
        table::insert(&mut c, GLOBAL_ACCESS_POLICY, &[], policy).await
    }

    async fn get_global_access_policy(&self) -> Result<pb::AccessPolicy> {
        table::get(&mut *self.conn().await?, GLOBAL_ACCESS_POLICY, &[], false).await
    }

    async fn update_global_access_policy(
        &self,
        precondition: Precondition,
        mutate: Mutate<'_, pb::AccessPolicy>,
    ) -> Result<pb::AccessPolicy> {
        precondition.validate()?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        let out = table::update_locked(
            &mut tx,
            GLOBAL_ACCESS_POLICY,
            &[],
            &precondition.into(),
            Some(mutate),
            false,
        )
        .await?;
        tx.commit().await.map_err(db)?;
        Ok(out)
    }

    async fn create_hive_access_policy(
        &self,
        hive: &str,
        policy: &pb::AccessPolicy,
    ) -> Result<pb::AccessPolicy> {
        let mut c = self.conn().await?;
        table::insert(&mut c, HIVE_ACCESS_POLICIES, &[hive], policy).await
    }

    async fn get_hive_access_policy(&self, hive: &str) -> Result<pb::AccessPolicy> {
        table::get(
            &mut *self.conn().await?,
            HIVE_ACCESS_POLICIES,
            &[hive],
            false,
        )
        .await
    }

    async fn update_hive_access_policy(
        &self,
        hive: &str,
        precondition: Precondition,
        mutate: Mutate<'_, pb::AccessPolicy>,
    ) -> Result<pb::AccessPolicy> {
        precondition.validate()?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        let out = table::update_locked(
            &mut tx,
            HIVE_ACCESS_POLICIES,
            &[hive],
            &precondition.into(),
            Some(mutate),
            false,
        )
        .await?;
        tx.commit().await.map_err(db)?;
        Ok(out)
    }

    async fn delete_hive_access_policy(
        &self,
        hive: &str,
        precondition: DeletePreconditions,
    ) -> Result<pb::AccessPolicy> {
        let mut c = self.conn().await?;
        let mut removed: pb::AccessPolicy =
            table::delete(&mut c, HIVE_ACCESS_POLICIES, &[hive], &precondition).await?;
        removed.metadata_mut().delete_time = Some(now());
        Ok(removed)
    }
}

#[async_trait]
impl ActorTemplateStore for PgStore {
    async fn create_actor_template(
        &self,
        template: &pb::ActorTemplate,
    ) -> Result<pb::ActorTemplate> {
        let mut c = self.conn().await?;
        table::insert(
            &mut c,
            TEMPLATES,
            &[template.hive(), template.name()],
            template,
        )
        .await
    }

    async fn get_actor_template(&self, template: &ActorTemplateRef) -> Result<pb::ActorTemplate> {
        table::get(
            &mut *self.conn().await?,
            TEMPLATES,
            &[&template.hive, &template.name],
            false,
        )
        .await
    }

    async fn list_actor_templates(
        &self,
        hive: &str,
        opts: &ListOptions,
    ) -> Result<ListResponse<pb::ActorTemplate>> {
        table::list(
            &mut *self.conn().await?,
            TEMPLATES,
            hive,
            opts,
            ListKind::ActorTemplate,
        )
        .await
    }

    async fn update_actor_template(
        &self,
        template: &ActorTemplateRef,
        precondition: Precondition,
        mutate: Mutate<'_, pb::ActorTemplate>,
    ) -> Result<pb::ActorTemplate> {
        precondition.validate()?;
        let mut c = self.conn().await?;
        let keys = [template.hive.as_str(), template.name.as_str()];
        table::update(
            &mut c,
            TEMPLATES,
            &keys,
            &precondition.into(),
            Some(mutate),
            false,
        )
        .await
    }

    async fn mark_actor_template_for_deletion(
        &self,
        template: &ActorTemplateRef,
        precondition: DeletePreconditions,
        mutate: Option<Mutate<'_, pb::ActorTemplate>>,
    ) -> Result<pb::ActorTemplate> {
        let mut c = self.conn().await?;
        table::update(
            &mut c,
            TEMPLATES,
            &[&template.hive, &template.name],
            &precondition,
            mutate,
            true,
        )
        .await
    }

    async fn delete_actor_template(
        &self,
        template: &ActorTemplateRef,
        precondition: DeletePreconditions,
    ) -> Result<pb::ActorTemplate> {
        let mut c = self.conn().await?;
        table::delete(
            &mut c,
            TEMPLATES,
            &[&template.hive, &template.name],
            &precondition,
        )
        .await
    }
}

#[async_trait]
impl TagStore for PgStore {
    async fn create_tag(&self, tag: &pb::Tag) -> Result<pb::Tag> {
        let mut c = self.conn().await?;
        table::insert(&mut c, TAGS, &[tag.hive(), tag.name()], tag).await
    }

    async fn get_tag(&self, tag: &TagRef) -> Result<pb::Tag> {
        table::get(
            &mut *self.conn().await?,
            TAGS,
            &[&tag.hive, &tag.name],
            false,
        )
        .await
    }

    async fn list_tags(&self, hive: &str, opts: &ListOptions) -> Result<ListResponse<pb::Tag>> {
        table::list(&mut *self.conn().await?, TAGS, hive, opts, ListKind::Tag).await
    }

    async fn update_tag(
        &self,
        tag: &TagRef,
        precondition: Precondition,
        mutate: Mutate<'_, pb::Tag>,
    ) -> Result<pb::Tag> {
        precondition.validate()?;
        let mut c = self.conn().await?;
        table::update(
            &mut c,
            TAGS,
            &[&tag.hive, &tag.name],
            &precondition.into(),
            Some(mutate),
            false,
        )
        .await
    }

    async fn mark_tag_for_deletion(
        &self,
        tag: &TagRef,
        precondition: DeletePreconditions,
        mutate: Option<Mutate<'_, pb::Tag>>,
    ) -> Result<pb::Tag> {
        let mut c = self.conn().await?;
        table::update(
            &mut c,
            TAGS,
            &[&tag.hive, &tag.name],
            &precondition,
            mutate,
            true,
        )
        .await
    }

    async fn delete_tag(&self, tag: &TagRef, precondition: DeletePreconditions) -> Result<pb::Tag> {
        let mut c = self.conn().await?;
        table::delete(&mut c, TAGS, &[&tag.hive, &tag.name], &precondition).await
    }
}

//! Hive-scoped resources and access policies.

use std::collections::BTreeMap;

use async_trait::async_trait;
use hivekeep_core::resource::{ActorRef, ActorTemplateRef, TagRef};
use hivekeep_proto::api::v1 as pb;

use super::table::{self, read};
use super::{key, Key, MemoryStore};
use crate::api::*;
use crate::page_token::ListKind;
use crate::resource::{now, Resource};
use crate::{DeletePreconditions, ListOptions, ListResponse, Precondition, Result, StoreError};

/// Lists one Hive's records (keyed by name) or every Hive's (keyed by
/// `(hive, name)`).
fn list_scoped<T: Resource>(
    table: &BTreeMap<Key, T>,
    hive: &str,
    opts: &ListOptions,
    kind: ListKind,
) -> Result<ListResponse<T>> {
    if hive.is_empty() {
        let rows = table
            .iter()
            .map(|((h, n), v)| (vec![h.clone(), n.clone()], read(v)));
        table::paginate(rows, opts, kind, "", 2)
    } else {
        let rows = table
            .range(key(hive, "")..)
            .take_while(|((h, _), _)| h == hive)
            .map(|((_, n), v)| (vec![n.clone()], read(v)));
        table::paginate(rows, opts, kind, hive, 1)
    }
}

/// Whether `table` holds any record of `hive`.
fn in_hive<T>(table: &BTreeMap<Key, T>, hive: &str) -> bool {
    table
        .range(key(hive, "")..)
        .next()
        .is_some_and(|((h, _), _)| h == hive)
}

#[async_trait]
impl HiveStore for MemoryStore {
    async fn create_hive(&self, hive: &pb::Hive) -> Result<pb::Hive> {
        let mut st = self.state.lock();
        table::insert(&mut st.hives, hive.name().to_string(), hive)
    }

    async fn get_hive(&self, name: &str) -> Result<pb::Hive> {
        table::get(&self.state.lock().hives, &name.to_string())
    }

    async fn list_hives(&self, opts: &ListOptions) -> Result<ListResponse<pb::Hive>> {
        let st = self.state.lock();
        let rows = st.hives.iter().map(|(n, v)| (vec![n.clone()], read(v)));
        table::paginate(rows, opts, ListKind::Hive, "", 1)
    }

    async fn delete_hive(&self, name: &str, precondition: DeletePreconditions) -> Result<pb::Hive> {
        let mut st = self.state.lock();
        let stored = st.hives.get(name).ok_or(StoreError::NotFound)?;
        precondition.check(stored.metadata())?;
        let occupied =
            in_hive(&st.actors, name) || in_hive(&st.templates, name) || in_hive(&st.tags, name);
        if occupied {
            return Err(StoreError::FailedPrecondition);
        }
        st.hive_access_policies.remove(name);
        let mut removed = read(&st.hives.remove(name).expect("checked above"));
        removed.metadata_mut().delete_time = Some(now());
        Ok(removed)
    }
}

#[async_trait]
impl ActorStore for MemoryStore {
    async fn create_actor(&self, actor: &pb::Actor) -> Result<pb::Actor> {
        let mut st = self.state.lock();
        let k = key(actor.hive(), actor.name());
        if st.actors.contains_key(&k) {
            return Err(StoreError::AlreadyExists);
        }
        if !st.hives.contains_key(actor.hive()) {
            return Err(StoreError::FailedPrecondition);
        }
        table::insert(&mut st.actors, k, actor)
    }

    async fn get_actor(&self, actor: &ActorRef) -> Result<pb::Actor> {
        table::get(&self.state.lock().actors, &key(&actor.hive, &actor.name))
    }

    async fn list_actors(&self, hive: &str, opts: &ListOptions) -> Result<ListResponse<pb::Actor>> {
        list_scoped(&self.state.lock().actors, hive, opts, ListKind::Actor)
    }

    async fn update_actor(
        &self,
        actor: &ActorRef,
        precondition: Precondition,
        mutate: Mutate<'_, pb::Actor>,
    ) -> Result<pb::Actor> {
        precondition.validate()?;
        let mut st = self.state.lock();
        table::update(
            &mut st.actors,
            &key(&actor.hive, &actor.name),
            &precondition.into(),
            Some(mutate),
            false,
        )
    }

    async fn mark_actor_for_deletion(
        &self,
        actor: &ActorRef,
        precondition: DeletePreconditions,
        mutate: Option<Mutate<'_, pb::Actor>>,
    ) -> Result<pb::Actor> {
        let mut st = self.state.lock();
        table::update(
            &mut st.actors,
            &key(&actor.hive, &actor.name),
            &precondition,
            mutate,
            true,
        )
    }

    async fn delete_actor(
        &self,
        actor: &ActorRef,
        precondition: DeletePreconditions,
    ) -> Result<pb::Actor> {
        let mut st = self.state.lock();
        let k = key(&actor.hive, &actor.name);
        let removed = table::remove(&mut st.actors, &k, &precondition)?;
        st.egress_policies.remove(&k);
        Ok(removed)
    }
}

#[async_trait]
impl EgressPolicyStore for MemoryStore {
    async fn create_egress_policy(
        &self,
        actor: &ActorRef,
        policy: &pb::EgressPolicy,
    ) -> Result<pb::EgressPolicy> {
        let mut st = self.state.lock();
        let k = key(&actor.hive, &actor.name);
        if st.egress_policies.contains_key(&k) {
            return Err(StoreError::AlreadyExists);
        }
        if !st.actors.contains_key(&k) {
            return Err(StoreError::FailedPrecondition);
        }
        table::insert(&mut st.egress_policies, k, policy)
    }

    async fn get_egress_policy(&self, actor: &ActorRef) -> Result<pb::EgressPolicy> {
        table::get(
            &self.state.lock().egress_policies,
            &key(&actor.hive, &actor.name),
        )
    }

    async fn update_egress_policy(
        &self,
        actor: &ActorRef,
        precondition: Precondition,
        mutate: Mutate<'_, pb::EgressPolicy>,
    ) -> Result<pb::EgressPolicy> {
        precondition.validate()?;
        let mut st = self.state.lock();
        table::update(
            &mut st.egress_policies,
            &key(&actor.hive, &actor.name),
            &precondition.into(),
            Some(mutate),
            false,
        )
    }

    async fn delete_egress_policy(
        &self,
        actor: &ActorRef,
        precondition: DeletePreconditions,
    ) -> Result<pb::EgressPolicy> {
        let mut st = self.state.lock();
        let mut removed = table::remove(
            &mut st.egress_policies,
            &key(&actor.hive, &actor.name),
            &precondition,
        )?;
        removed.metadata_mut().delete_time = Some(now());
        Ok(removed)
    }
}

#[async_trait]
impl AccessPolicyStore for MemoryStore {
    async fn create_global_access_policy(
        &self,
        policy: &pb::AccessPolicy,
    ) -> Result<pb::AccessPolicy> {
        let mut st = self.state.lock();
        if st.global_access_policy.is_some() {
            return Err(StoreError::AlreadyExists);
        }
        let msg = crate::resource::prepare_create(policy)?;
        st.global_access_policy = Some(msg.clone());
        Ok(msg)
    }

    async fn get_global_access_policy(&self) -> Result<pb::AccessPolicy> {
        self.state
            .lock()
            .global_access_policy
            .as_ref()
            .map(read)
            .ok_or(StoreError::NotFound)
    }

    async fn update_global_access_policy(
        &self,
        precondition: Precondition,
        mutate: Mutate<'_, pb::AccessPolicy>,
    ) -> Result<pb::AccessPolicy> {
        precondition.validate()?;
        let mut st = self.state.lock();
        let stored = st
            .global_access_policy
            .as_ref()
            .map(read)
            .ok_or(StoreError::NotFound)?;
        let next =
            crate::resource::prepare_update(&stored, &precondition.into(), Some(mutate), false)?;
        st.global_access_policy = Some(next.clone());
        Ok(next)
    }

    async fn create_hive_access_policy(
        &self,
        hive: &str,
        policy: &pb::AccessPolicy,
    ) -> Result<pb::AccessPolicy> {
        let mut st = self.state.lock();
        if st.hive_access_policies.contains_key(hive) {
            return Err(StoreError::AlreadyExists);
        }
        if !st.hives.contains_key(hive) {
            return Err(StoreError::FailedPrecondition);
        }
        table::insert(&mut st.hive_access_policies, hive.to_string(), policy)
    }

    async fn get_hive_access_policy(&self, hive: &str) -> Result<pb::AccessPolicy> {
        table::get(&self.state.lock().hive_access_policies, &hive.to_string())
    }

    async fn update_hive_access_policy(
        &self,
        hive: &str,
        precondition: Precondition,
        mutate: Mutate<'_, pb::AccessPolicy>,
    ) -> Result<pb::AccessPolicy> {
        precondition.validate()?;
        let mut st = self.state.lock();
        table::update(
            &mut st.hive_access_policies,
            &hive.to_string(),
            &precondition.into(),
            Some(mutate),
            false,
        )
    }

    async fn delete_hive_access_policy(
        &self,
        hive: &str,
        precondition: DeletePreconditions,
    ) -> Result<pb::AccessPolicy> {
        let mut st = self.state.lock();
        let mut removed = table::remove(
            &mut st.hive_access_policies,
            &hive.to_string(),
            &precondition,
        )?;
        removed.metadata_mut().delete_time = Some(now());
        Ok(removed)
    }
}

#[async_trait]
impl ActorTemplateStore for MemoryStore {
    async fn create_actor_template(
        &self,
        template: &pb::ActorTemplate,
    ) -> Result<pb::ActorTemplate> {
        let mut st = self.state.lock();
        let k = key(template.hive(), template.name());
        if st.templates.contains_key(&k) {
            return Err(StoreError::AlreadyExists);
        }
        if !st.hives.contains_key(template.hive()) {
            return Err(StoreError::FailedPrecondition);
        }
        table::insert(&mut st.templates, k, template)
    }

    async fn get_actor_template(&self, template: &ActorTemplateRef) -> Result<pb::ActorTemplate> {
        table::get(
            &self.state.lock().templates,
            &key(&template.hive, &template.name),
        )
    }

    async fn list_actor_templates(
        &self,
        hive: &str,
        opts: &ListOptions,
    ) -> Result<ListResponse<pb::ActorTemplate>> {
        list_scoped(
            &self.state.lock().templates,
            hive,
            opts,
            ListKind::ActorTemplate,
        )
    }

    async fn update_actor_template(
        &self,
        template: &ActorTemplateRef,
        precondition: Precondition,
        mutate: Mutate<'_, pb::ActorTemplate>,
    ) -> Result<pb::ActorTemplate> {
        precondition.validate()?;
        let mut st = self.state.lock();
        table::update(
            &mut st.templates,
            &key(&template.hive, &template.name),
            &precondition.into(),
            Some(mutate),
            false,
        )
    }

    async fn mark_actor_template_for_deletion(
        &self,
        template: &ActorTemplateRef,
        precondition: DeletePreconditions,
        mutate: Option<Mutate<'_, pb::ActorTemplate>>,
    ) -> Result<pb::ActorTemplate> {
        let mut st = self.state.lock();
        table::update(
            &mut st.templates,
            &key(&template.hive, &template.name),
            &precondition,
            mutate,
            true,
        )
    }

    async fn delete_actor_template(
        &self,
        template: &ActorTemplateRef,
        precondition: DeletePreconditions,
    ) -> Result<pb::ActorTemplate> {
        let mut st = self.state.lock();
        table::remove(
            &mut st.templates,
            &key(&template.hive, &template.name),
            &precondition,
        )
    }
}

#[async_trait]
impl TagStore for MemoryStore {
    async fn create_tag(&self, tag: &pb::Tag) -> Result<pb::Tag> {
        let mut st = self.state.lock();
        let k = key(tag.hive(), tag.name());
        if st.tags.contains_key(&k) {
            return Err(StoreError::AlreadyExists);
        }
        if !st.hives.contains_key(tag.hive()) {
            return Err(StoreError::FailedPrecondition);
        }
        table::insert(&mut st.tags, k, tag)
    }

    async fn get_tag(&self, tag: &TagRef) -> Result<pb::Tag> {
        table::get(&self.state.lock().tags, &key(&tag.hive, &tag.name))
    }

    async fn list_tags(&self, hive: &str, opts: &ListOptions) -> Result<ListResponse<pb::Tag>> {
        list_scoped(&self.state.lock().tags, hive, opts, ListKind::Tag)
    }

    async fn update_tag(
        &self,
        tag: &TagRef,
        precondition: Precondition,
        mutate: Mutate<'_, pb::Tag>,
    ) -> Result<pb::Tag> {
        precondition.validate()?;
        let mut st = self.state.lock();
        table::update(
            &mut st.tags,
            &key(&tag.hive, &tag.name),
            &precondition.into(),
            Some(mutate),
            false,
        )
    }

    async fn mark_tag_for_deletion(
        &self,
        tag: &TagRef,
        precondition: DeletePreconditions,
        mutate: Option<Mutate<'_, pb::Tag>>,
    ) -> Result<pb::Tag> {
        let mut st = self.state.lock();
        table::update(
            &mut st.tags,
            &key(&tag.hive, &tag.name),
            &precondition,
            mutate,
            true,
        )
    }

    async fn delete_tag(&self, tag: &TagRef, precondition: DeletePreconditions) -> Result<pb::Tag> {
        let mut st = self.state.lock();
        table::remove(&mut st.tags, &key(&tag.hive, &tag.name), &precondition)
    }
}

//! A store that delegates to `MemoryStore` and can run hooks before, or
//! inject failures into, any method, and that records the lease tokens it
//! hands out so tests can make a holder lose its lease.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use hivekeep_core::resource::{ActorRef, ActorTemplateRef, TagRef};
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::*;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

pub type Hook = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;
pub type ErrFn = Box<dyn Fn() -> StoreError + Send + Sync>;

#[derive(Default)]
struct Faults {
    before: HashMap<&'static str, Vec<Hook>>,
    fail: HashMap<&'static str, Vec<ErrFn>>,
    fail_always: HashMap<&'static str, ErrFn>,
    calls: HashMap<&'static str, usize>,
}

pub struct FaultStore {
    pub inner: Arc<MemoryStore>,
    faults: Mutex<Faults>,
    leases: Mutex<HashMap<String, CancellationToken>>,
}

impl FaultStore {
    pub fn new(inner: Arc<MemoryStore>) -> Self {
        FaultStore {
            inner,
            faults: Mutex::new(Faults::default()),
            leases: Mutex::new(HashMap::new()),
        }
    }

    /// Runs `hook` once, right before the next call of `method`.
    pub fn before(&self, method: &'static str, hook: Hook) {
        self.faults
            .lock()
            .before
            .entry(method)
            .or_default()
            .push(hook);
    }

    /// Fails the next call of `method` with `err()`.
    pub fn fail_next(
        &self,
        method: &'static str,
        err: impl Fn() -> StoreError + Send + Sync + 'static,
    ) {
        self.faults
            .lock()
            .fail
            .entry(method)
            .or_default()
            .push(Box::new(err));
    }

    /// Fails every call of `method` until cleared.
    pub fn fail_always(
        &self,
        method: &'static str,
        err: impl Fn() -> StoreError + Send + Sync + 'static,
    ) {
        self.faults.lock().fail_always.insert(method, Box::new(err));
    }

    pub fn clear(&self, method: &'static str) {
        let mut f = self.faults.lock();
        f.fail_always.remove(method);
        f.fail.remove(method);
    }

    pub fn calls(&self, method: &'static str) -> usize {
        self.faults.lock().calls.get(method).copied().unwrap_or(0)
    }

    /// Makes whoever holds the lease `key` lose it.
    pub fn lose_lease(&self, key: &str) {
        if let Some(t) = self.leases.lock().get(key) {
            t.cancel();
        }
    }

    async fn gate(&self, method: &'static str) -> Result<()> {
        let hook = {
            let mut f = self.faults.lock();
            *f.calls.entry(method).or_default() += 1;
            f.before
                .get_mut(method)
                .and_then(|v| (!v.is_empty()).then(|| v.remove(0)))
        };
        if let Some(h) = hook {
            h().await;
        }
        let mut f = self.faults.lock();
        if let Some(e) = f.fail_always.get(method) {
            return Err(e());
        }
        if let Some(v) = f.fail.get_mut(method) {
            if !v.is_empty() {
                return Err(v.remove(0)());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl HiveStore for FaultStore {
    async fn create_hive(&self, h: &pb::Hive) -> Result<pb::Hive> {
        self.gate("create_hive").await?;
        self.inner.create_hive(h).await
    }
    async fn get_hive(&self, n: &str) -> Result<pb::Hive> {
        self.gate("get_hive").await?;
        self.inner.get_hive(n).await
    }
    async fn list_hives(&self, o: &ListOptions) -> Result<ListResponse<pb::Hive>> {
        self.gate("list_hives").await?;
        self.inner.list_hives(o).await
    }
    async fn delete_hive(&self, n: &str, p: DeletePreconditions) -> Result<pb::Hive> {
        self.gate("delete_hive").await?;
        self.inner.delete_hive(n, p).await
    }
}

#[async_trait]
impl ActorStore for FaultStore {
    async fn create_actor(&self, a: &pb::Actor) -> Result<pb::Actor> {
        self.gate("create_actor").await?;
        self.inner.create_actor(a).await
    }
    async fn get_actor(&self, r: &ActorRef) -> Result<pb::Actor> {
        self.gate("get_actor").await?;
        self.inner.get_actor(r).await
    }
    async fn list_actors(&self, h: &str, o: &ListOptions) -> Result<ListResponse<pb::Actor>> {
        self.gate("list_actors").await?;
        self.inner.list_actors(h, o).await
    }
    async fn update_actor(
        &self,
        r: &ActorRef,
        p: Precondition,
        m: Mutate<'_, pb::Actor>,
    ) -> Result<pb::Actor> {
        self.gate("update_actor").await?;
        self.inner.update_actor(r, p, m).await
    }
    async fn mark_actor_for_deletion(
        &self,
        r: &ActorRef,
        p: DeletePreconditions,
        m: Option<Mutate<'_, pb::Actor>>,
    ) -> Result<pb::Actor> {
        self.gate("mark_actor_for_deletion").await?;
        self.inner.mark_actor_for_deletion(r, p, m).await
    }
    async fn delete_actor(&self, r: &ActorRef, p: DeletePreconditions) -> Result<pb::Actor> {
        self.gate("delete_actor").await?;
        self.inner.delete_actor(r, p).await
    }
}

#[async_trait]
impl EgressPolicyStore for FaultStore {
    async fn create_egress_policy(
        &self,
        r: &ActorRef,
        p: &pb::EgressPolicy,
    ) -> Result<pb::EgressPolicy> {
        self.gate("create_egress_policy").await?;
        self.inner.create_egress_policy(r, p).await
    }
    async fn get_egress_policy(&self, r: &ActorRef) -> Result<pb::EgressPolicy> {
        self.gate("get_egress_policy").await?;
        self.inner.get_egress_policy(r).await
    }
    async fn update_egress_policy(
        &self,
        r: &ActorRef,
        p: Precondition,
        m: Mutate<'_, pb::EgressPolicy>,
    ) -> Result<pb::EgressPolicy> {
        self.gate("update_egress_policy").await?;
        self.inner.update_egress_policy(r, p, m).await
    }
    async fn delete_egress_policy(
        &self,
        r: &ActorRef,
        p: DeletePreconditions,
    ) -> Result<pb::EgressPolicy> {
        self.gate("delete_egress_policy").await?;
        self.inner.delete_egress_policy(r, p).await
    }
}

#[async_trait]
impl AccessPolicyStore for FaultStore {
    async fn create_global_access_policy(&self, p: &pb::AccessPolicy) -> Result<pb::AccessPolicy> {
        self.gate("create_global_access_policy").await?;
        self.inner.create_global_access_policy(p).await
    }
    async fn get_global_access_policy(&self) -> Result<pb::AccessPolicy> {
        self.gate("get_global_access_policy").await?;
        self.inner.get_global_access_policy().await
    }
    async fn update_global_access_policy(
        &self,
        p: Precondition,
        m: Mutate<'_, pb::AccessPolicy>,
    ) -> Result<pb::AccessPolicy> {
        self.gate("update_global_access_policy").await?;
        self.inner.update_global_access_policy(p, m).await
    }
    async fn create_hive_access_policy(
        &self,
        h: &str,
        p: &pb::AccessPolicy,
    ) -> Result<pb::AccessPolicy> {
        self.gate("create_hive_access_policy").await?;
        self.inner.create_hive_access_policy(h, p).await
    }
    async fn get_hive_access_policy(&self, h: &str) -> Result<pb::AccessPolicy> {
        self.gate("get_hive_access_policy").await?;
        self.inner.get_hive_access_policy(h).await
    }
    async fn update_hive_access_policy(
        &self,
        h: &str,
        p: Precondition,
        m: Mutate<'_, pb::AccessPolicy>,
    ) -> Result<pb::AccessPolicy> {
        self.gate("update_hive_access_policy").await?;
        self.inner.update_hive_access_policy(h, p, m).await
    }
    async fn delete_hive_access_policy(
        &self,
        h: &str,
        p: DeletePreconditions,
    ) -> Result<pb::AccessPolicy> {
        self.gate("delete_hive_access_policy").await?;
        self.inner.delete_hive_access_policy(h, p).await
    }
}

#[async_trait]
impl ActorTemplateStore for FaultStore {
    async fn create_actor_template(&self, t: &pb::ActorTemplate) -> Result<pb::ActorTemplate> {
        self.gate("create_actor_template").await?;
        self.inner.create_actor_template(t).await
    }
    async fn get_actor_template(&self, r: &ActorTemplateRef) -> Result<pb::ActorTemplate> {
        self.gate("get_actor_template").await?;
        self.inner.get_actor_template(r).await
    }
    async fn list_actor_templates(
        &self,
        h: &str,
        o: &ListOptions,
    ) -> Result<ListResponse<pb::ActorTemplate>> {
        self.gate("list_actor_templates").await?;
        self.inner.list_actor_templates(h, o).await
    }
    async fn update_actor_template(
        &self,
        r: &ActorTemplateRef,
        p: Precondition,
        m: Mutate<'_, pb::ActorTemplate>,
    ) -> Result<pb::ActorTemplate> {
        self.gate("update_actor_template").await?;
        self.inner.update_actor_template(r, p, m).await
    }
    async fn mark_actor_template_for_deletion(
        &self,
        r: &ActorTemplateRef,
        p: DeletePreconditions,
        m: Option<Mutate<'_, pb::ActorTemplate>>,
    ) -> Result<pb::ActorTemplate> {
        self.gate("mark_actor_template_for_deletion").await?;
        self.inner.mark_actor_template_for_deletion(r, p, m).await
    }
    async fn delete_actor_template(
        &self,
        r: &ActorTemplateRef,
        p: DeletePreconditions,
    ) -> Result<pb::ActorTemplate> {
        self.gate("delete_actor_template").await?;
        self.inner.delete_actor_template(r, p).await
    }
}

#[async_trait]
impl TagStore for FaultStore {
    async fn create_tag(&self, t: &pb::Tag) -> Result<pb::Tag> {
        self.gate("create_tag").await?;
        self.inner.create_tag(t).await
    }
    async fn get_tag(&self, r: &TagRef) -> Result<pb::Tag> {
        self.gate("get_tag").await?;
        self.inner.get_tag(r).await
    }
    async fn list_tags(&self, h: &str, o: &ListOptions) -> Result<ListResponse<pb::Tag>> {
        self.gate("list_tags").await?;
        self.inner.list_tags(h, o).await
    }
    async fn update_tag(
        &self,
        r: &TagRef,
        p: Precondition,
        m: Mutate<'_, pb::Tag>,
    ) -> Result<pb::Tag> {
        self.gate("update_tag").await?;
        self.inner.update_tag(r, p, m).await
    }
    async fn mark_tag_for_deletion(
        &self,
        r: &TagRef,
        p: DeletePreconditions,
        m: Option<Mutate<'_, pb::Tag>>,
    ) -> Result<pb::Tag> {
        self.gate("mark_tag_for_deletion").await?;
        self.inner.mark_tag_for_deletion(r, p, m).await
    }
    async fn delete_tag(&self, r: &TagRef, p: DeletePreconditions) -> Result<pb::Tag> {
        self.gate("delete_tag").await?;
        self.inner.delete_tag(r, p).await
    }
}

#[async_trait]
impl WorkerStore for FaultStore {
    async fn create_worker(&self, w: &pb::Worker) -> Result<pb::Worker> {
        self.gate("create_worker").await?;
        self.inner.create_worker(w).await
    }
    async fn get_worker(&self, n: &str) -> Result<pb::Worker> {
        self.gate("get_worker").await?;
        self.inner.get_worker(n).await
    }
    async fn list_workers(&self, o: &ListOptions) -> Result<ListResponse<pb::Worker>> {
        self.gate("list_workers").await?;
        self.inner.list_workers(o).await
    }
    async fn update_worker(
        &self,
        n: &str,
        p: Precondition,
        m: Mutate<'_, pb::Worker>,
    ) -> Result<pb::Worker> {
        self.gate("update_worker").await?;
        self.inner.update_worker(n, p, m).await
    }
    async fn mark_worker_for_deletion(
        &self,
        n: &str,
        p: DeletePreconditions,
        m: Option<Mutate<'_, pb::Worker>>,
    ) -> Result<pb::Worker> {
        self.gate("mark_worker_for_deletion").await?;
        self.inner.mark_worker_for_deletion(n, p, m).await
    }
    async fn delete_worker(&self, n: &str, p: DeletePreconditions) -> Result<pb::Worker> {
        self.gate("delete_worker").await?;
        self.inner.delete_worker(n, p).await
    }
    async fn watch_workers(&self) -> Result<WorkerWatch> {
        self.gate("watch_workers").await?;
        self.inner.watch_workers().await
    }
}

#[async_trait]
impl WorkerAssignmentStore for FaultStore {
    async fn bind_actor_to_worker(
        &self,
        w: &str,
        a: &pb::ActorAssignment,
        admit: Option<Admit<'_>>,
    ) -> Result<pb::ActorAssignment> {
        self.gate("bind_actor_to_worker").await?;
        self.inner.bind_actor_to_worker(w, a, admit).await
    }
    async fn release_actor_from_worker(&self, w: &str, uid: &str) -> Result<Option<pb::Worker>> {
        self.gate("release_actor_from_worker").await?;
        self.inner.release_actor_from_worker(w, uid).await
    }
    async fn get_worker_assignment(&self, w: &str, uid: &str) -> Result<pb::ActorAssignment> {
        self.gate("get_worker_assignment").await?;
        self.inner.get_worker_assignment(w, uid).await
    }
    async fn list_worker_assignments(
        &self,
        w: &str,
        o: &ListOptions,
    ) -> Result<ListResponse<pb::ActorAssignment>> {
        self.gate("list_worker_assignments").await?;
        self.inner.list_worker_assignments(w, o).await
    }
    async fn find_worker_hosting_actor(&self, uid: &str) -> Result<String> {
        self.gate("find_worker_hosting_actor").await?;
        self.inner.find_worker_hosting_actor(uid).await
    }
}

#[async_trait]
impl LeaseStore for FaultStore {
    async fn acquire_lease(&self, key: &str) -> Result<Lease> {
        self.gate("acquire_lease").await?;
        let l = self.inner.acquire_lease(key).await?;
        self.leases.lock().insert(key.to_string(), l.token());
        Ok(l)
    }
}

#[async_trait]
impl HealthCheck for FaultStore {
    async fn ping(&self) -> Result<()> {
        self.gate("ping").await?;
        self.inner.ping().await
    }
}

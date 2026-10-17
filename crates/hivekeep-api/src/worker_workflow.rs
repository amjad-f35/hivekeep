//! Multi-step Worker operations: deregistering a Worker whose pod is gone,
//! and releasing the Actors a restarted worker process lost.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::keeper::v1 as kp;
use hivekeep_store::{DeletePreconditions, ListOptions, Precondition, Store, StoreError};
use parking_lot::Mutex;
use tonic::Code;

use crate::error::{status, Error, Result};
use crate::events;
use crate::fields;
use crate::keeper_dialer::KeeperDialer;
use crate::leases;
use crate::metrics::{op, Metrics};
use crate::workflow::crash::{msg, new_crash};
use crate::workload_spec;

/// Bound of each reclaim Terminate, and how long a node that did not answer
/// is skipped by later reclaims.
pub const DEFAULT_RECLAIM_TIMEOUT: Duration = Duration::from_secs(30);

const RECORD_OBSERVED_EPOCH_ATTEMPTS: u32 = 5;

/// The Worker workflows.
pub struct WorkerWorkflow {
    store: Arc<dyn Store>,
    dialer: Option<Arc<dyn KeeperDialer>>,
    metrics: Arc<Metrics>,
    external_volumes: bool,
    reclaim_timeout: Duration,
    gave_up: Mutex<HashMap<String, Instant>>,
}

/// Whether the Worker's registered epoch is past its observed one.
pub fn needs_assignment_reconcile(w: &pb::Worker) -> bool {
    w.status
        .as_ref()
        .is_some_and(|s| s.registered_epoch > s.observed_epoch)
}

impl WorkerWorkflow {
    /// Workflows over `store`. Without a dialer, node state of released
    /// Actors is left for their revert or delete.
    pub fn new(
        store: Arc<dyn Store>,
        dialer: Option<Arc<dyn KeeperDialer>>,
        metrics: Arc<Metrics>,
        external_volumes: bool,
    ) -> Self {
        WorkerWorkflow {
            store,
            dialer,
            metrics,
            external_volumes,
            reclaim_timeout: DEFAULT_RECLAIM_TIMEOUT,
            gave_up: Mutex::new(HashMap::new()),
        }
    }

    /// Bounds each reclaim Terminate, and how long a node that did not
    /// answer is skipped, by `d`.
    pub fn with_reclaim_timeout(mut self, d: Duration) -> Self {
        self.reclaim_timeout = d;
        self
    }

    fn node_gave_up(&self, node: &str) -> bool {
        let now = Instant::now();
        let mut g = self.gave_up.lock();
        g.retain(|_, until| now < *until);
        g.contains_key(node)
    }

    fn give_up_on(&self, node: &str) {
        self.gave_up
            .lock()
            .insert(node.to_string(), Instant::now() + self.reclaim_timeout);
    }

    /// Deregisters a Worker: drains and marks it, crashes the Actors that
    /// were running on it, then removes the record. An absent Worker is
    /// NOT_FOUND.
    pub async fn delete_worker(&self, name: &str, pre: DeletePreconditions) -> Result<pb::Worker> {
        let worker = self.ensure_marked_for_deletion(name, &pre).await?;
        self.ensure_bound_actors_released(&worker).await?;
        let uid = fields::uid(worker.metadata.as_ref()).to_string();
        match self
            .store
            .delete_worker(name, DeletePreconditions::uid(uid.clone()))
            .await
        {
            Ok(w) => Ok(w),
            Err(StoreError::NotFound) => Err(status::not_found(format!("Worker {name} not found"))),
            Err(StoreError::UidConflict) => Err(status::aborted(format!(
                "Worker {name} does not have uid {uid}"
            ))),
            Err(e) => Err(Error::from(e).store_context("while deleting worker from DB")),
        }
    }

    async fn ensure_marked_for_deletion(
        &self,
        name: &str,
        pre: &DeletePreconditions,
    ) -> Result<pb::Worker> {
        let mut already: Option<pb::Worker> = None;
        let res = self
            .store
            .mark_worker_for_deletion(
                name,
                pre.clone(),
                Some(&mut |w: &mut pb::Worker| {
                    if w.metadata.as_ref().is_some_and(|m| m.delete_time.is_some()) {
                        already = Some(w.clone());
                        return Err(crate::error::reject(Error::AlreadyMarked));
                    }
                    // Draining first stops a concurrent resume from binding
                    // behind the sweep.
                    w.status.get_or_insert_with(Default::default).state =
                        pb::WorkerState::Draining as i32;
                    Ok(())
                }),
            )
            .await;
        match res {
            Ok(w) => Ok(w),
            Err(e) => match Error::from(e) {
                Error::AlreadyMarked => Ok(already.expect("set with the refusal")),
                Error::Store(StoreError::UidConflict) => Err(status::aborted(format!(
                    "Worker {name} does not have uid {}",
                    pre.uid
                ))),
                Error::Store(StoreError::VersionConflict) => Err(crate::error::conflict()),
                Error::Store(StoreError::NotFound) => {
                    Err(status::not_found(format!("Worker {name} not found")))
                }
                e => Err(e.context("while marking worker for deletion")),
            },
        }
    }

    async fn ensure_bound_actors_released(&self, worker: &pb::Worker) -> Result<()> {
        let name = fields::worker_name(worker);
        let mut busy = 0;
        let mut token = String::new();
        loop {
            let page = self
                .store
                .list_worker_assignments(name, &ListOptions::new(0, token.clone()))
                .await
                .map_err(|e| {
                    Error::from(e)
                        .context(format!("while listing the assignments of worker {name}"))
                })?;
            for a in &page.items {
                match self.release_bound_actor(worker, a).await {
                    Err(Error::ActorBusy) => busy += 1,
                    Err(e) => return Err(e),
                    Ok(()) => {}
                }
            }
            if page.next_page_token.is_empty() {
                break;
            }
            token = page.next_page_token;
        }
        if busy > 0 {
            return Err(status::aborted(format!(
                "{busy} actors on worker {name} have an operation in progress; left for a retry"
            )));
        }
        Ok(())
    }

    async fn lease_or_busy(&self, r: &ActorRef) -> Result<hivekeep_store::Lease> {
        match self.store.acquire_lease(&leases::actor_key(r)).await {
            Ok(l) => Ok(l),
            Err(StoreError::LeaseConflict) => Err(Error::ActorBusy),
            Err(e) => {
                Err(Error::from(e).context(format!("while acquiring the lease of actor {r}")))
            }
        }
    }

    async fn release_bound_actor(
        &self,
        worker: &pb::Worker,
        a: &pb::ActorAssignment,
    ) -> Result<()> {
        let Some(actor_ref) = a.actor.as_ref().map(ActorRef::from_object_ref) else {
            return Ok(());
        };
        let lease = self.lease_or_busy(&actor_ref).await?;
        let res = async {
            let actor = match self.store.get_actor(&actor_ref).await {
                Ok(x) => x,
                Err(StoreError::NotFound) => return Ok(()),
                Err(e) => {
                    return Err(Error::from(e).context("while getting actor to release from worker"))
                }
            };
            if fields::actor_uid(&actor) != a.actor_uid
                || fields::assigned_worker(&actor) != fields::worker_name(worker)
                || fields::state(&actor) == pb::ActorState::Suspended
            {
                return Ok(());
            }
            self.reclaim_actor_state_on_node(worker, &actor).await;
            self.crash_bound_actor(worker, &actor_ref, &actor, msg::WORKER_POD_GONE)
                .await
        }
        .await;
        lease.close().await;
        res
    }

    /// Crashes an Actor that lost its sandbox on `worker` and clears its
    /// pointer at it. A DELETING Actor is left to its delete.
    async fn crash_bound_actor(
        &self,
        worker: &pb::Worker,
        r: &ActorRef,
        actor: &pb::Actor,
        crash_msg: &str,
    ) -> Result<()> {
        let st = fields::state(actor);
        if st == pb::ActorState::Deleting {
            return Ok(());
        }
        let operation = match st {
            pb::ActorState::Resuming => op::RESUME,
            pb::ActorState::Suspending => op::SUSPEND,
            pb::ActorState::Reverting => op::REVERT,
            _ => op::UNKNOWN,
        };
        let was_crashed = st == pb::ActorState::Crashed;
        tracing::info!(actor = %r, worker = fields::worker_name(worker), "crashing an actor that lost its worker");
        let crash = new_crash(operation, crash_msg);
        let res = self
            .store
            .update_actor(r, Precondition::of(actor), &mut |a| {
                let s = fields::status_mut(a);
                s.state = pb::ActorState::Crashed as i32;
                if !was_crashed {
                    s.crash = Some(crash.clone());
                }
                s.worker_assignment = None;
                Ok(())
            })
            .await;
        match res {
            Ok(_) => {}
            Err(StoreError::NotFound) => return Ok(()),
            Err(StoreError::UidConflict | StoreError::VersionConflict) => {
                return Err(crate::error::conflict())
            }
            Err(e) => {
                return Err(Error::from(e).context(format!(
                    "while releasing actor from worker {}",
                    fields::worker_name(worker)
                )))
            }
        }
        if !was_crashed {
            events::crashed(actor, operation);
            self.metrics.record_crash(actor, operation);
        }
        Ok(())
    }

    /// Best effort: asks the node's keeper to terminate the Actor and clean
    /// its node state. A node that does not answer is skipped for a while.
    async fn reclaim_actor_state_on_node(&self, worker: &pb::Worker, actor: &pb::Actor) {
        let Some(dialer) = &self.dialer else { return };
        let node = &worker.node_name;
        if self.node_gave_up(node) {
            return;
        }
        let mut keeper = match dialer.dial(node).await {
            Ok(k) => k,
            Err(e) => {
                self.give_up_on(node);
                tracing::warn!(node, error = %e, "could not reach the keeper holding a released actor's state");
                return;
            }
        };
        let md = actor.metadata.clone().unwrap_or_default();
        let tmpl = actor.actor_template.clone().unwrap_or_default();
        let req = kp::TerminateRequest {
            worker_pod_uid: worker.worker_pod_uid.clone(),
            hive: md.hive,
            actor_name: md.name,
            actor_uid: md.uid,
            actor_template_hive: tmpl.hive,
            actor_template_name: tmpl.name,
            spec: Some(workload_spec::fallback(actor, self.external_volumes)),
        };
        match tokio::time::timeout(self.reclaim_timeout, keeper.terminate(req)).await {
            Ok(Ok(_)) => {}
            Ok(Err(s)) if s.code() == Code::NotFound => {}
            Ok(Err(s)) if matches!(s.code(), Code::DeadlineExceeded | Code::Unavailable) => {
                self.give_up_on(node);
                tracing::warn!(node, error = %s.message(), "keeper did not answer a reclaim");
            }
            Err(_) => {
                self.give_up_on(node);
                tracing::warn!(node, "keeper reclaim timed out");
            }
            Ok(Err(s)) => {
                tracing::warn!(node, error = %s.message(), "failed to reclaim a released actor's node state")
            }
        }
    }

    /// Brings the Worker's observed epoch up to its registered epoch,
    /// crashing and releasing the Actors earlier worker processes lost. A
    /// Worker that is gone is success.
    pub async fn reconcile_assignments(&self, name: &str) -> Result<()> {
        let worker = match self.store.get_worker(name).await {
            Ok(w) => w,
            Err(StoreError::NotFound) => return Ok(()),
            Err(e) => return Err(Error::from(e).context(format!("while fetching worker {name}"))),
        };
        if !needs_assignment_reconcile(&worker) {
            return Ok(());
        }
        let epoch = worker
            .status
            .as_ref()
            .map(|s| s.registered_epoch)
            .unwrap_or(0);
        self.release_assignments_before(&worker, epoch).await?;
        self.record_observed_epoch(name, fields::uid(worker.metadata.as_ref()), epoch)
            .await
    }

    async fn record_observed_epoch(&self, name: &str, uid: &str, epoch: i64) -> Result<()> {
        for attempt in 1.. {
            let w = match self.store.get_worker(name).await {
                Ok(w) => w,
                Err(StoreError::NotFound) => return Ok(()),
                Err(e) => {
                    return Err(Error::from(e).context(format!("while re-fetching worker {name}")))
                }
            };
            let version = w.metadata.as_ref().map(|m| m.version).unwrap_or(0);
            let res = self
                .store
                .update_worker(name, Precondition::new(uid, version), &mut |w| {
                    let s = w.status.get_or_insert_with(Default::default);
                    s.observed_epoch = s.observed_epoch.max(epoch);
                    Ok(())
                })
                .await;
            match res {
                Ok(_) | Err(StoreError::NotFound) | Err(StoreError::UidConflict) => return Ok(()),
                Err(StoreError::VersionConflict) if attempt < RECORD_OBSERVED_EPOCH_ATTEMPTS => {
                    continue
                }
                Err(e) => {
                    return Err(Error::from(e).context(format!(
                        "while recording observed epoch {epoch} on worker {name}"
                    )))
                }
            }
        }
        unreachable!()
    }

    async fn release_assignments_before(&self, worker: &pb::Worker, epoch: i64) -> Result<()> {
        let name = fields::worker_name(worker);
        let mut busy = 0;
        let mut token = String::new();
        loop {
            let page = self
                .store
                .list_worker_assignments(name, &ListOptions::new(0, token.clone()))
                .await
                .map_err(|e| {
                    Error::from(e)
                        .context(format!("while listing the assignments of worker {name}"))
                })?;
            for a in &page.items {
                match self.release_earlier_assignment(worker, a, epoch).await {
                    Err(Error::ActorBusy) => busy += 1,
                    Err(e) => return Err(e),
                    Ok(()) => {}
                }
            }
            if page.next_page_token.is_empty() {
                break;
            }
            token = page.next_page_token;
        }
        if busy > 0 {
            tracing::info!(worker = name, busy, "assignments left for a retry");
            return Err(Error::ActorBusy);
        }
        Ok(())
    }

    async fn release_earlier_assignment(
        &self,
        worker: &pb::Worker,
        a: &pb::ActorAssignment,
        epoch: i64,
    ) -> Result<()> {
        let name = fields::worker_name(worker);
        let release = || async {
            self.store
                .release_actor_from_worker(name, &a.actor_uid)
                .await
                .map(|_| ())
                .map_err(|e| {
                    Error::from(e).context(format!(
                        "while releasing actor {} from worker {name}",
                        a.actor_uid
                    ))
                })
        };
        let Some(r) = a.actor.as_ref().map(ActorRef::from_object_ref) else {
            return release().await;
        };
        let lease = self.lease_or_busy(&r).await?;
        let res = async {
            let actor = match self.store.get_actor(&r).await {
                Ok(x) => x,
                Err(StoreError::NotFound) => return release().await,
                Err(e) => {
                    return Err(Error::from(e)
                        .context(format!("while getting actor to release from worker {name}")))
                }
            };
            if fields::actor_uid(&actor) != a.actor_uid {
                return release().await;
            }
            if fields::assigned_worker(&actor) != name {
                return Ok(());
            }
            if fields::assignment(&actor)
                .map(|w| w.worker_epoch)
                .unwrap_or(0)
                >= epoch
            {
                return Ok(());
            }
            let st = fields::state(&actor);
            if st == pb::ActorState::Resuming {
                return Ok(());
            }
            if st != pb::ActorState::Suspended {
                self.crash_bound_actor(worker, &r, &actor, msg::WORKER_RESTARTED)
                    .await?;
            }
            release().await
        }
        .await;
        lease.close().await;
        res
    }
}

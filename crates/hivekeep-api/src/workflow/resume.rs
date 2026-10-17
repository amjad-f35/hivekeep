//! ResumeActor: place a SUSPENDED Actor on a worker and restore it.
//!
//! Steps: load, create volumes, assign a worker (RESUMING), attach volumes,
//! ask the keeper to Run (cold boot) or Restore, then commit RUNNING stamped
//! with the epoch of the worker process that took the workload.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::keeper::v1 as kp;
use hivekeep_store::{Precondition, StoreError};
use parking_lot::Mutex;
use tonic::Code;

use super::crash::{self, msg};
use super::{resolve_template, ActorWorkflow};
use crate::convert;
use crate::error::{status, Error, Result};
use crate::events;
use crate::fields;
use crate::leases::Ctx;
use crate::metrics::{op, outcome, OpLabels};
use crate::runtime_compat::{self, Requirement};
use crate::scheduling::{self, Constraints, ScheduleError};
use crate::snapshots;
use crate::volumes;
use crate::workload_spec;

/// Attempts of one assignment loop, and its backoff.
const ASSIGNMENT_ATTEMPTS: u32 = 6;
const ASSIGNMENT_BACKOFF: Duration = Duration::from_millis(15);
const ASSIGNMENT_BACKOFF_CAP: Duration = Duration::from_millis(250);

/// What a resume restored from, for the lifecycle metric.
#[derive(Debug, Clone, Default)]
pub(crate) struct RestoreTelemetry {
    pub kind: &'static str,
    pub fidelity: &'static str,
}

enum Attempt {
    Assigned(pb::Actor, Arc<pb::Worker>),
    Retry(Option<pb::Actor>),
}

/// Whether `snap` was captured under a template other than `t`.
pub fn template_replaced(snap: &pb::Snapshot, t: &pb::ActorTemplate) -> bool {
    !snap.actor_template_uid.is_empty() && snap.actor_template_uid != fields::template_uid(t)
}

/// The runtime the restore must match: the snapshot's, when the resume
/// restores memory state; `None` otherwise.
pub fn required_snapshot_runtime<'a>(
    snap: Option<&'a pb::Snapshot>,
    t: &pb::ActorTemplate,
) -> Option<&'a pb::ActorSandboxRuntime> {
    let snap = snap?;
    if snap.fidelity != pb::SnapshotFidelity::Memory as i32 || template_replaced(snap, t) {
        return None;
    }
    snap.sandbox_runtime.as_ref()
}

/// How the keeper may serve a snapshot: a Tag's is immutable and shared.
pub fn snapshot_sharing(owner: i32) -> kp::SnapshotSharing {
    if owner == pb::SnapshotOwner::Tag as i32 {
        kp::SnapshotSharing::Shared
    } else {
        kp::SnapshotSharing::Private
    }
}

/// The placement constraints of resuming `actor` under `t`.
pub fn scheduling_constraints(actor: &pb::Actor, t: &pb::ActorTemplate) -> Constraints {
    let st = actor.status.as_ref();
    let snap = snapshots::to_resume(st);
    let node = fields::assigned_node(actor);
    let required = Requirement::new(required_snapshot_runtime(snap, t));
    Constraints {
        sandbox_class: snap
            .and_then(|s| s.sandbox_runtime.as_ref())
            .map(|r| r.sandbox_class.clone())
            .unwrap_or_default(),
        required_runtime: required.clone(),
        snapshot_runtime: required,
        template_selector: t.worker_selector.as_ref().map(|s| s.match_labels.clone()),
        actor_selector: actor
            .worker_selector
            .as_ref()
            .map(|s| s.match_labels.clone())
            .unwrap_or_default(),
        required_nodes: if node.is_empty() {
            vec![]
        } else {
            vec![node.to_string()]
        },
        limits: t.resources.as_ref().and_then(|r| r.limits),
    }
}

/// Whether scheduling may drop the runtime requirement and restore without
/// memory state.
pub fn can_relax_runtime(actor: &pb::Actor, c: &Constraints) -> bool {
    if c.required_runtime.runtime().is_none() {
        return false;
    }
    let st = actor.status.as_ref();
    let node = fields::assigned_node(actor);
    // A pinned Actor detaches first, so a compatible worker elsewhere can
    // keep its memory state.
    if fields::state(actor) == pb::ActorState::Suspended && !node.is_empty() {
        return false;
    }
    let snap = snapshots::to_resume(st);
    match snap {
        Some(s) if !s.locality.is_empty() => {
            snapshots::local_elsewhere(Some(s), node) || snapshots::is_durable(s)
        }
        _ => true,
    }
}

/// The assignment recorded for an Actor placed on `w`.
pub fn worker_assignment_from(
    w: &pb::Worker,
    snapshot_runtime: &Requirement,
) -> pb::WorkerAssignment {
    pb::WorkerAssignment {
        worker: Some(pb::ObjectRef {
            hive: String::new(),
            name: fields::worker_name(w).to_string(),
        }),
        worker_namespace: w.worker_namespace.clone(),
        worker_pool: w.worker_pool.clone(),
        worker_pod: w.worker_pod.clone(),
        worker_pod_uid: w.worker_pod_uid.clone(),
        worker_pod_ips: w.status.as_ref().map(|s| s.ips.clone()).unwrap_or_default(),
        node_name: w.node_name.clone(),
        worker_epoch: 0,
        sandbox_runtime: snapshot_runtime.runtime_for(w.status.as_ref()),
        worker_spiffe_id: w.worker_spiffe_id.clone(),
    }
}

/// Whether the worker still hosts `actor_uid`, per the store's assignments.
pub(crate) async fn worker_hosts_actor(
    store: &dyn hivekeep_store::Store,
    worker: &str,
    actor_uid: &str,
) -> Result<bool> {
    match store.get_worker_assignment(worker, actor_uid).await {
        Ok(_) => Ok(true),
        Err(StoreError::NotFound) => Ok(false),
        Err(e) => Err(Error::from(e).context(format!(
            "while checking whether worker {worker:?} hosts actor {actor_uid:?}"
        ))),
    }
}

impl ActorWorkflow {
    /// Resumes an Actor. Returns it and whether it was resumed (`false` when
    /// it was already RUNNING).
    pub async fn resume_actor(&self, r: &ActorRef) -> Result<(pb::Actor, bool)> {
        let start = Instant::now();
        // Routed requests resume running Actors on every request; answer
        // those without taking the lease.
        let actor = self
            .store
            .get_actor(r)
            .await
            .map_err(|e| not_found_actor(e, r))?;
        if fields::state(&actor) == pb::ActorState::Running {
            return Ok((actor, false));
        }
        let mut tele = RestoreTelemetry::default();
        let mut last = Some(actor);
        let result = async {
            let lease = self.lease_actor(r).await?;
            let ctx = Ctx::background().with_lease(&lease);
            let res = self.resume_leased(&ctx, r, &mut last, &mut tele).await;
            lease.close().await;
            res
        }
        .await;
        match &result {
            Ok((_, false)) => {}
            _ => self.metrics.record_lifecycle_op(
                op::RESUME,
                start,
                result.as_ref().err().map(Error::code),
                &OpLabels::of(last.as_ref(), tele.kind, tele.fidelity),
            ),
        }
        result
    }

    async fn resume_leased(
        &self,
        ctx: &Ctx,
        r: &ActorRef,
        last: &mut Option<pb::Actor>,
        tele: &mut RestoreTelemetry,
    ) -> Result<(pb::Actor, bool)> {
        let actor = self
            .store
            .get_actor(r)
            .await
            .map_err(|e| not_found_actor(e, r))?;
        *last = Some(actor.clone());
        if fields::state(&actor) == pb::ActorState::Running {
            return Ok((actor, false));
        }
        let template = resolve_template(self.store.as_ref(), &actor).await?;
        let actor = self.ensure_volumes_created(r, actor, &template).await?;
        let (actor, worker) = match self
            .ensure_worker_assigned(ctx, r, actor.clone(), &template)
            .await
        {
            Err(Error::NoWorkerRoom) if should_detach_from_node(&actor) => {
                let detached = self
                    .detach_from_full_node(ctx, r, &actor, &template)
                    .await?;
                self.ensure_worker_assigned(ctx, r, detached, &template)
                    .await?
            }
            other => other?,
        };
        *last = Some(actor.clone());
        let publish = self
            .ensure_volumes_attached(&actor, &worker, &template)
            .await?;
        let epoch = self
            .ensure_keeper_restored(ctx, r, &actor, &template, &publish, tele)
            .await?;
        let running = self.finalize_running(r, epoch).await?;
        *last = Some(running.clone());
        Ok((running, true))
    }

    async fn ensure_volumes_created(
        &self,
        r: &ActorRef,
        actor: pb::Actor,
        t: &pb::ActorTemplate,
    ) -> Result<pb::Actor> {
        let pending = volumes::actor_volumes(&actor)
            .iter()
            .any(|v| v.storage_volume_id.is_empty());
        if !pending {
            return Ok(actor);
        }
        let st = fields::state(&actor);
        if st == pb::ActorState::Deleting {
            return Err(status::failed_precondition(format!(
                "cannot create volumes for Actor {r} in state {}",
                fields::state_name(st)
            )));
        }
        let (vols, create_err) = volumes::create_actor_volumes(
            self.volumes.as_ref(),
            fields::actor_uid(&actor),
            t,
            volumes::actor_volumes(&actor),
        )
        .await;
        let status_vols = volumes::external_volumes_status(vols);
        let stored = self
            .store
            .update_actor(r, Precondition::of(&actor), &mut |a| {
                fields::status_mut(a).external_volumes = status_vols.clone();
                Ok(())
            })
            .await;
        if let Some(e) = create_err {
            if let Err(ue) = stored {
                tracing::error!(error = %ue, "failed to record volumes after a failed creation");
            }
            return Err(e);
        }
        stored
            .map_err(|e| Error::from(e).store_context("while updating actor after volume creation"))
    }

    pub(crate) async fn ensure_worker_assigned(
        &self,
        ctx: &Ctx,
        r: &ActorRef,
        mut actor: pb::Actor,
        t: &pb::ActorTemplate,
    ) -> Result<(pb::Actor, Arc<pb::Worker>)> {
        match fields::state(&actor) {
            pb::ActorState::Resuming => {
                let w = self.validate_assigned_worker(r, &actor, t).await?;
                return Ok((actor, w));
            }
            pb::ActorState::Suspended => {}
            other => {
                return Err(status::failed_precondition(format!(
                    "AssignWorker prerequisite not met for Actor: {r} (got: {}, want {})",
                    fields::state_name(other),
                    fields::state_name(pb::ActorState::Suspended)
                )))
            }
        }
        let mut delay = ASSIGNMENT_BACKOFF;
        let mut last_elapsed = 0.0;
        for attempt in 0..ASSIGNMENT_ATTEMPTS {
            if ctx.is_done() {
                // Retried attempts hid their records; the loop leaves one.
                if attempt > 0 {
                    self.metrics.record_assignment(
                        last_elapsed,
                        outcome::ERROR,
                        "",
                        "",
                        &snapshot_sandbox_class(&actor),
                        Some(Code::Cancelled),
                    );
                }
                return Err(Error::Status(tonic::Status::cancelled(
                    "lease lost during worker assignment",
                )));
            }
            if attempt > 0 {
                let jitter = delay.mul_f64(rand_unit());
                tokio::time::sleep(delay + jitter).await;
                delay = (delay * 2).min(ASSIGNMENT_BACKOFF_CAP);
                if ctx.is_done() {
                    self.metrics.record_assignment(
                        last_elapsed,
                        outcome::ERROR,
                        "",
                        "",
                        &snapshot_sandbox_class(&actor),
                        Some(Code::Cancelled),
                    );
                    return Err(Error::Status(tonic::Status::cancelled(
                        "lease lost during worker assignment",
                    )));
                }
            }
            let started = Instant::now();
            let res = self.assign_worker_attempt(r, &actor, t).await;
            last_elapsed = started.elapsed().as_secs_f64();
            match res? {
                Attempt::Assigned(a, w) => return Ok((a, w)),
                Attempt::Retry(fresh) => {
                    if let Some(f) = fresh {
                        actor = f;
                    }
                }
            }
        }
        let err = crate::error::conflict();
        self.metrics.record_assignment(
            last_elapsed,
            outcome::ERROR,
            "",
            "",
            &snapshot_sandbox_class(&actor),
            Some(err.code()),
        );
        Err(err)
    }

    async fn validate_assigned_worker(
        &self,
        r: &ActorRef,
        actor: &pb::Actor,
        t: &pb::ActorTemplate,
    ) -> Result<Arc<pb::Worker>> {
        let store = self.store.as_ref();
        let crash_aborted = |m: &'static str| async move {
            crash::crash_actor(store, &self.metrics, r, op::RESUME, m).await?;
            Err::<Arc<pb::Worker>, _>(status::aborted(format!("actor {r} crashed")))
        };
        if fields::assignment(actor).is_none() {
            tracing::error!(actor = %r, "RESUMING actor has no worker assignment");
            return crash_aborted(msg::WORKER_ASSIGNMENT_MISSING).await;
        }
        let name = fields::assigned_worker(actor);
        let worker = match store.get_worker(name).await {
            Ok(w) => w,
            Err(StoreError::NotFound) => return crash_aborted(msg::WORKER_GONE).await,
            Err(e) => {
                return Err(
                    Error::from(e).context("failed to get already assigned worker for actor")
                )
            }
        };
        match fields::worker_state(&worker) {
            pb::WorkerState::Draining => return crash_aborted(msg::WORKER_DRAINING).await,
            pb::WorkerState::Unavailable => {
                return Err(status::unavailable(format!(
                    "worker {name} assigned to actor {r} is unavailable"
                )))
            }
            _ => {}
        }
        if !scheduling::registered(&worker) {
            return Err(status::unavailable(format!(
                "worker {name} assigned to actor {r} has not finished registering"
            )));
        }
        if !worker_hosts_actor(store, name, fields::actor_uid(actor)).await? {
            return crash_aborted(msg::WORKER_REASSIGNED).await;
        }
        let c = scheduling_constraints(actor, t);
        if !applies_with_fallback(&worker, &c, can_relax_runtime(actor, &c)) {
            store
                .release_actor_from_worker(name, fields::actor_uid(actor))
                .await
                .map_err(|e| Error::from(e).context("while releasing stale worker assignment"))?;
            return crash_aborted(msg::WORKER_INELIGIBLE).await;
        }
        Ok(Arc::new(worker))
    }

    async fn worker_holding_stale_claim(
        &self,
        actor: &pb::Actor,
        c: &Constraints,
        relaxable: bool,
    ) -> Result<Option<Arc<pb::Worker>>> {
        let uid = fields::actor_uid(actor);
        let name = match self.store.find_worker_hosting_actor(uid).await {
            Ok(n) => n,
            Err(StoreError::NotFound) => return Ok(None),
            Err(e) => {
                return Err(Error::from(e).context(format!(
                    "while looking for a worker already hosting actor {uid:?}"
                )))
            }
        };
        let worker = match self.store.get_worker(&name).await {
            Ok(w) => w,
            Err(StoreError::NotFound) => return Ok(None),
            Err(e) => {
                return Err(Error::from(e).context(format!(
                    "while reading worker {name:?} holding a stale claim"
                )))
            }
        };
        if !worker.node_name.is_empty() && applies_with_fallback(&worker, c, relaxable) {
            return Ok(Some(Arc::new(worker)));
        }
        self.store
            .release_actor_from_worker(&name, uid)
            .await
            .map_err(|e| {
                Error::from(e).context(format!("while releasing stale claim on worker {name:?}"))
            })?;
        Ok(None)
    }

    async fn assign_worker_attempt(
        &self,
        r: &ActorRef,
        actor: &pb::Actor,
        t: &pb::ActorTemplate,
    ) -> Result<Attempt> {
        check_snapshot_upload(r, actor)?;
        let started = Instant::now();
        let mut c = scheduling_constraints(actor, t);
        let class = c.sandbox_class.clone();
        let record = |outcome_: &str, ns: &str, pool: &str, class: &str, err: Option<Code>| {
            self.metrics.record_assignment(
                started.elapsed().as_secs_f64(),
                outcome_,
                ns,
                pool,
                class,
                err,
            );
        };
        let relaxable = can_relax_runtime(actor, &c);
        let stale = match self.worker_holding_stale_claim(actor, &c, relaxable).await {
            Ok(s) => s,
            Err(e) => {
                record(outcome::ERROR, "", "", &class, Some(e.code()));
                return Err(e);
            }
        };
        let chosen = match stale {
            Some(w) => {
                if relaxable && !c.snapshot_runtime.worker_matches(w.status.as_ref()) {
                    c.required_runtime = Requirement::default();
                }
                w
            }
            None => {
                let mut picked = self.scheduler.schedule(&c);
                if matches!(picked, Err(ScheduleError::NoCapacity(_))) && relaxable {
                    c.required_runtime = Requirement::default();
                    picked = self.scheduler.schedule(&c);
                }
                match picked {
                    Ok(w) => w,
                    Err(ScheduleError::NoCapacity(_)) => {
                        record(outcome::NO_CAPACITY, "", "", &class, None);
                        return Err(Error::NoWorkerRoom);
                    }
                    Err(ScheduleError::Source(m)) => {
                        let e = status::unavailable(format!("while listing workers: {m}"));
                        record(outcome::ERROR, "", "", &class, Some(e.code()));
                        return Err(e);
                    }
                }
            }
        };
        let name = fields::worker_name(&chosen).to_string();
        if chosen.node_name.is_empty() {
            let e = status::failed_precondition(format!("worker {name} has no node name"));
            record(outcome::ERROR, "", "", &class, Some(e.code()));
            return Err(e);
        }
        let Some(booking) = scheduling::booking(&chosen, &c) else {
            return Ok(Attempt::Retry(None));
        };
        let assignment = pb::ActorAssignment {
            actor: Some(fields::actor_ref(actor).to_object_ref()),
            actor_uid: fields::actor_uid(actor).to_string(),
            resources: booking,
            ..Default::default()
        };
        let bound: Mutex<Option<pb::Worker>> = Mutex::new(None);
        let admit = |fresh: &pb::Worker| -> hivekeep_store::Result<()> {
            if !scheduling::applies(fresh, &c) || !scheduling::has_room(fresh, &c) {
                return Err(crate::error::reject(Error::WorkerFilledUp));
            }
            if scheduling::booking(fresh, &c) != Some(booking) {
                return Err(crate::error::reject(Error::WorkerFilledUp));
            }
            *bound.lock() = Some(fresh.clone());
            Ok(())
        };
        match self
            .store
            .bind_actor_to_worker(&name, &assignment, Some(&admit))
            .await
        {
            Ok(_) => {}
            Err(StoreError::NotFound) => {
                self.cache.forget(&name);
                return Ok(Attempt::Retry(None));
            }
            Err(e) => match Error::from(e) {
                Error::WorkerFilledUp => return Ok(Attempt::Retry(None)),
                e => {
                    record(outcome::ERROR, "", "", &class, Some(e.code()));
                    return Err(e);
                }
            },
        }
        let bound = bound.lock().take().unwrap_or_else(|| (*chosen).clone());
        let new_assignment = worker_assignment_from(&bound, &c.snapshot_runtime);
        let res = self
            .store
            .update_actor(r, Precondition::of(actor), &mut |a| {
                let st = fields::status_mut(a);
                st.state = pb::ActorState::Resuming as i32;
                st.assigned_node = new_assignment.node_name.clone();
                st.worker_assignment = Some(new_assignment.clone());
                Ok(())
            })
            .await;
        let stored = match res {
            Ok(a) => a,
            Err(StoreError::VersionConflict) => {
                let fresh = self.store.get_actor(r).await;
                return match fresh {
                    Err(_) => Ok(Attempt::Retry(None)),
                    Ok(f) if fields::state(&f) == pb::ActorState::Suspended => {
                        Ok(Attempt::Retry(Some(f)))
                    }
                    Ok(f) => Err(status::aborted(format!(
                        "actor {r} is {} and can no longer be resumed",
                        fields::state_name(fields::state(&f))
                    ))),
                };
            }
            Err(e) => {
                let e = Error::from(e);
                record(outcome::ERROR, "", "", &class, Some(e.code()));
                return Err(e);
            }
        };
        record(
            outcome::ASSIGNED,
            &bound.worker_namespace,
            &bound.worker_pool,
            runtime_compat::sandbox_class(bound.status.as_ref()),
            None,
        );
        events::state_changed(&stored, op::RESUME);
        Ok(Attempt::Assigned(stored, Arc::new(bound)))
    }

    async fn ensure_volumes_attached(
        &self,
        actor: &pb::Actor,
        worker: &pb::Worker,
        t: &pb::ActorTemplate,
    ) -> Result<HashMap<String, HashMap<String, String>>> {
        if !volumes::actor_volumes(actor).is_empty() && !self.external_volumes_enabled() {
            return Err(status::failed_precondition(
                "external volumes are not supported unless the ExternalVolumes preview gate is enabled",
            ));
        }
        if worker.node_name.is_empty() {
            return Err(status::internal("assigned worker has no node name"));
        }
        volumes::attach_actor_volumes(self.volumes.as_ref(), actor, t, &worker.node_name).await
    }

    async fn ensure_keeper_restored(
        &self,
        ctx: &Ctx,
        r: &ActorRef,
        actor: &pb::Actor,
        t: &pb::ActorTemplate,
        publish: &HashMap<String, HashMap<String, String>>,
        tele: &mut RestoreTelemetry,
    ) -> Result<i64> {
        check_snapshot_upload(r, actor)?;
        let a = fields::assignment(actor).cloned().unwrap_or_default();
        let mut keeper = self.dialer.dial(&a.node_name).await.map_err(dial_error)?;
        let spec = workload_spec::from_template(t, actor, publish)?;
        let egress = self.egress_gateway();
        let resources = convert::resources_to_keeper(t.resources.as_ref());
        let runtime = convert::runtime_to_keeper(a.sandbox_runtime.as_ref());
        let md = actor.metadata.clone().unwrap_or_default();
        let tmpl = actor.actor_template.clone().unwrap_or_default();
        let store = self.store.as_ref();
        let Some(snap) = snapshots::to_resume(actor.status.as_ref()).cloned() else {
            tele.kind = "boot";
            let req = kp::RunRequest {
                worker_pod_uid: a.worker_pod_uid.clone(),
                hive: md.hive.clone(),
                actor_name: md.name.clone(),
                actor_uid: md.uid.clone(),
                actor_template_hive: tmpl.hive.clone(),
                actor_template_name: tmpl.name.clone(),
                spec: Some(spec),
                egress_gateway: egress,
                resources,
                sandbox_runtime: runtime,
            };
            return match ctx.run(async { keeper.run(req).await }).await {
                Ok(resp) => Ok(resp.into_inner().worker_epoch),
                Err(s) => Err(crash::handle_keeper_error(
                    ctx,
                    store,
                    &self.metrics,
                    r,
                    op::RESUME,
                    "Run",
                    false,
                    s,
                )
                .await),
            };
        };
        tele.kind = if !snap.locality.is_empty() && snap.locality == a.node_name {
            "local"
        } else {
            "latest"
        };
        let required = required_snapshot_runtime(Some(&snap), t);
        let incompatible = required
            .is_some_and(|req| !runtime_compat::matches(a.sandbox_runtime.as_ref(), Some(req)));
        let mut fidelity = convert::fidelity_to_keeper(snap.fidelity);
        if template_replaced(&snap, t) {
            fidelity = kp::SnapshotFidelity::Volumes;
        } else if incompatible {
            tracing::warn!(actor = %r, worker = %a.worker_pod, "assigned worker cannot restore the snapshot's runtime; restoring without memory state");
            fidelity = kp::SnapshotFidelity::Rootfs;
        }
        tele.fidelity = convert::fidelity_label(fidelity);
        let req = kp::RestoreRequest {
            worker_pod_uid: a.worker_pod_uid.clone(),
            hive: md.hive.clone(),
            actor_name: md.name.clone(),
            actor_uid: md.uid.clone(),
            actor_template_hive: tmpl.hive.clone(),
            actor_template_name: tmpl.name.clone(),
            spec: Some(spec),
            snapshot: Some(kp::Snapshot {
                snapshot_uuid: snap.uuid.clone(),
                generation: snap.generation,
                fidelity: fidelity as i32,
                // Named even while PENDING: the keeper may have uploaded and
                // detached before its report landed.
                object: Some(kp::ObjectStorage {
                    snapshot_uri: snapshots::durable_uri(&snap).to_string(),
                    sharing: snapshot_sharing(snap.owner) as i32,
                }),
            }),
            egress_gateway: egress,
            resources,
            sandbox_runtime: runtime,
        };
        match ctx.run(async { keeper.restore(req).await }).await {
            Ok(resp) => Ok(resp.into_inner().worker_epoch),
            Err(s)
                if !ctx.is_done()
                    && s.code() == Code::NotFound
                    && snapshots::local_elsewhere(Some(&snap), &a.node_name) =>
            {
                let m = format!(
                    "{}: {}",
                    msg::LOCAL_SNAPSHOT_NODE_GONE,
                    crash::keeper_crash_message("Restore", &s)
                );
                Err(crash::crash_and_fail(store, &self.metrics, r, op::RESUME, &m).await)
            }
            Err(s) => Err(crash::handle_keeper_error(
                ctx,
                store,
                &self.metrics,
                r,
                op::RESUME,
                "Restore",
                false,
                s,
            )
            .await),
        }
    }

    pub(crate) fn egress_gateway(&self) -> Option<kp::EgressGateway> {
        let a = &self.settings.egress_gateway_address;
        (!a.is_empty()).then(|| kp::EgressGateway { address: a.clone() })
    }

    async fn finalize_running(&self, r: &ActorRef, epoch: i64) -> Result<pb::Actor> {
        let latest = self.store.get_actor(r).await?;
        let name = fields::assigned_worker(&latest).to_string();
        let worker = match self.store.get_worker(&name).await {
            Ok(w) => w,
            Err(StoreError::NotFound) => {
                return Err(status::aborted(format!(
                    "worker {name} of actor {r} is gone"
                )))
            }
            Err(e) => return Err(Error::from(e).context(format!("while getting worker {name}"))),
        };
        let registered = worker
            .status
            .as_ref()
            .map(|s| s.registered_epoch)
            .unwrap_or(0);
        if epoch < registered {
            return Err(status::unavailable(format!(
                "worker {name} restarted while actor {r} was being restored, please retry"
            )));
        }
        if epoch > registered {
            return Err(status::internal(format!(
                "worker {name} reported epoch {epoch}, above its registered epoch {registered}"
            )));
        }
        let ips = worker
            .status
            .as_ref()
            .map(|s| s.ips.clone())
            .unwrap_or_default();
        let stored = self
            .store
            .update_actor(r, Precondition::of(&latest), &mut |a| {
                let st = fields::status_mut(a);
                st.state = pb::ActorState::Running as i32;
                if let Some(wa) = st.worker_assignment.as_mut() {
                    wa.worker_epoch = epoch;
                    wa.worker_pod_ips = ips.clone();
                }
                if let Err(e) = record_restored_upload(st) {
                    tracing::warn!(error = %e, "restored a snapshot from its upload but could not record the upload");
                }
                Ok(())
            })
            .await?;
        events::state_changed(&stored, op::RESUME);
        Ok(stored)
    }
}

/// Records the upload of a snapshot restored elsewhere after its node was
/// deleted: the restore proved the upload complete.
fn record_restored_upload(st: &mut pb::ActorStatus) -> Result<()> {
    let Some(snap) = snapshots::to_resume(Some(st)).cloned() else {
        return Ok(());
    };
    if !snapshots::local_elsewhere(Some(&snap), &st.assigned_node) {
        return Ok(());
    }
    let node = snap.locality.clone();
    snapshots::apply_latest_durable(
        st,
        Some(&pb::SnapshotReport {
            uuid: snap.uuid.clone(),
            generation: snap.generation,
            fidelity: snap.fidelity,
            object: snap
                .durable_snapshot
                .as_ref()
                .and_then(|d| d.object.clone()),
            ..Default::default()
        }),
    )?;
    snapshots::apply_detach(st, &node, snap.generation)
}

/// DATA_LOSS when the snapshot to resume records no upload.
pub fn check_snapshot_upload(r: &ActorRef, actor: &pb::Actor) -> Result<()> {
    match snapshots::to_resume(actor.status.as_ref()) {
        Some(s) if s.durable_snapshot.is_none() => Err(status::data_loss(format!(
            "Actor {r} snapshot {} records no upload",
            s.uuid
        ))),
        _ => Ok(()),
    }
}

/// Whether `worker` meets `c`, or meets it with the runtime relaxed when
/// `relaxable`.
pub fn applies_with_fallback(worker: &pb::Worker, c: &Constraints, relaxable: bool) -> bool {
    let mut relaxed = c.clone();
    relaxed.required_runtime = Requirement::default();
    if !scheduling::applies(worker, &relaxed) {
        return false;
    }
    relaxable || c.required_runtime.worker_matches(worker.status.as_ref())
}

fn snapshot_sandbox_class(actor: &pb::Actor) -> String {
    snapshots::to_resume(actor.status.as_ref())
        .and_then(|s| s.sandbox_runtime.as_ref())
        .map(|r| r.sandbox_class.clone())
        .unwrap_or_default()
}

/// Whether a SUSPENDED Actor's failed assignment should detach it from
/// the full node it is pinned to.
pub fn should_detach_from_node(actor: &pb::Actor) -> bool {
    fields::state(actor) == pb::ActorState::Suspended && !fields::assigned_node(actor).is_empty()
}

pub(crate) fn not_found_actor(e: StoreError, r: &ActorRef) -> Error {
    match e {
        StoreError::NotFound => status::not_found(format!("Actor {r} not found")),
        e => Error::from(e).context("while getting actor from DB"),
    }
}

/// Maps a dial failure: no keeper known on the node is UNAVAILABLE.
pub(crate) fn dial_error(e: Error) -> Error {
    match e {
        Error::NoKeeperOnNode(n) => status::unavailable(format!("no keeper found on node {n:?}")),
        e => e,
    }
}

fn rand_unit() -> f64 {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    (n % 1000) as f64 / 1000.0
}

//! SuspendActor: checkpoint a RUNNING Actor into a new snapshot and free its
//! worker.
//!
//! Steps: mark SUSPENDING (minting the snapshot record with a PENDING upload
//! under the Actor's own prefix), ask the keeper to Checkpoint, detach
//! volumes, then commit SUSPENDED with the snapshot recorded and the
//! assignment cleared, and release the snapshots that commit replaced.

use std::time::Instant;

use hivekeep_core::resource::ActorRef;
use hivekeep_core::snapshot::{new_snapshot_name, SnapshotUri};
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::keeper::v1 as kp;
use hivekeep_store::Precondition;
use tonic::Code;

use super::crash::{self, msg};
use super::resume::dial_error;
use super::{resolve_template, ActorWorkflow};
use crate::convert;
use crate::error::{status, Error, Result};
use crate::events;
use crate::fields;
use crate::leases::Ctx;
use crate::metrics::{op, OpLabels};
use crate::snapshots;
use crate::volumes;
use crate::workload_spec;

/// When a suspend is acknowledged relative to its upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuspendMode {
    /// The snapshot is durable before the suspend returns.
    Sync,
    /// The keeper uploads in the background; the Actor stays pinned to the
    /// node until the upload is reported.
    Async,
}

impl SuspendMode {
    /// The mode a request asks for; unset is asynchronous.
    pub fn from_options(o: Option<&pb::SuspendOptions>) -> Self {
        match o.map(|o| o.mode) {
            Some(m) if m == pb::DurabilityMode::Synchronous as i32 => SuspendMode::Sync,
            _ => SuspendMode::Async,
        }
    }

    fn upload_mode(self) -> kp::UploadMode {
        match self {
            SuspendMode::Sync => kp::UploadMode::Immediate,
            SuspendMode::Async => kp::UploadMode::Deferred,
        }
    }
}

/// Whether the keeper kept this checkpoint on the node.
fn keeper_kept_local(state: Option<&kp::ActorSnapshotState>, snap: &pb::Snapshot) -> bool {
    let Some(local) = state
        .and_then(|s| s.latest_local.as_ref())
        .and_then(|l| l.snapshot.as_ref())
    else {
        return false;
    };
    let uri = snapshots::durable_uri(snap);
    !uri.is_empty()
        && local.snapshot_uuid == snap.uuid
        && local.generation == snap.generation
        && local.object.as_ref().map(|o| o.snapshot_uri.as_str()) == Some(uri)
}

impl ActorWorkflow {
    /// Suspends an Actor. A SUSPENDED Actor is returned unchanged.
    pub async fn suspend_actor(&self, r: &ActorRef, mode: SuspendMode) -> Result<pb::Actor> {
        let start = Instant::now();
        let mut labels = OpLabels::default();
        let result = async {
            let lease = self.lease_actor(r).await?;
            let ctx = Ctx::background().with_lease(&lease);
            let res = self.suspend_leased(&ctx, r, mode, &mut labels).await;
            lease.close().await;
            res
        }
        .await;
        self.metrics.record_lifecycle_op(
            op::SUSPEND,
            start,
            result.as_ref().err().map(Error::code),
            &labels,
        );
        result
    }

    async fn suspend_leased(
        &self,
        ctx: &Ctx,
        r: &ActorRef,
        mode: SuspendMode,
        labels: &mut OpLabels,
    ) -> Result<pb::Actor> {
        let actor = self.store.get_actor(r).await?;
        *labels = OpLabels::of(Some(&actor), "", "");
        let template = resolve_template(self.store.as_ref(), &actor).await?;
        if fields::state(&actor) == pb::ActorState::Suspended {
            return Ok(actor);
        }
        let actor = self.ensure_marked_suspending(r, actor, &template).await?;
        let (fidelity, state) = self
            .ensure_keeper_suspended(ctx, r, &actor, &template, mode)
            .await?;
        volumes::detach_actor_volumes(self.volumes.as_ref(), &actor, Some(&template))
            .await
            .map_err(|e| e.context("workflow failed at step DetachVolumes"))?;
        *labels = OpLabels::of(Some(&actor), "", fidelity);
        self.ensure_suspended_finalized(r, mode, state.as_ref())
            .await
    }

    async fn ensure_marked_suspending(
        &self,
        r: &ActorRef,
        actor: pb::Actor,
        t: &pb::ActorTemplate,
    ) -> Result<pb::Actor> {
        match fields::state(&actor) {
            pb::ActorState::Suspending => return Ok(actor),
            pb::ActorState::Running => {}
            other => {
                return Err(status::failed_precondition(format!(
                    "MarkSuspending prerequisite not met for Actor: {r} (got: {}, want {})",
                    fields::state_name(other),
                    fields::state_name(pb::ActorState::Running)
                )))
            }
        }
        let name = new_snapshot_name();
        let location = self.snapshot_location(t)?;
        let uri = SnapshotUri::for_actor(
            &location,
            fields::actor_hive(&actor),
            fields::actor_uid(&actor),
            &name,
        )
        .map_err(|e| {
            status::failed_precondition(format!(
                "while building the snapshot URI for actor {r}: {e}"
            ))
        })?;
        let fidelity = fields::preferred_fidelity(t);
        let template_uid = fields::template_uid(t).to_string();
        let stored = self
            .store
            .update_actor(r, Precondition::of(&actor), &mut |a| {
                let st = fields::status_mut(a);
                st.state = pb::ActorState::Suspending as i32;
                let gen = st.last_assigned_generation + 1;
                st.last_assigned_generation = gen;
                let mut snap = snapshots::new_durable(
                    gen,
                    pb::SnapshotOwner::Actor,
                    fidelity,
                    &template_uid,
                    uri.name(),
                    uri.as_str(),
                    pb::SnapshotStorageState::Pending,
                );
                snap.sandbox_runtime = st
                    .worker_assignment
                    .as_ref()
                    .and_then(|w| w.sandbox_runtime.clone());
                st.snapshots.push(snap);
                Ok(())
            })
            .await?;
        events::state_changed(&stored, op::SUSPEND);
        Ok(stored)
    }

    async fn ensure_keeper_suspended(
        &self,
        ctx: &Ctx,
        r: &ActorRef,
        actor: &pb::Actor,
        t: &pb::ActorTemplate,
        mode: SuspendMode,
    ) -> Result<(&'static str, Option<kp::ActorSnapshotState>)> {
        let store = self.store.as_ref();
        let Some(a) = fields::assignment(actor).cloned() else {
            return Err(crash::crash_and_fail(
                store,
                &self.metrics,
                r,
                op::SUSPEND,
                msg::WORKER_ASSIGNMENT_MISSING,
            )
            .await);
        };
        let mut keeper = self.dialer.dial(&a.node_name).await.map_err(|e| {
            dial_error(e).context(format!(
                "while getting keeper conn for node {:?}",
                a.node_name
            ))
        })?;
        let spec = workload_spec::from_template(t, actor, &Default::default())?;
        let snap = snapshots::at_latest_generation(actor.status.as_ref())
            .cloned()
            .unwrap_or_default();
        let fidelity = convert::fidelity_to_keeper(fields::preferred_fidelity(t));
        let md = actor.metadata.clone().unwrap_or_default();
        let tmpl = actor.actor_template.clone().unwrap_or_default();
        let req = kp::CheckpointRequest {
            worker_pod_uid: a.worker_pod_uid.clone(),
            hive: md.hive,
            actor_name: md.name,
            actor_uid: md.uid,
            actor_template_hive: tmpl.hive,
            actor_template_name: tmpl.name,
            spec: Some(spec),
            snapshot: Some(kp::Snapshot {
                snapshot_uuid: snap.uuid.clone(),
                generation: snap.generation,
                fidelity: fidelity as i32,
                object: Some(kp::ObjectStorage {
                    snapshot_uri: snapshots::durable_uri(&snap).to_string(),
                    sharing: 0,
                }),
            }),
            upload_config: Some(kp::UploadConfig {
                mode: mode.upload_mode() as i32,
            }),
        };
        let label = convert::fidelity_label(fidelity);
        match ctx.run(async { keeper.checkpoint(req).await }).await {
            Ok(resp) => Ok((label, resp.into_inner().state)),
            // A snapshot conflict keeps the Actor and its assignment for a retry.
            Err(s) if !ctx.is_done() && s.code() == Code::Aborted => Err(status::aborted(format!(
                "while calling keeper Checkpoint: {}",
                s.message()
            ))),
            Err(s) => Err(crash::handle_keeper_error(
                ctx,
                store,
                &self.metrics,
                r,
                op::SUSPEND,
                "Checkpoint",
                false,
                s,
            )
            .await),
        }
    }

    async fn ensure_suspended_finalized(
        &self,
        r: &ActorRef,
        mode: SuspendMode,
        state: Option<&kp::ActorSnapshotState>,
    ) -> Result<pb::Actor> {
        let store = self.store.as_ref();
        let latest = store.get_actor(r).await?;
        let st = fields::state(&latest);
        if st != pb::ActorState::Suspending {
            return Err(status::failed_precondition(format!(
                "FinalizeSuspended prerequisite not met for Actor: {r} (got: {}, want {})",
                fields::state_name(st),
                fields::state_name(pb::ActorState::Suspending)
            )));
        }
        let status_ref = latest.status.as_ref();
        if mode == SuspendMode::Async {
            if fields::assigned_node(&latest).is_empty() {
                return Err(crash::crash_and_fail(
                    store,
                    &self.metrics,
                    r,
                    op::SUSPEND,
                    msg::LOCAL_SNAPSHOT_NODE_UNKNOWN,
                )
                .await);
            }
            if snapshots::at_latest_generation(status_ref).is_none() {
                return Err(crash::crash_and_fail(
                    store,
                    &self.metrics,
                    r,
                    op::SUSPEND,
                    msg::LOCAL_SNAPSHOT_MISSING,
                )
                .await);
            }
        }
        if snapshots::at_latest_generation(status_ref)
            .and_then(|s| s.durable_snapshot.as_ref())
            .is_none()
        {
            return Err(crash::crash_and_fail(
                store,
                &self.metrics,
                r,
                op::SUSPEND,
                msg::DURABLE_SNAPSHOT_MISSING,
            )
            .await);
        }
        let mut applied = latest.status.clone().unwrap_or_default();
        let reported =
            convert::snapshot_report_from_keeper(state.and_then(|s| s.latest_durable.as_ref()));
        if let Err(e) = snapshots::apply_latest_durable(&mut applied, reported.as_ref()) {
            tracing::warn!(actor = %r, error = %e, "snapshot state the keeper returned conflicts with the record");
            return Err(crash::crash_and_fail(
                store,
                &self.metrics,
                r,
                op::SUSPEND,
                msg::SNAPSHOT_STATE_CONFLICT,
            )
            .await);
        }
        if fields::assignment(&latest).is_some() {
            crash::release_worker(store, &latest).await?;
        }
        let stored = store
            .update_actor(r, Precondition::of(&latest), &mut |a| {
                let st = fields::status_mut(a);
                st.state = pb::ActorState::Suspended as i32;
                st.snapshots = applied.snapshots.clone();
                let gen = st.last_assigned_generation;
                let node = st.assigned_node.clone();
                let snap = snapshots::by_generation_mut(st, gen).expect("checked above");
                if mode == SuspendMode::Sync {
                    if let Some(d) = snap.durable_snapshot.as_mut() {
                        d.state = pb::SnapshotStorageState::Completed as i32;
                    }
                }
                let durable = snapshots::is_durable(snap);
                let keep_local = keeper_kept_local(state, snap) || !durable;
                if keep_local {
                    snap.locality = node;
                }
                if !keep_local {
                    st.assigned_node.clear();
                }
                snapshots::prune(st, Some(gen));
                st.worker_assignment = None;
                Ok(())
            })
            .await?;
        events::state_changed(&stored, op::SUSPEND);
        self.release_dropped_snapshots(&latest, &stored).await;
        Ok(stored)
    }
}

//! Detaching a SUSPENDED Actor from the full node its local snapshot pins
//! it to, so a resume can place it elsewhere.

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::keeper::v1 as kp;
use hivekeep_store::{Precondition, StoreError};
use tonic::Code;

use super::resume::{dial_error, scheduling_constraints};
use super::ActorWorkflow;
use crate::convert;
use crate::error::{status, Error, Result};
use crate::fields;
use crate::leases::Ctx;
use crate::runtime_compat::Requirement;
use crate::scheduling::ScheduleError;
use crate::snapshots;

/// Whether a keeper that cannot be reached may be forgotten: the Actor has
/// no worker or crash to clean up, and its newest local snapshot is durable.
pub fn can_detach_without_keeper(ctx: &Ctx, actor: &pb::Actor, e: &Error) -> bool {
    if ctx.is_done() || fields::assignment(actor).is_some() {
        return false;
    }
    if actor.status.as_ref().is_some_and(|s| s.crash.is_some()) {
        return false;
    }
    let reachable_failure = matches!(e, Error::NoKeeperOnNode(_))
        || matches!(e.code(), Code::Unavailable | Code::DeadlineExceeded);
    if !reachable_failure {
        return false;
    }
    snapshots::latest_local(actor.status.as_ref()).is_some_and(snapshots::is_durable)
}

fn detach_actor_error(ctx: &Ctx, s: tonic::Status) -> Error {
    let m = format!("while calling keeper DetachActor: {}", s.message());
    if ctx.is_done() {
        return status::aborted(format!("{m}: lease lost"));
    }
    match s.code() {
        Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded => status::unavailable(m),
        Code::Aborted => status::aborted(m),
        Code::FailedPrecondition | Code::NotFound => status::failed_precondition(m),
        _ => status::internal(m),
    }
}

impl ActorWorkflow {
    pub(crate) async fn detach_from_full_node(
        &self,
        ctx: &Ctx,
        r: &ActorRef,
        actor: &pb::Actor,
        t: &pb::ActorTemplate,
    ) -> Result<pb::Actor> {
        let node = fields::assigned_node(actor).to_string();
        let actor = self
            .load_pinned_actor(r, fields::actor_uid(actor), &node)
            .await?;
        let mut c = scheduling_constraints(&actor, t);
        c.required_nodes.clear();
        c.required_runtime = Requirement::default();
        match self.scheduler.schedule(&c) {
            Ok(_) => {}
            Err(ScheduleError::NoCapacity(_)) => return Err(Error::NoWorkerRoom),
            Err(ScheduleError::Source(m)) => return Err(status::unavailable(m)),
        }
        let mut keeper = match self.dialer.dial(&node).await {
            Ok(k) => k,
            Err(e) => {
                if can_detach_without_keeper(ctx, &actor, &e) {
                    return self.record_detach(r, &actor, &node, None).await;
                }
                if matches!(e, Error::NoKeeperOnNode(_))
                    && snapshots::latest_local_not_durable(actor.status.as_ref())
                {
                    let gone = self.dialer.node_gone(&node).await.map_err(|ne| {
                        status::unavailable(format!("while checking node {node:?}: {ne}"))
                    })?;
                    if gone {
                        return self.record_gone_node(r, &actor, &node).await;
                    }
                }
                return Err(
                    dial_error(e).context(format!("while getting keeper conn for node {node:?}"))
                );
            }
        };
        let req = kp::DetachActorRequest {
            hive: fields::actor_hive(&actor).into(),
            actor_name: fields::actor_name(&actor).into(),
            actor_uid: fields::actor_uid(&actor).into(),
        };
        let state = match ctx.run(async { keeper.detach_actor(req).await }).await {
            Ok(resp) => resp.into_inner().state.unwrap_or_default(),
            Err(s) => {
                let e = Error::Status(s.clone());
                if can_detach_without_keeper(ctx, &actor, &e) {
                    return self.record_detach(r, &actor, &node, None).await;
                }
                return Err(detach_actor_error(ctx, s));
            }
        };
        if state.latest_local.is_some() || state.uploading.is_some() {
            return Err(status::internal(format!(
                "keeper on node {node:?} detached Actor {r} but still reports a local snapshot"
            )));
        }
        let stored = self.record_detach(r, &actor, &node, Some(&state)).await?;
        tracing::info!(actor = %r, node, "detached actor from its full node to resume it on another node");
        Ok(stored)
    }

    async fn load_pinned_actor(&self, r: &ActorRef, uid: &str, node: &str) -> Result<pb::Actor> {
        let a = self
            .store
            .get_actor(r)
            .await
            .map_err(|e| Error::from(e).context(format!("while reading Actor {r}")))?;
        if fields::actor_uid(&a) != uid
            || fields::state(&a) != pb::ActorState::Suspended
            || fields::assigned_node(&a) != node
        {
            return Err(status::aborted(format!(
                "Actor {r} changed during the resume, please retry"
            )));
        }
        Ok(a)
    }

    /// Forgets local copies once the newest generation is durable. `state`
    /// is `None` when the keeper could not be asked.
    pub(crate) async fn record_detach(
        &self,
        r: &ActorRef,
        actor: &pb::Actor,
        node: &str,
        state: Option<&kp::ActorSnapshotState>,
    ) -> Result<pb::Actor> {
        let mut applied = actor.status.clone().unwrap_or_default();
        let reported =
            convert::snapshot_report_from_keeper(state.and_then(|s| s.latest_durable.as_ref()));
        snapshots::apply_latest_durable(&mut applied, reported.as_ref()).map_err(|e| {
            status::internal(format!(
                "keeper on node {node:?} reported a state for Actor {r} that conflicts with its record: {}",
                tonic::Status::from(e).message()
            ))
        })?;
        let up_to = applied.last_assigned_generation;
        snapshots::apply_detach(&mut applied, node, up_to)?;
        if applied.state == pb::ActorState::Reverting as i32 && applied.worker_assignment.is_none()
        {
            applied.assigned_node.clear();
        }
        self.store
            .update_actor(r, Precondition::of(actor), &mut |a| {
                let st = fields::status_mut(a);
                st.snapshots = applied.snapshots.clone();
                st.assigned_node = applied.assigned_node.clone();
                Ok(())
            })
            .await
            .map_err(|e| match e {
                StoreError::VersionConflict => crate::error::conflict(),
                e => Error::from(e).context(format!(
                    "while recording the detach of Actor {r} from node {node:?}"
                )),
            })
    }

    async fn record_gone_node(
        &self,
        r: &ActorRef,
        actor: &pb::Actor,
        node: &str,
    ) -> Result<pb::Actor> {
        let actor = self
            .load_pinned_actor(r, fields::actor_uid(actor), node)
            .await?;
        if !snapshots::latest_local_not_durable(actor.status.as_ref()) {
            return self.record_detach(r, &actor, node, None).await;
        }
        let stored = self
            .store
            .update_actor(r, Precondition::of(&actor), &mut |a| {
                fields::status_mut(a).assigned_node.clear();
                Ok(())
            })
            .await
            .map_err(|e| match e {
                StoreError::VersionConflict => {
                    status::aborted(format!("Actor {r} changed during the resume, please retry"))
                }
                e => Error::from(e).context(format!(
                    "while releasing Actor {r} from deleted node {node:?}"
                )),
            })?;
        tracing::info!(actor = %r, node, "released actor from its deleted node to restore its snapshot's upload elsewhere");
        Ok(stored)
    }
}

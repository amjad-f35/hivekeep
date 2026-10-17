//! RevertActor: discard the Actor's current execution and return it to
//! SUSPENDED at its newest durable snapshot. The recovery verb: accepted for
//! RUNNING and CRASHED Actors, and for SUSPENDED ones whose newest snapshot
//! exists only on a node. Needs no live worker.

use std::time::Instant;

use hivekeep_core::resource::ActorRef;
use hivekeep_core::snapshot::SnapshotUri;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::Precondition;

use super::crash;
use super::resume_detach::can_detach_without_keeper;
use super::{actor_snapshot_owner, is_template_not_found, resolve_template, ActorWorkflow};
use crate::error::{status, Error, Result};
use crate::events;
use crate::fields;
use crate::leases::Ctx;
use crate::metrics::{op, OpLabels};
use crate::snapshots;
use crate::volumes;

/// Whether a revert is accepted from the Actor's state and snapshots.
pub fn is_revertable(actor: &pb::Actor) -> bool {
    match fields::state(actor) {
        pb::ActorState::Running | pb::ActorState::Crashed => true,
        pb::ActorState::Suspended => snapshots::latest_is_local(actor.status.as_ref()),
        _ => false,
    }
}

impl ActorWorkflow {
    /// Reverts an Actor.
    pub async fn revert_actor(&self, r: &ActorRef) -> Result<pb::Actor> {
        let start = Instant::now();
        let mut labels = OpLabels::default();
        let result = async {
            let lease = self.lease_actor(r).await?;
            let ctx = Ctx::background().with_lease(&lease);
            let res = self.revert_leased(&ctx, r, &mut labels).await;
            lease.close().await;
            res
        }
        .await;
        self.metrics.record_lifecycle_op(
            op::REVERT,
            start,
            result.as_ref().err().map(Error::code),
            &labels,
        );
        result
    }

    async fn revert_leased(
        &self,
        ctx: &Ctx,
        r: &ActorRef,
        labels: &mut OpLabels,
    ) -> Result<pb::Actor> {
        let actor = self
            .store
            .get_actor(r)
            .await
            .map_err(|e| super::resume::not_found_actor(e, r))?;
        *labels = OpLabels::of(Some(&actor), "", "");
        let template = match resolve_template(self.store.as_ref(), &actor).await {
            Ok(t) => Some(t),
            Err(e) if is_template_not_found(&e) => {
                tracing::warn!(actor = %r, "reverting an actor whose template no longer resolves");
                None
            }
            Err(e) => return Err(e),
        };
        let actor = self.ensure_marked_reverting(r, actor).await?;
        self.ensure_worker_discarded(ctx, r, &actor, template.as_ref())
            .await?;
        self.ensure_in_progress_snapshot_discarded(&actor).await?;
        *labels = OpLabels::of(Some(&actor), "", "");
        self.ensure_reverted_finalized(r).await
    }

    async fn ensure_marked_reverting(&self, r: &ActorRef, actor: pb::Actor) -> Result<pb::Actor> {
        let st = fields::state(&actor);
        if st == pb::ActorState::Reverting {
            return Ok(actor);
        }
        if !is_revertable(&actor) {
            return Err(status::failed_precondition(format!(
                "Actor {r} is not in a revertable state (got: {}, want one of [ACTOR_STATE_RUNNING ACTOR_STATE_CRASHED], or ACTOR_STATE_SUSPENDED with a node-local snapshot)",
                fields::state_name(st)
            )));
        }
        let stored = self
            .store
            .update_actor(r, Precondition::of(&actor), &mut |a| {
                fields::status_mut(a).state = pb::ActorState::Reverting as i32;
                Ok(())
            })
            .await
            .map_err(|e| Error::from(e).store_context("while setting actor state to REVERTING"))?;
        events::state_changed(&stored, op::REVERT);
        Ok(stored)
    }

    async fn ensure_worker_discarded(
        &self,
        ctx: &Ctx,
        r: &ActorRef,
        actor: &pb::Actor,
        t: Option<&pb::ActorTemplate>,
    ) -> Result<()> {
        if !fields::assigned_node(actor).is_empty() {
            let mut forget_node = false;
            if let Err(e) = self
                .ensure_keeper_terminated(ctx, r, actor, t, op::REVERT)
                .await
            {
                if !can_detach_without_keeper(ctx, actor, &e) {
                    return Err(e);
                }
                forget_node = true;
            }
            volumes::detach_actor_volumes(self.volumes.as_ref(), actor, t).await?;
            if forget_node {
                self.record_detach(r, actor, fields::assigned_node(actor), None)
                    .await?;
            }
            if fields::assignment(actor).is_some() {
                crash::release_worker(self.store.as_ref(), actor)
                    .await
                    .map_err(|e| e.context("while releasing worker"))?;
            }
        }
        self.release_assignment_without_backlink(actor).await
    }

    async fn ensure_in_progress_snapshot_discarded(&self, actor: &pb::Actor) -> Result<()> {
        let in_progress = snapshots::pending_uris(actor.status.as_ref());
        if self.object_store.is_none() || in_progress.is_empty() {
            return Ok(());
        }
        let owner = actor_snapshot_owner(actor);
        let mut prefixes = Vec::new();
        for raw in &in_progress {
            let uri = SnapshotUri::parse(raw).map_err(|e| {
                status::internal(format!(
                    "while parsing the in-progress snapshot {raw:?}: {e}"
                ))
            })?;
            if !uri.owned_by(&owner) {
                return Err(status::internal(format!(
                    "the in-progress snapshot {raw:?} is not owned by actor {owner}"
                )));
            }
            prefixes.push(uri.prefix());
        }
        for p in prefixes {
            self.cleanup_snapshot(p.as_str()).await?;
        }
        Ok(())
    }

    async fn ensure_reverted_finalized(&self, r: &ActorRef) -> Result<pb::Actor> {
        let latest = self.store.get_actor(r).await?;
        let st = fields::state(&latest);
        if st != pb::ActorState::Reverting {
            return Err(status::failed_precondition(format!(
                "FinalizeReverted prerequisite not met for Actor: {r} (got: {}, want {})",
                fields::state_name(st),
                fields::state_name(pb::ActorState::Reverting)
            )));
        }
        let stored = self
            .store
            .update_actor(r, Precondition::of(&latest), &mut |a| {
                let st = fields::status_mut(a);
                st.state = pb::ActorState::Suspended as i32;
                st.worker_assignment = None;
                st.assigned_node.clear();
                snapshots::prune(st, None);
                st.crash = None;
                Ok(())
            })
            .await?;
        events::state_changed(&stored, op::REVERT);
        self.release_dropped_snapshots(&latest, &stored).await;
        Ok(stored)
    }
}

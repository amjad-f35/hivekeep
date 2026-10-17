//! What keepers report on their own: snapshot uploads and detaches, and
//! sandboxes that died.

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{Precondition, StoreError};

use super::crash;
use super::ActorWorkflow;
use crate::error::{status, Error, Result};
use crate::fields;
use crate::metrics::op;
use crate::snapshots;

impl ActorWorkflow {
    /// Applies the snapshot state the keeper on `node` reports for an Actor.
    /// Applied only to SUSPENDED, RUNNING or CRASHED Actors, under the
    /// Actor's lease, so a report never interleaves with a workflow. A report
    /// that changes nothing writes nothing.
    pub async fn apply_reported_snapshot_state(
        &self,
        r: &ActorRef,
        actor_uid: &str,
        node: &str,
        state: &pb::ActorSnapshotState,
    ) -> Result<()> {
        // Checked first so a keeper cannot hold the lease of an Actor that
        // is not on its node.
        self.load_reported_actor(r, actor_uid, node).await?;
        let lease = self.lease_actor(r).await?;
        let res = self.apply_report_leased(r, actor_uid, node, state).await;
        lease.close().await;
        res
    }

    async fn apply_report_leased(
        &self,
        r: &ActorRef,
        actor_uid: &str,
        node: &str,
        state: &pb::ActorSnapshotState,
    ) -> Result<()> {
        let before = self.load_reported_actor(r, actor_uid, node).await?;
        match fields::state(&before) {
            pb::ActorState::Suspended | pb::ActorState::Running | pb::ActorState::Crashed => {}
            s @ (pb::ActorState::Suspending
            | pb::ActorState::Resuming
            | pb::ActorState::Deleting
            | pb::ActorState::Reverting) => {
                return Err(status::aborted(format!(
                    "actor {r} is {}; retry once that finishes",
                    fields::state_name(s)
                )))
            }
            s => {
                return Err(status::internal(format!(
                    "actor {r} has unexpected state {}",
                    fields::state_name(s)
                )))
            }
        }
        let mut applied = before.status.clone().unwrap_or_default();
        snapshots::apply_actor_snapshot_state(&mut applied, node, state)?;
        if Some(&applied) == before.status.as_ref() {
            return Ok(());
        }
        self.store
            .update_actor(r, Precondition::of(&before), &mut |a| {
                a.status = Some(applied.clone());
                Ok(())
            })
            .await
            .map_err(|e| match e {
                StoreError::VersionConflict => crate::error::conflict(),
                e => e.into(),
            })?;
        Ok(())
    }

    /// The Actor if it is the incarnation `uid` and is placed on `node`;
    /// NOT_FOUND otherwise, so a keeper learns nothing about other nodes.
    async fn load_reported_actor(&self, r: &ActorRef, uid: &str, node: &str) -> Result<pb::Actor> {
        let actor = match self.store.get_actor(r).await {
            Ok(a) => a,
            Err(StoreError::NotFound) => {
                return Err(status::not_found(format!("Actor {r} not found")))
            }
            Err(e) => return Err(e.into()),
        };
        if fields::actor_uid(&actor) != uid {
            tracing::warn!(actor = %r, requested_uid = uid, "refusing a snapshot state report naming a stale actor incarnation");
            return Err(status::not_found(format!("Actor {r} not found")));
        }
        if fields::assigned_node(&actor) != node {
            tracing::warn!(actor = %r, caller_node = node, "refusing a snapshot state report for an actor not on the caller's node");
            return Err(status::not_found(format!("Actor {r} not found")));
        }
        Ok(actor)
    }

    /// Crashes a RUNNING Actor whose sandbox the hosting worker saw die.
    pub async fn crash_actor_reported_by_worker(
        &self,
        r: &ActorRef,
        actor_uid: &str,
        worker: &str,
        message: &str,
    ) -> Result<pb::Actor> {
        let lease = self.lease_actor(r).await?;
        let res = async {
            let actor = match self.store.get_actor(r).await {
                Ok(a) => a,
                Err(StoreError::NotFound) => {
                    return Err(status::not_found(format!("Actor {r} not found")))
                }
                Err(e) => return Err(Error::from(e).context(format!("while loading actor {r}"))),
            };
            if fields::actor_uid(&actor) != actor_uid || fields::assigned_worker(&actor) != worker {
                return Err(status::not_found(format!("Actor {r} not found")));
            }
            let st = fields::state(&actor);
            if st != pb::ActorState::Running {
                return Err(status::failed_precondition(format!(
                    "actor {r} is {}, not RUNNING",
                    fields::state_name(st)
                )));
            }
            crash::crash_actor(self.store.as_ref(), &self.metrics, r, op::UNKNOWN, message).await?;
            Ok(self.store.get_actor(r).await?)
        }
        .await;
        lease.close().await;
        res
    }
}

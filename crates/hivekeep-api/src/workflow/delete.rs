//! DeleteActor: terminate the workload, detach and delete volumes, release
//! the worker and the Actor's external snapshots, then remove the record.
//! Cleanup is best effort across steps; any failure leaves the Actor
//! DELETING for a retry.

use std::time::Instant;

use hivekeep_core::resource::ActorRef;
use hivekeep_core::snapshot::{SnapshotUri, StoragePrefix};
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::keeper::v1 as kp;
use hivekeep_store::{DeletePreconditions, Precondition, StoreError};
use tonic::Code;

use super::crash;
use super::resume::worker_hosts_actor;
use super::{actor_snapshot_owner, is_template_not_found, resolve_template, ActorWorkflow};
use crate::error::{status, Error, Result};
use crate::events;
use crate::fields;
use crate::leases::Ctx;
use crate::metrics::{op, OpLabels};
use crate::snapshots;
use crate::volumes;
use crate::workload_spec;

/// The prefix holding everything the Actor wrote to object storage, or
/// `None` when it wrote nothing of its own.
pub fn actor_snapshot_prefix(actor: &pb::Actor) -> Result<Option<StoragePrefix>> {
    let owner = actor_snapshot_owner(actor);
    let st = actor.status.as_ref();
    if let Some(snap) = snapshots::latest_completed(st) {
        let raw = snapshots::durable_uri(snap);
        let uri = SnapshotUri::parse(raw).map_err(|e| {
            status::internal(format!("while parsing the external snapshot {raw:?}: {e}"))
        })?;
        if uri.owned_by(&owner) {
            return Ok(Some(uri.owner_prefix()));
        }
    }
    let Some(pending) = snapshots::latest_durable(st, pb::SnapshotStorageState::Pending) else {
        return Ok(None);
    };
    let raw = snapshots::durable_uri(pending);
    if raw.is_empty() {
        return Ok(None);
    }
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
    Ok(Some(uri.owner_prefix()))
}

impl ActorWorkflow {
    /// Deletes an Actor. Idempotent.
    pub async fn delete_actor(&self, r: &ActorRef, pre: DeletePreconditions) -> Result<pb::Actor> {
        let start = Instant::now();
        let result = async {
            let lease = self.lease_actor(r).await?;
            let ctx = Ctx::background().with_lease(&lease);
            let res = self.delete_leased(&ctx, r, pre).await;
            lease.close().await;
            res
        }
        .await;
        let labels = OpLabels::of(result.as_ref().ok(), "", "");
        self.metrics.record_lifecycle_op(
            op::DELETE,
            start,
            result.as_ref().err().map(Error::code),
            &labels,
        );
        result
    }

    async fn delete_leased(
        &self,
        ctx: &Ctx,
        r: &ActorRef,
        pre: DeletePreconditions,
    ) -> Result<pb::Actor> {
        let mut actor = self.ensure_marked_deleting(r, &pre).await?;
        let mut errs: Vec<Error> = Vec::new();
        let template = match resolve_template(self.store.as_ref(), &actor).await {
            Ok(t) => Some(t),
            Err(e) if is_template_not_found(&e) => None,
            Err(e) => {
                errs.push(e.context("while fetching actor template"));
                None
            }
        };
        let terminated = self
            .ensure_keeper_terminated(ctx, r, &actor, template.as_ref(), op::DELETE)
            .await;
        let detached =
            volumes::detach_actor_volumes(self.volumes.as_ref(), &actor, template.as_ref()).await;
        let ok = terminated.is_ok() && detached.is_ok();
        if let Err(e) = terminated {
            errs.push(e.context("while terminating keeper workload"));
        }
        if let Err(e) = detached {
            errs.push(e.context("while detaching volumes"));
        }
        if ok {
            match self.ensure_worker_released(r, actor.clone()).await {
                Ok(a) => actor = a,
                Err(e) => errs.push(e.context("while releasing worker")),
            }
        } else {
            tracing::warn!(actor = %r, "skipping worker release after a failed terminate or detach");
        }
        if let Err(m) = volumes::delete_actor_volumes(
            self.volumes.as_ref(),
            fields::actor_uid(&actor),
            volumes::actor_volumes(&actor),
        )
        .await
        {
            errs.push(status::internal(format!("while deleting volumes: {m}")));
        }
        if let Err(e) = self.ensure_external_snapshots_released(&actor).await {
            errs.push(e.context("while releasing external snapshots"));
        }
        if !errs.is_empty() {
            let code = errs[0].code();
            let msg = errs
                .into_iter()
                .map(|e| tonic::Status::from(e).message().to_string())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(Error::Status(tonic::Status::new(code, msg)));
        }
        self.finalize_deleted(&actor).await
    }

    async fn ensure_marked_deleting(
        &self,
        r: &ActorRef,
        pre: &DeletePreconditions,
    ) -> Result<pb::Actor> {
        let mut already: Option<pb::Actor> = None;
        let res = self
            .store
            .mark_actor_for_deletion(
                r,
                pre.clone(),
                Some(&mut |a: &mut pb::Actor| {
                    if a.metadata.as_ref().is_some_and(|m| m.delete_time.is_some()) {
                        already = Some(a.clone());
                        return Err(crate::error::reject(Error::AlreadyMarked));
                    }
                    fields::status_mut(a).state = pb::ActorState::Deleting as i32;
                    Ok(())
                }),
            )
            .await;
        match res {
            Ok(a) => {
                events::state_changed(&a, op::DELETE);
                Ok(a)
            }
            Err(e) => match Error::from(e) {
                Error::AlreadyMarked => Ok(already.expect("set with the refusal")),
                Error::Store(StoreError::UidConflict) => Err(status::aborted(format!(
                    "Actor {r} does not have uid {}",
                    pre.uid
                ))),
                Error::Store(StoreError::VersionConflict) => Err(crate::error::conflict()),
                Error::Store(StoreError::NotFound) => {
                    Err(status::not_found(format!("Actor {r} not found")))
                }
                e => Err(e.context("while marking actor for deletion")),
            },
        }
    }

    /// Asks the node's keeper to terminate the Actor's workload and clean up
    /// its node state. Skipped when the Actor is on no node, or the node is
    /// gone for good.
    pub(crate) async fn ensure_keeper_terminated(
        &self,
        ctx: &Ctx,
        r: &ActorRef,
        actor: &pb::Actor,
        t: Option<&pb::ActorTemplate>,
        operation: &str,
    ) -> Result<()> {
        let node = fields::assigned_node(actor).to_string();
        if node.is_empty() {
            return Ok(());
        }
        let mut pod_uid = String::new();
        if let Some(a) = fields::assignment(actor) {
            pod_uid = a.worker_pod_uid.clone();
            let w = fields::assigned_worker(actor);
            if !w.is_empty()
                && !worker_hosts_actor(self.store.as_ref(), w, fields::actor_uid(actor)).await?
            {
                pod_uid.clear();
            }
        }
        let mut keeper = match self.dialer.dial(&node).await {
            Ok(k) => k,
            Err(Error::NoKeeperOnNode(_)) => {
                let gone = self
                    .dialer
                    .node_gone(&node)
                    .await
                    .map_err(|e| status::unavailable(format!("while connecting to keeper on node {node:?}: no keeper (and while checking the node: {e})")))?;
                if gone {
                    tracing::warn!(node, actor = %r, "assigned node no longer exists, skipping keeper terminate");
                    return Ok(());
                }
                return Err(status::unavailable(format!(
                    "while connecting to keeper on node {node:?}: no keeper found"
                )));
            }
            Err(e) => return Err(e.context(format!("while connecting to keeper on node {node:?}"))),
        };
        let spec = match t {
            Some(t) => workload_spec::from_template(t, actor, &Default::default())?,
            None => workload_spec::fallback(actor, self.external_volumes_enabled()),
        };
        let md = actor.metadata.clone().unwrap_or_default();
        let tmpl = actor.actor_template.clone().unwrap_or_default();
        let req = kp::TerminateRequest {
            worker_pod_uid: pod_uid,
            hive: md.hive,
            actor_name: md.name,
            actor_uid: md.uid,
            actor_template_hive: tmpl.hive,
            actor_template_name: tmpl.name,
            spec: Some(spec),
        };
        match ctx.run(async { keeper.terminate(req).await }).await {
            Ok(_) => Ok(()),
            Err(s) if s.code() == Code::NotFound => Ok(()),
            Err(s) => Err(crash::handle_keeper_error(
                ctx,
                self.store.as_ref(),
                &self.metrics,
                r,
                operation,
                "Terminate",
                true,
                s,
            )
            .await),
        }
    }

    /// Releases an assignment the Actor does not reference, found by uid.
    pub(crate) async fn release_assignment_without_backlink(
        &self,
        actor: &pb::Actor,
    ) -> Result<()> {
        let uid = fields::actor_uid(actor);
        let name = match self.store.find_worker_hosting_actor(uid).await {
            Ok(n) => n,
            Err(StoreError::NotFound) => return Ok(()),
            Err(e) => {
                return Err(Error::from(e).context(format!(
                    "while looking for a worker still hosting actor {uid}"
                )))
            }
        };
        match self.store.get_worker(&name).await {
            Ok(_) => {}
            Err(StoreError::NotFound) => return Ok(()),
            Err(e) => {
                return Err(
                    Error::from(e).context(format!("while getting worker {name} to release"))
                )
            }
        }
        tracing::info!(worker = %name, actor_uid = uid, "releasing an assignment the actor does not reference");
        match self.store.release_actor_from_worker(&name, uid).await {
            Ok(_) | Err(StoreError::NotFound) => Ok(()),
            Err(e) => Err(Error::from(e).context(format!("while releasing worker {name}"))),
        }
    }

    async fn ensure_worker_released(&self, r: &ActorRef, actor: pb::Actor) -> Result<pb::Actor> {
        if fields::assignment(&actor).is_none() {
            self.release_assignment_without_backlink(&actor).await?;
            return Ok(actor);
        }
        let latest = self.store.get_actor(r).await?;
        if fields::assignment(&latest).is_none() {
            return Ok(latest);
        }
        crash::release_worker(self.store.as_ref(), &latest).await?;
        let latest = self.store.get_actor(r).await?;
        let updated = self
            .store
            .update_actor(r, Precondition::of(&latest), &mut |a| {
                let st = fields::status_mut(a);
                snapshots::clear_local(st);
                st.assigned_node.clear();
                st.worker_assignment = None;
                Ok(())
            })
            .await?;
        Ok(updated)
    }

    async fn ensure_external_snapshots_released(&self, actor: &pb::Actor) -> Result<()> {
        if self.object_store.is_none() {
            return Ok(());
        }
        match actor_snapshot_prefix(actor)? {
            None => Ok(()),
            Some(p) => self.cleanup_snapshot(p.as_str()).await,
        }
    }

    async fn finalize_deleted(&self, actor: &pb::Actor) -> Result<pb::Actor> {
        let r = fields::actor_ref(actor);
        let uid = fields::actor_uid(actor).to_string();
        match self
            .store
            .delete_actor(&r, DeletePreconditions::uid(uid.clone()))
            .await
        {
            Ok(d) => {
                events::deleted(&d, op::DELETE);
                Ok(d)
            }
            Err(StoreError::NotFound) => Err(status::not_found(format!("Actor {r} not found"))),
            Err(StoreError::UidConflict) => Err(status::aborted(format!(
                "Actor {r} does not have uid {uid}"
            ))),
            Err(e) => Err(Error::from(e).store_context("while deleting actor from DB")),
        }
    }
}

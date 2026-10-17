//! Actor RPCs.

use std::time::Instant;

use hivekeep_core::defaults;
use hivekeep_core::field::{FieldError, Path};
use hivekeep_core::preview::{Gate, PREVIEW_HEADER};
use hivekeep_core::resource::{ActorRef, TagRef};
use hivekeep_core::snapshot::SnapshotUri;
use hivekeep_core::validation::api as v;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{Precondition, StoreError};

use super::{
    delete_preconditions, internal, invalid, list_options, scrub_for_create, scrub_for_update,
    ControlService,
};
use crate::authz::{checks, model::rel};
use crate::context::CallContext;
use crate::error::{reject, status, Error, Result};
use crate::events;
use crate::fields;
use crate::leases;
use crate::metrics::{op, OpLabels};
use crate::snapshots;
use crate::volumes;
use crate::workflow::suspend::SuspendMode;
use crate::workflow::{actor_snapshot_owner, resolve_template};

fn actor_of(r: Option<&pb::ObjectRef>) -> ActorRef {
    r.map(ActorRef::from_object_ref)
        .unwrap_or_else(|| ActorRef::new("", ""))
}

/// The RPC-level mapping of a workflow failure.
fn workflow_error(e: Error, r: &ActorRef) -> Error {
    match e {
        Error::Store(StoreError::VersionConflict) => crate::error::conflict(),
        Error::Store(StoreError::NotFound) => status::not_found(format!("Actor {r} not found")),
        e => e,
    }
}

/// Refuses a template repoint that changes volumes or the mounts of any
/// container both templates have: snapshot data is laid out per them.
pub fn validate_template_volumes_unchanged(
    old: &pb::ActorTemplate,
    new: &pb::ActorTemplate,
) -> Result<()> {
    if old.volumes != new.volumes {
        return Err(status::failed_precondition(
            "volumes differ between the current and the new actor template; volumes must be identical to repoint an actor",
        ));
    }
    for oc in &old.containers {
        if let Some(nc) = new.containers.iter().find(|c| c.name == oc.name) {
            if oc.volume_mounts != nc.volume_mounts {
                return Err(status::failed_precondition(format!(
                    "volume mounts of container {:?} differ between the current and the new actor template; volume mounts must be identical to repoint an actor",
                    oc.name
                )));
            }
        }
    }
    Ok(())
}

impl ControlService {
    pub(crate) async fn create_actor(
        &self,
        cc: &CallContext,
        mut req: pb::CreateActorRequest,
    ) -> Result<pb::Actor> {
        self.authorize(cc, checks::create_actor(&req)).await?;
        if let Some(a) = req.actor.as_mut() {
            scrub_for_create(a.metadata.as_mut());
            a.status = None;
            defaults::apply(a);
        }
        let op = self.op(cc);
        invalid(v::validate_create_actor_request(&op, &req))?;
        let start = Instant::now();
        let input = req.actor.unwrap_or_default();
        let res = self.create_actor_impl(&op, input.clone()).await;
        let labels = OpLabels::of(Some(&input), "", "");
        self.inner.workflow.metrics.record_lifecycle_op(
            op::CREATE,
            start,
            res.as_ref().err().map(Error::code),
            &labels,
        );
        res
    }

    async fn create_actor_impl(
        &self,
        op: &hivekeep_core::validation::Operation,
        input: pb::Actor,
    ) -> Result<pb::Actor> {
        let template = resolve_template(self.store(), &input).await?;
        let has_external = template.volumes.iter().any(|v| {
            v.external
                .as_ref()
                .and_then(|e| e.template.as_ref())
                .is_some()
        });
        let tag_ref = match &input.source_tag {
            Some(t) => {
                if has_external {
                    return Err(status::failed_precondition(
                        "Tag cloning does not support ActorTemplates with external volumes",
                    ));
                }
                Some(t.clone())
            }
            None => template
                .status
                .as_ref()
                .and_then(|s| s.golden_snapshot_status.as_ref())
                .and_then(|g| g.golden_tag.clone()),
        };
        let hive = fields::actor_hive(&input).to_string();
        let source = match &tag_ref {
            Some(r) => Some(self.resolve_tag_source(&hive, r, &template).await?),
            None => None,
        };
        let init =
            volumes::initial_external_volumes(self.inner.workflow.volumes.as_ref(), &template)
                .await?;
        let init = if op.gate_enabled(Gate::ExternalVolumes) {
            init
        } else if !init.is_empty() {
            return Err(Error::Status(
                hivekeep_core::field::ValidationError(
                    FieldError::forbidden(
                        &Path::new(["actor", "actor_template"]),
                        format!("ActorTemplates with external volumes require the ExternalVolumes preview gate to be enabled on the server and the client to opt in to preview features (hivectl --preview, or the {PREVIEW_HEADER} header)"),
                    )
                    .into(),
                )
                .to_invalid_argument(),
            ));
        } else {
            init
        };
        let mut out = input.clone();
        let mut st = pb::ActorStatus {
            state: pb::ActorState::Suspended as i32,
            external_volumes: volumes::external_volumes_status(init),
            ..Default::default()
        };
        if let Some(tag) = source {
            // The Actor borrows the Tag's snapshot; its first suspend writes
            // one under its own prefix.
            let mut snap = tag
                .status
                .as_ref()
                .and_then(|s| s.snapshot.clone())
                .unwrap_or_default();
            snap.generation = 1;
            snap.owner = pb::SnapshotOwner::Tag as i32;
            snap.actor_template_uid = tag
                .status
                .as_ref()
                .map(|s| s.actor_template_uid.clone())
                .unwrap_or_default();
            st.snapshots = vec![snap];
            st.last_assigned_generation = 1;
        }
        out.status = Some(st);
        internal(v::validate_actor_update(
            op,
            &Path::from("actor"),
            &out,
            &input,
            true,
        ))?;
        let stored = self.store().create_actor(&out).await.map_err(|e| match e {
            StoreError::AlreadyExists => status::already_exists(format!(
                "Actor {} already exists",
                fields::actor_name(&input)
            )),
            StoreError::FailedPrecondition => {
                status::failed_precondition(format!("Hive {hive} not found"))
            }
            e => Error::from(e).context("while recording actor"),
        })?;
        events::state_changed(&stored, op::CREATE);
        Ok(stored)
    }

    async fn resolve_tag_source(
        &self,
        actor_hive: &str,
        r: &pb::ObjectRef,
        template: &pb::ActorTemplate,
    ) -> Result<pb::Tag> {
        let tag = match self.store().get_tag(&TagRef::from_object_ref(r)).await {
            Ok(t) => t,
            Err(StoreError::NotFound) => return Err(status::failed_precondition("Tag not found")),
            Err(e) => return Err(Error::from(e).context("while getting tag")),
        };
        match pb::TagScope::try_from(tag.scope) {
            Ok(pb::TagScope::Hive) => {
                if tag.metadata.as_ref().map(|m| m.hive.as_str()) != Some(actor_hive) {
                    return Err(status::failed_precondition(
                        "Tag is not published outside its Hive",
                    ));
                }
            }
            Ok(pb::TagScope::Published) => {}
            _ => {
                return Err(status::failed_precondition(
                    "source Tag has an invalid scope",
                ))
            }
        }
        if snapshots::tag_durable_uri(&tag, Some(pb::SnapshotStorageState::Completed)).is_empty() {
            return Err(status::failed_precondition(
                "source Tag is still being created or failed creation",
            ));
        }
        let tag_template = tag
            .status
            .as_ref()
            .map(|s| s.actor_template_uid.as_str())
            .unwrap_or("");
        if tag_template != fields::template_uid(template) {
            return Err(status::failed_precondition(format!(
                "source Tag must be taken from an actor with ActorTemplate uid {tag_template:?}"
            )));
        }
        Ok(tag)
    }

    pub(crate) async fn get_actor(
        &self,
        cc: &CallContext,
        req: pb::GetActorRequest,
    ) -> Result<pb::Actor> {
        self.authorize(cc, checks::actor_ref(rel::CAN_GET, req.actor.as_ref()))
            .await?;
        invalid(v::validate_get_actor_request(&self.op(cc), &req))?;
        let r = actor_of(req.actor.as_ref());
        self.store().get_actor(&r).await.map_err(|e| match e {
            StoreError::NotFound => status::not_found(format!("Actor {r} not found")),
            e => Error::from(e).context("while getting actor from DB"),
        })
    }

    pub(crate) async fn list_actors(
        &self,
        cc: &CallContext,
        req: pb::ListActorsRequest,
    ) -> Result<pb::ListActorsResponse> {
        self.authorize(
            cc,
            checks::on_hive_or_global(rel::CAN_LIST_ACTORS, &req.hive, "hive"),
        )
        .await?;
        invalid(v::validate_list_actors_request(&self.op(cc), &req))?;
        let page = self
            .store()
            .list_actors(&req.hive, &list_options(req.options.as_ref()))
            .await?;
        Ok(pb::ListActorsResponse {
            actors: page.items,
            next_page_token: page.next_page_token,
        })
    }

    pub(crate) async fn update_actor(
        &self,
        cc: &CallContext,
        mut req: pb::UpdateActorRequest,
    ) -> Result<pb::Actor> {
        self.authorize(cc, checks::update_actor(&req)).await?;
        if let Some(a) = req.actor.as_mut() {
            scrub_for_update(a.metadata.as_mut());
            a.status = None;
        }
        let op = self.op(cc);
        invalid(v::validate_update_actor_request(&op, &req))?;
        let input = req.actor.unwrap_or_default();
        let r = fields::actor_ref(&input);
        let lease = leases::acquire(self.store(), &leases::actor_key(&r), "actor").await?;
        let res = self.update_actor_leased(&op, &r, &input).await;
        lease.close().await;
        res
    }

    async fn update_actor_leased(
        &self,
        op: &hivekeep_core::validation::Operation,
        r: &ActorRef,
        input: &pb::Actor,
    ) -> Result<pb::Actor> {
        // Template checks need reads the write closure cannot do, so they run
        // against the observed record, which the precondition pins.
        let stored = self.store().get_actor(r).await.map_err(|e| match e {
            StoreError::NotFound => status::not_found(format!("actor {r} not found")),
            e => e.into(),
        })?;
        if fields::uid(stored.metadata.as_ref()) == fields::uid(input.metadata.as_ref())
            && stored.metadata.as_ref().map(|m| m.version)
                == input.metadata.as_ref().map(|m| m.version)
            && stored.actor_template != input.actor_template
        {
            self.check_template_repoint(&stored, input).await?;
        }
        let path = Path::from("actor");
        let res = self
            .store()
            .update_actor(r, Precondition::of(input), &mut |to| {
                let old = to.clone();
                let (st, md) = (to.status.take(), to.metadata.take());
                *to = input.clone();
                to.status = st;
                to.metadata = md;
                defaults::apply(to);
                v::validate_actor_update(op, &path, to, &old, false)
                    .into_result()
                    .map_err(|e| reject(Error::Status(e.to_invalid_argument())))?;
                if old.actor_template != to.actor_template
                    && fields::state(&old) != pb::ActorState::Suspended
                {
                    return Err(reject(status::failed_precondition(format!(
                        "actor must be {} to change its actor template (got: {})",
                        fields::state_name(pb::ActorState::Suspended),
                        fields::state_name(fields::state(&old))
                    ))));
                }
                if old.actor_template != to.actor_template && old != stored {
                    return Err(reject(crate::error::conflict()));
                }
                v::validate_actor_update(op, &path, to, &old, true)
                    .into_result()
                    .map_err(|e| reject(Error::Status(e.to_internal())))
            })
            .await;
        res.map_err(|e| match Error::from(e) {
            Error::Store(StoreError::VersionConflict | StoreError::UidConflict) => {
                crate::error::conflict()
            }
            Error::Store(StoreError::NotFound) => status::not_found(format!("actor {r} not found")),
            Error::Store(e @ StoreError::PreconditionRequired(_)) => {
                status::invalid_argument(format!("while updating actor {r}: {e}"))
            }
            e => e,
        })
    }

    async fn check_template_repoint(&self, stored: &pb::Actor, input: &pb::Actor) -> Result<()> {
        let st = fields::state(stored);
        if st != pb::ActorState::Suspended {
            return Err(status::failed_precondition(format!(
                "actor must be {} to change its actor template (got: {})",
                fields::state_name(pb::ActorState::Suspended),
                fields::state_name(st)
            )));
        }
        let mut candidate = stored.clone();
        candidate.actor_template = input.actor_template.clone();
        let new_t = resolve_template(self.store(), &candidate).await?;
        self.validate_snapshot_location_unchanged(stored, &new_t)?;
        match resolve_template(self.store(), stored).await {
            Ok(old_t) => validate_template_volumes_unchanged(&old_t, &new_t),
            Err(e) if crate::workflow::is_template_not_found(&e) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Refuses a repoint that would store the Actor's next snapshots under
    /// another location than the one its own already live in, which would
    /// leak them when the Actor is deleted.
    fn validate_snapshot_location_unchanged(
        &self,
        actor: &pb::Actor,
        new_t: &pb::ActorTemplate,
    ) -> Result<()> {
        let Some(snap) = snapshots::latest(actor.status.as_ref(), |s| {
            !snapshots::durable_uri(s).is_empty()
        }) else {
            return Ok(());
        };
        let raw = snapshots::durable_uri(snap);
        let uri = SnapshotUri::parse(raw).map_err(|e| {
            status::internal(format!("while parsing the external snapshot {raw:?}: {e}"))
        })?;
        let owner = actor_snapshot_owner(actor);
        if !uri.owned_by(&owner) {
            return Ok(());
        }
        let loc = snapshots::resolve_location(
            &self.settings().snapshot_location,
            &self.settings().gates,
            new_t,
        )?;
        let next = owner.prefix(&loc).map_err(|e| {
            status::failed_precondition(format!(
                "while resolving the new actor template's storage location {loc:?}: {e}"
            ))
        })?;
        if next != uri.owner_prefix() {
            return Err(status::failed_precondition(format!(
                "the actor's snapshots are stored under {:?} but the new actor template stores them under {loc:?}: the storage location must be identical to repoint an actor that owns a snapshot",
                uri.location()
            )));
        }
        Ok(())
    }

    pub(crate) async fn delete_actor(
        &self,
        cc: &CallContext,
        req: pb::DeleteActorRequest,
    ) -> Result<pb::Actor> {
        self.authorize(cc, checks::actor_ref(rel::CAN_DELETE, req.actor.as_ref()))
            .await?;
        invalid(v::validate_delete_actor_request(&self.op(cc), &req))?;
        let r = actor_of(req.actor.as_ref());
        self.inner
            .workflow
            .delete_actor(&r, delete_preconditions(req.options.as_ref()))
            .await
            .map_err(|e| workflow_error(e, &r))
    }

    pub(crate) async fn resume_actor(
        &self,
        cc: &CallContext,
        req: pb::ResumeActorRequest,
    ) -> Result<pb::ResumeActorResponse> {
        self.authorize(cc, checks::actor_ref(rel::CAN_RESUME, req.actor.as_ref()))
            .await?;
        invalid(v::validate_resume_actor_request(&self.op(cc), &req))?;
        let r = actor_of(req.actor.as_ref());
        let (actor, resumed) = self
            .inner
            .workflow
            .resume_actor(&r)
            .await
            .map_err(|e| workflow_error(e, &r))?;
        Ok(pb::ResumeActorResponse {
            actor: Some(actor),
            resumed,
        })
    }

    pub(crate) async fn suspend_actor(
        &self,
        cc: &CallContext,
        req: pb::SuspendActorRequest,
    ) -> Result<pb::SuspendActorResponse> {
        self.authorize(cc, checks::actor_ref(rel::CAN_SUSPEND, req.actor.as_ref()))
            .await?;
        invalid(v::validate_suspend_actor_request(&self.op(cc), &req))?;
        let r = actor_of(req.actor.as_ref());
        let actor = self
            .inner
            .workflow
            .suspend_actor(&r, SuspendMode::from_options(req.options.as_ref()))
            .await
            .map_err(|e| workflow_error(e, &r))?;
        Ok(pb::SuspendActorResponse { actor: Some(actor) })
    }

    pub(crate) async fn revert_actor(
        &self,
        cc: &CallContext,
        req: pb::RevertActorRequest,
    ) -> Result<pb::RevertActorResponse> {
        self.authorize(cc, checks::actor_ref(rel::CAN_REVERT, req.actor.as_ref()))
            .await?;
        invalid(v::validate_revert_actor_request(&self.op(cc), &req))?;
        let r = actor_of(req.actor.as_ref());
        let actor = self
            .inner
            .workflow
            .revert_actor(&r)
            .await
            .map_err(|e| workflow_error(e, &r))?;
        Ok(pb::RevertActorResponse { actor: Some(actor) })
    }
}

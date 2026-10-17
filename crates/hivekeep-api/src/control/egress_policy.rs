//! Actor egress policy RPCs.

use hivekeep_core::defaults;
use hivekeep_core::field::Path;
use hivekeep_core::resource::ActorRef;
use hivekeep_core::validation::api as v;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{Precondition, StoreError};

use super::{delete_preconditions, invalid, scrub_for_create, scrub_for_update, ControlService};
use crate::authz::{checks, model::rel};
use crate::context::CallContext;
use crate::error::{reject, status, Error, Result};

fn actor_of(r: Option<&pb::ObjectRef>) -> ActorRef {
    r.map(ActorRef::from_object_ref)
        .unwrap_or_else(|| ActorRef::new("", ""))
}

/// Maps a store failure of an egress policy write.
pub(crate) fn map_write(e: StoreError) -> Error {
    match e {
        StoreError::NotFound => status::not_found("EgressPolicy not found"),
        StoreError::AlreadyExists => status::already_exists("EgressPolicy already exists"),
        StoreError::VersionConflict => status::aborted("EgressPolicy version conflict"),
        StoreError::UidConflict => status::aborted("EgressPolicy UID conflict"),
        StoreError::PreconditionRequired(_) => {
            status::invalid_argument("EgressPolicy UID and version are required")
        }
        StoreError::FailedPrecondition => {
            status::failed_precondition("parent Actor does not exist")
        }
        e => Error::from(e).context("while writing EgressPolicy"),
    }
}

impl ControlService {
    pub(crate) async fn create_actor_egress_policy(
        &self,
        cc: &CallContext,
        mut req: pb::CreateActorEgressPolicyRequest,
    ) -> Result<pb::EgressPolicy> {
        self.authorize(
            cc,
            checks::actor_ref(rel::CAN_CREATE_EGRESS_POLICY, req.actor.as_ref()),
        )
        .await?;
        if let Some(p) = req.egress_policy.as_mut() {
            scrub_for_create(p.metadata.as_mut());
            defaults::apply(p);
        }
        invalid(v::validate_create_actor_egress_policy_request(
            &self.op(cc),
            &req,
        ))?;
        let r = actor_of(req.actor.as_ref());
        self.store()
            .create_egress_policy(&r, &req.egress_policy.unwrap_or_default())
            .await
            .map_err(map_write)
    }

    pub(crate) async fn get_actor_egress_policy(
        &self,
        cc: &CallContext,
        req: pb::GetActorEgressPolicyRequest,
    ) -> Result<pb::EgressPolicy> {
        self.authorize(
            cc,
            checks::actor_ref(rel::CAN_GET_EGRESS_POLICY, req.actor.as_ref()),
        )
        .await?;
        invalid(v::validate_get_actor_egress_policy_request(
            &self.op(cc),
            &req,
        ))?;
        let r = actor_of(req.actor.as_ref());
        self.store()
            .get_egress_policy(&r)
            .await
            .map_err(|e| match e {
                StoreError::NotFound => {
                    status::not_found(format!("EgressPolicy for actor {r} not found"))
                }
                e => Error::from(e).context("while getting Actor egress policy"),
            })
    }

    pub(crate) async fn update_actor_egress_policy(
        &self,
        cc: &CallContext,
        mut req: pb::UpdateActorEgressPolicyRequest,
    ) -> Result<pb::EgressPolicy> {
        self.authorize(
            cc,
            checks::actor_ref(rel::CAN_UPDATE_EGRESS_POLICY, req.actor.as_ref()),
        )
        .await?;
        if let Some(p) = req.egress_policy.as_mut() {
            scrub_for_update(p.metadata.as_mut());
        }
        let op = self.op(cc);
        invalid(v::validate_update_actor_egress_policy_request(&op, &req))?;
        let r = actor_of(req.actor.as_ref());
        let input = req.egress_policy.unwrap_or_default();
        self.store()
            .update_egress_policy(&r, Precondition::of(&input), &mut |to| {
                let old = to.clone();
                let md = to.metadata.take();
                *to = input.clone();
                to.metadata = md;
                defaults::apply(to);
                v::validate_egress_policy_update(&op, &Path::from("egress_policy"), to, &old)
                    .into_result()
                    .map_err(|e| reject(Error::Status(e.to_invalid_argument())))
            })
            .await
            .map_err(|e| match Error::from(e) {
                Error::Store(e) => map_write(e),
                e => e,
            })
    }

    pub(crate) async fn delete_actor_egress_policy(
        &self,
        cc: &CallContext,
        req: pb::DeleteActorEgressPolicyRequest,
    ) -> Result<pb::EgressPolicy> {
        self.authorize(
            cc,
            checks::actor_ref(rel::CAN_DELETE_EGRESS_POLICY, req.actor.as_ref()),
        )
        .await?;
        invalid(v::validate_delete_actor_egress_policy_request(
            &self.op(cc),
            &req,
        ))?;
        let r = actor_of(req.actor.as_ref());
        self.store()
            .delete_egress_policy(&r, delete_preconditions(req.options.as_ref()))
            .await
            .map_err(map_write)
    }
}

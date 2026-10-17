//! Global and Hive access policy RPCs.

use hivekeep_core::defaults;
use hivekeep_core::field::Path;
use hivekeep_core::validation::api as v;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{Precondition, StoreError};

use super::{delete_preconditions, invalid, scrub_for_create, scrub_for_update, ControlService};
use crate::authz::{checks, model::rel};
use crate::context::CallContext;
use crate::error::{reject, status, Error, Result};

fn map_write(e: Error) -> Error {
    match e {
        Error::Store(e) => match e {
            StoreError::NotFound => status::not_found("AccessPolicy not found"),
            StoreError::AlreadyExists => status::already_exists("AccessPolicy already exists"),
            StoreError::VersionConflict => status::aborted("AccessPolicy version conflict"),
            StoreError::UidConflict => status::aborted("AccessPolicy UID conflict"),
            StoreError::PreconditionRequired(_) => {
                status::invalid_argument("AccessPolicy UID and version are required")
            }
            StoreError::FailedPrecondition => {
                status::failed_precondition("parent Hive does not exist")
            }
            e => Error::from(e).context("while writing AccessPolicy"),
        },
        e => e,
    }
}

fn hive_name(r: Option<&pb::ObjectRef>) -> String {
    r.map(|r| r.name.clone()).unwrap_or_default()
}

/// A whole-object replace keeping the stored metadata, then validated
/// against the stored policy by `check`.
fn replace(
    to: &mut pb::AccessPolicy,
    input: &pb::AccessPolicy,
    check: impl Fn(&pb::AccessPolicy, &pb::AccessPolicy) -> hivekeep_core::field::ErrorList,
) -> hivekeep_store::Result<()> {
    let old = to.clone();
    let md = to.metadata.take();
    *to = input.clone();
    to.metadata = md;
    defaults::apply(to);
    check(to, &old)
        .into_result()
        .map_err(|e| reject(Error::Status(e.to_invalid_argument())))
}

impl ControlService {
    pub(crate) async fn create_global_access_policy(
        &self,
        cc: &CallContext,
        mut req: pb::CreateGlobalAccessPolicyRequest,
    ) -> Result<pb::AccessPolicy> {
        self.authorize(cc, checks::on_global(rel::CAN_CREATE_ACCESS_POLICY))
            .await?;
        if let Some(p) = req.access_policy.as_mut() {
            scrub_for_create(p.metadata.as_mut());
            defaults::apply(p);
        }
        invalid(v::validate_create_global_access_policy_request(
            &self.op(cc),
            &req,
        ))?;
        self.store()
            .create_global_access_policy(&req.access_policy.unwrap_or_default())
            .await
            .map_err(|e| map_write(e.into()))
    }

    pub(crate) async fn get_global_access_policy(
        &self,
        cc: &CallContext,
        req: pb::GetGlobalAccessPolicyRequest,
    ) -> Result<pb::AccessPolicy> {
        self.authorize(cc, checks::on_global(rel::CAN_GET_ACCESS_POLICY))
            .await?;
        invalid(v::validate_get_global_access_policy_request(
            &self.op(cc),
            &req,
        ))?;
        self.store()
            .get_global_access_policy()
            .await
            .map_err(|e| match e {
                StoreError::NotFound => status::not_found("Global AccessPolicy not found"),
                e => Error::from(e).context("while getting Global access policy"),
            })
    }

    pub(crate) async fn update_global_access_policy(
        &self,
        cc: &CallContext,
        mut req: pb::UpdateGlobalAccessPolicyRequest,
    ) -> Result<pb::AccessPolicy> {
        self.authorize(cc, checks::on_global(rel::CAN_UPDATE_ACCESS_POLICY))
            .await?;
        if let Some(p) = req.access_policy.as_mut() {
            scrub_for_update(p.metadata.as_mut());
        }
        let op = self.op(cc);
        invalid(v::validate_update_global_access_policy_request(&op, &req))?;
        let input = req.access_policy.unwrap_or_default();
        let path = Path::from("access_policy");
        self.store()
            .update_global_access_policy(Precondition::of(&input), &mut |to| {
                replace(to, &input, |n, o| {
                    v::validate_global_access_policy_update(&op, &path, n, o)
                })
            })
            .await
            .map_err(|e| map_write(e.into()))
    }

    pub(crate) async fn create_hive_access_policy(
        &self,
        cc: &CallContext,
        mut req: pb::CreateHiveAccessPolicyRequest,
    ) -> Result<pb::AccessPolicy> {
        self.authorize(
            cc,
            checks::hive_ref(rel::CAN_CREATE_ACCESS_POLICY, req.hive.as_ref()),
        )
        .await?;
        if let Some(p) = req.access_policy.as_mut() {
            scrub_for_create(p.metadata.as_mut());
            defaults::apply(p);
        }
        invalid(v::validate_create_hive_access_policy_request(
            &self.op(cc),
            &req,
        ))?;
        self.store()
            .create_hive_access_policy(
                &hive_name(req.hive.as_ref()),
                &req.access_policy.unwrap_or_default(),
            )
            .await
            .map_err(|e| map_write(e.into()))
    }

    pub(crate) async fn get_hive_access_policy(
        &self,
        cc: &CallContext,
        req: pb::GetHiveAccessPolicyRequest,
    ) -> Result<pb::AccessPolicy> {
        self.authorize(
            cc,
            checks::hive_ref(rel::CAN_GET_ACCESS_POLICY, req.hive.as_ref()),
        )
        .await?;
        invalid(v::validate_get_hive_access_policy_request(
            &self.op(cc),
            &req,
        ))?;
        let name = hive_name(req.hive.as_ref());
        self.store()
            .get_hive_access_policy(&name)
            .await
            .map_err(|e| match e {
                StoreError::NotFound => {
                    status::not_found(format!("AccessPolicy for hive {name} not found"))
                }
                e => Error::from(e).context("while getting Hive access policy"),
            })
    }

    pub(crate) async fn update_hive_access_policy(
        &self,
        cc: &CallContext,
        mut req: pb::UpdateHiveAccessPolicyRequest,
    ) -> Result<pb::AccessPolicy> {
        self.authorize(
            cc,
            checks::hive_ref(rel::CAN_UPDATE_ACCESS_POLICY, req.hive.as_ref()),
        )
        .await?;
        if let Some(p) = req.access_policy.as_mut() {
            scrub_for_update(p.metadata.as_mut());
        }
        let op = self.op(cc);
        invalid(v::validate_update_hive_access_policy_request(&op, &req))?;
        let input = req.access_policy.clone().unwrap_or_default();
        let path = Path::from("access_policy");
        self.store()
            .update_hive_access_policy(
                &hive_name(req.hive.as_ref()),
                Precondition::of(&input),
                &mut |to| {
                    replace(to, &input, |n, o| {
                        v::validate_hive_access_policy_update(&op, &path, n, o)
                    })
                },
            )
            .await
            .map_err(|e| map_write(e.into()))
    }

    pub(crate) async fn delete_hive_access_policy(
        &self,
        cc: &CallContext,
        req: pb::DeleteHiveAccessPolicyRequest,
    ) -> Result<pb::AccessPolicy> {
        self.authorize(
            cc,
            checks::hive_ref(rel::CAN_DELETE_ACCESS_POLICY, req.hive.as_ref()),
        )
        .await?;
        invalid(v::validate_delete_hive_access_policy_request(
            &self.op(cc),
            &req,
        ))?;
        self.store()
            .delete_hive_access_policy(
                &hive_name(req.hive.as_ref()),
                delete_preconditions(req.options.as_ref()),
            )
            .await
            .map_err(|e| map_write(e.into()))
    }
}

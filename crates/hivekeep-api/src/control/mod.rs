//! The `Control` service.
//!
//! Each handler authorizes the caller, scrubs server-owned fields, applies
//! defaults, validates the request, and then either writes through the store
//! (re-validating the merged result against the stored value inside the
//! write) or runs a workflow.

mod access_policy;
pub mod actor;
mod egress_policy;
mod hive;
mod identity;
mod tag;
mod template;
mod worker;

pub use actor::validate_template_volumes_unchanged;
pub(crate) use identity::mint_error;

use std::sync::Arc;

use hivekeep_core::field::ErrorList;
use hivekeep_core::validation::Operation;
use hivekeep_identity::ca::CertificateAuthority;
use hivekeep_identity::jwt::authority::JwtSigner;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::api::v1::control_server::Control;
use hivekeep_store::{ListOptions, Store};
use tonic::{Request, Response, Status};

use crate::authz::{self, checks::Checks, Authorizer};
use crate::context::CallContext;
use crate::error::{Error, Result};
use crate::worker_workflow::WorkerWorkflow;
use crate::workflow::{ActorWorkflow, Settings};

/// Largest page any list returns; also the default.
pub const MAX_PAGE_SIZE: i32 = 1000;

/// Identity material the mint RPCs sign with.
#[derive(Clone)]
pub struct Identity {
    /// `iss` of every Actor JWT.
    pub jwt_issuer: String,
    /// Signs Actor JWTs.
    pub jwt_signer: Arc<dyn JwtSigner>,
    /// Signs Actor certificates.
    pub ca: Arc<dyn CertificateAuthority>,
}

pub(crate) struct Inner {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) workflow: Arc<ActorWorkflow>,
    pub(crate) workers: Arc<WorkerWorkflow>,
    pub(crate) authz: Arc<dyn Authorizer>,
    pub(crate) identity: Identity,
}

/// The Control service. Cheap to clone.
#[derive(Clone)]
pub struct ControlService {
    pub(crate) inner: Arc<Inner>,
}

impl ControlService {
    /// The service over its dependencies.
    pub fn new(
        store: Arc<dyn Store>,
        workflow: Arc<ActorWorkflow>,
        workers: Arc<WorkerWorkflow>,
        authz: Arc<dyn Authorizer>,
        identity: Identity,
    ) -> Self {
        ControlService {
            inner: Arc::new(Inner {
                store,
                workflow,
                workers,
                authz,
                identity,
            }),
        }
    }

    /// The Actor workflows, for in-process callers.
    pub fn workflow(&self) -> &Arc<ActorWorkflow> {
        &self.inner.workflow
    }

    pub(crate) fn store(&self) -> &dyn Store {
        self.inner.store.as_ref()
    }

    pub(crate) fn settings(&self) -> &Settings {
        &self.inner.workflow.settings
    }

    /// The validation context of a request.
    pub(crate) fn op(&self, cc: &CallContext) -> Operation {
        Operation::create()
            .with_gates(self.settings().gates.clone())
            .with_opt_out(!cc.preview_opt_in)
    }

    pub(crate) async fn authorize(&self, cc: &CallContext, checks: Checks) -> Result<()> {
        authz::authorize(self.inner.authz.as_ref(), cc, checks).await?;
        Ok(())
    }
}

/// INVALID_ARGUMENT for request validation errors.
pub(crate) fn invalid(errs: ErrorList) -> Result<()> {
    errs.into_result()
        .map_err(|e| Error::Status(e.to_invalid_argument()))
}

/// INTERNAL for a server-built value that fails validation.
pub(crate) fn internal(errs: ErrorList) -> Result<()> {
    errs.into_result()
        .map_err(|e| Error::Status(e.to_internal()))
}

/// Clears metadata a client may not set on create.
pub(crate) fn scrub_for_create(md: Option<&mut pb::ResourceMetadata>) {
    if let Some(m) = md {
        m.uid.clear();
        m.version = 0;
        m.create_time = None;
        m.update_time = None;
        m.delete_time = None;
    }
}

/// Clears metadata a client may not set on update; uid and version stay as
/// preconditions.
pub(crate) fn scrub_for_update(md: Option<&mut pb::ResourceMetadata>) {
    if let Some(m) = md {
        m.create_time = None;
        m.update_time = None;
        m.delete_time = None;
    }
}

/// Store list options of a request: the default page size for unset, and
/// oversized values coerced down.
pub(crate) fn list_options(o: Option<&pb::ListOptions>) -> ListOptions {
    let size = o.map(|o| o.page_size).unwrap_or(0);
    let size = if size == 0 || size > MAX_PAGE_SIZE {
        MAX_PAGE_SIZE
    } else {
        size
    };
    ListOptions::new(size, o.map(|o| o.page_token.clone()).unwrap_or_default())
}

/// Store delete preconditions of a request.
pub(crate) fn delete_preconditions(
    o: Option<&pb::DeleteOptions>,
) -> hivekeep_store::DeletePreconditions {
    hivekeep_store::DeletePreconditions::new(
        o.map(|o| o.uid.clone()).unwrap_or_default(),
        o.map(|o| o.version).unwrap_or(0),
    )
}

macro_rules! rpcs {
    ($($name:ident($req:ty) -> $resp:ty;)*) => {
        #[tonic::async_trait]
        impl Control for ControlService {
            $(
                async fn $name(&self, request: Request<$req>) -> std::result::Result<Response<$resp>, Status> {
                    let cc = CallContext::of(&request);
                    match self.$name(&cc, request.into_inner()).await {
                        Ok(r) => Ok(Response::new(r)),
                        Err(e) => Err(Status::from(e)),
                    }
                }
            )*
        }
    };
}

rpcs! {
    get_actor(pb::GetActorRequest) -> pb::Actor;
    create_actor(pb::CreateActorRequest) -> pb::Actor;
    update_actor(pb::UpdateActorRequest) -> pb::Actor;
    suspend_actor(pb::SuspendActorRequest) -> pb::SuspendActorResponse;
    resume_actor(pb::ResumeActorRequest) -> pb::ResumeActorResponse;
    revert_actor(pb::RevertActorRequest) -> pb::RevertActorResponse;
    delete_actor(pb::DeleteActorRequest) -> pb::Actor;
    list_actors(pb::ListActorsRequest) -> pb::ListActorsResponse;
    get_actor_egress_policy(pb::GetActorEgressPolicyRequest) -> pb::EgressPolicy;
    create_actor_egress_policy(pb::CreateActorEgressPolicyRequest) -> pb::EgressPolicy;
    update_actor_egress_policy(pb::UpdateActorEgressPolicyRequest) -> pb::EgressPolicy;
    delete_actor_egress_policy(pb::DeleteActorEgressPolicyRequest) -> pb::EgressPolicy;
    mint_actor_jwt(pb::MintActorJwtRequest) -> pb::MintActorJwtResponse;
    mint_actor_certificate(pb::MintActorCertificateRequest) -> pb::MintActorCertificateResponse;
    create_tag(pb::CreateTagRequest) -> pb::Tag;
    get_tag(pb::GetTagRequest) -> pb::Tag;
    list_tags(pb::ListTagsRequest) -> pb::ListTagsResponse;
    update_tag(pb::UpdateTagRequest) -> pb::Tag;
    delete_tag(pb::DeleteTagRequest) -> pb::Tag;
    list_workers(pb::ListWorkersRequest) -> pb::ListWorkersResponse;
    get_worker(pb::GetWorkerRequest) -> pb::Worker;
    create_worker(pb::CreateWorkerRequest) -> pb::Worker;
    update_worker(pb::UpdateWorkerRequest) -> pb::Worker;
    delete_worker(pb::DeleteWorkerRequest) -> pb::Worker;
    drain_worker(pb::DrainWorkerRequest) -> pb::DrainWorkerResponse;
    mark_worker_available(pb::MarkWorkerAvailableRequest) -> pb::MarkWorkerAvailableResponse;
    mark_worker_unavailable(pb::MarkWorkerUnavailableRequest) -> pb::MarkWorkerUnavailableResponse;
    list_worker_actor_assignments(pb::ListWorkerActorAssignmentsRequest) -> pb::ListWorkerActorAssignmentsResponse;
    create_hive(pb::CreateHiveRequest) -> pb::Hive;
    get_hive(pb::GetHiveRequest) -> pb::Hive;
    list_hives(pb::ListHivesRequest) -> pb::ListHivesResponse;
    delete_hive(pb::DeleteHiveRequest) -> pb::Hive;
    create_actor_template(pb::CreateActorTemplateRequest) -> pb::ActorTemplate;
    get_actor_template(pb::GetActorTemplateRequest) -> pb::ActorTemplate;
    list_actor_templates(pb::ListActorTemplatesRequest) -> pb::ListActorTemplatesResponse;
    delete_actor_template(pb::DeleteActorTemplateRequest) -> pb::ActorTemplate;
    get_global_access_policy(pb::GetGlobalAccessPolicyRequest) -> pb::AccessPolicy;
    create_global_access_policy(pb::CreateGlobalAccessPolicyRequest) -> pb::AccessPolicy;
    update_global_access_policy(pb::UpdateGlobalAccessPolicyRequest) -> pb::AccessPolicy;
    get_hive_access_policy(pb::GetHiveAccessPolicyRequest) -> pb::AccessPolicy;
    create_hive_access_policy(pb::CreateHiveAccessPolicyRequest) -> pb::AccessPolicy;
    update_hive_access_policy(pb::UpdateHiveAccessPolicyRequest) -> pb::AccessPolicy;
    delete_hive_access_policy(pb::DeleteHiveAccessPolicyRequest) -> pb::AccessPolicy;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_sizes() {
        assert_eq!(list_options(None).page_size, MAX_PAGE_SIZE);
        assert_eq!(
            list_options(Some(&pb::ListOptions {
                page_size: 5,
                page_token: "t".into()
            }))
            .page_size,
            5
        );
        assert_eq!(
            list_options(Some(&pb::ListOptions {
                page_size: 5000,
                page_token: String::new()
            }))
            .page_size,
            MAX_PAGE_SIZE
        );
        assert_eq!(
            list_options(Some(&pb::ListOptions {
                page_size: -1,
                page_token: String::new()
            }))
            .page_size,
            -1
        );
    }
}

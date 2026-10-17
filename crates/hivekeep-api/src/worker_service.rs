//! The `WorkerService`: what keepers report about their workers, and the
//! credentials they mint for the Actors they host.
//!
//! Every RPC authenticates the calling keeper by its mTLS identity and
//! authorizes against authoritative state only: a Worker or Actor that is
//! not on the caller's node is reported NOT_FOUND, so a keeper learns
//! nothing about other nodes.

use std::time::SystemTime;

use hivekeep_core::resource::ActorRef;
use hivekeep_core::validation::api as v;
use hivekeep_core::validation::Operation;
use hivekeep_identity::actor_cert::{mint_actor_certificate, ActorCertKind};
use hivekeep_identity::extension::ActorIdentity;
use hivekeep_identity::jwt::claims::Claims;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::api::v1::worker_service_server::WorkerService;
use hivekeep_store::{Precondition, StoreError};
use tonic::{Request, Response, Status};

use crate::context::{system_request, CallContext};
use crate::control::{ControlService, Identity};
use crate::error::{status, Error, Result};
use crate::fields;
use crate::hosting::hosting_node;
use crate::keeper_auth::{self, KeeperCaller};
use crate::runtime_compat;

/// The WorkerService.
#[derive(Clone)]
pub struct WorkerServiceImpl {
    control: ControlService,
    keeper_spiffe_id: String,
    identity: Identity,
}

fn worker_write_error(name: &str, doing: &str, e: StoreError) -> Error {
    match e {
        StoreError::NotFound => status::not_found(format!("Worker {name} not found")),
        StoreError::UidConflict | StoreError::VersionConflict => crate::error::conflict(),
        e => Error::from(e).context(format!("while {doing} for worker {name}")),
    }
}

fn sort_attributes(c: Option<&mut pb::VersionedSandboxCompat>) {
    if let Some(c) = c {
        c.attributes.sort_by(|a, b| a.key.cmp(&b.key));
    }
}

fn actor_of(r: Option<&pb::ObjectRef>) -> ActorRef {
    r.map(ActorRef::from_object_ref)
        .unwrap_or_else(|| ActorRef::new("", ""))
}

impl WorkerServiceImpl {
    /// The service, delegating Actor operations to `control`.
    pub fn new(
        control: ControlService,
        keeper_spiffe_id: impl Into<String>,
        identity: Identity,
    ) -> Self {
        WorkerServiceImpl {
            control,
            keeper_spiffe_id: keeper_spiffe_id.into(),
            identity,
        }
    }

    fn store(&self) -> &dyn hivekeep_store::Store {
        self.control.store()
    }

    fn op(&self) -> Operation {
        Operation::create().with_gates(self.control.settings().gates.clone())
    }

    fn auth(&self, cc: &CallContext) -> Result<KeeperCaller> {
        Ok(keeper_auth::authenticate(cc, &self.keeper_spiffe_id)?)
    }

    async fn worker_on_caller_node(&self, name: &str, caller: &KeeperCaller) -> Result<pb::Worker> {
        let mut w = self.store().get_worker(name).await.map_err(|e| match e {
            StoreError::NotFound => status::not_found(format!("Worker {name} not found")),
            e => Error::from(e).context(format!("while fetching worker {name}")),
        })?;
        if w.node_name != caller.node_name {
            tracing::warn!(worker = name, worker_node = %w.node_name, caller_node = %caller.node_name, "refusing a report for a worker on another node");
            return Err(status::not_found(format!("Worker {name} not found")));
        }
        w.status.get_or_insert_with(Default::default);
        Ok(w)
    }

    async fn authorize_hosted_actor(
        &self,
        caller: &KeeperCaller,
        worker: &str,
        r: &ActorRef,
        uid: &str,
    ) -> Result<()> {
        self.worker_on_caller_node(worker, caller).await?;
        let actor = self.store().get_actor(r).await.map_err(|e| match e {
            StoreError::NotFound => status::not_found(format!("Actor {r} not found")),
            e => Error::from(e).context(format!("while fetching actor {r}")),
        })?;
        if fields::actor_uid(&actor) != uid || fields::assigned_worker(&actor) != worker {
            tracing::warn!(actor = %r, worker, "refusing a request for an actor the worker does not host");
            return Err(status::not_found(format!("Actor {r} not found")));
        }
        Ok(())
    }

    async fn check_actor_on_node(
        &self,
        actor: &pb::Actor,
        node: &str,
        credential: &str,
    ) -> Result<()> {
        let hosting = hosting_node(self.store(), actor).await?;
        if hosting.is_empty() || hosting != node {
            tracing::warn!(actor = %fields::actor_ref(actor), hosting_node = %hosting, caller_node = node,
                "refusing a worker-for-actor {credential} for an actor the caller's node does not host");
            return Err(status::not_found("actor not found"));
        }
        Ok(())
    }

    async fn load_actor(&self, r: Option<&pb::ObjectRef>) -> Result<pb::Actor> {
        self.store()
            .get_actor(&actor_of(r))
            .await
            .map_err(|e| match e {
                StoreError::NotFound => status::not_found("actor not found"),
                e => Error::from(e).context("while retrieving actor"),
            })
    }

    async fn acquire_worker_epoch(
        &self,
        cc: &CallContext,
        req: pb::AcquireWorkerEpochRequest,
    ) -> Result<pb::AcquireWorkerEpochResponse> {
        let caller = self.auth(cc)?;
        crate::control_invalid(v::validate_acquire_worker_epoch_request(&self.op(), &req))?;
        let name = req.worker.map(|w| w.name).unwrap_or_default();
        let w = self.worker_on_caller_node(&name, &caller).await?;
        let updated = self
            .store()
            .update_worker(&name, Precondition::of(&w), &mut |w| {
                w.status.get_or_insert_with(Default::default).issued_epoch += 1;
                Ok(())
            })
            .await
            .map_err(|e| worker_write_error(&name, "acquiring an epoch", e))?;
        let epoch = updated.status.map(|s| s.issued_epoch).unwrap_or(0);
        tracing::info!(worker = %name, epoch, "worker acquired an epoch");
        Ok(pb::AcquireWorkerEpochResponse { epoch })
    }

    async fn register_worker(
        &self,
        cc: &CallContext,
        req: pb::RegisterWorkerRequest,
    ) -> Result<pb::RegisterWorkerResponse> {
        let caller = self.auth(cc)?;
        crate::control_invalid(v::validate_register_worker_request(&self.op(), &req))?;
        let name = req
            .worker
            .as_ref()
            .map(|w| w.name.clone())
            .unwrap_or_default();
        let w = self.worker_on_caller_node(&name, &caller).await?;
        let st = w.status.clone().unwrap_or_default();
        let epoch = req.epoch;
        if epoch > st.issued_epoch {
            return Err(status::failed_precondition(format!(
                "epoch {epoch} was never handed out for Worker {name}"
            )));
        }
        if epoch < st.registered_epoch {
            return Err(status::failed_precondition(format!(
                "epoch {epoch} of Worker {name} is superseded by epoch {}",
                st.registered_epoch
            )));
        }
        if epoch == st.registered_epoch {
            // A retry of the registration that landed is fine; anything else
            // is refused.
            if st.ips == req.ips && st.capacity == req.capacity {
                return Ok(pb::RegisterWorkerResponse { worker: Some(w) });
            }
            return Err(status::failed_precondition(format!(
                "epoch {epoch} of Worker {name} is already registered with other IPs or capacity"
            )));
        }
        let updated = self
            .store()
            .update_worker(&name, Precondition::of(&w), &mut |w| {
                let s = w.status.get_or_insert_with(Default::default);
                s.registered_epoch = epoch;
                // The new process reports its own runtimes; until then the
                // Worker takes no Actors.
                s.sandbox_classes.clear();
                s.ips = req.ips.clone();
                s.capacity = req.capacity;
                Ok(())
            })
            .await
            .map_err(|e| worker_write_error(&name, "registering", e))?;
        tracing::info!(worker = %name, epoch, previous_epoch = st.registered_epoch, "worker registered");
        Ok(pb::RegisterWorkerResponse {
            worker: Some(updated),
        })
    }

    async fn set_worker_sandbox_runtimes(
        &self,
        cc: &CallContext,
        mut req: pb::SetWorkerSandboxRuntimesRequest,
    ) -> Result<pb::SetWorkerSandboxRuntimesResponse> {
        let caller = self.auth(cc)?;
        crate::control_invalid(v::validate_set_worker_sandbox_runtimes_request(
            &self.op(),
            &req,
        ))?;
        // Attribute order carries no meaning; canonicalize before comparing.
        for c in &mut req.sandbox_classes {
            sort_attributes(c.default_runtime.as_mut().and_then(|r| r.version.as_mut()));
            for rt in &mut c.restorable_runtimes {
                sort_attributes(rt.version.as_mut());
            }
        }
        let name = req
            .worker
            .as_ref()
            .map(|w| w.name.clone())
            .unwrap_or_default();
        let w = self.worker_on_caller_node(&name, &caller).await?;
        let st = w.status.clone().unwrap_or_default();
        if req.epoch != st.registered_epoch {
            return Err(status::failed_precondition(format!(
                "epoch {} is not the registered epoch {} of Worker {name}",
                req.epoch, st.registered_epoch
            )));
        }
        let recorded = runtime_compat::sandbox_class(Some(&st));
        if !recorded.is_empty() {
            if let Some((i, c)) = req
                .sandbox_classes
                .iter()
                .enumerate()
                .find(|(_, c)| c.sandbox_class != recorded)
            {
                return Err(status::failed_precondition(format!(
                    "sandbox_classes[{i}].sandbox_class {:?} does not match Worker's recorded sandbox_class {recorded:?}",
                    c.sandbox_class
                )));
            }
        }
        if st.sandbox_classes == req.sandbox_classes {
            return Ok(pb::SetWorkerSandboxRuntimesResponse { worker: Some(w) });
        }
        let classes = req.sandbox_classes.clone();
        let updated = self
            .store()
            .update_worker(&name, Precondition::of(&w), &mut |w| {
                w.status
                    .get_or_insert_with(Default::default)
                    .sandbox_classes = classes.clone();
                Ok(())
            })
            .await
            .map_err(|e| worker_write_error(&name, "recording sandbox runtimes", e))?;
        Ok(pb::SetWorkerSandboxRuntimesResponse {
            worker: Some(updated),
        })
    }

    async fn mint_worker_actor_certificate(
        &self,
        cc: &CallContext,
        req: pb::MintWorkerActorCertificateRequest,
    ) -> Result<pb::MintWorkerActorCertificateResponse> {
        crate::control_invalid(v::validate_mint_worker_actor_certificate_request(
            &self.op(),
            &req,
        ))?;
        let caller = self.auth(cc)?;
        let actor = self.load_actor(req.actor.as_ref()).await?;
        self.check_actor_on_node(&actor, &caller.node_name, "certificate")
            .await?;
        let assigned = fields::assigned_worker(&actor);
        let requested = req.worker.as_ref().map(|w| w.name.as_str()).unwrap_or("");
        if assigned.is_empty() || assigned != requested {
            tracing::warn!(assigned, requested, "refusing a worker-for-actor certificate for an actor not assigned to the calling worker");
            return Err(status::not_found("actor not found"));
        }
        if fields::actor_uid(&actor) != req.actor_uid {
            return Err(status::aborted(
                "conflict; actor has been deleted and recreated",
            ));
        }
        let identity = ActorIdentity {
            hive: fields::actor_hive(&actor).into(),
            actor_name: fields::actor_name(&actor).into(),
            actor_uid: fields::actor_uid(&actor).into(),
        };
        let chain = mint_actor_certificate(
            self.identity.ca.as_ref(),
            &identity,
            ActorCertKind::WorkerForActor,
            &req.certificate_signing_request,
            SystemTime::now(),
        )
        .map_err(|e| crate::control::mint_error(e, "certificate"))?;
        Ok(pb::MintWorkerActorCertificateResponse {
            actor_certificates: chain,
        })
    }

    async fn mint_worker_actor_jwt(
        &self,
        cc: &CallContext,
        req: pb::MintWorkerActorJwtRequest,
    ) -> Result<pb::MintWorkerActorJwtResponse> {
        crate::control_invalid(v::validate_mint_worker_actor_jwt_request(&self.op(), &req))?;
        let caller = self.auth(cc)?;
        // Not necessarily running: tokens fetch snapshots before a resume
        // and upload them after a suspend.
        let actor = self.load_actor(req.actor.as_ref()).await?;
        self.check_actor_on_node(&actor, &caller.node_name, "JWT")
            .await?;
        if fields::actor_uid(&actor) != req.actor_uid {
            return Err(status::aborted(
                "conflict; actor has been deleted and recreated",
            ));
        }
        let claims = Claims::for_worker_for_actor(
            &self.identity.jwt_issuer,
            fields::actor_hive(&actor),
            fields::actor_name(&actor),
            fields::actor_uid(&actor),
            &caller.node_name,
            req.audiences.clone(),
            SystemTime::now(),
        )
        .map_err(|e| crate::control::mint_error(e, "worker-for-actor JWT"))?;
        let token = self
            .identity
            .jwt_signer
            .sign_jwt(&claims)
            .map_err(|e| crate::control::mint_error(e, "worker-for-actor JWT"))?;
        Ok(pb::MintWorkerActorJwtResponse {
            actor_jwt: token,
            expire_time: Some(claims.expiration.into()),
        })
    }

    async fn request_actor_suspend(
        &self,
        cc: &CallContext,
        req: pb::RequestActorSuspendRequest,
    ) -> Result<pb::RequestActorSuspendResponse> {
        let caller = self.auth(cc)?;
        crate::control_invalid(v::validate_request_actor_suspend_request(&self.op(), &req))?;
        let worker = req
            .worker
            .as_ref()
            .map(|w| w.name.clone())
            .unwrap_or_default();
        let r = actor_of(req.actor.as_ref());
        self.authorize_hosted_actor(&caller, &worker, &r, &req.actor_uid)
            .await?;
        // From here it is an ordinary suspend, subject to every precondition
        // a client's would be.
        let resp = WorkerServiceImpl::control_call(
            &self.control,
            pb::SuspendActorRequest {
                actor: req.actor.clone(),
                options: None,
            },
        )
        .await?;
        tracing::info!(worker = %worker, actor = %r, node = %caller.node_name, "worker requested an actor suspend");
        Ok(pb::RequestActorSuspendResponse { actor: resp.actor })
    }

    async fn control_call(
        control: &ControlService,
        req: pb::SuspendActorRequest,
    ) -> Result<pb::SuspendActorResponse> {
        let r = system_request(req);
        let cc = CallContext::of(&r);
        control.suspend_actor(&cc, r.into_inner()).await
    }

    async fn report_actor_snapshot_state(
        &self,
        cc: &CallContext,
        req: pb::ReportActorSnapshotStateRequest,
    ) -> Result<pb::ReportActorSnapshotStateResponse> {
        let caller = self.auth(cc)?;
        crate::control_invalid(v::validate_report_actor_snapshot_state_request(
            &self.op(),
            &req,
        ))?;
        let r = actor_of(req.actor.as_ref());
        self.control
            .workflow()
            .apply_reported_snapshot_state(
                &r,
                &req.actor_uid,
                &caller.node_name,
                &req.state.unwrap_or_default(),
            )
            .await?;
        Ok(pb::ReportActorSnapshotStateResponse {})
    }

    async fn report_actor_crashed(
        &self,
        cc: &CallContext,
        req: pb::ReportActorCrashedRequest,
    ) -> Result<pb::ReportActorCrashedResponse> {
        let caller = self.auth(cc)?;
        crate::control_invalid(v::validate_report_actor_crashed_request(&self.op(), &req))?;
        let worker = req
            .worker
            .as_ref()
            .map(|w| w.name.clone())
            .unwrap_or_default();
        let r = actor_of(req.actor.as_ref());
        self.authorize_hosted_actor(&caller, &worker, &r, &req.actor_uid)
            .await?;
        let actor = self
            .control
            .workflow()
            .crash_actor_reported_by_worker(&r, &req.actor_uid, &worker, &req.message)
            .await
            .map_err(|e| match e {
                Error::Store(StoreError::VersionConflict) => crate::error::conflict(),
                e => e,
            })?;
        tracing::info!(worker = %worker, actor = %r, node = %caller.node_name, message = %req.message, "worker reported an actor crash");
        Ok(pb::ReportActorCrashedResponse { actor: Some(actor) })
    }
}

macro_rules! rpcs {
    ($($name:ident($req:ty) -> $resp:ty;)*) => {
        #[tonic::async_trait]
        impl WorkerService for WorkerServiceImpl {
            $(
                async fn $name(&self, request: Request<$req>) -> std::result::Result<Response<$resp>, Status> {
                    let cc = CallContext::of(&request);
                    WorkerServiceImpl::$name(self, &cc, request.into_inner())
                        .await
                        .map(Response::new)
                        .map_err(Status::from)
                }
            )*
        }
    };
}

rpcs! {
    acquire_worker_epoch(pb::AcquireWorkerEpochRequest) -> pb::AcquireWorkerEpochResponse;
    register_worker(pb::RegisterWorkerRequest) -> pb::RegisterWorkerResponse;
    set_worker_sandbox_runtimes(pb::SetWorkerSandboxRuntimesRequest) -> pb::SetWorkerSandboxRuntimesResponse;
    mint_worker_actor_certificate(pb::MintWorkerActorCertificateRequest) -> pb::MintWorkerActorCertificateResponse;
    mint_worker_actor_jwt(pb::MintWorkerActorJwtRequest) -> pb::MintWorkerActorJwtResponse;
    request_actor_suspend(pb::RequestActorSuspendRequest) -> pb::RequestActorSuspendResponse;
    report_actor_snapshot_state(pb::ReportActorSnapshotStateRequest) -> pb::ReportActorSnapshotStateResponse;
    report_actor_crashed(pb::ReportActorCrashedRequest) -> pb::ReportActorCrashedResponse;
}

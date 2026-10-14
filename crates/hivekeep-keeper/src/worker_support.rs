//! The WorkerSupport service: the control-plane calls a worker pod makes
//! through its node's keeper, authenticated by the pod's certificate and
//! forwarded with the keeper's own credentials.

use std::sync::Arc;

use hivekeep_core::validation::keeper as v;
use hivekeep_core::validation::rules::Operation;
use hivekeep_proto::api::v1 as api;
use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::keeper::v1::worker_support_server;
use tonic::{Code, Request, Response, Status};

use crate::context::{request_ctx, Ctx};
use crate::control::{ControlApi, WorkerServiceApi};
use crate::egress_policy::to_keeper_egress_policy;
use crate::peer_identity::{peer_leaf, PeerAuthenticator, PodIdentity};

/// The singleton name of an actor's egress policy.
const EGRESS_POLICY_NAME: &str = hivekeep_core::resource::SINGLETON_NAME;

/// The WorkerSupport service.
#[derive(Clone)]
pub struct WorkerSupportService {
    workers: Arc<dyn WorkerServiceApi>,
    control: Arc<dyn ControlApi>,
    auth: Arc<dyn PeerAuthenticator>,
    validation: Operation,
}

impl WorkerSupportService {
    /// A service forwarding to `workers` and `control`.
    pub fn new(
        workers: Arc<dyn WorkerServiceApi>,
        control: Arc<dyn ControlApi>,
        auth: Arc<dyn PeerAuthenticator>,
    ) -> Self {
        WorkerSupportService {
            workers,
            control,
            auth,
            validation: Operation::create(),
        }
    }

    /// The tonic server for this service.
    pub fn into_server(self) -> worker_support_server::WorkerSupportServer<Self> {
        worker_support_server::WorkerSupportServer::new(self)
    }

    fn identity<T>(&self, req: &Request<T>) -> Result<PodIdentity, Status> {
        self.auth.worker_identity(peer_leaf(req).as_deref())
    }
}

fn check(errs: hivekeep_core::field::ErrorList) -> Result<(), Status> {
    errs.into_result().map_err(|e| e.to_invalid_argument())
}

fn worker_ref(id: &PodIdentity) -> Option<api::ObjectRef> {
    Some(api::ObjectRef {
        hive: String::new(),
        name: id.pod_uid.clone(),
    })
}

fn actor_ref(hive: &str, name: &str) -> Option<api::ObjectRef> {
    Some(api::ObjectRef {
        hive: hive.into(),
        name: name.into(),
    })
}

/// The control plane's form of the sandbox classes a worker offers.
pub fn to_api_sandbox_classes(classes: &[pb::WorkerSandboxClass]) -> Vec<api::WorkerSandboxClass> {
    let rt = |r: &pb::WorkerSandboxRuntime| api::WorkerSandboxRuntime {
        version: r.version.as_ref().map(|v| api::VersionedSandboxCompat {
            schema_version: v.schema_version.clone(),
            attributes: v
                .attributes
                .iter()
                .map(|a| api::AttributeEntry {
                    key: a.key.clone(),
                    value: a.value.clone(),
                })
                .collect(),
        }),
        actor_overhead: r.actor_overhead.as_ref().map(|o| api::RuntimeOverhead {
            memory_bytes: o.memory_bytes,
            memory_bytes_per_gib: o.memory_bytes_per_gib,
        }),
    };
    classes
        .iter()
        .map(|c| api::WorkerSandboxClass {
            sandbox_class: c.sandbox_class.clone(),
            default_runtime: c.default_runtime.as_ref().map(rt),
            restorable_runtimes: c.restorable_runtimes.iter().map(rt).collect(),
        })
        .collect()
}

#[tonic::async_trait]
impl worker_support_server::WorkerSupport for WorkerSupportService {
    async fn mint_actor_certificate(
        &self,
        req: Request<pb::MintActorCertificateRequest>,
    ) -> Result<Response<pb::MintActorCertificateResponse>, Status> {
        let id = self.identity(&req)?;
        let ctx: Ctx = request_ctx(&req);
        let r = req.into_inner();
        check(v::validate_mint_actor_certificate_request(
            &self.validation,
            &r,
        ))?;
        let resp = self
            .workers
            .mint_worker_actor_certificate(
                &ctx,
                api::MintWorkerActorCertificateRequest {
                    worker: worker_ref(&id),
                    actor: actor_ref(&r.actor_hive, &r.actor_name),
                    actor_uid: r.actor_uid,
                    certificate_signing_request: r.certificate_signing_request,
                },
            )
            .await
            .map_err(|e| {
                Status::new(e.code(), format!("mint actor certificate: {}", e.message()))
            })?;
        Ok(Response::new(pb::MintActorCertificateResponse {
            actor_certificates: resp.actor_certificates,
        }))
    }

    async fn acquire_worker_epoch(
        &self,
        req: Request<pb::AcquireWorkerEpochRequest>,
    ) -> Result<Response<pb::AcquireWorkerEpochResponse>, Status> {
        let id = self.identity(&req)?;
        let ctx = request_ctx(&req);
        let resp = self
            .workers
            .acquire_worker_epoch(
                &ctx,
                api::AcquireWorkerEpochRequest {
                    worker: worker_ref(&id),
                },
            )
            .await?;
        Ok(Response::new(pb::AcquireWorkerEpochResponse {
            epoch: resp.epoch,
        }))
    }

    async fn register_worker(
        &self,
        req: Request<pb::RegisterWorkerRequest>,
    ) -> Result<Response<pb::RegisterWorkerResponse>, Status> {
        let id = self.identity(&req)?;
        let ctx = request_ctx(&req);
        let r = req.into_inner();
        check(v::validate_register_worker_request(&self.validation, &r))?;
        self.workers
            .register_worker(
                &ctx,
                api::RegisterWorkerRequest {
                    worker: worker_ref(&id),
                    epoch: r.epoch,
                    ips: r.ips.clone(),
                    capacity: r.capacity.as_ref().map(|c| api::WorkerResources {
                        cpu_millicores: c.cpu_millicores,
                        memory_bytes: c.memory_bytes,
                        actors: c.actors,
                    }),
                },
            )
            .await?;
        tracing::info!(pod_uid = %id.pod_uid, epoch = r.epoch, ips = ?r.ips, "Registered worker");
        Ok(Response::new(pb::RegisterWorkerResponse {}))
    }

    async fn set_worker_sandbox_runtimes(
        &self,
        req: Request<pb::SetWorkerSandboxRuntimesRequest>,
    ) -> Result<Response<pb::SetWorkerSandboxRuntimesResponse>, Status> {
        let id = self.identity(&req)?;
        let ctx = request_ctx(&req);
        let r = req.into_inner();
        check(v::validate_set_worker_sandbox_runtimes_request(
            &self.validation,
            &r,
        ))?;
        self.workers
            .set_worker_sandbox_runtimes(
                &ctx,
                api::SetWorkerSandboxRuntimesRequest {
                    worker: worker_ref(&id),
                    epoch: r.epoch,
                    sandbox_classes: to_api_sandbox_classes(&r.sandbox_classes),
                },
            )
            .await?;
        tracing::info!(pod_uid = %id.pod_uid, epoch = r.epoch, classes = r.sandbox_classes.len(), "Recorded worker sandbox runtimes");
        Ok(Response::new(pb::SetWorkerSandboxRuntimesResponse {}))
    }

    async fn request_actor_suspend(
        &self,
        req: Request<pb::RequestActorSuspendRequest>,
    ) -> Result<Response<pb::RequestActorSuspendResponse>, Status> {
        let id = self.identity(&req)?;
        let ctx = request_ctx(&req);
        let r = req.into_inner();
        check(v::validate_request_actor_suspend_request(
            &self.validation,
            &r,
        ))?;
        self.workers
            .request_actor_suspend(
                &ctx,
                api::RequestActorSuspendRequest {
                    worker: worker_ref(&id),
                    actor: actor_ref(&r.actor_hive, &r.actor_name),
                    actor_uid: r.actor_uid.clone(),
                },
            )
            .await?;
        tracing::info!(pod_uid = %id.pod_uid, actor_hive = %r.actor_hive, actor_name = %r.actor_name, actor_uid = %r.actor_uid, "Forwarded an actor suspend request");
        Ok(Response::new(pb::RequestActorSuspendResponse {}))
    }

    async fn get_actor_egress_policy(
        &self,
        req: Request<pb::GetActorEgressPolicyRequest>,
    ) -> Result<Response<pb::GetActorEgressPolicyResponse>, Status> {
        let id = self.identity(&req)?;
        let ctx = request_ctx(&req);
        let r = req.into_inner();
        check(v::validate_get_actor_egress_policy_request(
            &self.validation,
            &r,
        ))?;
        let res = self
            .control
            .get_actor_egress_policy(
                &ctx,
                api::GetActorEgressPolicyRequest {
                    actor: actor_ref(&r.actor_hive, &r.actor_name),
                    name: EGRESS_POLICY_NAME.into(),
                },
            )
            .await;
        match res {
            // An actor without a policy gets none.
            Err(e) if e.code() == Code::NotFound => {
                Ok(Response::new(pb::GetActorEgressPolicyResponse::default()))
            }
            Err(e) => Err(e),
            Ok(policy) => {
                tracing::debug!(pod_uid = %id.pod_uid, actor_hive = %r.actor_hive, actor_name = %r.actor_name, "Fetched an actor egress policy");
                Ok(Response::new(pb::GetActorEgressPolicyResponse {
                    egress_policy: Some(to_keeper_egress_policy(&policy)),
                }))
            }
        }
    }

    async fn report_actor_crashed(
        &self,
        req: Request<pb::ReportActorCrashedRequest>,
    ) -> Result<Response<pb::ReportActorCrashedResponse>, Status> {
        let id = self.identity(&req)?;
        let ctx = request_ctx(&req);
        let r = req.into_inner();
        check(v::validate_report_actor_crashed_request(
            &self.validation,
            &r,
        ))?;
        self.workers
            .report_actor_crashed(
                &ctx,
                api::ReportActorCrashedRequest {
                    worker: worker_ref(&id),
                    actor: actor_ref(&r.actor_hive, &r.actor_name),
                    actor_uid: r.actor_uid.clone(),
                    message: r.message.clone(),
                },
            )
            .await?;
        tracing::info!(pod_uid = %id.pod_uid, actor_hive = %r.actor_hive, actor_name = %r.actor_name, actor_uid = %r.actor_uid, "Forwarded an actor crash report");
        Ok(Response::new(pb::ReportActorCrashedResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker_jwt::tests::FakeWorkers;
    use async_trait::async_trait;
    use hivekeep_proto::keeper::v1::worker_support_server::WorkerSupport;

    struct FixedAuth(Option<PodIdentity>);
    impl PeerAuthenticator for FixedAuth {
        fn worker_identity(&self, _: Option<&[u8]>) -> Result<PodIdentity, Status> {
            self.0
                .clone()
                .ok_or_else(|| Status::unauthenticated("missing peer certificate"))
        }
    }

    struct Control(Option<Status>);
    #[async_trait]
    impl ControlApi for Control {
        async fn get_actor_egress_policy(
            &self,
            _: &Ctx,
            _: api::GetActorEgressPolicyRequest,
        ) -> Result<api::EgressPolicy, Status> {
            match &self.0 {
                Some(e) => Err(e.clone()),
                None => Ok(api::EgressPolicy {
                    metadata: None,
                    rules: vec![api::EgressRule::default()],
                }),
            }
        }
    }

    fn id() -> PodIdentity {
        PodIdentity {
            pod_uid: "pod-1".into(),
            ..Default::default()
        }
    }

    fn svc(
        auth: Option<PodIdentity>,
        control: Option<Status>,
    ) -> (WorkerSupportService, Arc<FakeWorkers>) {
        let w = Arc::new(FakeWorkers::default());
        (
            WorkerSupportService::new(
                w.clone(),
                Arc::new(Control(control)),
                Arc::new(FixedAuth(auth)),
            ),
            w,
        )
    }

    #[tokio::test]
    async fn forwards_with_the_callers_pod_uid() {
        let (s, w) = svc(Some(id()), None);
        let resp = s
            .acquire_worker_epoch(Request::new(pb::AcquireWorkerEpochRequest {}))
            .await
            .unwrap();
        assert_eq!(resp.into_inner().epoch, 7);
        assert_eq!(*w.calls.lock(), vec!["acquire:pod-1"]);
    }

    #[tokio::test]
    async fn refuses_unauthenticated_callers() {
        let (s, w) = svc(None, None);
        let err = s
            .acquire_worker_epoch(Request::new(pb::AcquireWorkerEpochRequest {}))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
        assert!(w.calls.lock().is_empty());
    }

    #[tokio::test]
    async fn egress_policy_not_found_is_an_empty_answer() {
        let req = || {
            Request::new(pb::GetActorEgressPolicyRequest {
                actor_hive: "demo".into(),
                actor_name: "counter".into(),
            })
        };
        let (s, _) = svc(Some(id()), Some(Status::not_found("none")));
        assert!(s
            .get_actor_egress_policy(req())
            .await
            .unwrap()
            .into_inner()
            .egress_policy
            .is_none());
        let (s, _) = svc(Some(id()), None);
        assert_eq!(
            s.get_actor_egress_policy(req())
                .await
                .unwrap()
                .into_inner()
                .egress_policy
                .unwrap()
                .rules
                .len(),
            1
        );
        let (s, _) = svc(Some(id()), Some(Status::unavailable("down")));
        assert_eq!(
            s.get_actor_egress_policy(req()).await.unwrap_err().code(),
            Code::Unavailable
        );
    }

    #[tokio::test]
    async fn validates_requests() {
        let (s, _) = svc(Some(id()), None);
        let err = s
            .report_actor_crashed(Request::new(pb::ReportActorCrashedRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[test]
    fn converts_sandbox_classes() {
        let c = pb::WorkerSandboxClass {
            sandbox_class: "gvisor".into(),
            default_runtime: Some(pb::WorkerSandboxRuntime {
                version: Some(pb::VersionedSandboxCompat {
                    schema_version: "1".into(),
                    attributes: vec![pb::AttributeEntry {
                        key: "k".into(),
                        value: "v".into(),
                    }],
                }),
                actor_overhead: Some(pb::RuntimeOverhead {
                    memory_bytes: 1,
                    memory_bytes_per_gib: 2,
                }),
            }),
            restorable_runtimes: vec![pb::WorkerSandboxRuntime::default()],
        };
        let out = to_api_sandbox_classes(&[c]);
        let d = out[0].default_runtime.as_ref().unwrap();
        assert_eq!(d.version.as_ref().unwrap().attributes[0].value, "v");
        assert_eq!(d.actor_overhead.as_ref().unwrap().memory_bytes_per_gib, 2);
        assert_eq!(out[0].restorable_runtimes.len(), 1);
    }
}

#[cfg(test)]
mod behavior_tests;

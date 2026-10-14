//! WorkerSupport: same-node admission, and each call forwarded in the
//! calling worker's name, validated first.

#![allow(clippy::type_complexity)] // test case tables

use async_trait::async_trait;
use hivekeep_proto::keeper::v1::worker_support_server::WorkerSupport;
use parking_lot::Mutex;

use super::*;
use crate::peer_identity::{check_same_node, pod_identity_from_der, DEFAULT_POD_IDENTITY_OID};
use crate::sandbox_test_support::RecordingWorkers;

const ACTOR_UID: &str = "01234567-89ab-cdef-0123-456789abcdef";

/// Admits every caller as `pod-a`, or none.
struct Auth(bool);

impl PeerAuthenticator for Auth {
    fn worker_identity(&self, _: Option<&[u8]>) -> Result<PodIdentity, Status> {
        if !self.0 {
            return Err(Status::unauthenticated("missing peer certificate"));
        }
        Ok(PodIdentity {
            namespace: "workers".into(),
            pod_name: "worker".into(),
            pod_uid: "pod-a".into(),
            node_name: "node".into(),
            node_uid: "node-uid".into(),
            ..Default::default()
        })
    }
}

/// Records egress policy reads and answers with `resp` or `err`.
#[derive(Default)]
struct Control {
    got: Mutex<Vec<api::GetActorEgressPolicyRequest>>,
    resp: Option<api::EgressPolicy>,
    err: Option<Status>,
}

#[async_trait]
impl ControlApi for Control {
    async fn get_actor_egress_policy(
        &self,
        _: &Ctx,
        req: api::GetActorEgressPolicyRequest,
    ) -> Result<api::EgressPolicy, Status> {
        self.got.lock().push(req);
        match &self.err {
            Some(e) => Err(e.clone()),
            None => Ok(self.resp.clone().unwrap_or_default()),
        }
    }
}

fn svc(
    workers: Arc<RecordingWorkers>,
    control: Arc<Control>,
    authenticated: bool,
) -> WorkerSupportService {
    WorkerSupportService::new(workers, control, Arc::new(Auth(authenticated)))
}

fn workers_svc(
    err: Option<Status>,
    authenticated: bool,
) -> (WorkerSupportService, Arc<RecordingWorkers>) {
    let w = Arc::new(RecordingWorkers {
        epoch: 7,
        ..Default::default()
    });
    *w.err.lock() = err;
    (
        svc(w.clone(), Arc::new(Control::default()), authenticated),
        w,
    )
}

fn pod_a() -> Option<api::ObjectRef> {
    Some(api::ObjectRef {
        hive: String::new(),
        name: "pod-a".into(),
    })
}

fn actor_1() -> Option<api::ObjectRef> {
    Some(api::ObjectRef {
        hive: "team-a".into(),
        name: "actor-1".into(),
    })
}

fn der(pem: &str) -> Vec<u8> {
    rustls_pemfile::certs(&mut pem.as_bytes())
        .next()
        .unwrap()
        .unwrap()
        .as_ref()
        .to_vec()
}

/// A worker is admitted on its own node only: another node, or a
/// replacement node of the same name, is refused.
#[test]
fn verifies_the_client_is_on_this_node() {
    let leaf = der(include_str!("../testdata/tls/workers_default_node_a.pem"));
    let worker = pod_identity_from_der(&leaf, DEFAULT_POD_IDENTITY_OID)
        .unwrap()
        .unwrap();
    let node_a = PodIdentity {
        node_name: worker.node_name.clone(),
        node_uid: worker.node_uid.clone(),
        ..Default::default()
    };
    check_same_node(&node_a, Some(&leaf), DEFAULT_POD_IDENTITY_OID)
        .expect("same-node worker rejected");
    let node_b = PodIdentity {
        node_name: "node-b".into(),
        ..node_a.clone()
    };
    assert!(
        check_same_node(&node_b, Some(&leaf), DEFAULT_POD_IDENTITY_OID).is_err(),
        "cross-node worker accepted"
    );
    let replacement = PodIdentity {
        node_uid: "replacement-node".into(),
        ..node_a.clone()
    };
    assert!(
        check_same_node(&replacement, Some(&leaf), DEFAULT_POD_IDENTITY_OID).is_err(),
        "replacement node accepted"
    );
    assert_eq!(
        check_same_node(&node_a, None, DEFAULT_POD_IDENTITY_OID)
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
}

#[tokio::test]
async fn acquire_worker_epoch() {
    // (name, authenticated, control plane error, want code, want epoch, forwarded)
    for (name, auth, err, want, epoch, forwarded) in [
        (
            "acquires for the calling worker",
            true,
            None,
            Code::Ok,
            7,
            true,
        ),
        (
            "requires a certificate",
            false,
            None,
            Code::Unauthenticated,
            0,
            false,
        ),
        (
            "surfaces the control plane's rejection",
            true,
            Some(Status::not_found("no such worker")),
            Code::NotFound,
            0,
            false,
        ),
    ] {
        let (s, w) = workers_svc(err, auth);
        let res = s
            .acquire_worker_epoch(Request::new(pb::AcquireWorkerEpochRequest {}))
            .await;
        let code = res.as_ref().map(|_| Code::Ok).unwrap_or_else(|e| e.code());
        assert_eq!(code, want, "{name}: {res:?}");
        assert_eq!(
            res.map(|r| r.into_inner().epoch).unwrap_or(0),
            epoch,
            "{name}"
        );
        let want_fwd = if forwarded {
            vec![api::AcquireWorkerEpochRequest { worker: pod_a() }]
        } else {
            vec![]
        };
        assert_eq!(*w.acquired.lock(), want_fwd, "{name}");
    }
}

fn register(capacity: Option<pb::WorkerResources>) -> pb::RegisterWorkerRequest {
    pb::RegisterWorkerRequest {
        epoch: 3,
        ips: vec!["10.0.0.1".into()],
        capacity,
    }
}

fn resources(actors: i32, cpu: i64, mem: i64) -> pb::WorkerResources {
    pb::WorkerResources {
        actors,
        cpu_millicores: cpu,
        memory_bytes: mem,
    }
}

#[tokio::test]
async fn register_worker() {
    let forwarded = |actors, cpu, mem| {
        vec![api::RegisterWorkerRequest {
            worker: pod_a(),
            epoch: 3,
            ips: vec!["10.0.0.1".into()],
            capacity: Some(api::WorkerResources {
                actors,
                cpu_millicores: cpu,
                memory_bytes: mem,
            }),
        }]
    };
    let cases: Vec<(
        &str,
        bool,
        Option<Status>,
        pb::RegisterWorkerRequest,
        Code,
        Vec<api::RegisterWorkerRequest>,
    )> = vec![
        (
            "records what the worker says",
            true,
            None,
            register(Some(resources(4, 2000, 4 << 30))),
            Code::Ok,
            forwarded(4, 2000, 4294967296),
        ),
        (
            "omits undetermined compute",
            true,
            None,
            register(Some(resources(1, 0, 0))),
            Code::Ok,
            forwarded(1, 0, 0),
        ),
        (
            "forwards empty capacity",
            true,
            None,
            register(Some(resources(0, 0, 0))),
            Code::Ok,
            forwarded(0, 0, 0),
        ),
        (
            "rejects invalid capacity without forwarding",
            true,
            None,
            register(Some(resources(0, -1, 0))),
            Code::InvalidArgument,
            vec![],
        ),
        (
            "rejects missing capacity without forwarding",
            true,
            None,
            register(None),
            Code::InvalidArgument,
            vec![],
        ),
        (
            "requires a certificate",
            false,
            None,
            register(Some(resources(1, 0, 0))),
            Code::Unauthenticated,
            vec![],
        ),
        (
            "surfaces the control plane's rejection",
            true,
            Some(Status::unknown("no such worker")),
            register(Some(resources(1, 0, 0))),
            Code::Unknown,
            vec![],
        ),
    ];
    for (name, auth, err, req, want, fwd) in cases {
        let (s, w) = workers_svc(err, auth);
        let res = s.register_worker(Request::new(req)).await;
        let code = res.as_ref().map(|_| Code::Ok).unwrap_or_else(|e| e.code());
        assert_eq!(code, want, "{name}: {res:?}");
        assert_eq!(*w.registered.lock(), fwd, "{name}");
    }
}

fn compat(hash: &str) -> pb::VersionedSandboxCompat {
    pb::VersionedSandboxCompat {
        schema_version: "v1".into(),
        attributes: vec![
            pb::AttributeEntry {
                key: "architecture".into(),
                value: "amd64".into(),
            },
            pb::AttributeEntry {
                key: "sandbox_bundle_hash".into(),
                value: hash.into(),
            },
        ],
    }
}

fn api_compat(hash: &str) -> api::VersionedSandboxCompat {
    api::VersionedSandboxCompat {
        schema_version: "v1".into(),
        attributes: vec![
            api::AttributeEntry {
                key: "architecture".into(),
                value: "amd64".into(),
            },
            api::AttributeEntry {
                key: "sandbox_bundle_hash".into(),
                value: hash.into(),
            },
        ],
    }
}

fn classes(restorable: &[&str]) -> Vec<pb::WorkerSandboxClass> {
    vec![pb::WorkerSandboxClass {
        sandbox_class: "gvisor".into(),
        default_runtime: Some(pb::WorkerSandboxRuntime {
            version: Some(compat("9988")),
            actor_overhead: None,
        }),
        restorable_runtimes: restorable
            .iter()
            .map(|h| pb::WorkerSandboxRuntime {
                version: Some(compat(h)),
                actor_overhead: None,
            })
            .collect(),
    }]
}

fn forwarded_runtimes(restorable: &[&str]) -> Vec<api::SetWorkerSandboxRuntimesRequest> {
    vec![api::SetWorkerSandboxRuntimesRequest {
        worker: pod_a(),
        epoch: 3,
        sandbox_classes: vec![api::WorkerSandboxClass {
            sandbox_class: "gvisor".into(),
            default_runtime: Some(api::WorkerSandboxRuntime {
                version: Some(api_compat("9988")),
                actor_overhead: None,
            }),
            restorable_runtimes: restorable
                .iter()
                .map(|h| api::WorkerSandboxRuntime {
                    version: Some(api_compat(h)),
                    actor_overhead: None,
                })
                .collect(),
        }],
    }]
}

#[tokio::test]
async fn set_worker_sandbox_runtimes() {
    let with_overhead = {
        let mut c = classes(&["d547"]);
        c[0].default_runtime.as_mut().unwrap().actor_overhead = Some(pb::RuntimeOverhead {
            memory_bytes: 4 << 20,
            memory_bytes_per_gib: 8 << 20,
        });
        c[0].restorable_runtimes[0].actor_overhead = Some(pb::RuntimeOverhead {
            memory_bytes: 16 << 20,
            memory_bytes_per_gib: 0,
        });
        c
    };
    let want_overhead = {
        let mut w = forwarded_runtimes(&["d547"]);
        let c = &mut w[0].sandbox_classes[0];
        c.default_runtime.as_mut().unwrap().actor_overhead = Some(api::RuntimeOverhead {
            memory_bytes: 4 << 20,
            memory_bytes_per_gib: 8 << 20,
        });
        c.restorable_runtimes[0].actor_overhead = Some(api::RuntimeOverhead {
            memory_bytes: 16 << 20,
            memory_bytes_per_gib: 0,
        });
        w
    };
    let two_classes = vec![
        pb::WorkerSandboxClass {
            sandbox_class: "gvisor".into(),
            default_runtime: Some(pb::WorkerSandboxRuntime {
                version: Some(compat("9988")),
                actor_overhead: None,
            }),
            restorable_runtimes: vec![],
        },
        pb::WorkerSandboxClass {
            sandbox_class: "microvm".into(),
            default_runtime: Some(pb::WorkerSandboxRuntime {
                version: Some(compat("9988")),
                actor_overhead: None,
            }),
            restorable_runtimes: vec![],
        },
    ];
    let req = |epoch, sandbox_classes| pb::SetWorkerSandboxRuntimesRequest {
        epoch,
        sandbox_classes,
    };
    let cases: Vec<(
        &str,
        bool,
        Option<Status>,
        pb::SetWorkerSandboxRuntimesRequest,
        Code,
        Vec<api::SetWorkerSandboxRuntimesRequest>,
    )> = vec![
        (
            "records what the worker says",
            true,
            None,
            req(3, classes(&[])),
            Code::Ok,
            forwarded_runtimes(&[]),
        ),
        (
            "forwards the runtimes' actor overhead",
            true,
            None,
            req(3, with_overhead),
            Code::Ok,
            want_overhead,
        ),
        (
            "forwards restorable runtimes",
            true,
            None,
            req(3, classes(&["d547"])),
            Code::Ok,
            forwarded_runtimes(&["d547"]),
        ),
        (
            "rejects missing sandbox classes without forwarding",
            true,
            None,
            req(3, vec![]),
            Code::InvalidArgument,
            vec![],
        ),
        (
            "rejects missing epoch without forwarding",
            true,
            None,
            req(0, classes(&[])),
            Code::InvalidArgument,
            vec![],
        ),
        (
            "rejects missing default runtime without forwarding",
            true,
            None,
            req(
                3,
                vec![pb::WorkerSandboxClass {
                    sandbox_class: "gvisor".into(),
                    ..Default::default()
                }],
            ),
            Code::InvalidArgument,
            vec![],
        ),
        (
            "rejects two sandbox classes without forwarding",
            true,
            None,
            req(3, two_classes),
            Code::InvalidArgument,
            vec![],
        ),
        (
            "requires a certificate",
            false,
            None,
            req(3, classes(&[])),
            Code::Unauthenticated,
            vec![],
        ),
        (
            "surfaces the control plane's refusal",
            true,
            Some(Status::failed_precondition("epoch superseded")),
            req(3, classes(&[])),
            Code::FailedPrecondition,
            vec![],
        ),
    ];
    for (name, auth, err, r, want, fwd) in cases {
        let (s, w) = workers_svc(err, auth);
        let res = s.set_worker_sandbox_runtimes(Request::new(r)).await;
        let code = res.as_ref().map(|_| Code::Ok).unwrap_or_else(|e| e.code());
        assert_eq!(code, want, "{name}: {res:?}");
        assert_eq!(*w.runtimes.lock(), fwd, "{name}");
    }
}

fn suspend_req() -> pb::RequestActorSuspendRequest {
    pb::RequestActorSuspendRequest {
        actor_hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: ACTOR_UID.into(),
    }
}

#[tokio::test]
async fn request_actor_suspend_names_the_calling_worker() {
    let (s, w) = workers_svc(None, true);
    s.request_actor_suspend(Request::new(suspend_req()))
        .await
        .unwrap();
    assert_eq!(
        *w.suspends.lock(),
        vec![api::RequestActorSuspendRequest {
            worker: pod_a(),
            actor: actor_1(),
            actor_uid: ACTOR_UID.into(),
        }]
    );
}

#[tokio::test]
async fn request_actor_suspend_requires_a_certificate() {
    let (s, w) = workers_svc(None, false);
    let err = s
        .request_actor_suspend(Request::new(suspend_req()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
    assert!(
        w.suspends.lock().is_empty(),
        "an unauthenticated request was forwarded"
    );
}

#[tokio::test]
async fn request_actor_suspend_surfaces_the_refusal() {
    let (s, _) = workers_svc(Some(Status::failed_precondition("Actor is RESUMING")), true);
    let err = s
        .request_actor_suspend(Request::new(suspend_req()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
}

fn mint_req() -> pb::MintActorCertificateRequest {
    pb::MintActorCertificateRequest {
        actor_hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: ACTOR_UID.into(),
        certificate_signing_request: b"csr-bytes".to_vec(),
    }
}

#[tokio::test]
async fn mint_actor_certificate_forwards_to_the_worker_service() {
    let w = Arc::new(RecordingWorkers {
        mint_certs: vec![b"cert-der-bytes".to_vec()],
        ..Default::default()
    });
    let s = svc(w.clone(), Arc::new(Control::default()), true);
    let resp = s
        .mint_actor_certificate(Request::new(mint_req()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        *w.minted.lock(),
        vec![api::MintWorkerActorCertificateRequest {
            worker: pod_a(),
            actor: actor_1(),
            actor_uid: ACTOR_UID.into(),
            certificate_signing_request: b"csr-bytes".to_vec(),
        }]
    );
    assert_eq!(resp.actor_certificates, vec![b"cert-der-bytes".to_vec()]);
}

#[tokio::test]
async fn mint_actor_certificate_requires_a_certificate() {
    let (s, w) = workers_svc(None, false);
    let err = s
        .mint_actor_certificate(Request::new(mint_req()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
    assert!(w.minted.lock().is_empty());
}

#[tokio::test]
async fn get_actor_egress_policy() {
    let policy = api::EgressPolicy {
        metadata: Some(api::ResourceMetadata {
            hive: "team-a".into(),
            name: "default".into(),
            ..Default::default()
        }),
        rules: vec![api::EgressRule {
            http: Some(api::HttpRule {
                hostnames: vec!["api.example.com".into()],
                ports: Some(api::Ports {
                    all: None,
                    numbers: vec![80],
                }),
                effects: None,
            }),
            ..Default::default()
        }],
    };
    let copied = pb::EgressPolicy {
        rules: vec![pb::EgressRule {
            http: Some(pb::EgressDestinations {
                hostnames: vec!["api.example.com".into()],
                ports: Some(pb::EgressPorts {
                    all: None,
                    numbers: vec![80],
                }),
            }),
            ..Default::default()
        }],
    };
    let req = || pb::GetActorEgressPolicyRequest {
        actor_hive: "team-a".into(),
        actor_name: "actor-1".into(),
    };
    let forwarded = vec![api::GetActorEgressPolicyRequest {
        actor: actor_1(),
        name: EGRESS_POLICY_NAME.into(),
    }];
    let cases: Vec<(
        &str,
        bool,
        pb::GetActorEgressPolicyRequest,
        Option<api::EgressPolicy>,
        Option<Status>,
        Code,
        bool,
        Option<pb::EgressPolicy>,
    )> = vec![
        (
            "returns the policy",
            true,
            req(),
            Some(policy),
            None,
            Code::Ok,
            true,
            Some(copied),
        ),
        (
            "leaves no policy unset",
            true,
            req(),
            None,
            Some(Status::not_found(
                "EgressPolicy for actor team-a/actor-1 not found",
            )),
            Code::Ok,
            true,
            None,
        ),
        (
            "requires a certificate",
            false,
            req(),
            None,
            None,
            Code::Unauthenticated,
            false,
            None,
        ),
        (
            "rejects an invalid request without forwarding",
            true,
            pb::GetActorEgressPolicyRequest {
                actor_hive: "team-a".into(),
                actor_name: String::new(),
            },
            None,
            None,
            Code::InvalidArgument,
            false,
            None,
        ),
        (
            "surfaces the control plane's failure",
            true,
            req(),
            None,
            Some(Status::unavailable("control plane unavailable")),
            Code::Unavailable,
            true,
            None,
        ),
    ];
    for (name, auth, r, resp, err, want, fwd, want_policy) in cases {
        let control = Arc::new(Control {
            resp,
            err,
            ..Default::default()
        });
        let s = svc(Arc::new(RecordingWorkers::default()), control.clone(), auth);
        let res = s.get_actor_egress_policy(Request::new(r)).await;
        let code = res.as_ref().map(|_| Code::Ok).unwrap_or_else(|e| e.code());
        assert_eq!(code, want, "{name}: {res:?}");
        assert_eq!(
            *control.got.lock(),
            if fwd { forwarded.clone() } else { vec![] },
            "{name}"
        );
        if let Ok(r) = res {
            assert_eq!(r.into_inner().egress_policy, want_policy, "{name}");
        }
    }
}

fn crash_req() -> pb::ReportActorCrashedRequest {
    pb::ReportActorCrashedRequest {
        actor_hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: ACTOR_UID.into(),
        message: "cloud-hypervisor exited".into(),
    }
}

#[tokio::test]
async fn report_actor_crashed_names_the_calling_worker() {
    let (s, w) = workers_svc(None, true);
    s.report_actor_crashed(Request::new(crash_req()))
        .await
        .unwrap();
    assert_eq!(
        *w.crashes.lock(),
        vec![api::ReportActorCrashedRequest {
            worker: pod_a(),
            actor: actor_1(),
            actor_uid: ACTOR_UID.into(),
            message: "cloud-hypervisor exited".into(),
        }]
    );
}

#[tokio::test]
async fn report_actor_crashed_requires_a_certificate() {
    let (s, w) = workers_svc(None, false);
    let err = s
        .report_actor_crashed(Request::new(crash_req()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
    assert!(w.crashes.lock().is_empty());
}

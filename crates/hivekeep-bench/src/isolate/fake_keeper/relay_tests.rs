use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_proto::api::v1 as api;
use hivekeep_proto::api::v1::worker_service_client::WorkerServiceClient;
use hivekeep_proto::api::v1::worker_service_server::WorkerService;
use parking_lot::Mutex;
use tonic::{Code, Request, Status};

use super::*;
use crate::isolate::fake_keeper::epochs::WorkerEpochs;
use crate::tls::client::{client_config, lazy_channel, PeerCheck};
use crate::tls::testca::{rotate, TestCa};
use crate::tls::testing::serve_worker_service;

const KEEPER_ID: &str = "spiffe://cluster.local/ns/hivekeep-system/sa/keeper";
const ALLOWED: &str = "spiffe://cluster.local/ns/hivekeep-system/sa/fake-workersync";

/// Records the last request of each kind and answers with `err`.
#[derive(Default)]
struct RecordingWorkerService {
    got: Mutex<Option<api::RegisterWorkerRequest>>,
    acquire_got: Mutex<Option<api::AcquireWorkerEpochRequest>>,
    runtimes_got: Mutex<Option<api::SetWorkerSandboxRuntimesRequest>>,
    err: Option<Status>,
}

impl RecordingWorkerService {
    fn failing(code: Code, msg: &str) -> Self {
        RecordingWorkerService {
            err: Some(Status::new(code, msg)),
            ..Default::default()
        }
    }
    fn fail(&self) -> Result<(), Status> {
        match &self.err {
            Some(e) => Err(Status::new(e.code(), e.message())),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl CapacityClient for RecordingWorkerService {
    async fn acquire_worker_epoch(
        &self,
        req: api::AcquireWorkerEpochRequest,
    ) -> Result<api::AcquireWorkerEpochResponse, Status> {
        *self.acquire_got.lock() = Some(req);
        self.fail()?;
        Ok(api::AcquireWorkerEpochResponse { epoch: 4 })
    }

    async fn register_worker(
        &self,
        req: api::RegisterWorkerRequest,
    ) -> Result<api::RegisterWorkerResponse, Status> {
        *self.got.lock() = Some(req);
        self.fail()?;
        Ok(api::RegisterWorkerResponse::default())
    }

    async fn set_worker_sandbox_runtimes(
        &self,
        req: api::SetWorkerSandboxRuntimesRequest,
    ) -> Result<api::SetWorkerSandboxRuntimesResponse, Status> {
        *self.runtimes_got.lock() = Some(req);
        self.fail()?;
        Ok(api::SetWorkerSandboxRuntimesResponse::default())
    }
}

fn worker_ref() -> Option<api::ObjectRef> {
    Some(api::ObjectRef {
        hive: String::new(),
        name: "fake-r1-c52b9bd3-0".into(),
    })
}

fn runtimes_report() -> api::SetWorkerSandboxRuntimesRequest {
    api::SetWorkerSandboxRuntimesRequest {
        worker: worker_ref(),
        epoch: 4,
        sandbox_classes: vec![api::WorkerSandboxClass {
            sandbox_class: "gvisor".into(),
            default_runtime: Some(api::WorkerSandboxRuntime {
                version: Some(api::VersionedSandboxCompat {
                    schema_version: "v1".into(),
                    attributes: vec![api::AttributeEntry {
                        key: "architecture".into(),
                        value: "amd64".into(),
                    }],
                }),
                ..Default::default()
            }),
            ..Default::default()
        }],
    }
}

fn report() -> api::RegisterWorkerRequest {
    api::RegisterWorkerRequest {
        worker: worker_ref(),
        capacity: Some(api::WorkerResources {
            actors: 1000,
            cpu_millicores: 64000,
            memory_bytes: 256 << 30,
        }),
        ..Default::default()
    }
}

fn relay(svc: Arc<RecordingWorkerService>) -> Relay {
    Relay::new(svc, Arc::new(WorkerEpochs::new()))
}

#[tokio::test]
async fn forwards_the_acquire_unchanged() {
    let svc = Arc::new(RecordingWorkerService::default());
    let req = api::AcquireWorkerEpochRequest {
        worker: worker_ref(),
    };
    let resp = relay(svc.clone())
        .acquire_worker_epoch(Request::new(req.clone()))
        .await
        .unwrap();
    assert_eq!(*svc.acquire_got.lock(), Some(req));
    assert_eq!(resp.into_inner().epoch, 4);
}

#[tokio::test]
async fn forwards_the_runtimes_unchanged() {
    let svc = Arc::new(RecordingWorkerService::default());
    relay(svc.clone())
        .set_worker_sandbox_runtimes(Request::new(runtimes_report()))
        .await
        .unwrap();
    assert_eq!(*svc.runtimes_got.lock(), Some(runtimes_report()));
}

#[tokio::test]
async fn forwards_the_report_unchanged() {
    let svc = Arc::new(RecordingWorkerService::default());
    relay(svc.clone())
        .register_worker(Request::new(report()))
        .await
        .unwrap();
    assert_eq!(*svc.got.lock(), Some(report()));
}

// The herder reports the epoch a Worker registered with, so the relay
// records it, and only once hivekeep-api has accepted the registration.
#[tokio::test]
async fn records_the_registered_epoch() {
    let mut req = report();
    req.epoch = 4;
    let refused = Arc::new(WorkerEpochs::new());
    let svc = Arc::new(RecordingWorkerService::failing(
        Code::FailedPrecondition,
        "epoch superseded",
    ));
    assert!(Relay::new(svc, refused.clone())
        .register_worker(Request::new(req.clone()))
        .await
        .is_err());
    assert_eq!(refused.get("fake-r1-c52b9bd3-0"), 0);

    let accepted = Arc::new(WorkerEpochs::new());
    Relay::new(
        Arc::new(RecordingWorkerService::default()),
        accepted.clone(),
    )
    .register_worker(Request::new(req))
    .await
    .unwrap();
    assert_eq!(accepted.get("fake-r1-c52b9bd3-0"), 4);
}

// fake-workersync retries a Worker hivekeep-api has not seen yet and gives
// up on a rejected report, so the relay must keep hivekeep-api's status.
#[tokio::test]
async fn passes_through_the_api_status() {
    for code in [
        Code::NotFound,
        Code::InvalidArgument,
        Code::PermissionDenied,
        Code::Unavailable,
        Code::Internal,
    ] {
        let r = relay(Arc::new(RecordingWorkerService::failing(
            code,
            "from hivekeep-api",
        )));
        let errs = [
            (
                "AcquireWorkerEpoch",
                r.acquire_worker_epoch(Request::new(Default::default()))
                    .await
                    .err(),
            ),
            (
                "RegisterWorker",
                r.register_worker(Request::new(report())).await.err(),
            ),
            (
                "SetWorkerSandboxRuntimes",
                r.set_worker_sandbox_runtimes(Request::new(runtimes_report()))
                    .await
                    .err(),
            ),
        ];
        for (name, err) in errs {
            let err = err.unwrap_or_else(|| panic!("{name} {code:?} succeeded"));
            assert_eq!(err.code(), code, "{name}");
            assert!(err.message().contains("from hivekeep-api"), "{name}: {err}");
        }
    }
}

// The relay calls hivekeep-api under the keeper's identity, so it must
// forward nothing but capacity reports.
#[tokio::test]
async fn serves_only_capacity_reports() {
    let err = relay(Arc::new(RecordingWorkerService::default()))
        .mint_worker_actor_certificate(Request::new(Default::default()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);
}

async fn call(
    addr: std::net::SocketAddr,
    client_bundle: &std::path::Path,
    trust: &std::path::Path,
) -> Result<(), Status> {
    let cfg = client_config(
        client_bundle.to_str().unwrap(),
        trust.to_str().unwrap(),
        PeerCheck::SpiffeId(KEEPER_ID.into()),
    )
    .unwrap();
    let channel = lazy_channel(&addr.to_string(), cfg).unwrap();
    WorkerServiceClient::new(channel)
        .register_worker(report())
        .await
        .map(|_| ())
}

// The relay admits fake-workersync and no one else: any other workload with
// a pod certificate could otherwise set any Worker's capacity.
#[tokio::test]
async fn tls_admits_only_the_allowed_id() {
    let ca = TestCa::new();
    let tls = relay_tls(
        ca.issue(KEEPER_ID).to_str().unwrap(),
        ca.trust_bundle.to_str().unwrap(),
        ALLOWED,
    )
    .unwrap();
    let addr = serve_worker_service(relay(Arc::new(RecordingWorkerService::default())), tls).await;
    call(addr, &ca.issue(ALLOWED), &ca.trust_bundle)
        .await
        .expect("fake-workersync refused");
    assert!(call(
        addr,
        &ca.issue("spiffe://cluster.local/ns/benchmarking/sa/default"),
        &ca.trust_bundle
    )
    .await
    .is_err());
    let other = TestCa::new();
    assert!(call(addr, &other.issue(ALLOWED), &ca.trust_bundle)
        .await
        .is_err());
}

// A pod-identity CA rotation during a long run must not cut fake-workersync
// off: the relay reads the trust bundle at each handshake.
#[tokio::test]
async fn tls_follows_a_trust_bundle_rotation() {
    let ca = TestCa::new();
    // The client keeps trusting the original CA, which signed the relay.
    let client_trust = ca.trust_bundle.with_file_name("client-trust.pem");
    std::fs::copy(&ca.trust_bundle, &client_trust).unwrap();
    let tls = relay_tls(
        ca.issue(KEEPER_ID).to_str().unwrap(),
        ca.trust_bundle.to_str().unwrap(),
        ALLOWED,
    )
    .unwrap();
    let addr = serve_worker_service(relay(Arc::new(RecordingWorkerService::default())), tls).await;
    let next = TestCa::new();
    rotate(&ca.trust_bundle, &[&next]);
    call(addr, &next.issue(ALLOWED), &client_trust)
        .await
        .expect("a certificate from the rotated-in CA was refused");
}

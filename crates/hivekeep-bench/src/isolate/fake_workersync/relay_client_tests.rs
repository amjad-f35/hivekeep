use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_proto::api::v1 as api;
use hivekeep_proto::api::v1::worker_service_server::WorkerService;
use parking_lot::Mutex;
use tonic::{Code, Request, Response, Status};

use super::*;
use crate::tls::server::ServerTls;
use crate::tls::testca::{rotate, TestCa};
use crate::tls::testing::serve_worker_service;

const KEEPER_ID: &str = "spiffe://cluster.local/ns/hivekeep-system/sa/keeper";
const WORKER_SYNC_ID: &str = "spiffe://cluster.local/ns/hivekeep-system/sa/fake-workersync";
/// The epoch the stub relay hands out.
const STUB_EPOCH: i64 = 9;

#[derive(Default)]
struct Recorded {
    acquire: Vec<api::AcquireWorkerEpochRequest>,
    register: Vec<api::RegisterWorkerRequest>,
    runtimes: Vec<api::SetWorkerSandboxRuntimesRequest>,
}

/// A stand-in fake-keeper relay: records each request and answers with
/// `err`.
#[derive(Clone, Default)]
struct StubRelay {
    got: Arc<Mutex<Recorded>>,
    err: Option<(Code, String)>,
}

impl StubRelay {
    fn fail(&self) -> Result<(), Status> {
        match &self.err {
            Some((c, m)) => Err(Status::new(*c, m.clone())),
            None => Ok(()),
        }
    }
}

fn unimplemented<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented("stub"))
}

#[async_trait]
impl WorkerService for StubRelay {
    async fn acquire_worker_epoch(
        &self,
        r: Request<api::AcquireWorkerEpochRequest>,
    ) -> Result<Response<api::AcquireWorkerEpochResponse>, Status> {
        self.got.lock().acquire.push(r.into_inner());
        self.fail()?;
        Ok(Response::new(api::AcquireWorkerEpochResponse {
            epoch: STUB_EPOCH,
        }))
    }
    async fn register_worker(
        &self,
        r: Request<api::RegisterWorkerRequest>,
    ) -> Result<Response<api::RegisterWorkerResponse>, Status> {
        self.got.lock().register.push(r.into_inner());
        self.fail()?;
        Ok(Response::new(Default::default()))
    }
    async fn set_worker_sandbox_runtimes(
        &self,
        r: Request<api::SetWorkerSandboxRuntimesRequest>,
    ) -> Result<Response<api::SetWorkerSandboxRuntimesResponse>, Status> {
        self.got.lock().runtimes.push(r.into_inner());
        self.fail()?;
        Ok(Response::new(Default::default()))
    }
    async fn mint_worker_actor_certificate(
        &self,
        _r: Request<api::MintWorkerActorCertificateRequest>,
    ) -> Result<Response<api::MintWorkerActorCertificateResponse>, Status> {
        unimplemented()
    }
    async fn mint_worker_actor_jwt(
        &self,
        _r: Request<api::MintWorkerActorJwtRequest>,
    ) -> Result<Response<api::MintWorkerActorJwtResponse>, Status> {
        unimplemented()
    }
    async fn request_actor_suspend(
        &self,
        _r: Request<api::RequestActorSuspendRequest>,
    ) -> Result<Response<api::RequestActorSuspendResponse>, Status> {
        unimplemented()
    }
    async fn report_actor_snapshot_state(
        &self,
        _r: Request<api::ReportActorSnapshotStateRequest>,
    ) -> Result<Response<api::ReportActorSnapshotStateResponse>, Status> {
        unimplemented()
    }
    async fn report_actor_crashed(
        &self,
        _r: Request<api::ReportActorCrashedRequest>,
    ) -> Result<Response<api::ReportActorCrashedResponse>, Status> {
        unimplemented()
    }
}

/// Serves `svc` with a certificate for `server_id` from `ca`, requiring
/// fake-workersync's certificate chaining to `client_ca`.
async fn relay_server_trusting(
    ca: &TestCa,
    client_ca: &TestCa,
    server_id: &str,
    svc: StubRelay,
) -> SocketAddr {
    // The server reads a private copy of the client CA, so rotating the
    // client's trust bundle leaves it alone.
    let server_trust = client_ca
        .trust_bundle
        .with_file_name(format!("server-trust-{}.pem", rand::random::<u64>()));
    std::fs::copy(&client_ca.trust_bundle, &server_trust).unwrap();
    let tls = ServerTls::new(
        ca.issue(server_id).to_str().unwrap(),
        server_trust.to_str().unwrap(),
        Some(WORKER_SYNC_ID),
    )
    .unwrap();
    serve_worker_service(svc, tls).await
}

async fn relay_server(ca: &TestCa, server_id: &str, svc: StubRelay) -> SocketAddr {
    relay_server_trusting(ca, ca, server_id, svc).await
}

fn test_relay(ca: &TestCa) -> GrpcRelay {
    GrpcRelay::new(
        ca.issue(WORKER_SYNC_ID).to_str().unwrap(),
        ca.trust_bundle.to_str().unwrap(),
        KEEPER_ID,
    )
    .unwrap()
}

fn test_classes() -> Vec<api::WorkerSandboxClass> {
    vec![api::WorkerSandboxClass {
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
    }]
}

fn capacity_report() -> WorkerReport {
    WorkerReport {
        name: "fake-r1-c52b9bd3-0".into(),
        epoch: 0,
        ips: vec!["10.0.0.7".into()],
        capacity: Some(api::WorkerResources {
            actors: 1000,
            ..Default::default()
        }),
        sandbox_classes: test_classes(),
    }
}

fn worker_ref() -> Option<api::ObjectRef> {
    Some(api::ObjectRef {
        hive: String::new(),
        name: "fake-r1-c52b9bd3-0".into(),
    })
}

// A Worker with no epoch acquires one, registers with it, then reports its
// runtimes under it, as a worker process does at startup.
#[tokio::test]
async fn sends_the_report() {
    let ca = TestCa::new();
    let svc = StubRelay::default();
    let addr = relay_server(&ca, KEEPER_ID, svc.clone()).await;
    let relay = test_relay(&ca);
    for _ in 0..2 {
        relay
            .report(&addr.to_string(), capacity_report())
            .await
            .unwrap();
    }
    let got = svc.got.lock();
    assert_eq!(
        got.acquire,
        vec![
            api::AcquireWorkerEpochRequest {
                worker: worker_ref()
            };
            2
        ]
    );
    let want_register = api::RegisterWorkerRequest {
        worker: worker_ref(),
        epoch: STUB_EPOCH,
        ips: vec!["10.0.0.7".into()],
        capacity: capacity_report().capacity,
    };
    assert_eq!(got.register, vec![want_register; 2]);
    let want_runtimes = api::SetWorkerSandboxRuntimesRequest {
        worker: worker_ref(),
        epoch: STUB_EPOCH,
        sandbox_classes: test_classes(),
    };
    assert_eq!(got.runtimes, vec![want_runtimes; 2]);
    assert_eq!(relay.conns.lock().len(), 1);
}

// A Worker that already has a registered epoch registers it again without
// acquiring a new one.
#[tokio::test]
async fn reuses_the_given_epoch() {
    let ca = TestCa::new();
    let svc = StubRelay::default();
    let addr = relay_server(&ca, KEEPER_ID, svc.clone()).await;
    let mut report = capacity_report();
    report.epoch = 3;
    test_relay(&ca)
        .report(&addr.to_string(), report)
        .await
        .unwrap();
    let got = svc.got.lock();
    assert!(got.acquire.is_empty());
    assert_eq!(
        got.register.iter().map(|r| r.epoch).collect::<Vec<_>>(),
        vec![3]
    );
    assert_eq!(
        got.runtimes.iter().map(|r| r.epoch).collect::<Vec<_>>(),
        vec![3]
    );
}

// fake-workersync reports only to the keeper identity.
#[tokio::test]
async fn refuses_a_server_that_is_not_the_keeper() {
    let ca = TestCa::new();
    let relay = test_relay(&ca);
    let impostor = StubRelay::default();
    let addr = relay_server(
        &ca,
        "spiffe://cluster.local/ns/benchmarking/sa/default",
        impostor.clone(),
    )
    .await;
    assert!(relay
        .report(&addr.to_string(), capacity_report())
        .await
        .is_err());
    let other = TestCa::new();
    let untrusted = StubRelay::default();
    let addr = relay_server(&other, KEEPER_ID, untrusted.clone()).await;
    assert!(relay
        .report(&addr.to_string(), capacity_report())
        .await
        .is_err());
    assert!(impostor.got.lock().register.is_empty() && untrusted.got.lock().register.is_empty());
}

// A pod-identity CA rotation during a long run must not cut the relays off:
// the client reads the trust bundle at each handshake.
#[tokio::test]
async fn follows_a_trust_bundle_rotation() {
    let ca = TestCa::new();
    let relay = test_relay(&ca);
    let next = TestCa::new();
    // The relay is certified by the new CA and still admits
    // fake-workersync's certificate from the old one.
    let addr = relay_server_trusting(&next, &ca, KEEPER_ID, StubRelay::default()).await;
    rotate(&ca.trust_bundle, &[&next]);
    relay
        .report(&addr.to_string(), capacity_report())
        .await
        .expect("report to a relay certified by the rotated-in CA");
}

// A relay gone from the pod list has its connection closed; the next report
// to it dials afresh.
#[tokio::test]
async fn prunes_gone_relays() {
    let ca = TestCa::new();
    let kept = relay_server(&ca, KEEPER_ID, StubRelay::default())
        .await
        .to_string();
    let gone = relay_server(&ca, KEEPER_ID, StubRelay::default())
        .await
        .to_string();
    let relay = test_relay(&ca);
    for addr in [&kept, &gone] {
        relay.report(addr, capacity_report()).await.unwrap();
    }
    relay.prune(&HashMap::from([("node-a".to_string(), kept.clone())]));
    {
        let conns = relay.conns.lock();
        assert!(
            !conns.contains_key(&gone) && conns.len() == 1,
            "{:?}",
            conns.keys().collect::<Vec<_>>()
        );
    }
    relay
        .report(&gone, capacity_report())
        .await
        .expect("report after pruning");
}

// The controller tells a Worker hivekeep-api has not seen yet from a
// rejected report by the status code, so the relay's must arrive intact.
#[tokio::test]
async fn carries_the_api_status() {
    let ca = TestCa::new();
    let svc = StubRelay {
        err: Some((Code::NotFound, "Worker fake-r1-c52b9bd3-0 not found".into())),
        ..Default::default()
    };
    let addr = relay_server(&ca, KEEPER_ID, svc).await;
    let err = test_relay(&ca)
        .report(&addr.to_string(), capacity_report())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    assert!(err.message().contains("not found"), "{err}");
}

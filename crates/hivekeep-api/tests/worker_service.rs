//! WorkerService: keeper authentication, epochs, runtimes, credentials and
//! reports.

mod common;

use std::sync::Arc;
use std::time::SystemTime;

use common::*;
use hivekeep_api::context::{request_as, Principal};
use hivekeep_api::keeper_auth::testing as certs;
use hivekeep_identity::jwt::verifier::{SignerKeySource, Verifier};
use hivekeep_proto::api::v1 as pb;
use tonic::Code;

async fn plain_worker(h: &Harness, name: &str, node: &str) {
    h.c()
        .create_worker(h.req(pb::CreateWorkerRequest {
            worker: Some(pb::Worker {
                metadata: Some(pb::ResourceMetadata {
                    name: name.into(),
                    ..Default::default()
                }),
                worker_namespace: "workers".into(),
                worker_pool: "pool1".into(),
                worker_pod: name.into(),
                worker_pod_uid: uuid::Uuid::new_v4().to_string(),
                node_name: node.into(),
                ..Default::default()
            }),
        }))
        .await
        .unwrap();
}

fn acquire(name: &str) -> pb::AcquireWorkerEpochRequest {
    pb::AcquireWorkerEpochRequest {
        worker: Some(wref(name)),
    }
}

fn register(name: &str, epoch: i64, slots: i32) -> pb::RegisterWorkerRequest {
    pb::RegisterWorkerRequest {
        worker: Some(wref(name)),
        epoch,
        ips: vec!["10.0.0.1".into()],
        capacity: Some(pb::WorkerResources {
            cpu_millicores: 1000,
            memory_bytes: 1 << 30,
            actors: slots,
        }),
    }
}

#[tokio::test]
async fn acquire_worker_epoch() {
    let h = Harness::new().await;
    plain_worker(&h, "w1", NODE).await;
    let e1 = h
        .ws()
        .acquire_worker_epoch(h.keeper_req(NODE, acquire("w1")))
        .await
        .unwrap()
        .into_inner()
        .epoch;
    let e2 = h
        .ws()
        .acquire_worker_epoch(h.keeper_req(NODE, acquire("w1")))
        .await
        .unwrap()
        .into_inner()
        .epoch;
    assert_eq!((e1, e2), (1, 2));
    let w = h.store.get_worker("w1").await.unwrap();
    assert_eq!(w.status.as_ref().unwrap().issued_epoch, 2);
    assert_eq!(w.status.unwrap().registered_epoch, 0);
}

#[tokio::test]
async fn acquire_worker_epoch_errors() {
    let h = Harness::new().await;
    plain_worker(&h, "w1", NODE).await;
    assert_code(
        h.ws()
            .acquire_worker_epoch(h.keeper_req("node2", acquire("w1")))
            .await,
        Code::NotFound,
        "Worker w1 not found",
    );
    assert_code(
        h.ws()
            .acquire_worker_epoch(h.keeper_req(NODE, acquire("nope")))
            .await,
        Code::NotFound,
        "",
    );
    // Not a keeper.
    let other = certs::pod_cert(&h.pod_ca, "default", "app", NODE);
    let r = request_as(
        acquire("w1"),
        Some(Principal::mtls("spiffe://x")),
        Some(other),
    );
    assert_code(
        h.ws().acquire_worker_epoch(r).await,
        Code::PermissionDenied,
        "not permitted",
    );
    // No certificate at all.
    assert_code(
        h.ws().acquire_worker_epoch(h.req(acquire("w1"))).await,
        Code::Unauthenticated,
        "",
    );
}

#[tokio::test]
async fn register_worker_epochs() {
    let h = Harness::new().await;
    plain_worker(&h, "w1", NODE).await;
    assert_code(
        h.ws()
            .register_worker(h.keeper_req(NODE, register("w1", 1, 2)))
            .await,
        Code::FailedPrecondition,
        "never handed out",
    );
    let e = h
        .ws()
        .acquire_worker_epoch(h.keeper_req(NODE, acquire("w1")))
        .await
        .unwrap()
        .into_inner()
        .epoch;
    let w = h
        .ws()
        .register_worker(h.keeper_req(NODE, register("w1", e, 2)))
        .await
        .unwrap()
        .into_inner()
        .worker
        .unwrap();
    let st = w.status.as_ref().unwrap();
    assert_eq!(st.registered_epoch, e);
    assert_eq!(st.ips, vec!["10.0.0.1"]);
    assert_eq!(st.capacity.unwrap().actors, 2);
    // An identical retry is accepted without a write.
    let again = h
        .ws()
        .register_worker(h.keeper_req(NODE, register("w1", e, 2)))
        .await
        .unwrap()
        .into_inner()
        .worker
        .unwrap();
    assert_eq!(
        again.metadata.unwrap().version,
        w.metadata.as_ref().unwrap().version
    );
    // A different registration of the same epoch is refused.
    assert_code(
        h.ws()
            .register_worker(h.keeper_req(NODE, register("w1", e, 3)))
            .await,
        Code::FailedPrecondition,
        "already registered",
    );
    // A newer epoch supersedes; the older one is then refused.
    let e2 = h
        .ws()
        .acquire_worker_epoch(h.keeper_req(NODE, acquire("w1")))
        .await
        .unwrap()
        .into_inner()
        .epoch;
    h.ws()
        .register_worker(h.keeper_req(NODE, register("w1", e2, 3)))
        .await
        .unwrap();
    assert_code(
        h.ws()
            .register_worker(h.keeper_req(NODE, register("w1", e, 2)))
            .await,
        Code::FailedPrecondition,
        "superseded",
    );
    // Another node's keeper learns nothing.
    assert_code(
        h.ws()
            .register_worker(h.keeper_req("node2", register("w1", e2, 3)))
            .await,
        Code::NotFound,
        "",
    );
    // Nonsense is refused by validation.
    assert_code(
        h.ws()
            .register_worker(h.keeper_req(NODE, register("w1", 0, 3)))
            .await,
        Code::InvalidArgument,
        "",
    );
}

#[tokio::test]
async fn register_clears_previous_runtimes() {
    let h = Harness::new().await;
    h.create_worker("w1", NODE, "pool1", 2).await;
    let e = h
        .ws()
        .acquire_worker_epoch(h.keeper_req(NODE, acquire("w1")))
        .await
        .unwrap()
        .into_inner()
        .epoch;
    let w = h
        .ws()
        .register_worker(h.keeper_req(NODE, register("w1", e, 2)))
        .await
        .unwrap()
        .into_inner()
        .worker
        .unwrap();
    assert!(w.status.unwrap().sandbox_classes.is_empty());
}

#[tokio::test]
async fn set_worker_sandbox_runtimes() {
    let h = Harness::new().await;
    plain_worker(&h, "w1", NODE).await;
    let e = h
        .ws()
        .acquire_worker_epoch(h.keeper_req(NODE, acquire("w1")))
        .await
        .unwrap()
        .into_inner()
        .epoch;
    h.ws()
        .register_worker(h.keeper_req(NODE, register("w1", e, 2)))
        .await
        .unwrap();
    let req = |epoch, class: pb::WorkerSandboxClass| pb::SetWorkerSandboxRuntimesRequest {
        worker: Some(wref("w1")),
        epoch,
        sandbox_classes: vec![class],
    };
    assert_code(
        h.ws()
            .set_worker_sandbox_runtimes(h.keeper_req(NODE, req(e + 1, gvisor_class("v1", "a"))))
            .await,
        Code::FailedPrecondition,
        "not the registered epoch",
    );
    // Attributes are sorted before they are stored.
    let mut class = gvisor_class("v1", "a");
    let v = class
        .default_runtime
        .as_mut()
        .unwrap()
        .version
        .as_mut()
        .unwrap();
    v.attributes.insert(
        0,
        pb::AttributeEntry {
            key: "z".into(),
            value: "1".into(),
        },
    );
    v.attributes.push(pb::AttributeEntry {
        key: "architecture".into(),
        value: "arm64".into(),
    });
    let w = h
        .ws()
        .set_worker_sandbox_runtimes(h.keeper_req(NODE, req(e, class.clone())))
        .await
        .unwrap()
        .into_inner()
        .worker
        .unwrap();
    let keys: Vec<String> = w.status.as_ref().unwrap().sandbox_classes[0]
        .default_runtime
        .as_ref()
        .unwrap()
        .version
        .as_ref()
        .unwrap()
        .attributes
        .iter()
        .map(|a| a.key.clone())
        .collect();
    assert_eq!(keys, vec!["architecture", "sandbox_bundle_hash", "z"]);
    // The same runtimes again write nothing.
    let again = h
        .ws()
        .set_worker_sandbox_runtimes(h.keeper_req(NODE, req(e, class)))
        .await
        .unwrap()
        .into_inner()
        .worker
        .unwrap();
    assert_eq!(
        again.metadata.unwrap().version,
        w.metadata.as_ref().unwrap().version
    );
    // Another class contradicts the recorded one.
    let mut micro = gvisor_class("v1", "a");
    micro.sandbox_class = "microvm".into();
    assert_code(
        h.ws()
            .set_worker_sandbox_runtimes(h.keeper_req(NODE, req(e, micro)))
            .await,
        Code::FailedPrecondition,
        "does not match",
    );
}

#[tokio::test]
async fn mint_worker_actor_jwt() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    let req = |uid: &str| pb::MintWorkerActorJwtRequest {
        actor: Some(oref(HIVE, "a1")),
        actor_uid: uid.into(),
        audiences: vec!["storage".into()],
    };
    let resp = h
        .ws()
        .mint_worker_actor_jwt(h.keeper_req(NODE, req(&uid)))
        .await
        .unwrap()
        .into_inner();
    let v = Verifier::new(
        ISSUER,
        vec!["storage".into()],
        Arc::new(SignerKeySource(h.jwt.clone())),
    )
    .with_actor_claims();
    let t = v.verify(&resp.actor_jwt, SystemTime::now()).unwrap();
    let (kind, claims) = t.actor.unwrap();
    assert_eq!(
        kind,
        hivekeep_identity::jwt::claims::TokenKind::WorkerForActor
    );
    assert_eq!(claims.node, NODE);
    assert_eq!(
        t.expiration.duration_since(t.issued_at).unwrap().as_secs(),
        3600
    );
    // Other nodes and stale incarnations are refused.
    assert_code(
        h.ws()
            .mint_worker_actor_jwt(h.keeper_req("node2", req(&uid)))
            .await,
        Code::NotFound,
        "actor not found",
    );
    assert_code(
        h.ws()
            .mint_worker_actor_jwt(h.keeper_req(NODE, req(&uuid::Uuid::new_v4().to_string())))
            .await,
        Code::Aborted,
        "recreated",
    );
    // A SUSPENDED actor with a local snapshot may still mint on its node.
    h.suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    h.ws()
        .mint_worker_actor_jwt(h.keeper_req(NODE, req(&uid)))
        .await
        .unwrap();
}

#[tokio::test]
async fn mint_worker_actor_certificate() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    let key = rcgen::KeyPair::generate().unwrap();
    let csr = rcgen::CertificateParams::new(vec![])
        .unwrap()
        .serialize_request(&key)
        .unwrap()
        .der()
        .to_vec();
    let req = |worker: &str, uid: &str, csr: Vec<u8>| pb::MintWorkerActorCertificateRequest {
        actor: Some(oref(HIVE, "a1")),
        actor_uid: uid.into(),
        certificate_signing_request: csr,
        worker: Some(wref(worker)),
    };
    let certs = h
        .ws()
        .mint_worker_actor_certificate(h.keeper_req(NODE, req("w1", &uid, csr.clone())))
        .await
        .unwrap()
        .into_inner()
        .actor_certificates;
    assert_eq!(
        hivekeep_api::peer_cert::uri_sans(&certs[0]),
        vec![format!(
            "spiffe://hivekeep-actor.local/worker-for-actor/{HIVE}/a1"
        )]
    );
    assert_code(
        h.ws()
            .mint_worker_actor_certificate(h.keeper_req(NODE, req("w2", &uid, csr.clone())))
            .await,
        Code::NotFound,
        "",
    );
    assert_code(
        h.ws()
            .mint_worker_actor_certificate(h.keeper_req("node2", req("w1", &uid, csr.clone())))
            .await,
        Code::NotFound,
        "",
    );
    assert_code(
        h.ws()
            .mint_worker_actor_certificate(h.keeper_req(
                NODE,
                req("w1", &uuid::Uuid::new_v4().to_string(), csr.clone()),
            ))
            .await,
        Code::Aborted,
        "",
    );
    let mut bad = csr.clone();
    let n = bad.len();
    bad[n - 3] ^= 0xff;
    assert_code(
        h.ws()
            .mint_worker_actor_certificate(h.keeper_req(NODE, req("w1", &uid, bad)))
            .await,
        Code::InvalidArgument,
        "",
    );
    // After the worker is released, the node no longer hosts the actor.
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    assert_code(
        h.ws()
            .mint_worker_actor_certificate(h.keeper_req(NODE, req("w1", &uid, csr)))
            .await,
        Code::NotFound,
        "",
    );
}

#[tokio::test]
async fn request_actor_suspend() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    let req = |worker: &str, uid: &str| pb::RequestActorSuspendRequest {
        worker: Some(wref(worker)),
        actor: Some(oref(HIVE, "a1")),
        actor_uid: uid.into(),
    };
    assert_code(
        h.ws()
            .request_actor_suspend(h.keeper_req("node2", req("w1", &uid)))
            .await,
        Code::NotFound,
        "Worker w1 not found",
    );
    assert_code(
        h.ws()
            .request_actor_suspend(h.keeper_req(NODE, req("w1", &uuid::Uuid::new_v4().to_string())))
            .await,
        Code::NotFound,
        "Actor",
    );
    h.create_worker("w2", NODE, "pool1", 1).await;
    assert_code(
        h.ws()
            .request_actor_suspend(h.keeper_req(NODE, req("w2", &uid)))
            .await,
        Code::NotFound,
        "Actor",
    );
    let got = h
        .ws()
        .request_actor_suspend(h.keeper_req(NODE, req("w1", &uid)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        state(got.actor.as_ref().unwrap()),
        pb::ActorState::Suspended
    );
    // The actor no longer names the worker.
    assert_code(
        h.ws()
            .request_actor_suspend(h.keeper_req(NODE, req("w1", &uid)))
            .await,
        Code::NotFound,
        "",
    );
}

#[tokio::test]
async fn request_actor_suspend_passes_through_the_workflows_refusal() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    let lease = h
        .store
        .acquire_lease(&format!("lease:actor:{HIVE}:a1"))
        .await
        .unwrap();
    let r = h
        .ws()
        .request_actor_suspend(h.keeper_req(
            NODE,
            pb::RequestActorSuspendRequest {
                worker: Some(wref("w1")),
                actor: Some(oref(HIVE, "a1")),
                actor_uid: uid,
            },
        ))
        .await;
    assert_code(r, Code::Aborted, "another operation");
    lease.close().await;
}

#[tokio::test]
async fn report_actor_crashed() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    let req = |node_worker: &str| pb::ReportActorCrashedRequest {
        worker: Some(wref(node_worker)),
        actor: Some(oref(HIVE, "a1")),
        actor_uid: uid.clone(),
        message: "exit 137".into(),
    };
    assert_code(
        h.ws()
            .report_actor_crashed(h.keeper_req("node2", req("w1")))
            .await,
        Code::NotFound,
        "",
    );
    let got = h
        .ws()
        .report_actor_crashed(h.keeper_req(NODE, req("w1")))
        .await
        .unwrap()
        .into_inner();
    let actor = got.actor.unwrap();
    assert_eq!(state(&actor), pb::ActorState::Crashed);
    assert_eq!(actor.status.unwrap().crash.unwrap().message, "exit 137");
    // Refused once nothing hosts it.
    assert_code(
        h.ws()
            .report_actor_crashed(h.keeper_req(NODE, req("w1")))
            .await,
        Code::NotFound,
        "",
    );
}

#[tokio::test]
async fn report_actor_snapshot_state_refused() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    let local = pb::SnapshotReport {
        uuid: uuid::Uuid::new_v4().to_string(),
        generation: 1,
        fidelity: pb::SnapshotFidelity::Memory as i32,
        object: Some(pb::ObjectSnapshot {
            snapshot_uri: format!("{LOCATION}/x"),
        }),
        ..Default::default()
    };
    let req = |uid: &str| pb::ReportActorSnapshotStateRequest {
        actor: Some(oref(HIVE, "a1")),
        actor_uid: uid.into(),
        state: Some(pb::ActorSnapshotState {
            latest_local: Some(local.clone()),
            ..Default::default()
        }),
    };
    assert_code(
        h.ws()
            .report_actor_snapshot_state(h.keeper_req("node2", req(&uid)))
            .await,
        Code::NotFound,
        "",
    );
    assert_code(
        h.ws()
            .report_actor_snapshot_state(h.keeper_req(NODE, req(&uuid::Uuid::new_v4().to_string())))
            .await,
        Code::NotFound,
        "",
    );
    // A report that changes nothing writes nothing.
    let before = h.get_actor("a1").await;
    h.ws()
        .report_actor_snapshot_state(h.keeper_req(NODE, req(&uid)))
        .await
        .unwrap();
    assert_eq!(
        h.get_actor("a1").await.metadata.unwrap().version,
        before.metadata.unwrap().version
    );
    // Transitioning actors are retried later.
    h.set_actor_status("a1", |s| s.state = pb::ActorState::Suspending as i32)
        .await;
    assert_code(
        h.ws()
            .report_actor_snapshot_state(h.keeper_req(NODE, req(&uid)))
            .await,
        Code::Aborted,
        "retry once that finishes",
    );
}

#[tokio::test]
async fn hosting_node_after_worker_release_and_without_worker() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    assert_eq!(
        hivekeep_api::hosting::hosting_node(h.store.as_ref(), &a)
            .await
            .unwrap(),
        NODE
    );
    // Released from the store's assignments but still named: no node.
    h.store
        .release_actor_from_worker("w1", &a.metadata.as_ref().unwrap().uid)
        .await
        .unwrap();
    assert_eq!(
        hivekeep_api::hosting::hosting_node(h.store.as_ref(), &a)
            .await
            .unwrap(),
        ""
    );
    let mut suspended = a.clone();
    let st = suspended.status.as_mut().unwrap();
    st.state = pb::ActorState::Suspended as i32;
    st.worker_assignment = None;
    assert_eq!(
        hivekeep_api::hosting::hosting_node(h.store.as_ref(), &suspended)
            .await
            .unwrap(),
        NODE
    );
    suspended.status.as_mut().unwrap().state = pb::ActorState::Crashed as i32;
    assert_eq!(
        hivekeep_api::hosting::hosting_node(h.store.as_ref(), &suspended)
            .await
            .unwrap(),
        ""
    );
}

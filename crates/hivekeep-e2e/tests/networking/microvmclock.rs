//! Delayed-restore TLS on the micro-VM runtime: the origin's trust chain is
//! issued only after a suspended Actor has been idle, so a guest clock that
//! resumed at its snapshot time would reject the fresh certificate. The only
//! wait is the deliberate interval; ingress readiness is checked separately
//! before the one measured HTTPS operation.

use std::time::{Duration, SystemTime};

use hivekeep_e2e::harness::T;
use hivekeep_e2e::images::Image;
use hivekeep_e2e::retry::resume_actor_await_capacity;
use hivekeep_e2e::sandbox::is_microvm;
use hivekeep_e2e::serverpod::{Server, ServerPod};
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_proto::api::v1 as pb;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::jiff::Timestamp;
use kube::api::Api;

use crate::networking::{
    assert_protocol_gateway, log_scan_start, post_egress_once, prepare_protocol_actor,
    prepare_protocol_origin, wait_for_route_ready, ProtocolObservation,
};

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn microvm_clock_after_delayed_restore() {
    e2e_test("TestMicroVMClockAfterDelayedRestore", |t| async move {
        if !is_microvm() {
            t.skip("micro-VM clock contract");
        }
        let (actor_name, router, aref) = prepare_protocol_actor(&t, "clock").await;
        let c = clients();
        let oref = aref.to_object_ref();
        t.log(format!(
            "clock evidence host_utc_before_suspend={} actor={}/{actor_name}",
            Timestamp::now(),
            aref.hive
        ));
        let before = c
            .api()
            .get_actor(pb::GetActorRequest {
                actor: Some(oref.clone()),
            })
            .await
            .unwrap_or_else(|e| t.fatal(format!("GetActor before SuspendActor: {e}")))
            .into_inner();
        let assignment = before.status.and_then(|s| s.worker_assignment);
        if assignment
            .as_ref()
            .and_then(|a| a.worker.as_ref())
            .is_none_or(|w| w.name.is_empty())
        {
            t.fatal(format!(
                "actor had no worker assignment before SuspendActor: {assignment:?}"
            ));
        }
        let suspend_boundary = SystemTime::now();
        let suspended = c
            .api()
            .suspend_actor(pb::SuspendActorRequest {
                actor: Some(oref.clone()),
                ..Default::default()
            })
            .await
            .unwrap_or_else(|e| t.fatal(format!("SuspendActor: {e}")))
            .into_inner();
        let status = suspended.actor.and_then(|a| a.status).unwrap_or_default();
        if status.state != pb::ActorState::Suspended as i32 {
            t.fatal(format!(
                "SuspendActor returned state {:?}, want SUSPENDED",
                pb::ActorState::try_from(status.state)
            ));
        }
        if status.worker_assignment.is_some() {
            t.fatal(format!(
                "SuspendActor returned worker assignment {:?}, want it cleared",
                status.worker_assignment
            ));
        }
        // Long enough to separate the snapshot boundary from the fresh
        // certificate and the resumed request.
        const DELAY: Duration = Duration::from_secs(70);
        t.log(format!(
            "clock evidence suspended; waiting {DELAY:?} before issuing a fresh root and leaf"
        ));
        tokio::time::sleep(DELAY).await;
        t.log(format!(
            "clock evidence host_utc_after_delay={}",
            Timestamp::now()
        ));

        let spec = ServerPod {
            name: "clock-origin".into(),
            image: Image::Testserver,
            args: vec!["http".into(), "--body=delayed restore clock".into()],
            port: 8080,
            tls_port: 8443,
            ..Default::default()
        };
        let (target, address, root_ca) = prepare_protocol_origin(&t, spec, true).await;
        log_origin_certificate_validity(&t, &target, &root_ca, suspend_boundary).await;
        resume_actor_await_capacity(&t, &c, pb::ResumeActorRequest { actor: Some(oref) })
            .await
            .unwrap_or_else(|e| t.fatal(format!("ResumeActor: {e}")));
        wait_for_route_ready(&t, "restored actor ingress readiness", || {
            router.get(&aref, "/readyz")
        })
        .await;
        let since = log_scan_start();
        let raw = post_egress_once(
            &t,
            &router,
            &aref,
            "/",
            &serde_json::json!({ "url": format!("https://{address}/healthz"), "http1": true }),
        )
        .await;
        let got: ProtocolObservation = serde_json::from_slice(&raw).unwrap_or_else(|e| {
            t.fatal(format!("decoding delayed-restore HTTPS observation: {e}"))
        });
        if !got.error.is_empty()
            || got.status_code != 200
            || got.body != "delayed restore clock"
            || got.protocol != "HTTP/1.1"
            || !got.tls
        {
            t.error(format!("delayed-restore HTTPS observation = {got:?}; want one verified TLS HTTP/1.1 8443 request"));
        }
        assert_protocol_gateway(&t, since, &actor_name, &target.tls_address()).await;
    });
}

/// The validity window of a PEM certificate, as UNIX seconds.
fn validity(t: &T, pem: &[u8], what: &str) -> (i64, i64) {
    let der = rustls_pemfile::certs(&mut &pem[..])
        .next()
        .unwrap_or_else(|| t.fatal(format!("{what} is not PEM")))
        .unwrap_or_else(|e| t.fatal(format!("{what} is not PEM: {e}")));
    let (_, cert) = x509_parser::parse_x509_certificate(&der)
        .unwrap_or_else(|e| t.fatal(format!("parsing {what}: {e}")));
    (
        cert.validity().not_before.timestamp(),
        cert.validity().not_after.timestamp(),
    )
}

async fn log_origin_certificate_validity(
    t: &T,
    target: &Server,
    root_ca: &str,
    suspend_boundary: SystemTime,
) {
    let (root_nb, root_na) = validity(t, root_ca.as_bytes(), "origin root CA");
    let secret = Api::<Secret>::namespaced(clients().kube(), &target.namespace)
        .get("origin-tls")
        .await
        .unwrap_or_else(|e| t.fatal(format!("reading origin leaf metadata: {e}")));
    let leaf_pem = secret
        .data
        .unwrap_or_default()
        .get("tls.crt")
        .map(|b| b.0.clone())
        .unwrap_or_default();
    let (leaf_nb, leaf_na) = validity(t, &leaf_pem, "origin leaf");
    let boundary = suspend_boundary
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default();
    if leaf_nb <= boundary {
        t.fatal(format!("origin leaf NotBefore {leaf_nb} is not after suspend boundary {boundary} (UNIX seconds)"));
    }
    t.log(format!(
        "clock evidence host_utc_after_origin={} root_not_before={root_nb} root_not_after={root_na} leaf_not_before={leaf_nb} leaf_not_after={leaf_na} (UNIX seconds)",
        Timestamp::now()
    ));
}

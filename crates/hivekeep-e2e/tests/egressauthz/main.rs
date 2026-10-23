//! The egress gate's front door: the two ways it refuses a caller that is
//! not a running Actor. Both tests are negative, on purpose.
//!
//! - `gateway_refuses_a_non_actor_workload` needs a credential no test
//!   process can mint: the probe's pod-identity certificate, issued by
//!   kubelet from a real signer. Its refusal proves the gate's downstream
//!   trust anchor is the actor-identity CA and not merely some Hivekeep
//!   anchor; get that wrong and every workload in the cluster can tunnel.
//! - `gateway_refuses_an_unknown_actor` presents a cryptographically perfect
//!   credential and is denied only by the control-plane lookup, proving the
//!   gate actually consults external authorization on the CONNECT and
//!   honours a deny.

mod actoridentity;

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use hivekeep_e2e::collector_metrics::reqwest_like;
use hivekeep_e2e::env::{resource_name, system_namespace};
use hivekeep_e2e::fixtures::testserver::egressprobe::{
    HandshakeResult, STAGE_CONNECT, STAGE_GATEWAY_TLS,
};
use hivekeep_e2e::harness::T;
use hivekeep_e2e::images::Image;
use hivekeep_e2e::manifest::{kubectl_apply, render_manifest, Substitutions};
use hivekeep_e2e::namespace::{random_namespace_name, NAMESPACE_LABEL};
use hivekeep_e2e::pod::wait_for_pod_ready;
use hivekeep_e2e::portforward::{self, Forward};
use hivekeep_e2e::suite::{e2e_test, register_suite_cleanup, runtime};
use k8s_openapi::api::core::v1::Namespace;
use kube::api::{Api, DeleteParams, PostParams};

use actoridentity::{
    provision_probe_credentials, POD_IDENTITY_CREDENTIAL_PATH, UNKNOWN_ACTOR_CREDENTIAL_PATH,
};

const PROBE_NAME: &str = "egressprobe";
/// The egress gate's Service.
const EGRESS_GATEWAY_SERVICE: &str = "gate-egress";

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn gateway_refuses_a_non_actor_workload() {
    e2e_test("TestGatewayRefusesANonActorWorkload", |t| async move {
        let probe = shared_probe(&t).await;
        let result = handshake_as(
            &t,
            probe,
            "podidentity.example.com",
            POD_IDENTITY_CREDENTIAL_PATH,
        )
        .await;
        if result.ok {
            fail(&t, "the gateway opened a tunnel for the probe's podidentity credential; its front door is accepting workloads that are not actors");
        }
        if result.stage != STAGE_GATEWAY_TLS {
            fail(
                &t,
                &format!(
                    "podidentity credential failed at stage {:?}, want {STAGE_GATEWAY_TLS:?}; the probe was stopped by something other than the front door, so this test is not checking it: {}",
                    result.stage, result.error
                ),
            );
        }
        t.log(format!(
            "gateway refused the non-actor credential at its front door as expected: {}",
            result.error
        ));
    });
}

/// The credential is signed by the real actor-identity CA with the right
/// identity and purpose, so the handshake completes and the CONNECT is
/// denied only because the control plane has no such Actor. Without this,
/// nothing tells a gate that authorizes on the certificate alone from one
/// that authorizes on control-plane state.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn gateway_refuses_an_unknown_actor() {
    e2e_test("TestGatewayRefusesAnUnknownActor", |t| async move {
        let probe = shared_probe(&t).await;
        let result = handshake_as(
            &t,
            probe,
            "unknown.example.com",
            UNKNOWN_ACTOR_CREDENTIAL_PATH,
        )
        .await;
        if result.ok {
            fail(&t, "the gateway tunneled for an actor the control plane has never heard of; the external identity check is not running");
        }
        // A 403 on the CONNECT, not a TLS failure: the certificate was
        // accepted and the identity it carries rejected.
        if result.stage != STAGE_CONNECT || result.connect_status != 403 {
            fail(
                &t,
                &format!(
                    "unknown actor was refused at stage {:?} with CONNECT status {}, want {STAGE_CONNECT:?} and 403; something other than the external identity check turned it away: {}",
                    result.stage, result.connect_status, result.error
                ),
            );
        }
        t.log(format!(
            "gateway denied the unknown actor at CONNECT as expected: {}",
            result.error
        ));
    });
}

static ANY_FAILED: AtomicBool = AtomicBool::new(false);

fn fail(t: &T, msg: &str) -> ! {
    ANY_FAILED.store(true, Ordering::SeqCst);
    t.fatal(msg)
}

/// The in-cluster egress probe, reached over a forward. The SNI each test
/// passes is decorative (neither may open the tunneled handshake); it
/// travels so the probe's logs and errors name the case.
struct ProbeClient {
    base_url: String,
    _forward: Forward,
}

static PROBE: tokio::sync::OnceCell<Result<ProbeClient, String>> =
    tokio::sync::OnceCell::const_new();

/// The one probe pod the whole suite uses.
async fn shared_probe(t: &T) -> &'static ProbeClient {
    let r = PROBE.get_or_init(|| start_probe(t)).await;
    match r {
        Ok(p) => p,
        Err(e) => fail(t, &format!("starting the shared egress probe: {e}")),
    }
}

/// Creates the probe's namespace, mints its credentials there, deploys the
/// probe, waits for it and returns a client. The namespace outlives the
/// first test, so it is removed at the end of the run, and kept when a test
/// failed.
async fn start_probe(t: &T) -> Result<ProbeClient, String> {
    let kube = hivekeep_e2e::suite::clients().kube();
    let ns = random_namespace_name();
    let mut namespace = Namespace::default();
    namespace.metadata.name = Some(ns.clone());
    namespace.metadata.labels = Some([(NAMESPACE_LABEL.to_string(), "true".to_string())].into());
    Api::<Namespace>::all(kube.clone())
        .create(&PostParams::default(), &namespace)
        .await
        .map_err(|e| format!("creating namespace {ns}: {e}"))?;
    t.log(format!("created namespace {ns} for the egress probe"));
    let k = kube.clone();
    let n = ns.clone();
    register_suite_cleanup(move || {
        if ANY_FAILED.load(Ordering::SeqCst) {
            println!("keeping namespace {n} for diagnosis");
            return;
        }
        let _ = runtime().block_on(async {
            Api::<Namespace>::all(k)
                .delete(&n, &DeleteParams::default())
                .await
        });
    });

    provision_probe_credentials(t, &ns).await;
    let mut subs = Substitutions::default();
    subs.inline("${NAMESPACE}", ns.clone())
        .inline("${SYSTEM_NAMESPACE}", system_namespace())
        .inline(
            "${EGRESS_GATEWAY_SERVICE}",
            resource_name(EGRESS_GATEWAY_SERVICE),
        )
        .inline("${TESTSERVER_IMAGE}", Image::Testserver.reference());
    let manifest = render_manifest(t, "fixtures/testserver/egressprobe.yaml.tmpl", &subs);
    kubectl_apply(t, &manifest);
    wait_for_pod_ready(t, &ns, PROBE_NAME, Duration::from_secs(180)).await;
    let forward = portforward::service_port_forward(&kube, &ns, PROBE_NAME, 8080)
        .await
        .map_err(|e| format!("port-forwarding {ns}/{PROBE_NAME}: {e}"))?;
    Ok(ProbeClient {
        base_url: format!("http://{}", forward.addr()),
        _forward: forward,
    })
}

/// Asks the probe for one CONNECT plus inner TLS handshake for `sni`,
/// presenting the credential at `credential`. A refusal comes back as a
/// result with `ok == false`: refusal is the outcome under test.
async fn handshake_as(t: &T, probe: &ProbeClient, sni: &str, credential: &str) -> HandshakeResult {
    let esc = |s: &str| url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>();
    let endpoint = format!(
        "{}/handshake?sni={}&credential-bundle={}",
        probe.base_url,
        esc(sni),
        esc(credential)
    );
    let (status, body) = reqwest_like::Client::new(Duration::from_secs(90))
        .get(&endpoint)
        .await
        .unwrap_or_else(|e| fail(t, &format!("calling probe for {sni:?}: {e}")));
    if status != 200 {
        let body: String = body.chars().take(4 << 10).collect();
        fail(
            t,
            &format!("probe returned {status} for {sni:?}: {}", body.trim()),
        );
    }
    serde_json::from_str(&body)
        .unwrap_or_else(|e| fail(t, &format!("decoding probe response for {sni:?}: {e}")))
}

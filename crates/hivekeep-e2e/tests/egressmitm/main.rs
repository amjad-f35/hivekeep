//! The trust half of intercepted egress TLS: an Actor that projects the
//! egress trust bundle completes a TLS handshake with the egress gate's
//! per-SNI minted leaf using only the projected anchors, and passthrough
//! traffic reaches the origin untouched.

mod common;
mod goldenboot;

use hivekeep_e2e::env::check_env;
use hivekeep_e2e::probe::{deploy_probe, ProbeOptions, SYSTEM_ROOTS_BUNDLE_NAME};
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::serverpod::deploy_server_pod;
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_e2e::trustbundle::{ensure_egress_trust_bundle, service_dns_trust_bundle};
use hivekeep_proto::api::v1 as pb;

use common::*;

/// An Actor does TLS through the intercepting egress gate with only the
/// projected trust bundle. The origins run in the cluster with service-DNS
/// certificates, which the gate trusts and the probe's bundle does not:
///
/// - positive: `/fetch` with `roots=bundle` succeeds, so the gate's minted
///   leaf validates against the projected anchors; the bundle holds no
///   service-DNS CA, so a pass certifies interception is on;
/// - negative: trusting only the service-DNS CA fails verification, so the
///   Actor saw the minted leaf, not the origin's own certificate.
///
/// It runs once per sandbox class, since trust delivery differs per class.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_mitm_trust() {
    e2e_test("TestActorEgressMITMTrust", |t| async move {
        let env = check_env(&["HK_E2E_BUCKET_NAME"])
            .unwrap_or_else(|e| t.fatal(format!("CheckEnv failed: {e}")));
        let clients = clients();

        // Ensure, never replace: the gate signs with the pool mounted into
        // its pod, and replacing it here would race the Secret's propagation
        // into that mount.
        ensure_egress_trust_bundle(&t, &clients).await;

        // One pod answers http and https for the intercepted name;
        // passthrough is decided by name, so it needs an origin of its own.
        let mitm = deploy_server_pod(&t, &origin_spec("mitm-origin")).await;
        let mut pt_spec = origin_spec("passthrough-origin");
        pt_spec.namespace = mitm.namespace.clone();
        let passthrough = deploy_server_pod(&t, &pt_spec).await;
        let origin_roots = format!(
            "ca={}",
            escape(&service_dns_trust_bundle(&t, &clients).await)
        );

        let (hive, _) = deploy_probe(
            &t,
            &clients,
            &env["HK_E2E_BUCKET_NAME"],
            "egressmitm",
            ProbeOptions::default()
                .with_trust_bundle()
                .with_system_roots(),
        )
        .await;

        let id = "probe-mitm";
        create_and_resume_actor(&t, &clients, &hive, id, &mitm, &passthrough).await;
        wait_for_actor_state(&t, &clients, &hive, id, pb::ActorState::Running).await;

        let rc = RouterClient::new(&t, &clients)
            .await
            .unwrap_or_else(|e| t.fatal(format!("NewRouterClient: {e}")));
        let origin = format!("https://{}/healthz", mitm.svc_name());

        let pos = fetch_until_reached(&t, &rc, &hive, id, &origin, "bundle", &[]).await;
        if !pos.error.is_empty() {
            t.fatal(format!(
                "TLS through the MITM egress gate with the projected trust bundle failed: {} (the projected anchors did not validate the gate's minted leaf, or interception/minting is broken)",
                pos.error
            ));
        }
        if pos.status != "200" {
            t.fatal(format!(
                "fetch {origin} via projected bundle: status {}, want 200 (a 5xx is the gate failing to reach or trust the origin; does the gate trust the service-DNS signer?) body {:?}",
                pos.status, pos.body
            ));
        }

        let neg = probe_fetch(
            &t,
            &rc,
            &hive,
            id,
            &origin,
            "pem",
            std::slice::from_ref(&origin_roots),
        )
        .await;
        if neg.error.is_empty() {
            t.error(format!(
                "fetch trusting only the origin's CA unexpectedly succeeded (status {}): the Actor saw the origin's own certificate; is the egress gate terminating TLS?",
                neg.status
            ));
        } else if !neg.error.contains("certificate")
            && !neg.error.contains("x509")
            && !neg.error.to_lowercase().contains("unknownissuer")
        {
            t.error(format!("fetch trusting only the origin's CA failed, but not with a certificate-verification error: {}", neg.error));
        }

        // A port in the Host is never dialed: the gate dials the Host's name
        // on the port the Actor dialed, and nothing answers on port 1.
        for (o, dial) in [
            (
                format!("https://{}:1/healthz", mitm.svc_name()),
                mitm.tls_address(),
            ),
            (
                format!("http://{}:1/healthz", mitm.svc_name()),
                mitm.address(),
            ),
        ] {
            let res = probe_fetch(
                &t,
                &rc,
                &hive,
                id,
                &o,
                "bundle",
                &[format!("dial={}", escape(&dial))],
            )
            .await;
            if !res.error.is_empty() {
                t.error(format!(
                    "fetch of {o} dialed at {dial} failed: {}",
                    res.error
                ));
            } else if res.status != "200" {
                t.error(format!("fetch of {o} dialed at {dial}: status {}, want 200 (the gate dialed the port in the Host instead of the one the Actor dialed)", res.status));
            }
        }

        // Passthrough TLS is relayed unread, so the origin's own certificate
        // must validate against its CA alone, which also proves it was not
        // intercepted.
        let pt_origin = format!("https://{}/healthz", passthrough.svc_name());
        let pt = fetch_until_reached(
            &t,
            &rc,
            &hive,
            id,
            &pt_origin,
            "pem",
            std::slice::from_ref(&origin_roots),
        )
        .await;
        if !pt.error.is_empty() {
            t.fatal(format!(
                "TLS passthrough to {pt_origin} failed: {} (the origin's certificate did not validate against its CA, so the connection was intercepted, misrouted, or closed)",
                pt.error
            ));
        }
        if pt.status != "200" {
            t.fatal(format!(
                "passthrough fetch {pt_origin}: status {}, want 200; body {:?}",
                pt.status, pt.body
            ));
        }

        // The gate dials the name the SNI claims.
        let by_sni = fetch_until_reached(
            &t,
            &rc,
            &hive,
            id,
            &pt_origin,
            "pem",
            &[
                origin_roots.clone(),
                format!("dial={}", escape(UNREACHABLE_ADDRESS)),
            ],
        )
        .await;
        if !by_sni.error.is_empty() {
            t.error(format!(
                "TLS passthrough to {pt_origin} dialed at {UNREACHABLE_ADDRESS} failed: {} (the gate dialed the Actor's address instead of resolving the SNI)",
                by_sni.error
            ));
        } else if by_sni.status != "200" {
            t.error(format!("passthrough fetch {pt_origin} dialed at {UNREACHABLE_ADDRESS}: status {}, want 200", by_sni.status));
        }

        // Each name is an origin's fully qualified alias: only the wrong-port
        // rule lists the first and no rule lists the second. Neither may
        // complete TLS; the dial bypasses DNS.
        let denials = [
            (
                "passthrough rule excludes port 443",
                format!("https://{}.cluster.local/healthz", passthrough.svc_name()),
                passthrough.tls_address(),
            ),
            (
                "host outside the policy",
                format!("https://{}.cluster.local/healthz", mitm.svc_name()),
                mitm.tls_address(),
            ),
        ];
        for (name, o, dial) in denials {
            let (rc, hive) = (&rc, &hive);
            t.run(name, |t| async move {
                let denied = probe_fetch(&t, rc, hive, id, &o, "bundle", &[format!("dial={}", escape(&dial))]).await;
                if denied.tls_handshake != "closed" || !denied.status.is_empty() || denied.error.is_empty() {
                    t.error(format!(
                        "fetch of {o}: handshake {:?}, status {:?}, error {:?}; want connection closed during TLS with no HTTP response",
                        denied.tls_handshake, denied.status, denied.error
                    ));
                }
            })
            .await;
        }

        // No fetch here verifies a public certificate, so check that the
        // keeper merged its WebPKI roots into the projected bundle.
        let (rc, hive) = (&rc, &hive);
        t.run("projected bundle holds the system roots", |t| async move {
            let trust = probe_trust(&t, rc, hive, id).await;
            if !holds_certificate(&t, &trust, SYSTEM_ROOT) {
                t.error(format!("the probe's projected trust bundle has no certificate for {SYSTEM_ROOT:?}; the keeper did not merge {SYSTEM_ROOTS_BUNDLE_NAME} into it"));
            }
        })
        .await;
    });
}

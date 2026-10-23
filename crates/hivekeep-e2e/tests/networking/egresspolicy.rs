//! The EgressPolicy half of egress: the other egress tests give their
//! Actors an allow-everything policy and prove traffic flows; these give
//! theirs a narrow one and prove what does not. A request the gate can read
//! is decided per request on its Host, by the http rules in the clear and
//! the https rules once decrypted.

use std::time::Duration;

use bytes::Bytes;
use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::dataplane::Dataplane;
use hivekeep_e2e::egresspolicy::{
    egress_allow_http, egress_allow_https, egress_allow_passthrough, ensure_egress_policy,
};
use hivekeep_e2e::harness::T;
use hivekeep_e2e::images::Image;
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::sandbox::egress_fixture;
use hivekeep_e2e::serverpod::{deploy_server_pod, ServerPod};
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_e2e::trustbundle::service_dns_trust_bundle;

use crate::networking::{
    actor_ref, assert_egress_gateway_connect, create_and_resume_actor_with_egress,
    decode_egress_fetch, egress_http_target, egress_https_target, fetch_through_egress_actor,
    log_scan_start, lossy, must_router_client, not_transient, post_through_egress_actor_until,
    prepare_protocol_origin, reached, wait_for_actor_route,
};

/// [`fetch_through_egress_actor`] with the caller deciding which answer is
/// final.
pub async fn fetch_through_egress_actor_until(
    t: &T,
    router: &RouterClient,
    aref: &ActorRef,
    url: &str,
    done: fn(u16, &[u8]) -> bool,
) -> (u16, Bytes) {
    let payload = serde_json::to_vec(&serde_json::json!({ "url": url })).unwrap_or_default();
    post_through_egress_actor_until(t, router, aref, "/", payload, done).await
}

/// Resolver phrasings of an NXDOMAIN answer.
const NXDOMAIN_MARKERS: &[&str] = &[
    "no such host",
    "failed to lookup address",
    "Name or service not known",
    "nodename nor servname",
];

/// Whether `body` is the demo's report of a lookup of `name` the sandbox's
/// DNS relay denied: the relay answers NXDOMAIN, which the demo's resolver
/// reports in one of [`NXDOMAIN_MARKERS`]' phrasings.
pub fn is_denied_lookup(body: &str, name: &str) -> bool {
    body.contains(name) && NXDOMAIN_MARKERS.iter().any(|m| body.contains(m))
}

/// The policy names one hostname, so the origin is reachable by that name
/// and refused by its address, which is the same server.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_policy_denies_unlisted_host() {
    e2e_test("TestActorEgressPolicyDeniesUnlistedHost", |t| async move {
        let dataplane = Dataplane::current();
        let origin = egress_http_target();
        let target = deploy_server_pod(&t, &origin).await;
        let allowed = format!("{}.{}.svc.cluster.local", origin.name, target.namespace);
        let (_, actor_name, _) = create_and_resume_actor_with_egress(
            &t,
            "egress-policy",
            &egress_fixture(),
            &[egress_allow_http(&[allowed.as_str()])],
        )
        .await;
        let router = must_router_client(&t).await;
        let aref = actor_ref(&actor_name);
        let url = format!("http://{allowed}/healthz");
        let (status, body) = fetch_through_egress_actor(&t, &router, &aref, &url).await;
        if status != 200 {
            t.fatal(format!(
                "fetch of the allowed host {url} returned HTTP {status}, want 200; body: {}",
                lossy(&body)
            ));
        }
        let url = format!("http://{}/healthz", target.address());
        let (status, body) =
            fetch_through_egress_actor_until(&t, &router, &aref, &url, not_transient).await;
        if !dataplane.is_egress_policy_denied(status, &lossy(&body)) {
            t.fatal(format!("fetch of the same origin by address {url} returned HTTP {status}, want an egress-policy denial; body: {}", lossy(&body)));
        }
        t.log(format!(
            "fetch by address was denied as expected: {}",
            lossy(&body)
        ));
    });
}

/// An Actor with no EgressPolicy is denied.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_requires_policy() {
    e2e_test("TestActorEgressRequiresPolicy", |t| async move {
        let dataplane = Dataplane::current();
        let target = deploy_server_pod(&t, &egress_http_target()).await;
        let (_, actor_name, _) =
            create_and_resume_actor_with_egress(&t, "egress-nopolicy", &egress_fixture(), &[])
                .await;
        let router = must_router_client(&t).await;
        let aref = actor_ref(&actor_name);
        // No allowed fetch proves the route is up, so wait on readiness;
        // otherwise a 503 from an unpropagated route would pass as a denial.
        wait_for_actor_route(&t, &router, &aref).await;
        let url = format!("http://{}/healthz", target.address());
        let (status, body) =
            fetch_through_egress_actor_until(&t, &router, &aref, &url, not_transient).await;
        if !dataplane.is_egress_policy_denied(status, &lossy(&body)) {
            t.fatal(format!("fetch by an actor with no policy returned HTTP {status}, want an egress-policy denial; body: {}", lossy(&body)));
        }
        t.log(format!("egress was denied as expected: {}", lossy(&body)));
    });
}

/// One origin allowed by name for interception, another for passthrough, and
/// a name no rule lists refused. Both origins serve service-DNS
/// certificates, which the gate trusts and the Actor does not, so the
/// certificate the Actor verifies tells the two paths apart.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_https_by_hostname() {
    e2e_test("TestActorEgressHTTPSByHostname", |t| async move {
        let dataplane = Dataplane::current();
        const MITM_BODY: &str = "mitm origin";
        const PASSTHROUGH_BODY: &str = "passthrough origin";
        let mitm = deploy_server_pod(&t, &egress_https_target("mitm-origin", MITM_BODY)).await;
        let mut pspec = egress_https_target("passthrough-origin", PASSTHROUGH_BODY);
        pspec.namespace = mitm.namespace.clone();
        let passthrough = deploy_server_pod(&t, &pspec).await;
        // An http rule for the .cluster.local names lets cluster DNS search
        // resolve them, while the 443 rules still match only the svc names.
        let (mitm_fqdn, pass_fqdn) = (
            format!("{}.cluster.local", mitm.svc_name()),
            format!("{}.cluster.local", passthrough.svc_name()),
        );
        let rules = vec![
            egress_allow_http(&[mitm_fqdn.as_str(), pass_fqdn.as_str()]),
            egress_allow_https(&[mitm.svc_name().as_str()]),
            egress_allow_passthrough(&[passthrough.svc_name().as_str()]),
        ];
        let (_, actor_name, _) =
            create_and_resume_actor_with_egress(&t, "egress-sni", &egress_fixture(), &rules).await;
        let router = must_router_client(&t).await;
        let aref = actor_ref(&actor_name);
        wait_for_actor_route(&t, &router, &aref).await;
        let (r, a, m, p) = (&router, &aref, &mitm, &passthrough);

        t.run("mitm", |t| async move {
            // The Actor trusts the gate's CA but not the service-DNS signer,
            // so a verified fetch means the gate minted the leaf.
            let url = format!("https://{}/healthz", m.svc_name());
            let (status, body) = fetch_through_egress_actor_until(&t, r, a, &url, reached).await;
            if status != 200 {
                t.fatal(format!(
                    "fetch of {url} returned HTTP {status}, want 200; body: {}",
                    lossy(&body)
                ));
            }
            let got = decode_egress_fetch(&t, &body);
            if got.body != MITM_BODY || !got.tls {
                t.error(format!(
                    "fetch of {url} = {got:?}, want verified TLS and body {MITM_BODY:?}"
                ));
            }
        })
        .await;

        t.run("passthrough", |t| async move {
            // Trusting only the service-DNS signer, the fetch verifies against
            // the origin's own certificate, which interception would replace.
            let url = format!("https://{}/healthz", p.svc_name());
            let root = service_dns_trust_bundle(&t, &clients()).await;
            let payload = serde_json::to_vec(&serde_json::json!({ "url": url, "rootCA": root }))
                .unwrap_or_default();
            let (status, body) =
                post_through_egress_actor_until(&t, r, a, "/", payload, reached).await;
            if status != 200 {
                t.fatal(format!(
                    "fetch of {url} returned HTTP {status}, want 200; body: {}",
                    lossy(&body)
                ));
            }
            let got = decode_egress_fetch(&t, &body);
            if got.body != PASSTHROUGH_BODY || !got.tls {
                t.error(format!(
                    "fetch of {url} = {got:?}, want verified TLS and body {PASSTHROUGH_BODY:?}"
                ));
            }
        })
        .await;

        t.run("unlisted host denied", |t| async move {
            // The MITM origin by its fully qualified name, which no rule lists
            // and its certificate does not cover. Wrong interception fails
            // upstream; wrong passthrough hands the Actor that certificate.
            let url = format!("https://{}.cluster.local/healthz", m.svc_name());
            let (status, body) = fetch_through_egress_actor_until(&t, r, a, &url, not_transient).await;
            if !dataplane.is_egress_policy_denied(status, &lossy(&body)) {
                t.fatal(format!("fetch of {url}, outside the policy, returned HTTP {status}, want an egress-policy denial; body: {}", lossy(&body)));
            }
            // Denied TLS closes at ClientHello; a certificate error means the
            // handshake reached the origin.
            let got = decode_egress_fetch(&t, &body);
            if got.error.contains("x509") || got.error.contains("certificate") {
                t.fatal(format!("fetch of {url}, outside the policy, failed certificate verification instead of being denied, so the gateway passed it through; body: {}", lossy(&body)));
            }
            t.log(format!("fetch of {url} was denied as expected: {}", lossy(&body)));
        })
        .await;
    });
}

/// A TLS passthrough connection opened after a policy change is denied,
/// even while one the previous policy allowed is still in flight.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_passthrough_policy_refresh() {
    e2e_test("TestActorEgressPassthroughPolicyRefresh", |t| async move {
        let dataplane = Dataplane::current();
        let spec = ServerPod {
            name: "passthrough-origin".into(),
            image: Image::Testserver,
            args: vec!["http".into()],
            port: 80,
            target_port: 8080,
            tls_port: 443,
            ..Default::default()
        };
        let (origin, address, root_ca) = prepare_protocol_origin(&t, spec, true).await;
        let (hive, actor_name, _) = create_and_resume_actor_with_egress(
            &t,
            "egress-passthrough-refresh",
            &egress_fixture(),
            &[egress_allow_passthrough(&["*"])],
        )
        .await;
        let router = must_router_client(&t).await;
        let aref = actor_ref(&actor_name);
        wait_for_actor_route(&t, &router, &aref).await;
        // The test server's /fetch waits for the whole body before answering,
        // which keeps the first connection open across the policy change.
        let url = format!("https://{address}/fetch");
        let payload = serde_json::to_vec(&serde_json::json!({ "url": url, "rootCA": root_ca }))
            .unwrap_or_default();
        let since = log_scan_start();
        let first = post_through_egress_actor_until(
            &t,
            &router,
            &aref,
            "/post",
            payload.clone(),
            not_transient,
        );
        let rest = async {
            // Let the first connection establish before changing the policy.
            assert_egress_gateway_connect(&t, since, &hive, &actor_name, "443").await;
            // Keep DNS allowed so the second connection tests enforcement,
            // not the DNS relay.
            let fqdn = format!("{}.cluster.local", origin.svc_name());
            ensure_egress_policy(
                &t,
                &clients(),
                &aref.to_object_ref(),
                vec![egress_allow_http(&[fqdn.as_str()])],
            )
            .await;
            // Outlast the gate's cached policy (fresh for 10 s).
            tokio::time::sleep(Duration::from_secs(11)).await;
            post_through_egress_actor_until(
                &t,
                &router,
                &aref,
                "/post",
                payload.clone(),
                not_transient,
            )
            .await
        };
        let ((first_status, first_body), (status, body)) = tokio::join!(first, rest);
        if !dataplane.is_egress_policy_denied(status, &lossy(&body)) {
            t.fatal(format!("second POST to {url} after policy update returned HTTP {status}, want an egress-policy denial; body: {}", lossy(&body)));
        }
        if first_status != 200 {
            t.fatal(format!(
                "first POST to {url} returned HTTP {first_status}, want 200; body: {}",
                lossy(&first_body)
            ));
        }
    });
}

/// A plaintext request stalled mid-headers must not hold up an intercepted
/// HTTPS request from the same Actor.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_pending_plaintext_headers_do_not_block_mitm() {
    e2e_test(
        "TestActorEgressPendingPlaintextHeadersDoNotBlockMITM",
        |t| async move {
            const MITM_BODY: &str = "mitm origin";
            let mitm = deploy_server_pod(&t, &egress_https_target("mitm-origin", MITM_BODY)).await;
            let mut pspec = egress_http_target();
            pspec.namespace = mitm.namespace.clone();
            let plaintext = deploy_server_pod(&t, &pspec).await;
            let (m_svc, m_fqdn) = (
                mitm.svc_name(),
                format!("{}.cluster.local", mitm.svc_name()),
            );
            let (p_svc, p_fqdn) = (
                plaintext.svc_name(),
                format!("{}.cluster.local", plaintext.svc_name()),
            );
            let rules = vec![
                egress_allow_https(&[m_svc.as_str(), m_fqdn.as_str()]),
                egress_allow_http(&[p_svc.as_str(), p_fqdn.as_str()]),
            ];
            let (hive, actor_name, _) = create_and_resume_actor_with_egress(
                &t,
                "egress-slow-headers",
                &egress_fixture(),
                &rules,
            )
            .await;
            let router = must_router_client(&t).await;
            let aref = actor_ref(&actor_name);
            wait_for_actor_route(&t, &router, &aref).await;
            let plaintext_url = format!("http://{p_svc}/healthz");
            let slow = serde_json::to_vec(&serde_json::json!({ "url": plaintext_url }))
                .unwrap_or_default();
            let since = log_scan_start();
            let pending = post_through_egress_actor_until(
                &t,
                &router,
                &aref,
                "/slow-headers",
                slow,
                not_transient,
            );
            let rest = async {
                // Let the plaintext request send its incomplete headers first.
                assert_egress_gateway_connect(&t, since, &hive, &actor_name, "80").await;
                let mitm_url = format!("https://{m_svc}/healthz");
                let (status, body) =
                    fetch_through_egress_actor_until(&t, &router, &aref, &mitm_url, reached).await;
                if status != 200 {
                    t.fatal(format!(
                        "fetch of {mitm_url} returned HTTP {status}, want 200; body: {}",
                        lossy(&body)
                    ));
                }
                let got = decode_egress_fetch(&t, &body);
                if got.body != MITM_BODY || !got.tls {
                    t.fatal(format!(
                        "fetch of {mitm_url} = {got:?}, want verified TLS and body {MITM_BODY:?}"
                    ));
                }
            };
            let ((status, body), ()) = tokio::join!(pending, rest);
            if status != 200 {
                t.fatal(format!(
                    "fetch of {plaintext_url} returned HTTP {status}, want 200; body: {}",
                    lossy(&body)
                ));
            }
        },
    );
}

/// The sandbox's DNS relay denies queries of an Actor with no policy, or for
/// a host outside its policy, and answers them once a rule matches, without
/// a restart.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_dns_requires_policy() {
    e2e_test("TestActorDNSRequiresPolicy", |t| async move {
        let origin = egress_http_target();
        let target = deploy_server_pod(&t, &origin).await;
        let name = format!("{}.{}.svc.cluster.local", origin.name, target.namespace);
        let url = format!("http://{name}/healthz");
        let (_, actor_name, _) =
            create_and_resume_actor_with_egress(&t, "dns-nopolicy", &egress_fixture(), &[]).await;
        let router = must_router_client(&t).await;
        let aref = actor_ref(&actor_name);
        wait_for_actor_route(&t, &router, &aref).await;
        let (status, body) =
            fetch_through_egress_actor_until(&t, &router, &aref, &url, not_transient).await;
        if status != 502 || !is_denied_lookup(&lossy(&body), &name) {
            t.fatal(format!("fetch of {url} by an actor with no policy returned HTTP {status}, want its lookup denied; body: {}", lossy(&body)));
        }
        t.log(format!("lookup was denied as expected: {}", lossy(&body)));
        // The relay refetches the policy within seconds. A wildcard over the
        // target namespace's Services allows the name and still refuses an
        // unlisted host.
        let wildcard = format!("*.{}.svc.cluster.local", target.namespace);
        ensure_egress_policy(
            &t,
            &clients(),
            &aref.to_object_ref(),
            vec![egress_allow_http(&[wildcard.as_str()])],
        )
        .await;
        let (status, body) =
            fetch_through_egress_actor_until(&t, &router, &aref, &url, reached).await;
        if status != 200 {
            t.fatal(format!("fetch of {url} after the actor was given a policy returned HTTP {status}, want 200; body: {}", lossy(&body)));
        }
        const UNLISTED: &str = "unlisted.example.com";
        let (status, body) = fetch_through_egress_actor_until(
            &t,
            &router,
            &aref,
            &format!("http://{UNLISTED}/healthz"),
            not_transient,
        )
        .await;
        if status != 502 || !is_denied_lookup(&lossy(&body), UNLISTED) {
            t.fatal(format!("fetch of unlisted host {UNLISTED} returned HTTP {status}, want its lookup denied; body: {}", lossy(&body)));
        }
    });
}

#[test]
fn denied_lookup_detection() {
    assert!(is_denied_lookup(
        "dial: lookup a.example on 10.0.0.10:53: no such host",
        "a.example"
    ));
    assert!(is_denied_lookup(
        "fetching http://a.example/: dns error: failed to lookup address information",
        "a.example"
    ));
    assert!(!is_denied_lookup("lookup a.example: timeout", "a.example"));
    assert!(!is_denied_lookup(
        "lookup b.example: no such host",
        "a.example"
    ));
}

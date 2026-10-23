//! Ingress and egress basics, and the helpers the rest of the suite shares:
//! creating and resuming Actors, talking to the egress demo through the
//! router, reading the egress gate's access log, and standing up origins
//! (optionally with TLS) with diagnostics on failure.

use std::net::IpAddr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hivekeep_core::resource::ActorRef;
use hivekeep_core::spiffe::worker_for_actor_spiffe_id;
use hivekeep_e2e::dataplane::Dataplane;
use hivekeep_e2e::egresspolicy::{egress_allow_all, egress_allow_https, ensure_egress_policy};
use hivekeep_e2e::harness::T;
use hivekeep_e2e::images::Image;
use hivekeep_e2e::router_client::{RouterClient, RouterResponse};
use hivekeep_e2e::sandbox::{counter_fixture, egress_fixture, Fixture, HiveFixture};
use hivekeep_e2e::serverpod::{
    create_server_service, deploy_server_pod, service_dns_tls, wait_for_server_endpoint, Server,
    ServerPod,
};
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_e2e::{env, install, retry, testcert};
use hivekeep_proto::api::v1 as pb;
use k8s_openapi::api::core::v1::{
    Event, Pod, Secret, SecretVolumeSource, Service, Volume, VolumeMount,
};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::jiff::{SignedDuration, Timestamp};
use k8s_openapi::ByteString;
use kube::api::{Api, ListParams, LogParams, PostParams};
use serde::Deserialize;

/// The hive every Actor of this suite lives in.
pub const NETWORKING_HIVE: &str = "networking-e2e";

/// The snapshot bucket the suite's own fixtures store into.
pub const BUCKET_ENV: &str = "HK_E2E_BUCKET_NAME";

/// The namespace and Secret holding the service-DNS signer's CA pool, which
/// the egress gate trusts for in-cluster origins.
pub const SERVICE_DNS_CA_NAMESPACE: &str = "podcertificate-controller-system";
/// See [`SERVICE_DNS_CA_NAMESPACE`].
pub const SERVICE_DNS_CA_SECRET: &str = "service-dns-ca-pool";

/// The ref of `name` in the suite's hive.
pub fn actor_ref(name: &str) -> ActorRef {
    ActorRef::new(NETWORKING_HIVE, name)
}

/// Now, minus a minute of slack for clock skew against the gate's node: the
/// lower bound of an access-log scan for lines a test produced.
pub fn log_scan_start() -> Timestamp {
    Timestamp::now()
        .checked_sub(SignedDuration::from_secs(60))
        .unwrap_or(Timestamp::UNIX_EPOCH)
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_direct_access() {
    e2e_test("TestActorDirectAccess", |t| async move {
        let (_, actor_name, actor) =
            create_and_resume_hive_actor(&t, "direct", &counter_fixture()).await;
        let router = must_router_client(&t).await;
        let r = &router;
        t.run("direct", |t| async move {
            assert_direct_actor_access(&t, &actor).await
        })
        .await;
        let name = actor_name.clone();
        t.run("via ingress", |t| async move {
            let aref = actor_ref(&name);
            let body = wait_for_route_ready(&t, "Actor access through ingress", || {
                r.get(&aref, "/readyz")
            })
            .await;
            t.log(format!(
                "Actor access through ingress succeeded; body: {body}"
            ));
        })
        .await;
        t.run("duplicate actor header rejected", |t| async move {
            let aref = actor_ref(&actor_name);
            let mut req = http::Request::get("/readyz")
                .header(http::header::HOST, r.address())
                .body(http_body_util::Full::new(Bytes::new()))
                .unwrap_or_else(|e| t.fatal(format!("creating request: {e}")));
            let h = http::HeaderName::from_static(install::TARGET_ACTOR_HEADER);
            req.headers_mut()
                .append(h.clone(), aref.to_string().parse().expect("header value"));
            req.headers_mut()
                .append(h, http::HeaderValue::from_static("untrusted/actor"));
            let resp = r
                .send(req)
                .await
                .unwrap_or_else(|e| t.fatal(format!("Error while sending request: {e}")));
            if resp.status != 404 {
                t.fatal(format!(
                    "duplicate actor header on plain HTTP returned {}, want 404; body: {}",
                    resp.status,
                    resp.text()
                ));
            }
        })
        .await;
    });
}

/// The origin the egress tests dial: the test server's `http` subcommand,
/// published on port 80 and listening on 8080.
pub fn egress_http_target() -> ServerPod {
    ServerPod {
        name: "egresshttp".into(),
        image: Image::Testserver,
        args: vec!["http".into()],
        port: 80,
        target_port: 8080,
        ..Default::default()
    }
}

/// The full egress path: the Actor's outbound connection is redirected into
/// its tunnel, wrapped in mTLS with the Actor's own certificate and a
/// CONNECT to the egress gate, authorized there and only then dialed out.
/// A masqueraded egress would reach the origin as well; the access-log
/// assertion is what proves the traffic went through the gate.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress() {
    e2e_test("TestActorEgress", |t| async move {
        // Deploy the origin first so a fixture failure costs no resume.
        let origin = egress_http_target();
        let target = deploy_server_pod(&t, &origin).await;
        let (hive, actor_name, _) = create_and_resume_actor_with_egress(
            &t,
            "egress",
            &egress_fixture(),
            &egress_allow_all(),
        )
        .await;
        let router = must_router_client(&t).await;
        let aref = actor_ref(&actor_name);
        let url = format!("http://{}/healthz", target.address());
        let since = log_scan_start();
        let (status, body) = fetch_through_egress_actor(&t, &router, &aref, &url).await;
        if status != 200 {
            t.fatal(format!(
                "Actor egress fetch of {url} returned HTTP {status}, want 200; body: {}",
                lossy(&body)
            ));
        }
        t.log(format!(
            "Actor egress fetch of {url} succeeded; body: {}",
            lossy(&body)
        ));
        assert_egress_gateway_connect(&t, since, &hive, &actor_name, &origin.port.to_string())
            .await;

        let (r, a, target) = (&router, &aref, &target);
        t.run("by DNS name", |t| async move {
            let url = format!(
                "http://{}.{}.svc.cluster.local/healthz",
                origin.name, target.namespace
            );
            let (status, body) = fetch_through_egress_actor(&t, r, a, &url).await;
            if status != 200 {
                t.fatal(format!(
                    "Actor egress fetch of {url} returned HTTP {status}, want 200; body: {}",
                    lossy(&body)
                ));
            }
            t.log(format!(
                "Actor egress fetch of {url} succeeded; body: {}",
                lossy(&body)
            ));
        })
        .await;
    });
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_multiplexing() {
    e2e_test("TestActorEgressMultiplexing", |t| async move {
        let dataplane = Dataplane::current();
        let origin = egress_http_target();
        let target = deploy_server_pod(&t, &origin).await;
        let (hive, actor_name, _) = create_and_resume_actor_with_egress(
            &t,
            "egress-mux",
            &egress_fixture(),
            &egress_allow_all(),
        )
        .await;
        let router = must_router_client(&t).await;
        let aref = actor_ref(&actor_name);
        wait_for_actor_route(&t, &router, &aref).await;
        let kube = clients().kube();
        let before = dataplane
            .scrape_metrics(&kube)
            .await
            .unwrap_or_else(|e| t.fatal(format!("ScrapeMetrics before /mux: {e}")));
        let url = format!("http://{}/fetch", target.address());
        let payload = serde_json::to_vec(&serde_json::json!({ "url": url })).unwrap_or_default();
        let since = log_scan_start();
        let (status, body) = post_through_egress_actor(&t, &router, &aref, "/mux", payload).await;
        if status != 200 {
            t.fatal(format!(
                "Actor egress mux of {url} returned HTTP {status}, want 200; body: {}",
                lossy(&body)
            ));
        }
        t.log(format!(
            "Actor egress mux of {url} succeeded; body: {}",
            lossy(&body)
        ));
        assert_egress_gateway_connect(&t, since, &hive, &actor_name, &origin.port.to_string())
            .await;
        let after = dataplane
            .scrape_metrics(&kube)
            .await
            .unwrap_or_else(|e| t.fatal(format!("ScrapeMetrics after /mux: {e}")));
        // Dataplanes may differ in stream concurrency.
        const WANT_CONNECTIONS_AT_LEAST: i64 = 1;
        const WANT_REQUESTS: i64 = 250;
        let stats = dataplane.egress_connect_stats(&before, &after);
        if stats.connections != -1 && stats.connections < WANT_CONNECTIONS_AT_LEAST {
            t.error(format!(
                "egress connect connections = {}, want at least {WANT_CONNECTIONS_AT_LEAST}",
                stats.connections
            ));
        }
        if stats.requests != -1 && stats.requests != WANT_REQUESTS {
            t.error(format!(
                "egress connect requests = {}, want {WANT_REQUESTS}",
                stats.requests
            ));
        }
    });
}

/// An origin whose `/healthz` answers `body` over HTTPS on 443 (and in
/// cleartext on 80), with a service-DNS certificate the egress gate trusts.
pub fn egress_https_target(name: &str, body: &str) -> ServerPod {
    service_dns_tls(ServerPod {
        name: name.into(),
        image: Image::Testserver,
        args: vec!["http".into(), format!("--body={body}")],
        ..Default::default()
    })
}

/// The path of [`actor_egress`] with a TLS origin, through the gate's TLS
/// interception.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_https() {
    e2e_test("TestActorEgressHTTPS", |t| async move {
        const ORIGIN_BODY: &str = "https origin";
        let target = deploy_server_pod(&t, &egress_https_target("httpstarget", ORIGIN_BODY)).await;
        let (hive, actor_name, _) = create_and_resume_actor_with_egress(
            &t,
            "egress-https",
            &egress_fixture(),
            &egress_allow_all(),
        )
        .await;
        let router = must_router_client(&t).await;
        let since = log_scan_start();
        let aref = actor_ref(&actor_name);
        let url = format!("https://{}/healthz", target.svc_name());
        let (status, body) = fetch_through_egress_actor(&t, &router, &aref, &url).await;
        if status != 200 {
            t.fatal(format!(
                "Actor HTTPS egress fetch of {url} returned HTTP {status}, want 200; body: {}",
                lossy(&body)
            ));
        }
        let got = decode_egress_fetch(&t, &body);
        if got.body != ORIGIN_BODY || !got.tls {
            t.error(format!("Actor HTTPS egress fetch of {url} = {got:?}, want verified TLS and body {ORIGIN_BODY:?}"));
        }
        t.log(format!(
            "Actor HTTPS egress fetch of {url} succeeded; body: {}",
            lossy(&body)
        ));
        assert_egress_gateway_connect(&t, since, &hive, &actor_name, "443").await;
    });
}

/// The origin [`actor_egress_non_standard_port`] dials: plain HTTP on a port
/// that is neither 80 nor 443.
pub fn http_target() -> ServerPod {
    ServerPod {
        name: "httptarget".into(),
        image: Image::Testserver,
        args: vec!["http".into()],
        port: 8080,
        ..Default::default()
    }
}

/// Cleartext HTTP/1.1 egress to a port that is neither 80 nor 443. Nothing
/// in the egress path holds the port as a constant: it is the Actor's own
/// destination port, recovered after the in-sandbox redirect and written
/// verbatim into the CONNECT authority. The 80/443 tests would still pass if
/// it were defaulted from the scheme.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_non_standard_port() {
    e2e_test("TestActorEgressNonStandardPort", |t| async move {
        // Stand the target up first, so a fixture failure leaves no resumed
        // Actor idling.
        let spec = http_target();
        let target = deploy_server_pod(&t, &spec).await;
        let (hive, actor_name, _) = create_and_resume_actor_with_egress(
            &t,
            "egress-port",
            &egress_fixture(),
            &egress_allow_all(),
        )
        .await;
        let router = must_router_client(&t).await;
        let since = log_scan_start();
        // The ClusterIP literal, not the Service name: the tunnel's authority
        // is always an address, and a name would only add a dependency on the
        // sandbox's DNS path.
        let url = format!("http://{}/healthz", target.address());
        let aref = actor_ref(&actor_name);
        let (status, body) = fetch_through_egress_actor(&t, &router, &aref, &url).await;
        if status != 200 {
            t.fatal(format!(
                "Actor egress fetch of {url} returned HTTP {status}, want 200; body: {}",
                lossy(&body)
            ));
        }
        t.log(format!("Actor egress fetch of {url} succeeded"));
        assert_egress_gateway_connect(&t, since, &hive, &actor_name, &spec.port.to_string()).await;
    });
}

/// HTTP/1.1 inside TLS, with the original destination port preserved
/// through the egress tunnel.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_https_non_standard_port() {
    e2e_test("TestActorEgressHTTPSNonStandardPort", |t| async move {
        const EXPECTED_BODY: &str = "https nonstandard origin";
        let (target, address, _) = prepare_protocol_origin(
            &t,
            ServerPod {
                name: "https-origin".into(),
                image: Image::Testserver,
                args: vec!["http".into(), format!("--body={EXPECTED_BODY}")],
                port: 8080,
                tls_port: 8443,
                ..Default::default()
            },
            true,
        )
        .await;
        let (actor_name, router, aref) = prepare_protocol_actor(&t, "https-port").await;
        let since = log_scan_start();
        let raw = post_egress_once(
            &t,
            &router,
            &aref,
            "/",
            &serde_json::json!({ "url": format!("https://{address}/healthz"), "http1": true }),
        )
        .await;
        let got: ProtocolObservation = serde_json::from_slice(&raw)
            .unwrap_or_else(|e| t.fatal(format!("decoding HTTPS observation: {e}")));
        if !got.error.is_empty()
            || got.status_code != 200
            || got.body != EXPECTED_BODY
            || got.protocol != "HTTP/1.1"
            || !got.tls
        {
            t.error(format!("HTTPS observation = {got:?}; want verified TLS, HTTP/1.1, status 200 and body {EXPECTED_BODY:?}"));
        }
        assert_protocol_gateway(&t, since, &actor_name, &target.tls_address()).await;
    });
}

/// The egress demo's report of one HTTP exchange.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ProtocolObservation {
    /// The origin's status.
    #[serde(rename = "statusCode")]
    pub status_code: u16,
    /// The origin's body.
    pub body: String,
    /// The negotiated protocol.
    pub protocol: String,
    /// Whether the exchange went over verified TLS.
    pub tls: bool,
    /// What failed, if anything.
    pub error: String,
}

/// Lossy text of a body, for messages.
pub fn lossy(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// Asks the egress demo Actor to fetch `url`; returns the status and body
/// it echoes back.
pub async fn fetch_through_egress_actor(
    t: &T,
    router: &RouterClient,
    aref: &ActorRef,
    url: &str,
) -> (u16, Bytes) {
    let payload = serde_json::to_vec(&serde_json::json!({ "url": url })).unwrap_or_default();
    post_through_egress_actor(t, router, aref, "/", payload).await
}

/// The egress demo's answer to a fetch.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct EgressFetchResponse {
    /// The origin's status.
    #[serde(rename = "statusCode")]
    pub status_code: u16,
    /// The origin's body.
    pub body: String,
    /// Whether the fetch went over verified TLS.
    pub tls: bool,
    /// What failed.
    pub error: String,
}

/// Parses the body a fetch returned.
pub fn decode_egress_fetch(t: &T, body: &[u8]) -> EgressFetchResponse {
    serde_json::from_slice(body).unwrap_or_else(|e| {
        t.fatal(format!(
            "decoding the egress demo's fetch response: {e} (body {:?})",
            lossy(body)
        ))
    })
}

/// POSTs `payload` to `path` on the egress demo Actor, retrying a non-200
/// answer for up to 30 s: ResumeActor can return before the route reaches
/// the router (a transient 503), and an origin reachable but not yet
/// answering is reported as a 502.
pub async fn post_through_egress_actor(
    t: &T,
    router: &RouterClient,
    aref: &ActorRef,
    path: &str,
    payload: Vec<u8>,
) -> (u16, Bytes) {
    post_through_egress_actor_until(t, router, aref, path, payload, reached).await
}

/// [`post_through_egress_actor`] with the caller deciding which answer is
/// final.
pub async fn post_through_egress_actor_until(
    t: &T,
    router: &RouterClient,
    aref: &ActorRef,
    path: &str,
    payload: Vec<u8>,
    done: fn(u16, &[u8]) -> bool,
) -> (u16, Bytes) {
    let timeout = Duration::from_secs(30);
    let deadline = Instant::now() + timeout;
    loop {
        let resp = router
            .post_json(aref, path, payload.clone())
            .await
            .unwrap_or_else(|e| {
                t.fatal(format!("POST {path} to egress Actor through ingress: {e}"))
            });
        if done(resp.status, &resp.body) || Instant::now() > deadline {
            return (resp.status, resp.body);
        }
        t.log(format!(
            "POST {path} through egress Actor returned HTTP {}; retrying... body: {}",
            resp.status,
            resp.text()
        ));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Stops the retry loop only on success.
pub fn reached(status: u16, _: &[u8]) -> bool {
    status == 200
}

/// Stops the retry loop on anything but the 503 a request sees while the
/// Actor's route is still propagating; a denial is a final answer.
pub fn not_transient(status: u16, _: &[u8]) -> bool {
    status != 503
}

/// Waits for the egress gate's log to show a CONNECT to `port` opened by
/// `hive/actor_name`. Envoy logs the authenticated peer in its successful
/// CONNECT record.
pub async fn assert_egress_gateway_connect(
    t: &T,
    since: Timestamp,
    hive: &str,
    actor_name: &str,
    port: &str,
) {
    let want = format!("a CONNECT to port {port} by actor {hive}/{actor_name}");
    let suffix = format!(":{port}");
    let slug = format!("/worker-for-actor/{hive}/{actor_name}");
    wait_for_access_log(t, since, &want, |lines| {
        lines.iter().any(|line| match line.container.as_str() {
            "envoy" => {
                access_log_field(&line.text, "authority").is_some_and(|a| a.ends_with(&suffix))
                    && line.text.contains(&slug)
            }
            _ => false,
        })
    })
    .await;
}

/// One access-log line of an egress gate replica.
#[derive(Debug, Clone)]
pub struct GatewayAccessLogLine {
    /// The dataplane container, `envoy`.
    pub container: String,
    /// The line.
    pub text: String,
}

/// Polls every egress gate replica's access log until `predicate` accepts
/// the lines written since `since` (the log may lag the traffic).
pub async fn wait_for_access_log(
    t: &T,
    since: Timestamp,
    want: &str,
    predicate: impl Fn(&[GatewayAccessLogLine]) -> bool,
) {
    let namespace = env::system_namespace();
    let pods_api: Api<Pod> = Api::namespaced(clients().kube(), &namespace);
    let pods = pods_api
        .list(&ListParams::default().labels(install::EGRESS_APP_LABEL))
        .await
        .unwrap_or_else(|e| {
            t.fatal(format!(
                "listing {} pods in {namespace}: {e}",
                install::EGRESS_APP_LABEL
            ))
        });
    if pods.items.is_empty() {
        t.fatal(format!(
            "no {} pods in {namespace}; the egress gateway is not deployed",
            install::EGRESS_APP_LABEL
        ));
    }
    let timeout = Duration::from_secs(30);
    let deadline = Instant::now() + timeout;
    loop {
        let mut lines = Vec::new();
        for pod in &pods.items {
            let name = pod.metadata.name.clone().unwrap_or_default();
            let containers = pod
                .spec
                .as_ref()
                .map(|s| s.containers.clone())
                .unwrap_or_default();
            let Some(container) = containers
                .iter()
                .map(|c| c.name.clone())
                .find(|n| n == "envoy")
            else {
                t.fatal(format!("egress gateway pod {name} has no Envoy container"));
            };
            let params = LogParams {
                container: Some(container.clone()),
                since_time: Some(since),
                ..Default::default()
            };
            let logs = pods_api
                .logs(&name, &params)
                .await
                .unwrap_or_else(|e| t.fatal(format!("reading logs of {namespace}/{name}: {e}")));
            for line in logs.split('\n') {
                let keep = container == "envoy" && line.contains("[egress] ");
                if keep {
                    lines.push(GatewayAccessLogLine {
                        container: container.clone(),
                        text: line.to_string(),
                    });
                }
            }
        }
        if predicate(&lines) {
            return;
        }
        if Instant::now() > deadline {
            let seen: Vec<&str> = lines.iter().map(|l| l.text.as_str()).collect();
            t.fatal(format!(
                "no gate-egress access-log line for {want} after {timeout:?}; lines seen:\n{}",
                seen.join("\n")
            ));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// The value of the space-separated `key=value` field `key` of an access-log
/// line, unquoted. Keys match whole.
pub fn access_log_field(line: &str, key: &str) -> Option<String> {
    line.split_whitespace().find_map(|field| {
        let (k, v) = field.split_once('=')?;
        (k == key).then(|| v.trim_matches('"').to_string())
    })
}

/// Creates an Actor from `template`, gives it an EgressPolicy of exactly
/// `rules` (none leaves it without one) and resumes it.
pub async fn create_and_resume_actor_with_egress(
    t: &T,
    prefix: &str,
    template: &Fixture,
    rules: &[pb::EgressRule],
) -> (String, String, pb::Actor) {
    let actor = pb::Actor {
        actor_template: Some(pb::ObjectRef {
            hive: template.namespace.clone(),
            name: template.name.clone(),
        }),
        ..Default::default()
    };
    create_and_resume(
        t,
        prefix,
        actor,
        &format!("{}/{}", template.namespace, template.name),
        &template.deploy_with,
        rules,
    )
    .await
}

/// [`create_and_resume_actor_with_egress`] for an ActorTemplate fixture,
/// with an allow-everything policy.
pub async fn create_and_resume_hive_actor(
    t: &T,
    prefix: &str,
    template: &HiveFixture,
) -> (String, String, pb::Actor) {
    let actor = pb::Actor {
        actor_template: Some(pb::ObjectRef {
            hive: template.hive.clone(),
            name: template.name.clone(),
        }),
        ..Default::default()
    };
    create_and_resume(
        t,
        prefix,
        actor,
        &format!("{}/{}", template.hive, template.name),
        &template.deploy_with,
        &egress_allow_all(),
    )
    .await
}

/// Creates the Actor, gives it a policy of `rules` (none when empty) before
/// the resume so its first outbound connection already finds it, and
/// resumes it.
async fn create_and_resume(
    t: &T,
    prefix: &str,
    mut actor: pb::Actor,
    source: &str,
    deploy_with: &str,
    rules: &[pb::EgressRule],
) -> (String, String, pb::Actor) {
    let c = clients();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let actor_name = format!("{prefix}-{nanos}");
    let oref = pb::ObjectRef {
        hive: NETWORKING_HIVE.into(),
        name: actor_name.clone(),
    };
    t.log(format!("creating actor {NETWORKING_HIVE}/{actor_name}"));
    let _ = c
        .api()
        .create_hive(pb::CreateHiveRequest {
            hive: Some(pb::Hive {
                metadata: Some(pb::ResourceMetadata {
                    name: NETWORKING_HIVE.into(),
                    ..Default::default()
                }),
            }),
        })
        .await;
    actor.metadata = Some(pb::ResourceMetadata {
        hive: NETWORKING_HIVE.into(),
        name: actor_name.clone(),
        ..Default::default()
    });
    if let Err(e) = c
        .api()
        .create_actor(pb::CreateActorRequest { actor: Some(actor) })
        .await
    {
        t.fatal(format!(
            "CreateActor from {source}: {e} (deploy the fixture with {deploy_with})"
        ));
    }
    let mut api = c.api();
    let r = oref.clone();
    t.cleanup(move |_| async move {
        let _ = api
            .suspend_actor(pb::SuspendActorRequest {
                actor: Some(r.clone()),
                ..Default::default()
            })
            .await;
        let _ = api
            .delete_actor(pb::DeleteActorRequest {
                actor: Some(r),
                ..Default::default()
            })
            .await;
    });
    if !rules.is_empty() {
        ensure_egress_policy(t, &c, &oref, rules.to_vec()).await;
    }
    let resp =
        retry::resume_actor_await_capacity(t, &c, pb::ResumeActorRequest { actor: Some(oref) })
            .await
            .unwrap_or_else(|e| t.fatal(format!("ResumeActor: {e}")));
    t.log(format!("resumed actor {NETWORKING_HIVE}/{actor_name}"));
    let actor = resp.actor.unwrap_or_default();
    let m = actor.metadata.clone().unwrap_or_default();
    (m.hive, m.name, actor)
}

/// A router client, or a failed test.
pub async fn must_router_client(t: &T) -> RouterClient {
    RouterClient::new(t, &clients())
        .await
        .unwrap_or_else(|e| t.fatal(format!("NewRouterClient: {e}")))
}

/// Retries `request` until it answers 200 (30 s budget) and returns that
/// body. This rides out the window between ResumeActor returning and the
/// route reaching the router, where requests see a transient 503.
pub async fn wait_for_route_ready<F, Fut>(t: &T, what: &str, mut request: F) -> String
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<RouterResponse, String>>,
{
    let timeout = Duration::from_secs(30);
    let deadline = Instant::now() + timeout;
    loop {
        let resp = request()
            .await
            .unwrap_or_else(|e| t.fatal(format!("{what}: {e}")));
        if resp.status == 200 {
            return resp.text();
        }
        if Instant::now() > deadline {
            t.fatal(format!(
                "{what} returned HTTP {} after {timeout:?}; body: {}",
                resp.status,
                resp.text()
            ));
        }
        t.log(format!("{what} returned HTTP {}; retrying...", resp.status));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// The API server's pod proxy dials the assigned worker's port 80 from
/// inside the cluster, bypassing the router: it must not get through, which
/// shows the direct path stays closed without needing a route to the pod
/// network from here.
async fn assert_direct_actor_access(t: &T, actor: &pb::Actor) {
    let wa = actor
        .status
        .as_ref()
        .and_then(|s| s.worker_assignment.clone())
        .unwrap_or_default();
    if wa.worker_namespace.is_empty() || wa.worker_pod.is_empty() {
        t.fatal(format!(
            "resumed Actor has no worker pod assignment: {actor:?}"
        ));
    }
    let path = format!(
        "/api/v1/namespaces/{}/pods/{}:80/proxy/readyz",
        wa.worker_namespace, wa.worker_pod
    );
    let req = http::Request::get(path).body(Vec::new()).expect("request");
    match clients().kube().request_text(req).await {
        Ok(body) => t.fatal(format!(
            "direct Actor access through {}/{}:80 unexpectedly succeeded; body: {body}",
            wa.worker_namespace, wa.worker_pod
        )),
        Err(e) => t.log(format!(
            "direct Actor access through {}/{}:80 was blocked as expected: {e}",
            wa.worker_namespace, wa.worker_pod
        )),
    }
}

/// Stands up `spec` in a fresh namespace and returns it, the host:port an
/// Actor fetches it at, and (when `encrypted`) the PEM root its certificate
/// chains to. An encrypted origin gets a certificate for its reserved
/// Service IP and DNS name from the service-DNS CA, which the egress gate
/// trusts for in-cluster origins.
pub async fn prepare_protocol_origin(
    t: &T,
    mut spec: ServerPod,
    encrypted: bool,
) -> (Server, String, String) {
    spec.namespace = hivekeep_e2e::namespace::create_namespace(t).await.name;
    register_origin_diagnostics(t, &spec.namespace, &spec.name);
    let mut reserved = None;
    let mut root_ca = String::new();
    if encrypted {
        if spec.tls_port == 0 {
            t.fatal(format!(
                "encrypted origin {} sets no tls_port to publish its HTTPS listener on",
                spec.name
            ));
        }
        let r = create_server_service(t, &spec).await;
        let secret = Api::<Secret>::namespaced(clients().kube(), SERVICE_DNS_CA_NAMESPACE)
            .get(SERVICE_DNS_CA_SECRET)
            .await
            .unwrap_or_else(|e| t.fatal(format!("reading service-DNS CA pool: {e}")));
        let pool_bytes = secret
            .data
            .unwrap_or_default()
            .get("pool")
            .map(|b| b.0.clone())
            .unwrap_or_default();
        let pool = hivekeep_identity::ca::CaPool::from_json(&pool_bytes)
            .unwrap_or_else(|e| t.fatal(format!("parsing service-DNS CA pool: {e}")));
        let Some(ca) = pool.cas().first() else {
            t.fatal("service-DNS CA pool is empty")
        };
        let ip: IpAddr = r
            .cluster_ip
            .parse()
            .unwrap_or_else(|e| t.fatal(format!("origin Service IP {:?}: {e}", r.cluster_ip)));
        let dns = format!("{}.{}.svc.cluster.local", spec.name, spec.namespace);
        let material = testcert::server_tls_with_ca(ca, ip, &[dns.as_str()]);
        root_ca = lossy(&material.root_ca);
        let mut secret = Secret {
            type_: Some("kubernetes.io/tls".into()),
            data: Some(
                [
                    ("tls.crt".to_string(), ByteString(material.certificate)),
                    ("tls.key".to_string(), ByteString(material.private_key)),
                ]
                .into(),
            ),
            ..Default::default()
        };
        secret.metadata.name = Some("origin-tls".into());
        secret.metadata.namespace = Some(spec.namespace.clone());
        Api::<Secret>::namespaced(clients().kube(), &spec.namespace)
            .create(&PostParams::default(), &secret)
            .await
            .unwrap_or_else(|e| t.fatal(format!("creating origin credentials: {e}")));
        spec.volumes = vec![Volume {
            name: "tls".into(),
            secret: Some(SecretVolumeSource {
                secret_name: Some("origin-tls".into()),
                ..Default::default()
            }),
            ..Default::default()
        }];
        spec.volume_mounts = vec![VolumeMount {
            name: "tls".into(),
            mount_path: "/run/origin-tls".into(),
            read_only: Some(true),
            ..Default::default()
        }];
        spec.args.push("--tls-cert=/run/origin-tls/tls.crt".into());
        spec.args.push("--tls-key=/run/origin-tls/tls.key".into());
        reserved = Some(r);
    }
    let target = deploy_server_pod(t, &spec).await;
    if let Some(r) = &reserved {
        if target.cluster_ip != r.cluster_ip {
            t.fatal(format!(
                "Service IP changed from {} to {} after certificate issuance",
                r.cluster_ip, target.cluster_ip
            ));
        }
    }
    wait_for_server_endpoint(t, &spec).await;
    let family = match target.cluster_ip.parse::<IpAddr>() {
        Ok(IpAddr::V4(_)) => "IPv4",
        _ => "IPv6",
    };
    let dialed = if encrypted {
        target.tls_address()
    } else {
        target.address()
    };
    t.log(format!(
        "origin {}/{} at {dialed} ({family}), TLS={encrypted}",
        target.namespace, spec.name
    ));
    let address = protocol_origin_request(&target);
    (target, address, root_ca)
}

const ORIGIN_DIAGNOSTIC_LOG_LINES: usize = 80;
const ORIGIN_DIAGNOSTIC_LOG_BYTES: usize = 16 << 10;
const ORIGIN_DIAGNOSTIC_FIELD_BYTES: usize = 1024;
const ORIGIN_DIAGNOSTIC_ITEMS: usize = 20;

/// Collects the origin's state into the log if the test fails.
fn register_origin_diagnostics(t: &T, namespace: &str, name: &str) {
    let (ns, n) = (namespace.to_string(), name.to_string());
    t.cleanup(move |t| async move {
        if !t.failed() {
            return;
        }
        let _ = tokio::time::timeout(
            Duration::from_secs(20),
            collect_origin_diagnostics(&t, &ns, &n),
        )
        .await;
    });
}

async fn collect_origin_diagnostics(t: &T, namespace: &str, name: &str) {
    let kube = clients().kube();
    t.log(format!(
        "origin diagnostics observed_at={} namespace={namespace} name={name}",
        Timestamp::now()
    ));
    let pods: Api<Pod> = Api::namespaced(kube.clone(), namespace);
    match pods.get(name).await {
        Err(e) => t.log(format!(
            "origin diagnostics: get Pod {namespace}/{name}: {e}"
        )),
        Ok(pod) => {
            let st = pod.status.clone().unwrap_or_default();
            let ts = |v: Option<&k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>| {
                v.map(|t| t.0.to_string()).unwrap_or_default()
            };
            let containers: Vec<String> = st
                .container_statuses
                .clone()
                .unwrap_or_default()
                .iter()
                .take(ORIGIN_DIAGNOSTIC_ITEMS)
                .map(|c| {
                    bounded_origin_diagnostic_field(&format!(
                        "{} ready={} restarts={}",
                        c.name, c.ready, c.restart_count
                    ))
                })
                .collect();
            t.log(format!(
                "origin Pod status: uid={} phase={} pod_ip={} host_ip={} reason={} message={:?} created={} deletion={} containers={}",
                pod.metadata.uid.clone().unwrap_or_default(),
                st.phase.unwrap_or_default(),
                st.pod_ip.unwrap_or_default(),
                st.host_ip.unwrap_or_default(),
                st.reason.unwrap_or_default(),
                bounded_origin_diagnostic_field(&st.message.unwrap_or_default()),
                ts(pod.metadata.creation_timestamp.as_ref()),
                ts(pod.metadata.deletion_timestamp.as_ref()),
                containers.join("; "),
            ));
            for previous in [false, true] {
                let params = LogParams {
                    previous,
                    tail_lines: Some(ORIGIN_DIAGNOSTIC_LOG_LINES as i64),
                    limit_bytes: Some(ORIGIN_DIAGNOSTIC_LOG_BYTES as i64),
                    timestamps: true,
                    ..Default::default()
                };
                let label = if previous { "previous" } else { "current" };
                match pods.logs(name, &params).await {
                    Err(e) => t.log(format!("origin {label} logs unavailable: {e}")),
                    Ok(logs) => t.log(format!(
                        "origin {label} logs (bounded):\n{}",
                        bounded_origin_diagnostic_text(
                            &logs,
                            ORIGIN_DIAGNOSTIC_LOG_LINES,
                            ORIGIN_DIAGNOSTIC_LOG_BYTES
                        )
                    )),
                }
            }
        }
    }
    match Api::<Service>::namespaced(kube.clone(), namespace)
        .get(name)
        .await
    {
        Err(e) => t.log(format!(
            "origin diagnostics: get Service {namespace}/{name}: {e}"
        )),
        Ok(svc) => {
            let spec = svc.spec.unwrap_or_default();
            let ports: Vec<String> = spec
                .ports
                .unwrap_or_default()
                .iter()
                .take(ORIGIN_DIAGNOSTIC_ITEMS)
                .map(|p| {
                    format!(
                        "{}/{}->{:?}/{}",
                        p.name.clone().unwrap_or_default(),
                        p.port,
                        p.target_port,
                        p.protocol.clone().unwrap_or_default()
                    )
                })
                .collect();
            let mut selector: Vec<String> = spec
                .selector
                .unwrap_or_default()
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}={}",
                        bounded_origin_diagnostic_field(k),
                        bounded_origin_diagnostic_field(v)
                    )
                })
                .collect();
            selector.sort();
            selector.truncate(ORIGIN_DIAGNOSTIC_ITEMS);
            t.log(format!(
                "origin Service: cluster_ip={} ports={} selector={}",
                spec.cluster_ip.unwrap_or_default(),
                bounded_origin_diagnostic_field(&ports.join(",")),
                bounded_origin_diagnostic_field(&selector.join(","))
            ));
        }
    }
    let slices = Api::<EndpointSlice>::namespaced(kube.clone(), namespace)
        .list(
            &ListParams::default()
                .labels(&format!("kubernetes.io/service-name={name}"))
                .limit(20),
        )
        .await;
    match slices {
        Err(e) => t.log(format!(
            "origin diagnostics: list EndpointSlices for {namespace}/{name}: {e}"
        )),
        Ok(list) => {
            for s in list.items {
                let ports: Vec<String> = s
                    .ports
                    .unwrap_or_default()
                    .iter()
                    .take(ORIGIN_DIAGNOSTIC_ITEMS)
                    .map(|p| {
                        format!(
                            "{}/{}/{}",
                            bounded_origin_diagnostic_field(&p.name.clone().unwrap_or_default()),
                            p.port.unwrap_or_default(),
                            p.protocol.clone().unwrap_or_default()
                        )
                    })
                    .collect();
                let endpoints: Vec<String> = s
                    .endpoints
                    .unwrap_or_default()
                    .iter()
                    .take(ORIGIN_DIAGNOSTIC_ITEMS)
                    .map(|e| {
                        let c = e.conditions.clone().unwrap_or_default();
                        let fmt_b = |b: Option<bool>| {
                            b.map(|v| v.to_string()).unwrap_or_else(|| "unknown".into())
                        };
                        let uid = e
                            .target_ref
                            .as_ref()
                            .and_then(|r| r.uid.clone())
                            .unwrap_or_default();
                        let addrs: Vec<String> = e
                            .addresses
                            .iter()
                            .take(ORIGIN_DIAGNOSTIC_ITEMS)
                            .map(|a| bounded_origin_diagnostic_field(a))
                            .collect();
                        bounded_origin_diagnostic_field(&format!(
                            "addresses={} uid={uid} ready={} terminating={}",
                            addrs.join(","),
                            fmt_b(c.ready),
                            fmt_b(c.terminating)
                        ))
                    })
                    .collect();
                t.log(format!(
                    "origin EndpointSlice: name={} uid={} ports={} endpoints={}",
                    s.metadata.name.unwrap_or_default(),
                    s.metadata.uid.unwrap_or_default(),
                    bounded_origin_diagnostic_field(&ports.join(",")),
                    endpoints.join("; ")
                ));
            }
        }
    }
    match Api::<Event>::namespaced(kube, namespace)
        .list(&ListParams::default().limit(50))
        .await
    {
        Err(e) => t.log(format!(
            "origin diagnostics: list namespace events {namespace}: {e}"
        )),
        Ok(events) => {
            for ev in events
                .items
                .iter()
                .filter(|e| e.involved_object.name.as_deref() == Some(name))
            {
                t.log(format!(
                    "origin event: type={} reason={} message={:?} count={}",
                    ev.type_.clone().unwrap_or_default(),
                    ev.reason.clone().unwrap_or_default(),
                    bounded_origin_diagnostic_field(&ev.message.clone().unwrap_or_default()),
                    ev.count.unwrap_or_default()
                ));
            }
        }
    }
}

/// The last `max_lines` lines of `value`, then its last `max_bytes` bytes.
pub fn bounded_origin_diagnostic_text(value: &str, max_lines: usize, max_bytes: usize) -> String {
    let lines: Vec<&str> = value.split('\n').collect();
    let start = lines.len().saturating_sub(max_lines);
    let result = lines[start..].join("\n");
    if result.len() > max_bytes {
        let mut cut = result.len() - max_bytes;
        while !result.is_char_boundary(cut) {
            cut += 1;
        }
        return result[cut..].to_string();
    }
    result
}

fn bounded_origin_diagnostic_field(value: &str) -> String {
    if value.len() <= ORIGIN_DIAGNOSTIC_FIELD_BYTES {
        return value.to_string();
    }
    let mut cut = ORIGIN_DIAGNOSTIC_FIELD_BYTES - 3;
    while !value.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}...", &value[..cut])
}

/// The host:port an Actor fetches `target` at: its DNS name (the gate needs
/// it for SNI), on its TLS port if it has one. The CONNECT authority in the
/// gate's log is the ClusterIP on the same port.
pub fn protocol_origin_request(target: &Server) -> String {
    let port = if target.tls_port != 0 {
        target.tls_port
    } else {
        target.port
    };
    format!(
        "{}.{}.svc.cluster.local:{port}",
        target.name, target.namespace
    )
}

#[test]
fn protocol_origin_request_cases() {
    for (name, ip, tls_port, want) in [
        ("IPv4", "10.0.0.7", 0, "origin.test.svc.cluster.local:8080"),
        ("IPv6", "fd00::7", 0, "origin.test.svc.cluster.local:8080"),
        (
            "TLS",
            "10.0.0.7",
            8443,
            "origin.test.svc.cluster.local:8443",
        ),
    ] {
        let target = Server {
            name: "origin".into(),
            namespace: "test".into(),
            cluster_ip: ip.into(),
            port: 8080,
            tls_port,
        };
        assert_eq!(protocol_origin_request(&target), want, "{name}");
    }
}

#[test]
fn bounded_origin_diagnostic_text_cases() {
    assert_eq!(
        bounded_origin_diagnostic_text("one\ntwo\nthree\nfour", 2, 16),
        "three\nfour"
    );
    assert_eq!(bounded_origin_diagnostic_text("123456789", 10, 4), "6789");
    let long = "x".repeat(2000);
    assert_eq!(
        bounded_origin_diagnostic_field(&long).len(),
        ORIGIN_DIAGNOSTIC_FIELD_BYTES
    );
}

/// An egress demo Actor allowed everything plus HTTPS on 8443, with its
/// route ready, and a router client.
pub async fn prepare_protocol_actor(t: &T, prefix: &str) -> (String, RouterClient, ActorRef) {
    let mut https8443 = egress_allow_https(&["*"]);
    if let Some(h) = https8443.https.as_mut() {
        h.ports = Some(pb::Ports {
            all: None,
            numbers: vec![8443],
        });
    }
    let mut rules = egress_allow_all();
    rules.push(https8443);
    let (_, actor_name, _) =
        create_and_resume_actor_with_egress(t, prefix, &egress_fixture(), &rules).await;
    let router = must_router_client(t).await;
    let aref = actor_ref(&actor_name);
    wait_for_route_ready(t, "egress actor ingress readiness", || {
        router.get(&aref, "/readyz")
    })
    .await;
    (actor_name, router, aref)
}

/// Starts the measured operation once the route is ready; unlike
/// [`post_through_egress_actor`], a failure is never retried.
pub async fn post_egress_once(
    t: &T,
    router: &RouterClient,
    aref: &ActorRef,
    path: &str,
    input: &serde_json::Value,
) -> Bytes {
    post_egress_with_status(t, router, aref, path, input, 200).await
}

/// [`post_egress_once`] expecting `want_status`.
pub async fn post_egress_with_status(
    t: &T,
    router: &RouterClient,
    aref: &ActorRef,
    path: &str,
    input: &serde_json::Value,
    want_status: u16,
) -> Bytes {
    let payload = serde_json::to_vec(input)
        .unwrap_or_else(|e| t.fatal(format!("encoding actor operation: {e}")));
    let resp = router
        .post_json(aref, path, payload)
        .await
        .unwrap_or_else(|e| {
            t.fatal(format!(
                "actor {} operation {path} transport failed: {e}",
                aref.name
            ))
        });
    if resp.body.len() > 1 << 20 {
        t.fatal("actor operation response exceeds 1 MiB");
    }
    if resp.status != want_status {
        t.fatal(format!(
            "actor {} operation {path} returned HTTP {}, want {want_status}: {}",
            aref.name,
            resp.status,
            resp.text()
        ));
    }
    resp.body
}

/// Waits for gate-log evidence of successful traffic from `actor_name`'s
/// tunnel identity to `authority`.
pub async fn assert_protocol_gateway(t: &T, since: Timestamp, actor_name: &str, authority: &str) {
    let aref = actor_ref(actor_name);
    let identity = worker_for_actor_spiffe_id(&aref);
    wait_for_access_log(
        t,
        since,
        &format!("successful gateway traffic for {identity} to {authority}"),
        |lines| {
            lines.iter().any(|entry| {
                let ok = protocol_gateway_access_log_matches(entry, &aref, authority);
                if ok {
                    t.log(format!("gateway evidence: {}", entry.text));
                }
                ok
            })
        },
    )
    .await;
}

/// Whether `entry` records successful traffic of `aref`'s tunnel to
/// `authority`. Envoy records the authority, status and peer SAN.
pub fn protocol_gateway_access_log_matches(
    entry: &GatewayAccessLogLine,
    aref: &ActorRef,
    authority: &str,
) -> bool {
    let field = |k: &str| access_log_field(&entry.text, k);
    match entry.container.as_str() {
        "envoy" => {
            if field("authority").as_deref() != Some(authority)
                || field("code").as_deref() != Some("200")
            {
                return false;
            }
            let want = worker_for_actor_spiffe_id(aref);
            field("peer_san")
                .unwrap_or_default()
                .split(',')
                .any(|p| p == want)
        }
        _ => false,
    }
}

/// Polls the demo's readiness endpoint through the router until it answers
/// (30 s), which is when the Actor's route has reached the ingress
/// dataplane.
pub async fn wait_for_actor_route(t: &T, router: &RouterClient, aref: &ActorRef) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let resp = router.get(aref, "/readyz").await.unwrap_or_else(|e| {
            t.fatal(format!(
                "GET /readyz on the egress Actor through ingress: {e}"
            ))
        });
        if resp.status == 200 {
            return;
        }
        if Instant::now() > deadline {
            t.fatal(format!(
                "the egress Actor's route did not come up: GET /readyz returned HTTP {}",
                resp.status
            ));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// The snapshot bucket from the environment, or a failed test.
pub fn bucket(t: &T) -> String {
    env::check_env(&[BUCKET_ENV])
        .map(|m| m[BUCKET_ENV].clone())
        .unwrap_or_else(|e| t.fatal(format!("CheckEnv failed: {e}")))
}

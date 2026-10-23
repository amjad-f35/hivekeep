//! Ingress to ports other than an Actor's primary one, over CONNECT with
//! the target port in the CONNECT authority.
//!
//! Establishing the CONNECT succeeds as soon as the router hands the tunnel
//! to its internal listener, without touching the Actor, so it proves
//! nothing by itself: every case sends a real request through the tunnel
//! and inspects that response, which is where Actor resolution and the dial
//! to the Actor's pod happen.

use std::sync::LazyLock;
use std::time::{Duration, Instant};

use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::install;
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::sandbox::counter_fixture;
use hivekeep_e2e::suite::e2e_test;
use http::{HeaderMap, HeaderName, HeaderValue};
use http_body_util::Empty;
use hyper_util::rt::TokioIo;
use regex::Regex;
use tokio::io::{AsyncWriteExt, BufReader};

use crate::httpwire::{
    get_through_tunnel, h2_handshake, read_response, request_tunneled, router_tls,
};
use crate::networking::{
    actor_ref, create_and_resume_hive_actor, must_router_client, wait_for_route_ready,
};

/// The counter demo's second listener, beside its primary port 80.
pub const COUNTER_EXTRA_PORT: u16 = 9090;

/// Matches the `hello from: <IP> | ...` answer of the counter's port 80.
static COUNTER_DEFAULT_PORT_IP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^hello from: (\S+)").expect("static regex"));

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_arbitrary_port_access() {
    e2e_test("TestActorArbitraryPortAccess", |t| async move {
        let (_, actor_name, _) =
            create_and_resume_hive_actor(&t, "arbitraryport", &counter_fixture()).await;
        let aref = actor_ref(&actor_name);
        // A tunnel ending in an error ends its whole forward, so later
        // subtests open their own clients.
        let router = must_router_client(&t).await;
        let (r, a) = (&router, &aref);

        t.run("regression: default port still reachable", |t| async move {
            let body = wait_for_route_ready(&t, "default-port access through ingress", || {
                r.get(a, "/readyz")
            })
            .await;
            t.log(format!(
                "default-port access through ingress succeeded; body: {body}"
            ));
        })
        .await;

        t.run("arbitrary port reachable", |t| async move {
            let body = wait_for_tunneled_route_ready(&t, r, a, COUNTER_EXTRA_PORT).await;
            if !body.contains("extra port 9090") {
                t.fatal(format!("tunneled response body = {body:?}, want it to mention extra port 9090"));
            }
            // Both ports self-report the sandbox's own address (not the pod
            // IP, but the same for both ports of one Actor), so a match shows
            // the tunnel reached this Actor's extra port.
            let default = r.get(a, "/").await.unwrap_or_else(|e| t.fatal(format!("GET the actor's default port through ingress: {e}")));
            let want_ip = self_reported_ip(&t, &default.text());
            if !body.contains(&want_ip) {
                t.fatal(format!("tunneled response body = {body:?}, want it to mention the same self-reported IP as the default port ({want_ip}): {:?}", default.text()));
            }
            t.log(format!("arbitrary-port access through CONNECT succeeded; body: {body}"));
        })
        .await;

        t.run("tunnels to different ports do not cross", |t| async move {
            // Two streams on one HTTP/2 connection land on the same proxy
            // worker, and the held tunnel keeps its upstream open while the
            // second one opens.
            let work = async {
                let router = must_router_client(&t).await;
                let forward = router.connect_port_forward().await.unwrap_or_else(|e| t.fatal(e));
                // The CONNECT listener offers no ALPN: HTTP/2 by prior
                // knowledge over the TLS connection.
                let tls = router_tls(&t, &router, &forward.addr(), &[]).await;
                let mut cc = h2_handshake(&t, tls).await;
                let mut held = connect_h2(&t, &mut cc, a, 80).await;
                let body = get_through_tunnel(&t, &mut held, &a.name).await;
                if !COUNTER_DEFAULT_PORT_IP.is_match(&body) {
                    t.fatal(format!("tunnel to port 80 got {body:?}, want the default port's response"));
                }
                let mut extra = connect_h2(&t, &mut cc, a, COUNTER_EXTRA_PORT).await;
                let body = get_through_tunnel(&t, &mut extra, &a.name).await;
                if !body.contains("extra port 9090") {
                    t.fatal(format!("tunnel to port {COUNTER_EXTRA_PORT} got {body:?}, want the extra port's response"));
                }
                drop(held);
            };
            tokio::time::timeout(Duration::from_secs(60), work).await.unwrap_or_else(|_| t.fatal("timed out"));
        })
        .await;

        t.run("unlisted port rejected", |t| async move {
            // Nothing listens here. The tunnel still establishes, so the
            // failure must show on the request through it: an HTTP error, or
            // a reset, never a hang or a 200.
            const UNLISTED_PORT: u16 = 6553;
            let router = must_router_client(&t).await;
            let mut conn = router
                .connect(a, UNLISTED_PORT)
                .await
                .unwrap_or_else(|e| t.fatal(format!("CONNECT to an unlisted port unexpectedly failed to establish: {e}")));
            let req = format!("GET / HTTP/1.1\r\nHost: {}\r\n{}: {a}\r\nConnection: close\r\n\r\n", a.name, install::TARGET_ACTOR_HEADER);
            let exchange = async {
                conn.write_all(req.as_bytes()).await.map_err(|e| format!("writing tunneled request: {e}"))?;
                conn.flush().await.map_err(|e| e.to_string())?;
                read_response(&mut BufReader::new(&mut conn)).await
            };
            match tokio::time::timeout(Duration::from_secs(10), exchange).await {
                Ok(Ok(resp)) if resp.status == 200 => t.fatal(format!("tunneled request to an unlisted port unexpectedly returned HTTP 200; body: {}", resp.text())),
                Ok(Ok(resp)) => t.log(format!("tunneled request to an unlisted port correctly returned HTTP {}; body: {}", resp.status, resp.text())),
                Ok(Err(e)) => t.log(format!("tunneled request to an unlisted port correctly failed to read a response: {e}")),
                Err(_) => t.log("tunneled request to an unlisted port correctly failed to read a response: deadline exceeded"),
            }
        })
        .await;

        t.run("duplicate actor header rejected on CONNECT", |t| async move {
            let mut headers = HeaderMap::new();
            let h = HeaderName::from_static(install::TARGET_ACTOR_HEADER);
            headers.append(h.clone(), HeaderValue::from_str(&a.to_string()).expect("header"));
            headers.append(h, HeaderValue::from_static("untrusted/actor"));
            let router = must_router_client(&t).await;
            let mut conn = match router.connect_with_headers(a, COUNTER_EXTRA_PORT, &headers).await {
                Ok(c) => c,
                Err(e) => {
                    // A refused CONNECT is a valid rejection.
                    t.log(format!("CONNECT with duplicate headers rejected: {e}"));
                    return;
                }
            };
            match request_tunneled(&mut conn, &a.name).await {
                Err(e) => t.log(format!("tunneled request with duplicate CONNECT headers correctly failed: {e}")),
                Ok(resp) if resp.status != 404 => {
                    t.fatal(format!("request through CONNECT with duplicate actor headers returned {}, want 404; body: {}", resp.status, resp.text()))
                }
                Ok(_) => {}
            }
        })
        .await;
    });
}

/// Retries a whole CONNECT exchange while the route propagates. A tunnel is
/// tied to one upstream connection, so each retry opens a fresh one.
async fn wait_for_tunneled_route_ready(
    t: &T,
    router: &RouterClient,
    aref: &ActorRef,
    port: u16,
) -> String {
    let timeout = Duration::from_secs(30);
    let deadline = Instant::now() + timeout;
    loop {
        let (last_err, last) = match router.connect(aref, port).await {
            Ok(mut conn) => match request_tunneled(&mut conn, &aref.name).await {
                Ok(resp) if resp.status == 200 => return resp.text(),
                Ok(resp) => (None, Some(resp)),
                Err(e) => (Some(e), None),
            },
            Err(e) => (Some(e), None),
        };
        if Instant::now() > deadline {
            match (last_err, last) {
                (Some(e), _) => t.fatal(format!("tunneled request to port {port} did not become ready within {timeout:?}: {e}")),
                (None, Some(resp)) => t.fatal(format!("tunneled request to port {port} returned HTTP {} after {timeout:?}, want 200; body: {}", resp.status, resp.text())),
                (None, None) => t.fatal(format!("tunneled request to port {port} did not become ready within {timeout:?}")),
            }
        }
        match (&last_err, &last) {
            (Some(e), _) => t.log(format!(
                "tunneled request to port {port} failed: {e}; retrying..."
            )),
            (None, Some(resp)) => t.log(format!(
                "tunneled request to port {port} returned HTTP {}; retrying...",
                resp.status
            )),
            _ => {}
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Opens a CONNECT tunnel to `port` on `aref` as a new stream of `cc`.
async fn connect_h2(
    t: &T,
    cc: &mut hyper::client::conn::http2::SendRequest<Empty<bytes::Bytes>>,
    aref: &ActorRef,
    port: u16,
) -> TokioIo<hyper::upgrade::Upgraded> {
    let destination = format!("{}:{port}", aref.name);
    let req = http::Request::connect(destination.as_str())
        .header(install::TARGET_ACTOR_HEADER, aref.to_string())
        .body(Empty::new())
        .unwrap_or_else(|e| t.fatal(format!("building CONNECT: {e}")));
    cc.ready()
        .await
        .unwrap_or_else(|e| t.fatal(format!("CONNECT to port {port}: {e}")));
    let resp = cc
        .send_request(req)
        .await
        .unwrap_or_else(|e| t.fatal(format!("CONNECT to port {port}: {e}")));
    if resp.status() != 200 {
        t.fatal(format!(
            "CONNECT to port {port} returned HTTP {}",
            resp.status()
        ));
    }
    let upgraded = hyper::upgrade::on(resp)
        .await
        .unwrap_or_else(|e| t.fatal(format!("CONNECT to port {port}: upgrading the stream: {e}")));
    TokioIo::new(upgraded)
}

/// The address the counter reported in a default-port body.
fn self_reported_ip(t: &T, body: &str) -> String {
    COUNTER_DEFAULT_PORT_IP
        .captures(body)
        .map(|c| c[1].to_string())
        .unwrap_or_else(|| {
            t.fatal(format!(
                "default-port response body = {body:?}, want it to match {:?}",
                COUNTER_DEFAULT_PORT_IP.as_str()
            ))
        })
}

#[test]
fn counter_default_port_pattern() {
    let c = COUNTER_DEFAULT_PORT_IP
        .captures("hello from: 10.1.2.3 | count 4")
        .unwrap();
    assert_eq!(&c[1], "10.1.2.3");
    assert!(!COUNTER_DEFAULT_PORT_IP.is_match("extra port 9090"));
}

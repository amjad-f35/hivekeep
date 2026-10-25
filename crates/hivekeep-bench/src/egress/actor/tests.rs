//! End-to-end tests of the actor's loop against local endpoints: real
//! sockets for connection and TLS behaviour, in-memory pipes on a paused
//! clock for the schedule.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::RootCertStore;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;

use super::api::*;
use super::client::{BoxStream, DialFn, Trace};
use super::endpoints::{endpoint_host, endpoint_url, ENDPOINT_DOMAIN};
use super::hist::{Distribution, BOUNDS};
use super::run::LoopRun;
use super::server::{ActorServer, Answer};
use super::stats::{Endpoint, Stats};
use crate::egress::targetcert;

const MS: Duration = Duration::from_millis(1);

// ---------------------------------------------------------------- targets

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// An endpoint's behaviour: given the request's Host, the status to answer
/// with, or `None` to hold the request until the client gives up.
type Handler = Arc<dyn Fn(String) -> BoxFut<Option<u16>> + Send + Sync>;

fn ok_handler() -> Handler {
    Arc::new(|_| Box::pin(async { Some(200) }))
}

fn status_handler(code: u16) -> Handler {
    Arc::new(move |_| Box::pin(async move { Some(code) }))
}

fn hang_handler() -> Handler {
    Arc::new(|_| Box::pin(std::future::pending()))
}

/// Serves `handler` on one connection; `keep_alive` false closes it after
/// the first answer, as a gateway closes an idle tunnel.
async fn serve_conn<S>(stream: S, handler: Handler, requests: Arc<AtomicI64>, keep_alive: bool)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
        requests.fetch_add(1, Ordering::SeqCst);
        let host = req
            .headers()
            .get(hyper::header::HOST)
            .and_then(|h| h.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let fut = handler(host);
        async move {
            let status = fut.await.unwrap_or(500);
            let body = if status == 200 {
                "ok".to_string()
            } else {
                "egress denied\n".to_string()
            };
            let mut resp = hyper::Response::new(Full::new(Bytes::from(body)));
            *resp.status_mut() = hyper::StatusCode::from_u16(status).unwrap();
            Ok::<_, Infallible>(resp)
        }
    });
    let _ = hyper::server::conn::http1::Builder::new()
        .keep_alive(keep_alive)
        .serve_connection(TokioIo::new(stream), svc)
        .await;
}

/// A local endpoint counting the connections and requests it sees.
struct Target {
    addr: SocketAddr,
    conns: Arc<AtomicI64>,
    requests: Arc<AtomicI64>,
    task: JoinHandle<()>,
}

impl Drop for Target {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Target {
    fn host(&self) -> String {
        self.addr.to_string()
    }
    fn conns(&self) -> i64 {
        self.conns.load(Ordering::SeqCst)
    }
    fn requests(&self) -> i64 {
        self.requests.load(Ordering::SeqCst)
    }
}

#[derive(Default, Clone)]
struct TargetOpts {
    tls: Option<Arc<rustls::ServerConfig>>,
    /// Closes the first connection after its first answer.
    cut_first: bool,
}

async fn new_target_with(handler: Handler, opts: TargetOpts) -> Target {
    let ln = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap();
    let conns = Arc::new(AtomicI64::new(0));
    let requests = Arc::new(AtomicI64::new(0));
    let (c, r) = (conns.clone(), requests.clone());
    let cut_done = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = ln.accept().await else {
                return;
            };
            c.fetch_add(1, Ordering::SeqCst);
            let keep_alive = !(opts.cut_first && !cut_done.swap(true, Ordering::SeqCst));
            let (h, r, tls) = (handler.clone(), r.clone(), opts.tls.clone());
            tokio::spawn(async move {
                match tls {
                    Some(cfg) => {
                        if let Ok(s) = TlsAcceptor::from(cfg).accept(stream).await {
                            serve_conn(s, h, r, keep_alive).await;
                        }
                    }
                    None => serve_conn(stream, h, r, keep_alive).await,
                }
            });
        }
    });
    Target {
        addr,
        conns,
        requests,
        task,
    }
}

async fn new_target(handler: Handler) -> Target {
    new_target_with(handler, TargetOpts::default()).await
}

/// A CA and a leaf for `names`, as a server config and the roots trusting it.
fn test_pki(names: &[&str]) -> (Arc<rustls::ServerConfig>, RootCertStore) {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca.self_signed(&ca_key).unwrap();
    let key = KeyPair::generate().unwrap();
    let leaf = CertificateParams::new(names.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .unwrap()
        .signed_by(&key, &Issuer::from_params(&ca, &ca_key))
        .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(ca_cert.der().clone()).unwrap();
    (
        server_config(
            vec![leaf.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        ),
        roots,
    )
}

fn server_config(
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Arc<rustls::ServerConfig> {
    Arc::new(
        rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap(),
    )
}

/// A server config serving the endpoints' certificate from `bundle`.
fn bundle_config(bundle: &targetcert::Bundle) -> Arc<rustls::ServerConfig> {
    let chain = rustls_pemfile::certs(&mut &bundle.cert[..])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = rustls_pemfile::private_key(&mut &bundle.key[..])
        .unwrap()
        .unwrap();
    server_config(chain, key)
}

/// An HTTPS endpoint whose certificate names 127.0.0.1, and the roots that
/// trust it.
async fn new_tls_target(handler: Handler) -> (Target, RootCertStore) {
    let (cfg, roots) = test_pki(&["127.0.0.1"]);
    (
        new_target_with(
            handler,
            TargetOpts {
                tls: Some(cfg),
                ..Default::default()
            },
        )
        .await,
        roots,
    )
}

// ---------------------------------------------------------------- actor helpers

/// The actor with endpoint i mapped to `hosts[i]` (a host:port), keeping
/// the scheme the loop asks for.
fn serve(hosts: &[String]) -> Arc<ActorServer> {
    serve_tls(None, hosts)
}

fn serve_tls(roots: Option<RootCertStore>, hosts: &[String]) -> Arc<ActorServer> {
    let hosts = hosts.to_vec();
    let mut s = ActorServer::new();
    s.root_cas = roots;
    s.endpoint_url = Arc::new(move |i, scheme| format!("{scheme}://{}/", hosts[i]));
    Arc::new(s)
}

/// The actor over `n` fresh targets, one per endpoint; the targets are
/// returned so they outlive the test body.
async fn serve_targets(n: usize) -> (Arc<ActorServer>, Vec<Target>) {
    let mut targets = Vec::new();
    for _ in 0..n {
        targets.push(new_target(ok_handler()).await);
    }
    let hosts: Vec<String> = targets.iter().map(Target::host).collect();
    (serve(&hosts), targets)
}

async fn post(h: &ActorServer, path: &str, body: Option<&StartRequest>) -> Answer {
    let body = body
        .map(|b| serde_json::to_vec(b).unwrap())
        .unwrap_or_default();
    h.handle("POST", path, &body).await
}

async fn post_raw(h: &ActorServer, path: &str, body: &str) -> Answer {
    h.handle("POST", path, body.as_bytes()).await
}

async fn get(h: &ActorServer, path: &str) -> Answer {
    h.handle("GET", path, b"").await
}

fn body(a: &Answer) -> String {
    String::from_utf8_lossy(&a.body).into_owned()
}

fn decode_stats(what: &str, a: &Answer) -> Stats {
    assert_eq!(a.status, 200, "{what} = {} {}", a.status, body(a));
    serde_json::from_slice(&a.body).unwrap_or_else(|e| panic!("{what}: decoding {}: {e}", body(a)))
}

const STATS_PATH: &str = "/stats?endpoints=1";
const STOP_PATH: &str = "/stop?endpoints=1";

fn collect_path(w: i64, endpoints: bool) -> String {
    let mut p = format!("{COLLECT_ROUTE}?{WINDOW_PARAM}={w}");
    if endpoints {
        p.push_str("&endpoints=1");
    }
    p
}

async fn start(h: &ActorServer, req: StartRequest) -> Answer {
    post(h, START_ROUTE, Some(&req)).await
}

async fn read_stats(h: &ActorServer) -> Stats {
    decode_stats("GET /stats", &get(h, STATS_PATH).await)
}

async fn collect_stats(h: &ActorServer, w: i64) -> Stats {
    decode_stats(
        "POST /collect",
        &post(h, &collect_path(w, true), None).await,
    )
}

async fn stop_loop(h: &ActorServer) -> Stats {
    decode_stats("POST /stop", &post(h, STOP_PATH, None).await)
}

/// Starts a loop, waits until `cond` holds on the live counters (or fails
/// after 10 s), then stops it and returns the final Stats.
async fn run_loop(h: &ActorServer, req: StartRequest, cond: impl Fn(&Stats) -> bool) -> Stats {
    let a = start(h, req).await;
    assert_eq!(a.status, 204, "POST /start = {} {}", a.status, body(&a));
    wait_for(h, cond).await;
    stop_loop(h).await
}

async fn wait_for(h: &ActorServer, cond: impl Fn(&Stats) -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let s = read_stats(h).await;
        if cond(&s) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "condition not reached; last stats {s:?}"
        );
        tokio::time::sleep(5 * MS).await;
    }
}

fn at_least(n: i64) -> impl Fn(&Stats) -> bool {
    move |s| s.requests >= n
}

fn samples(s: &Stats, d: Distribution) -> u64 {
    s.distributions.get(&d).map(|p| p.count).unwrap_or(0)
}

fn req(endpoints: i64) -> StartRequest {
    StartRequest {
        endpoints,
        ..Default::default()
    }
}

// ---------------------------------------------------------------- real-socket tests

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keep_alive_reuses_one_connection_per_endpoint() {
    let targets = [
        new_target(ok_handler()).await,
        new_target(ok_handler()).await,
        new_target(ok_handler()).await,
    ];
    let hosts: Vec<String> = targets.iter().map(Target::host).collect();
    let h = serve(&hosts);
    let s = run_loop(&h, req(3), |s| s.rounds >= 10).await;
    assert!(
        s.scheme == SCHEME_HTTP
            && samples(&s, Distribution::Tls) == 0
            && samples(&s, Distribution::Dns) == 0,
        "ran over {:?} with {} handshakes and {} lookups",
        s.scheme,
        samples(&s, Distribution::Tls),
        samples(&s, Distribution::Dns)
    );
    assert!(s.successes == s.requests && s.errors.is_empty(), "{s:?}");
    for (i, t) in targets.iter().enumerate() {
        assert_eq!(t.conns(), 1, "target {i} connections");
    }
    assert_eq!(s.new_conns, 3);
    let (opening, latency) = (
        samples(&s, Distribution::Opening),
        samples(&s, Distribution::Latency),
    );
    assert!(
        opening == 3 && (opening + latency) as i64 == s.successes,
        "opening {opening} latency {latency}"
    );
    for (i, e) in s.endpoints.iter().enumerate() {
        assert!(e.requests >= 10, "endpoint {i} got {}", e.requests);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_during_opening_does_not_count_the_cancelled_open() {
    let first = new_target(ok_handler()).await;
    let blocked = new_target(hang_handler()).await;
    let h = serve(&[first.host(), blocked.host()]);
    let s = run_loop(&h, req(2), |s| s.opened >= 1).await;
    assert_eq!(s.opened, 1);
    assert!(
        s.rounds == 0 && s.opening_done.is_zero() && s.endpoints[1].requests == 0,
        "{s:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn request_timeout_is_classified() {
    let slow = new_target(hang_handler()).await;
    let h = serve(&[slow.host()]);
    let s = run_loop(
        &h,
        StartRequest {
            endpoints: 1,
            request_timeout_ms: 20,
            ..Default::default()
        },
        at_least(2),
    )
    .await;
    assert!(
        s.errors.get("timeout") == Some(&s.requests) && s.successes == 0,
        "{s:?}"
    );
    assert_eq!(samples(&s, Distribution::Latency), 0);
    assert_eq!(s.endpoints[0].errors, s.requests);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_200_is_an_error() {
    let ok = new_target(ok_handler()).await;
    let failing = new_target(status_handler(503)).await;
    let h = serve(&[ok.host(), failing.host()]);
    let s = run_loop(&h, req(2), at_least(10)).await;
    assert!(
        s.errors.get("HTTP 503").copied().unwrap_or(0) > 0,
        "{:?}",
        s.errors
    );
    assert!(
        s.endpoints[0].errors == 0 && s.endpoints[1].errors == s.endpoints[1].requests,
        "{:?}",
        s.endpoints
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_stop_lifecycle() {
    let tg = new_target(ok_handler()).await;
    let h = serve(&[tg.host()]);
    assert_eq!(get(&h, STATS_ROUTE).await.status, 404);
    assert_eq!(post(&h, STOP_ROUTE, None).await.status, 404);

    let first = run_loop(&h, req(1), at_least(5)).await;
    tokio::time::sleep(20 * MS).await;
    // A request the stop cut short can still reach the target after /stop
    // returns; nothing else may.
    let extra = tg.requests() - first.requests;
    assert!(extra <= 1, "target got {extra} uncounted requests");

    assert_eq!(start(&h, req(1)).await.status, 204);
    assert_eq!(
        start(&h, req(1)).await.status,
        409,
        "second /start while running"
    );
    let second = stop_loop(&h).await;
    let since_restart = tg.requests() - first.requests;
    assert!(
        second.requests <= since_restart,
        "restarted loop kept the old counts: {} > {since_restart}",
        second.requests
    );
}

#[tokio::test]
async fn start_rejects_bad_requests() {
    let h = ActorServer::new();
    for (name, b) in [
        ("not json", "not json"),
        ("empty object", "{}"),
        ("invalid", r#"{"endpoints":1,"scheme":"ftp"}"#),
    ] {
        assert_eq!(post_raw(&h, START_ROUTE, b).await.status, 400, "{name}");
    }
    assert_eq!(get(&h, READYZ_ROUTE).await.status, 200);
}

#[test]
fn default_urls_are_the_endpoint_services() {
    let s = ActorServer::new();
    for i in [0, 9, 255] {
        for scheme in [SCHEME_HTTP, SCHEME_HTTPS] {
            assert_eq!((s.endpoint_url)(i, scheme), endpoint_url(i, scheme));
        }
    }
    assert!(
        s.root_cas.is_none(),
        "the roots must stay unset so SSL_CERT_FILE applies"
    );
    assert!(
        s.dial.is_none(),
        "the dial hook must stay unset so a plain dialer is used"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn https_failures_are_classified() {
    let bundle = targetcert::generate(std::time::SystemTime::now(), ENDPOINT_DOMAIN).unwrap();
    let endpoint_roots = targetcert::tests::roots_of(&bundle.ca);
    // Serves the endpoints' certificate, which names no IP address.
    let wrong_name = new_target_with(
        ok_handler(),
        TargetOpts {
            tls: Some(bundle_config(&bundle)),
            ..Default::default()
        },
    )
    .await;
    let (untrusted, _) = new_tls_target(ok_handler()).await;
    let plain = new_target(ok_handler()).await;
    // Accepts, reads the ClientHello so closing sends a FIN rather than a
    // reset, and closes: a peer refusing the handshake, as the gateway does
    // for an SNI no rule allows.
    let closer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let closer_addr = closer.local_addr().unwrap();
    let closer_task = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        loop {
            let Ok((mut c, _)) = closer.accept().await else {
                return;
            };
            let mut buf = vec![0u8; 4096];
            let _ = c.read(&mut buf).await;
            drop(c);
        }
    });
    let cases = vec![
        (
            "untrusted CA",
            RootCertStore::empty(),
            untrusted.host(),
            "tls: unknown authority",
        ),
        (
            "hostname mismatch",
            endpoint_roots,
            wrong_name.host(),
            "tls: hostname mismatch",
        ),
        (
            "plain HTTP server",
            RootCertStore::empty(),
            plain.host(),
            "tls: not a TLS server",
        ),
        (
            "peer closes during handshake",
            RootCertStore::empty(),
            closer_addr.to_string(),
            "tls handshake: ",
        ),
    ];
    for (name, roots, host, want) in cases {
        let h = serve_tls(Some(roots), &[host]);
        let s = run_loop(
            &h,
            StartRequest {
                endpoints: 1,
                scheme: SCHEME_HTTPS.into(),
                ..Default::default()
            },
            at_least(2),
        )
        .await;
        let matched: i64 = s
            .errors
            .iter()
            .filter(|(c, _)| c.starts_with(want))
            .map(|(_, n)| n)
            .sum();
        assert!(
            matched == s.requests && s.successes == 0 && samples(&s, Distribution::Tls) == 0,
            "{name}: errors {:?}, {} successes for {} requests; want every request classed {want:?}",
            s.errors,
            s.successes,
            s.requests
        );
    }
    closer_task.abort();
}

/// A dial hook connecting each endpoint name to a local listener instead;
/// TLS still verifies the name the loop asked for.
fn dial_to(addrs: HashMap<String, SocketAddr>) -> DialFn {
    let addrs = Arc::new(addrs);
    Arc::new(move |host, port| {
        let addrs = addrs.clone();
        Box::pin(async move {
            let real = *addrs.get(&host).ok_or_else(|| {
                std::io::Error::other(format!("no local listener for {host}:{port}"))
            })?;
            Ok(Box::new(tokio::net::TcpStream::connect(real).await?) as BoxStream)
        })
    })
}

// Runs the loop as the actor does in the cluster: the default endpoint URLs,
// eight endpoints, and over HTTPS the target's own certificate verified
// under each endpoint's name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn endpoints_by_name_over_both_schemes() {
    const C: usize = 8;
    let bundle = targetcert::generate(std::time::SystemTime::now(), ENDPOINT_DOMAIN).unwrap();
    for scheme in [SCHEME_HTTP, SCHEME_HTTPS] {
        let mut addrs = HashMap::new();
        let mut targets = Vec::new();
        for i in 0..C {
            let t = if scheme == SCHEME_HTTPS {
                new_target_with(
                    ok_handler(),
                    TargetOpts {
                        tls: Some(bundle_config(&bundle)),
                        ..Default::default()
                    },
                )
                .await
            } else {
                new_target(ok_handler()).await
            };
            addrs.insert(endpoint_host(i), t.addr);
            targets.push(t);
        }
        let mut s = ActorServer::new();
        s.root_cas = Some(targetcert::tests::roots_of(&bundle.ca));
        s.dial = Some(dial_to(addrs));
        let h = Arc::new(s);
        let interval = 2 * MS;
        let st = run_loop(
            &h,
            StartRequest {
                endpoints: C as i64,
                scheme: scheme.into(),
                interval_ms: 2,
                ..Default::default()
            },
            |s| s.rounds >= 3,
        )
        .await;
        assert!(
            st.scheme == scheme && st.opened == C as i64 && st.rounds >= 3,
            "{scheme}: {st:?}"
        );
        assert!(
            st.successes == st.requests && st.errors.is_empty(),
            "{scheme}: {:?}",
            st.errors
        );
        assert_eq!(st.new_conns, C as i64, "{scheme}");
        let want_tls = if scheme == SCHEME_HTTPS { C as u64 } else { 0 };
        assert_eq!(samples(&st, Distribution::Tls), want_tls, "{scheme}");
        for (i, t) in targets.iter().enumerate() {
            assert_eq!(t.conns(), 1, "{scheme}: endpoint {i}");
        }
        let period = interval * C as u32;
        assert!(
            st.longest_gap >= period / 2 && st.longest_gap <= Duration::from_secs(5),
            "{scheme}: longest gap {:?}, period {period:?}",
            st.longest_gap
        );
    }
}

// A tunnel the gateway closed shows up as a redial: NewConns grows by one
// while Errors stays empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_connection_is_redialed_without_an_error() {
    let cut = new_target_with(
        ok_handler(),
        TargetOpts {
            cut_first: true,
            ..Default::default()
        },
    )
    .await;
    let steady = [
        new_target(ok_handler()).await,
        new_target(ok_handler()).await,
    ];
    let h = serve(&[steady[0].host(), cut.host(), steady[1].host()]);
    let s = run_loop(
        &h,
        StartRequest {
            endpoints: 3,
            interval_ms: 2,
            ..Default::default()
        },
        |s| s.rounds >= 2,
    )
    .await;
    assert!(s.successes == s.requests && s.errors.is_empty(), "{s:?}");
    assert_eq!(s.new_conns, 4, "one per endpoint plus the redial");
    assert_eq!(cut.conns(), 2);
    for (i, t) in steady.iter().enumerate() {
        assert_eq!(t.conns(), 1, "steady endpoint {i}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settled_window_has_no_dns_or_tls() {
    let by_name = new_target(ok_handler()).await;
    let (tls_target, roots) = new_tls_target(ok_handler()).await;
    let cases = vec![
        (
            "dns over http by name",
            serve(&[format!("localhost:{}", by_name.addr.port())]),
            req(1),
            Distribution::Dns,
        ),
        (
            "tls over https",
            serve_tls(Some(roots), &[tls_target.host()]),
            StartRequest {
                endpoints: 1,
                scheme: SCHEME_HTTPS.into(),
                ..Default::default()
            },
            Distribution::Tls,
        ),
    ];
    for (name, h, r, dist) in cases {
        assert_eq!(start(&h, r).await.status, 204, "{name}");
        wait_for(&h, at_least(2)).await;
        let first = collect_stats(&h, 1).await;
        assert!(
            samples(&first, dist) > 0,
            "{name}: the window with the open has no {dist:?} sample"
        );
        wait_for(&h, at_least(first.requests + 2)).await;
        let second = collect_stats(&h, 2).await;
        assert!(
            !second.distributions.contains_key(&dist),
            "{name}: a window with no open carries {dist:?}"
        );
        assert!(
            samples(&second, Distribution::Latency) > 0 && second.successes > first.successes,
            "{name}"
        );
        stop_loop(&h).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_returns_final_stats() {
    let tg = new_target(ok_handler()).await;
    let h = serve(&[tg.host()]);
    for (name, a) in [
        ("GET /stats", get(&h, STATS_ROUTE).await),
        ("POST /collect", post(&h, COLLECT_ROUTE, None).await),
        ("POST /stop", post(&h, STOP_ROUTE, None).await),
    ] {
        assert!(
            a.status == 404 && body(&a).trim() == NO_LOOP_MESSAGE,
            "{name}: {} {:?}",
            a.status,
            body(&a)
        );
    }
    assert_eq!(start(&h, req(1)).await.status, 204);
    wait_for(&h, at_least(1)).await;
    let a = post(&h, STOP_ROUTE, None).await;
    let fin = decode_stats("POST /stop", &a);
    assert!(
        a.header("Content-Type") == Some("application/json")
            && fin.scheme == SCHEME_HTTP
            && fin.endpoints.is_empty()
            && fin.requests > 0,
        "{a:?}"
    );
    assert_eq!(get(&h, STATS_ROUTE).await.status, 404);
    assert_eq!(post(&h, &collect_path(1, false), None).await.status, 404);
    // The final report is kept: a /stop the driver did not hear is answered
    // again, byte for byte, until a new loop replaces it.
    let again = post(&h, STOP_ROUTE, None).await;
    assert!(
        again.status == 200 && again.body == a.body,
        "second /stop: {again:?}"
    );
    let with_ep = decode_stats("third POST /stop", &post(&h, STOP_PATH, None).await);
    assert!(with_ep.endpoints.len() == 1 && with_ep.requests == fin.requests);
    let r = StartRequest {
        endpoints: 1,
        interval_ms: 60_000,
        offset_ms: 60_000,
        ..Default::default()
    };
    assert_eq!(start(&h, r).await.status, 204);
    assert_eq!(stop_loop(&h).await.requests, 0, "the new loop's own report");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collect_window_ids() {
    let (h, _targets) = serve_targets(1).await;
    let r = StartRequest {
        endpoints: 1,
        interval_ms: 60_000,
        offset_ms: 60_000,
        ..Default::default()
    };
    assert_eq!(start(&h, r).await.status, 204);
    let route = format!("{COLLECT_ROUTE}?{WINDOW_PARAM}=");
    let steps: Vec<(&str, String, u16, i64)> = vec![
        ("missing", COLLECT_ROUTE.to_string(), 400, 1),
        ("zero", format!("{route}0"), 400, 1),
        ("negative", format!("{route}-1"), 400, 1),
        ("non-numeric", format!("{route}one"), 400, 1),
        ("skip ahead before the first", format!("{route}2"), 409, 1),
        ("first cut is 1", format!("{route}1"), 200, 0),
        ("last again", format!("{route}1"), 200, 0),
        (
            "last again with endpoints",
            format!("{route}1&endpoints=1"),
            200,
            0,
        ),
        ("next cuts", format!("{route}2"), 200, 0),
        ("lower", format!("{route}1"), 409, 3),
        ("zero after cuts", format!("{route}0"), 400, 3),
        ("skip ahead", format!("{route}4"), 409, 3),
        ("next cuts again", format!("{route}3"), 200, 0),
    ];
    for (name, path, want, next) in steps {
        let a = post(&h, &path, None).await;
        assert_eq!(
            a.status,
            want,
            "{name}: POST {path} = {} {}",
            a.status,
            body(&a)
        );
        if next == 0 {
            decode_stats(name, &a);
        } else {
            let w = format!("the next window is {next}");
            assert!(
                body(&a).contains(&w),
                "{name}: body {:?} does not say {w:?}",
                body(&a)
            );
        }
    }
    stop_loop(&h).await;
}

// The same id answered again is the same bytes; the sample recorded between
// the two answers is in the next window, not lost.
#[tokio::test]
async fn repeated_collect_consumes_nothing() {
    let run = LoopRun::new(&req(1));
    let s = ActorServer::new();
    s.set_run(run.clone()).await;
    run.record(0, 5 * MS, 200, None, &Trace::default());
    let first = post(&s, &collect_path(1, false), None).await;
    run.record(0, 7 * MS, 200, None, &Trace::default());
    let again = post(&s, &collect_path(1, false), None).await;
    assert!(first.status == 200 && again.status == 200 && first.body == again.body);
    let with_ep = decode_stats(
        "window 1 with endpoints",
        &post(&s, &collect_path(1, true), None).await,
    );
    assert!(
        with_ep.requests == 1
            && with_ep.endpoints.len() == 1
            && samples(&with_ep, Distribution::Opening) == 1
    );
    let second = collect_stats(&s, 2).await;
    let l = &second.distributions[&Distribution::Latency];
    assert!(
        second.requests == 2 && l.count == 1 && l.min == 0.007,
        "{second:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_recorder_per_loop() {
    let (h, _targets) = serve_targets(2).await;
    let first = run_loop(&h, req(2), at_least(10)).await;
    assert!(samples(&first, Distribution::Latency) > 0);
    // Nothing in the new loop fires for a minute, so its first collect sees
    // only what the previous loop might have left behind.
    let r = StartRequest {
        endpoints: 2,
        interval_ms: 60_000,
        offset_ms: 60_000,
        ..Default::default()
    };
    assert_eq!(start(&h, r).await.status, 204);
    let second = collect_stats(&h, 1).await;
    assert!(
        second.requests == 0 && second.opened == 0 && second.distributions.is_empty(),
        "{second:?}"
    );
    stop_loop(&h).await;
}

// Cuts windows while requests land: each window's samples equal the growth
// of successes since the previous cut, the windows add up to the final
// count, and every tenth cut asked twice comes back unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collect_invariant_under_race() {
    let (h, _targets) = serve_targets(8).await;
    assert_eq!(start(&h, req(8)).await.status, 204);
    let bad_reads = Arc::new(AtomicI64::new(0));
    let mut readers = Vec::new();
    for _ in 0..4 {
        let (h, bad) = (h.clone(), bad_reads.clone());
        readers.push(tokio::spawn(async move {
            for _ in 0..50 {
                let st = read_stats(&h).await;
                let failed: i64 = st.errors.values().sum();
                if st.successes + failed != st.requests || !st.distributions.is_empty() {
                    bad.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    let (mut total, mut prev, mut bad_cuts, mut bad_retries) = (0u64, 0i64, 0, 0);
    for id in 1..=200 {
        let a = post(&h, &collect_path(id, true), None).await;
        let w = decode_stats("POST /collect", &a);
        if id % 10 == 0 && post(&h, &collect_path(id, true), None).await.body != a.body {
            bad_retries += 1;
        }
        let got = samples(&w, Distribution::Opening) + samples(&w, Distribution::Latency);
        if got as i64 != w.successes - prev {
            bad_cuts += 1;
        }
        prev = w.successes;
        total += got;
        tokio::task::yield_now().await;
    }
    for r in readers {
        r.await.unwrap();
    }
    let fin = stop_loop(&h).await;
    total += samples(&fin, Distribution::Opening) + samples(&fin, Distribution::Latency);
    assert!(bad_reads.load(Ordering::SeqCst) == 0 && bad_cuts == 0 && bad_retries == 0);
    assert!(
        total as i64 == fin.successes && fin.successes > 0,
        "{total} samples for {} successes",
        fin.successes
    );
}

// Cuts and reads racing a stop answer 200 while the loop runs or 404 once it
// is stopped, never a 500 from a released recorder.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collect_during_stop_is_never_500() {
    let (h, _targets) = serve_targets(8).await;
    assert_eq!(start(&h, req(8)).await.status, 204);
    let ok = Arc::new(AtomicI64::new(0));
    let other = Arc::new(AtomicI64::new(0));
    let stopped = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    for i in 0..16 {
        let (h, ok, other, stopped) = (h.clone(), ok.clone(), other.clone(), stopped.clone());
        tasks.push(tokio::spawn(async move {
            let mut window = 0;
            loop {
                let a = if i == 0 {
                    window += 1;
                    post(&h, &collect_path(window, false), None).await
                } else {
                    get(&h, STATS_ROUTE).await
                };
                if a.status == 200 {
                    ok.fetch_add(1, Ordering::SeqCst);
                } else if !(a.status == 404 && body(&a).trim() == NO_LOOP_MESSAGE) {
                    other.fetch_add(1, Ordering::SeqCst);
                    eprintln!("answered {} {}", a.status, body(&a));
                }
                if stopped.load(Ordering::SeqCst) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        }));
    }
    tokio::time::sleep(20 * MS).await;
    stop_loop(&h).await;
    stopped.store(true, Ordering::SeqCst);
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(post(&h, &collect_path(1, false), None).await.status, 404);
    assert!(ok.load(Ordering::SeqCst) > 0 && other.load(Ordering::SeqCst) == 0);
}

#[tokio::test]
async fn every_route_carries_protocol_header() {
    assert_eq!(PROTOCOL_VERSION, "2");
    let h = ActorServer::new();
    let bad = StartRequest::default();
    for (name, a, want) in [
        ("readyz", get(&h, READYZ_ROUTE).await, 200),
        (
            "start rejected",
            post(&h, START_ROUTE, Some(&bad)).await,
            400,
        ),
        ("stats without a loop", get(&h, STATS_ROUTE).await, 404),
        (
            "collect without a loop",
            post(&h, &collect_path(1, false), None).await,
            404,
        ),
        ("stop without a loop", post(&h, STOP_ROUTE, None).await, 404),
        ("unknown route", get(&h, "/metrics").await, 404),
    ] {
        assert_eq!(a.status, want, "{name}");
        assert_eq!(a.header(PROTOCOL_HEADER), Some(PROTOCOL_VERSION), "{name}");
    }
}

// The answer sizes at the largest C, since the driver polls every actor:
// /stats under 1 KB, a /collect with the four distributions in a few KB,
// and the per-endpoint counts only when asked for.
#[tokio::test]
async fn payload_budget() {
    const C: i64 = super::endpoints::MAX_ENDPOINTS;
    let run = LoopRun::new(&StartRequest {
        endpoints: C,
        scheme: SCHEME_HTTPS.into(),
        ..Default::default()
    });
    run.with_state(|s, rounds, _| {
        s.requests = 12345 * C;
        s.successes = 12339 * C;
        s.new_conns = C;
        s.opened = C;
        s.opening_done = Duration::from_millis(102400);
        s.longest_gap = Duration::from_micros(12345678);
        s.errors = BTreeMap::from([
            ("timeout".to_string(), 3 * C),
            ("HTTP 503".to_string(), 3 * C),
        ]);
        *rounds = 12345;
        for e in s.endpoints.iter_mut() {
            *e = Endpoint {
                requests: 12345,
                errors: 6,
                longest_gap: Duration::from_micros(12345678),
            };
        }
    });
    // Every bucket non-zero, so no count is a short "0".
    let fill = || {
        run.with_state(|_, _, rec| {
            for d in Distribution::ALL {
                for b in BOUNDS.iter() {
                    rec.record(d, *b);
                }
            }
        })
    };
    let s = ActorServer::new();
    s.set_run(run.clone()).await;
    fill();
    let collect = post(&s, &collect_path(1, false), None).await;
    fill();
    let collect_ep = post(&s, &collect_path(2, true), None).await;
    let stats = get(&s, STATS_ROUTE).await;
    for (name, a, budget, endpoints, dists) in [
        ("/stats", &stats, 1 << 10, false, false),
        ("/collect", &collect, 12 << 10, false, true),
        ("/collect?endpoints=1", &collect_ep, 80 << 10, true, true),
    ] {
        assert_eq!(a.status, 200, "{name}");
        assert!(
            a.body.len() < budget,
            "{name} = {} B, budget {budget} B",
            a.body.len()
        );
        let st: Stats = serde_json::from_slice(&a.body).unwrap();
        assert!(
            (st.endpoints.len() == C as usize) == endpoints
                && (st.distributions.len() == 4) == dists
                && st.rounds == 12345,
            "{name}"
        );
    }
}

// ---------------------------------------------------------------- paused-clock tests

/// Serves `handler` over in-memory pipes, so a loop runs on the paused clock.
fn pipe_dial(handler: Handler) -> DialFn {
    Arc::new(move |_, _| {
        let handler = handler.clone();
        Box::pin(async move {
            let (client, server) = tokio::io::duplex(1 << 16);
            tokio::spawn(serve_conn(
                server,
                handler,
                Arc::new(AtomicI64::new(0)),
                true,
            ));
            Ok(Box::new(client) as BoxStream)
        })
    })
}

/// The actor over pipes with endpoint i named `endpoint-i`, started with
/// `r`.
async fn start_pipe_loop(handler: Handler, r: StartRequest) -> Arc<ActorServer> {
    let mut s = ActorServer::new();
    s.dial = Some(pipe_dial(handler));
    s.endpoint_url = Arc::new(|i, scheme| format!("{scheme}://endpoint-{i}/"));
    let s = Arc::new(s);
    let a = start(&s, r).await;
    assert_eq!(a.status, 204, "POST /start = {} {}", a.status, body(&a));
    s
}

/// Lets every task woken at the current instant run.
async fn settle() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

async fn sleep_until(start: Instant, at: Duration) {
    tokio::time::sleep_until(start + at).await;
    settle().await;
}

fn paced(endpoints: i64, interval: Duration) -> StartRequest {
    StartRequest {
        endpoints,
        interval_ms: interval.as_millis() as i64,
        ..Default::default()
    }
}

struct Step {
    at: Duration,
    requests: i64,
    opened: i64,
    rounds: i64,
    opening_done: Duration,
    longest_gap: Duration,
    why: &'static str,
}

fn check_step(st: &Stats, tc: &Step) {
    assert_eq!(
        (
            st.requests,
            st.opened,
            st.rounds,
            st.opening_done,
            st.longest_gap
        ),
        (
            tc.requests,
            tc.opened,
            tc.rounds,
            tc.opening_done,
            tc.longest_gap
        ),
        "at {:?}: {}",
        tc.at,
        tc.why
    );
}

// Endpoint i opens i intervals in, each endpoint is then requested once per
// period of C intervals, so every gap is one period; the windows add up to
// one opening per opened endpoint and the rest latency.
#[tokio::test(start_paused = true)]
async fn pacing_and_gaps_are_exact() {
    const C: i64 = 4;
    let interval = 100 * MS;
    let period = interval * C as u32;
    let start_at = Instant::now();
    let h = start_pipe_loop(ok_handler(), paced(C, interval)).await;
    let steps = [
        Step {
            at: interval / 2,
            requests: 1,
            opened: 1,
            rounds: 0,
            opening_done: Duration::ZERO,
            longest_gap: Duration::ZERO,
            why: "endpoint 0 opens at once, the others wait their turn",
        },
        Step {
            at: period - interval / 2,
            requests: C,
            opened: C,
            rounds: 1,
            opening_done: interval * 3,
            longest_gap: Duration::ZERO,
            why: "one open per interval; the last completes at C-1 intervals",
        },
        Step {
            at: period * 2 - interval / 2,
            requests: 2 * C,
            opened: C,
            rounds: 2,
            opening_done: interval * 3,
            longest_gap: period,
            why: "each second request comes one period after the first",
        },
        Step {
            at: period * 3 - interval / 2,
            requests: 3 * C,
            opened: C,
            rounds: 3,
            opening_done: interval * 3,
            longest_gap: period,
            why: "steady state: every gap is one period",
        },
    ];
    let (mut opening, mut latency) = (0, 0);
    for (i, tc) in steps.iter().enumerate() {
        sleep_until(start_at, tc.at).await;
        let st = collect_stats(&h, i as i64 + 1).await;
        check_step(&st, tc);
        opening += samples(&st, Distribution::Opening);
        latency += samples(&st, Distribution::Latency);
        assert!(
            opening as i64 == tc.opened && (opening + latency) as i64 == st.successes,
            "at {:?}",
            tc.at
        );
    }
    settle().await;
    for (i, e) in read_stats(&h).await.endpoints.iter().enumerate() {
        assert_eq!(e.longest_gap, period, "endpoint {i}");
    }
    stop_loop(&h).await;
}

// offsetMs delays the whole schedule and nothing else.
#[tokio::test(start_paused = true)]
async fn offset_shifts_every_endpoint() {
    const C: i64 = 4;
    let (interval, offset) = (100 * MS, 50 * MS);
    let period = interval * C as u32;
    let start_at = Instant::now();
    let mut r = paced(C, interval);
    r.offset_ms = 50;
    let h = start_pipe_loop(ok_handler(), r).await;
    let steps = [
        Step {
            at: offset / 2,
            requests: 0,
            opened: 0,
            rounds: 0,
            opening_done: Duration::ZERO,
            longest_gap: Duration::ZERO,
            why: "nothing opens before the offset",
        },
        Step {
            at: offset + interval / 2,
            requests: 1,
            opened: 1,
            rounds: 0,
            opening_done: Duration::ZERO,
            longest_gap: Duration::ZERO,
            why: "endpoint 0 opens at the offset",
        },
        Step {
            at: offset + period - interval / 2,
            requests: C,
            opened: C,
            rounds: 1,
            opening_done: offset + interval * 3,
            longest_gap: Duration::ZERO,
            why: "the last open completes at the offset plus C-1 intervals",
        },
        Step {
            at: offset + period * 2 - interval / 2,
            requests: 2 * C,
            opened: C,
            rounds: 2,
            opening_done: offset + interval * 3,
            longest_gap: period,
            why: "each repeat comes one period after the open it follows",
        },
    ];
    for tc in &steps {
        sleep_until(start_at, tc.at).await;
        check_step(&read_stats(&h).await, tc);
    }
    for (i, e) in read_stats(&h).await.endpoints.iter().enumerate() {
        assert_eq!(e.longest_gap, period, "endpoint {i}");
    }
    stop_loop(&h).await;
}

// A request hanging until its timeout on one endpoint leaves the others'
// gaps at one period; the hung endpoint's own gap is the timeout.
#[tokio::test(start_paused = true)]
async fn slow_endpoint_delays_only_itself() {
    const C: i64 = 3;
    let (interval, timeout) = (50 * MS, 300 * MS);
    let period = interval * C as u32;
    let handler: Handler = Arc::new(|host| {
        Box::pin(async move {
            if host == "endpoint-0" {
                std::future::pending::<()>().await;
            }
            Some(200)
        })
    });
    let start_at = Instant::now();
    let mut r = paced(C, interval);
    r.request_timeout_ms = 300;
    let h = start_pipe_loop(handler, r).await;
    sleep_until(start_at, Duration::from_secs(1) + interval / 2).await;
    let st = read_stats(&h).await;
    for i in 1..C as usize {
        assert_eq!(st.endpoints[i].longest_gap, period, "endpoint {i}");
        assert_eq!(st.endpoints[i].errors, 0, "endpoint {i}");
    }
    assert!(
        st.endpoints[0].longest_gap == timeout && st.longest_gap == timeout,
        "{st:?}"
    );
    let t = st.errors.get("timeout").copied().unwrap_or(0);
    assert!(t > 0 && t == st.endpoints[0].errors, "{:?}", st.errors);
    assert!(st.opened == C && st.opening_done == timeout, "{st:?}");
    stop_loop(&h).await;
}

/// A handler answering each Host's first request after `latency` (except
/// those in `fast`) and every later one at once.
fn slow_first(
    latency: Duration,
    slow_hosts: Option<HashSet<String>>,
    fast: Option<&'static str>,
) -> Handler {
    let opened = Arc::new(Mutex::new(HashSet::<String>::new()));
    Arc::new(move |host: String| {
        let first = opened.lock().insert(host.clone());
        let slow = first
            && fast != Some(host.as_str())
            && slow_hosts.as_ref().is_none_or(|s| s.contains(&host));
        Box::pin(async move {
            if slow {
                tokio::time::sleep(latency).await;
            }
            Some(200)
        })
    })
}

// Slow opens stretch only their own endpoint's first gap; an endpoint
// already open keeps its period while the others open.
#[tokio::test(start_paused = true)]
async fn slow_opens_do_not_delay_opened_endpoints() {
    const C: i64 = 4;
    let (interval, open_latency) = (100 * MS, 500 * MS);
    let period = interval * C as u32;
    let start_at = Instant::now();
    let h = start_pipe_loop(
        slow_first(open_latency, None, Some("endpoint-0")),
        paced(C, interval),
    )
    .await;
    sleep_until(start_at, period + interval / 2).await;
    let st = read_stats(&h).await;
    assert!(
        st.opened == 1
            && st.endpoints[0].requests == 2
            && st.endpoints[0].longest_gap == period
            && st.requests == 2,
        "{st:?}"
    );
    sleep_until(start_at, period * 3 + interval / 2).await;
    let st = read_stats(&h).await;
    assert!(
        st.opened == C && st.opening_done == interval * 3 + open_latency,
        "{st:?}"
    );
    assert!(
        st.endpoints[0].longest_gap == period && st.endpoints[0].requests == 4,
        "{:?}",
        st.endpoints[0]
    );
    for i in 1..C as usize {
        assert_eq!(st.endpoints[i].longest_gap, open_latency, "endpoint {i}");
    }
    let fin = stop_loop(&h).await;
    let op = &fin.distributions[&Distribution::Opening];
    assert!(
        st.longest_gap == open_latency
            && op.count == C as u64
            && op.max == open_latency.as_secs_f64(),
        "{:?} {op:?}",
        st.longest_gap
    );
}

// A request longer than the period is followed at once by the next, so the
// gap is the request's latency, not latency plus a period.
#[tokio::test(start_paused = true)]
async fn slow_request_resumes_on_the_next_tick() {
    let (period, latency) = (50 * MS, 120 * MS);
    let start_at = Instant::now();
    let starts = Arc::new(Mutex::new(Vec::<Duration>::new()));
    let s2 = starts.clone();
    let handler: Handler = Arc::new(move |_| {
        s2.lock().push(start_at.elapsed());
        Box::pin(async move {
            tokio::time::sleep(latency).await;
            Some(200)
        })
    });
    let h = start_pipe_loop(handler, paced(1, period)).await;
    sleep_until(start_at, latency * 4 + period / 2).await;
    let st = read_stats(&h).await;
    let got = starts.lock().clone();
    assert_eq!(
        got,
        vec![
            Duration::ZERO,
            latency,
            latency * 2,
            latency * 3,
            latency * 4
        ]
    );
    assert_eq!(st.longest_gap, latency);
    stop_loop(&h).await;
}

// A stop neither waits for in-flight requests' timeouts nor leaks them.
#[tokio::test(start_paused = true)]
async fn stop_with_requests_in_flight_returns_at_once() {
    const C: i64 = 8;
    let mut r = req(C);
    r.request_timeout_ms = 5000;
    let h = start_pipe_loop(hang_handler(), r).await;
    tokio::time::sleep(10 * MS).await;
    settle().await;
    let before = Instant::now();
    let st = stop_loop(&h).await;
    assert_eq!(
        before.elapsed(),
        Duration::ZERO,
        "/stop must not wait for a timeout"
    );
    assert!(
        st.requests == 0 && st.opened == 0 && st.distributions.is_empty(),
        "{st:?}"
    );
    assert_eq!(get(&h, STATS_ROUTE).await.status, 404);
}

// Each /collect answers with the samples since the previous one, so the
// windows add up to the whole and a window's min and max are its own.
#[tokio::test(start_paused = true)]
async fn two_cuts_partition_the_run() {
    const C: i64 = 2;
    let interval = 100 * MS;
    let (fast, slow) = (10 * MS, 100 * MS);
    let delay = Arc::new(Mutex::new(fast));
    let d2 = delay.clone();
    let handler: Handler = Arc::new(move |_| {
        let d = *d2.lock();
        Box::pin(async move {
            tokio::time::sleep(d).await;
            Some(200)
        })
    });
    let start_at = Instant::now();
    let h = start_pipe_loop(handler, paced(C, interval)).await;

    sleep_until(start_at, 350 * MS).await;
    let first = collect_stats(&h, 1).await;
    let (op, l) = (
        &first.distributions[&Distribution::Opening],
        &first.distributions[&Distribution::Latency],
    );
    assert!(
        first.successes == 4
            && op.count == 2
            && l.count == 2
            && l.min == fast.as_secs_f64()
            && l.max == fast.as_secs_f64(),
        "first window: {first:?}"
    );

    *delay.lock() = slow;
    sleep_until(start_at, 650 * MS).await;
    let polled = read_stats(&h).await;
    assert!(
        polled.successes == 6 && polled.distributions.is_empty(),
        "/stats inside the second window: {polled:?}"
    );
    sleep_until(start_at, 850 * MS).await;
    let second = collect_stats(&h, 2).await;
    assert!(!second.distributions.contains_key(&Distribution::Opening));
    let l = &second.distributions[&Distribution::Latency];
    assert!(
        second.successes == 8
            && l.count == 4
            && l.min == slow.as_secs_f64()
            && l.max == slow.as_secs_f64(),
        "second window: {second:?}"
    );

    sleep_until(start_at, 1150 * MS).await;
    let fin = stop_loop(&h).await;
    let total: u64 = [&first, &second, &fin]
        .iter()
        .map(|s| samples(s, Distribution::Opening) + samples(s, Distribution::Latency))
        .sum();
    assert!(
        fin.successes == 11 && total as i64 == fin.successes,
        "{total} samples, {} successes",
        fin.successes
    );
}

// The count a blended window prices: the requests finished when the last
// endpoint opened, repeats on endpoints already open included, fixed after.
#[tokio::test(start_paused = true)]
async fn requests_at_opening_done() {
    const C: i64 = 2;
    let (interval, open_latency) = (100 * MS, 250 * MS);
    let slow_hosts = Some(HashSet::from(["endpoint-1".to_string()]));
    let start_at = Instant::now();
    let h = start_pipe_loop(
        slow_first(open_latency, slow_hosts, None),
        paced(C, interval),
    )
    .await;
    // Endpoint 0 opens at 0 and repeats at 200; endpoint 1 opens at 100 and
    // finishes at 350, the third request; its first repeat follows at once.
    sleep_until(start_at, interval + open_latency).await;
    let st = read_stats(&h).await;
    assert!(
        st.opened == C
            && st.opening_done == interval + open_latency
            && st.requests_at_opening_done == 3
            && st.requests == 4,
        "{st:?}"
    );
    sleep_until(start_at, Duration::from_secs(1)).await;
    let st = read_stats(&h).await;
    assert!(
        st.requests_at_opening_done == 3 && st.requests > 4,
        "{st:?}"
    );
    assert_eq!(stop_loop(&h).await.requests_at_opening_done, 3);
}

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;

use super::server::{activation_dialer, Config, Dial, Server};
use super::transport::{
    empty, is_grpc, BoxError, FnTransport, ProtocolMirrorTransport, ProxyBody, Transport,
};
use super::*;
use crate::test_pki::{insecure_client, LeafOpts, TestCa};
use crate::tunnel::egress::BoxIo;

const ROUTER_ID: &str = "spiffe://cluster.local/ns/hivekeep-system/sa/gate-router";

struct Files {
    _dir: tempfile::TempDir,
    bundle: std::path::PathBuf,
    trust: std::path::PathBuf,
    ca: TestCa,
}

fn files() -> Files {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let leaf = ca.issue(&LeafOpts {
        server: true,
        dns: vec!["worker.test".into()],
        ..Default::default()
    });
    let bundle = dir.path().join("server.pem");
    let trust = dir.path().join("trust.pem");
    std::fs::write(&bundle, leaf.bundle_pem()).unwrap();
    std::fs::write(&trust, ca.pem()).unwrap();
    Files {
        _dir: dir,
        bundle,
        trust,
        ca,
    }
}

fn config(f: &Files, upstream: &str) -> Config {
    Config {
        credential_bundle_path: f.bundle.clone(),
        trust_bundle_path: f.trust.clone(),
        allowed_client_id: ROUTER_ID.into(),
        upstream: upstream.parse().unwrap(),
    }
}

fn test_dial() -> Dial {
    Arc::new(|addr: String| {
        Box::pin(async move { Ok(Box::new(TcpStream::connect(addr).await?) as BoxIo) })
    })
}

fn new_test_server(upstream: &str) -> (Server, Files) {
    let f = files();
    (Server::new(config(&f, upstream)).unwrap(), f)
}

fn transport<F>(f: F) -> Arc<dyn Transport>
where
    F: Fn(Request<ProxyBody>) -> Result<Response<ProxyBody>, BoxError> + Send + Sync + 'static,
{
    Arc::new(FnTransport(move |r| {
        let res = f(r);
        Box::pin(async move { res }) as futures::future::BoxFuture<'static, _>
    }))
}

fn status(code: u16) -> Response<ProxyBody> {
    let mut r = Response::new(empty());
    *r.status_mut() = StatusCode::from_u16(code).unwrap();
    r
}

fn get(target: Option<&str>, host: Option<&str>) -> Request<Empty<Bytes>> {
    let mut b = Request::builder().method(Method::GET).uri("/hello");
    if let Some(t) = target {
        b = b.header(TARGET_ACTOR_HEADER, t);
    }
    if let Some(h) = host {
        b = b.header(http::header::HOST, h);
    }
    b.body(Empty::new()).unwrap()
}

async fn body_string(r: Response<ProxyBody>) -> String {
    String::from_utf8(r.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap()
}

#[tokio::test]
async fn activation_dialer_closes_late_connection() {
    let (client, mut peer) = tokio::io::duplex(64);
    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    let client = Arc::new(Mutex::new(Some(client)));
    let dial: Dial = Arc::new(move |_| {
        let (c2, client) = (c2.clone(), client.clone());
        Box::pin(async move {
            c2.cancel();
            Ok(Box::new(client.lock().take().unwrap()) as BoxIo)
        })
    });
    let r = activation_dialer(cancel, dial)("actor:80".into()).await;
    assert_eq!(
        r.err().map(|e| e.kind()),
        Some(std::io::ErrorKind::Interrupted)
    );
    let mut b = [0u8; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), peer.read(&mut b))
            .await
            .unwrap()
            .unwrap(),
        0,
        "late connection not closed"
    );
}

async fn text_backend(body: &'static str) -> String {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((s, _)) = lis.accept().await {
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(
                    move |_r: Request<hyper::body::Incoming>| async move {
                        Ok::<_, std::convert::Infallible>(Response::new(http_body_util::Full::new(
                            Bytes::from(body),
                        )))
                    },
                );
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(s), svc)
                    .await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn deactivate_isolates_late_dials() {
    let first = text_backend("actor-1").await;
    let second = text_backend("actor-2").await;
    let (server, _f) = new_test_server("http://127.0.0.1:80");
    let calls = Arc::new(AtomicU32::new(0));
    let (first_ready, release_first, second_ready, release_second) = (
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
    );
    let dial: Dial = {
        let (calls, fr, rf, sr, rs) = (
            calls.clone(),
            first_ready.clone(),
            release_first.clone(),
            second_ready.clone(),
            release_second.clone(),
        );
        Arc::new(move |_| {
            let (calls, fr, rf, sr, rs, first, second) = (
                calls.clone(),
                fr.clone(),
                rf.clone(),
                sr.clone(),
                rs.clone(),
                first.clone(),
                second.clone(),
            );
            Box::pin(async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    let c = TcpStream::connect(first).await;
                    fr.notify_one();
                    rf.notified().await;
                    return Ok(Box::new(c?) as BoxIo);
                }
                sr.notify_one();
                rs.notified().await;
                Ok(Box::new(TcpStream::connect(second).await?) as BoxIo)
            })
        })
    };
    server
        .activate("team-a", "actor-1", "uid-actor-1", dial.clone())
        .unwrap();
    let s1 = server.clone();
    let first_req = tokio::spawn(async move {
        body_string(
            s1.handle_http(get(Some("team-a/actor-1"), None), None)
                .await,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), first_ready.notified())
        .await
        .unwrap();
    server.deactivate("team-a", "actor-1", "uid-actor-1").await;
    let first_body = tokio::time::timeout(Duration::from_secs(5), first_req)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        first_body, "actor-1",
        "first request completed after deactivation"
    );
    server
        .activate("team-a", "actor-2", "uid-actor-2", dial)
        .unwrap();
    let s2 = server.clone();
    let second_req = tokio::spawn(async move {
        body_string(
            s2.handle_http(get(Some("team-a/actor-2"), None), None)
                .await,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), second_ready.notified())
        .await
        .unwrap();
    release_first.notify_one();
    release_second.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), second_req)
            .await
            .unwrap()
            .unwrap(),
        "actor-2"
    );
    server.deactivate("team-a", "actor-2", "uid-actor-2").await;
}

#[tokio::test]
async fn serve_http_routes_by_header() {
    let (s, _f) = new_test_server("http://actor.internal:80");
    let hosts = Arc::new(Mutex::new(Vec::<String>::new()));
    s.activate("team-a", "actor-1", "uid-actor-1", test_dial())
        .unwrap();
    let h = hosts.clone();
    s.set_transport(
        "team-a",
        "actor-1",
        transport(move |r| {
            h.lock().push(
                r.headers()
                    .get(http::header::HOST)
                    .map(|v| v.to_str().unwrap().to_string())
                    .unwrap_or_default(),
            );
            Ok(status(204))
        }),
    );
    let cases: Vec<(&str, Option<&str>, &str, bool, u16)> = vec![
        (
            "active actor",
            Some("client.example"),
            "team-a/actor-1",
            false,
            204,
        ),
        (
            "mixed-case routing header",
            None,
            "team-a/actor-1",
            true,
            204,
        ),
        (
            "host does not identify actor",
            Some("actor-2.team-b.example"),
            "team-a/actor-1",
            false,
            204,
        ),
        ("empty host", None, "team-a/actor-1", false, 204),
        ("wrong actor", None, "team-a/actor-2", false, 421),
        ("wrong hive", None, "team-b/actor-1", false, 421),
        ("missing actor name", None, "team-a/", false, 421),
        ("missing hive", None, "/actor-1", false, 421),
        ("invalid actor name", None, "team-a/INVALID", false, 421),
    ];
    for (name, host, target, mixed, want) in cases {
        let mut req = get(None, host);
        let key = if mixed {
            "Hk-Target-Actor"
        } else {
            TARGET_ACTOR_HEADER
        };
        req.headers_mut().insert(
            http::header::HeaderName::from_bytes(key.as_bytes()).unwrap(),
            target.parse().unwrap(),
        );
        let resp = s.handle_http(req, None).await;
        assert_eq!(resp.status().as_u16(), want, "{name}");
        if want == 421 {
            assert_eq!(
                resp.headers().get(STALE_ASSIGNMENT_HEADER).unwrap(),
                "true",
                "{name}"
            );
        }
    }
    assert_eq!(
        *hosts.lock(),
        vec!["client.example", "", "actor-2.team-b.example", ""]
    );
}

#[tokio::test]
async fn rejects_duplicate_or_comma_target_actor_headers() {
    let (s, _f) = new_test_server("http://actor.internal:80");
    let called = Arc::new(AtomicU32::new(0));
    s.activate("team-a", "actor-1", "uid-actor-1", test_dial())
        .unwrap();
    let c = called.clone();
    s.set_transport(
        "team-a",
        "actor-1",
        transport(move |_| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(status(204))
        }),
    );
    for (name, values) in [
        (
            "duplicate different actors",
            vec!["team-a/actor-1", "secret/x"],
        ),
        (
            "duplicate identical actors",
            vec!["team-a/actor-1", "team-a/actor-1"],
        ),
        ("comma-joined actors", vec!["team-a/actor-1,secret/x"]),
    ] {
        let mut req = get(None, Some("actor-1.team-a.actors.example"));
        for v in values {
            req.headers_mut()
                .append(TARGET_ACTOR_HEADER, v.parse().unwrap());
        }
        let resp = s.handle_http(req, None).await;
        assert_eq!(resp.status(), StatusCode::MISDIRECTED_REQUEST, "{name}");
        assert_eq!(resp.headers().get(STALE_ASSIGNMENT_HEADER).unwrap(), "true");
        assert_eq!(
            called.load(Ordering::SeqCst),
            0,
            "{name}: upstream was called"
        );
    }
}

#[tokio::test]
async fn strips_stale_assignment_header_from_actor() {
    let (s, _f) = new_test_server("http://actor.internal:80");
    s.activate("team-a", "actor-1", "uid-actor-1", test_dial())
        .unwrap();
    s.set_transport(
        "team-a",
        "actor-1",
        transport(|_| {
            let mut r = status(200);
            r.headers_mut()
                .insert(STALE_ASSIGNMENT_HEADER, "true".parse().unwrap());
            Ok(r)
        }),
    );
    let resp = s.handle_http(get(Some("team-a/actor-1"), None), None).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get(STALE_ASSIGNMENT_HEADER).is_none());
}

#[tokio::test]
async fn honors_target_port_header() {
    let (s, _f) = new_test_server("http://actor.internal:80");
    s.activate("team-a", "actor-1", "uid-actor-1", test_dial())
        .unwrap();
    let seen = Arc::new(Mutex::new(None::<(String, http::HeaderMap)>));
    let sn = seen.clone();
    s.set_transport(
        "team-a",
        "actor-1",
        transport(move |r| {
            *sn.lock() = Some((
                r.uri().authority().unwrap().to_string(),
                r.headers().clone(),
            ));
            Ok(status(204))
        }),
    );
    for (name, port, want) in [
        (
            "arbitrary port overrides upstream's",
            Some("9090"),
            "actor.internal:9090",
        ),
        (
            "absent falls back to upstream's own port",
            None,
            "actor.internal:80",
        ),
        (
            "invalid falls back to upstream's own port",
            Some("not-a-port"),
            "actor.internal:80",
        ),
        (
            "out of range falls back to upstream's own port",
            Some("70000"),
            "actor.internal:80",
        ),
    ] {
        let mut req = get(Some("team-a/actor-1"), Some("client.example"));
        if let Some(p) = port {
            req.headers_mut()
                .insert(TARGET_PORT_HEADER, p.parse().unwrap());
        }
        assert_eq!(
            s.handle_http(req, None).await.status(),
            StatusCode::NO_CONTENT,
            "{name}"
        );
        let (authority, headers) = seen.lock().take().unwrap();
        assert_eq!(authority, want, "{name}");
        assert_eq!(
            headers.get(http::header::HOST).unwrap(),
            "client.example",
            "{name}"
        );
        assert!(
            headers.get(TARGET_PORT_HEADER).is_none() && headers.get(TARGET_ACTOR_HEADER).is_none(),
            "{name}: routing headers leaked"
        );
    }
}

#[tokio::test]
async fn serve_connect_validates_method_and_authority() {
    let (s, _f) = new_test_server("http://actor.internal:80");
    s.activate("team-a", "actor-1", "uid-actor-1", test_dial())
        .unwrap();
    for (name, method, authority, target, want) in [
        (
            "rejects non CONNECT",
            Method::GET,
            "actor-1.team-a.example:9090",
            "team-a/actor-1",
            405,
        ),
        (
            "requires authority port",
            Method::CONNECT,
            "actor-1.team-a.example",
            "team-a/actor-1",
            400,
        ),
        (
            "rejects invalid authority port",
            Method::CONNECT,
            "actor-1.team-a.example:70000",
            "team-a/actor-1",
            400,
        ),
        (
            "rejects inactive actor",
            Method::CONNECT,
            "actor-2.team-a.example:9090",
            "team-a/actor-2",
            421,
        ),
    ] {
        let req = Request::builder()
            .method(method)
            .uri(authority)
            .header(TARGET_ACTOR_HEADER, target)
            .body(Empty::<Bytes>::new())
            .unwrap();
        assert_eq!(
            s.handle_connect(req).await.status().as_u16(),
            want,
            "{name}"
        );
    }
}

struct IdleClosing(Arc<AtomicU32>);
impl Transport for IdleClosing {
    fn round_trip(
        &self,
        _: Request<ProxyBody>,
    ) -> futures::future::BoxFuture<'static, Result<Response<ProxyBody>, BoxError>> {
        Box::pin(async { Ok(status(204)) })
    }
    fn close_idle(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn deactivate_closes_idle_upstream_connections() {
    let (s, _f) = new_test_server("http://actor.internal:80");
    let closed = Arc::new(AtomicU32::new(0));
    s.activate("team-a", "actor-1", "uid-actor-1", test_dial())
        .unwrap();
    s.set_transport("team-a", "actor-1", Arc::new(IdleClosing(closed.clone())));
    assert_eq!(
        s.handle_http(get(Some("team-a/actor-1"), None), None)
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    s.deactivate("team-a", "actor-1", "uid-actor-1").await;
    assert!(
        closed.load(Ordering::SeqCst) > 0,
        "deactivate did not close idle connections"
    );
}

#[tokio::test]
async fn inactive() {
    let (s, _f) = new_test_server("http://127.0.0.1:1");
    assert_eq!(
        s.handle_http(get(Some("team-a/actor-1"), None), None)
            .await
            .status(),
        StatusCode::MISDIRECTED_REQUEST,
        "before activation"
    );
    s.activate("team-a", "actor-1", "uid-actor-1", test_dial())
        .unwrap();
    s.deactivate("team-a", "actor-1", "uid-actor-1").await;
    assert_eq!(
        s.handle_http(get(Some("team-a/actor-1"), None), None)
            .await
            .status(),
        StatusCode::MISDIRECTED_REQUEST,
        "after deactivation"
    );
}

async fn tls_handshake(
    server: Arc<rustls::ServerConfig>,
    client: Arc<rustls::ClientConfig>,
) -> bool {
    let (a, b) = tokio::io::duplex(64 << 10);
    let srv = tokio::spawn(async move {
        let mut s = tokio_rustls::TlsAcceptor::from(server).accept(a).await?;
        s.write_all(b"y").await?;
        s.shutdown().await?;
        Ok::<_, std::io::Error>(())
    });
    let name = rustls::pki_types::ServerName::try_from("worker.test").unwrap();
    let client_ok = match tokio_rustls::TlsConnector::from(client)
        .connect(name, b)
        .await
    {
        Ok(mut c) => {
            let mut buf = [0u8; 1];
            // A TLS 1.3 server that rejects the client cert alerts after the
            // client's handshake completes.
            matches!(c.read(&mut buf).await, Ok(1))
        }
        Err(_) => false,
    };
    let server_ok = tokio::time::timeout(Duration::from_secs(5), srv)
        .await
        .unwrap()
        .unwrap()
        .is_ok();
    client_ok && server_ok
}

#[tokio::test]
async fn mutual_tls_client_authentication() {
    let f = files();
    let s = Server::new(config(&f, "http://actor.internal:80")).unwrap();
    let untrusted = TestCa::new();
    let leaf = |ca: &TestCa, id: &str| {
        ca.issue(&LeafOpts {
            uri: Some(id.into()),
            client: true,
            ..Default::default()
        })
    };
    for (name, cert, want_ok) in [
        ("allowed client", leaf(&f.ca, ROUTER_ID), true),
        (
            "wrong client ID",
            leaf(
                &f.ca,
                "spiffe://cluster.local/ns/hivekeep-system/sa/not-the-gateway",
            ),
            false,
        ),
        ("untrusted client", leaf(&untrusted, ROUTER_ID), false),
    ] {
        let ok = tls_handshake(
            s.tls().server_config(&[]).unwrap(),
            insecure_client(Some(&cert), &[]),
        )
        .await;
        assert_eq!(ok, want_ok, "{name}");
    }
}

#[tokio::test]
async fn mutual_tls_client_authentication_reloads_trust_bundle() {
    let f = files();
    let s = Server::new(config(&f, "http://actor.internal:80")).unwrap();
    let rotated = TestCa::new();
    let cert = rotated.issue(&LeafOpts {
        uri: Some(ROUTER_ID.into()),
        client: true,
        ..Default::default()
    });
    assert!(
        !tls_handshake(
            s.tls().server_config(&[]).unwrap(),
            insecure_client(Some(&cert), &[])
        )
        .await,
        "not-yet-trusted CA accepted"
    );
    std::fs::write(&f.trust, format!("{}{}", f.ca.pem(), rotated.pem())).unwrap();
    assert!(
        tls_handshake(
            s.tls().server_config(&[]).unwrap(),
            insecure_client(Some(&cert), &[])
        )
        .await,
        "rotated CA rejected"
    );
}

/// A backend reporting the HTTP version of each request it serves.
async fn mirror_backend(h1: bool, h2c: bool) -> (String, mpsc::UnboundedReceiver<http::Version>) {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((s, _)) = lis.accept().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |r: Request<hyper::body::Incoming>| {
                    let _ = tx.send(r.version());
                    async {
                        Ok::<_, std::convert::Infallible>(Response::new(Empty::<Bytes>::new()))
                    }
                });
                let io = TokioIo::new(s);
                match (h1, h2c) {
                    (true, false) => {
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, svc)
                            .await;
                    }
                    _ => {
                        let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                            .serve_connection(io, svc)
                            .await;
                    }
                }
            });
        }
    });
    (addr, rx)
}

async fn recv<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out")
        .unwrap()
}

#[tokio::test]
async fn serve_negotiates_h2() {
    let f = files();
    let (backend, mut seen) = mirror_backend(true, true).await;
    let s = Server::new(config(&f, &format!("http://{backend}"))).unwrap();
    s.activate("team-a", "actor-1", "uid-actor-1", test_dial())
        .unwrap();
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let served = tokio::spawn({
        let (s, c) = (s.clone(), cancel.clone());
        async move { s.serve(lis, c).await }
    });
    let client_cert = f.ca.issue(&LeafOpts {
        uri: Some(ROUTER_ID.into()),
        client: true,
        ..Default::default()
    });
    let connect = |alpn: &'static [u8]| {
        let cfg = insecure_client(Some(&client_cert), &[alpn]);
        async move {
            let tcp = TcpStream::connect(addr).await.unwrap();
            let name = rustls::pki_types::ServerName::try_from("worker.test").unwrap();
            tokio_rustls::TlsConnector::from(cfg)
                .connect(name, tcp)
                .await
                .unwrap()
        }
    };
    let req = |method: Method, ct: Option<&str>| {
        let mut b = Request::builder()
            .method(method)
            .uri(format!("https://{addr}/"))
            .header(http::header::HOST, "team-a-agent.example.com")
            .header(TARGET_ACTOR_HEADER, "team-a/actor-1");
        if let Some(ct) = ct {
            b = b.header(http::header::CONTENT_TYPE, ct);
        }
        b.body(Empty::<Bytes>::new()).unwrap()
    };
    let tls = connect(b"h2").await;
    assert_eq!(tls.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
    let (mut h2, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tls))
            .await
            .unwrap();
    tokio::spawn(conn);
    let res = h2
        .send_request(req(Method::POST, Some("application/grpc")))
        .await
        .unwrap();
    assert_eq!(res.version(), http::Version::HTTP_2);
    assert_eq!(
        recv(&mut seen).await,
        http::Version::HTTP_2,
        "gRPC must reach the Actor over HTTP/2"
    );
    h2.send_request(req(Method::GET, None)).await.unwrap();
    assert_eq!(
        recv(&mut seen).await,
        http::Version::HTTP_11,
        "plain GET over h2 reaches the Actor as HTTP/1.1"
    );
    let tls = connect(b"http/1.1").await;
    let (mut h1, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
        .await
        .unwrap();
    tokio::spawn(conn);
    let res = h1.send_request(req(Method::GET, None)).await.unwrap();
    assert_eq!(res.version(), http::Version::HTTP_11);
    assert_eq!(recv(&mut seen).await, http::Version::HTTP_11);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), served)
        .await
        .expect("serve did not stop")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn deactivate_cancels_inflight_request() {
    let (s, _f) = new_test_server("http://actor.internal:80");
    s.activate("team-a", "actor-1", "uid-actor-1", test_dial())
        .unwrap();
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    struct OnDrop(Arc<Notify>);
    impl Drop for OnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }
    let (st, dr) = (started.clone(), dropped.clone());
    s.set_transport(
        "team-a",
        "actor-1",
        Arc::new(FnTransport(move |_r| {
            let (st, guard) = (st.clone(), OnDrop(dr.clone()));
            Box::pin(async move {
                let _guard = guard;
                st.notify_one();
                std::future::pending::<Result<Response<ProxyBody>, BoxError>>().await
            }) as futures::future::BoxFuture<'static, _>
        })),
    );
    let cancel = s.activation_cancelled("team-a", "actor-1").unwrap();
    let s2 = s.clone();
    let done = tokio::spawn(async move {
        s2.handle_http(get(Some("team-a/actor-1"), None), None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    s.deactivate("team-a", "actor-1", "uid-actor-1").await;
    tokio::time::timeout(Duration::from_secs(5), done)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), dropped.notified())
        .await
        .expect("in-flight request not canceled");
    assert!(cancel.is_cancelled(), "canceled by deactivation");
}

#[test]
fn is_grpc_cases() {
    const HTTP_11: http::Version = http::Version::HTTP_11;
    const HTTP_2: http::Version = http::Version::HTTP_2;
    for (name, v, m, ct, want) in [
        ("grpc", HTTP_2, Method::POST, Some("application/grpc"), true),
        (
            "grpc+proto",
            HTTP_2,
            Method::POST,
            Some("application/grpc+proto"),
            true,
        ),
        (
            "grpc with params",
            HTTP_2,
            Method::POST,
            Some("application/grpc;charset=utf-8"),
            true,
        ),
        (
            "uppercase content type",
            HTTP_2,
            Method::POST,
            Some("Application/GRPC"),
            true,
        ),
        (
            "uppercase with subtype",
            HTTP_2,
            Method::POST,
            Some("APPLICATION/GRPC+PROTO"),
            true,
        ),
        (
            "grpc-web is not grpc",
            HTTP_2,
            Method::POST,
            Some("application/grpc-web+proto"),
            false,
        ),
        (
            "grpc content type over http/1.1",
            HTTP_11,
            Method::POST,
            Some("application/grpc"),
            false,
        ),
        (
            "non-POST",
            HTTP_2,
            Method::GET,
            Some("application/grpc"),
            false,
        ),
        (
            "plain h2 json",
            HTTP_2,
            Method::POST,
            Some("application/json"),
            false,
        ),
        ("no content type", HTTP_2, Method::POST, None, false),
    ] {
        assert_eq!(is_grpc(v, &m, ct), want, "{name}");
    }
}

#[tokio::test]
async fn protocol_mirror_transport() {
    const HTTP_11: http::Version = http::Version::HTTP_11;
    const HTTP_2: http::Version = http::Version::HTTP_2;
    for (name, h1, h2c, v, m, ct, want) in [
        (
            "h1 GET to h1-only actor",
            true,
            false,
            HTTP_11,
            Method::GET,
            None,
            Some(HTTP_11),
        ),
        (
            "h2 GET downgraded for h1-only actor",
            true,
            false,
            HTTP_2,
            Method::GET,
            None,
            Some(HTTP_11),
        ),
        (
            "h2 POST json downgraded for h1-only actor",
            true,
            false,
            HTTP_2,
            Method::POST,
            Some("application/json"),
            Some(HTTP_11),
        ),
        (
            "grpc-web stays h1 for h1-only actor",
            true,
            false,
            HTTP_2,
            Method::POST,
            Some("application/grpc-web+proto"),
            Some(HTTP_11),
        ),
        (
            "grpc to h1-only actor fails",
            true,
            false,
            HTTP_2,
            Method::POST,
            Some("application/grpc"),
            None,
        ),
        (
            "grpc to grpc actor",
            true,
            true,
            HTTP_2,
            Method::POST,
            Some("application/grpc"),
            Some(HTTP_2),
        ),
        (
            "grpc+proto to grpc actor",
            true,
            true,
            HTTP_2,
            Method::POST,
            Some("application/grpc+proto"),
            Some(HTTP_2),
        ),
        (
            "h2 GET downgraded even for h2c-capable actor",
            true,
            true,
            HTTP_2,
            Method::GET,
            None,
            Some(HTTP_11),
        ),
    ] {
        let (addr, mut seen) = mirror_backend(h1, h2c).await;
        let t = ProtocolMirrorTransport::new(None);
        let mut b = Request::builder()
            .method(m)
            .uri(format!("http://{addr}/"))
            .version(v);
        if let Some(ct) = ct {
            b = b.header(http::header::CONTENT_TYPE, ct);
        }
        let res = t.round_trip(b.body(empty()).unwrap()).await;
        match want {
            None => assert!(res.is_err(), "{name}: no silent HTTP/1.1 fallback for gRPC"),
            Some(w) => {
                res.unwrap_or_else(|e| panic!("{name}: {e}"));
                assert_eq!(recv(&mut seen).await, w, "{name}");
            }
        }
    }
}

#[tokio::test]
async fn serve_connect_dials_the_sandbox() {
    let actor = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let actor_addr = actor.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (mut c, _) = actor.accept().await.unwrap();
        c.write_all(b"hello from the sandbox").await.unwrap();
    });
    let f = files();
    let s = Server::new(config(&f, "http://actor.internal:80")).unwrap();
    let dialed = Arc::new(Mutex::new(String::new()));
    let d = dialed.clone();
    let dial: Dial = Arc::new(move |addr| {
        *d.lock() = addr;
        let a = actor_addr.clone();
        Box::pin(async move { Ok(Box::new(TcpStream::connect(a).await?) as BoxIo) })
    });
    s.activate("team-a", "actor-1", "uid-actor-1", dial)
        .unwrap();
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap();
    let cancel = CancellationToken::new();
    tokio::spawn({
        let (s, c) = (s.clone(), cancel.clone());
        async move { s.serve_connect(lis, c).await }
    });
    let cert = f.ca.issue(&LeafOpts {
        uri: Some(ROUTER_ID.into()),
        client: true,
        ..Default::default()
    });
    let tcp = TcpStream::connect(addr).await.unwrap();
    let tls = tokio_rustls::TlsConnector::from(insecure_client(Some(&cert), &[b"h2"]))
        .connect(
            rustls::pki_types::ServerName::try_from("worker.test").unwrap(),
            tcp,
        )
        .await
        .unwrap();
    let (send, conn) = h2::client::handshake(tls).await.unwrap();
    tokio::spawn(conn);
    let mut send = send.ready().await.unwrap();
    let req = Request::builder()
        .method(Method::CONNECT)
        .uri("actor-1.team-a.example:9090")
        .header(TARGET_ACTOR_HEADER, "team-a/actor-1")
        .body(())
        .unwrap();
    let (resp, _stream) = send.send_request(req, false).unwrap();
    let resp = tokio::time::timeout(Duration::from_secs(5), resp)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "the tunnel never reached the sandbox"
    );
    let mut body = resp.into_body();
    let mut got = Vec::new();
    while got.len() < 22 {
        let d = tokio::time::timeout(Duration::from_secs(5), body.data())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let _ = body.flow_control().release_capacity(d.len());
        got.extend_from_slice(&d);
    }
    assert_eq!(got, b"hello from the sandbox");
    assert_eq!(*dialed.lock(), "actor.internal:9090");
    cancel.cancel();
}

#[tokio::test]
async fn routes_each_actor_to_its_own_sandbox() {
    let (s, _f) = new_test_server("http://actor.internal:80");
    let (tx, mut served) = mpsc::unbounded_channel();
    for name in ["actor-1", "actor-2"] {
        s.activate("team-a", name, &format!("uid-{name}"), test_dial())
            .unwrap();
        let tx = tx.clone();
        s.set_transport(
            "team-a",
            name,
            transport(move |_| {
                tx.send(name).unwrap();
                Ok(status(204))
            }),
        );
    }
    for name in ["actor-2", "actor-1"] {
        let resp = s
            .handle_http(get(Some(&format!("team-a/{name}")), None), None)
            .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(served.recv().await.unwrap(), name);
    }
}

#[tokio::test]
async fn deactivate_leaves_other_actors_serving() {
    let (s, _f) = new_test_server("http://actor.internal:80");
    for name in ["actor-1", "actor-2"] {
        s.activate("team-a", name, &format!("uid-{name}"), test_dial())
            .unwrap();
        s.set_transport("team-a", name, transport(|_| Ok(status(204))));
    }
    s.deactivate("team-a", "actor-1", "uid-actor-1").await;
    for (name, want) in [("actor-1", 421), ("actor-2", 204)] {
        let resp = s
            .handle_http(get(Some(&format!("team-a/{name}")), None), None)
            .await;
        assert_eq!(resp.status().as_u16(), want, "{name}");
    }
}

#[tokio::test]
async fn reincarnation_survives_stale_deactivate() {
    let (s, _f) = new_test_server("http://actor.internal:80");
    s.activate("team-a", "actor-1", "uid-old", test_dial())
        .unwrap();
    let old = s.activation_cancelled("team-a", "actor-1").unwrap();
    s.activate("team-a", "actor-1", "uid-new", test_dial())
        .expect("new incarnation");
    assert!(
        old.is_cancelled(),
        "the replaced incarnation was not canceled"
    );
    s.deactivate("team-a", "actor-1", "uid-old").await;
    assert_eq!(
        s.active_uid("team-a", "actor-1").as_deref(),
        Some("uid-new")
    );
    assert!(
        s.activate("team-a", "actor-1", "uid-new", test_dial())
            .is_err(),
        "same incarnation twice"
    );
    s.deactivate("team-a", "actor-1", "uid-new").await;
    assert!(s.active_uid("team-a", "actor-1").is_none());
}

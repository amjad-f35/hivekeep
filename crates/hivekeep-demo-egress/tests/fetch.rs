//! The `/` fetch endpoint.

mod support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use hivekeep_demo_egress::tls::issuer_string;
use hivekeep_demo_egress::wire::{FetchRequest, FetchResponse};
use hivekeep_demo_egress::{Fetch, Fetched, HttpFetcher, Outbound};
use support::*;

const TRACEPARENT: &str = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";

/// A default client that records what it was asked and answers a teapot.
#[derive(Default)]
struct Recording(Mutex<Vec<Outbound>>);

#[async_trait]
impl Fetch for Recording {
    async fn get(&self, request: Outbound) -> Result<Fetched, String> {
        self.0.lock().unwrap().push(request);
        Ok(Fetched {
            status: 418,
            body: b"hello from upstream".to_vec(),
            ..Default::default()
        })
    }
}

struct Blocked;

#[async_trait]
impl Fetch for Blocked {
    async fn get(&self, _: Outbound) -> Result<Fetched, String> {
        Err("request failed: blocked".into())
    }
}

fn req(url: &str) -> FetchRequest {
    FetchRequest {
        url: url.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn fetch_relays_status_body_and_trace_context() {
    let recording = Arc::new(Recording::default());
    let body = serde_json::to_string(&req("https://allowed.example/")).unwrap();
    let (status, _, raw) = call(
        app_with(recording.clone()),
        "POST",
        "/",
        body,
        &[("traceparent", TRACEPARENT)],
    )
    .await;
    assert_eq!(status, 418);
    let got: FetchResponse = serde_json::from_slice(&raw).unwrap();
    assert_eq!(
        (got.status_code, got.body.as_str()),
        (418, "hello from upstream")
    );
    let seen = recording.0.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].url, "https://allowed.example/");
    assert_eq!(seen[0].traceparent.as_deref(), Some(TRACEPARENT));
}

#[tokio::test]
async fn fetch_http_and_https() {
    init();
    let http_url = serve(handler(|_| async { reply(200, "from http") }), None).await;
    let tls = loopback_tls();
    let https_url = serve(
        handler(|_| async { reply(200, "from https") }),
        Some(tls.server.clone()),
    )
    .await;
    // The default client verifies origins with the roots it was built with:
    // the projected trust bundle in a cluster, the test CA here.
    let app = app_with(Arc::new(HttpFetcher::with_roots(&tls.root_pem).unwrap()));
    let want_cert = issuer_string(&tls.leaf_der);
    assert!(!want_cert.is_empty());
    for (name, url, want, cert) in [
        ("http", http_url, "from http", String::new()),
        ("https", https_url, "from https", want_cert),
    ] {
        let (status, got): (u16, FetchResponse) = post_json(app.clone(), "/", &req(&url)).await;
        assert_eq!(status, 200, "{name}: {got:?}");
        assert_eq!(
            (got.status_code, got.body.as_str(), got.server_cert.as_str()),
            (200, want, cert.as_str()),
            "{name}"
        );
        assert_eq!(got.tls, name == "https", "{name}");
    }
}

#[tokio::test]
async fn invalid_requests() {
    for (name, method, body, want) in [
        ("method", "GET", "{}", 405),
        ("malformed JSON", "POST", "{", 400),
        (
            "missing hostname",
            "POST",
            r#"{"url":"https:///path"}"#,
            400,
        ),
        (
            "unsupported scheme",
            "POST",
            r#"{"url":"file:///etc/passwd"}"#,
            400,
        ),
    ] {
        let (status, body) = status_of(app(), method, "/", body).await;
        assert_eq!(status, want, "{name}: {body}");
    }
    let (_, headers, _) = call(app(), "GET", "/", "{}", &[]).await;
    assert_eq!(headers.get("allow").unwrap(), "POST");
}

#[tokio::test]
async fn outbound_failure_is_bad_gateway() {
    let (status, got): (u16, FetchResponse) = post_json(
        app_with(Arc::new(Blocked)),
        "/",
        &req("https://example.com/"),
    )
    .await;
    assert_eq!(status, 502);
    assert!(got.error.contains("blocked"));
}

#[tokio::test]
async fn request_scoped_tls_and_http1() {
    init();
    let tls = loopback_tls();
    let saw_http2 = Arc::new(AtomicBool::new(false));
    let flag = saw_http2.clone();
    let url = serve(
        handler(move |r| {
            let flag = flag.clone();
            async move {
                if r.version() != http::Version::HTTP_11 {
                    flag.store(true, Ordering::SeqCst);
                }
                reply(200, "trusted")
            }
        }),
        Some(tls.server.clone()),
    )
    .await;
    let input = FetchRequest {
        url,
        root_ca: tls.root_pem.clone(),
        http1: true,
    };
    let (status, got): (u16, FetchResponse) = post_json(app(), "/", &input).await;
    assert_eq!(status, 200, "{got:?}");
    assert_eq!(
        (
            got.status_code,
            got.body.as_str(),
            got.protocol.as_str(),
            got.tls
        ),
        (200, "trusted", "HTTP/1.1", true)
    );
    assert!(
        !saw_http2.load(Ordering::SeqCst),
        "origin saw a non-HTTP/1.1 request"
    );
}

#[tokio::test]
async fn tls_verification_failure() {
    init();
    let tls = loopback_tls();
    let url = serve(
        handler(|_| async { reply(200, "should not arrive") }),
        Some(tls.server.clone()),
    )
    .await;
    let (status, got): (u16, FetchResponse) = post_json(app(), "/", &req(&url)).await;
    assert_eq!(status, 502);
    assert!(
        !got.error.is_empty() && got.body.is_empty() && got.status_code == 0,
        "{got:?}"
    );
}

#[tokio::test]
async fn tls_wrong_root_fails() {
    init();
    let tls = loopback_tls();
    let url = serve(
        handler(|_| async { reply(200, "should not arrive") }),
        Some(tls.server.clone()),
    )
    .await;
    let other = loopback_tls();
    let input = FetchRequest {
        url,
        root_ca: other.root_pem,
        http1: false,
    };
    let (status, _): (u16, FetchResponse) = post_json(app(), "/", &input).await;
    assert_eq!(status, 502);
}

#[tokio::test]
async fn does_not_follow_redirects() {
    init();
    let redirected = Arc::new(AtomicBool::new(false));
    let flag = redirected.clone();
    let target = serve(
        handler(move |_| {
            let flag = flag.clone();
            async move {
                flag.store(true, Ordering::SeqCst);
                reply(200, "redirected")
            }
        }),
        None,
    )
    .await;
    let source = serve(
        handler(move |_| {
            let target = target.clone();
            async move {
                http::Response::builder()
                    .status(302)
                    .header("location", target)
                    .body(Default::default())
                    .unwrap()
            }
        }),
        None,
    )
    .await;
    let input = FetchRequest {
        url: source,
        http1: true,
        ..Default::default()
    };
    let (status, got): (u16, FetchResponse) = post_json(app(), "/", &input).await;
    assert_eq!(status, 302, "{got:?}");
    assert!(
        !redirected.load(Ordering::SeqCst),
        "redirect target received a request"
    );
    assert_eq!(got.status_code, 302);
    assert!(!got.protocol.is_empty());
}

#[tokio::test]
async fn readyz_answers_ok() {
    let (status, _, body) = call(app(), "GET", "/readyz", "", &[]).await;
    assert_eq!((status, body.as_slice()), (200, b"ok\n".as_slice()));
}

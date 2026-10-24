//! The endpoints that write requests byte by byte: `/mux`, `/post` and
//! `/slow-headers`.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hivekeep_demo_egress::limits::{MUX_BODY_SIZE, MUX_REQUEST_COUNT};
use hivekeep_demo_egress::wire::{FetchRequest, FetchResponse};
use hivekeep_demo_egress::{router, AppState, Delays, HttpFetcher};
use http_body_util::BodyExt;
use support::*;
use tokio::sync::Notify;

fn req(url: &str) -> FetchRequest {
    FetchRequest {
        url: url.into(),
        ..Default::default()
    }
}

fn app_with_delays(delays: Delays) -> axum::Router {
    init();
    let mut state = AppState::new(Arc::new(HttpFetcher::system().unwrap()));
    state.delays = delays;
    router(state)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mux_completes_every_request() {
    init();
    let completed = Arc::new(AtomicUsize::new(0));
    let wrong = Arc::new(AtomicUsize::new(0));
    let (c, w) = (completed.clone(), wrong.clone());
    let url = serve(
        handler(move |r| {
            let (c, w) = (c.clone(), w.clone());
            async move {
                if r.method() != http::Method::POST || r.uri().path() != "/fetch" {
                    w.fetch_add(1, Ordering::SeqCst);
                }
                let body = r
                    .into_body()
                    .collect()
                    .await
                    .map(|b| b.to_bytes().len())
                    .unwrap_or(0);
                if body != MUX_BODY_SIZE {
                    w.fetch_add(1, Ordering::SeqCst);
                }
                c.fetch_add(1, Ordering::SeqCst);
                reply(200, "")
            }
        }),
        None,
    )
    .await;
    let (status, got): (u16, FetchResponse) =
        post_json(app(), "/mux", &req(&format!("{url}/fetch"))).await;
    assert_eq!(status, 200, "{got:?}");
    assert_eq!(got.status_code, 200);
    assert_eq!(completed.load(Ordering::SeqCst), MUX_REQUEST_COUNT);
    assert_eq!(
        wrong.load(Ordering::SeqCst),
        0,
        "origin saw a wrong method, path or body size"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mux_non_200_fails() {
    init();
    let seen = Arc::new(AtomicUsize::new(0));
    let s = seen.clone();
    let url = serve(
        handler(move |r| {
            let s = s.clone();
            async move {
                let _ = r.into_body().collect().await;
                if s.fetch_add(1, Ordering::SeqCst) + 1 == MUX_REQUEST_COUNT {
                    return reply(500, "boom\n");
                }
                reply(200, "")
            }
        }),
        None,
    )
    .await;
    let (status, got): (u16, FetchResponse) =
        post_json(app(), "/mux", &req(&format!("{url}/fetch"))).await;
    assert_eq!(status, 500, "{got:?}");
}

#[tokio::test]
async fn mux_invalid_requests() {
    for (name, method, body, want) in [
        ("method", "GET", "{}", 405),
        ("malformed JSON", "POST", "{", 400),
        ("missing hostname", "POST", r#"{"url":"http:///path"}"#, 400),
        (
            "https scheme",
            "POST",
            r#"{"url":"https://example.com/fetch"}"#,
            400,
        ),
    ] {
        let (status, body) = status_of(app(), method, "/mux", body).await;
        assert_eq!(status, want, "{name}: {body}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_sends_headers_before_body() {
    init();
    let tls = loopback_tls();
    let headers_received = Arc::new(Notify::new());
    let body_bytes = Arc::new(AtomicUsize::new(usize::MAX));
    let (h, b) = (headers_received.clone(), body_bytes.clone());
    let url = serve(
        handler(move |r| {
            let (h, b) = (h.clone(), b.clone());
            async move {
                let ok = r.method() == http::Method::POST && r.uri().path() == "/fetch";
                b.store(0, Ordering::SeqCst);
                h.notify_one();
                let n = r
                    .into_body()
                    .collect()
                    .await
                    .map(|x| x.to_bytes().len())
                    .unwrap_or(0);
                b.store(n, Ordering::SeqCst);
                reply(if ok { 200 } else { 400 }, "")
            }
        }),
        Some(tls.server.clone()),
    )
    .await;
    let app = app_with_delays(Delays {
        post_body: Duration::from_millis(50),
        ..Delays::default()
    });
    let input = FetchRequest {
        url: format!("{url}/fetch"),
        root_ca: tls.root_pem.clone(),
        http1: false,
    };
    let call =
        tokio::spawn(async move { post_json::<_, FetchResponse>(app, "/post", &input).await });

    headers_received.notified().await;
    assert_eq!(
        body_bytes.load(Ordering::SeqCst),
        0,
        "body arrived together with the headers"
    );
    let (status, got) = call.await.unwrap();
    assert_eq!(status, 200, "{got:?}");
    assert!(got.tls);
    assert_eq!(body_bytes.load(Ordering::SeqCst), MUX_BODY_SIZE);
}

#[tokio::test]
async fn post_invalid_requests() {
    let (status, _) = status_of(app(), "GET", "/post", "").await;
    assert_eq!(status, 405, "method not allowed");
    let input = FetchRequest {
        url: "https://example.com/fetch".into(),
        root_ca: "not-a-cert".into(),
        http1: false,
    };
    let (status, got): (u16, FetchResponse) = post_json(app(), "/post", &input).await;
    assert_eq!(status, 400, "invalid rootCA: {got:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_headers_waits_before_finishing_the_head() {
    init();
    let completed = Arc::new(Notify::new());
    let c = completed.clone();
    let url = serve(
        handler(move |r| {
            let c = c.clone();
            async move {
                let status = if r.method() == http::Method::GET {
                    200
                } else {
                    405
                };
                c.notify_one();
                reply(status, "ok")
            }
        }),
        None,
    )
    .await;
    let app = app_with_delays(Delays {
        slow_header: Duration::from_millis(80),
        ..Delays::default()
    });
    let input = req(&format!("{url}/"));
    let call =
        tokio::spawn(
            async move { post_json::<_, FetchResponse>(app, "/slow-headers", &input).await },
        );

    tokio::select! {
        _ = completed.notified() => panic!("headers completed before the delay elapsed"),
        _ = tokio::time::sleep(Duration::from_millis(20)) => {}
    }
    completed.notified().await;
    let (status, got) = call.await.unwrap();
    assert_eq!(status, 200, "{got:?}");
    assert_eq!(got.body, "ok");
}

#[tokio::test]
async fn slow_headers_invalid_requests() {
    let (status, _) = status_of(app(), "GET", "/slow-headers", "").await;
    assert_eq!(status, 405, "method not allowed");
    let (status, got): (u16, FetchResponse) =
        post_json(app(), "/slow-headers", &req("https://example.net/")).await;
    assert_eq!(status, 400, "https rejected: {got:?}");
}

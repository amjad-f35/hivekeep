use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::time::Instant;

use super::*;

/// A stand-in for the Envoy admin interface.
#[derive(Default)]
struct FakeAdmin {
    posts: Vec<String>,
    stats_calls: usize,
    /// Successive `downstream_cx_active` values; the last repeats.
    active_series: Vec<u64>,
}

fn serve_admin(ln: TcpListener, admin: Arc<Mutex<FakeAdmin>>) {
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = ln.accept().await else {
                return;
            };
            let admin = admin.clone();
            tokio::spawn(async move {
                let svc =
                    hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let mut a = admin.lock();
                        let mut body = String::new();
                        if req.method() == http::Method::POST {
                            let uri = req
                                .uri()
                                .path_and_query()
                                .map(|p| p.as_str().to_string())
                                .unwrap_or_default();
                            a.posts.push(uri);
                        } else if req.uri().path().starts_with("/stats") {
                            let i = a.stats_calls.min(a.active_series.len() - 1);
                            let active = a.active_series[i];
                            a.stats_calls += 1;
                            // The admin line must be excluded from the sum.
                            body = format!(
                                "listener.admin.downstream_cx_active: 1\n\
                             listener.0.0.0.0_8080.downstream_cx_active: {active}\n\
                             http.ingress_http.downstream_cx_active_unrelated: 99\n"
                            );
                        }
                        std::future::ready(Ok::<_, Infallible>(http::Response::new(Full::new(
                            Bytes::from(body),
                        ))))
                    });
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(tcp), svc)
                    .await;
            });
        }
    });
}

async fn start_admin(series: &[u64]) -> (String, Arc<Mutex<FakeAdmin>>) {
    let ln = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap().to_string();
    let admin = Arc::new(Mutex::new(FakeAdmin {
        active_series: series.to_vec(),
        ..Default::default()
    }));
    serve_admin(ln, admin.clone());
    (addr, admin)
}

// Both admin calls are POSTed and stats are polled until active
// connections reach zero, admin listeners excluded.
#[tokio::test]
async fn envoy_drainer_drains_to_zero() {
    let (addr, admin) = start_admin(&[2, 1, 0]).await;
    let d = EnvoyDrainer::new(addr).with_poll_interval(Duration::from_millis(5));
    d.drain(Duration::from_secs(3)).await.unwrap();
    let a = admin.lock();
    assert_eq!(
        a.posts,
        ["/healthcheck/fail", "/drain_listeners?graceful&skip_exit"]
    );
    assert!(
        a.stats_calls >= 3,
        "stats polled {} times, want >= 3 (series 2,1,0)",
        a.stats_calls
    );
}

// The admin sockets may bind only the IPv6 loopback; a drainer pinned to
// 127.0.0.1 would find nothing, read that as an exited Envoy and report a
// drain it never performed. Hence the assertion on the POSTs.
#[tokio::test]
async fn envoy_drainer_reaches_ipv6_only_admin() {
    let Ok(ln) = TcpListener::bind("[::1]:0").await else {
        eprintln!("no IPv6 loopback on this host; skipping");
        return;
    };
    let port = ln.local_addr().unwrap().port();
    let admin = Arc::new(Mutex::new(FakeAdmin {
        active_series: vec![0],
        ..Default::default()
    }));
    serve_admin(ln, admin.clone());
    let d =
        EnvoyDrainer::new(format!("localhost:{port}")).with_poll_interval(Duration::from_millis(5));
    d.drain(Duration::from_secs(3)).await.unwrap();
    assert_eq!(
        admin.lock().posts.len(),
        2,
        "the drain should have reached the IPv6 loopback"
    );
}

// An unreachable admin (Envoy already exited) is a completed drain,
// reported promptly.
#[tokio::test]
async fn envoy_drainer_admin_gone() {
    let ln = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap().to_string();
    drop(ln);
    let start = Instant::now();
    EnvoyDrainer::new(addr)
        .drain(Duration::from_secs(3))
        .await
        .expect("drain against a gone admin");
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "drain against a gone admin took {:?}",
        start.elapsed()
    );
}

// Connections that never reach zero surface the deadline so the shutdown
// sequence can log it and continue.
#[tokio::test]
async fn envoy_drainer_deadline_with_active_connections() {
    let (addr, _admin) = start_admin(&[5; 12]).await;
    let d = EnvoyDrainer::new(addr).with_poll_interval(Duration::from_millis(5));
    let err = d
        .drain(Duration::from_millis(50))
        .await
        .expect_err("persistent connections should report the deadline");
    assert!(err.contains("deadline"), "{err}");
}

#[test]
fn sum_active_connections_skips_admin_and_unrelated() {
    let stats = "listener.admin.downstream_cx_active: 1\n\
                 listener.0.0.0.0_8080.downstream_cx_active: 3\n\
                 listener.0.0.0.0_8443.downstream_cx_active: 4\n\
                 http.ingress_http.downstream_cx_active_unrelated: 99\n\
                 garbage line\n\
                 listener.x.downstream_cx_active: nan\n";
    assert_eq!(sum_active_connections(stats), 7);
}

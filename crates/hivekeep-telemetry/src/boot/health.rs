//! Readiness and liveness for the kubelet probes, and the Prometheus
//! `/metrics` endpoint, on separate servers so turning one off can never
//! take the other with it.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use tokio::net::TcpListener;

/// Bounds the dependency checks of one `/readyz` request. It sits under
/// the kubelet's default 1s probe timeout so a hung dependency gets a 503
/// rather than a timed-out probe.
pub const CHECK_TIMEOUT: Duration = Duration::from_millis(500);

/// Bounds how long a client can take to send the request headers, so a slow
/// client cannot hold a connection open forever.
pub const HTTP_SERVER_READ_HEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// A readiness dependency check.
pub type CheckFn =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>;

struct ReadinessCheck {
    name: String,
    check: CheckFn,
    failing: AtomicBool,
}

/// Process readiness. The default reports ready. [`Readiness::mark_not_ready`]
/// flips it permanently to not ready (draining); checks added with
/// [`Readiness::add_check`] make `/readyz` answer 503 only while they fail.
#[derive(Default)]
pub struct Readiness {
    not_ready: AtomicBool,
    checks: RwLock<Vec<Arc<ReadinessCheck>>>,
}

impl Readiness {
    /// Makes `/readyz` answer 503 from now on.
    pub fn mark_not_ready(&self) {
        self.not_ready.store(true, Ordering::SeqCst);
    }

    /// Whether [`Readiness::mark_not_ready`] has not been called. Does not
    /// run the checks.
    pub fn ready(&self) -> bool {
        !self.not_ready.load(Ordering::SeqCst)
    }

    /// Registers a dependency check that `/readyz` runs on every request.
    /// While it fails, or does not finish within [`CHECK_TIMEOUT`],
    /// `/readyz` answers 503, and 200 again once it passes. Liveness is
    /// unaffected. Check errors are logged when a check starts and stops
    /// failing, and never sent to the client.
    pub fn add_check<F, Fut>(&self, name: &str, check: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        let check: CheckFn = Arc::new(move || Box::pin(check()));
        self.checks
            .write()
            .expect("readiness lock")
            .push(Arc::new(ReadinessCheck {
                name: name.to_string(),
                check,
                failing: AtomicBool::new(false),
            }));
    }

    /// Why `/readyz` should answer 503, or `None` for 200. Draining wins
    /// over the checks, which are then not run.
    pub async fn not_ready_reason(&self) -> Option<String> {
        if !self.ready() {
            return Some("draining".into());
        }
        let checks = self.checks.read().expect("readiness lock").clone();
        let deadline = tokio::time::Instant::now() + CHECK_TIMEOUT;
        for c in checks {
            let result = match tokio::time::timeout_at(deadline, (c.check)()).await {
                Ok(r) => r,
                Err(_) => Err("check timed out".to_string()),
            };
            match result {
                Err(err) => {
                    if !c.failing.swap(true, Ordering::SeqCst) {
                        tracing::warn!(check = %c.name, err = %err, "Readiness check failing");
                    }
                    return Some(format!("{} check failed", c.name));
                }
                Ok(()) => {
                    if c.failing.swap(false, Ordering::SeqCst) {
                        tracing::info!(check = %c.name, "Readiness check recovered");
                    }
                }
            }
        }
        None
    }
}

fn text(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Full::new(body.into()))
        .expect("static response")
}

/// The health server's handler: `/readyz` follows `readiness`, `/healthz`
/// always answers 200 so liveness keeps succeeding while a draining server
/// fails readiness.
pub async fn health_response(readiness: &Readiness, path: &str) -> Response<Full<Bytes>> {
    match path {
        "/readyz" => match readiness.not_ready_reason().await {
            Some(reason) => text(StatusCode::SERVICE_UNAVAILABLE, format!("{reason}\n")),
            None => text(StatusCode::OK, "ok"),
        },
        "/healthz" => text(StatusCode::OK, "ok"),
        _ => text(StatusCode::NOT_FOUND, "404 page not found\n"),
    }
}

/// The metrics server's handler: `/metrics` in the Prometheus text format
/// from `registry`.
pub fn metrics_response(registry: &prometheus::Registry, path: &str) -> Response<Full<Bytes>> {
    if path != "/metrics" {
        return text(StatusCode::NOT_FOUND, "404 page not found\n");
    }
    let mut buf = Vec::new();
    let encoder = prometheus::TextEncoder::new();
    if let Err(e) = prometheus::Encoder::encode(&encoder, &registry.gather(), &mut buf) {
        return text(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
    }
    let mut resp = text(StatusCode::OK, buf);
    resp.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    resp
}

/// Serves `handler` on `listener` until the task is dropped.
async fn serve<F, Fut>(listener: TcpListener, handler: F) -> std::io::Result<()>
where
    F: Fn(String) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response<Full<Bytes>>> + Send + 'static,
{
    loop {
        let (stream, _) = listener.accept().await?;
        let handler = handler.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req: Request<Incoming>| {
                let handler = handler.clone();
                async move {
                    Ok::<_, std::convert::Infallible>(handler(req.uri().path().to_string()).await)
                }
            });
            let mut builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
            builder
                .http1()
                .timer(TokioTimer::new())
                .header_read_timeout(HTTP_SERVER_READ_HEADER_TIMEOUT);
            let _ = builder.serve_connection(TokioIo::new(stream), svc).await;
        });
    }
}

/// Runs the health server on `addr`. Returns only on failure; the caller
/// should exit, since a pod whose probes can never succeed never turns
/// Ready, and dying loudly lets the kubelet restart it.
pub async fn serve_health(addr: SocketAddr, readiness: Arc<Readiness>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "Starting health HTTP server");
    serve_health_on(listener, readiness).await
}

/// [`serve_health`] on an already bound listener.
pub async fn serve_health_on(
    listener: TcpListener,
    readiness: Arc<Readiness>,
) -> std::io::Result<()> {
    serve(listener, move |path| {
        let readiness = readiness.clone();
        async move { health_response(&readiness, &path).await }
    })
    .await
}

/// Runs the metrics server for the default Prometheus registry on `addr`.
/// Returns only on failure.
pub async fn serve_metrics(addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "Starting metrics HTTP server");
    serve(listener, |path| async move {
        metrics_response(prometheus::default_registry(), &path)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contextlogging::tests::capture;
    use http_body_util::BodyExt;
    use std::sync::atomic::AtomicUsize;

    async fn code(r: &Readiness, path: &str) -> StatusCode {
        health_response(r, path).await.status()
    }

    async fn body(resp: Response<Full<Bytes>>) -> String {
        String::from_utf8(
            resp.into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn readyz_drains_while_healthz_stays_up() {
        let r = Readiness::default();
        assert_eq!(code(&r, "/readyz").await, StatusCode::OK);
        assert_eq!(code(&r, "/healthz").await, StatusCode::OK);
        r.mark_not_ready();
        assert_eq!(code(&r, "/readyz").await, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            code(&r, "/healthz").await,
            StatusCode::OK,
            "liveness must not fail while draining"
        );
    }

    #[tokio::test]
    async fn readyz_static_with_default_readiness() {
        assert_eq!(code(&Readiness::default(), "/readyz").await, StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_follows_check() {
        let down = Arc::new(AtomicBool::new(false));
        let r = Readiness::default();
        let d = down.clone();
        r.add_check("store", move || {
            let d = d.clone();
            async move {
                if d.load(Ordering::SeqCst) {
                    Err("failed to connect to host=db.internal user=hk".into())
                } else {
                    Ok(())
                }
            }
        });
        assert_eq!(code(&r, "/readyz").await, StatusCode::OK);
        down.store(true, Ordering::SeqCst);
        let resp = health_response(&r, "/readyz").await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let b = body(resp).await;
        assert!(b.contains("store") && !b.contains("db.internal"), "{b}");
        assert_eq!(code(&r, "/healthz").await, StatusCode::OK);
        down.store(false, Ordering::SeqCst);
        assert_eq!(code(&r, "/readyz").await, StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_check_timeout() {
        let r = Readiness::default();
        r.add_check("store", std::future::pending::<Result<(), String>>);
        let start = std::time::Instant::now();
        assert_eq!(code(&r, "/readyz").await, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "took {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn readyz_drain_skips_checks() {
        let calls = Arc::new(AtomicUsize::new(0));
        let r = Readiness::default();
        let c = calls.clone();
        r.add_check("store", move || {
            c.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        });
        r.mark_not_ready();
        assert_eq!(code(&r, "/readyz").await, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn readyz_logs_check_transitions_once() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let down = Arc::new(AtomicBool::new(false));
        let r = Readiness::default();
        let d = down.clone();
        r.add_check("store", move || {
            let d = d.clone();
            async move {
                if d.load(Ordering::SeqCst) {
                    Err("connection refused".into())
                } else {
                    Ok(())
                }
            }
        });
        let out = capture(|| {
            rt.block_on(async {
                down.store(true, Ordering::SeqCst);
                for _ in 0..3 {
                    code(&r, "/readyz").await;
                }
                down.store(false, Ordering::SeqCst);
                for _ in 0..3 {
                    code(&r, "/readyz").await;
                }
            })
        });
        assert_eq!(out.matches("Readiness check failing").count(), 1, "{out}");
        assert_eq!(out.matches("Readiness check recovered").count(), 1, "{out}");
    }

    #[tokio::test]
    async fn metrics_and_health_serve_separate_endpoints() {
        let reg = prometheus::Registry::new();
        let r = Readiness::default();
        for (server, path, want) in [
            ("metrics", "/metrics", StatusCode::OK),
            ("metrics", "/readyz", StatusCode::NOT_FOUND),
            ("metrics", "/healthz", StatusCode::NOT_FOUND),
            ("health", "/metrics", StatusCode::NOT_FOUND),
            ("health", "/readyz", StatusCode::OK),
            ("health", "/healthz", StatusCode::OK),
        ] {
            let got = if server == "metrics" {
                metrics_response(&reg, path).status()
            } else {
                code(&r, path).await
            };
            assert_eq!(got, want, "{server} GET {path}");
        }
    }

    #[tokio::test]
    async fn http_server_bounds_header_read() {
        assert!(HTTP_SERVER_READ_HEADER_TIMEOUT > Duration::ZERO);
        // A live server answers probes over a real connection.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_health_on(listener, Arc::new(Readiness::default())));
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        s.write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
    }
}

//! Process start-up shared by the benchmark binaries: logging, the rustls
//! provider, a Prometheus `/metrics` listener and termination signals.

use std::convert::Infallible;
use std::net::SocketAddr;

use bytes::Bytes;
use http_body_util::Full;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prometheus::{Encoder, Registry, TextEncoder};
use tokio::net::TcpListener;

/// The version string `--version` prints.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Starts process telemetry for `service`: JSON logging to stdout at
/// `HK_LOG_LEVEL` (or `RUST_LOG`, default `info`) and, when an OTLP
/// endpoint is configured, trace and metric export. Safe to call more than
/// once; later calls return `None`. Keep the returned value alive for the
/// life of the process.
pub fn init_logging(service: &str) -> Option<hivekeep_telemetry::Telemetry> {
    hivekeep_telemetry::init(hivekeep_telemetry::InitOptions {
        service_name: service.to_string(),
        metrics: hivekeep_telemetry::MetricsMode::PushOnly,
        ..Default::default()
    })
    .ok()
}

/// Installs ring as the process-wide rustls provider. Idempotent.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Renders `registry` in the Prometheus text format.
pub fn render_metrics(registry: &Registry) -> String {
    let mut buf = Vec::new();
    // Encoding into a Vec cannot fail for the text format.
    let _ = TextEncoder::new().encode(&registry.gather(), &mut buf);
    String::from_utf8(buf).unwrap_or_default()
}

/// Binds `addr` (`:9090` style addresses mean all interfaces) and serves
/// `/metrics` from `registry` until the task is dropped or aborted.
pub async fn serve_metrics(
    addr: &str,
    registry: Registry,
) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let listener = TcpListener::bind(normalize_addr(addr)).await?;
    Ok(tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let registry = registry.clone();
            tokio::spawn(async move {
                let svc = service_fn(move |req: Request<hyper::body::Incoming>| {
                    let registry = registry.clone();
                    async move {
                        let resp = if req.uri().path() == "/metrics" {
                            let mut r =
                                Response::new(Full::new(Bytes::from(render_metrics(&registry))));
                            r.headers_mut().insert(
                                http::header::CONTENT_TYPE,
                                TextEncoder::new().format_type().parse().unwrap(),
                            );
                            r
                        } else {
                            let mut r = Response::new(Full::new(Bytes::from_static(
                                b"404 page not found\n",
                            )));
                            *r.status_mut() = StatusCode::NOT_FOUND;
                            r
                        };
                        Ok::<_, Infallible>(resp)
                    }
                });
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    }))
}

/// Turns a `:port` listen address into `0.0.0.0:port`; other forms are
/// returned unchanged.
pub fn normalize_addr(addr: &str) -> String {
    if let Some(port) = addr.strip_prefix(':') {
        format!("0.0.0.0:{port}")
    } else {
        addr.to_string()
    }
}

/// Parses a listen address after [`normalize_addr`].
pub fn parse_addr(addr: &str) -> Result<SocketAddr, std::net::AddrParseError> {
    normalize_addr(addr).parse()
}

/// Resolves on SIGINT or SIGTERM.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn colon_port_means_all_interfaces() {
        assert_eq!(normalize_addr(":9090"), "0.0.0.0:9090");
        assert_eq!(normalize_addr("127.0.0.1:1"), "127.0.0.1:1");
        assert!(parse_addr(":8080").is_ok());
    }

    #[tokio::test]
    async fn metrics_listener_serves_the_registry() {
        let registry = Registry::new();
        let c = prometheus::IntCounter::new("boot_test_total", "help").unwrap();
        registry.register(Box::new(c.clone())).unwrap();
        c.inc();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let task = serve_metrics(&addr.to_string(), registry).await.unwrap();
        let body = reqwest::get(format!("http://{addr}/metrics"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(body.contains("boot_test_total 1"), "{body}");
        let nf = reqwest::get(format!("http://{addr}/other")).await.unwrap();
        assert_eq!(nf.status(), 404);
        task.abort();
    }
}

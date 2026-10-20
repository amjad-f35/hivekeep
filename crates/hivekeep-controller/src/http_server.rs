//! The metrics and health listeners.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// Answers one request: `/metrics` (when `registry` is set), `/healthz`
/// and `/readyz`.
pub fn route(registry: Option<&prometheus::Registry>, path: &str) -> Response<Full<Bytes>> {
    let text = |code: StatusCode, body: String| {
        Response::builder()
            .status(code)
            .header("content-type", "text/plain; charset=utf-8")
            .body(Full::new(Bytes::from(body)))
            .expect("response")
    };
    match (path, registry) {
        ("/metrics", Some(r)) => {
            let mut buf = String::new();
            match prometheus::TextEncoder::new().encode_utf8(&r.gather(), &mut buf) {
                Ok(()) => text(StatusCode::OK, buf),
                Err(e) => text(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            }
        }
        ("/healthz" | "/readyz", _) => text(StatusCode::OK, "ok".into()),
        _ => text(StatusCode::NOT_FOUND, "not found".into()),
    }
}

/// Serves [`route`] on `addr` until `shutdown`.
pub async fn serve(
    addr: &str,
    registry: Option<prometheus::Registry>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let registry = Arc::new(registry);
    loop {
        let (stream, _) = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            r = listener.accept() => r?,
        };
        let registry = registry.clone();
        tokio::spawn(async move {
            let svc = hyper::service::service_fn(move |req: Request<Incoming>| {
                let resp = route(registry.as_ref().as_ref(), req.uri().path());
                async move { Ok::<_, Infallible>(resp) }
            });
            let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), svc)
                .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes() {
        let r = prometheus::Registry::new();
        let c = prometheus::IntCounter::new("hivekeep_test_total", "t").unwrap();
        r.register(Box::new(c.clone())).unwrap();
        c.inc();
        assert_eq!(route(Some(&r), "/metrics").status(), StatusCode::OK);
        assert_eq!(route(None, "/metrics").status(), StatusCode::NOT_FOUND);
        assert_eq!(route(None, "/healthz").status(), StatusCode::OK);
        assert_eq!(route(None, "/readyz").status(), StatusCode::OK);
        assert_eq!(route(None, "/nope").status(), StatusCode::NOT_FOUND);
    }
}

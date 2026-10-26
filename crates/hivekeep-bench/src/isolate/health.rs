//! `/healthz` and `/readyz` for the fake data plane's probes.

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::service::service_fn;
use hyper::{Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;

/// The status a probe of `path` gets: `/healthz` is 200 while the process
/// runs, `/readyz` 200 once `ready`, 503 before; anything else is 404.
pub fn status(path: &str, ready: bool) -> StatusCode {
    match path {
        "/healthz" => StatusCode::OK,
        "/readyz" if ready => StatusCode::OK,
        "/readyz" => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::NOT_FOUND,
    }
}

/// Serves the probes on `addr` until the task is aborted.
pub async fn serve(
    addr: &str,
    ready: Arc<AtomicBool>,
) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let listener = TcpListener::bind(crate::boot::normalize_addr(addr)).await?;
    Ok(tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let ready = ready.clone();
            tokio::spawn(async move {
                let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                    let code = status(req.uri().path(), ready.load(Ordering::Relaxed));
                    async move {
                        let mut r = Response::new(Full::new(Bytes::new()));
                        *r.status_mut() = code;
                        Ok::<_, Infallible>(r)
                    }
                });
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    // The fake keeper holds no state that must be ready first: both probes
    // answer 200 (its readiness flag starts set).
    #[test]
    fn keeper_probes_always_ok() {
        for path in ["/healthz", "/readyz"] {
            assert_eq!(status(path, true), StatusCode::OK, "{path}");
        }
    }

    // fake-workersync is not ready until it has adopted the registered
    // Workers, so a rollout wait covers hivekeep-api answering it.
    #[tokio::test]
    async fn ready_after_adopt() {
        let ready = Arc::new(AtomicBool::new(false));
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        let task = serve(&addr.to_string(), ready.clone()).await.unwrap();
        let get = |p: &'static str| async move {
            reqwest::get(format!("http://{addr}{p}"))
                .await
                .unwrap()
                .status()
                .as_u16()
        };
        assert_eq!(get("/healthz").await, 200);
        assert_eq!(get("/readyz").await, 503);
        ready.store(true, Ordering::Relaxed);
        assert_eq!(get("/readyz").await, 200);
        task.abort();
    }
}

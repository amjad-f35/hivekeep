//! A minimal plain-HTTP/1.1 server for the side endpoints (health,
//! status). Handlers are synchronous functions of the request head.

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

/// Response type of a handler.
pub type HttpResponse = http::Response<Full<Bytes>>;

/// A response with `status`, `content_type` and `body`.
pub fn response(
    status: http::StatusCode,
    content_type: &str,
    body: impl Into<Bytes>,
) -> HttpResponse {
    let mut r = http::Response::new(Full::new(body.into()));
    *r.status_mut() = status;
    if let Ok(v) = http::HeaderValue::from_str(content_type) {
        r.headers_mut().insert(http::header::CONTENT_TYPE, v);
    }
    r
}

/// A plain-text 404.
pub fn not_found() -> HttpResponse {
    response(
        http::StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        "404 page not found\n",
    )
}

/// Serves `handler` on `listener` until `shutdown` resolves. Connections
/// already open are dropped at shutdown; these endpoints carry nothing
/// worth draining.
pub async fn serve<H, F>(listener: TcpListener, handler: H, shutdown: F)
where
    H: Fn(&http::request::Parts) -> HttpResponse + Send + Sync + 'static,
    F: Future<Output = ()>,
{
    let handler = Arc::new(handler);
    tokio::pin!(shutdown);
    loop {
        let (tcp, _) = tokio::select! {
            _ = &mut shutdown => return,
            accepted = listener.accept() => match accepted {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(err = %e, "Accepting an HTTP connection failed");
                    continue;
                }
            },
        };
        let handler = handler.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req: http::Request<Incoming>| {
                let handler = handler.clone();
                async move {
                    let (parts, _) = req.into_parts();
                    Ok::<_, Infallible>(handler(&parts))
                }
            });
            let conn = hyper::server::conn::http1::Builder::new()
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(std::time::Duration::from_secs(10))
                .serve_connection(TokioIo::new(tcp), svc);
            if let Err(e) = conn.await {
                tracing::debug!(err = %e, "HTTP connection ended with an error");
            }
        });
    }
}

/// Turns a `:port` listen address (all interfaces) into one the socket
/// API accepts; other addresses pass through.
pub fn listen_addr(addr: &str) -> String {
    match addr.strip_prefix(':') {
        Some(port) => format!("0.0.0.0:{port}"),
        None => addr.to_string(),
    }
}

/// Resolves when the process receives SIGINT or SIGTERM.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
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
    fn listen_addresses() {
        assert_eq!(listen_addr(":50051"), "0.0.0.0:50051");
        assert_eq!(listen_addr("127.0.0.1:4040"), "127.0.0.1:4040");
    }

    #[tokio::test]
    async fn serves_until_shutdown() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve(
            listener,
            |req| {
                response(
                    http::StatusCode::OK,
                    "text/plain",
                    req.uri.path().to_string(),
                )
            },
            async {
                let _ = rx.await;
            },
        ));
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        c.write_all(b"GET /hello HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut out = String::new();
        c.read_to_string(&mut out).await.unwrap();
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
        assert!(out.ends_with("/hello"), "{out}");
        tx.send(()).unwrap();
        server.await.unwrap();
    }
}

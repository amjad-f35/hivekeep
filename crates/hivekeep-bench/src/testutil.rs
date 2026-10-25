#![allow(dead_code)]

//! Test helpers shared across the crate's modules.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper::{Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// A request as a test handler sees it.
#[derive(Debug, Clone)]
pub struct Req {
    /// Method.
    pub method: http::Method,
    /// Path and query.
    pub uri: http::Uri,
    /// Headers.
    pub headers: http::HeaderMap,
    /// Body.
    pub body: Bytes,
}

/// What a test handler answers.
#[derive(Debug, Clone)]
pub struct Resp {
    /// Status code.
    pub status: StatusCode,
    /// Headers.
    pub headers: Vec<(String, String)>,
    /// Body.
    pub body: Vec<u8>,
}

impl Resp {
    /// A 200 with `body`.
    pub fn ok(body: impl Into<Vec<u8>>) -> Self {
        Resp {
            status: StatusCode::OK,
            headers: Vec::new(),
            body: body.into(),
        }
    }

    /// `status` with `body`.
    pub fn status(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Resp {
            status: StatusCode::from_u16(status).unwrap(),
            headers: Vec::new(),
            body: body.into(),
        }
    }

    /// Adds a header.
    pub fn header(mut self, k: &str, v: &str) -> Self {
        self.headers.push((k.to_string(), v.to_string()));
        self
    }
}

type Handler = Arc<dyn Fn(Req) -> Resp + Send + Sync>;

/// A loopback HTTP/1.1 + h2c server running a closure. Dropping it stops
/// the listener.
pub struct HttpServer {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl HttpServer {
    /// Serves `handler` on a fresh loopback port.
    pub async fn start(handler: impl Fn(Req) -> Resp + Send + Sync + 'static) -> Self {
        let handler: Handler = Arc::new(handler);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let handler = handler.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                        let handler = handler.clone();
                        async move {
                            let (parts, body) = req.into_parts();
                            let body = body
                                .collect()
                                .await
                                .map(|b| b.to_bytes())
                                .unwrap_or_default();
                            let r = handler(Req {
                                method: parts.method,
                                uri: parts.uri,
                                headers: parts.headers,
                                body,
                            });
                            let mut resp = Response::new(Full::new(Bytes::from(r.body)));
                            *resp.status_mut() = r.status;
                            for (k, v) in r.headers {
                                resp.headers_mut().insert(
                                    http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                                    v.parse().unwrap(),
                                );
                            }
                            Ok::<_, Infallible>(resp)
                        }
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });
        HttpServer { addr, task }
    }

    /// `http://127.0.0.1:<port>`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The listening address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

/// Polls `cond` every 10ms until it holds or `timeout` passes; returns
/// whether it held.
pub async fn eventually(timeout: std::time::Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    cond()
}

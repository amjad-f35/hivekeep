//! A stand-in for an Actor's program receiving ingress: an HTTP server
//! answering every request with the same body. The tunnel reaches it over
//! HTTP/1.1, h2c for gRPC, and raw HTTP/1.1 inside CONNECT tunnels.

use std::convert::Infallible;
use std::io;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http::{Response, StatusCode};
use http_body_util::Full;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::fixed_body;

/// The ingress actor's counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngressActorStats {
    pub requests: u64,
    pub connections_seen: u64,
    pub connections_open: i64,
}

#[derive(Default)]
struct Counters {
    requests: AtomicU64,
    connections_seen: AtomicU64,
    connections_open: AtomicI64,
}

/// The ingress actor.
#[derive(Clone)]
pub struct IngressActor {
    body: Bytes,
    counters: Arc<Counters>,
}

impl IngressActor {
    /// An actor answering with `body_bytes` of body.
    pub fn new(body_bytes: usize) -> Self {
        IngressActor {
            body: Bytes::from(fixed_body(body_bytes)),
            counters: Arc::default(),
        }
    }

    /// The counters now.
    pub fn stats(&self) -> IngressActorStats {
        IngressActorStats {
            requests: self.counters.requests.load(Ordering::Relaxed),
            connections_seen: self.counters.connections_seen.load(Ordering::Relaxed),
            connections_open: self.counters.connections_open.load(Ordering::Relaxed),
        }
    }

    /// Serves HTTP/1.1 and h2c on `lis` until `cancel`.
    pub async fn serve(&self, lis: TcpListener, cancel: CancellationToken) -> io::Result<()> {
        loop {
            let (conn, _) = tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                a = lis.accept() => a?,
            };
            let _ = conn.set_nodelay(true);
            let me = self.clone();
            me.counters.connections_seen.fetch_add(1, Ordering::Relaxed);
            me.counters.connections_open.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                let svc = {
                    let me = me.clone();
                    hyper::service::service_fn(move |_req| {
                        me.counters.requests.fetch_add(1, Ordering::Relaxed);
                        let resp = Response::builder()
                            .status(StatusCode::OK)
                            .header(http::header::CONTENT_TYPE, "text/plain")
                            .header(http::header::CONTENT_LENGTH, me.body.len())
                            .body(Full::new(me.body.clone()))
                            .expect("static response");
                        async move { Ok::<_, Infallible>(resp) }
                    })
                };
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection_with_upgrades(TokioIo::new(conn), svc)
                    .await;
                me.counters.connections_open.fetch_sub(1, Ordering::Relaxed);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn answers_with_its_body_and_counts() {
        let actor = IngressActor::new(5);
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap();
        let cancel = CancellationToken::new();
        let a = actor.clone();
        let c = cancel.clone();
        tokio::spawn(async move { a.serve(lis, c).await });

        let mut conn = tokio::net::TcpStream::connect(addr).await.unwrap();
        conn.write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut out = String::new();
        conn.read_to_string(&mut out).await.unwrap();
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
        assert!(out.ends_with("\r\n\r\nabcde"), "body: {out}");
        let st = actor.stats();
        assert_eq!((st.requests, st.connections_seen), (1, 1), "{st:?}");
        cancel.cancel();
    }
}

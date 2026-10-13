//! The metrics and health endpoints: a tiny HTTP/1.1 server answering
//! `GET /metrics`, `/healthz` and `/readyz`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use prometheus::{Encoder, Registry, TextEncoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::context::Ctx;

/// Whether the keeper is ready: serving its port and not draining.
#[derive(Debug, Default)]
pub struct Readiness {
    serving: AtomicBool,
    draining: AtomicBool,
}

impl Readiness {
    /// Marks the gRPC port open.
    pub fn mark_serving(&self) {
        self.serving.store(true, Ordering::SeqCst);
    }

    /// Marks the keeper draining, so /readyz fails while /healthz passes.
    pub fn mark_not_ready(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    /// Why the keeper is not ready, if it is not.
    pub fn check(&self) -> Result<(), &'static str> {
        if self.draining.load(Ordering::SeqCst) {
            return Err("the keeper is draining");
        }
        if !self.serving.load(Ordering::SeqCst) {
            return Err("the keeper is starting; the gRPC port is not open yet");
        }
        Ok(())
    }
}

/// What a request to `path` is answered with.
pub fn respond(
    path: &str,
    registry: Option<&Registry>,
    readiness: Option<&Readiness>,
) -> (u16, String, Vec<u8>) {
    match (path, registry, readiness) {
        ("/metrics", Some(reg), _) => {
            let mut buf = Vec::new();
            let enc = TextEncoder::new();
            match enc.encode(&reg.gather(), &mut buf) {
                Ok(()) => (200, enc.format_type().to_string(), buf),
                Err(e) => (500, "text/plain".into(), e.to_string().into_bytes()),
            }
        }
        ("/healthz", _, Some(_)) => (200, "text/plain".into(), b"ok".to_vec()),
        ("/readyz", _, Some(r)) => match r.check() {
            Ok(()) => (200, "text/plain".into(), b"ok".to_vec()),
            Err(why) => (503, "text/plain".into(), why.as_bytes().to_vec()),
        },
        _ => (404, "text/plain".into(), b"not found".to_vec()),
    }
}

/// Serves `registry` and/or `readiness` on `addr` until `ctx` ends.
pub async fn serve(
    ctx: Ctx,
    addr: String,
    registry: Option<Registry>,
    readiness: Option<Arc<Readiness>>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    loop {
        let (mut sock, _) = match ctx.run(listener.accept()).await {
            Ok(r) => r?,
            Err(_) => return Ok(()),
        };
        let (registry, readiness) = (registry.clone(), readiness.clone());
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let n = match sock.read(&mut buf).await {
                Ok(n) => n,
                Err(_) => return,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = req
                .lines()
                .next()
                .and_then(|l| l.split_whitespace().nth(1))
                .unwrap_or("/")
                .split('?')
                .next()
                .unwrap_or("/")
                .to_string();
            let (code, ctype, body) = respond(&path, registry.as_ref(), readiness.as_deref());
            let reason = match code {
                200 => "OK",
                404 => "Not Found",
                503 => "Service Unavailable",
                _ => "Internal Server Error",
            };
            let head = format!(
                "HTTP/1.1 {code} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = sock.write_all(head.as_bytes()).await;
            let _ = sock.write_all(&body).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_and_routes() {
        let r = Readiness::default();
        assert_eq!(respond("/readyz", None, Some(&r)).0, 503);
        r.mark_serving();
        assert_eq!(respond("/readyz", None, Some(&r)).0, 200);
        r.mark_not_ready();
        assert_eq!(respond("/readyz", None, Some(&r)).0, 503);
        assert_eq!(respond("/healthz", None, Some(&r)).0, 200);
        let reg = Registry::new();
        assert_eq!(respond("/metrics", Some(&reg), None).0, 200);
        assert_eq!(respond("/other", Some(&reg), None).0, 404);
    }

    #[tokio::test]
    async fn serves_over_tcp() {
        let ctx = Ctx::background();
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        drop(l);
        let r = Arc::new(Readiness::default());
        tokio::spawn(serve(ctx.clone(), addr.clone(), None, Some(r)));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut s = tokio::net::TcpStream::connect(&addr).await.unwrap();
        s.write_all(b"GET /healthz HTTP/1.1\r\n\r\n").await.unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        assert!(out.starts_with("HTTP/1.1 200"));
        ctx.cancel();
    }
}

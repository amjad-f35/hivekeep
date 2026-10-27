//! A stand-in for the remote egress gateway: an mTLS server accepting
//! CONNECT over HTTP/2 or HTTP/1.1 and handling the bytes inside each tunnel
//! by destination port. It never dials the destination.

use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use hivekeep_gate::tunnel::egress::h2_stream::H2Stream;
use http::{Method, Response, StatusCode};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::fixed_body;
use crate::admin::{self, Extra, Handler};
use crate::httpio::{self, BodyKind};
use crate::{pki, Error, Result};

/// Tunnels to this port carry HTTP/1.1 requests, which the gateway answers
/// with its fixed body, as a web server behind a real gateway would.
pub const ANSWER_PORT: &str = "80";
/// Tunnels to this port carry raw bytes, which the gateway reads and drops,
/// for throughput.
pub const DISCARD_PORT: &str = "9";

/// The fake gateway's counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayStats {
    /// CONNECTs accepted; `tunnels_h2` those that arrived as HTTP/2
    /// streams, which is how the tunnel prefers to send them.
    pub tunnels: u64,
    pub tunnels_h2: u64,
    pub tunnels_open: i64,
    /// TLS connections from the tunnel. Under HTTP/2 many tunnels share one.
    pub connections: u64,
    pub rejected: u64,
    /// HTTP requests answered inside answer-port tunnels.
    pub requests: u64,
    pub bytes_in: u64,
}

#[derive(Default)]
struct Counters {
    tunnels: AtomicU64,
    tunnels_h2: AtomicU64,
    tunnels_open: AtomicI64,
    connections: AtomicU64,
    rejected: AtomicU64,
    requests: AtomicU64,
    bytes_in: Arc<AtomicU64>,
}

/// Configures a [`Gateway`].
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// The gateway's serving certificate and key.
    pub credential_bundle: PathBuf,
    /// Verifies the Actor certificates the tunnel presents.
    pub client_ca: PathBuf,
    /// Size of the answer body.
    pub body_bytes: usize,
}

#[derive(Debug)]
struct OneCert(Arc<CertifiedKey>);

impl ResolvesServerCert for OneCert {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.0.clone())
    }
}

/// The fake egress gateway.
#[derive(Clone)]
pub struct Gateway {
    tls: Arc<rustls::ServerConfig>,
    response: Bytes,
    counters: Arc<Counters>,
}

/// Decrements the open-tunnel count when a tunnel ends.
struct OpenTunnel(Arc<Counters>);

impl Drop for OpenTunnel {
    fn drop(&mut self) {
        self.0.tunnels_open.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Gateway {
    /// Loads the gateway's TLS material.
    pub fn new(cfg: &GatewayConfig) -> Result<Gateway> {
        let cert = pki::certified_key(&cfg.credential_bundle)?;
        let roots = Arc::new(pki::roots(&cfg.client_ca)?);
        let verifier = WebPkiClientVerifier::builder_with_provider(roots, pki::provider())
            .build()
            .map_err(|e| Error::msg(format!("client verifier: {e}")))?;
        let mut tls = rustls::ServerConfig::builder_with_provider(pki::provider())
            .with_safe_default_protocol_versions()?
            .with_client_cert_verifier(verifier)
            .with_cert_resolver(Arc::new(OneCert(cert)));
        tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let body = fixed_body(cfg.body_bytes);
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(&body);
        Ok(Gateway {
            tls: Arc::new(tls),
            response: Bytes::from(response),
            counters: Arc::default(),
        })
    }

    /// The counters now.
    pub fn stats(&self) -> GatewayStats {
        let c = &self.counters;
        GatewayStats {
            tunnels: c.tunnels.load(Ordering::Relaxed),
            tunnels_h2: c.tunnels_h2.load(Ordering::Relaxed),
            tunnels_open: c.tunnels_open.load(Ordering::Relaxed),
            connections: c.connections.load(Ordering::Relaxed),
            rejected: c.rejected.load(Ordering::Relaxed),
            requests: c.requests.load(Ordering::Relaxed),
            bytes_in: c.bytes_in.load(Ordering::Relaxed),
        }
    }

    /// Serves TLS on `lis` until `cancel`.
    pub async fn serve(&self, lis: TcpListener, cancel: CancellationToken) -> io::Result<()> {
        let acceptor = tokio_rustls::TlsAcceptor::from(self.tls.clone());
        loop {
            let (conn, _) = tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                a = lis.accept() => a?,
            };
            let _ = conn.set_nodelay(true);
            self.counters.connections.fetch_add(1, Ordering::Relaxed);
            let (me, acceptor, cancel) = (self.clone(), acceptor.clone(), cancel.clone());
            tokio::spawn(async move {
                let Ok(Ok(tls)) =
                    tokio::time::timeout(Duration::from_secs(10), acceptor.accept(conn)).await
                else {
                    return;
                };
                let h2 = tls.get_ref().1.alpn_protocol() == Some(b"h2");
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    _ = async { if h2 { me.serve_h2(tls).await } else { me.serve_h1(tls).await } } => {}
                }
            });
        }
    }

    async fn serve_h2<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(&self, io: T) {
        // The HTTP/2 settings of the real gateway's Envoy listener. They
        // shape the tunnel's side: it opens a new connection per 100
        // tunnels, and sizes each tunnel's send buffer from the advertised
        // frame size, Envoy's default 16 KiB.
        let mut conn = match h2::server::Builder::new()
            .max_concurrent_streams(100)
            .max_frame_size(16 << 10)
            .initial_window_size(1 << 20)
            .initial_connection_window_size(1 << 20)
            .handshake::<_, Bytes>(io)
            .await
        {
            Ok(c) => c,
            Err(_) => return,
        };
        while let Some(Ok((req, mut respond))) = conn.accept().await {
            let me = self.clone();
            tokio::spawn(async move {
                if req.method() != Method::CONNECT {
                    me.counters.rejected.fetch_add(1, Ordering::Relaxed);
                    let _ = respond.send_response(status(StatusCode::METHOD_NOT_ALLOWED), true);
                    return;
                }
                let port = req
                    .uri()
                    .authority()
                    .and_then(|a| a.port())
                    .map(|p| p.as_str().to_string());
                let Some(port) = port.filter(|p| p == ANSWER_PORT || p == DISCARD_PORT) else {
                    me.counters.rejected.fetch_add(1, Ordering::Relaxed);
                    let _ = respond.send_response(status(StatusCode::BAD_GATEWAY), true);
                    return;
                };
                let guard = me.open_tunnel();
                me.counters.tunnels_h2.fetch_add(1, Ordering::Relaxed);
                // An HTTP/2 tunnel is the stream itself: the request body
                // carries the tunnel's bytes and the response body ours.
                let Ok(send) = respond.send_response(status(StatusCode::OK), false) else {
                    return;
                };
                let stream = H2Stream::new(send, req.into_body(), None);
                me.tunnel(stream, &port).await;
                drop(guard);
            });
        }
    }

    async fn serve_h1<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(&self, io: T) {
        let mut r = BufReader::new(io);
        let Ok(Ok(Some(head))) =
            tokio::time::timeout(Duration::from_secs(10), httpio::read_head(&mut r)).await
        else {
            return;
        };
        let (method, target) = head.request_line().unwrap_or(("", ""));
        if method != "CONNECT" {
            self.counters.rejected.fetch_add(1, Ordering::Relaxed);
            let _ = r.write_all(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 17\r\nConnection: close\r\n\r\nCONNECT required\n").await;
            return;
        }
        let port = target
            .rsplit_once(':')
            .map(|(_, p)| p.to_string())
            .unwrap_or_default();
        if port != ANSWER_PORT && port != DISCARD_PORT {
            self.counters.rejected.fetch_add(1, Ordering::Relaxed);
            let _ = r.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 25\r\nConnection: close\r\n\r\nunknown destination port\n").await;
            return;
        }
        let _guard = self.open_tunnel();
        // An HTTP/1.1 tunnel takes over the connection.
        if r.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await
            .is_err()
            || r.flush().await.is_err()
        {
            return;
        }
        self.tunnel(r, &port).await;
    }

    fn open_tunnel(&self) -> OpenTunnel {
        self.counters.tunnels.fetch_add(1, Ordering::Relaxed);
        self.counters.tunnels_open.fetch_add(1, Ordering::Relaxed);
        OpenTunnel(self.counters.clone())
    }

    async fn tunnel<T: AsyncRead + AsyncWrite + Unpin>(&self, io: T, port: &str) {
        let (rd, mut wr) = tokio::io::split(io);
        let counted = Counting {
            inner: rd,
            n: self.counters.bytes_in.clone(),
        };
        if port == DISCARD_PORT {
            let mut counted = counted;
            let _ = tokio::io::copy(&mut counted, &mut tokio::io::sink()).await;
            return;
        }
        // Answer every HTTP/1.1 request in the tunnel until it closes.
        let mut r = BufReader::new(counted);
        while let Ok(Some(head)) = httpio::read_head(&mut r).await {
            if httpio::discard_body(&mut r, &head, BodyKind::Request)
                .await
                .is_err()
            {
                return;
            }
            self.counters.requests.fetch_add(1, Ordering::Relaxed);
            if wr.write_all(&self.response).await.is_err() || wr.flush().await.is_err() {
                return;
            }
        }
    }

    /// `/stats` with the gateway's counters, and `/healthz`.
    pub fn admin_handler(&self) -> Handler {
        let me = self.clone();
        let extra: Extra = Arc::new(move || {
            let mut m = serde_json::Map::new();
            m.insert(
                "gateway".into(),
                serde_json::to_value(me.stats()).unwrap_or_default(),
            );
            m
        });
        admin::stats_handler(Some(extra))
    }
}

fn status(s: StatusCode) -> Response<()> {
    Response::builder()
        .status(s)
        .body(())
        .expect("static response")
}

/// Counts the bytes read through it.
struct Counting<R> {
    inner: R,
    n: Arc<AtomicU64>,
}

impl<R: AsyncRead + Unpin> AsyncRead for Counting<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let r = Pin::new(&mut self.inner).poll_read(cx, buf);
        let read = buf.filled().len() - before;
        self.n.fetch_add(read as u64, Ordering::Relaxed);
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_gate::tunnel::egress::{Client, ClientCertificateSource, ClientConfig};
    use rustls::pki_types::ServerName;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct Fixed(Arc<CertifiedKey>);

    impl ClientCertificateSource for Fixed {
        fn current(&self) -> Option<Arc<CertifiedKey>> {
            Some(self.0.clone())
        }
    }

    async fn start_gateway() -> (Gateway, String, tempfile::TempDir, CancellationToken) {
        let dir = tempfile::tempdir().unwrap();
        pki::generate(
            dir.path(),
            &pki::Options {
                worker_hosts: vec!["127.0.0.1".into()],
                gateway_hosts: vec!["127.0.0.1".into()],
                ..Default::default()
            },
        )
        .unwrap();
        let g = Gateway::new(&GatewayConfig {
            credential_bundle: dir.path().join(pki::GATEWAY_BUNDLE),
            client_ca: dir.path().join(pki::CA_FILE),
            body_bytes: 16,
        })
        .unwrap();
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap().to_string();
        let cancel = CancellationToken::new();
        let (g2, c2) = (g.clone(), cancel.clone());
        tokio::spawn(async move { g2.serve(lis, c2).await });
        (g, addr, dir, cancel)
    }

    /// The tunnel's own egress client, presenting the router certificate,
    /// which the gateway, checking only the chain, accepts as an Actor's.
    fn egress_client(addr: &str, dir: &std::path::Path) -> Client {
        let cert = pki::certified_key(&dir.join(pki::ROUTER_BUNDLE)).unwrap();
        Client::new(ClientConfig {
            gateway_address: addr.into(),
            server_name: "127.0.0.1".into(),
            client_certificate: Arc::new(Fixed(cert)),
            trust_bundle_path: dir.join(pki::CA_FILE),
        })
        .unwrap()
    }

    async fn wait_for(cond: impl Fn() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !cond() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "condition not met within 5s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn gateway_through_the_tunnels_client() {
        let (g, addr, dir, cancel) = start_gateway().await;
        let client = egress_client(&addr, dir.path());

        let conn = client
            .dial(&format!("192.0.2.10:{ANSWER_PORT}"))
            .await
            .unwrap();
        let mut r = BufReader::new(conn);
        for _ in 0..2 {
            r.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
                .await
                .unwrap();
            r.flush().await.unwrap();
            let head = httpio::read_head(&mut r).await.unwrap().unwrap();
            let n = httpio::discard_body(&mut r, &head, BodyKind::Response)
                .await
                .unwrap();
            assert_eq!((head.status(), n), (Some(200), 16));
        }
        drop(r);

        let mut discard = client
            .dial(&format!("192.0.2.10:{DISCARD_PORT}"))
            .await
            .unwrap();
        discard.write_all(&[0u8; 1000]).await.unwrap();
        discard.shutdown().await.unwrap();
        // Read to the gateway's end of the tunnel, as the proxy does, so
        // the stream closes cleanly instead of being reset.
        let mut rest = Vec::new();
        discard.read_to_end(&mut rest).await.unwrap();
        drop(discard);

        wait_for(|| {
            let st = g.stats();
            st.requests == 2 && st.bytes_in >= 1000 && st.tunnels_open <= 1
        })
        .await;
        let st = g.stats();
        assert_eq!(
            (st.tunnels, st.tunnels_h2),
            (2, 2),
            "want 2 tunnels, both HTTP/2: {st:?}"
        );
        cancel.cancel();
    }

    #[tokio::test]
    async fn gateway_over_http1() {
        let (g, addr, dir, cancel) = start_gateway().await;
        let tls = pki::client_config(
            &dir.path().join(pki::ROUTER_BUNDLE),
            &dir.path().join(pki::CA_FILE),
            &[b"http/1.1"],
        )
        .unwrap();
        let tcp = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let conn = tokio_rustls::TlsConnector::from(tls)
            .connect(ServerName::try_from("127.0.0.1").unwrap(), tcp)
            .await
            .unwrap();
        let mut r = BufReader::new(conn);
        r.write_all(format!("CONNECT 192.0.2.10:{ANSWER_PORT} HTTP/1.1\r\nHost: 192.0.2.10:{ANSWER_PORT}\r\n\r\n").as_bytes()).await.unwrap();
        let head = httpio::read_head(&mut r).await.unwrap().unwrap();
        assert_eq!(head.status(), Some(200), "CONNECT: {head:?}");
        r.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let inner = httpio::read_head(&mut r).await.unwrap().unwrap();
        assert_eq!(inner.status(), Some(200), "request in tunnel: {inner:?}");
        let mut body = [0u8; 16];
        r.read_exact(&mut body).await.unwrap();
        let st = g.stats();
        assert_eq!(
            (st.tunnels, st.tunnels_h2),
            (1, 0),
            "want 1 HTTP/1.1 tunnel: {st:?}"
        );
        cancel.cancel();
    }

    #[tokio::test]
    async fn gateway_rejects_an_unknown_port() {
        let (g, addr, dir, cancel) = start_gateway().await;
        let client = egress_client(&addr, dir.path());
        assert!(
            client.dial("192.0.2.10:443").await.is_err(),
            "tunnel to an unknown port opened"
        );
        assert_eq!(g.stats().rejected, 1);
        cancel.cancel();
    }
}

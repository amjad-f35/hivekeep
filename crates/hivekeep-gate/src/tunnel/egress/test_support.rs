//! Test gateways (HTTP/1.1 and HTTP/2 CONNECT over mTLS) and clients.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use rustls::sign::CertifiedKey;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::client::{Client, ClientCertificateSource, ClientConfig, GatewayDial};
use super::BoxIo;
use crate::test_pki::{server_config, LeafOpts, TestCa, ALL_VERSIONS};

/// The SPIFFE ID test clients present.
pub const TEST_ACTOR_ID: &str = "spiffe://hivekeep-actor.local/worker-for-actor/team/actor";

/// A client certificate that can be swapped.
pub struct SwappableCert(pub Mutex<Arc<CertifiedKey>>);

impl ClientCertificateSource for SwappableCert {
    fn current(&self) -> Option<Arc<CertifiedKey>> {
        Some(self.0.lock().clone())
    }
}

/// A client leaf for the test Actor.
pub fn actor_leaf(ca: &TestCa) -> Arc<CertifiedKey> {
    ca.issue(&LeafOpts {
        uri: Some(TEST_ACTOR_ID.into()),
        client: true,
        ..Default::default()
    })
    .certified()
}

/// Dials `addr` over TCP.
pub fn dial_fixed(addr: String) -> GatewayDial {
    Arc::new(move || {
        let addr = addr.clone();
        Box::pin(async move { Ok(Box::new(TcpStream::connect(addr).await?) as BoxIo) })
    })
}

/// A stream that reports when it is dropped (closed).
pub struct NotifyOnDrop<T> {
    /// The wrapped stream.
    pub inner: T,
    /// Signalled on drop.
    pub tx: Option<mpsc::UnboundedSender<()>>,
}

impl<T> Drop for NotifyOnDrop<T> {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for NotifyOnDrop<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for NotifyOnDrop<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Dials `addr`, counting dials and reporting when connection `nth` (1-based,
/// 0 for all) closes.
pub fn dial_counting(
    addr: String,
    dials: Arc<std::sync::atomic::AtomicU32>,
    nth: u32,
    closed: mpsc::UnboundedSender<()>,
) -> GatewayDial {
    Arc::new(move || {
        let (addr, dials, closed) = (addr.clone(), dials.clone(), closed.clone());
        Box::pin(async move {
            let s = TcpStream::connect(addr).await?;
            let n = dials.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            let tx = (nth == 0 || n == nth).then_some(closed);
            Ok(Box::new(NotifyOnDrop { inner: s, tx }) as BoxIo)
        })
    })
}

/// A client trusting `ca`, presenting a fresh test Actor certificate.
pub fn new_test_client(
    ca: &TestCa,
    dial: GatewayDial,
) -> (Client, Arc<SwappableCert>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let trust = dir.path().join("trust.pem");
    std::fs::write(&trust, ca.pem()).unwrap();
    let cert = Arc::new(SwappableCert(Mutex::new(actor_leaf(ca))));
    let client = Client::with_dialer(
        ClientConfig {
            gateway_address: "127.0.0.1:1".into(),
            server_name: "egress.test".into(),
            client_certificate: cert.clone(),
            trust_bundle_path: trust,
        },
        dial,
    )
    .unwrap();
    (client, cert, dir)
}

/// A CONNECT request as an HTTP/1.1 gateway read it.
#[derive(Debug, Clone)]
pub struct H1Request {
    /// Request method.
    pub method: String,
    /// Request target.
    pub target: String,
    /// Header lines, lowercase names.
    pub headers: Vec<(String, String)>,
    /// The client's leaf certificate.
    pub peer_leaf: Vec<u8>,
}

impl H1Request {
    /// The Host header.
    pub fn host(&self) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == "host")
            .map(|(_, v)| v.as_str())
    }
}

/// The TLS stream an HTTP/1.1 test gateway hands its handler.
pub type GatewayStream = tokio_rustls::server::TlsStream<TcpStream>;

/// Serves one HTTP/1.1 CONNECT over mTLS, handing the stream to `handle`.
pub async fn serve_connect_gateway<F>(ca: &TestCa, handle: F) -> String
where
    F: FnOnce(GatewayStream, H1Request) -> BoxFuture<'static, ()> + Send + 'static,
{
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let cfg = server_config(&ca.server("egress.test"), Some(ca), &[], ALL_VERSIONS);
    tokio::spawn(async move {
        let Ok((tcp, _)) = lis.accept().await else {
            return;
        };
        let Ok(mut tls) = tokio_rustls::TlsAcceptor::from(cfg).accept(tcp).await else {
            return;
        };
        let mut head = Vec::new();
        let mut b = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            if tls.read(&mut b).await.unwrap_or(0) == 0 {
                return;
            }
            head.push(b[0]);
        }
        let text = String::from_utf8_lossy(&head).to_string();
        let mut lines = text.split("\r\n");
        let mut first = lines.next().unwrap_or("").split(' ');
        let method = first.next().unwrap_or("").to_string();
        let target = first.next().unwrap_or("").to_string();
        let headers = lines
            .filter_map(|l| l.split_once(':'))
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
            .collect();
        let peer_leaf = tls
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|c| c.first())
            .map(|c| c.to_vec())
            .unwrap_or_default();
        handle(
            tls,
            H1Request {
                method,
                target,
                headers,
                peer_leaf,
            },
        )
        .await;
    });
    addr
}

/// One CONNECT stream as an HTTP/2 test gateway sees it.
pub struct H2Request {
    /// The request head.
    pub head: http::request::Parts,
    /// The client's leaf certificate.
    pub peer_leaf: Vec<u8>,
    /// The request body (the client's half of the tunnel).
    pub body: h2::RecvStream,
    /// Where the response goes.
    pub respond: h2::server::SendResponse<Bytes>,
}

impl H2Request {
    /// Answers 200 and returns the response body stream.
    pub fn ok(&mut self) -> h2::SendStream<Bytes> {
        self.respond
            .send_response(
                http::Response::builder().status(200).body(()).unwrap(),
                false,
            )
            .unwrap()
    }

    /// Answers `status` with `body` and ends the stream.
    pub fn error(&mut self, status: u16, body: &str) {
        let mut s = self
            .respond
            .send_response(
                http::Response::builder().status(status).body(()).unwrap(),
                false,
            )
            .unwrap();
        let _ = s.send_data(Bytes::from(format!("{body}\n")), true);
    }

    /// The `:authority` of the CONNECT.
    pub fn authority(&self) -> String {
        self.head
            .uri
            .authority()
            .map(|a| a.to_string())
            .unwrap_or_default()
    }
}

/// Reads a request body to its end.
pub async fn read_body(body: &mut h2::RecvStream) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(Ok(d)) = body.data().await {
        let _ = body.flow_control().release_capacity(d.len());
        out.extend_from_slice(&d);
    }
    out
}

/// An HTTP/2 stream handler.
pub type H2Handler = Arc<dyn Fn(H2Request) -> BoxFuture<'static, ()> + Send + Sync>;

/// Serves HTTP/2 CONNECT over mTLS with `handler`; the returned function
/// closes every connection accepted so far.
pub async fn serve_h2_gateway(
    ca: &TestCa,
    handler: H2Handler,
) -> (String, impl Fn() + Send + Sync) {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let cfg = server_config(&ca.server("egress.test"), Some(ca), &[b"h2"], ALL_VERSIONS);
    let conns: Arc<Mutex<Vec<CancellationToken>>> = Arc::default();
    let c2 = conns.clone();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = lis.accept().await {
            let (cfg, handler) = (cfg.clone(), handler.clone());
            let token = CancellationToken::new();
            c2.lock().push(token.clone());
            tokio::spawn(async move {
                let Ok(tls) = tokio_rustls::TlsAcceptor::from(cfg).accept(tcp).await else {
                    return;
                };
                let peer_leaf = tls
                    .get_ref()
                    .1
                    .peer_certificates()
                    .and_then(|c| c.first())
                    .map(|c| c.to_vec())
                    .unwrap_or_default();
                let Ok(mut conn) = h2::server::handshake(tls).await else {
                    return;
                };
                loop {
                    let next = tokio::select! {
                        n = conn.accept() => n,
                        _ = token.cancelled() => return,
                    };
                    let Some(Ok((req, respond))) = next else {
                        return;
                    };
                    let (head, body) = req.into_parts();
                    let h = handler.clone();
                    let leaf = peer_leaf.clone();
                    let t = token.clone();
                    tokio::spawn(async move {
                        tokio::select! {
                            _ = h(H2Request { head, peer_leaf: leaf, body, respond }) => {}
                            _ = t.cancelled() => {}
                        }
                    });
                }
            });
        }
    });
    let close = move || {
        for t in conns.lock().drain(..) {
            t.cancel();
        }
    };
    (addr, close)
}

/// An HTTP/2 gateway echoing every stream.
pub async fn serve_h2_echo_gateway(ca: &TestCa) -> String {
    let handler: H2Handler = Arc::new(|mut r: H2Request| {
        Box::pin(async move {
            let mut send = r.ok();
            while let Some(Ok(d)) = r.body.data().await {
                let n = d.len();
                send.reserve_capacity(n);
                let mut left = d;
                while !left.is_empty() {
                    let Some(Ok(cap)) = futures::future::poll_fn(|cx| send.poll_capacity(cx)).await
                    else {
                        return;
                    };
                    let chunk = left.split_to(cap.min(left.len()));
                    if send.send_data(chunk, false).is_err() {
                        return;
                    }
                }
                let _ = r.body.flow_control().release_capacity(n);
            }
            let _ = send.send_data(Bytes::new(), true);
        })
    });
    serve_h2_gateway(ca, handler).await.0
}

/// A gateway that completes (or fails) the handshake and hangs up: its
/// client CA is not the one the client chains to.
pub async fn serve_refusing_gateway(
    server_ca: &TestCa,
    client_ca: &TestCa,
    versions: &[&'static rustls::SupportedProtocolVersion],
    alpn: &[&[u8]],
) -> String {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let cfg = server_config(
        &server_ca.server("egress.test"),
        Some(client_ca),
        alpn,
        versions,
    );
    tokio::spawn(async move {
        let Ok((tcp, _)) = lis.accept().await else {
            return;
        };
        let _ = tokio_rustls::TlsAcceptor::from(cfg).accept(tcp).await;
    });
    addr
}

/// Waits up to five seconds for a message.
pub async fn recv_within<T>(rx: &mut mpsc::UnboundedReceiver<T>, what: &str) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
        .unwrap_or_else(|| panic!("channel closed waiting for {what}"))
}

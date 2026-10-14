//! The tunnel client: opens CONNECT tunnels to the egress gateway over
//! mutually authenticated TLS, speaking HTTP/2 when the gateway offers it
//! (one connection multiplexing every tunnel of the Actor) and HTTP/1.1
//! otherwise (one connection per tunnel).
//!
//! When the Actor's certificate is renewed, HTTP/2 connections that
//! presented the old one are retired: they carry no new tunnels and close
//! once their last tunnel ends.

use std::io;
use std::net::IpAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::BoxFuture;
use h2::client::SendRequest;
use parking_lot::Mutex;
use rustls::client::ResolvesClientCert;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::sign::CertifiedKey;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use super::error::{connect_exchange_error, TunnelError};
use super::h2_stream::{h2_to_io, H2Stream};
use super::BoxIo;

/// Most of a rejection body kept in the error.
const MAX_REJECTION_BODY: usize = 4 << 10;
/// Largest CONNECT response head accepted.
const MAX_RESPONSE_HEAD: usize = 16 << 10;

/// The Actor certificate presented to the gateway, possibly renewed over
/// time.
pub trait ClientCertificateSource: Send + Sync + 'static {
    /// The certificate to present now, if one is valid.
    fn current(&self) -> Option<Arc<CertifiedKey>>;
}

/// Opens the transport to the gateway (TCP by default).
pub type GatewayDial = Arc<dyn Fn() -> BoxFuture<'static, io::Result<BoxIo>> + Send + Sync>;

/// How to reach and authenticate to the egress gateway.
pub struct ClientConfig {
    /// `host:port` of the gateway.
    pub gateway_address: String,
    /// The name the gateway's certificate must carry.
    pub server_name: String,
    /// The Actor's client certificate.
    pub client_certificate: Arc<dyn ClientCertificateSource>,
    /// PEM file of CAs the gateway's certificate chains to.
    pub trust_bundle_path: std::path::PathBuf,
}

struct ConnFailure {
    kind: io::ErrorKind,
    alert: Option<rustls::AlertDescription>,
    message: String,
}

impl ConnFailure {
    fn of(e: &io::Error) -> Self {
        let alert = e
            .get_ref()
            .and_then(|i| i.downcast_ref::<rustls::Error>())
            .and_then(|r| match r {
                rustls::Error::AlertReceived(a) => Some(*a),
                _ => None,
            });
        ConnFailure {
            kind: e.kind(),
            alert,
            message: e.to_string(),
        }
    }

    fn to_io(&self) -> io::Error {
        match self.alert {
            Some(a) => io::Error::new(io::ErrorKind::InvalidData, rustls::Error::AlertReceived(a)),
            None => io::Error::new(self.kind, self.message.clone()),
        }
    }
}

/// One HTTP/2 connection to the gateway.
struct H2Conn {
    send: SendRequest<Bytes>,
    cert: Option<CertificateDer<'static>>,
    closed: Arc<AtomicBool>,
    failure: Arc<Mutex<Option<ConnFailure>>>,
    verified: AtomicBool,
}

impl H2Conn {
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn conn_err(&self, fallback: io::Error) -> io::Error {
        self.failure
            .lock()
            .as_ref()
            .map(|f| f.to_io())
            .unwrap_or(fallback)
    }
}

#[derive(Default)]
struct Pool {
    closed: bool,
    conns: Vec<Arc<H2Conn>>,
}

/// Records which certificate a handshake presented.
struct RecordingResolver {
    source: Arc<dyn ClientCertificateSource>,
    presented: Mutex<Option<CertificateDer<'static>>>,
}

impl std::fmt::Debug for RecordingResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordingResolver")
    }
}

impl ResolvesClientCert for RecordingResolver {
    fn resolve(&self, _: &[&[u8]], _: &[rustls::SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        let cert = self.source.current();
        *self.presented.lock() = cert.as_ref().and_then(|c| c.cert.first().cloned());
        cert
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// Opens tunnels through the egress gateway.
pub struct Client {
    gateway: String,
    server_name: ServerName<'static>,
    roots: Arc<rustls::RootCertStore>,
    certs: Arc<dyn ClientCertificateSource>,
    dial: GatewayDial,
    pool: Mutex<Pool>,
    dial_lock: tokio::sync::Mutex<()>,
}

/// A tunnel opened over HTTP/1.1: the TLS connection, with any bytes the
/// gateway sent right after its response head.
pub struct BufferedConn {
    buffered: Bytes,
    inner: TlsStream<BoxIo>,
}

impl AsyncRead for BufferedConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.buffered.is_empty() {
            let n = self.buffered.len().min(buf.remaining());
            let chunk = self.buffered.split_to(n);
            buf.put_slice(&chunk);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for BufferedConn {
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

/// Checks a destination is `<ip literal>:<port>` with a port in 1..=65535.
pub fn validate_destination(destination: &str) -> Result<(), TunnelError> {
    let bad = |reason| TunnelError::InvalidDestination {
        destination: destination.to_string(),
        reason,
    };
    let (host, port) = if let Some(rest) = destination.strip_prefix('[') {
        let (h, after) = rest.split_once(']').ok_or_else(|| bad("missing ']'"))?;
        (
            h,
            after.strip_prefix(':').ok_or_else(|| bad("missing port"))?,
        )
    } else {
        let (h, p) = destination
            .rsplit_once(':')
            .ok_or_else(|| bad("missing port"))?;
        if h.contains(':') {
            return Err(bad("too many colons"));
        }
        (h, p)
    };
    if host.is_empty() {
        return Err(bad("host is empty"));
    }
    if host.parse::<IpAddr>().is_err() {
        return Err(bad("host must be an IP address"));
    }
    if super::parse_port(port).is_none() {
        return Err(bad("port must be between 1 and 65535"));
    }
    Ok(())
}

enum Acquired {
    H2(Arc<H2Conn>),
    H1(Box<TlsStream<BoxIo>>),
}

enum H2Outcome {
    Stream(H2Stream),
    Retry,
}

fn same_cert(a: Option<&CertificateDer<'_>>, b: Option<&Arc<CertifiedKey>>) -> bool {
    match (a, b.and_then(|c| c.cert.first())) {
        (Some(a), Some(b)) => a.as_ref() == b.as_ref(),
        (None, None) => true,
        _ => false,
    }
}

impl Client {
    /// A client for `cfg`, dialing the gateway over TCP.
    pub fn new(cfg: ClientConfig) -> Result<Client, io::Error> {
        let addr = cfg.gateway_address.clone();
        let dial: GatewayDial = Arc::new(move || {
            let addr = addr.clone();
            Box::pin(async move {
                let s = tokio::net::TcpStream::connect(addr.as_str()).await?;
                Ok(Box::new(s) as BoxIo)
            })
        });
        Client::with_dialer(cfg, dial)
    }

    /// A client using `dial` to reach the gateway.
    pub fn with_dialer(cfg: ClientConfig, dial: GatewayDial) -> Result<Client, io::Error> {
        let invalid = |m: String| io::Error::new(io::ErrorKind::InvalidInput, m);
        if cfg.gateway_address.rsplit_once(':').is_none() {
            return Err(invalid(format!(
                "egress: invalid egress gateway address {:?}",
                cfg.gateway_address
            )));
        }
        if cfg.server_name.is_empty() {
            return Err(invalid("egress: gateway server name is required".into()));
        }
        if cfg.trust_bundle_path.as_os_str().is_empty() {
            return Err(invalid("egress: trust bundle path is required".into()));
        }
        let roots = load_roots(&cfg.trust_bundle_path)?;
        let server_name = ServerName::try_from(cfg.server_name.clone())
            .map_err(|e| invalid(format!("egress: invalid server name: {e}")))?;
        Ok(Client {
            gateway: cfg.gateway_address,
            server_name,
            roots: Arc::new(roots),
            certs: cfg.client_certificate,
            dial,
            pool: Mutex::new(Pool::default()),
            dial_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// The gateway address.
    pub fn gateway_address(&self) -> &str {
        &self.gateway
    }

    /// Stops opening tunnels and closes idle connections; open tunnels
    /// finish on their own.
    pub fn close(&self) {
        let mut pool = self.pool.lock();
        pool.closed = true;
        pool.conns.clear();
    }

    /// Opens a tunnel to `destination` (`<ip>:<port>`).
    pub async fn dial(&self, destination: &str) -> Result<BoxIo, TunnelError> {
        validate_destination(destination)?;
        loop {
            match self.acquire().await? {
                Acquired::H1(tls) => {
                    return self
                        .connect_h1(*tls, destination)
                        .await
                        .map(|c| Box::new(c) as BoxIo)
                }
                Acquired::H2(conn) => match self.connect_h2(&conn, destination).await? {
                    H2Outcome::Stream(s) => return Ok(Box::new(s)),
                    H2Outcome::Retry => {
                        self.remove(&conn);
                        continue;
                    }
                },
            }
        }
    }

    fn remove(&self, conn: &Arc<H2Conn>) {
        self.pool.lock().conns.retain(|c| !Arc::ptr_eq(c, conn));
    }

    /// A usable pooled HTTP/2 connection, retiring those that presented a
    /// certificate other than the current one.
    fn pick(&self) -> Result<Option<Arc<H2Conn>>, TunnelError> {
        let current = self.certs.current();
        let mut pool = self.pool.lock();
        if pool.closed {
            return Err(TunnelError::Closed);
        }
        pool.conns
            .retain(|c| !c.is_closed() && same_cert(c.cert.as_ref(), current.as_ref()));
        Ok(pool.conns.first().cloned())
    }

    async fn acquire(&self) -> Result<Acquired, TunnelError> {
        if let Some(c) = self.pick()? {
            return Ok(Acquired::H2(c));
        }
        let _guard = self.dial_lock.lock().await;
        if let Some(c) = self.pick()? {
            return Ok(Acquired::H2(c));
        }
        let (tls, presented) = self.handshake().await?;
        if tls.get_ref().1.alpn_protocol() != Some(b"h2") {
            return Ok(Acquired::H1(Box::new(tls)));
        }
        let (send, connection) = h2::client::Builder::new()
            .handshake::<_, Bytes>(tls)
            .await
            .map_err(|e| connect_exchange_error("writing CONNECT request", h2_to_io(e)))?;
        let closed = Arc::new(AtomicBool::new(false));
        let failure = Arc::new(Mutex::new(None));
        let (c2, f2) = (closed.clone(), failure.clone());
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                let e = h2_to_io(e);
                *f2.lock() = Some(ConnFailure::of(&e));
            }
            c2.store(true, Ordering::Release);
        });
        let conn = Arc::new(H2Conn {
            send,
            cert: presented,
            closed,
            failure,
            verified: AtomicBool::new(false),
        });
        let mut pool = self.pool.lock();
        if pool.closed {
            return Err(TunnelError::Closed);
        }
        pool.conns.push(conn.clone());
        Ok(Acquired::H2(conn))
    }

    async fn handshake(
        &self,
    ) -> Result<(TlsStream<BoxIo>, Option<CertificateDer<'static>>), TunnelError> {
        let io = (self.dial)().await.map_err(TunnelError::Connect)?;
        let resolver = Arc::new(RecordingResolver {
            source: self.certs.clone(),
            presented: Mutex::new(None),
        });
        let mut cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| TunnelError::Connect(io::Error::other(e)))?
        .with_root_certificates(self.roots.clone())
        .with_client_cert_resolver(resolver.clone());
        cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let tls = TlsConnector::from(Arc::new(cfg))
            .connect(self.server_name.clone(), io)
            .await
            .map_err(|source| TunnelError::GatewayHandshake { op: None, source })?;
        let presented = resolver.presented.lock().take();
        Ok((tls, presented))
    }

    async fn connect_h1(
        &self,
        mut tls: TlsStream<BoxIo>,
        destination: &str,
    ) -> Result<BufferedConn, TunnelError> {
        let req = format!("CONNECT {destination} HTTP/1.1\r\nHost: {destination}\r\n\r\n");
        tls.write_all(req.as_bytes())
            .await
            .map_err(|e| connect_exchange_error("writing CONNECT request", e))?;
        let (head, rest) = read_head(&mut tls)
            .await
            .map_err(|e| connect_exchange_error("reading CONNECT response", e))?;
        let (status, reason, content_length) =
            parse_head(&head).map_err(|e| connect_exchange_error("reading CONNECT response", e))?;
        if (200..300).contains(&status) {
            return Ok(BufferedConn {
                buffered: Bytes::from(rest),
                inner: tls,
            });
        }
        let mut body = rest;
        let want = content_length
            .unwrap_or(MAX_REJECTION_BODY)
            .min(MAX_REJECTION_BODY);
        let mut buf = [0u8; 1024];
        while body.len() < want {
            match tls.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => body.extend_from_slice(&buf[..n]),
            }
        }
        body.truncate(want);
        Err(rejected(status, reason, &body))
    }

    async fn connect_h2(
        &self,
        conn: &Arc<H2Conn>,
        destination: &str,
    ) -> Result<H2Outcome, TunnelError> {
        let was_verified = conn.verified.load(Ordering::Acquire);
        let fail = |e: h2::Error, conn: &Arc<H2Conn>| -> Result<H2Outcome, TunnelError> {
            let connection_level = conn.is_closed() || e.is_io() || e.is_go_away();
            if was_verified && connection_level {
                return Ok(H2Outcome::Retry);
            }
            if conn.is_closed() || connection_level {
                self.remove(conn);
            }
            Err(connect_exchange_error(
                "reading CONNECT response",
                conn.conn_err(h2_to_io(e)),
            ))
        };
        let mut send = match conn.send.clone().ready().await {
            Ok(s) => s,
            Err(e) => return fail(e, conn),
        };
        let req = http::Request::builder()
            .method(http::Method::CONNECT)
            .uri(destination)
            .body(())
            .map_err(|e| TunnelError::Exchange {
                op: "writing CONNECT request",
                source: io::Error::other(e),
            })?;
        let (response, send_stream) = match send.send_request(req, false) {
            Ok(r) => r,
            Err(e) => return fail(e, conn),
        };
        let resp = match response.await {
            Ok(r) => r,
            Err(e) => return fail(e, conn),
        };
        conn.verified.store(true, Ordering::Release);
        let status = resp.status();
        let mut recv = resp.into_body();
        if !status.is_success() {
            let mut body = Vec::new();
            while body.len() < MAX_REJECTION_BODY {
                match recv.data().await {
                    Some(Ok(d)) => {
                        let _ = recv.flow_control().release_capacity(d.len());
                        body.extend_from_slice(&d);
                    }
                    _ => break,
                }
            }
            body.truncate(MAX_REJECTION_BODY);
            return Err(rejected(
                status.as_u16(),
                status.canonical_reason().unwrap_or("").to_string(),
                &body,
            ));
        }
        let guard: Box<dyn Send + Sync> = Box::new(conn.clone());
        Ok(H2Outcome::Stream(H2Stream::new(
            send_stream,
            recv,
            Some(guard),
        )))
    }
}

fn rejected(status: u16, reason: String, body: &[u8]) -> TunnelError {
    let mut message = String::from_utf8_lossy(body).trim().to_string();
    if message.is_empty() {
        message = http::StatusCode::from_u16(status)
            .ok()
            .and_then(|s| s.canonical_reason())
            .unwrap_or("")
            .to_string();
    }
    TunnelError::ConnectRejected {
        status,
        reason,
        message,
    }
}

/// Reads up to and including the blank line ending a response head.
async fn read_head<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<(Vec<u8>, Vec<u8>)> {
    let mut data = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        if let Some(i) = data.windows(4).position(|w| w == b"\r\n\r\n") {
            let rest = data.split_off(i + 4);
            return Ok((data, rest));
        }
        if data.len() > MAX_RESPONSE_HEAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CONNECT response head too large",
            ));
        }
        let n = r.read(&mut buf).await?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        data.extend_from_slice(&buf[..n]);
    }
}

/// Status code, reason phrase and Content-Length of a response head.
fn parse_head(head: &[u8]) -> io::Result<(u16, String, Option<usize>)> {
    let malformed = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "malformed HTTP response {:?}",
                String::from_utf8_lossy(head)
            ),
        )
    };
    let text = std::str::from_utf8(head).map_err(|_| malformed())?;
    let mut lines = text.split("\r\n");
    let status_line = lines.next().ok_or_else(malformed)?;
    let mut parts = status_line.splitn(3, ' ');
    let proto = parts.next().unwrap_or("");
    if !proto.starts_with("HTTP/1.") {
        return Err(malformed());
    }
    let status: u16 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(malformed)?;
    let reason = parts.next().unwrap_or("").to_string();
    let content_length = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse().ok());
    Ok((status, reason, content_length))
}

/// Loads the PEM certificates of `path` into a root store.
pub fn load_roots(path: &Path) -> io::Result<rustls::RootCertStore> {
    let pem = std::fs::read(path)
        .map_err(|e| io::Error::new(e.kind(), format!("egress: reading trust bundle: {e}")))?;
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
        let cert = cert?;
        roots
            .add(cert)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    }
    if roots.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("egress: trust bundle {:?} contains no certificates", path),
        ));
    }
    Ok(roots)
}

impl super::TunnelDialer for Client {
    fn dial<'a>(&'a self, destination: &'a str) -> BoxFuture<'a, Result<BoxIo, TunnelError>> {
        Box::pin(Client::dial(self, destination))
    }

    fn close(&self) {
        Client::close(self);
    }
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;

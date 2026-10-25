//! The loop's HTTP/1.1 client: one kept-alive connection per endpoint,
//! dialed and TLS-wrapped by hand so each request can report whether it
//! opened a connection, how long its lookup took and how long its handshake
//! took.

use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use rustls::ClientConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_rustls::TlsConnector;

use super::classify::{DnsFailure, RequestError};

/// The most the client reads of an endpoint's answer; the rest is dropped
/// with the connection.
pub const MAX_BODY_BYTES: usize = 1 << 20;

/// A byte stream a connection runs over.
pub trait Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Stream for T {}

/// A boxed [`Stream`].
pub type BoxStream = Box<dyn Stream>;

/// Opens one connection to `host:port`, in place of a resolving TCP dial.
/// Tests send the in-cluster endpoint names to local listeners through it.
pub type DialFn = Arc<
    dyn Fn(String, u16) -> Pin<Box<dyn Future<Output = io::Result<BoxStream>> + Send>>
        + Send
        + Sync,
>;

/// What one request did on the way, as the moments its steps happened.
#[derive(Debug, Default, Clone)]
pub struct Trace {
    /// The request went out on a connection opened for it.
    pub new_conn: bool,
    /// Lookup start and end.
    pub dns_start: Option<Instant>,
    /// Set once the lookup returned.
    pub dns_done: Option<Instant>,
    /// Handshake start.
    pub tls_start: Option<Instant>,
    /// Set only when the handshake completed.
    pub tls_done: Option<Instant>,
}

/// Where one endpoint lives.
#[derive(Debug, Clone)]
struct Target {
    https: bool,
    host: String,
    port: u16,
    authority: String,
    path: String,
}

fn parse_target(url: &str) -> Result<Target, String> {
    let u = url::Url::parse(url).map_err(|e| format!("parsing {url:?}: {e}"))?;
    let https = match u.scheme() {
        "http" => false,
        "https" => true,
        s => return Err(format!("unsupported scheme {s:?}")),
    };
    let host = u
        .host_str()
        .ok_or_else(|| format!("{url:?} has no host"))?
        .to_string();
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    let port = u
        .port_or_known_default()
        .unwrap_or(if https { 443 } else { 80 });
    let authority = match u.port() {
        Some(p) => format!("{}:{p}", u.host_str().unwrap_or_default()),
        None => u.host_str().unwrap_or_default().to_string(),
    };
    let mut path = u.path().to_string();
    if let Some(q) = u.query() {
        path = format!("{path}?{q}");
    }
    Ok(Target {
        https,
        host,
        port,
        authority,
        path,
    })
}

struct Conn {
    sender: http1::SendRequest<Empty<Bytes>>,
    task: JoinHandle<()>,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The client of one endpoint. At most one request is in flight on it.
pub struct EndpointClient {
    target: Target,
    dial: Option<DialFn>,
    tls: Arc<ClientConfig>,
    conn: Option<Conn>,
}

impl EndpointClient {
    /// A client for `url`, dialing through `dial` (a resolving TCP dial when
    /// `None`) and verifying HTTPS with `tls`.
    pub fn new(url: &str, dial: Option<DialFn>, tls: Arc<ClientConfig>) -> Result<Self, String> {
        Ok(EndpointClient {
            target: parse_target(url)?,
            dial,
            tls,
            conn: None,
        })
    }

    /// Drops the kept-alive connection, if any.
    pub fn reset(&mut self) {
        self.conn = None;
    }

    /// Sends one GET, filling `trace`, and returns the answer's status. A
    /// failure drops the connection. A request that failed on a kept-alive
    /// connection before any of its answer arrived is retried once on a new
    /// one, as a reset tunnel is redialed rather than counted as an error.
    pub async fn get(&mut self, trace: &mut Trace) -> Result<u16, RequestError> {
        let mut reused = false;
        if let Some(c) = self.conn.as_mut() {
            if c.sender.ready().await.is_ok() {
                reused = true;
            } else {
                self.conn = None;
            }
        }
        if self.conn.is_none() {
            self.conn = Some(self.connect(trace).await?);
            trace.new_conn = true;
        }
        match self.send().await {
            Ok(status) => Ok(status),
            Err((err, retryable)) if reused && retryable => {
                let _ = err;
                self.conn = Some(self.connect(trace).await?);
                trace.new_conn = true;
                self.send().await.map_err(|(e, _)| {
                    self.conn = None;
                    e
                })
            }
            Err((err, _)) => {
                self.conn = None;
                Err(err)
            }
        }
    }

    async fn send(&mut self) -> Result<u16, (RequestError, bool)> {
        let conn = self.conn.as_mut().expect("connected");
        let req = hyper::Request::get(&self.target.path)
            .header(hyper::header::HOST, &self.target.authority)
            .body(Empty::<Bytes>::new())
            .map_err(|e| (RequestError::Other(e.to_string()), false))?;
        let resp = conn.sender.send_request(req).await.map_err(|e| {
            let retryable = e.is_incomplete_message() || e.is_canceled() || e.is_closed();
            (hyper_error(e, "read"), retryable)
        })?;
        let status = resp.status().as_u16();
        let mut body = resp.into_body();
        let mut read = 0usize;
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| (hyper_error(e, "read"), false))?;
            if let Some(data) = frame.data_ref() {
                read += data.len();
                if read > MAX_BODY_BYTES {
                    // The rest is not read, so the connection cannot be reused.
                    self.conn = None;
                    break;
                }
            }
        }
        Ok(status)
    }

    async fn connect(&self, trace: &mut Trace) -> Result<Conn, RequestError> {
        let t = &self.target;
        let tcp: BoxStream = match &self.dial {
            Some(dial) => dial(t.host.clone(), t.port)
                .await
                .map_err(|err| RequestError::Io { op: "dial", err })?,
            None => Box::new(dial_tcp(&t.host, t.port, trace).await?),
        };
        let stream: BoxStream = if t.https {
            let name = ServerName::try_from(t.host.clone())
                .map_err(|e| RequestError::Other(format!("server name {:?}: {e}", t.host)))?;
            trace.tls_start = Some(Instant::now());
            let tls = TlsConnector::from(self.tls.clone())
                .connect(name, tcp)
                .await
                .map_err(tls_error)?;
            trace.tls_done = Some(Instant::now());
            Box::new(tls)
        } else {
            tcp
        };
        let (sender, conn) = http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| hyper_error(e, "write"))?;
        let task = tokio::spawn(async move {
            let _ = conn.await;
        });
        Ok(Conn { sender, task })
    }
}

/// Resolves `host` (recording the lookup) unless it is an address, then
/// connects to each address in turn until one answers.
async fn dial_tcp(host: &str, port: u16, trace: &mut Trace) -> Result<TcpStream, RequestError> {
    let addrs: Vec<std::net::SocketAddr> = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![(ip, port).into()]
    } else {
        trace.dns_start = Some(Instant::now());
        let res = tokio::net::lookup_host((host, port)).await;
        trace.dns_done = Some(Instant::now());
        match res {
            Ok(it) => {
                let v: Vec<_> = it.collect();
                if v.is_empty() {
                    return Err(RequestError::Dns(DnsFailure::NotFound));
                }
                v
            }
            Err(e) => return Err(RequestError::Dns(dns_failure(&e))),
        }
    };
    let mut last = io::Error::other("no addresses");
    for addr in addrs {
        match TcpStream::connect(addr).await {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                return Ok(s);
            }
            Err(e) => last = e,
        }
    }
    Err(RequestError::Io {
        op: "dial",
        err: last,
    })
}

fn dns_failure(e: &io::Error) -> DnsFailure {
    if e.kind() == io::ErrorKind::TimedOut {
        return DnsFailure::Timeout;
    }
    let msg = e.to_string().to_lowercase();
    if msg.contains("not known")
        || msg.contains("no address")
        || msg.contains("not found")
        || msg.contains("no such host")
    {
        DnsFailure::NotFound
    } else if msg.contains("timed out") || msg.contains("temporary failure") {
        DnsFailure::Timeout
    } else {
        DnsFailure::Other
    }
}

fn tls_error(err: io::Error) -> RequestError {
    if err.get_ref().is_some_and(|e| e.is::<rustls::Error>()) {
        let inner = err.into_inner().expect("checked");
        return RequestError::Tls(*inner.downcast::<rustls::Error>().expect("checked"));
    }
    RequestError::Io { op: "read", err }
}

fn hyper_error(e: hyper::Error, op: &'static str) -> RequestError {
    if e.is_incomplete_message() {
        return RequestError::Eof;
    }
    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&e);
    while let Some(s) = source {
        if let Some(io) = s.downcast_ref::<io::Error>() {
            return RequestError::Io {
                op,
                err: io::Error::new(io.kind(), io.to_string()),
            };
        }
        source = s.source();
    }
    RequestError::Other(e.to_string())
}

/// The client TLS configuration: `roots` when given, else the platform's
/// roots (which `SSL_CERT_FILE` extends with the gateway's interception CA).
/// HTTP/1.1 only: one request per connection at a time.
pub fn tls_config(roots: Option<rustls::RootCertStore>) -> Arc<ClientConfig> {
    let roots = roots.unwrap_or_else(|| {
        let mut store = rustls::RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs().certs {
            let _ = store.add(cert);
        }
        store
    });
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("ring supports the default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Arc::new(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets_keep_host_port_and_path() {
        let t = parse_target("http://127.0.0.1:8080/").unwrap();
        assert_eq!(
            (
                t.https,
                t.host.as_str(),
                t.port,
                t.authority.as_str(),
                t.path.as_str()
            ),
            (false, "127.0.0.1", 8080, "127.0.0.1:8080", "/")
        );
        let t = parse_target("https://endpoint-3/").unwrap();
        assert_eq!(
            (t.https, t.port, t.authority.as_str()),
            (true, 443, "endpoint-3")
        );
        assert!(parse_target("ftp://x/").is_err());
    }
}

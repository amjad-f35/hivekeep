//! A gRPC connection to a CSI driver and its Identity, Controller and Node
//! service clients.
//!
//! The connection is lazy: building a client never dials, and the first
//! call connects (and reconnects after a failure), so a driver that is
//! still starting does not make construction fail.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tonic::transport::{Channel, Endpoint as TonicEndpoint};

use super::endpoint::{parse_endpoint, Endpoint};
use super::pb::controller_client::ControllerClient;
use super::pb::identity_client::IdentityClient;
use super::pb::node_client::NodeClient;
use super::tls::ControllerTls;

/// How long one connection attempt may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// A byte stream to the driver, plaintext or TLS, over a socket or TCP.
pub trait DriverIo: AsyncRead + AsyncWrite + Send + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> DriverIo for T {}

/// The client side of a CSI driver.
#[derive(Debug, Clone)]
pub struct CsiClient {
    endpoint: Endpoint,
    channel: Channel,
}

impl CsiClient {
    /// A client of the driver at `endpoint`, over `tls` when given.
    pub fn new(endpoint: &str, tls: Option<&ControllerTls>) -> Result<Self, String> {
        let ep = parse_endpoint(endpoint)?;
        let tls = match tls {
            Some(t) => Some((t.client_config()?, t.rustls_server_name()?)),
            None => None,
        };
        let authority = match &ep {
            Endpoint::Unix { .. } => "csi.local".to_string(),
            other => dial_address(other.host_port().unwrap_or_default()),
        };
        let uri = format!("http://{authority}");
        let tonic_ep = TonicEndpoint::from_shared(uri.clone())
            .or_else(|_| TonicEndpoint::from_shared("http://csi.local".to_string()))
            .map_err(|e| format!("failed to dial CSI endpoint {:?}: {e}", ep.target()))?
            .connect_timeout(CONNECT_TIMEOUT);
        let channel = tonic_ep.connect_with_connector_lazy(Connector {
            target: Arc::new(ep.clone()),
            tls,
        });
        Ok(CsiClient {
            endpoint: ep,
            channel,
        })
    }

    /// The parsed endpoint.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The Identity service.
    pub fn identity(&self) -> IdentityClient<Channel> {
        IdentityClient::new(self.channel.clone())
    }

    /// The Controller service.
    pub fn controller(&self) -> ControllerClient<Channel> {
        ControllerClient::new(self.channel.clone())
    }

    /// The Node service.
    pub fn node(&self) -> NodeClient<Channel> {
        NodeClient::new(self.channel.clone())
    }
}

/// A `host:port` with the gaps filled: a missing host is the local host and
/// a missing port the TLS default.
fn dial_address(host_port: &str) -> String {
    let (host, port) = match host_port.rsplit_once(':') {
        Some((h, p))
            if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) && !h.ends_with(':') =>
        {
            (h, p)
        }
        _ => (host_port, "443"),
    };
    let host = if host.is_empty() { "localhost" } else { host };
    format!("{host}:{port}")
}

/// Opens the transport for every connection the channel makes.
#[derive(Clone)]
struct Connector {
    target: Arc<Endpoint>,
    tls: Option<(Arc<rustls::ClientConfig>, ServerName<'static>)>,
}

type BoxIo = Box<dyn DriverIo>;

impl tower::Service<http::Uri> for Connector {
    type Response = TokioIo<BoxIo>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, io::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Uri) -> Self::Future {
        let (target, tls) = (self.target.clone(), self.tls.clone());
        Box::pin(async move {
            let raw: BoxIo = match target.as_ref() {
                #[cfg(unix)]
                Endpoint::Unix { path, .. } => {
                    Box::new(tokio::net::UnixStream::connect(path).await?)
                }
                #[cfg(not(unix))]
                Endpoint::Unix { .. } => {
                    return Err(io::Error::other(
                        "unix sockets are unsupported on this platform",
                    ))
                }
                other => {
                    let addr = dial_address(other.host_port().unwrap_or_default());
                    let tcp = tokio::net::TcpStream::connect(&addr).await?;
                    tcp.set_nodelay(true)?;
                    Box::new(tcp)
                }
            };
            let io: BoxIo = match tls {
                Some((cfg, name)) => Box::new(
                    tokio_rustls::TlsConnector::from(cfg)
                        .connect(name, raw)
                        .await?,
                ),
                None => raw,
            };
            Ok(TokioIo::new(io))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dial_address_fills_gaps() {
        assert_eq!(dial_address("127.0.0.1:50051"), "127.0.0.1:50051");
        assert_eq!(dial_address("127.0.0.1"), "127.0.0.1:443");
        assert_eq!(dial_address(":50051"), "localhost:50051");
        assert_eq!(dial_address("csi-service:9000"), "csi-service:9000");
    }

    #[tokio::test]
    async fn construction_never_dials() {
        // Nothing listens here; a lazy client is still built.
        let c = CsiClient::new("unix:///nonexistent/csi.sock", None).unwrap();
        assert_eq!(c.endpoint().scheme(), "unix");
        assert!(CsiClient::new("http://x", None).is_err());
    }
}

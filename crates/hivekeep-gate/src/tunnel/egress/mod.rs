//! The sandbox side of egress: every TCP connection an Actor opens (other
//! than DNS) is redirected here and carried to the egress gateway in its
//! own CONNECT tunnel, authenticated with a short-lived certificate for
//! the Actor that the keeper mints and this module renews.

pub mod client;
pub mod credential;
pub mod error;
pub mod h2_stream;
pub mod proxy;
pub mod stream_copy;
#[cfg(test)]
pub(crate) mod test_support;

use std::any::Any;
use std::io;
use std::time::SystemTime;

use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncWrite};

pub use client::{Client, ClientCertificateSource, ClientConfig};
pub use credential::{BrokerCertificateSource, BrokerConfig};
pub use error::TunnelError;
pub use proxy::{Accept, Binding, OriginalDestination, Proxy};

/// A bidirectional byte stream (TCP, TLS, an HTTP/2 tunnel, ...).
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send + Any {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + Any> Io for T {}

/// A boxed [`Io`].
pub type BoxIo = Box<dyn Io>;

/// Opens tunnels to destinations through the gateway.
pub trait TunnelDialer: Send + Sync + 'static {
    /// Opens a tunnel to `destination` (`<ip>:<port>`).
    fn dial<'a>(&'a self, destination: &'a str) -> BoxFuture<'a, Result<BoxIo, TunnelError>>;
    /// Releases idle resources; open tunnels finish on their own.
    fn close(&self);
}

/// A failed certificate mint, with the gRPC code when the keeper answered.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct MintError {
    /// The status code returned by the keeper, if any.
    pub code: Option<tonic::Code>,
    /// What went wrong.
    pub message: String,
}

impl MintError {
    /// An error without a status code.
    pub fn other(message: impl Into<String>) -> Self {
        MintError {
            code: None,
            message: message.into(),
        }
    }
}

impl From<tonic::Status> for MintError {
    fn from(s: tonic::Status) -> Self {
        MintError {
            code: Some(s.code()),
            message: format!("egress: mint actor certificate: {}", s.message()),
        }
    }
}

/// Renews the Actor certificate the tunnel presents.
pub trait ActorCertificateSource: Send + Sync + 'static {
    /// Mints a fresh certificate and returns when it expires.
    fn mint(&self) -> BoxFuture<'_, Result<SystemTime, MintError>>;
}

/// Parses a decimal port in 1..=65535.
pub fn parse_port(s: &str) -> Option<u16> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<u16>().ok().filter(|p| *p != 0)
}

/// The port of a listen address such as `0.0.0.0:15001`.
pub fn port_from_listen_addr(listen: &str) -> io::Result<u16> {
    let (_, port) = listen.rsplit_once(':').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("egress: listen address {listen:?}: missing port"),
        )
    })?;
    parse_port(port).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("egress: listen address {listen:?} has no usable port"),
        )
    })
}

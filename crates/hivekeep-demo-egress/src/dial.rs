//! Raw connections for the endpoints that drive the wire themselves.

use std::sync::Arc;

use rustls::ClientConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::tls;

/// A bidirectional byte stream, plain or TLS.
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// A boxed [`Io`].
pub type BoxedIo = Box<dyn Io>;

/// Dials `address`, then runs a TLS handshake as `server_name` when `tls`
/// is set. TLS errors are prefixed `TLS handshake`.
pub async fn open(
    address: &str,
    tls: Option<(&str, Arc<ClientConfig>)>,
) -> Result<BoxedIo, String> {
    let tcp = TcpStream::connect(address)
        .await
        .map_err(|e| format!("dial tcp {address}: {e}"))?;
    let _ = tcp.set_nodelay(true);
    match tls {
        None => Ok(Box::new(tcp)),
        Some((name, config)) => {
            let stream = tls::handshake(tcp, name, config)
                .await
                .map_err(|e| format!("TLS handshake: {e}"))?;
            Ok(Box::new(stream))
        }
    }
}

/// The text a timed-out operation reports.
pub const DEADLINE_EXCEEDED: &str = "context deadline exceeded";

//! The worker half of the relay.

use std::path::Path;

use tonic::transport::{Channel, Endpoint};

use super::config::validate_socket_path;
use super::server::RelayError;

/// Opens a channel to the relay socket, for the OTLP exporters (see
/// [`crate::boot::tracing::TracingOptions::exporter_channel`]).
///
/// Returns `None` when `sock_path` is empty or absent, which the caller
/// reads as "fall back to the OTLP endpoint". The existence check is what
/// makes the fallback deterministic at startup: the channel is lazy, so a
/// channel to a missing socket would be created happily and only fail
/// later, per export, with the telemetry already lost.
///
/// The connection is plaintext by design: a unix socket cannot leave the
/// node, and access is controlled by the socket's file mode.
#[cfg(unix)]
pub fn dial(sock_path: &Path) -> Result<Option<Channel>, RelayError> {
    if sock_path.as_os_str().is_empty() {
        return Ok(None);
    }
    validate_socket_path(sock_path)?;
    match std::fs::metadata(sock_path) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!(socket = %sock_path.display(), "OTLP relay socket absent, falling back to the OTLP endpoint, if one is set");
            return Ok(None);
        }
        Err(e) => {
            return Err(RelayError::Io(format!(
                "while checking the OTLP relay socket {}: {e}",
                sock_path.display()
            )))
        }
    }
    let path = sock_path.to_path_buf();
    // The URI is a placeholder; the connector dials the socket.
    let channel = Endpoint::from_static("http://otlp-relay.local").connect_with_connector_lazy(
        tower::service_fn(move |_: http::Uri| {
            let path = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }),
    );
    tracing::info!(socket = %sock_path.display(), "Exporting telemetry through the keeper OTLP relay");
    Ok(Some(channel))
}

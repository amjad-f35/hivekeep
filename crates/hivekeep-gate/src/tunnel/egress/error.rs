//! Errors of the egress tunnel client.

use std::io;

/// Why a tunnel could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum TunnelError {
    /// The destination is not `<ip literal>:<port>`.
    #[error("egress: invalid egress destination {destination:?}: {reason}")]
    InvalidDestination {
        /// The destination given.
        destination: String,
        /// What is wrong with it.
        reason: &'static str,
    },
    /// The gateway refused the connection before answering a CONNECT: the
    /// TLS handshake failed, or the gateway hung up or sent an alert right
    /// after it (as TLS 1.3 servers do when they reject a client
    /// certificate).
    #[error("egress: gateway TLS handshake{}: {source}", op.map(|o| format!(": {o}")).unwrap_or_default())]
    GatewayHandshake {
        /// The exchange step that failed, if past the handshake.
        op: Option<&'static str>,
        /// The underlying error.
        source: io::Error,
    },
    /// The gateway answered the CONNECT with a non-2xx status.
    #[error("egress: gateway rejected CONNECT with {status} {reason}: {message}")]
    ConnectRejected {
        /// The HTTP status code.
        status: u16,
        /// The status's reason phrase.
        reason: String,
        /// The response body (trimmed, at most 4 KiB) or the reason phrase.
        message: String,
    },
    /// Exchanging the CONNECT failed for another reason.
    #[error("egress: {op}: {source}")]
    Exchange {
        /// The step that failed.
        op: &'static str,
        /// The underlying error.
        source: io::Error,
    },
    /// Reaching the gateway failed.
    #[error("egress: connecting to egress gateway: {0}")]
    Connect(#[source] io::Error),
    /// The client was closed.
    #[error("egress: connecting to egress gateway: use of closed client")]
    Closed,
    /// The caller gave up (timeout or cancellation).
    #[error("egress: connecting to egress gateway: canceled")]
    Canceled,
}

impl TunnelError {
    /// Whether the gateway turned the client away at the TLS layer.
    pub fn is_gateway_handshake(&self) -> bool {
        matches!(self, TunnelError::GatewayHandshake { .. })
    }

    /// The status of a rejected CONNECT.
    pub fn rejected_status(&self) -> Option<u16> {
        match self {
            TunnelError::ConnectRejected { status, .. } => Some(*status),
            _ => None,
        }
    }
}

/// Whether `err` means the gateway hung up or alerted, rather than that a
/// local deadline passed or it answered with something other than HTTP.
pub fn gateway_hung_up(err: &io::Error) -> bool {
    if matches!(
        err.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
    ) {
        return true;
    }
    let alert = err
        .get_ref()
        .and_then(|e| e.downcast_ref::<rustls::Error>())
        .is_some_and(|e| matches!(e, rustls::Error::AlertReceived(_)));
    // Errors that crossed the HTTP/2 layer keep only their message.
    alert || err.to_string().starts_with("received fatal alert")
}

/// Classifies a failure while exchanging the CONNECT.
pub fn connect_exchange_error(op: &'static str, err: io::Error) -> TunnelError {
    if gateway_hung_up(&err) {
        TunnelError::GatewayHandshake {
            op: Some(op),
            source: err,
        }
    } else {
        TunnelError::Exchange { op, source: err }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_exchange_error_classification() {
        let cases: Vec<(&str, io::Error, bool)> = vec![
            (
                "peer TLS alert",
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    rustls::Error::AlertReceived(rustls::AlertDescription::UnknownCA),
                ),
                true,
            ),
            (
                "peer closed without alerting",
                io::ErrorKind::UnexpectedEof.into(),
                true,
            ),
            (
                "peer reset mid-response",
                io::ErrorKind::ConnectionReset.into(),
                true,
            ),
            (
                "peer reset while the CONNECT was going out",
                io::ErrorKind::BrokenPipe.into(),
                true,
            ),
            (
                "our own deadline expired",
                io::ErrorKind::TimedOut.into(),
                false,
            ),
            (
                "the gateway answered, but not with HTTP",
                io::Error::other("malformed HTTP response \"garbage\""),
                false,
            ),
        ];
        for (name, err, want) in cases {
            let kind = err.kind();
            let got = connect_exchange_error("reading CONNECT response", err);
            assert_eq!(got.is_gateway_handshake(), want, "{name}");
            let inner = match &got {
                TunnelError::GatewayHandshake { source, .. }
                | TunnelError::Exchange { source, .. } => source,
                other => panic!("{name}: {other:?}"),
            };
            assert_eq!(
                inner.kind(),
                kind,
                "{name}: still carries the underlying error"
            );
        }
    }
}

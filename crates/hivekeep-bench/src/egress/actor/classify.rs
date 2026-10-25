//! Naming a request's outcome in a few low-cardinality classes.

use std::io;

use rustls::{AlertDescription, CertificateError};

/// How a lookup failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DnsFailure {
    /// The name does not exist.
    NotFound,
    /// The resolver did not answer in time.
    Timeout,
    /// Anything else.
    Other,
}

/// Why one request failed.
#[derive(Debug)]
pub enum RequestError {
    /// The request outlived its timeout.
    Timeout,
    /// The endpoint name did not resolve.
    Dns(DnsFailure),
    /// The TLS handshake failed.
    Tls(rustls::Error),
    /// A socket operation (`dial`, `read`, `write`) failed.
    Io {
        /// The operation.
        op: &'static str,
        /// What failed.
        err: io::Error,
    },
    /// The peer closed the connection before the answer was complete.
    Eof,
    /// Anything else, by its message.
    Other(String),
}

impl std::fmt::Display for RequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestError::Timeout => f.write_str("timeout"),
            RequestError::Dns(d) => write!(f, "dns: {d:?}"),
            RequestError::Tls(e) => write!(f, "tls: {e}"),
            RequestError::Io { op, err } => write!(f, "{op}: {err}"),
            RequestError::Eof => f.write_str("EOF"),
            RequestError::Other(m) => f.write_str(m),
        }
    }
}

/// Bounds the message kept for an error [`classify`] does not recognize, so
/// one odd error cannot flood the report.
pub const MAX_OTHER_LEN: usize = 80;

/// Names the outcome of a request; `""` is a success (no error, HTTP 200).
pub fn classify(err: Option<&RequestError>, status: u16) -> String {
    let Some(err) = err else {
        if status == 200 {
            return String::new();
        }
        return format!("HTTP {status}");
    };
    match err {
        RequestError::Timeout => "timeout".into(),
        RequestError::Dns(DnsFailure::NotFound) => "dns: not found".into(),
        RequestError::Dns(DnsFailure::Timeout) => "dns: timeout".into(),
        RequestError::Dns(DnsFailure::Other) => "dns: error".into(),
        RequestError::Tls(e) => classify_tls(e),
        RequestError::Eof => "EOF".into(),
        RequestError::Io { op, err } => classify_io(op, err),
        RequestError::Other(msg) => {
            if msg.contains("server gave HTTP response to HTTPS client") {
                return "tls: not a TLS server".into();
            }
            format!("other: {}", cut(msg))
        }
    }
}

fn classify_io(op: &str, err: &io::Error) -> String {
    if let Some(tls) = err
        .get_ref()
        .and_then(|e| e.downcast_ref::<rustls::Error>())
    {
        return classify_tls(tls);
    }
    match err.kind() {
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => "timeout".into(),
        io::ErrorKind::ConnectionRefused => "connection refused".into(),
        io::ErrorKind::ConnectionReset => "connection reset".into(),
        io::ErrorKind::UnexpectedEof => "EOF".into(),
        _ => format!("net: {op}: {}", os_message(err)),
    }
}

/// An I/O error's message without the error number, lowercased the way the
/// C library spells it in lowercase-first form (`broken pipe`).
fn os_message(err: &io::Error) -> String {
    let msg = err.to_string();
    let msg = match msg.find(" (os error") {
        Some(i) => msg[..i].to_string(),
        None => msg,
    };
    let mut chars = msg.chars();
    match chars.next() {
        Some(c) => c.to_lowercase().collect::<String>() + chars.as_str(),
        None => msg,
    }
}

fn classify_tls(e: &rustls::Error) -> String {
    match e {
        rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer) => {
            "tls: unknown authority".into()
        }
        rustls::Error::InvalidCertificate(
            CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
        ) => "tls: hostname mismatch".into(),
        rustls::Error::InvalidCertificate(_) => "tls: invalid certificate".into(),
        // The first record did not look like TLS at all: a plain-text peer.
        rustls::Error::InvalidMessage(_) => "tls: not a TLS server".into(),
        rustls::Error::AlertReceived(a) => format!("tls: peer alert: {}", alert_name(*a)),
        other => format!("other: {}", cut(&other.to_string())),
    }
}

/// An alert's name in words (`handshake failure`).
fn alert_name(a: AlertDescription) -> String {
    match a {
        AlertDescription::UnknownCA => return "unknown certificate authority".into(),
        AlertDescription::ProtocolVersion => return "protocol version not supported".into(),
        AlertDescription::UnrecognisedName => return "unrecognized name".into(),
        _ => {}
    }
    let debug = format!("{a:?}");
    let mut out = String::new();
    for (i, c) in debug.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            out.push(' ');
        }
        out.extend(c.to_lowercase());
    }
    out
}

/// Cuts a message to [`MAX_OTHER_LEN`] bytes on a character boundary, so one
/// message always yields one class.
fn cut(msg: &str) -> &str {
    if msg.len() <= MAX_OTHER_LEN {
        return msg;
    }
    let mut end = MAX_OTHER_LEN;
    while !msg.is_char_boundary(end) {
        end -= 1;
    }
    &msg[..end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::InvalidMessage;

    fn io(op: &'static str, kind: io::ErrorKind) -> RequestError {
        RequestError::Io {
            op,
            err: io::Error::from(kind),
        }
    }

    #[test]
    fn classify_table() {
        let x = "x".repeat(MAX_OTHER_LEN + 20);
        let accented = "a".repeat(MAX_OTHER_LEN - 1) + "é!";
        let cases: Vec<(&str, Option<RequestError>, u16, String)> = vec![
            ("success", None, 200, String::new()),
            ("http status", None, 503, "HTTP 503".into()),
            ("deadline", Some(RequestError::Timeout), 0, "timeout".into()),
            (
                "net timeout",
                Some(io("dial", io::ErrorKind::TimedOut)),
                0,
                "timeout".into(),
            ),
            (
                "refused",
                Some(io("dial", io::ErrorKind::ConnectionRefused)),
                0,
                "connection refused".into(),
            ),
            (
                "reset",
                Some(io("read", io::ErrorKind::ConnectionReset)),
                0,
                "connection reset".into(),
            ),
            ("eof", Some(RequestError::Eof), 0, "EOF".into()),
            (
                "unexpected eof",
                Some(io("read", io::ErrorKind::UnexpectedEof)),
                0,
                "EOF".into(),
            ),
            (
                "dns not found",
                Some(RequestError::Dns(DnsFailure::NotFound)),
                0,
                "dns: not found".into(),
            ),
            (
                "dns other",
                Some(RequestError::Dns(DnsFailure::Other)),
                0,
                "dns: error".into(),
            ),
            (
                "dns timeout",
                Some(RequestError::Dns(DnsFailure::Timeout)),
                0,
                "dns: timeout".into(),
            ),
            (
                "long other is cut",
                Some(RequestError::Other(x)),
                0,
                format!("other: {}", "x".repeat(MAX_OTHER_LEN)),
            ),
            (
                "cut keeps whole characters",
                Some(RequestError::Other(accented)),
                0,
                format!("other: {}", "a".repeat(MAX_OTHER_LEN - 1)),
            ),
            (
                "other strips url",
                Some(RequestError::Other("boom".into())),
                0,
                "other: boom".into(),
            ),
            (
                "unknown authority",
                Some(RequestError::Tls(rustls::Error::InvalidCertificate(
                    CertificateError::UnknownIssuer,
                ))),
                0,
                "tls: unknown authority".into(),
            ),
            (
                "hostname mismatch",
                Some(RequestError::Tls(rustls::Error::InvalidCertificate(
                    CertificateError::NotValidForName,
                ))),
                0,
                "tls: hostname mismatch".into(),
            ),
            (
                "expired",
                Some(RequestError::Tls(rustls::Error::InvalidCertificate(
                    CertificateError::Expired,
                ))),
                0,
                "tls: invalid certificate".into(),
            ),
            (
                "not tls",
                Some(RequestError::Tls(rustls::Error::InvalidMessage(
                    InvalidMessage::InvalidContentType,
                ))),
                0,
                "tls: not a TLS server".into(),
            ),
            (
                "plain http answer",
                Some(RequestError::Other(
                    "http: server gave HTTP response to HTTPS client".into(),
                )),
                0,
                "tls: not a TLS server".into(),
            ),
            (
                "peer alert",
                Some(RequestError::Tls(rustls::Error::AlertReceived(
                    AlertDescription::HandshakeFailure,
                ))),
                0,
                "tls: peer alert: handshake failure".into(),
            ),
            (
                "net error without its addresses",
                Some(RequestError::Io {
                    op: "write",
                    err: io::Error::from(io::ErrorKind::BrokenPipe),
                }),
                0,
                "net: write: broken pipe".into(),
            ),
            (
                "tls error inside an io error",
                Some(RequestError::Io {
                    op: "read",
                    err: io::Error::new(
                        io::ErrorKind::InvalidData,
                        rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer),
                    ),
                }),
                0,
                "tls: unknown authority".into(),
            ),
        ];
        for (name, err, status, want) in cases {
            assert_eq!(classify(err.as_ref(), status), want, "{name}");
        }
    }
}

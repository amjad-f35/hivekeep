//! Parsing the OTLP collector address into the host and port of Envoy's
//! plaintext tracer cluster.

use super::consts::OTLP_DEFAULT_PORT;

/// An OTLP collector address Envoy cannot use.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct OtlpAddressError(String);

/// Splits `host:port`, accepting bracketed IPv6 hosts.
fn split_host_port(hostport: &str) -> Option<(String, String)> {
    if let Some(rest) = hostport.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        let port = after.strip_prefix(':')?;
        return Some((host.to_string(), port.to_string()));
    }
    let (host, port) = hostport.rsplit_once(':')?;
    if host.contains(':') {
        return None;
    }
    Some((host.to_string(), port.to_string()))
}

/// Normalizes a collector address given as `host[:port]` or an `http://`
/// URL into a host and port; the port defaults to 4317. `https` is refused
/// since the tracer cluster speaks plaintext h2c.
pub fn normalize_otlp_collector(addr: &str) -> Result<(String, u32), OtlpAddressError> {
    let mut hostport = addr.to_string();
    if let Some((scheme, _)) = addr.split_once("://") {
        match scheme {
            "http" => {}
            "https" => {
                return Err(OtlpAddressError(format!(
                    "OTLP collector endpoint {addr:?} uses https, which Envoy-side tracing does not support: the tracer cluster is plaintext h2c. Point --otlp-collector-address at an http:// endpoint, or pass it empty to disable Envoy-side tracing"
                )))
            }
            other => return Err(OtlpAddressError(format!("OTLP collector endpoint {addr:?} has unsupported scheme {other:?}, want http"))),
        }
        let rest = &addr[scheme.len() + 3..];
        let (authority, path) = match rest.find(['/', '?', '#']) {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        };
        let path = path.split(['?', '#']).next().unwrap_or_default();
        if !path.trim_matches('/').is_empty() {
            tracing::warn!(address = addr, path, "Ignoring the path of the OTLP collector endpoint: Envoy's gRPC tracer posts to the fixed OTLP service path");
        }
        hostport = authority
            .rsplit_once('@')
            .map_or(authority, |(_, h)| h)
            .to_string();
    }
    let (host, port) = split_host_port(&hostport).unwrap_or_else(|| {
        (
            hostport.trim_matches(['[', ']']).to_string(),
            OTLP_DEFAULT_PORT.to_string(),
        )
    });
    if host.is_empty() {
        return Err(OtlpAddressError(format!(
            "OTLP collector endpoint {addr:?} names no host"
        )));
    }
    let port: u32 = port
        .parse()
        .map_err(|e| OtlpAddressError(format!("parse OTLP collector port from {addr:?}: {e}")))?;
    Ok((host, port))
}

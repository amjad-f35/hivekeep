//! Where the guest process API is dialed: the gate router's address.

use crate::loadgen::client::default_router_url;

/// Turns the router's HTTPS URL into a `dns:///host:port` gRPC target
/// (port 443 when the URL has none). A bare `host[:port]` is read as
/// HTTPS; any other scheme, or no host, is an error.
pub fn router_grpc_target(raw: &str) -> Result<String, String> {
    let raw = if raw.is_empty() {
        default_router_url()
    } else {
        raw.to_string()
    };
    let with_scheme = if raw.contains("://") {
        raw.clone()
    } else {
        format!("https://{raw}")
    };
    let (scheme, rest) = with_scheme
        .split_once("://")
        .unwrap_or(("https", &with_scheme));
    if !scheme.eq_ignore_ascii_case("https") {
        return Err(format!("router URL {raw:?} uses {scheme}, want https"));
    }
    if rest.starts_with('/') || rest.is_empty() {
        return Err(format!("router URL {raw:?} has empty host"));
    }
    let u = url::Url::parse(&with_scheme).map_err(|e| format!("router URL {raw:?}: {e}"))?;
    let host = u
        .host_str()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| format!("router URL {raw:?} has empty host"))?;
    let port = u.port().unwrap_or(443);
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    Ok(format!("dns:///{host}:{port}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_grpc_target_table() {
        for (input, want) in [
            ("", "dns:///gate-router.hivekeep-system.svc:443"),
            (
                "https://gate-router.hivekeep-system.svc",
                "dns:///gate-router.hivekeep-system.svc:443",
            ),
            (
                "https://gate-router.hivekeep-system.svc.cluster.local",
                "dns:///gate-router.hivekeep-system.svc.cluster.local:443",
            ),
            (
                "https://gate-router.hivekeep-system.svc.cluster.local:8443",
                "dns:///gate-router.hivekeep-system.svc.cluster.local:8443",
            ),
            (
                "gate-router.hivekeep-system.svc",
                "dns:///gate-router.hivekeep-system.svc:443",
            ),
            (
                "router.example.internal:9000",
                "dns:///router.example.internal:9000",
            ),
        ] {
            assert_eq!(router_grpc_target(input).unwrap(), want, "{input:?}");
        }
        assert!(router_grpc_target("https:///missing-host").is_err());
        assert!(
            router_grpc_target("http://gate-router.hivekeep-system.svc.cluster.local").is_err()
        );
    }
}

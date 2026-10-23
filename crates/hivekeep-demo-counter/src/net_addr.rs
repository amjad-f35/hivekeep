//! The address a response reports as "where this ran".

use std::net::IpAddr;

/// Shown when the interface list cannot be read at all.
pub const UNKNOWN_IP: &str = "x.x.x.x";
/// Shown when there is no non-loopback IPv4 address.
pub const NO_IPV4: &str = "y.y.y.y";

/// The first non-loopback IPv4 address of this host, so a response reveals
/// which worker the Actor is currently running on.
pub fn current_ip() -> String {
    match nix::ifaddrs::getifaddrs() {
        Ok(addrs) => {
            let ips = addrs.filter_map(|a| {
                a.address
                    .and_then(|s| s.as_sockaddr_in().map(|v4| IpAddr::V4(v4.ip())))
            });
            pick(ips)
        }
        Err(e) => {
            tracing::error!(err = %e, "Error getting interface addresses");
            UNKNOWN_IP.to_string()
        }
    }
}

/// Picks the first non-loopback IPv4 address from `ips`.
pub fn pick(ips: impl IntoIterator<Item = IpAddr>) -> String {
    ips.into_iter()
        .find(|ip| ip.is_ipv4() && !ip.is_loopback())
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| NO_IPV4.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_loopback_and_ipv6() {
        let ips: Vec<IpAddr> = vec![
            "127.0.0.1".parse().unwrap(),
            "fe80::1".parse().unwrap(),
            "10.1.2.3".parse().unwrap(),
        ];
        assert_eq!(pick(ips), "10.1.2.3");
    }

    #[test]
    fn none_found() {
        let ips: Vec<IpAddr> = vec!["127.0.0.1".parse().unwrap()];
        assert_eq!(pick(ips), NO_IPV4);
    }

    #[test]
    fn current_ip_is_never_empty() {
        assert!(!current_ip().is_empty());
    }
}

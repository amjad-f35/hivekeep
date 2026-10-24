//! The address a response reports as "where this ran".

use std::net::IpAddr;

/// The first non-loopback IPv4 address of this host; `x.x.x.x` when the
/// interface list is unreadable, `y.y.y.y` when there is none.
pub fn current_ip() -> String {
    match nix::ifaddrs::getifaddrs() {
        Ok(addrs) => addrs
            .filter_map(|a| {
                a.address
                    .and_then(|s| s.as_sockaddr_in().map(|v4| IpAddr::V4(v4.ip())))
            })
            .find(|ip| !ip.is_loopback())
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "y.y.y.y".to_string()),
        Err(e) => {
            tracing::error!(err = %e, "Error getting interface addresses");
            "x.x.x.x".to_string()
        }
    }
}

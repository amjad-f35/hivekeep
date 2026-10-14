//! Destinations parsed from an `:authority` or `Host` value.

use std::net::IpAddr;

use hivekeep_core::hostname::normalize_hostname;

/// Where a request is going, as far as the leg evaluating it can tell.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Destination {
    /// The normalized DNS name (lowercase ASCII, no trailing dot); empty
    /// when the destination was not named by a hostname.
    pub hostname: String,
    /// The address named instead of a hostname. Only the `*` pattern
    /// matches it.
    pub ip: Option<IpAddr>,
    /// The port the Actor dialed, or 0 when the leg does not know it, in
    /// which case rule ports are not enforced. A port written in a request's
    /// authority is not this.
    pub port: u16,
}

impl Destination {
    /// A destination named by `hostname` on an unknown port.
    pub fn host(hostname: impl Into<String>) -> Self {
        Destination {
            hostname: hostname.into(),
            ..Default::default()
        }
    }

    /// A destination named by an address on an unknown port.
    pub fn addr(ip: IpAddr) -> Self {
        Destination {
            ip: Some(ip),
            ..Default::default()
        }
    }

    /// The same destination on `port`.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
}

/// An authority that cannot be turned into a [`Destination`]; callers deny.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidAuthority {
    /// The authority is empty.
    #[error("authority is empty")]
    Empty,
    /// The port is not a number in 1..=65535.
    #[error("authority {0:?} has an invalid port")]
    Port(String),
    /// An IPv6 literal with a zone.
    #[error("authority {0:?} has an IPv6 zone")]
    Zone(String),
    /// Neither a hostname nor an address.
    #[error("authority {0:?} is neither a DNS hostname nor an IP literal")]
    Host(String),
}

/// Splits `host:port` or `[host]:port`, failing like a strict splitter
/// would on a missing port or extra colons.
fn split_host_port(authority: &str) -> Option<(&str, &str)> {
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        let port = after.strip_prefix(':')?;
        if host.contains('[') || host.contains(']') || port.contains(['[', ']']) {
            return None;
        }
        return Some((host, port));
    }
    let (host, port) = authority.rsplit_once(':')?;
    if host.contains(':') || host.contains(['[', ']']) || port.contains(['[', ']']) {
        return None;
    }
    Some((host, port))
}

/// Parses a port written in decimal digits only; 0 is invalid.
fn parse_port(raw: &str) -> Option<u16> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    raw.parse::<u16>().ok().filter(|p| *p != 0)
}

/// Turns an authority into a [`Destination`]: port split off, IP literal to
/// an address (IPv4-mapped IPv6 unmapped), DNS name lowercased with one
/// trailing dot removed and checked as a DNS-1123 subdomain.
pub fn normalize_authority(authority: &str) -> Result<Destination, InvalidAuthority> {
    if authority.is_empty() {
        return Err(InvalidAuthority::Empty);
    }
    let mut host = authority;
    let mut port = 0;
    if let Some((h, p)) = split_host_port(authority) {
        port = parse_port(p).ok_or_else(|| InvalidAuthority::Port(authority.to_string()))?;
        host = h;
    } else if let Some(inner) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        host = inner;
    }
    if host.contains('%') && host.contains(':') {
        return Err(InvalidAuthority::Zone(authority.to_string()));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        let ip = match ip {
            IpAddr::V6(v6) => v6
                .to_ipv4_mapped()
                .map(IpAddr::V4)
                .unwrap_or(IpAddr::V6(v6)),
            v4 => v4,
        };
        return Ok(Destination {
            hostname: String::new(),
            ip: Some(ip),
            port,
        });
    }
    let hostname =
        normalize_hostname(host).ok_or_else(|| InvalidAuthority::Host(authority.to_string()))?;
    Ok(Destination {
        hostname,
        ip: None,
        port,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn normalize_authority_cases() {
        let ok = [
            ("example.com", Destination::host("example.com")),
            (
                "example.com:8443",
                Destination::host("example.com").with_port(8443),
            ),
            ("API.Example.COM", Destination::host("api.example.com")),
            ("example.com.", Destination::host("example.com")),
            (
                "example.com.:443",
                Destination::host("example.com").with_port(443),
            ),
            ("192.0.2.1", Destination::addr(ip("192.0.2.1"))),
            (
                "192.0.2.1:80",
                Destination::addr(ip("192.0.2.1")).with_port(80),
            ),
            (
                "[2001:db8::1]:443",
                Destination::addr(ip("2001:db8::1")).with_port(443),
            ),
            ("[2001:db8::1]", Destination::addr(ip("2001:db8::1"))),
            ("2001:db8::1", Destination::addr(ip("2001:db8::1"))),
            (
                "[::ffff:192.0.2.1]:80",
                Destination::addr(ip("192.0.2.1")).with_port(80),
            ),
        ];
        for (authority, want) in ok {
            assert_eq!(normalize_authority(authority), Ok(want), "{authority:?}");
        }
        let bad = [
            "",
            "example.com..",
            "example.com:0",
            "example.com:99999",
            "example.com:https",
            "[fe80::1%25eth0]:443",
            "bücher.example",
            "exa mple.com",
            "http://example.com",
            "example.com/path",
            "_dmarc.example.com",
        ];
        for authority in bad {
            assert!(
                normalize_authority(authority).is_err(),
                "{authority:?} should fail"
            );
        }
    }
}

//! Dialing from inside a namespace: the socket is created in the
//! namespace, then connected from the async runtime.

use std::io;
use std::net::{IpAddr, SocketAddr};

use tokio::net::{TcpStream, UdpSocket};
use tokio_util::sync::CancellationToken;

use super::NetNs;

/// Transport of a namespace dial.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Network {
    /// TCP over either family.
    Tcp,
    /// TCP over IPv4.
    Tcp4,
    /// TCP over IPv6.
    Tcp6,
    /// UDP over either family.
    Udp,
    /// UDP over IPv4.
    Udp4,
    /// UDP over IPv6.
    Udp6,
}

impl Network {
    /// Parses `tcp`, `tcp4`, `tcp6`, `udp`, `udp4` or `udp6`.
    pub fn parse(s: &str) -> io::Result<Network> {
        Ok(match s {
            "tcp" => Network::Tcp,
            "tcp4" => Network::Tcp4,
            "tcp6" => Network::Tcp6,
            "udp" => Network::Udp,
            "udp4" => Network::Udp4,
            "udp6" => Network::Udp6,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("unknown network {other}"),
                ))
            }
        })
    }

    fn is_udp(self) -> bool {
        matches!(self, Network::Udp | Network::Udp4 | Network::Udp6)
    }
}

/// A validated dial target: an IP literal, optional IPv6 zone and a port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DialTarget {
    /// The transport.
    pub network: Network,
    /// Address and port; the zone is resolved at dial time.
    pub addr: SocketAddr,
    /// The IPv6 zone (interface name or index), if any.
    pub zone: Option<String>,
}

/// Checks that `addr` is `<ip literal>:<port>` and `network` is supported.
/// Hostnames are rejected: resolving them would use the caller's
/// namespace's resolver, not the target's.
pub fn validate_dial_target(network: &str, addr: &str) -> io::Result<DialTarget> {
    let network = Network::parse(network)?;
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidInput, msg);
    let (host, port) = if let Some(rest) = addr.strip_prefix('[') {
        let (host, after) = rest
            .split_once(']')
            .ok_or_else(|| invalid(format!("address {addr}: missing ']'")))?;
        let port = after
            .strip_prefix(':')
            .ok_or_else(|| invalid(format!("address {addr}: missing port")))?;
        (host, port)
    } else {
        let (host, port) = addr
            .rsplit_once(':')
            .ok_or_else(|| invalid(format!("address {addr}: missing port")))?;
        if host.contains(':') {
            return Err(invalid(format!("address {addr}: too many colons")));
        }
        (host, port)
    };
    let (ip, zone) = match host.split_once('%') {
        Some((ip, zone)) if !zone.is_empty() => (ip, Some(zone.to_string())),
        Some(_) => return Err(invalid(format!("address {addr}: empty zone"))),
        None => (host, None),
    };
    let ip: IpAddr = ip.parse().map_err(|_| {
        invalid(format!(
            "namespace dials support only IP literals (got {host:?})"
        ))
    })?;
    if zone.is_some() && !ip.is_ipv6() {
        return Err(invalid(format!("address {addr}: zone on an IPv4 address")));
    }
    let port: u16 = port
        .parse()
        .map_err(|_| invalid(format!("address {addr}: invalid port")))?;
    Ok(DialTarget {
        network,
        addr: SocketAddr::new(ip, port),
        zone,
    })
}

/// A connection opened inside a namespace.
#[derive(Debug)]
pub enum NsConn {
    /// A connected TCP stream.
    Tcp(TcpStream),
    /// A connected UDP socket.
    Udp(UdpSocket),
}

/// Dials `addr` over `network` from inside `ns`.
pub async fn dial(ns: &NetNs, network: &str, addr: &str) -> io::Result<NsConn> {
    dial_cancellable(ns, network, addr, &CancellationToken::new()).await
}

/// Like [`dial`], abandoning the dial when `cancel` fires (or has already).
pub async fn dial_cancellable(
    ns: &NetNs,
    network: &str,
    addr: &str,
    cancel: &CancellationToken,
) -> io::Result<NsConn> {
    let canceled = || io::Error::new(io::ErrorKind::Interrupted, "dial canceled");
    if cancel.is_cancelled() {
        return Err(canceled());
    }
    let target = validate_dial_target(network, addr)?;
    let socket = ns.run(|| make_socket(&target))?;
    let connect = async {
        let mut addr = target.addr;
        if let (SocketAddr::V6(v6), Some(zone)) = (&mut addr, &target.zone) {
            v6.set_scope_id(zone_index(ns, zone)?);
        }
        if target.network.is_udp() {
            let udp = UdpSocket::from_std(socket.into())?;
            udp.connect(addr).await?;
            Ok(NsConn::Udp(udp))
        } else {
            let tcp = tokio::net::TcpSocket::from_std_stream(socket.into());
            Ok(NsConn::Tcp(tcp.connect(addr).await?))
        }
    };
    tokio::select! {
        r = connect => r,
        _ = cancel.cancelled() => Err(canceled()),
    }
}

fn make_socket(target: &DialTarget) -> io::Result<socket2::Socket> {
    use socket2::{Domain, Protocol, Socket, Type};
    let domain = Domain::for_address(target.addr);
    let socket = if target.network.is_udp() {
        Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?
    } else {
        Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?
    };
    socket.set_nonblocking(true)?;
    Ok(socket)
}

/// Resolves an IPv6 zone inside the namespace: numeric, or an interface name.
fn zone_index(ns: &NetNs, zone: &str) -> io::Result<u32> {
    if let Ok(n) = zone.parse() {
        return Ok(n);
    }
    let name = std::ffi::CString::new(zone)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid zone"))?;
    ns.run(move || {
        // SAFETY: `name` is a valid NUL-terminated string.
        match unsafe { libc::if_nametoindex(name.as_ptr()) } {
            0 => Err(io::Error::last_os_error()),
            n => Ok(n),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_dial_target_cases() {
        let cases = [
            ("tcp", "127.0.0.1:80", false),
            ("tcp4", "127.0.0.1:80", false),
            ("tcp6", "[::1]:80", false),
            ("udp", "127.0.0.1:53", false),
            ("udp4", "127.0.0.1:53", false),
            ("udp6", "[fe80::1%eth0]:53", false),
            ("tcp", "localhost:80", true),
            ("tcp", ":80", true),
            ("tcp", "127.0.0.1", true),
            ("unix", "/tmp/socket", true),
            ("ip", "127.0.0.1:80", true),
        ];
        for (network, addr, want_err) in cases {
            assert_eq!(
                validate_dial_target(network, addr).is_err(),
                want_err,
                "{network}/{addr}"
            );
        }
        assert_eq!(
            validate_dial_target("unix", "/tmp/socket")
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn dialer_rejects_non_ip_targets() {
        let ns = NetNs::current().unwrap();
        for (network, addr) in [
            ("tcp", "localhost:80"),
            ("tcp", ":80"),
            ("tcp", "127.0.0.1"),
            ("unix", "/tmp/socket"),
        ] {
            assert!(dial(&ns, network, addr).await.is_err(), "{network} {addr}");
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn dialer_canceled_context() {
        let ns = NetNs::current().unwrap();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let err = dial_cancellable(&ns, "tcp", "127.0.0.1:1", &cancel)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Interrupted);
    }
}

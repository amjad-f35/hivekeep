//! The relay configuration shared by every Actor of a worker: which
//! resolvers to forward to.

use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;
use std::sync::Arc;

use tokio::net::{TcpListener, UdpSocket};

use super::policy::PolicySource;
use super::server::{Server, ServerSockets};
use super::DNS_PORT;
use crate::netns::NetNs;

/// `addr` with an IPv4-mapped address unmapped, as `ip:port`.
pub fn normalize_addr(addr: &SocketAddr) -> String {
    match addr {
        SocketAddr::V6(v6) => match v6.ip().to_ipv4_mapped() {
            Some(v4) => SocketAddr::new(v4.into(), v6.port()).to_string(),
            None => SocketAddr::new((*v6.ip()).into(), v6.port()).to_string(),
        },
        v4 => v4.to_string(),
    }
}

/// Like [`normalize_addr`] for a string; non-addresses are returned as is.
pub fn normalize_addr_str(s: &str) -> String {
    s.parse::<SocketAddr>()
        .map(|a| normalize_addr(&a))
        .unwrap_or_else(|_| s.to_string())
}

/// Resolves an upstream `host:port`.
pub(crate) fn resolve(u: &str) -> io::Result<SocketAddr> {
    if let Ok(a) = u.parse() {
        return Ok(a);
    }
    u.to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("no address for {u}")))
}

/// Forwards Actor DNS queries allowed by their egress policy to the
/// worker's resolvers.
#[derive(Debug, Clone)]
pub struct Relay {
    upstreams: Vec<String>,
}

impl Relay {
    /// A relay forwarding to the nameservers of `resolv_conf`.
    pub fn new(resolv_conf: &Path) -> io::Result<Relay> {
        Relay::for_upstreams(super::resolv_conf::nameservers(resolv_conf)?)
    }

    /// A relay forwarding to `upstreams` (`host:port`), tried in order.
    pub fn for_upstreams(upstreams: Vec<String>) -> io::Result<Relay> {
        if upstreams.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dns: at least one upstream resolver is required",
            ));
        }
        for u in &upstreams {
            if u.rsplit_once(':').is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("dns: invalid upstream resolver {u:?}: missing port"),
                ));
            }
            resolve(u).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("dns: invalid upstream resolver {u:?}: {e}"),
                )
            })?;
        }
        tracing::info!(?upstreams, "DNS relay configured");
        Ok(Relay { upstreams })
    }

    /// The upstream resolvers.
    pub fn upstreams(&self) -> &[String] {
        &self.upstreams
    }

    /// Serves DNS on port 53 of the Actor network namespace `ns`, deciding
    /// queries with `policy`; upstream traffic leaves from the caller's
    /// namespace.
    pub async fn serve(
        &self,
        ns: &NetNs,
        policy: Option<Arc<dyn PolicySource>>,
    ) -> io::Result<Server> {
        let (udp, tcp) = ns.run(|| {
            let addr = SocketAddr::from(([0, 0, 0, 0], DNS_PORT));
            let udp = std::net::UdpSocket::bind(addr).map_err(|e| {
                io::Error::new(e.kind(), format!("while opening the Actor DNS socket: {e}"))
            })?;
            let tcp = std::net::TcpListener::bind(addr).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("while opening the Actor DNS listener: {e}"),
                )
            })?;
            udp.set_nonblocking(true)?;
            tcp.set_nonblocking(true)?;
            Ok((udp, tcp))
        })?;
        let egress = UdpSocket::bind("0.0.0.0:0").await.map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("while opening the worker DNS egress socket: {e}"),
            )
        })?;
        Ok(self.serve_on(
            ServerSockets {
                udp: UdpSocket::from_std(udp)?,
                tcp: TcpListener::from_std(tcp)?,
                egress_udp: egress,
            },
            policy,
        ))
    }

    /// Serves on sockets the caller opened.
    pub fn serve_on(
        &self,
        sockets: ServerSockets,
        policy: Option<Arc<dyn PolicySource>>,
    ) -> Server {
        Server::start(&self.upstreams, policy, sockets)
    }
}

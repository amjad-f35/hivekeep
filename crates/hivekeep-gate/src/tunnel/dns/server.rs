//! A running relay for one Actor.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, UdpSocket};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::handler::DnsHandler;
use super::limiter::Limiter;
use super::pending::PendingRequests;
use super::policy::{EgressPolicyCheck, PolicySource};
use super::tcp::TcpHandler;
use super::udp::UdpHandler;

/// The sockets one relay serves on.
pub struct ServerSockets {
    /// The Actor-facing UDP socket.
    pub udp: UdpSocket,
    /// The Actor-facing TCP listener.
    pub tcp: TcpListener,
    /// The socket used to reach upstream resolvers over UDP.
    pub egress_udp: UdpSocket,
}

/// A relay serving one Actor until stopped.
pub struct Server {
    limiter: Arc<Limiter>,
    cancel: CancellationToken,
    tracker: TaskTracker,
}

impl Server {
    pub(crate) fn start(
        upstreams: &[String],
        policy: Option<Arc<dyn PolicySource>>,
        sockets: ServerSockets,
    ) -> Server {
        let cancel = CancellationToken::new();
        let limiter = Arc::new(Limiter::new());
        let pending = Arc::new(PendingRequests::new(limiter.clone()));
        let check = Arc::new(EgressPolicyCheck::new(policy, cancel.clone()));
        let dns = Arc::new(DnsHandler::new(
            pending.clone(),
            limiter.clone(),
            Some(check),
        ));
        let udp_upstreams: Vec<SocketAddr> = upstreams
            .iter()
            .filter_map(|u| super::relay::resolve(u).ok())
            .collect();
        let udp = Arc::new(UdpHandler::new(
            Arc::new(sockets.udp),
            Arc::new(sockets.egress_udp),
            udp_upstreams,
            dns.clone(),
            pending,
        ));
        let tcp = Arc::new(TcpHandler::new(
            upstreams.to_vec(),
            sockets.tcp,
            dns,
            limiter.clone(),
        ));
        let tracker = TaskTracker::new();
        tracker.spawn(udp.serve(cancel.clone()));
        tracker.spawn(tcp.serve(cancel.clone()));
        tracker.close();
        Server {
            limiter,
            cancel,
            tracker,
        }
    }

    /// The relay's limits, for introspection.
    pub fn limiter(&self) -> &Limiter {
        &self.limiter
    }

    /// Stops serving: closes the sockets, abandons queries in flight
    /// (including policy fetches) and closes every TCP connection, then
    /// waits for all of it to finish. Calling it again is harmless.
    pub async fn stop(&self) {
        self.cancel.cancel();
        self.tracker.wait().await;
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

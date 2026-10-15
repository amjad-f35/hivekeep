//! A served sandbox network: an Actor's namespaces plus the DNS relay and
//! egress listeners in its gateway namespace, torn down together.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use hivekeep_gate::netns::{self, NetNs, NsConn};
use hivekeep_gate::tunnel::dns::{PolicySource, Relay, Server as DnsServer};
use hivekeep_gate::tunnel::egress::Proxy;
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::setup::{
    cleanup_sandbox_network, setup_sandbox_network, SandboxNetwork, SandboxNetworkConfig,
};

/// Serves one listener's captured connections until cancelled.
pub type ServeFn =
    Box<dyn FnOnce(TcpListener, CancellationToken) -> BoxFuture<'static, io::Result<()>> + Send>;

/// Whatever serves an Actor's redirected TCP: the tunnel's egress proxy,
/// or a test double.
pub trait EgressServer: Send + Sync {
    /// Attaches to the Actor before anything is served, so a connection
    /// accepted here cannot pick up a later activation's credentials.
    fn bind(&self, actor_uid: &str) -> io::Result<ServeFn>;
}

impl EgressServer for Proxy {
    fn bind(&self, actor_uid: &str) -> io::Result<ServeFn> {
        let binding = Proxy::bind(self, actor_uid)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        Ok(Box::new(move |listener, cancel| {
            Box::pin(binding.serve(listener, cancel))
        }))
    }
}

/// What a sandbox session serves besides its namespaces. Each part left
/// out stays unserved, which fails closed.
#[derive(Default, Clone)]
pub struct SessionServices {
    /// Serves redirected TCP.
    pub egress: Option<Arc<dyn EgressServer>>,
    /// Answers DNS on the gateway address.
    pub dns_relay: Option<Arc<Relay>>,
    /// The policy DNS queries are checked against; `None` refuses every
    /// query.
    pub dns_policy: Option<Arc<dyn PolicySource>>,
}

#[derive(Default)]
struct State {
    network: Option<SandboxNetwork>,
    dns: Option<DnsServer>,
}

/// An Actor's network and the sockets serving it.
pub struct SandboxSession {
    state: Mutex<State>,
    cancel: CancellationToken,
    serving: TaskTracker,
}

impl std::fmt::Debug for SandboxSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SandboxSession")
            .field("open", &self.state.lock().network.is_some())
            .finish_non_exhaustive()
    }
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "the sandbox network is closed")
}

impl SandboxSession {
    /// A session holding nothing, as one is after [`SandboxSession::close`].
    pub fn closed() -> SandboxSession {
        SandboxSession {
            state: Mutex::new(State::default()),
            cancel: CancellationToken::new(),
            serving: TaskTracker::new(),
        }
    }

    /// Builds the Actor's network and serves DNS and egress from its
    /// gateway namespace.
    pub async fn serve(
        cfg: SandboxNetworkConfig,
        services: SessionServices,
    ) -> io::Result<Arc<SandboxSession>> {
        let setup_cfg = cfg.clone();
        let network = tokio::task::spawn_blocking(move || setup_sandbox_network(&setup_cfg))
            .await
            .map_err(io::Error::other)??;
        let session = Arc::new(SandboxSession::closed());
        session.state.lock().network = Some(network);
        if let Err(e) = session.start(&cfg, services).await {
            if let Err(close) = session.close(None).await {
                tracing::warn!(err = %close, "Failed to remove a half-built sandbox network");
            }
            return Err(e);
        }
        Ok(session)
    }

    /// Another handle to the gateway namespace, or an error once closed.
    fn gateway(&self) -> io::Result<NetNs> {
        let st = self.state.lock();
        let n = st.network.as_ref().ok_or_else(closed)?;
        n.gateway_netns().try_clone().map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("while retaining the sandbox namespace: {e}"),
            )
        })
    }

    async fn start(&self, cfg: &SandboxNetworkConfig, services: SessionServices) -> io::Result<()> {
        let gateway = self.gateway()?;
        if let Some(relay) = &services.dns_relay {
            let server = relay.serve(&gateway, services.dns_policy.clone()).await?;
            self.state.lock().dns = Some(server);
        }
        // Egress last: once bound, nothing may fail before serving starts.
        if let Some(egress) = &services.egress {
            let listeners = netns::listen(&gateway, &[cfg.egress_port]).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("while opening actor egress listeners: {e}"),
                )
            })?;
            let listeners = listeners
                .into_iter()
                .map(|l| {
                    l.set_nonblocking(true)?;
                    TcpListener::from_std(l)
                })
                .collect::<io::Result<Vec<_>>>()?;
            for listener in listeners {
                let serve = egress.bind(&cfg.actor_uid)?;
                let cancel = self.cancel.child_token();
                self.serving.spawn(async move {
                    if let Err(err) = serve(listener, cancel).await {
                        tracing::warn!(%err, "Sandbox egress listener stopped");
                    }
                });
            }
        }
        Ok(())
    }

    /// Whether the network is still up.
    pub fn is_open(&self) -> bool {
        self.state.lock().network.is_some()
    }

    /// Dials `addr` over `network` from the gateway namespace, the only
    /// place the sandbox address is routable.
    pub async fn dial(&self, network: &str, addr: &str) -> io::Result<NsConn> {
        let gateway = self.gateway()?;
        netns::dial(&gateway, network, addr).await
    }

    /// Stops serving, waits for the serving tasks (at most `wait`, when
    /// given), then removes the namespaces. The namespaces go even when
    /// the wait runs out, since leaving them would wedge the Actor's next
    /// activation. Repeated calls are fine; every step's error is returned.
    pub async fn close(&self, wait: Option<Duration>) -> io::Result<()> {
        let (dns, network) = {
            let mut st = self.state.lock();
            (st.dns.take(), st.network.take())
        };
        self.cancel.cancel();
        self.serving.close();
        let mut errs = Vec::new();
        if let Some(dns) = dns {
            dns.stop().await;
        }
        match wait {
            Some(d) => {
                if tokio::time::timeout(d, self.serving.wait()).await.is_err() {
                    errs.push(format!(
                        "while waiting for the sandbox's serving tasks: deadline exceeded after {d:?}"
                    ));
                }
            }
            None => self.serving.wait().await,
        }
        if let Some(network) = network {
            let removed = tokio::task::spawn_blocking(move || cleanup_sandbox_network(network))
                .await
                .map_err(io::Error::other)
                .and_then(|r| r);
            if let Err(e) = removed {
                errs.push(e.to_string());
            }
        }
        if errs.is_empty() {
            Ok(())
        } else {
            let kind = if errs.iter().any(|e| e.contains("deadline exceeded")) {
                io::ErrorKind::TimedOut
            } else {
                io::ErrorKind::Other
            };
            Err(io::Error::new(kind, errs.join("; ")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dialing_after_close_reports_closed() {
        let session = SandboxSession::closed();
        session.close(None).await.unwrap();
        let err = session.dial("tcp", "127.0.0.1:1").await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotConnected);
        assert!(!session.is_open());
        // Closing again is fine.
        session.close(Some(Duration::from_millis(1))).await.unwrap();
    }
}

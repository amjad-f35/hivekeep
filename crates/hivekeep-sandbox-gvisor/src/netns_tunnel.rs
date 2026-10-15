//! The worker's [`Tunnel`] seam backed by per-Actor network namespaces and
//! the gate's tunnel: egress is minted and activated through the tunnel,
//! each Actor's namespaces are served by an [`actor_net`] session, and
//! ingress is forwarded into the Actor's gateway namespace.
//!
//! Without a gate tunnel (a worker run without the node's identity
//! bundles) Actors still get their own namespaces, but egress and DNS stay
//! unserved, so they fail closed.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hivekeep_core::resource::ActorAttribution;
use hivekeep_gate::netns::NsConn;
use hivekeep_gate::tunnel::egress::BoxIo;
use hivekeep_gate::tunnel::ingress::Dial;
use hivekeep_gate::tunnel::{ActorEgress, Tunnel as GateTunnel};
use hivekeep_proto::worker::v1 as pb;
use parking_lot::Mutex;

use crate::actor_net::{SandboxNetworkConfig, SandboxSession, SessionServices};
use crate::error::{Error, Result};
use crate::ports::{Dialer, Egress, SandboxNetwork, Tunnel};

/// How long closing a sandbox network waits for its serving tasks.
pub const CLOSE_WAIT: Duration = Duration::from_secs(60);

/// The egress port used when no gate tunnel runs; nothing listens on it.
const UNSERVED_EGRESS_PORT: u16 = 15001;

/// The namespace-backed tunnel seam.
pub struct NetnsTunnel {
    gate: Option<Arc<GateTunnel>>,
    veth: bool,
    /// Egress prepared by `prepare_egress`, waiting for `activate_egress`.
    prepared: Mutex<HashMap<String, ActorEgress>>,
}

impl std::fmt::Debug for NetnsTunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetnsTunnel")
            .field("gate", &self.gate.is_some())
            .field("veth", &self.veth)
            .finish_non_exhaustive()
    }
}

impl NetnsTunnel {
    /// A seam over `gate`. `veth` gives each Actor a separate runtime
    /// namespace joined by a veth pair (gVisor); without it the runtime and
    /// the gateway share one namespace and the runtime adds its own link
    /// (a micro-VM tap).
    pub fn new(gate: Option<Arc<GateTunnel>>, veth: bool) -> NetnsTunnel {
        NetnsTunnel {
            gate,
            veth,
            prepared: Mutex::new(HashMap::new()),
        }
    }
}

/// A served sandbox network, as the worker sees it.
#[derive(Debug)]
pub struct SessionNetwork(pub Arc<SandboxSession>);

#[async_trait]
impl SandboxNetwork for SessionNetwork {
    fn dialer(&self) -> Dialer {
        let session = self.0.clone();
        Arc::new(move |addr| {
            let session = session.clone();
            Box::pin(async move {
                match session.dial("tcp", &addr).await? {
                    NsConn::Tcp(s) => Ok(s),
                    NsConn::Udp(_) => Err(io::Error::other("dialed UDP for a TCP connection")),
                }
            })
        })
    }

    async fn close(&self) -> Result<()> {
        self.0
            .close(Some(CLOSE_WAIT))
            .await
            .map_err(|e| Error::io("while removing the sandbox network", e))
    }
}

/// The worker's dialer as the gate's ingress dial.
pub fn ingress_dial(dialer: Dialer) -> Dial {
    Arc::new(move |addr| {
        let fut = dialer(addr);
        Box::pin(async move { fut.await.map(|s| Box::new(s) as BoxIo) })
    })
}

#[async_trait]
impl Tunnel for NetnsTunnel {
    async fn deactivate(&self, a: &ActorAttribution) -> Result<()> {
        self.prepared.lock().remove(&a.uid);
        if let Some(gate) = &self.gate {
            gate.deactivate(a).await;
        }
        Ok(())
    }

    async fn prepare_egress(
        &self,
        a: &ActorAttribution,
        gateway: Option<&pb::EgressGateway>,
    ) -> Result<Egress> {
        let out = Egress {
            gateway: gateway.map(|g| g.address.clone()),
        };
        let Some(gate) = &self.gate else {
            return Ok(out);
        };
        match gate.prepare_egress(a, gateway).await.map_err(Error::msg)? {
            Some(prepared) => {
                self.prepared.lock().insert(a.uid.clone(), prepared);
            }
            None => {
                self.prepared.lock().remove(&a.uid);
            }
        }
        Ok(out)
    }

    fn activate_egress(&self, a: &ActorAttribution, _: Egress) -> Result<()> {
        let prepared = self.prepared.lock().remove(&a.uid);
        match &self.gate {
            Some(gate) => gate.activate_egress(a, prepared).map_err(Error::msg),
            None => Ok(()),
        }
    }

    fn activate_ingress(&self, a: &ActorAttribution, dialer: Dialer) -> Result<()> {
        match &self.gate {
            Some(gate) => gate
                .activate_ingress(a, ingress_dial(dialer))
                .map_err(Error::msg),
            None => Ok(()),
        }
    }

    async fn serve_sandbox(&self, a: &ActorAttribution) -> Result<Arc<dyn SandboxNetwork>> {
        let (egress_port, services) = match &self.gate {
            Some(gate) => (
                gate.egress_port,
                SessionServices {
                    egress: Some(Arc::new(gate.egress.clone())),
                    dns_relay: Some(Arc::new(gate.dns_relay.clone())),
                    dns_policy: Some(gate.actor_egress_policy(a)),
                },
            ),
            None => (UNSERVED_EGRESS_PORT, SessionServices::default()),
        };
        let cfg = SandboxNetworkConfig {
            actor_uid: a.uid.clone(),
            veth: self.veth,
            egress_port,
        };
        let session = SandboxSession::serve(cfg, services)
            .await
            .map_err(|e| Error::io("while setting up the sandbox network", e))?;
        Ok(Arc::new(SessionNetwork(session)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attribution() -> ActorAttribution {
        ActorAttribution {
            actor: hivekeep_core::resource::ActorRef::new("h", "a"),
            uid: "uid-1".into(),
            template_hive: String::new(),
            template_name: String::new(),
        }
    }

    #[tokio::test]
    async fn without_a_gate_egress_is_inert() {
        let t = NetnsTunnel::new(None, true);
        let gw = pb::EgressGateway {
            address: "gate-egress:443".into(),
        };
        let e = t.prepare_egress(&attribution(), Some(&gw)).await.unwrap();
        assert_eq!(e.gateway.as_deref(), Some("gate-egress:443"));
        t.activate_egress(&attribution(), e).unwrap();
        t.activate_ingress(&attribution(), crate::ports::direct_dialer())
            .unwrap();
        t.deactivate(&attribution()).await.unwrap();
    }

    #[tokio::test]
    async fn a_closed_session_refuses_dials() {
        let n = SessionNetwork(Arc::new(SandboxSession::closed()));
        let err = (n.dialer())("127.0.0.1:1".into()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotConnected);
        n.close().await.unwrap();
    }

    #[cfg(not(target_os = "linux"))]
    #[tokio::test]
    async fn sandbox_networks_need_linux() {
        let t = NetnsTunnel::new(None, true);
        assert!(t.serve_sandbox(&attribution()).await.is_err());
    }
}

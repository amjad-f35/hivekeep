//! Command-line flags both sandbox workers take for the node around them:
//! the tunnel (flags emitted by the worker pool controller), the keeper's
//! worker-support connection, and the pod's projected capacity.

use std::path::PathBuf;
use std::sync::Arc;

use hivekeep_gate::tunnel::{Tunnel as GateTunnel, TunnelConfig};
use tokio_util::sync::CancellationToken;

use crate::actor_net::ACTOR_HTTP_UPSTREAM;
use crate::keeper_dial::{KeeperDialConfig, DEFAULT_POD_IDENTITY_OID};
use crate::netns_tunnel::NetnsTunnel;
use crate::node_path;
use crate::register::CAPACITY_MOUNT_PATH;

/// The node-facing flags.
#[derive(Debug, Clone, clap::Args)]
pub struct NodeFlags {
    /// The tunnel carrying Actor traffic to and from the gate.
    #[command(flatten)]
    pub tunnel: TunnelConfig,
    /// Run without the tunnel. Actors still get network namespaces of their
    /// own, but their egress and DNS go unserved and no ingress reaches
    /// them. For running a worker without the node's identity bundles.
    #[arg(long)]
    pub no_tunnel: bool,
    /// The keeper's worker-support socket.
    #[arg(long, default_value_os_t = node_path::worker_support_socket())]
    pub keeper_socket: PathBuf,
    /// The certificate extension carrying the pod identity.
    #[arg(long, default_value = DEFAULT_POD_IDENTITY_OID)]
    pub pod_identity_oid: String,
    /// Where the pod's resource limits are projected.
    #[arg(long, default_value = CAPACITY_MOUNT_PATH)]
    pub capacity_dir: PathBuf,
}

impl NodeFlags {
    /// The keeper connection, authenticated with the tunnel's pod
    /// credentials and expecting the tunnel's keeper identity.
    pub fn keeper_dial(&self) -> KeeperDialConfig {
        KeeperDialConfig {
            socket: self.keeper_socket.clone(),
            credential_bundle: self.tunnel.credential_bundle.clone(),
            trust_bundle: self.tunnel.trust_bundle.clone(),
            keeper_spiffe_id: self.tunnel.keeper_identity.clone(),
            pod_identity_oid: self.pod_identity_oid.clone(),
        }
    }

    /// Starts the gate tunnel (unless disabled) and wraps it in the
    /// per-Actor namespace seam; `veth` as in [`NetnsTunnel::new`].
    pub async fn start_tunnel(
        &self,
        veth: bool,
        cancel: CancellationToken,
    ) -> Result<NetnsTunnel, String> {
        if self.no_tunnel {
            tracing::warn!(
                "Running without the tunnel; Actor egress, DNS and ingress are not served"
            );
            return Ok(NetnsTunnel::new(None, veth));
        }
        let gate = GateTunnel::start_with(
            self.tunnel.clone(),
            ACTOR_HTTP_UPSTREAM,
            std::path::Path::new(hivekeep_gate::tunnel::runtime::RESOLV_CONF),
            self.keeper_socket.clone(),
            cancel,
        )
        .await
        .map_err(|e| format!("while starting the tunnel: {e}"))?;
        Ok(NetnsTunnel::new(Some(Arc::new(gate)), veth))
    }
}

/// Parses node flags alone (tests and tools).
#[cfg(test)]
#[derive(Debug, clap::Parser)]
struct Only {
    #[command(flatten)]
    node: NodeFlags,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn accepts_the_controllers_worker_flags() {
        let o = Only::try_parse_from([
            "w",
            "--tunnel-listen-address=:443",
            "--tunnel-connect-listen-address=:8443",
            "--tunnel-credential-bundle=/run/podidentity.podcert.hivekeep.io/credential-bundle.pem",
            "--tunnel-trust-bundle=/run/podidentity.podcert.hivekeep.io/trust-bundle.pem",
            "--tunnel-client-identity=spiffe://cluster.local/ns/x/sa/hivekeep-gate-router",
            "--tunnel-broker-identity=spiffe://cluster.local/ns/x/sa/hivekeep-keeper",
            "--tunnel-egress-listen-address=0.0.0.0:15001",
            "--tunnel-egress-trust-bundle=/run/servicedns.podcert.hivekeep.io/trust-bundle.pem",
            "--dns-egress-policy=enforce-in-tunnel",
        ])
        .unwrap();
        let dial = o.node.keeper_dial();
        assert_eq!(
            dial.keeper_spiffe_id,
            "spiffe://cluster.local/ns/x/sa/hivekeep-keeper"
        );
        assert_eq!(
            dial.credential_bundle,
            PathBuf::from("/run/podidentity.podcert.hivekeep.io/credential-bundle.pem")
        );
        assert_eq!(dial.socket, node_path::worker_support_socket());
        assert_eq!(o.node.tunnel.dns_egress_policy, "enforce-in-tunnel");
        assert!(!o.node.no_tunnel);
    }

    #[test]
    fn defaults_name_the_canonical_install() {
        let o = Only::try_parse_from(["w"]).unwrap();
        assert_eq!(
            o.node.keeper_dial().keeper_spiffe_id,
            hivekeep_gate::install::keeper_spiffe_id(hivekeep_gate::install::SYSTEM_NAMESPACE)
        );
        assert_eq!(o.node.capacity_dir, PathBuf::from(CAPACITY_MOUNT_PATH));
    }

    #[tokio::test]
    async fn no_tunnel_starts_nothing() {
        let o = Only::try_parse_from(["w", "--no-tunnel"]).unwrap();
        o.node
            .start_tunnel(true, CancellationToken::new())
            .await
            .unwrap();
    }
}

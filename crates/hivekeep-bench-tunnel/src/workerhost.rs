//! Hosts the tunnel the way a worker does, with the worker's own code: the
//! tunnel started once, then per Actor the calls a worker makes to run one,
//! in its order. Only the sandbox itself is missing: each Actor's
//! namespaces, veth, redirect, egress listener and DNS relay are real, but
//! no sandbox runtime runs in them.

use std::path::{Path, PathBuf};

use hivekeep_core::resource::{ActorAttribution, ActorRef};
use hivekeep_gate::tunnel::{Tunnel, TunnelConfig};
use hivekeep_proto::worker::v1::EgressGateway;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::sandbox_net::{self, SandboxConfig, SandboxSession};
use crate::{Error, Result};

/// The hive every benchmark Actor is in.
pub const HIVE: &str = "bench";

/// Where the ingress actor listens inside each sandbox.
pub const ACTOR_PORT: u16 = sandbox_net::ACTOR_PORT;

/// One hosted Actor, as a process placing an ingress actor in it needs it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sandbox {
    /// `hive/name`, the value of the routing header.
    pub actor: String,
    pub uid: String,
    /// The namespace the Actor's runtime would run in, where its ingress
    /// actor listens and its egress starts.
    pub netns: PathBuf,
}

/// The hosted Actors, for the load generator and the supervisor.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Info {
    pub sandboxes: Vec<Sandbox>,
    /// The port a CONNECT names to reach an ingress actor.
    pub actor_port: u16,
}

/// The UID of benchmark Actor `i`.
pub fn actor_uid(i: usize) -> String {
    format!("00000000-0000-4000-8000-{i:012}")
}

struct Hosted {
    attribution: ActorAttribution,
    session: SandboxSession,
}

/// A running tunnel with its Actors.
pub struct Host {
    tunnel: Tunnel,
    actors: Mutex<Vec<std::sync::Arc<Hosted>>>,
}

impl Host {
    /// Starts the tunnel from `cfg`, as a worker's main does, and hosts
    /// `actors` Actors whose egress tunnels to `gateway`.
    pub async fn start(
        cfg: TunnelConfig,
        actors: usize,
        gateway: &str,
        cancel: CancellationToken,
    ) -> Result<Host> {
        let tunnel = Tunnel::start(cfg, sandbox_net::ACTOR_HTTP_UPSTREAM, cancel)
            .await
            .map_err(Error::Msg)?;
        Self::host_all(tunnel, actors, gateway).await
    }

    /// [`Host::start`] with an explicit resolv.conf and keeper socket.
    pub async fn start_with(
        cfg: TunnelConfig,
        actors: usize,
        gateway: &str,
        resolv_conf: &Path,
        keeper_socket: PathBuf,
        cancel: CancellationToken,
    ) -> Result<Host> {
        let tunnel = Tunnel::start_with(
            cfg,
            sandbox_net::ACTOR_HTTP_UPSTREAM,
            resolv_conf,
            keeper_socket,
            cancel,
        )
        .await
        .map_err(Error::Msg)?;
        Self::host_all(tunnel, actors, gateway).await
    }

    async fn host_all(tunnel: Tunnel, actors: usize, gateway: &str) -> Result<Host> {
        let h = Host {
            tunnel,
            actors: Mutex::new(Vec::new()),
        };
        for i in 0..actors {
            let name = format!("actor-{i}");
            let attribution = ActorAttribution {
                actor: ActorRef::new(HIVE, name.clone()),
                uid: actor_uid(i),
                template_hive: String::new(),
                template_name: String::new(),
            };
            if let Err(e) = h.host(attribution, gateway).await {
                let closed = h
                    .close()
                    .await
                    .err()
                    .map(|c| format!("; closing: {c}"))
                    .unwrap_or_default();
                return Err(Error::msg(format!("hosting {name}: {e}{closed}")));
            }
        }
        Ok(h)
    }

    /// Runs one Actor's network as a worker runs a workload: clear any
    /// previous activation, prepare egress (minting the Actor certificate
    /// through the keeper), set up the sandbox network, then activate.
    async fn host(&self, attribution: ActorAttribution, gateway: &str) -> Result<()> {
        self.tunnel.deactivate(&attribution).await;
        let egress = self
            .tunnel
            .prepare_egress(
                &attribution,
                Some(&EgressGateway {
                    address: gateway.to_string(),
                }),
            )
            .await
            .map_err(Error::Msg)?;
        let session = sandbox_net::serve_sandbox(
            SandboxConfig {
                actor_uid: attribution.uid.clone(),
                egress_port: self.tunnel.egress_port,
                dns_policy: Some(self.tunnel.actor_egress_policy(&attribution)),
            },
            &self.tunnel.egress,
            &self.tunnel.dns_relay,
        )
        .await?;
        let dial = session.dialer();
        let hosted = std::sync::Arc::new(Hosted {
            attribution,
            session,
        });
        self.actors.lock().push(hosted.clone());
        self.tunnel
            .activate_egress(&hosted.attribution, egress)
            .map_err(Error::Msg)?;
        self.tunnel
            .activate_ingress(&hosted.attribution, dial)
            .map_err(Error::Msg)
    }

    /// The hosted Actors.
    pub fn info(&self) -> Info {
        let actors = self.actors.lock();
        Info {
            actor_port: ACTOR_PORT,
            sandboxes: actors
                .iter()
                .map(|a| Sandbox {
                    actor: format!("{}/{}", a.attribution.actor.hive, a.attribution.actor.name),
                    uid: a.attribution.uid.clone(),
                    netns: a.session.runtime_netns_path(),
                })
                .collect(),
        }
    }

    /// Tears every Actor down as a worker does when an Actor stops:
    /// deactivate its traffic, then remove its network.
    pub async fn close(&self) -> Result<()> {
        let actors: Vec<_> = std::mem::take(&mut *self.actors.lock());
        let mut errs = Vec::new();
        for a in actors {
            self.tunnel.deactivate(&a.attribution).await;
            if let Err(e) = a.session.close().await {
                errs.push(e.to_string());
            }
        }
        if errs.is_empty() {
            return Ok(());
        }
        let err = errs.join("; ");
        tracing::error!(%err, "tearing down Actors");
        Err(Error::msg(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_uids_are_uuids() {
        assert_eq!(actor_uid(7), "00000000-0000-4000-8000-000000000007");
    }

    #[test]
    fn info_wire_shape() {
        let info = Info {
            sandboxes: vec![Sandbox {
                actor: "bench/actor-0".into(),
                uid: actor_uid(0),
                netns: sandbox_net::actor_netns_path(&actor_uid(0)),
            }],
            actor_port: ACTOR_PORT,
        };
        let v = serde_json::to_value(&info).unwrap();
        assert_eq!(v["sandboxes"][0]["actor"], "bench/actor-0");
        assert_eq!(v["actor_port"], 80);
        // The load generator reads the same JSON into its own view.
        let seen: crate::load::stats::TunnelInfo = serde_json::from_value(v).unwrap();
        assert_eq!(
            (seen.sandboxes[0].actor.as_str(), seen.actor_port),
            ("bench/actor-0", 80)
        );
    }
}

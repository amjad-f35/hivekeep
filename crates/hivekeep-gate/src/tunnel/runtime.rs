//! The tunnel a worker runs for its Actors: ingress listeners, the egress
//! proxy, the DNS relay, and the per-Actor activation around them.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use hivekeep_core::resource::ActorAttribution;
use hivekeep_proto::keeper::v1 as keeper;
use hivekeep_proto::worker::v1 as worker;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::dns::{PolicySource, Relay};
use super::egress::{self, BrokerCertificateSource, BrokerConfig, Client, ClientConfig, Proxy};
use super::ingress::{self, Dial};
use super::keeper_dial;
use super::policy_source::{get_egress_policy, tracing_log, ActorEgressPolicy, Fetch};
use crate::egress_policy::validate_dns_egress_policy;
use crate::install;

/// The resolv.conf the DNS relay forwards to.
pub const RESOLV_CONF: &str = "/etc/resolv.conf";

/// Worker-side tunnel settings.
#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct TunnelConfig {
    /// Address for Actor ingress HTTPS.
    #[arg(long = "tunnel-listen-address", default_value = ":443")]
    pub listen_address: String,
    /// Address for Actor ingress mTLS CONNECT.
    #[arg(long = "tunnel-connect-listen-address", default_value = ":8443")]
    pub connect_listen_address: String,
    /// Worker Pod credential bundle for inbound serving and outbound mTLS.
    #[arg(
        long = "tunnel-credential-bundle",
        default_value = "/run/podidentity.podcert.hivekeep.io/credential-bundle.pem"
    )]
    pub credential_bundle: PathBuf,
    /// Pod identity trust bundle for router clients and the node-local keeper.
    #[arg(
        long = "tunnel-trust-bundle",
        default_value = "/run/podidentity.podcert.hivekeep.io/trust-bundle.pem"
    )]
    pub trust_bundle: PathBuf,
    /// SPIFFE identity allowed to call Actor ingress.
    #[arg(long = "tunnel-client-identity", default_value_t = install::router_spiffe_id(install::SYSTEM_NAMESPACE))]
    pub client_identity: String,
    /// SPIFFE identity the node-local keeper must present (on the
    /// credential broker and worker-support connections).
    #[arg(long = "tunnel-keeper-identity", alias = "tunnel-broker-identity", default_value_t = install::keeper_spiffe_id(install::SYSTEM_NAMESPACE))]
    pub keeper_identity: String,
    /// Address for transparently intercepted Actor egress TCP.
    #[arg(long = "tunnel-egress-listen-address", default_value = "0.0.0.0:15001")]
    pub egress_listen_address: String,
    /// Service DNS trust bundle for the egress gateway.
    #[arg(
        long = "tunnel-egress-trust-bundle",
        default_value = "/run/servicedns.podcert.hivekeep.io/trust-bundle.pem"
    )]
    pub egress_trust_bundle: PathBuf,
    /// DNS egress policy enforcement mode (must be `enforce-in-tunnel`).
    #[arg(long = "dns-egress-policy", default_value = "")]
    pub dns_egress_policy: String,
}

/// An Actor's prepared egress: a gateway client and its certificate.
pub struct ActorEgress {
    client: Arc<Client>,
    source: Arc<BrokerCertificateSource>,
    expires_at: SystemTime,
}

/// The running tunnel.
pub struct Tunnel {
    /// Actor ingress.
    pub ingress: ingress::Server,
    /// Actor egress.
    pub egress: Proxy,
    /// The port intercepted egress is redirected to.
    pub egress_port: u16,
    /// The DNS relay configuration.
    pub dns_relay: Relay,
    cfg: TunnelConfig,
    keeper_tls: Arc<rustls::ClientConfig>,
    support_socket: PathBuf,
}

fn listen_addr(address: &str) -> String {
    if address.starts_with(':') {
        format!("0.0.0.0{address}")
    } else {
        address.to_string()
    }
}

async fn serve<F, Fut>(
    name: &'static str,
    address: &str,
    cancel: CancellationToken,
    f: F,
) -> Result<(), String>
where
    F: FnOnce(TcpListener, CancellationToken) -> Fut,
    Fut: std::future::Future<Output = std::io::Result<()>> + Send + 'static,
{
    let lis = TcpListener::bind(listen_addr(address))
        .await
        .map_err(|e| format!("while opening {name} listener: {e}"))?;
    let fut = f(lis, cancel);
    tokio::spawn(async move {
        if let Err(err) = fut.await {
            tracing::error!(%err, "failed to serve {name}");
        }
    });
    tracing::info!(address, "{name} serving");
    Ok(())
}

impl Tunnel {
    /// Starts the ingress listeners and prepares egress and DNS, forwarding
    /// ingress to `upstream` inside each Actor's namespace.
    pub async fn start(
        cfg: TunnelConfig,
        upstream: &str,
        cancel: CancellationToken,
    ) -> Result<Tunnel, String> {
        Self::start_with(
            cfg,
            upstream,
            Path::new(RESOLV_CONF),
            PathBuf::from(install::WORKER_SUPPORT_SOCKET),
            cancel,
        )
        .await
    }

    /// Like [`Tunnel::start`] with explicit resolv.conf and keeper socket.
    pub async fn start_with(
        cfg: TunnelConfig,
        upstream: &str,
        resolv_conf: &Path,
        support_socket: PathBuf,
        cancel: CancellationToken,
    ) -> Result<Tunnel, String> {
        validate_dns_egress_policy(&cfg.dns_egress_policy).map_err(|e| e.to_string())?;
        let upstream: url::Url = upstream
            .parse()
            .map_err(|e| format!("while parsing tunnel upstream: {e}"))?;
        let dns_relay =
            Relay::new(resolv_conf).map_err(|e| format!("configuring DNS relay: {e}"))?;
        let keeper_tls = keeper_dial::tls_config(
            &cfg.credential_bundle,
            &cfg.trust_bundle,
            &cfg.keeper_identity,
        )
        .map_err(|e| format!("configuring the keeper connection: {e}"))?;
        let ingress = ingress::Server::new(ingress::Config {
            credential_bundle_path: cfg.credential_bundle.clone(),
            trust_bundle_path: cfg.trust_bundle.clone(),
            allowed_client_id: cfg.client_identity.clone(),
            upstream,
        })
        .map_err(|e| format!("configuring ingress: {e}"))?;
        let srv = ingress.clone();
        serve(
            "tunnel",
            &cfg.listen_address,
            cancel.clone(),
            move |l, c| async move { srv.serve(l, c).await },
        )
        .await?;
        let srv = ingress.clone();
        serve(
            "tunnel CONNECT",
            &cfg.connect_listen_address,
            cancel.clone(),
            move |l, c| async move { srv.serve_connect(l, c).await },
        )
        .await?;
        let egress = Proxy::new(Arc::new(super::original_dst::tcp_original_destination));
        let egress_port =
            egress::port_from_listen_addr(&cfg.egress_listen_address).map_err(|e| e.to_string())?;
        Ok(Tunnel {
            ingress,
            egress,
            egress_port,
            dns_relay,
            cfg,
            keeper_tls,
            support_socket,
        })
    }

    /// Mints the Actor's certificate and builds its gateway client.
    /// `Ok(None)` when the Actor has no egress gateway.
    pub async fn prepare_egress(
        &self,
        actor: &ActorAttribution,
        gateway: Option<&worker::EgressGateway>,
    ) -> Result<Option<ActorEgress>, String> {
        let Some(gateway) = gateway else {
            return Ok(None);
        };
        prepare_egress_checks(gateway)?;
        let server_name = split_host(&gateway.address)?;
        let source = Arc::new(
            BrokerCertificateSource::new(BrokerConfig {
                socket_path: self.support_socket.clone(),
                credential_bundle_path: self.cfg.credential_bundle.clone(),
                trust_bundle_path: self.cfg.trust_bundle.clone(),
                actor_hive: actor.actor.hive.clone(),
                actor_name: actor.actor.name.clone(),
                actor_uid: actor.uid.clone(),
                keeper_spiffe_id: self.cfg.keeper_identity.clone(),
            })
            .map_err(|e| format!("while configuring Actor certificate broker: {e}"))?,
        );
        let expires_at = source
            .mint_worker_certificate()
            .await
            .map_err(|e| format!("while obtaining Actor certificate: {e}"))?;
        let client = Client::new(ClientConfig {
            gateway_address: gateway.address.clone(),
            server_name,
            client_certificate: source.clone(),
            trust_bundle_path: self.cfg.egress_trust_bundle.clone(),
        })
        .map_err(|e| format!("while configuring Actor egress client: {e}"))?;
        Ok(Some(ActorEgress {
            client: Arc::new(client),
            source,
            expires_at,
        }))
    }

    /// Turns on the Actor's prepared egress.
    pub fn activate_egress(
        &self,
        actor: &ActorAttribution,
        egress: Option<ActorEgress>,
    ) -> Result<(), String> {
        let Some(e) = egress else {
            return Ok(());
        };
        self.egress
            .activate(&actor.uid, e.client, e.source, e.expires_at)
            .map_err(|e| format!("while activating Actor egress: {e}"))
    }

    /// Routes ingress for the Actor through `dial`.
    pub fn activate_ingress(&self, actor: &ActorAttribution, dial: Dial) -> Result<(), String> {
        self.ingress
            .activate(&actor.actor.hive, &actor.actor.name, &actor.uid, dial)
            .map_err(|e| format!("while activating Actor ingress: {e}"))
    }

    /// Ends the Actor's ingress and egress, waiting for open requests and
    /// tunnels to finish.
    pub async fn deactivate(&self, actor: &ActorAttribution) {
        tokio::join!(
            self.ingress
                .deactivate(&actor.actor.hive, &actor.actor.name, &actor.uid),
            self.egress.deactivate(&actor.uid),
        );
    }

    /// The Actor's egress policy, fetched from the keeper.
    pub fn actor_egress_policy(&self, actor: &ActorAttribution) -> Arc<dyn PolicySource> {
        let (socket, tls, r) = (
            self.support_socket.clone(),
            self.keeper_tls.clone(),
            actor.actor.clone(),
        );
        let fetch: Fetch = Arc::new(move || {
            let (socket, tls, r) = (socket.clone(), tls.clone(), r.clone());
            Box::pin(async move {
                let channel = keeper_dial::connect(&socket, tls)
                    .await
                    .map_err(|e| keeper_dial::error_chain(&e))?;
                let mut client = keeper::worker_support_client::WorkerSupportClient::new(channel);
                get_egress_policy(&mut client, &r)
                    .await
                    .map_err(|e| format!("while fetching the Actor's egress policy: {e}"))
            })
        });
        Arc::new(ActorEgressPolicy::new(
            fetch,
            tracing_log(actor.actor.clone()),
        ))
    }
}

fn prepare_egress_checks(gateway: &worker::EgressGateway) -> Result<(), String> {
    if gateway.address.is_empty() {
        return Err("egress gateway address is required".into());
    }
    Ok(())
}

fn split_host(address: &str) -> Result<String, String> {
    let bad = || format!("invalid egress gateway address {address:?}");
    if let Some(rest) = address.strip_prefix('[') {
        let (h, after) = rest.split_once(']').ok_or_else(bad)?;
        after.strip_prefix(':').ok_or_else(bad)?;
        return Ok(h.to_string());
    }
    let (h, _) = address.rsplit_once(':').ok_or_else(bad)?;
    if h.contains(':') {
        return Err(bad());
    }
    Ok(h.to_string())
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;

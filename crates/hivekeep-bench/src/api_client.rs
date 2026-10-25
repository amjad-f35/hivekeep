//! The mTLS connection to hivekeep-api. A `k8s:///` target is balanced over
//! the ready endpoints of the Service's EndpointSlices; any other target is
//! dialed as `https://host:port`.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{bail, Context};
use hivekeep_k8s::endpoint_resolver::{self, EndpointResolver, ResolverSink};
use hivekeep_proto::api::v1::control_client::ControlClient;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tonic::transport::channel::Change;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

/// How to reach hivekeep-api.
#[derive(Debug, Clone)]
pub struct DialConfig {
    /// `k8s:///<svc>.<ns>.svc:<port>`, `host:port` or an `https://` URL.
    pub target: String,
    /// CA bundle the server is verified against.
    pub ca_file: String,
    /// DNS name expected on the server certificate.
    pub server_name: String,
    /// Client credential bundle (key and chain).
    pub client_cred_bundle: String,
}

/// The TLS configuration for `cfg`. The files are read once.
pub fn tls_config(cfg: &DialConfig) -> anyhow::Result<ClientTlsConfig> {
    if cfg.ca_file.is_empty() || cfg.client_cred_bundle.is_empty() {
        bail!("a CA file and a client credential bundle are required");
    }
    let ca =
        std::fs::read(&cfg.ca_file).with_context(|| format!("loading CA file {}", cfg.ca_file))?;
    let bundle = std::fs::read(&cfg.client_cred_bundle).with_context(|| {
        format!(
            "loading client credential bundle {}",
            cfg.client_cred_bundle
        )
    })?;
    hivekeep_identity::cred_bundle::CredentialBundle::parse(&bundle)
        .context("parsing client credential bundle")?;
    Ok(ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(&bundle, &bundle))
        .domain_name(cfg.server_name.clone()))
}

struct BalancerSink {
    tls: ClientTlsConfig,
    known: Mutex<HashSet<String>>,
    tx: mpsc::Sender<Change<String, Endpoint>>,
}

impl ResolverSink for BalancerSink {
    fn update_addresses(&self, addrs: Vec<String>) {
        let want: HashSet<String> = addrs.into_iter().collect();
        let mut known = self.known.lock();
        for gone in known.difference(&want).cloned().collect::<Vec<_>>() {
            let _ = self.tx.try_send(Change::Remove(gone.clone()));
            known.remove(&gone);
        }
        for new in want.difference(&known).cloned().collect::<Vec<_>>() {
            match Endpoint::from_shared(format!("https://{new}"))
                .and_then(|e| e.tls_config(self.tls.clone()))
            {
                Ok(ep) => {
                    if self.tx.try_send(Change::Insert(new.clone(), ep)).is_ok() {
                        known.insert(new);
                    }
                }
                Err(err) => tracing::warn!(addr = %new, %err, "Skipping hivekeep-api endpoint"),
            }
        }
    }

    fn report_error(&self, err: String) {
        tracing::warn!(%err, "Resolving hivekeep-api endpoints failed");
    }
}

/// A connected client and the resolver feeding it, kept alive with it.
pub struct ApiConnection {
    /// The Control API client.
    pub client: ControlClient<Channel>,
    /// The resolver behind a `k8s:` target.
    pub resolver: Option<Arc<EndpointResolver>>,
}

/// Dials hivekeep-api lazily: no request is made until the first RPC.
pub fn connect(kube: kube::Client, cfg: &DialConfig) -> anyhow::Result<ApiConnection> {
    let tls = tls_config(cfg)?;
    if cfg
        .target
        .starts_with(&format!("{}:", endpoint_resolver::SCHEME))
    {
        let (channel, tx) = Channel::balance_channel::<String>(1024);
        let sink = Arc::new(BalancerSink {
            tls,
            known: Mutex::new(HashSet::new()),
            tx,
        });
        let resolver =
            EndpointResolver::start(kube, &cfg.target, sink).map_err(anyhow::Error::msg)?;
        return Ok(ApiConnection {
            client: ControlClient::new(channel),
            resolver: Some(Arc::new(resolver)),
        });
    }
    let url = if cfg.target.contains("://") {
        cfg.target.replace("dns:///", "https://")
    } else {
        format!("https://{}", cfg.target)
    };
    let channel = Endpoint::from_shared(url)
        .context("parsing --api-endpoint")?
        .tls_config(tls)?
        .connect_lazy();
    Ok(ApiConnection {
        client: ControlClient::new(channel),
        resolver: None,
    })
}

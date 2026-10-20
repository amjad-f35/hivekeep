//! The mTLS gRPC connection to hivekeep-api.
//!
//! A `k8s:///` target is load-balanced over the ready endpoints of the
//! Service's EndpointSlices; any other target is dialed as a URL.

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
    /// `k8s:///...` or an `https://` URL.
    pub target: String,
    /// CA bundle PEM file.
    pub ca_file: String,
    /// Expected server name; empty derives it from the target.
    pub server_name: String,
    /// Client credential bundle PEM file (key and chain).
    pub client_cred_bundle: String,
}

/// The server name a target implies: `<svc>.<ns>.svc` for a `k8s:` target,
/// the URL host otherwise.
pub fn default_server_name(target: &str) -> anyhow::Result<String> {
    if target.starts_with(&format!("{}:", endpoint_resolver::SCHEME)) {
        let t = endpoint_resolver::parse_target(target).map_err(anyhow::Error::msg)?;
        return Ok(format!("{}.{}.svc", t.service, t.namespace));
    }
    let url = url_host(target).context("parsing --api-conn-spec")?;
    Ok(url)
}

fn url_host(target: &str) -> anyhow::Result<String> {
    let uri: http::Uri = target.parse()?;
    match uri.host() {
        Some(h) => Ok(h.to_string()),
        None => bail!("{target:?} has no host"),
    }
}

/// The TLS configuration for `cfg`.
pub fn tls_config(cfg: &DialConfig) -> anyhow::Result<ClientTlsConfig> {
    if cfg.ca_file.is_empty() {
        bail!("a CA file is required");
    }
    if cfg.client_cred_bundle.is_empty() {
        bail!("a client credential bundle (mTLS) is required");
    }
    let ca =
        std::fs::read(&cfg.ca_file).with_context(|| format!("loading CA file {}", cfg.ca_file))?;
    let bundle = std::fs::read(&cfg.client_cred_bundle).with_context(|| {
        format!(
            "loading client credential bundle {}",
            cfg.client_cred_bundle
        )
    })?;
    // Validate the bundle up front so a bad file fails at startup.
    hivekeep_identity::cred_bundle::CredentialBundle::parse(&bundle)
        .context("parsing client credential bundle")?;
    let server_name = if cfg.server_name.is_empty() {
        default_server_name(&cfg.target)?
    } else {
        cfg.server_name.clone()
    };
    Ok(ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(&bundle, &bundle))
        .domain_name(server_name))
}

/// Turns resolver reports into balancer changes.
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

/// A connected client, and the resolver feeding it (kept alive with it).
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
    let channel = Endpoint::from_shared(cfg.target.clone())
        .context("parsing --api-conn-spec")?
        .tls_config(tls)?
        .connect_lazy();
    Ok(ApiConnection {
        client: ControlClient::new(channel),
        resolver: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_names() {
        assert_eq!(
            default_server_name("k8s:///hivekeep-api.hivekeep-system.svc:443").unwrap(),
            "hivekeep-api.hivekeep-system.svc"
        );
        assert_eq!(
            default_server_name("k8s:///ns/api:443").unwrap(),
            "api.ns.svc"
        );
        assert_eq!(
            default_server_name("https://api.example.com:8443").unwrap(),
            "api.example.com"
        );
    }

    #[test]
    fn tls_requires_files() {
        let mut cfg = DialConfig {
            target: "k8s:///api.ns.svc:443".into(),
            ca_file: String::new(),
            server_name: String::new(),
            client_cred_bundle: "/x".into(),
        };
        assert!(tls_config(&cfg)
            .unwrap_err()
            .to_string()
            .contains("CA file"));
        cfg.ca_file = "/nonexistent/ca.pem".into();
        cfg.client_cred_bundle = String::new();
        assert!(tls_config(&cfg).unwrap_err().to_string().contains("mTLS"));
        cfg.client_cred_bundle = "/nonexistent/bundle.pem".into();
        assert!(tls_config(&cfg)
            .unwrap_err()
            .to_string()
            .contains("loading CA file"));
    }
}

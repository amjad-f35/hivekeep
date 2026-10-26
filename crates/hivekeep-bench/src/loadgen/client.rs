//! Connections a load generator needs: hivekeep-api over mTLS, and an
//! HTTP/1.1 client presenting the pod certificate to the gate router.

use std::time::Duration;

use hivekeep_k8s::install_defaults;
use hivekeep_proto::api::v1::control_client::ControlClient;
use rustls::ClientConfig;
use tonic::transport::Channel;

use crate::api_client::{self, ApiConnection, DialConfig};
use crate::tls::client::{client_config, PeerCheck};
use crate::tls::TlsError;

/// Trust bundle that verifies hivekeep-api's and the gate router's
/// Service-DNS serving certificates; projected into the load generator pod.
pub const SERVICE_DNS_CA_FILE: &str = "/run/servicedns.podcert.hivekeep.io/trust-bundle.pem";

/// The load generator's own pod-identity credential bundle.
pub const POD_IDENTITY_CRED_BUNDLE: &str =
    "/run/podidentity.podcert.hivekeep.io/credential-bundle.pem";

/// The default dial target for hivekeep-api.
pub fn default_api_endpoint() -> String {
    format!(
        "k8s:///{}:443",
        install_defaults::api_service_host(install_defaults::SYSTEM_NAMESPACE)
    )
}

/// The DNS name on the gate router's serving certificate.
pub fn router_server_name() -> String {
    format!(
        "{}.{}.svc",
        install_defaults::ROUTER_SERVICE_NAME,
        install_defaults::SYSTEM_NAMESPACE
    )
}

/// The default HTTPS base URL of the gate router.
pub fn default_router_url() -> String {
    format!("https://{}", router_server_name())
}

/// A TLS 1.3 client configuration for the gate router: the server is
/// verified against `ca_file` as `server_name` (whatever host the URL
/// names), the bundle at `cred_bundle` is presented and re-read whenever it
/// rotates, and only HTTP/1.1 is offered so each user keeps one connection.
pub fn router_tls_config(
    ca_file: &str,
    cred_bundle: &str,
    server_name: &str,
) -> Result<ClientConfig, TlsError> {
    std::fs::metadata(cred_bundle).map_err(|source| {
        TlsError::Identity(hivekeep_identity::IdentityError::Io {
            path: format!("checking router client credential bundle {cred_bundle}"),
            source,
        })
    })?;
    let cfg = client_config(
        cred_bundle,
        ca_file,
        PeerCheck::ServerName(server_name.to_string()),
    )?;
    let mut cfg = (*cfg).clone();
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(cfg)
}

/// The HTTP client every user shares to reach the gate router. Every user
/// talks to one address, so the pool must keep as many idle connections
/// as there are users or each request opens and closes its own (and the
/// pod runs out of ephemeral ports). The idle timeout outlasts a user's
/// gap between requests.
pub fn router_http_client(
    tls: ClientConfig,
    max_idle_per_host: usize,
) -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .tls_backend_preconfigured(tls)
        .http1_only()
        .pool_max_idle_per_host(max_idle_per_host)
        .pool_idle_timeout(Duration::from_secs(300))
        .timeout(Duration::from_secs(30))
        .build()
}

/// Opens an authenticated, lazily connected channel to hivekeep-api at
/// `endpoint`, presenting the pod certificate. A `k8s:///` target is
/// balanced over the Service's ready endpoints, found through the cluster
/// (in-cluster config or the local kubeconfig).
pub async fn dial_control(
    endpoint: &str,
) -> anyhow::Result<(ControlClient<Channel>, ApiConnection)> {
    let kube = kube::Client::try_default().await?;
    let cfg = DialConfig {
        target: endpoint.to_string(),
        ca_file: SERVICE_DNS_CA_FILE.to_string(),
        server_name: install_defaults::api_service_host(install_defaults::SYSTEM_NAMESPACE),
        client_cred_bundle: POD_IDENTITY_CRED_BUNDLE.to_string(),
    };
    let conn = api_client::connect(kube, &cfg)?;
    Ok((conn.client.clone(), conn))
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;

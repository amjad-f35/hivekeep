//! The identity the harness presents to the gate router, and the TLS
//! configuration that verifies the router.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{Api, PostParams};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};

use crate::clients::Clients;
use crate::env::{resource_name, system_namespace};
use crate::harness::T;
use crate::install;
use crate::kube_util::{
    is_already_exists, live_signer_roots, request_pod_certificate, PodCertificate,
};
use crate::namespace::NAMESPACE_LABEL;

/// Namespace of the router client's identity.
pub const ROUTER_CLIENT_NAMESPACE: &str = "hivekeep-e2e-router-client";
/// ServiceAccount of the router client's identity, which the router
/// allowlists.
pub const ROUTER_CLIENT_SERVICE_ACCOUNT: &str = "router-client";
/// Bounds the wait for the signer to issue.
pub const ROUTER_CLIENT_CERT_TIMEOUT: Duration = Duration::from_secs(120);

/// The SPIFFE ID issued for `service_account` in [`ROUTER_CLIENT_NAMESPACE`].
pub fn router_client_spiffe_id(service_account: &str) -> String {
    install::spiffe_id(ROUTER_CLIENT_NAMESPACE, service_account)
}

static CERTS: LazyLock<tokio::sync::Mutex<HashMap<String, PodCertificate>>> =
    LazyLock::new(Default::default);

/// A pod-identity certificate for `service_account` in
/// [`ROUTER_CLIENT_NAMESPACE`], requested once per test process (it
/// outlives any suite).
pub async fn router_client_certificate(
    t: &T,
    clients: &Clients,
    service_account: &str,
) -> PodCertificate {
    let mut certs = CERTS.lock().await;
    if let Some(c) = certs.get(service_account) {
        return c.clone();
    }
    ensure_router_client_namespace(clients)
        .await
        .unwrap_or_else(|e| {
            t.fatal(format!(
                "preparing namespace {ROUTER_CLIENT_NAMESPACE}: {e}"
            ))
        });
    let cert = request_pod_certificate(&clients.kube(), "e2e", ROUTER_CLIENT_NAMESPACE, service_account, ROUTER_CLIENT_CERT_TIMEOUT)
        .await
        .unwrap_or_else(|e| t.fatal(format!("requesting a podidentity certificate for {ROUTER_CLIENT_NAMESPACE}/{service_account}: {e}")));
    certs.insert(service_account.to_string(), cert.clone());
    cert
}

/// Creates [`ROUTER_CLIENT_NAMESPACE`] (labeled, so label cleanup removes
/// it), leaving an existing one in place.
async fn ensure_router_client_namespace(clients: &Clients) -> Result<(), String> {
    let mut ns = Namespace::default();
    ns.metadata.name = Some(ROUTER_CLIENT_NAMESPACE.into());
    ns.metadata.labels = Some([(NAMESPACE_LABEL.to_string(), "true".to_string())].into());
    match Api::<Namespace>::all(clients.kube())
        .create(&PostParams::default(), &ns)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) if is_already_exists(&e) => Ok(()),
        Err(e) => Err(format!("creating namespace: {e}")),
    }
}

/// The DNS name on the router's service-DNS serving certificate.
pub fn router_server_name() -> String {
    format!(
        "{}.{}.svc",
        resource_name(install::ROUTER_SERVICE),
        system_namespace()
    )
}

/// Parses a PEM chain and key into rustls form.
pub fn rustls_identity(
    chain_pem: &str,
    key_pem: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), String> {
    let chain = rustls_pemfile::certs(&mut chain_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("parsing certificate chain: {e}"))?;
    if chain.is_empty() {
        return Err("certificate chain is empty".into());
    }
    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
        .map_err(|e| format!("parsing private key: {e}"))?
        .ok_or("no private key found")?;
    Ok((chain, key))
}

/// Root store of the PEM certificates in `pem`.
pub fn root_store(pem: &str) -> Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut pem.as_bytes()) {
        roots
            .add(cert.map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    }
    if roots.is_empty() {
        return Err("no certificates in the trust bundle".into());
    }
    Ok(roots)
}

/// A client config trusting `roots_pem` and presenting `client` if given.
/// The caller sets ALPN; the server name travels separately.
pub fn client_tls_config(
    roots_pem: &str,
    client: Option<&PodCertificate>,
) -> Result<ClientConfig, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(root_store(roots_pem)?);
    match client {
        Some(c) => {
            let (chain, key) = rustls_identity(&c.chain_pem, &c.key_pem)?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| format!("client certificate: {e}"))
        }
        None => Ok(builder.with_no_client_auth()),
    }
}

/// A TLS config for the router's listeners that verifies its service-DNS
/// serving certificate and presents `client_cert`, if any. Pair it with
/// [`router_server_name`].
pub async fn router_tls_config(
    clients: &Clients,
    client_cert: Option<&PodCertificate>,
) -> Result<ClientConfig, String> {
    let roots = live_signer_roots(&clients.kube(), install::SERVICE_DNS_SIGNER)
        .await
        .map_err(|e| format!("loading the servicedns trust bundle: {e}"))?;
    client_tls_config(&roots, client_cert)
}

/// The router's server name in rustls form.
pub fn router_rustls_server_name() -> ServerName<'static> {
    ServerName::try_from(router_server_name()).expect("a DNS name")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_and_roots() {
        let ca = hivekeep_identity::ca::Ca::generate(
            "t",
            hivekeep_identity::key::KeyAlgorithm::EcdsaP256,
            Duration::from_secs(60),
        )
        .unwrap();
        assert_eq!(root_store(&ca.root_pem()).unwrap().len(), 1);
        assert!(root_store("").is_err());
        let cert = PodCertificate {
            chain_pem: ca.root_pem(),
            key_pem: ca.key_pem(),
        };
        let _ = rustls::crypto::ring::default_provider().install_default();
        assert!(client_tls_config(&ca.root_pem(), Some(&cert)).is_ok());
        assert!(rustls_identity("", &ca.key_pem()).is_err());
        assert_eq!(
            router_client_spiffe_id("router-client"),
            "spiffe://cluster.local/ns/hivekeep-e2e-router-client/sa/router-client"
        );
    }
}

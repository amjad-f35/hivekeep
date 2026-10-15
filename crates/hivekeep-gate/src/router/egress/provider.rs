//! The credential provider: its connection, the names it serves, and the
//! checks on what it hands back.

use std::path::PathBuf;
use std::sync::Arc;

use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::credprovider::v1::credential_provider_client::CredentialProviderClient;

/// Scheme of credential URIs (`hk-secret://<provider>/<tail>`).
pub const CREDENTIAL_URI_SCHEME: &str = "hk-secret";

/// How to reach the credential provider.
#[derive(Debug, Clone, Default)]
pub struct ProviderDialConfig {
    /// `host:port` (or URL) of the provider.
    pub address: String,
    /// CA bundle the provider's certificate chains to.
    pub ca_file: Option<PathBuf>,
    /// Client credential bundle (chain and key, PEM).
    pub client_cert: Option<PathBuf>,
    /// Name to verify the provider's certificate against.
    pub server_name: Option<String>,
    /// Plaintext (development only).
    pub insecure: bool,
}

/// A client for the configured provider. The connection is lazy.
pub fn dial_provider(
    cfg: &ProviderDialConfig,
) -> Result<CredentialProviderClient<Channel>, String> {
    let scheme = if cfg.insecure { "http" } else { "https" };
    let uri = if cfg.address.contains("://") {
        cfg.address.clone()
    } else {
        format!("{scheme}://{}", cfg.address)
    };
    let mut ep =
        Endpoint::from_shared(uri).map_err(|e| format!("credential provider address: {e}"))?;
    if cfg.insecure {
        tracing::warn!("dialing credential provider WITHOUT TLS (--credential-provider-insecure); development only");
    } else {
        let (Some(ca), Some(cert)) = (&cfg.ca_file, &cfg.client_cert) else {
            return Err("building credential-provider dial options: CA file and client credential bundle are required".into());
        };
        let ca = std::fs::read(ca).map_err(|e| format!("reading credential provider CA: {e}"))?;
        let bundle = std::fs::read(cert)
            .map_err(|e| format!("reading credential provider client bundle: {e}"))?;
        let mut tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca))
            .identity(Identity::from_pem(bundle.clone(), bundle));
        if let Some(name) = &cfg.server_name {
            tls = tls.domain_name(name.clone());
        }
        ep = ep
            .tls_config(tls)
            .map_err(|e| format!("credential provider TLS: {e}"))?;
    }
    Ok(CredentialProviderClient::new(ep.connect_lazy()))
}

/// The provider named by a credential URI.
pub fn provider_name_from_uri(raw: &str) -> Result<String, String> {
    let u = url::Url::parse(raw).map_err(|e| format!("parsing credential URI {raw:?}: {e}"))?;
    if u.scheme() != CREDENTIAL_URI_SCHEME {
        return Err(format!(
            "credential URI {raw:?}: scheme is {:?}, want {CREDENTIAL_URI_SCHEME:?}",
            u.scheme()
        ));
    }
    match u.host_str() {
        Some(h) if !h.is_empty() => Ok(h.to_string()),
        _ => Err(format!("credential URI {raw:?}: missing provider name")),
    }
}

/// Validates a `--credential-provider-name` value; "" means any.
pub fn provider_name(name: &str) -> Result<String, String> {
    if name.is_empty() {
        return Ok(String::new());
    }
    match provider_name_from_uri(&format!("{CREDENTIAL_URI_SCHEME}://{name}")) {
        Ok(got) if got == name && !name.contains(['/', '?', '#', '@', ':']) => Ok(got),
        _ => Err(format!("{name:?} is not a provider name such as k8s.io")),
    }
}

/// Refuses headers the gateway must not mutate.
pub fn validate_inject_header(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("credential injection header is required".into());
    }
    if name.starts_with(':') || name.eq_ignore_ascii_case("host") {
        return Err(format!(
            "credential injection header {name:?} is a system header the gateway forbids mutating"
        ));
    }
    Ok(())
}

/// Strips trailing newlines and refuses empty secrets or control bytes.
pub fn sanitize_secret(secret: &[u8]) -> Result<Vec<u8>, String> {
    let end = secret
        .iter()
        .rposition(|b| *b != b'\r' && *b != b'\n')
        .map_or(0, |i| i + 1);
    let secret = &secret[..end];
    if secret.is_empty() {
        return Err("resolved credential is empty".into());
    }
    if secret.iter().any(|b| *b < 0x20 || *b == 0x7f) {
        return Err("resolved credential contains a control character".into());
    }
    Ok(secret.to_vec())
}

/// A provider shared by handler clones.
pub type SharedProvider = Arc<dyn super::api::CredentialProvider>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dial_provider_requires_tls_material() {
        assert!(dial_provider(&ProviderDialConfig {
            address: "provider:50051".into(),
            ..Default::default()
        })
        .is_err());
        assert!(dial_provider(&ProviderDialConfig {
            address: "provider:50051".into(),
            insecure: true,
            ..Default::default()
        })
        .is_ok());
    }

    #[test]
    fn provider_name_cases() {
        for (input, want) in [
            ("", Some("")),
            ("k8s.io", Some("k8s.io")),
            ("hk-secret://k8s.io", None),
            ("k8s.io/default", None),
            ("k8s.io?x=1", None),
        ] {
            assert_eq!(provider_name(input).ok().as_deref(), want, "{input:?}");
        }
    }

    #[test]
    fn sanitize_and_validate() {
        assert_eq!(sanitize_secret(b"s3cr3t\r\n").unwrap(), b"s3cr3t");
        assert!(sanitize_secret(b"\n").is_err());
        assert!(sanitize_secret(b"a\x01b").is_err());
        assert!(validate_inject_header(":path").is_err());
        assert!(validate_inject_header("Host").is_err());
        assert!(validate_inject_header("authorization").is_ok());
        assert_eq!(
            provider_name_from_uri("hk-secret://k8s/default/token").unwrap(),
            "k8s"
        );
        assert!(provider_name_from_uri("https://k8s/x").is_err());
    }
}

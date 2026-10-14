//! The worker ingress TLS configuration: the worker's credential bundle,
//! clients verified against a trust bundle and required to be the router.
//! Both files are re-read for every handshake, so rotations apply to new
//! connections without a restart.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use hivekeep_identity::cred_bundle::CredentialBundle;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::WebPkiClientVerifier;
use rustls::{DigitallySignedStruct, DistinguishedName, SignatureScheme};

use crate::tunnel::keeper_dial::{certified_key, uri_sans};

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Loads the PEM trust anchors of `path` into a root store.
pub fn load_roots(path: &Path) -> Result<rustls::RootCertStore, String> {
    let anchors = hivekeep_identity::cred_bundle::load_trust_anchors(path)
        .map_err(|e| format!("ingress: loading trust bundle: {e}"))?;
    let mut roots = rustls::RootCertStore::empty();
    for der in anchors {
        roots
            .add(CertificateDer::from(der))
            .map_err(|e| format!("ingress: loading trust bundle: {e}"))?;
    }
    if roots.is_empty() {
        return Err("ingress: trust bundle has no certificates".into());
    }
    Ok(roots)
}

/// Verifies the client chain, then requires `allowed` among its URI SANs.
#[derive(Debug)]
struct AllowedClient {
    inner: Arc<dyn ClientCertVerifier>,
    allowed: String,
}

impl ClientCertVerifier for AllowedClient {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.inner
            .verify_client_cert(end_entity, intermediates, now)?;
        if uri_sans(end_entity).contains(&self.allowed) {
            Ok(ClientCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "ingress: client is not {:?}",
                self.allowed
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Always presents one certificate.
#[derive(Debug)]
pub struct FixedCert(pub Arc<rustls::sign::CertifiedKey>);

impl rustls::server::ResolvesServerCert for FixedCert {
    fn resolve(
        &self,
        _: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(self.0.clone())
    }
}

/// Builds server TLS configs from files.
#[derive(Debug, Clone)]
pub struct IngressTls {
    /// The worker credential bundle (chain and key, PEM).
    pub credential_bundle: PathBuf,
    /// The trust bundle client certificates chain to.
    pub trust_bundle: PathBuf,
    /// The SPIFFE ID the client must present.
    pub allowed_client_id: String,
}

impl IngressTls {
    /// A config for one handshake, offering `alpn`.
    pub fn server_config(&self, alpn: &[&[u8]]) -> Result<Arc<rustls::ServerConfig>, String> {
        let roots = load_roots(&self.trust_bundle)?;
        let inner = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
            .build()
            .map_err(|e| format!("ingress: client verifier: {e}"))?;
        let verifier = Arc::new(AllowedClient {
            inner,
            allowed: self.allowed_client_id.clone(),
        });
        let bundle = CredentialBundle::load(&self.credential_bundle)
            .map_err(|e| format!("ingress: reading credential bundle: {e}"))?;
        let key = certified_key(&bundle)
            .map_err(|e| format!("ingress: parsing credential bundle: {e}"))?;
        let mut cfg = rustls::ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .with_client_cert_verifier(verifier)
            .with_cert_resolver(Arc::new(FixedCert(key)));
        cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
        Ok(Arc::new(cfg))
    }
}

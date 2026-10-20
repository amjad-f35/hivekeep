//! Mutual TLS for the gRPC listener.
//!
//! The provider presents its credential bundle and requires each caller to
//! present a certificate that chains to the client CA bundle and carries
//! the injector's SPIFFE ID as a URI SAN. The configuration is rebuilt for
//! every connection from files cached against their status, so a rotated
//! server certificate or a newly published client CA takes effect on the
//! next connection without a restart.

use std::fmt;
use std::io;
use std::sync::Arc;

use futures::Stream;
use hivekeep_identity::cred_loader::{CredentialBundleLoader, TrustAnchorLoader};
use rustls::client::danger::HandshakeSignatureValid;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::WebPkiClientVerifier;
use rustls::{
    DigitallySignedStruct, DistinguishedName, RootCertStore, ServerConfig, SignatureScheme,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::ReceiverStream;
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::{FromDer, X509Certificate};

/// Why a TLS configuration could not be built.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A credential or trust file could not be loaded.
    #[error(transparent)]
    Identity(#[from] hivekeep_identity::IdentityError),
    /// rustls refused the material.
    #[error("TLS configuration: {0}")]
    Config(String),
}

/// Accepts a caller only when its leaf certificate carries `expected` as a
/// URI SAN. `leaf` is `None` when the caller presented no certificate.
pub fn verify_client_san(leaf: Option<&[u8]>, expected: &str) -> Result<(), String> {
    let der = leaf.ok_or("client certificate is required")?;
    let (_, cert) =
        X509Certificate::from_der(der).map_err(|e| format!("parsing client certificate: {e}"))?;
    let uris: Vec<&str> = cert
        .subject_alternative_name()
        .map_err(|e| format!("parsing client certificate SANs: {e}"))?
        .map(|ext| {
            ext.value
                .general_names
                .iter()
                .filter_map(|n| match n {
                    GeneralName::URI(u) => Some(*u),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    if uris.contains(&expected) {
        Ok(())
    } else {
        Err(format!(
            "client certificate URI SANs {uris:?} do not include the expected injector identity {expected:?}"
        ))
    }
}

/// Wraps a chain verifier with the injector SAN check, so a wrong identity
/// fails the handshake itself.
struct SanPinningVerifier {
    chain: Arc<dyn ClientCertVerifier>,
    expected: String,
}

impl fmt::Debug for SanPinningVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SanPinningVerifier")
            .field("expected", &self.expected)
            .finish_non_exhaustive()
    }
}

impl ClientCertVerifier for SanPinningVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.chain.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let verified = self
            .chain
            .verify_client_cert(end_entity, intermediates, now)?;
        verify_client_san(Some(end_entity.as_ref()), &self.expected)
            .map_err(rustls::Error::General)?;
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.chain.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.chain.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.chain.supported_verify_schemes()
    }
}

/// The provider's server side of mutual TLS.
#[derive(Debug)]
pub struct MutualTls {
    server_bundle: CredentialBundleLoader,
    client_ca: TrustAnchorLoader,
    injector_identity: String,
    provider: Arc<CryptoProvider>,
}

impl MutualTls {
    /// Mutual TLS presenting the bundle at `server_bundle` and admitting
    /// callers chaining to `client_ca_file` with `injector_identity` as a URI
    /// SAN. The client CA is read once now so a missing or empty projection
    /// fails startup.
    pub fn new(
        server_bundle: &str,
        client_ca_file: &str,
        injector_identity: &str,
        provider: Arc<CryptoProvider>,
    ) -> Result<Self, TlsError> {
        let client_ca = TrustAnchorLoader::trust_anchors(client_ca_file);
        client_ca.get()?;
        Ok(MutualTls {
            server_bundle: CredentialBundleLoader::credential_bundle(server_bundle),
            client_ca,
            injector_identity: injector_identity.to_string(),
            provider,
        })
    }

    /// A TLS 1.3 configuration from the current files.
    pub fn server_config(&self) -> Result<Arc<ServerConfig>, TlsError> {
        let mut roots = RootCertStore::empty();
        for der in self.client_ca.get()?.iter() {
            roots
                .add(CertificateDer::from(der.clone()))
                .map_err(|e| TlsError::Config(format!("client CA: {e}")))?;
        }
        let chain =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots), self.provider.clone())
                .build()
                .map_err(|e| TlsError::Config(format!("client verifier: {e}")))?;
        let verifier = Arc::new(SanPinningVerifier {
            chain,
            expected: self.injector_identity.clone(),
        });

        let bundle = self.server_bundle.get()?;
        let certs = bundle
            .chain
            .iter()
            .cloned()
            .map(CertificateDer::from)
            .collect();
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(bundle.key.pkcs8_der().to_vec()));
        let mut cfg = ServerConfig::builder_with_provider(self.provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| TlsError::Config(e.to_string()))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::Config(format!("server certificate: {e}")))?;
        cfg.alpn_protocols = vec![b"h2".to_vec()];
        Ok(Arc::new(cfg))
    }

    /// Runs the server handshake on one accepted connection.
    pub async fn accept(&self, tcp: TcpStream) -> io::Result<TlsStream<TcpStream>> {
        let cfg = self
            .server_config()
            .map_err(|e| io::Error::other(e.to_string()))?;
        TlsAcceptor::from(cfg).accept(tcp).await
    }

    /// Accepts connections from `listener`, handshaking each on its own
    /// task, and yields those that complete. Failed handshakes are logged
    /// and dropped so one bad caller cannot stall the listener.
    pub fn incoming(
        self: Arc<Self>,
        listener: TcpListener,
    ) -> impl Stream<Item = io::Result<TlsStream<TcpStream>>> {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let (tcp, peer) = match listener.accept().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(err = %e, "Accepting a connection failed");
                        continue;
                    }
                };
                if tx.is_closed() {
                    return;
                }
                let tls = self.clone();
                let conn_tx = tx.clone();
                tokio::spawn(async move {
                    match tls.accept(tcp).await {
                        Ok(stream) => {
                            let _ = conn_tx.send(Ok(stream)).await;
                        }
                        Err(e) => tracing::warn!(%peer, err = %e, "TLS handshake failed"),
                    }
                });
            }
        });
        ReceiverStream::new(rx)
    }
}

#[cfg(test)]
#[path = "tls_tests.rs"]
mod tests;

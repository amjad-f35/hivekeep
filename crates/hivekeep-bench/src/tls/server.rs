//! The serving side: present the pod-identity certificate, require a client
//! certificate chaining to the trust bundle as it is at each handshake, and
//! optionally admit only one SPIFFE ID.

use std::fmt;
use std::io;
use std::sync::Arc;

use futures::Stream;
use hivekeep_identity::cred_loader::{CredentialBundleLoader, TrustAnchorLoader};
use rustls::client::danger::HandshakeSignatureValid;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{
    DigitallySignedStruct, DistinguishedName, RootCertStore, ServerConfig, SignatureScheme,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::ReceiverStream;

use super::peer_id::verify_peer_id;
use super::{certified_key, provider, root_store, TlsError};

/// Resolves the serving certificate from the bundle file at each handshake,
/// so the server starts even before the kubelet has written it.
struct BundleResolver {
    bundle: CredentialBundleLoader,
    provider: Arc<CryptoProvider>,
}

impl fmt::Debug for BundleResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BundleResolver")
            .field("bundle", &self.bundle)
            .finish_non_exhaustive()
    }
}

impl ResolvesServerCert for BundleResolver {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        match self
            .bundle
            .get()
            .map_err(TlsError::from)
            .and_then(|b| certified_key(&b, &self.provider))
        {
            Ok(k) => Some(Arc::new(k)),
            Err(err) => {
                tracing::warn!(%err, "Serving credential bundle unavailable");
                None
            }
        }
    }
}

/// Wraps a chain verifier with a SPIFFE ID pin, so a wrong identity fails
/// the handshake itself.
struct PinnedVerifier {
    chain: Arc<dyn ClientCertVerifier>,
    allowed: String,
}

impl fmt::Debug for PinnedVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedVerifier")
            .field("allowed", &self.allowed)
            .finish_non_exhaustive()
    }
}

impl ClientCertVerifier for PinnedVerifier {
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
        verify_peer_id(&[end_entity.as_ref()], &self.allowed).map_err(rustls::Error::General)?;
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

/// Server-side mutual TLS whose files are re-read per handshake.
pub struct ServerTls {
    resolver: Arc<BundleResolver>,
    client_ca: TrustAnchorLoader,
    allowed: Option<String>,
    provider: Arc<CryptoProvider>,
}

impl fmt::Debug for ServerTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerTls")
            .field("client_ca", &self.client_ca)
            .field("allowed", &self.allowed)
            .finish_non_exhaustive()
    }
}

impl ServerTls {
    /// Serves the bundle at `serving_bundle` and requires clients chaining
    /// to `client_ca`, pinned to `allowed` when set. The CA bundle is read
    /// once now, so a missing projection fails startup.
    pub fn new(
        serving_bundle: &str,
        client_ca: &str,
        allowed: Option<&str>,
    ) -> Result<ServerTls, TlsError> {
        let ca = TrustAnchorLoader::trust_anchors(client_ca);
        ca.get()?;
        let provider = provider();
        Ok(ServerTls {
            resolver: Arc::new(BundleResolver {
                bundle: CredentialBundleLoader::credential_bundle(serving_bundle),
                provider: provider.clone(),
            }),
            client_ca: ca,
            allowed: allowed.map(str::to_string),
            provider,
        })
    }

    /// The client CA roots as the file is now.
    pub fn client_roots(&self) -> Result<RootCertStore, TlsError> {
        root_store(&self.client_ca.get()?)
    }

    /// A TLS 1.3 configuration with the client CA as it is now.
    pub fn server_config(&self) -> Result<Arc<ServerConfig>, TlsError> {
        let chain = WebPkiClientVerifier::builder_with_provider(
            Arc::new(self.client_roots()?),
            self.provider.clone(),
        )
        .build()
        .map_err(|e| TlsError::Config(format!("client verifier: {e}")))?;
        let verifier: Arc<dyn ClientCertVerifier> = match &self.allowed {
            Some(allowed) => Arc::new(PinnedVerifier {
                chain,
                allowed: allowed.clone(),
            }),
            None => chain,
        };
        let mut cfg = ServerConfig::builder_with_provider(self.provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| TlsError::Config(e.to_string()))?
            .with_client_cert_verifier(verifier)
            .with_cert_resolver(self.resolver.clone());
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
    /// task, and yields those that complete. A failed handshake is logged
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
                    Err(err) => {
                        tracing::warn!(%err, "Accepting a connection failed");
                        continue;
                    }
                };
                if tx.is_closed() {
                    return;
                }
                let tls = self.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    match tls.accept(tcp).await {
                        Ok(stream) => {
                            let _ = tx.send(Ok(stream)).await;
                        }
                        Err(err) => tracing::debug!(%peer, %err, "TLS handshake failed"),
                    }
                });
            }
        });
        ReceiverStream::new(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::testca::TestCa;

    fn roots_der(store: &RootCertStore) -> Vec<Vec<u8>> {
        store
            .roots
            .iter()
            .map(|r| r.subject_public_key_info.as_ref().to_vec())
            .collect()
    }

    // TestServerTLSConfigRejectsUnreadableCACerts
    #[test]
    fn rejects_unreadable_ca_certs() {
        let dir = tempfile::tempdir().unwrap();
        let absent = dir.path().join("absent.pem");
        assert!(ServerTls::new(
            "/nonexistent-cred-bundle.pem",
            absent.to_str().unwrap(),
            None
        )
        .is_err());
    }

    // TestServerTLSConfigReloadsCACertsWithoutRestart: a CA rotation on disk
    // reaches the next handshake, and the serving bundle is only needed at
    // handshake time.
    #[test]
    fn reloads_ca_certs_without_restart() {
        let a = TestCa::new();
        let b = TestCa::new();
        let path = a.trust_bundle.clone();
        let tls =
            ServerTls::new("/nonexistent-cred-bundle.pem", path.to_str().unwrap(), None).unwrap();
        tls.server_config()
            .expect("config without a serving bundle on disk");
        let before = roots_der(&tls.client_roots().unwrap());
        // A fresh file (new inode), as a projected volume rotates.
        crate::tls::testca::rotate(&path, &[&b]);
        let after = roots_der(&tls.client_roots().unwrap());
        tls.server_config().unwrap();
        assert_ne!(before, after, "the rotated trust bundle was not picked up");
    }
}

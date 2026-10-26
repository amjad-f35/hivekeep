//! Mutual TLS built on files the kubelet rotates.
//!
//! Every handshake re-reads (through a status-keyed cache) the credential
//! bundle it presents and the trust bundle it verifies the peer against, so
//! a certificate or CA rotation during a long benchmark is picked up without
//! a restart. Peers can additionally be pinned to one SPIFFE ID.

use std::sync::Arc;

use hivekeep_identity::cred_bundle::CredentialBundle;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::sign::CertifiedKey;
use rustls::RootCertStore;

pub mod client;
pub mod peer_id;
pub mod server;
#[cfg(test)]
pub(crate) mod testca;

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

/// The crypto provider every configuration here uses.
pub fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Turns a credential bundle into the key and chain rustls presents.
pub fn certified_key(
    bundle: &CredentialBundle,
    provider: &CryptoProvider,
) -> Result<CertifiedKey, TlsError> {
    let certs: Vec<CertificateDer<'static>> = bundle
        .chain
        .iter()
        .cloned()
        .map(CertificateDer::from)
        .collect();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(bundle.key.pkcs8_der().to_vec()));
    let signing = provider
        .key_provider
        .load_private_key(key)
        .map_err(|e| TlsError::Config(format!("private key: {e}")))?;
    Ok(CertifiedKey::new(certs, signing))
}

/// A root store of the DER certificates `anchors`.
pub fn root_store(anchors: &[Vec<u8>]) -> Result<RootCertStore, TlsError> {
    let mut roots = RootCertStore::empty();
    for der in anchors {
        roots
            .add(CertificateDer::from(der.clone()))
            .map_err(|e| TlsError::Config(format!("trust bundle: {e}")))?;
    }
    Ok(roots)
}

#[cfg(test)]
pub(crate) mod testing {
    //! Loopback servers for TLS tests.

    use std::net::SocketAddr;
    use std::sync::Arc;

    use hivekeep_proto::api::v1::worker_service_server::{WorkerService, WorkerServiceServer};
    use tokio::net::TcpListener;

    use super::server::ServerTls;

    /// Serves `svc` over `tls` on a loopback port until the test's runtime
    /// ends.
    pub async fn serve_worker_service<T: WorkerService>(svc: T, tls: ServerTls) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = Arc::new(tls).incoming(listener);
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(WorkerServiceServer::new(svc))
                .serve_with_incoming(incoming),
        );
        addr
    }
}

//! Trust anchors and TLS client configuration for the probe's fetches.

use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore};

/// Parses every CERTIFICATE block in `pem` into a root store. Like a PEM pool
/// append, it fails only when no certificate at all could be used.
pub fn roots_from_pem(pem: &[u8]) -> Option<RootCertStore> {
    let mut store = RootCertStore::empty();
    let mut reader = pem;
    for cert in rustls_pemfile::certs(&mut reader).flatten() {
        let _ = store.add(cert);
    }
    (!store.is_empty()).then_some(store)
}

/// The image's system trust anchors.
pub fn system_roots() -> RootCertStore {
    let mut store = RootCertStore::empty();
    let found = rustls_native_certs::load_native_certs();
    let certs: Vec<CertificateDer<'static>> = found.certs;
    store.add_parsable_certificates(certs);
    store
}

/// A TLS client configuration that trusts exactly `roots` and speaks
/// HTTP/1.1.
pub fn client_config(roots: RootCertStore) -> Result<Arc<ClientConfig>, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(cfg))
}

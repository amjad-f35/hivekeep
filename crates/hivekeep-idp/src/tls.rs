//! The serving certificate, looked up from its credential bundle on every
//! handshake. The bundle file is cached against its status, so a rotated
//! certificate is presented on the next connection without a restart.

use std::fmt;
use std::sync::Arc;

use hivekeep_identity::cred_bundle::CredentialBundle;
use hivekeep_identity::cred_loader::CredentialBundleLoader;
use parking_lot::Mutex;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls::ServerConfig;

/// Resolves the certificate from a credential bundle file.
pub struct BundleCertResolver {
    loader: CredentialBundleLoader,
    provider: Arc<CryptoProvider>,
    /// The last bundle and the key built from it, so the key is parsed only
    /// when the file changes.
    last: Mutex<Option<(Arc<CredentialBundle>, Arc<CertifiedKey>)>>,
}

impl fmt::Debug for BundleCertResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BundleCertResolver")
            .field("bundle", &self.loader.path())
            .finish_non_exhaustive()
    }
}

impl BundleCertResolver {
    /// A resolver for the bundle at `path`.
    pub fn new(path: &str, provider: Arc<CryptoProvider>) -> Self {
        BundleCertResolver {
            loader: CredentialBundleLoader::credential_bundle(path),
            provider,
            last: Mutex::new(None),
        }
    }

    /// The certificate and key from the current file.
    pub fn certified_key(&self) -> Result<Arc<CertifiedKey>, String> {
        let bundle = self.loader.get().map_err(|e| e.to_string())?;
        let mut last = self.last.lock();
        if let Some((b, k)) = last.as_ref() {
            if Arc::ptr_eq(b, &bundle) {
                return Ok(k.clone());
            }
        }
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(bundle.key.pkcs8_der().to_vec()));
        let signing = self
            .provider
            .key_provider
            .load_private_key(key)
            .map_err(|e| format!("loading the serving key: {e}"))?;
        let certs = bundle
            .chain
            .iter()
            .cloned()
            .map(CertificateDer::from)
            .collect();
        let ck = Arc::new(CertifiedKey::new(certs, signing));
        *last = Some((bundle, ck.clone()));
        Ok(ck)
    }
}

impl ResolvesServerCert for BundleCertResolver {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        match self.certified_key() {
            Ok(k) => Some(k),
            Err(e) => {
                tracing::error!(err = %e, "Cannot load the serving certificate");
                None
            }
        }
    }
}

/// A TLS 1.2+ server configuration presenting the bundle at `path`,
/// offering HTTP/2 and HTTP/1.1.
pub fn server_config(
    path: &str,
    provider: Arc<CryptoProvider>,
) -> Result<ServerConfig, rustls::Error> {
    let resolver = Arc::new(BundleCertResolver::new(path, provider.clone()));
    let mut cfg = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
        .with_no_client_auth()
        .with_cert_resolver(resolver);
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    fn write_bundle(path: &std::path::Path, cn: &str) -> Vec<u8> {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["idp.test".to_string()]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        let cert = params.self_signed(&key).unwrap();
        std::fs::write(path, format!("{}{}", cert.pem(), key.serialize_pem())).unwrap();
        cert.der().to_vec()
    }

    #[test]
    fn follows_the_bundle_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bundle.pem");
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let r = BundleCertResolver::new(path.to_str().unwrap(), provider);
        assert!(r.certified_key().is_err(), "loaded a missing bundle");

        let first = write_bundle(&path, "first");
        let k1 = r.certified_key().unwrap();
        assert_eq!(k1.cert[0].as_ref(), first.as_slice());
        assert!(
            Arc::ptr_eq(&k1, &r.certified_key().unwrap()),
            "key rebuilt for an unchanged file"
        );

        let second = write_bundle(&path, "second-one");
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(SystemTime::now() + Duration::from_secs(1))
            .unwrap();
        assert_eq!(
            r.certified_key().unwrap().cert[0].as_ref(),
            second.as_slice()
        );
    }
}

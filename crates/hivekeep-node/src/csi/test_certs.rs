//! Certificates for the CSI TLS tests: a CA per test, serving certificates
//! for a DNS name and client credential bundles in the projected layout.

use std::path::Path;
use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;

use super::tls::TlsPaths;

/// A self-signed CA issuing one test's certificates.
pub(crate) struct TestCa {
    issuer: Issuer<'static, KeyPair>,
    der: Vec<u8>,
    pem: String,
}

/// An issued certificate and its key.
pub(crate) struct Leaf {
    der: Vec<u8>,
    pem: String,
    key: KeyPair,
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

impl TestCa {
    pub(crate) fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::default();
        params
            .distinguished_name
            .push(DnType::CommonName, "test-ca");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        let cert = params.self_signed(&key).unwrap();
        TestCa {
            der: cert.der().to_vec(),
            pem: cert.pem(),
            issuer: Issuer::new(params, key),
        }
    }

    /// The CA as a trust bundle holds it.
    pub(crate) fn cert_pem(&self) -> String {
        self.pem.clone()
    }

    fn issue(&self, cn: &str, dns: Option<&str>, eku: ExtendedKeyUsagePurpose) -> Leaf {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::default();
        params.distinguished_name.push(DnType::CommonName, cn);
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![eku];
        params.subject_alt_names = dns
            .map(|d| SanType::DnsName(d.try_into().unwrap()))
            .into_iter()
            .collect();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        Leaf {
            der: cert.der().to_vec(),
            pem: cert.pem(),
            key,
        }
    }

    /// A client credential bundle: the PKCS#8 key, then the certificate.
    pub(crate) fn client_bundle(&self) -> String {
        let leaf = self.issue("test-client", None, ExtendedKeyUsagePurpose::ClientAuth);
        format!("{}{}", leaf.key.serialize_pem(), leaf.pem)
    }

    /// A TLS 1.3 server config presenting a certificate for `dns_name` and
    /// requiring client certificates issued by `client_ca`.
    pub(crate) fn server_config(
        &self,
        dns_name: &str,
        client_ca: &TestCa,
    ) -> Arc<rustls::ServerConfig> {
        let leaf = self.issue(
            "test-server",
            Some(dns_name),
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from(client_ca.der.clone()))
            .unwrap();
        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
            .build()
            .unwrap();
        let mut cfg = rustls::ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![CertificateDer::from(leaf.der.clone())],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf.key.serialize_der())),
            )
            .unwrap();
        cfg.alpn_protocols = vec![b"h2".to_vec()];
        Arc::new(cfg)
    }
}

/// Writes a credential bundle and trust bundle the way the projected
/// volumes lay them out, returning their paths.
pub(crate) fn write_creds(dir: &Path, client_bundle: &str, trust_bundle: &str) -> TlsPaths {
    let paths = TlsPaths {
        client_cert: dir.join("credential-bundle.pem"),
        ca_cert: dir.join("trust-bundle.pem"),
    };
    std::fs::write(&paths.client_cert, client_bundle).unwrap();
    std::fs::write(&paths.ca_cert, trust_bundle).unwrap();
    paths
}

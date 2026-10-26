//! Certificates for TLS tests: a throwaway CA, leaf certificates carrying a
//! SPIFFE ID as their only SAN (as the pod-identity signer issues them),
//! and DNS-named serving certificates.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, SanType,
};

/// A throwaway signer whose files live in a temporary directory.
pub(crate) struct TestCa {
    issuer: Issuer<'static, KeyPair>,
    root_der: Vec<u8>,
    dir: tempfile::TempDir,
    serial: AtomicU64,
    /// The CA certificate as a PEM trust bundle.
    pub trust_bundle: PathBuf,
}

impl TestCa {
    /// A new CA with its trust bundle written.
    pub fn new() -> TestCa {
        let key = KeyPair::generate().expect("CA key");
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "fakeworker-test");
        params.distinguished_name = dn;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cert = params.self_signed(&key).expect("CA certificate");
        let dir = tempfile::tempdir().expect("temp dir");
        let trust_bundle = dir.path().join("trust-bundle.pem");
        std::fs::write(&trust_bundle, cert.pem()).expect("write trust bundle");
        TestCa {
            root_der: cert.der().to_vec(),
            issuer: Issuer::new(params, key),
            dir,
            serial: AtomicU64::new(1),
            trust_bundle,
        }
    }

    /// Writes a credential bundle (leaf certificate, then its PKCS #8 key)
    /// for `spiffe_id` and returns its path.
    pub fn issue(&self, spiffe_id: &str) -> PathBuf {
        let key = KeyPair::generate().expect("leaf key");
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("leaf params");
        params.distinguished_name = DistinguishedName::new();
        params.subject_alt_names = vec![SanType::URI(spiffe_id.try_into().expect("SPIFFE ID"))];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params
            .signed_by(&key, &self.issuer)
            .expect("leaf certificate");
        let n = self.serial.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.path().join(format!("{n}-bundle.pem"));
        std::fs::write(&path, format!("{}{}", cert.pem(), key.serialize_pem()))
            .expect("write bundle");
        path
    }

    /// Writes a credential bundle for a server certificate valid for the
    /// DNS name `dns` and returns its path.
    pub fn issue_dns(&self, dns: &str) -> PathBuf {
        let key = KeyPair::generate().expect("leaf key");
        let mut params = CertificateParams::new(vec![dns.to_string()]).expect("leaf params");
        params.distinguished_name = DistinguishedName::new();
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let cert = params
            .signed_by(&key, &self.issuer)
            .expect("leaf certificate");
        let n = self.serial.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.path().join(format!("{n}-bundle.pem"));
        std::fs::write(&path, format!("{}{}", cert.pem(), key.serialize_pem()))
            .expect("write bundle");
        path
    }

    fn root_pem(&self) -> String {
        let b64 =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &self.root_der);
        let mut out = String::from("-----BEGIN CERTIFICATE-----\n");
        for chunk in b64.as_bytes().chunks(64) {
            out.push_str(std::str::from_utf8(chunk).unwrap());
            out.push('\n');
        }
        out.push_str("-----END CERTIFICATE-----\n");
        out
    }
}

/// Replaces the trust bundle at `path` with the certificates of `cas`, as
/// the kubelet rotates a projected bundle: a new file renamed over the old.
pub(crate) fn rotate(path: &Path, cas: &[&TestCa]) {
    let pem: String = cas.iter().map(|c| c.root_pem()).collect();
    let tmp = path.with_extension("new");
    std::fs::write(&tmp, pem).expect("write rotated bundle");
    std::fs::rename(&tmp, path).expect("rename rotated bundle");
}

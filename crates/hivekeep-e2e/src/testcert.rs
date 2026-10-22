//! Throwaway serving certificates for origins the suites stand up.
//!
//! An origin a test talks TLS to needs a certificate for the address it is
//! dialed at -- usually a Service ClusterIP, sometimes a Service DNS name --
//! and the client needs the CA that issued it. Only the public half of the CA
//! leaves this module; its signing key stays in the test process.

use std::net::IpAddr;
use std::time::{Duration, SystemTime};

use hivekeep_identity::ca::{Ca, LeafSpec};
use hivekeep_identity::key::{KeyAlgorithm, PrivateKey};
use hivekeep_identity::pem;

/// An origin's key pair plus the CA a client must trust to verify it, all PEM.
#[derive(Debug, Clone)]
pub struct ServerTls {
    /// The issuing CA's root certificate.
    pub root_ca: Vec<u8>,
    /// The origin's leaf certificate.
    pub certificate: Vec<u8>,
    /// The origin's PKCS#8 private key.
    pub private_key: Vec<u8>,
}

/// Issues a certificate for `ip` from a fresh single-use CA.
///
/// Panics (failing the calling test) if any step fails.
pub fn new_server_tls(ip: IpAddr) -> ServerTls {
    let ca = Ca::generate(
        "e2e-origin",
        KeyAlgorithm::EcdsaP256,
        Duration::from_secs(24 * 60 * 60),
    )
    .unwrap_or_else(|e| panic!("generating origin CA: {e}"));
    server_tls_with_ca(&ca, ip, &[])
}

/// Issues a certificate for `ip` and any `dns_names` from `ca`, a CA the
/// client (or the egress gateway's upstream TLS) already trusts.
///
/// Panics (failing the calling test) if any step fails.
pub fn server_tls_with_ca(ca: &Ca, ip: IpAddr, dns_names: &[&str]) -> ServerTls {
    let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256)
        .unwrap_or_else(|e| panic!("generating origin key: {e}"));
    let now = SystemTime::now();
    let mut leaf = LeafSpec::new(
        now - Duration::from_secs(60),
        now + Duration::from_secs(60 * 60),
    );
    leaf.ip_addresses = vec![ip];
    leaf.dns_names = dns_names.iter().map(|s| s.to_string()).collect();
    leaf.server_auth = true;
    let der = ca
        .sign(&leaf, &key.public_key())
        .unwrap_or_else(|e| panic!("issuing origin certificate: {e}"));
    ServerTls {
        root_ca: ca.root_pem().into_bytes(),
        certificate: pem::encode(pem::CERTIFICATE, &der).into_bytes(),
        private_key: key.to_pem().into_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustls::client::danger::ServerCertVerifier;
    use rustls::client::WebPkiServerVerifier;
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::RootCertStore;

    use super::*;

    fn verifier(roots_pem: &[&[u8]]) -> Arc<WebPkiServerVerifier> {
        let mut roots = RootCertStore::empty();
        for pem_bytes in roots_pem {
            for der in pem::decode_certificates(pem_bytes).unwrap() {
                roots.add(CertificateDer::from(der)).unwrap();
            }
        }
        if roots.is_empty() {
            // An empty store is refused by the builder; trust an unrelated CA
            // instead, which is the same as trusting nothing relevant.
            let other = Ca::generate(
                "unrelated",
                KeyAlgorithm::EcdsaP256,
                Duration::from_secs(3600),
            )
            .unwrap();
            roots
                .add(CertificateDer::from(other.root_der().to_vec()))
                .unwrap();
        }
        WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .unwrap()
    }

    fn verify(v: &WebPkiServerVerifier, cert_pem: &[u8], host: &str) -> bool {
        let der = pem::decode_certificates(cert_pem).unwrap().remove(0);
        let name = ServerName::try_from(host.to_string()).unwrap();
        v.verify_server_cert(&CertificateDer::from(der), &[], &name, &[], UnixTime::now())
            .is_ok()
    }

    fn is_ca(cert_pem: &[u8]) -> bool {
        let der = pem::decode_certificates(cert_pem).unwrap().remove(0);
        let (_, cert) = x509_parser::parse_x509_certificate(&der).unwrap();
        cert.is_ca()
    }

    #[test]
    fn new_server_tls_verifies_only_its_ip() {
        let material = new_server_tls("10.0.0.7".parse().unwrap());
        PrivateKey::from_pem(&material.private_key).expect("loading serving key");
        let trusted = verifier(&[&material.root_ca]);
        assert!(
            verify(&trusted, &material.certificate, "10.0.0.7"),
            "verifying Service IP"
        );
        assert!(
            !verify(&trusted, &material.certificate, "10.0.0.8"),
            "accepted a different Service IP"
        );
        let untrusted = verifier(&[]);
        assert!(
            !verify(&untrusted, &material.certificate, "10.0.0.7"),
            "accepted an untrusted certificate"
        );
        assert!(
            !is_ca(&material.certificate),
            "origin received a CA certificate"
        );
    }

    #[test]
    fn server_tls_with_ca_covers_ip_and_dns_name() {
        let ca = Ca::generate(
            "service-dns",
            KeyAlgorithm::EcdsaP256,
            Duration::from_secs(3600),
        )
        .unwrap();
        const NAME: &str = "https-origin.networking-test.svc.cluster.local";
        let material = server_tls_with_ca(&ca, "10.0.0.7".parse().unwrap(), &[NAME]);
        PrivateKey::from_pem(&material.private_key).unwrap();
        let root = ca.root_pem();
        let trusted = verifier(&[root.as_bytes()]);
        for host in [NAME, "10.0.0.7"] {
            assert!(
                verify(&trusted, &material.certificate, host),
                "verifying {host} against supplied CA"
            );
        }
        assert!(
            !verify(
                &trusted,
                &material.certificate,
                "another.namespace.svc.cluster.local"
            ),
            "accepted a different Service DNS name"
        );
        let untrusted = verifier(&[]);
        assert!(
            !verify(&untrusted, &material.certificate, NAME),
            "accepted an untrusted Service DNS certificate"
        );
        assert!(
            !is_ca(&material.certificate),
            "origin received a CA certificate"
        );
    }
}

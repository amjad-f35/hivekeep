//! The name-constrained CA and the certificate the target serves over HTTPS,
//! for a driver to deploy and the actor to verify against.

use std::path::Path;
use std::time::{Duration, SystemTime};

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, GeneralSubtree, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, NameConstraints, SerialNumber, PKCS_ECDSA_P256_SHA256,
};
use time::OffsetDateTime;

/// Long enough that a test cluster never sees the certificate expire; there
/// is no rotation, since nothing can sign a replacement.
pub const VALIDITY: Duration = Duration::from_secs(5 * 365 * 24 * 3600);

/// Backdates the start of validity so a node whose clock runs slightly
/// behind still accepts a certificate issued moments ago.
pub const CLOCK_SKEW: Duration = Duration::from_secs(3600);

/// A CA certificate and the target certificate and key it signed, all PEM.
#[derive(Debug, Clone)]
pub struct Bundle {
    /// The CA certificate.
    pub ca: Vec<u8>,
    /// The target's certificate.
    pub cert: Vec<u8>,
    /// The target's PKCS#8 private key.
    pub key: Vec<u8>,
}

/// Why issuing failed.
#[derive(Debug, thiserror::Error)]
#[error("{what}: {source}")]
pub struct Error {
    what: &'static str,
    #[source]
    source: rcgen::Error,
}

fn ctx(what: &'static str) -> impl FnOnce(rcgen::Error) -> Error {
    move |source| Error { what, source }
}

fn odt(t: SystemTime) -> OffsetDateTime {
    OffsetDateTime::from(t)
}

fn serial() -> SerialNumber {
    let mut b: [u8; 16] = rand::random();
    // A positive integer: the top bit clear.
    b[0] &= 0x7f;
    SerialNumber::from_slice(&b)
}

/// Issues a new CA and the certificate the target serves over HTTPS, valid
/// from `now`, for every name directly under `domain`. The gateway
/// re-originates TLS to the target and has to trust the CA, and the gateway
/// is shared, so the CA vouches for nothing but the endpoint names: it is
/// name-constrained to `domain` (critically), and its key is thrown away
/// once the target's certificate is signed.
pub fn generate(now: SystemTime, domain: &str) -> Result<Bundle, Error> {
    generate_with_ca(now, domain).map(|(b, _, _)| b)
}

/// [`generate`], also returning the CA's parameters and key so tests can
/// show what the CA refuses to vouch for.
pub(crate) fn generate_with_ca(
    now: SystemTime,
    domain: &str,
) -> Result<(Bundle, CertificateParams, KeyPair), Error> {
    let ca_key =
        KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).map_err(ctx("generating CA key"))?;
    let mut ca_params = CertificateParams::default();
    ca_params.serial_number = Some(serial());
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "egress-tests target CA");
    ca_params.not_before = odt(now - CLOCK_SKEW);
    ca_params.not_after = odt(now + VALIDITY);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    ca_params.name_constraints = Some(NameConstraints {
        permitted_subtrees: vec![GeneralSubtree::DnsName(domain.to_string())],
        excluded_subtrees: Vec::new(),
    });
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .map_err(ctx("creating CA certificate"))?;

    let leaf_key =
        KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).map_err(ctx("generating target key"))?;
    // One wildcard covers every endpoint name, which differ only in the
    // leftmost label.
    let mut leaf = CertificateParams::new(vec![format!("*.{domain}")])
        .map_err(ctx("creating target certificate"))?;
    leaf.serial_number = Some(serial());
    leaf.distinguished_name
        .push(DnType::CommonName, "egress-tests target");
    leaf.not_before = odt(now - CLOCK_SKEW);
    leaf.not_after = odt(now + VALIDITY);
    leaf.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let issuer = Issuer::from_params(&ca_params, &ca_key);
    let leaf_cert = leaf
        .signed_by(&leaf_key, &issuer)
        .map_err(ctx("creating target certificate"))?;
    let bundle = Bundle {
        ca: ca_cert.pem().into_bytes(),
        cert: leaf_cert.pem().into_bytes(),
        key: leaf_key.serialize_pem().into_bytes(),
    };
    Ok((bundle, ca_params, ca_key))
}

impl Bundle {
    /// Stores the bundle in `dir` as `ca.crt`, `tls.crt` and `tls.key`, the
    /// keys of the target's TLS Secret; only the key is private.
    pub fn write(&self, dir: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        }
        for (name, data, mode) in [
            ("ca.crt", &self.ca, 0o644),
            ("tls.crt", &self.cert, 0o644),
            ("tls.key", &self.key, 0o600),
        ] {
            let path = dir.join(name);
            std::fs::write(&path, data)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))?;
            }
            #[cfg(not(unix))]
            let _ = mode;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use rustls::client::danger::ServerCertVerifier;
    use rustls::client::WebPkiServerVerifier;
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{CertificateError, RootCertStore};

    use super::*;

    const DOMAIN: &str = "egress-tests-targets.svc.cluster.local";

    pub(crate) fn certs(pem: &[u8]) -> Vec<CertificateDer<'static>> {
        rustls_pemfile::certs(&mut &pem[..])
            .collect::<Result<_, _>>()
            .unwrap()
    }

    pub(crate) fn roots_of(pem: &[u8]) -> RootCertStore {
        let mut roots = RootCertStore::empty();
        for c in certs(pem) {
            roots.add(c).unwrap();
        }
        roots
    }

    fn verify(ca: &[u8], cert: &[u8], name: &str, at: SystemTime) -> Result<(), rustls::Error> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let v = WebPkiServerVerifier::builder_with_provider(Arc::new(roots_of(ca)), provider)
            .build()
            .unwrap();
        let leaf = certs(cert).remove(0);
        let name = ServerName::try_from(name.to_string()).unwrap();
        let at = UnixTime::since_unix_epoch(at.duration_since(SystemTime::UNIX_EPOCH).unwrap());
        v.verify_server_cert(&leaf, &[], &name, &[], at).map(|_| ())
    }

    fn is_name_error(e: &rustls::Error) -> bool {
        matches!(
            e,
            rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. }
            )
        )
    }

    #[test]
    fn certificate_covers_every_name_under_the_domain() {
        let now = SystemTime::now();
        let b = generate(now, DOMAIN).unwrap();
        for name in [
            format!("egress-target-0.{DOMAIN}"),
            format!("egress-target-1023.{DOMAIN}"),
            format!("x.{DOMAIN}"),
        ] {
            verify(&b.ca, &b.cert, &name, now).unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }

    #[test]
    fn certificate_rejects_other_names() {
        let now = SystemTime::now();
        let b = generate(now, DOMAIN).unwrap();
        for name in [
            format!("a.egress-target-0.{DOMAIN}"), // a wildcard covers one label only
            "egress-target-0.other-namespace.svc.cluster.local".to_string(),
            "example.com".to_string(),
        ] {
            let err = verify(&b.ca, &b.cert, &name, now).unwrap_err();
            assert!(is_name_error(&err), "{name}: {err:?}");
        }
    }

    #[test]
    fn certificate_validity_window() {
        let now = SystemTime::now();
        let b = generate(now, DOMAIN).unwrap();
        let name = format!("egress-target-0.{DOMAIN}");
        verify(&b.ca, &b.cert, &name, now - Duration::from_secs(1800))
            .expect("inside the clock-skew allowance");
        for (what, at) in [
            ("before the skew allowance", now - Duration::from_secs(7200)),
            ("after expiry", now + VALIDITY + Duration::from_secs(3600)),
        ] {
            let err = verify(&b.ca, &b.cert, &name, at).unwrap_err();
            assert!(
                matches!(
                    err,
                    rustls::Error::InvalidCertificate(
                        CertificateError::Expired
                            | CertificateError::ExpiredContext { .. }
                            | CertificateError::NotValidYet
                            | CertificateError::NotValidYetContext { .. }
                    )
                ),
                "{what}: {err:?}"
            );
        }
    }

    // The gateway trusts this CA for every upstream it dials, so the CA must
    // be unable to vouch for any name outside the endpoints' domain, even in
    // a certificate it signed itself.
    #[test]
    fn ca_refuses_names_outside_the_endpoint_domain() {
        let now = SystemTime::now();
        let (b, ca_params, ca_key) = generate_with_ca(now, DOMAIN).unwrap();
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let mut evil = CertificateParams::new(vec!["evil.example.com".to_string()]).unwrap();
        evil.distinguished_name.push(DnType::CommonName, "evil");
        evil.not_before = odt(now - Duration::from_secs(3600));
        evil.not_after = odt(now + Duration::from_secs(3600));
        evil.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let issuer = Issuer::from_params(&ca_params, &ca_key);
        let evil = evil.signed_by(&key, &issuer).unwrap();
        let err = verify(&b.ca, evil.pem().as_bytes(), "evil.example.com", now).unwrap_err();
        assert!(
            format!("{err:?}").contains("NameConstraint"),
            "a certificate for evil.example.com signed by the CA verified with {err:?}, want a name-constraint violation"
        );
    }

    #[test]
    fn write() {
        let b = generate(SystemTime::now(), DOMAIN).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("certs");
        b.write(&dir).unwrap();
        let cert = std::fs::read(dir.join("tls.crt")).unwrap();
        let key = std::fs::read(dir.join("tls.key")).unwrap();
        let chain = certs(&cert);
        let key = rustls_pemfile::private_key(&mut &key[..]).unwrap().unwrap();
        rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .expect("written certificate and key load as a pair");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.join("tls.key"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
        assert_eq!(std::fs::read(dir.join("ca.crt")).unwrap(), b.ca);
    }
}

//! Server certificates for intercepted hosts, minted by the egress gateway
//! so it can terminate TLS on an Actor's behalf.
//!
//! One key is generated per minter and reused for every host: the
//! certificates are short-lived and only ever presented to the gateway's own
//! Actors, which trust the minting CA.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use x509_parser::prelude::{FromDer, X509Certificate};

use crate::ca::{CertificateAuthority, LeafSpec};
use crate::error::{IdentityError, Result};
use crate::key::{KeyAlgorithm, PrivateKey};
use crate::pem;

/// Lifetime of a host certificate unless configured otherwise.
pub const DEFAULT_HOST_CERT_TTL: Duration = Duration::from_secs(15 * 60);

/// A minted certificate with its key, ready for a TLS server.
#[derive(Clone, PartialEq, Eq)]
pub struct MintedCert {
    /// The leaf followed by any intermediates, PEM.
    pub cert_chain_pem: String,
    /// The leaf's private key, PEM.
    pub private_key_pem: String,
    /// End of the leaf's validity.
    pub not_after: SystemTime,
    /// The leaf's serial, lowercase hex, for the issuance log.
    pub serial: String,
}

impl std::fmt::Debug for MintedCert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MintedCert")
            .field("not_after", &self.not_after)
            .field("serial", &self.serial)
            .finish_non_exhaustive()
    }
}

/// Mints server certificates for host names and IP addresses.
pub struct HostCertMinter {
    ttl: Duration,
    ca: Arc<dyn CertificateAuthority>,
    leaf_key: PrivateKey,
}

impl HostCertMinter {
    /// A minter signing with `ca`; a zero `ttl` means
    /// [`DEFAULT_HOST_CERT_TTL`].
    pub fn new(ca: Arc<dyn CertificateAuthority>, ttl: Duration) -> Result<Self> {
        Ok(HostCertMinter {
            ttl: if ttl.is_zero() {
                DEFAULT_HOST_CERT_TTL
            } else {
                ttl
            },
            ca,
            leaf_key: PrivateKey::generate(KeyAlgorithm::EcdsaP256)?,
        })
    }

    /// Mints a certificate naming `host`, a DNS name or an IP address.
    pub fn certificate(&self, host: &str, now: SystemTime) -> Result<MintedCert> {
        check_host(host)?;
        let mut leaf = LeafSpec::new(now - Duration::from_secs(5 * 60), now + self.ttl);
        leaf.server_auth = true;
        match host.parse::<IpAddr>() {
            Ok(ip) => leaf.ip_addresses.push(ip),
            Err(_) => leaf.dns_names.push(host.to_string()),
        }
        let chain = self.ca.sign(&leaf, &self.leaf_key.public_key())?;
        let (_, cert) = X509Certificate::from_der(&chain[0])
            .map_err(|e| IdentityError::Certificate(format!("parsing issued leaf: {e}")))?;
        let not_after = SystemTime::UNIX_EPOCH
            + Duration::from_secs(cert.validity().not_after.timestamp().max(0) as u64);
        let serial = cert.raw_serial_as_string().replace(':', "");
        tracing::info!(host, serial, "issued host certificate");
        Ok(MintedCert {
            cert_chain_pem: pem::encode_certificates(chain.iter().map(Vec::as_slice)),
            private_key_pem: self.leaf_key.to_pem(),
            not_after,
            serial,
        })
    }
}

/// Accepts DNS names (labels of letters, digits, `_` and inner `-`, up to
/// 253 characters, optional trailing dot) and IP addresses.
pub fn check_host(host: &str) -> Result<()> {
    if host.parse::<IpAddr>().is_ok() || is_dns_name(host) {
        Ok(())
    } else {
        Err(IdentityError::Refused(format!(
            "invalid host name {host:?}"
        )))
    }
}

fn is_dns_name(host: &str) -> bool {
    let host = host.strip_suffix('.').unwrap_or(host);
    !host.is_empty() && host.len() <= 253 && host.split('.').all(is_dns_label)
}

fn is_dns_label(label: &str) -> bool {
    let b = label.as_bytes();
    !b.is_empty()
        && b.len() <= 63
        && b.iter().enumerate().all(|(i, &c)| {
            c.is_ascii_alphanumeric() || c == b'_' || (c == b'-' && i > 0 && i < b.len() - 1)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ca::tests::test_pool;
    use x509_parser::extensions::GeneralName;

    #[test]
    fn hosts() {
        for ok in [
            "example.com",
            "example.com.",
            "a_b.example",
            "10.0.0.1",
            "::1",
            "x",
        ] {
            assert!(check_host(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            ".",
            "-a.com",
            "a-.com",
            "a..b",
            "a b",
            &"a".repeat(64),
            "a/b",
        ] {
            assert!(check_host(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn mints_for_names_and_addresses() {
        let minter = HostCertMinter::new(Arc::new(test_pool()), Duration::ZERO).unwrap();
        let now = SystemTime::now();
        let dns = minter.certificate("api.example.com", now).unwrap();
        let ders = pem::decode_certificates(dns.cert_chain_pem.as_bytes()).unwrap();
        let (_, cert) = X509Certificate::from_der(&ders[0]).unwrap();
        let san = cert.subject_alternative_name().unwrap().unwrap();
        assert!(matches!(
            san.value.general_names[0],
            GeneralName::DNSName("api.example.com")
        ));
        assert!(dns.not_after > now && dns.not_after <= now + DEFAULT_HOST_CERT_TTL);
        assert!(!dns.serial.is_empty());

        let ip = minter.certificate("10.1.2.3", now).unwrap();
        // One key serves every host.
        assert_eq!(ip.private_key_pem, dns.private_key_pem);
        let ders = pem::decode_certificates(ip.cert_chain_pem.as_bytes()).unwrap();
        let (_, cert) = X509Certificate::from_der(&ders[0]).unwrap();
        let san = cert.subject_alternative_name().unwrap().unwrap();
        assert!(
            matches!(san.value.general_names[0], GeneralName::IPAddress(b) if b == [10, 1, 2, 3])
        );
        assert!(matches!(
            minter.certificate("bad host", now),
            Err(IdentityError::Refused(_))
        ));
    }
}

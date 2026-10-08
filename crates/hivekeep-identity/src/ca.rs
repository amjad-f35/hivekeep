//! Certificate authorities: a pool of CAs, one of which signs, all of which
//! are trusted.

use std::fmt;
use std::net::IpAddr;
use std::time::{Duration, SystemTime};

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use rcgen::{
    BasicConstraints, CertificateParams, CustomExtension, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyUsagePurpose, SanType, SerialNumber,
    SubjectPublicKeyInfo,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::error::{IdentityError, Result};
use crate::key::{KeyAlgorithm, PrivateKey, PublicKey};
use crate::pem;

/// What a leaf certificate certifies. The issuer adds the key usage
/// (digital signature), `CA:FALSE`, a random serial and the authority key
/// identifier.
#[derive(Debug, Clone)]
pub struct LeafSpec {
    /// Start of validity.
    pub not_before: SystemTime,
    /// End of validity.
    pub not_after: SystemTime,
    /// Subject common name, if any.
    pub common_name: Option<String>,
    /// URI SANs, e.g. SPIFFE IDs.
    pub uris: Vec<String>,
    /// DNS SANs.
    pub dns_names: Vec<String>,
    /// IP SANs.
    pub ip_addresses: Vec<IpAddr>,
    /// Usable for TLS servers.
    pub server_auth: bool,
    /// Usable for TLS clients.
    pub client_auth: bool,
    /// Extra extensions.
    pub extensions: Vec<CustomExtension>,
}

impl LeafSpec {
    /// A spec valid over `[not_before, not_after]` with no names.
    pub fn new(not_before: SystemTime, not_after: SystemTime) -> Self {
        LeafSpec {
            not_before,
            not_after,
            common_name: None,
            uris: Vec::new(),
            dns_names: Vec::new(),
            ip_addresses: Vec::new(),
            server_auth: false,
            client_auth: false,
            extensions: Vec::new(),
        }
    }

    fn params(&self) -> Result<CertificateParams> {
        let cert_err = |e: rcgen::Error| IdentityError::Certificate(e.to_string());
        let mut p = CertificateParams::default();
        p.not_before = to_offset(self.not_before)?;
        p.not_after = to_offset(self.not_after)?;
        p.serial_number = Some(random_serial());
        p.distinguished_name = DistinguishedName::new();
        if let Some(cn) = &self.common_name {
            p.distinguished_name.push(DnType::CommonName, cn.clone());
        }
        for uri in &self.uris {
            p.subject_alt_names
                .push(SanType::URI(uri.clone().try_into().map_err(cert_err)?));
        }
        for dns in &self.dns_names {
            p.subject_alt_names
                .push(SanType::DnsName(dns.clone().try_into().map_err(cert_err)?));
        }
        for ip in &self.ip_addresses {
            p.subject_alt_names.push(SanType::IpAddress(*ip));
        }
        p.is_ca = IsCa::ExplicitNoCa;
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        if self.client_auth {
            p.extended_key_usages
                .push(ExtendedKeyUsagePurpose::ClientAuth);
        }
        if self.server_auth {
            p.extended_key_usages
                .push(ExtendedKeyUsagePurpose::ServerAuth);
        }
        p.use_authority_key_identifier_extension = true;
        p.custom_extensions = self.extensions.clone();
        Ok(p)
    }
}

/// Something that issues leaf certificates.
pub trait CertificateAuthority: Send + Sync {
    /// Signs a certificate for `subject` and returns the chain, leaf first,
    /// in DER.
    fn sign(&self, leaf: &LeafSpec, subject: &PublicKey) -> Result<Vec<Vec<u8>>>;

    /// The root certificates of every CA, in DER.
    fn trust_anchors(&self) -> Result<Vec<Vec<u8>>>;

    /// The trust anchors as a PEM bundle.
    fn trust_bundle_pem(&self) -> Result<String> {
        Ok(pem::encode_certificates(
            self.trust_anchors()?.iter().map(Vec::as_slice),
        ))
    }
}

/// One CA: a self-signed root and its key. There are no intermediates.
#[derive(Clone)]
pub struct Ca {
    id: String,
    key: PrivateKey,
    root_der: Vec<u8>,
}

impl fmt::Debug for Ca {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ca")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Ca {
    /// Generates a CA whose root is valid from now for `validity`.
    pub fn generate(
        id: impl Into<String>,
        algorithm: KeyAlgorithm,
        validity: Duration,
    ) -> Result<Self> {
        let key = PrivateKey::generate(algorithm)?;
        let now = SystemTime::now();
        let mut p = CertificateParams::default();
        p.not_before = to_offset(now)?;
        p.not_after = to_offset(now + validity)?;
        p.serial_number = Some(random_serial());
        // A random subject keeps a root from ever looking like the issuer of
        // a different root.
        p.distinguished_name = DistinguishedName::new();
        p.distinguished_name.push(
            DnType::CommonName,
            uuid::Uuid::new_v4().simple().to_string(),
        );
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        p.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
        ];
        let cert = p
            .self_signed(key.key_pair())
            .map_err(|e| IdentityError::Certificate(format!("self-signing root: {e}")))?;
        Self::from_parts(id, key, cert.der().to_vec())
    }

    /// A CA from its key and root certificate.
    pub fn from_parts(id: impl Into<String>, key: PrivateKey, root_der: Vec<u8>) -> Result<Self> {
        let (_, root) = X509Certificate::from_der(&root_der)
            .map_err(|e| IdentityError::Certificate(format!("parsing root: {e}")))?;
        if root.public_key().raw != key.public_key().spki_der() {
            return Err(IdentityError::Pool(
                "CA key does not match its root certificate".into(),
            ));
        }
        Ok(Ca {
            id: id.into(),
            key,
            root_der,
        })
    }

    /// The CA's id within its pool.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The root certificate, DER.
    pub fn root_der(&self) -> &[u8] {
        &self.root_der
    }

    /// The root certificate, PEM.
    pub fn root_pem(&self) -> String {
        pem::encode(pem::CERTIFICATE, &self.root_der)
    }

    /// The signing key, PEM.
    pub fn key_pem(&self) -> String {
        self.key.to_pem()
    }

    /// Signs a leaf for `subject`.
    pub fn sign(&self, leaf: &LeafSpec, subject: &PublicKey) -> Result<Vec<u8>> {
        let issuer =
            Issuer::from_ca_cert_der(&self.root_der.as_slice().into(), self.key.key_pair())
                .map_err(|e| IdentityError::Certificate(format!("loading CA {}: {e}", self.id)))?;
        let spki = SubjectPublicKeyInfo::from_der(subject.spki_der())
            .map_err(|e| IdentityError::Certificate(format!("subject key: {e}")))?;
        let cert = leaf
            .params()?
            .signed_by(&spki, &issuer)
            .map_err(|e| IdentityError::Certificate(format!("signing: {e}")))?;
        Ok(cert.der().to_vec())
    }
}

/// A set of CAs, one active for signing. Every CA's root stays trusted, so
/// a new CA can be added (trusted first), activated, and the old one removed
/// once nothing it signed is still valid.
#[derive(Debug, Clone, Default)]
pub struct CaPool {
    cas: Vec<Ca>,
    active_for_signing: String,
}

#[derive(Serialize, Deserialize)]
struct WirePool {
    #[serde(rename = "CAs", default)]
    cas: Vec<WireCa>,
    #[serde(rename = "ActiveForSigning", default)]
    active_for_signing: String,
}

#[derive(Serialize, Deserialize)]
struct WireCa {
    #[serde(rename = "ID")]
    id: String,
    #[serde(rename = "SigningKeyPKCS8", with = "b64")]
    signing_key_pkcs8: Vec<u8>,
    #[serde(rename = "RootCertificateDER", with = "b64")]
    root_certificate_der: Vec<u8>,
}

pub(crate) mod b64 {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        STANDARD.decode(s).map_err(serde::de::Error::custom)
    }
}

impl CaPool {
    /// A pool of `cas`, signing with `active_for_signing` (or the first CA
    /// when empty).
    pub fn new(cas: Vec<Ca>, active_for_signing: impl Into<String>) -> Self {
        CaPool {
            cas,
            active_for_signing: active_for_signing.into(),
        }
    }

    /// The CAs.
    pub fn cas(&self) -> &[Ca] {
        &self.cas
    }

    /// The CA that signs.
    pub fn active(&self) -> Result<&Ca> {
        if self.active_for_signing.is_empty() {
            return self
                .cas
                .first()
                .ok_or_else(|| IdentityError::Pool("pool has no CAs".into()));
        }
        self.cas
            .iter()
            .find(|c| c.id == self.active_for_signing)
            .ok_or_else(|| {
                IdentityError::Pool(format!(
                    "active CA {:?} is not in the pool",
                    self.active_for_signing
                ))
            })
    }

    /// Serializes the pool, keys included, as its JSON state file.
    pub fn to_json(&self) -> Result<Vec<u8>> {
        let wire = WirePool {
            cas: self
                .cas
                .iter()
                .map(|c| WireCa {
                    id: c.id.clone(),
                    signing_key_pkcs8: c.key.pkcs8_der().to_vec(),
                    root_certificate_der: c.root_der.clone(),
                })
                .collect(),
            active_for_signing: self.active_for_signing.clone(),
        };
        serde_json::to_vec(&wire).map_err(|e| IdentityError::Pool(e.to_string()))
    }

    /// Parses a JSON state file.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        let wire: WirePool = serde_json::from_slice(bytes)
            .map_err(|e| IdentityError::Pool(format!("parsing CA pool: {e}")))?;
        let cas = wire
            .cas
            .into_iter()
            .map(|c| {
                Ca::from_parts(
                    c.id,
                    PrivateKey::from_pkcs8_der(&c.signing_key_pkcs8)?,
                    c.root_certificate_der,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(CaPool {
            cas,
            active_for_signing: wire.active_for_signing,
        })
    }
}

impl CertificateAuthority for CaPool {
    fn sign(&self, leaf: &LeafSpec, subject: &PublicKey) -> Result<Vec<Vec<u8>>> {
        Ok(vec![self.active()?.sign(leaf, subject)?])
    }

    fn trust_anchors(&self) -> Result<Vec<Vec<u8>>> {
        Ok(self.cas.iter().map(|c| c.root_der.clone()).collect())
    }
}

fn to_offset(t: SystemTime) -> Result<OffsetDateTime> {
    let secs = t
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| IdentityError::Certificate("time before 1970".into()))?
        .as_secs();
    OffsetDateTime::from_unix_timestamp(secs as i64)
        .map_err(|e| IdentityError::Certificate(e.to_string()))
}

/// A random positive 128-bit serial.
fn random_serial() -> SerialNumber {
    let mut b = *uuid::Uuid::new_v4().as_bytes();
    b[0] &= 0x7f;
    if b[0] == 0 {
        b[0] = 1;
    }
    SerialNumber::from_slice(&b)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use x509_parser::extensions::GeneralName;

    pub(crate) fn test_pool() -> CaPool {
        CaPool::new(
            vec![Ca::generate("ca-1", KeyAlgorithm::EcdsaP256, Duration::from_secs(3600)).unwrap()],
            "",
        )
    }

    #[test]
    fn signs_leaves_that_chain_to_the_root() {
        for alg in [KeyAlgorithm::Ed25519, KeyAlgorithm::EcdsaP256] {
            let ca = Ca::generate("ca", alg, Duration::from_secs(3600)).unwrap();
            let subject = PrivateKey::generate(KeyAlgorithm::EcdsaP256)
                .unwrap()
                .public_key();
            let now = SystemTime::now();
            let mut leaf = LeafSpec::new(now, now + Duration::from_secs(60));
            leaf.uris.push("spiffe://example.org/x".into());
            leaf.client_auth = true;
            let der = ca.sign(&leaf, &subject).unwrap();
            let (_, cert) = X509Certificate::from_der(&der).unwrap();
            let (_, root) = X509Certificate::from_der(ca.root_der()).unwrap();
            cert.verify_signature(Some(root.public_key())).unwrap();
            assert_eq!(cert.issuer(), root.subject());
            assert_eq!(cert.public_key().raw, subject.spki_der());
            assert!(!cert.is_ca());
            let san = cert.subject_alternative_name().unwrap().unwrap();
            assert!(matches!(
                san.value.general_names[0],
                GeneralName::URI("spiffe://example.org/x")
            ));
            assert!(root.is_ca());
        }
    }

    #[test]
    fn pool_round_trips_and_selects() {
        let a = Ca::generate("a", KeyAlgorithm::EcdsaP256, Duration::from_secs(3600)).unwrap();
        let b = Ca::generate("b", KeyAlgorithm::Ed25519, Duration::from_secs(3600)).unwrap();
        let pool = CaPool::new(vec![a.clone(), b.clone()], "b");
        let again = CaPool::from_json(&pool.to_json().unwrap()).unwrap();
        assert_eq!(again.active().unwrap().id(), "b");
        assert_eq!(
            again.trust_anchors().unwrap(),
            vec![a.root_der().to_vec(), b.root_der().to_vec()]
        );
        assert_eq!(CaPool::new(vec![a.clone()], "").active().unwrap().id(), "a");
        assert!(CaPool::new(vec![a], "missing").active().is_err());
        assert!(CaPool::default().active().is_err());
        let bundle = again.trust_bundle_pem().unwrap();
        assert_eq!(
            pem::decode_certificates(bundle.as_bytes()).unwrap().len(),
            2
        );
    }

    #[test]
    fn rejects_mismatched_parts() {
        let a = Ca::generate("a", KeyAlgorithm::EcdsaP256, Duration::from_secs(3600)).unwrap();
        let other = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
        assert!(Ca::from_parts("a", other, a.root_der().to_vec()).is_err());
        assert!(CaPool::from_json(b"{").is_err());
    }
}

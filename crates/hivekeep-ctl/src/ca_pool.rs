//! CA pools for signing controllers: self-signed CAs and the serialized pool
//! format the controllers load.
//!
//! The pool is JSON: `{"CAs": [{"ID", "SigningKeyPKCS8", "RootCertificateDER"}],
//! "ActiveForSigning"}`, with binary fields in standard base64.

use std::time::Duration;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose};
use serde::Serialize;

/// Key algorithm of a CA.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaKeyType {
    /// ECDSA over P-256; the most broadly accepted by external relying parties.
    EcdsaP256,
    /// Ed25519.
    Ed25519,
}

impl std::str::FromStr for CaKeyType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "ECDSAP256" => Ok(CaKeyType::EcdsaP256),
            "ED25519" => Ok(CaKeyType::Ed25519),
            other => Err(format!("unknown key type {other:?}")),
        }
    }
}

/// A CA: its signing key and self-signed root certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ca {
    /// The CA's ID within its pool.
    pub id: String,
    /// PKCS#8 DER of the signing key.
    pub key_pkcs8: Vec<u8>,
    /// DER of the root certificate.
    pub cert_der: Vec<u8>,
}

fn pem(tag: &str, der: &[u8]) -> Vec<u8> {
    let b64 = STANDARD.encode(der);
    let mut out = format!("-----BEGIN {tag}-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).unwrap_or_default());
        out.push('\n');
    }
    out.push_str(&format!("-----END {tag}-----\n"));
    out.into_bytes()
}

impl Ca {
    /// The root certificate in PEM, as TLS servers expect it.
    pub fn cert_pem(&self) -> Vec<u8> {
        pem("CERTIFICATE", &self.cert_der)
    }

    /// The signing key in PKCS#8 PEM, as TLS servers expect it.
    pub fn key_pem(&self) -> Vec<u8> {
        pem("PRIVATE KEY", &self.key_pkcs8)
    }
}

/// Generates a CA with a fresh key, valid from now for `validity`. The
/// subject carries random text so that no two CAs share one.
pub fn generate_ca(id: &str, key_type: CaKeyType, validity: Duration) -> Result<Ca, String> {
    let alg = match key_type {
        CaKeyType::EcdsaP256 => &rcgen::PKCS_ECDSA_P256_SHA256,
        CaKeyType::Ed25519 => &rcgen::PKCS_ED25519,
    };
    let key = KeyPair::generate_for(alg).map_err(|e| format!("while generating root key: {e}"))?;
    let mut params = CertificateParams::default();
    params.distinguished_name.push(
        DnType::CommonName,
        uuid::Uuid::new_v4().simple().to_string(),
    );
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
    ];
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now;
    params.not_after =
        now + time::Duration::try_from(validity).map_err(|e| format!("invalid validity: {e}"))?;
    let cert = params
        .self_signed(&key)
        .map_err(|e| format!("while generating root certificate: {e}"))?;
    Ok(Ca {
        id: id.to_string(),
        key_pkcs8: key.serialize_der(),
        cert_der: cert.der().to_vec(),
    })
}

#[derive(Serialize)]
struct WirePool<'a> {
    #[serde(rename = "CAs")]
    cas: Vec<WireCa<'a>>,
    #[serde(rename = "ActiveForSigning")]
    active_for_signing: &'a str,
}

#[derive(Serialize)]
struct WireCa<'a> {
    #[serde(rename = "ID")]
    id: &'a str,
    #[serde(rename = "SigningKeyPKCS8")]
    key: String,
    #[serde(rename = "RootCertificateDER")]
    cert: String,
}

/// Serializes a pool of `cas`, with `active` the one that signs.
pub fn marshal_pool(cas: &[Ca], active: &str) -> Vec<u8> {
    let wire = WirePool {
        cas: cas
            .iter()
            .map(|c| WireCa {
                id: &c.id,
                key: STANDARD.encode(&c.key_pkcs8),
                cert: STANDARD.encode(&c.cert_der),
            })
            .collect(),
        active_for_signing: active,
    };
    serde_json::to_vec(&wire).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_types() {
        assert_eq!("ECDSAP256".parse::<CaKeyType>(), Ok(CaKeyType::EcdsaP256));
        assert_eq!("ED25519".parse::<CaKeyType>(), Ok(CaKeyType::Ed25519));
        assert!("RSA"
            .parse::<CaKeyType>()
            .unwrap_err()
            .contains("unknown key type"));
    }

    #[test]
    fn generated_ca_is_a_ca() {
        for kt in [CaKeyType::EcdsaP256, CaKeyType::Ed25519] {
            let ca = generate_ca("ca-1", kt, Duration::from_secs(3600)).unwrap();
            let (_, cert) = x509_parser::parse_x509_certificate(&ca.cert_der).unwrap();
            assert!(cert.is_ca());
            let usage = cert.key_usage().unwrap().unwrap().value;
            assert!(usage.key_cert_sign() && usage.digital_signature());
            let lifetime =
                cert.validity().not_after.timestamp() - cert.validity().not_before.timestamp();
            assert_eq!(lifetime, 3600);
            assert!(ca.cert_pem().starts_with(b"-----BEGIN CERTIFICATE-----\n"));
            assert!(ca.key_pem().starts_with(b"-----BEGIN PRIVATE KEY-----\n"));
            KeyPair::try_from(ca.key_pkcs8.as_slice()).unwrap();
        }
    }

    #[test]
    fn pool_format() {
        let ca = Ca {
            id: "ca-1".into(),
            key_pkcs8: vec![1, 2],
            cert_der: vec![3],
        };
        let v: serde_json::Value = serde_json::from_slice(&marshal_pool(&[ca], "ca-1")).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "CAs": [{"ID": "ca-1", "SigningKeyPKCS8": "AQI=", "RootCertificateDER": "Aw=="}],
                "ActiveForSigning": "ca-1"
            })
        );
    }
}

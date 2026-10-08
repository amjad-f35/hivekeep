//! Private and public keys.

use std::fmt;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rcgen::{KeyPair, PublicKeyData, RsaKeySize};
use sha2::{Digest, Sha256};
use x509_parser::prelude::FromDer;
use x509_parser::public_key::PublicKey as ParsedKey;
use x509_parser::x509::SubjectPublicKeyInfo;

use crate::error::{IdentityError, Result};
use crate::pem;

const OID_EC_PUBLIC_KEY: &str = "1.2.840.10045.2.1";
const OID_P256: &str = "1.2.840.10045.3.1.7";
const OID_ED25519: &str = "1.3.101.112";
const OID_RSA: &str = "1.2.840.113549.1.1.1";

/// Key types this crate generates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyAlgorithm {
    /// Ed25519.
    Ed25519,
    /// ECDSA on P-256.
    EcdsaP256,
    /// RSA, 2048-bit modulus.
    Rsa2048,
    /// RSA, 4096-bit modulus.
    Rsa4096,
}

/// The family of a key, as found in its encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyKind {
    /// Ed25519.
    Ed25519,
    /// ECDSA on P-256.
    EcdsaP256,
    /// RSA of any size.
    Rsa,
    /// Anything else (other curves, DSA, ...).
    Other,
}

/// A private key, held as PKCS#8 and usable for signing.
pub struct PrivateKey {
    pkcs8: Vec<u8>,
    pair: KeyPair,
    kind: KeyKind,
}

impl fmt::Debug for PrivateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrivateKey")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl Clone for PrivateKey {
    fn clone(&self) -> Self {
        Self::from_pkcs8_der(&self.pkcs8).expect("a parsed key re-parses")
    }
}

impl PrivateKey {
    /// Generates a fresh key.
    pub fn generate(algorithm: KeyAlgorithm) -> Result<Self> {
        let pair = match algorithm {
            KeyAlgorithm::Ed25519 => KeyPair::generate_for(&rcgen::PKCS_ED25519),
            KeyAlgorithm::EcdsaP256 => KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256),
            KeyAlgorithm::Rsa2048 => {
                KeyPair::generate_rsa_for(&rcgen::PKCS_RSA_SHA256, RsaKeySize::_2048)
            }
            KeyAlgorithm::Rsa4096 => {
                KeyPair::generate_rsa_for(&rcgen::PKCS_RSA_SHA256, RsaKeySize::_4096)
            }
        }
        .map_err(|e| IdentityError::Key(format!("generating {algorithm:?} key: {e}")))?;
        Self::from_pkcs8_der(&pair.serialize_der())
    }

    /// Parses a PKCS#8 DER private key.
    pub fn from_pkcs8_der(der: &[u8]) -> Result<Self> {
        let pair = KeyPair::try_from(der)
            .map_err(|e| IdentityError::Key(format!("parsing PKCS#8 key: {e}")))?;
        let kind = PublicKey::from_spki_der(pair.subject_public_key_info())?.kind();
        Ok(PrivateKey {
            pkcs8: der.to_vec(),
            pair,
            kind,
        })
    }

    /// Parses the first `PRIVATE KEY` block of `pem`.
    pub fn from_pem(input: &[u8]) -> Result<Self> {
        let block = pem::decode_all(input)?
            .into_iter()
            .find(|b| b.label == pem::PRIVATE_KEY)
            .ok_or_else(|| IdentityError::Key("no PRIVATE KEY block".into()))?;
        Self::from_pkcs8_der(&block.der)
    }

    /// The PKCS#8 DER encoding.
    pub fn pkcs8_der(&self) -> &[u8] {
        &self.pkcs8
    }

    /// The key as a `PRIVATE KEY` PEM block.
    pub fn to_pem(&self) -> String {
        pem::encode(pem::PRIVATE_KEY, &self.pkcs8)
    }

    /// The key's family.
    pub fn kind(&self) -> KeyKind {
        self.kind
    }

    /// The matching public key.
    pub fn public_key(&self) -> PublicKey {
        PublicKey {
            spki: self.pair.subject_public_key_info(),
            kind: self.kind,
        }
    }

    pub(crate) fn key_pair(&self) -> &KeyPair {
        &self.pair
    }
}

/// A public key, held as its SubjectPublicKeyInfo DER.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PublicKey {
    spki: Vec<u8>,
    kind: KeyKind,
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublicKey")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// The parts of a public key that a JWK publishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyComponents {
    /// An RSA modulus and exponent, big-endian without leading zeros.
    Rsa {
        /// Modulus.
        n: Vec<u8>,
        /// Public exponent.
        e: Vec<u8>,
    },
    /// The affine coordinates of a P-256 point.
    P256 {
        /// X coordinate (32 bytes).
        x: Vec<u8>,
        /// Y coordinate (32 bytes).
        y: Vec<u8>,
    },
    /// An Ed25519 public key (32 bytes).
    Ed25519(Vec<u8>),
}

fn strip_leading_zeros(b: &[u8]) -> Vec<u8> {
    let i = b
        .iter()
        .position(|&x| x != 0)
        .unwrap_or(b.len().saturating_sub(1));
    b[i..].to_vec()
}

impl PublicKey {
    /// Parses SubjectPublicKeyInfo DER.
    pub fn from_spki_der(der: impl Into<Vec<u8>>) -> Result<Self> {
        let spki = der.into();
        let kind = {
            let (_, info) = SubjectPublicKeyInfo::from_der(&spki)
                .map_err(|e| IdentityError::Key(format!("parsing public key: {e}")))?;
            let alg = info.algorithm.algorithm.to_id_string();
            let curve = info
                .algorithm
                .parameters
                .as_ref()
                .and_then(|p| p.as_oid().ok())
                .map(|o| o.to_id_string());
            match (alg.as_str(), curve.as_deref()) {
                (OID_RSA, _) => KeyKind::Rsa,
                (OID_EC_PUBLIC_KEY, Some(OID_P256)) => KeyKind::EcdsaP256,
                (OID_ED25519, _) => KeyKind::Ed25519,
                _ => KeyKind::Other,
            }
        };
        Ok(PublicKey { spki, kind })
    }

    /// The SubjectPublicKeyInfo DER.
    pub fn spki_der(&self) -> &[u8] {
        &self.spki
    }

    /// The key's family.
    pub fn kind(&self) -> KeyKind {
        self.kind
    }

    /// The key's components, for publishing as a JWK.
    pub fn components(&self) -> Result<KeyComponents> {
        let (_, info) = SubjectPublicKeyInfo::from_der(&self.spki)
            .map_err(|e| IdentityError::Key(format!("parsing public key: {e}")))?;
        match self.kind {
            KeyKind::Ed25519 => Ok(KeyComponents::Ed25519(
                info.subject_public_key.data.to_vec(),
            )),
            KeyKind::Rsa | KeyKind::EcdsaP256 => match info.parsed() {
                Ok(ParsedKey::RSA(k)) => Ok(KeyComponents::Rsa {
                    n: strip_leading_zeros(k.modulus),
                    e: strip_leading_zeros(k.exponent),
                }),
                Ok(ParsedKey::EC(p)) if p.data().len() == 65 && p.data()[0] == 4 => {
                    Ok(KeyComponents::P256 {
                        x: p.data()[1..33].to_vec(),
                        y: p.data()[33..65].to_vec(),
                    })
                }
                _ => Err(IdentityError::Key("malformed public key".into())),
            },
            KeyKind::Other => Err(IdentityError::Key("unsupported public key type".into())),
        }
    }

    /// RFC 7638-style stable key id: base64url of the SHA-256 of the
    /// SubjectPublicKeyInfo DER. Defined for RSA and P-256 keys.
    pub fn thumbprint(&self) -> Result<String> {
        match self.kind {
            KeyKind::Rsa | KeyKind::EcdsaP256 => {
                Ok(URL_SAFE_NO_PAD.encode(Sha256::digest(&self.spki)))
            }
            other => Err(IdentityError::Key(format!(
                "no thumbprint for {other:?} keys"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_keys_round_trip() {
        for alg in [
            KeyAlgorithm::Ed25519,
            KeyAlgorithm::EcdsaP256,
            KeyAlgorithm::Rsa2048,
        ] {
            let k = PrivateKey::generate(alg).unwrap();
            let again = PrivateKey::from_pem(k.to_pem().as_bytes()).unwrap();
            assert_eq!(again.public_key(), k.public_key(), "{alg:?}");
            assert_eq!(again.kind(), k.kind());
        }
    }

    #[test]
    fn kinds_and_components() {
        let ec = PrivateKey::generate(KeyAlgorithm::EcdsaP256)
            .unwrap()
            .public_key();
        assert_eq!(ec.kind(), KeyKind::EcdsaP256);
        assert!(
            matches!(ec.components().unwrap(), KeyComponents::P256 { x, y } if x.len() == 32 && y.len() == 32)
        );

        let rsa = PrivateKey::generate(KeyAlgorithm::Rsa2048)
            .unwrap()
            .public_key();
        assert_eq!(rsa.kind(), KeyKind::Rsa);
        match rsa.components().unwrap() {
            KeyComponents::Rsa { n, e } => {
                assert_eq!(n.len(), 256);
                assert_eq!(e, vec![1, 0, 1]);
            }
            other => panic!("{other:?}"),
        }

        let ed = PrivateKey::generate(KeyAlgorithm::Ed25519)
            .unwrap()
            .public_key();
        assert_eq!(ed.kind(), KeyKind::Ed25519);
        assert!(ed.thumbprint().is_err());
    }

    #[test]
    fn thumbprints_are_stable_and_distinct() {
        let a = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
        let b = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
        let ta = a.public_key().thumbprint().unwrap();
        assert_eq!(ta, a.clone().public_key().thumbprint().unwrap());
        assert_ne!(ta, b.public_key().thumbprint().unwrap());
        assert_eq!(ta.len(), 43);
    }

    #[test]
    fn rejects_garbage() {
        assert!(PrivateKey::from_pkcs8_der(b"nope").is_err());
        assert!(PrivateKey::from_pem(b"").is_err());
        assert!(PublicKey::from_spki_der(b"nope".to_vec()).is_err());
    }
}

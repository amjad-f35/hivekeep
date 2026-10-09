//! Credential bundles: one file holding a `PRIVATE KEY` block followed by
//! `CERTIFICATE` blocks (leaf first), as the kubelet writes Pod
//! certificates.

use std::path::Path;

use crate::error::{IdentityError, Result};
use crate::key::PrivateKey;
use crate::pem;

use x509_parser::prelude::{FromDer, X509Certificate};

/// A key and the certificate chain for it.
#[derive(Debug, Clone)]
pub struct CredentialBundle {
    /// The leaf's private key.
    pub key: PrivateKey,
    /// The chain, leaf first, DER.
    pub chain: Vec<Vec<u8>>,
}

impl CredentialBundle {
    /// Parses a bundle. Any block other than one `PRIVATE KEY` and the
    /// `CERTIFICATE`s is an error, as is a leaf that is not a certificate.
    pub fn parse(input: &[u8]) -> Result<Self> {
        let mut key = None;
        let mut chain = Vec::new();
        for block in pem::decode_all(input)? {
            match block.label.as_str() {
                pem::CERTIFICATE => chain.push(block.der),
                pem::PRIVATE_KEY => key = Some(block.der),
                other => {
                    return Err(IdentityError::Pem(format!(
                        "unknown PEM block type {other:?}"
                    )))
                }
            }
        }
        let key = key.ok_or_else(|| IdentityError::Pem("no PRIVATE KEY block found".into()))?;
        if chain.is_empty() {
            return Err(IdentityError::Pem("no CERTIFICATE blocks found".into()));
        }
        let key = PrivateKey::from_pkcs8_der(&key)?;
        X509Certificate::from_der(&chain[0])
            .map_err(|e| IdentityError::Certificate(format!("parsing leaf certificate: {e}")))?;
        Ok(CredentialBundle { key, chain })
    }

    /// Reads and parses a bundle file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let bytes = std::fs::read(path).map_err(|source| IdentityError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::parse(&bytes)
    }
}

/// Reads a trust bundle file into DER certificates. An empty bundle is an
/// error: it would silently reject every peer.
pub fn load_trust_anchors(path: impl AsRef<Path>) -> Result<Vec<Vec<u8>>> {
    let path = path.as_ref();
    let bytes = std::fs::read(path).map_err(|source| IdentityError::Io {
        path: path.display().to_string(),
        source,
    })?;
    let certs = pem::decode_certificates(&bytes)?;
    if certs.is_empty() {
        return Err(IdentityError::TrustBundle(format!(
            "trust bundle {} contains no certificates",
            path.display()
        )));
    }
    Ok(certs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::KeyAlgorithm;

    fn leaf_der(key: &PrivateKey) -> Vec<u8> {
        rcgen::CertificateParams::new(vec!["leaf.example".to_string()])
            .unwrap()
            .self_signed(key.key_pair())
            .unwrap()
            .der()
            .to_vec()
    }

    #[test]
    fn parses_key_and_chain() {
        let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
        let leaf = leaf_der(&key);
        let input = format!(
            "{}{}{}",
            key.to_pem(),
            pem::encode(pem::CERTIFICATE, &leaf),
            pem::encode(pem::CERTIFICATE, b"root")
        );
        let b = CredentialBundle::parse(input.as_bytes()).unwrap();
        assert_eq!(b.chain, vec![leaf, b"root".to_vec()]);
        assert_eq!(b.key.public_key(), key.public_key());
    }

    #[test]
    fn rejects_a_leaf_that_is_not_a_certificate() {
        let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
        let input = format!("{}{}", key.to_pem(), pem::encode(pem::CERTIFICATE, b"leaf"));
        let err = CredentialBundle::parse(input.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(err.contains("leaf certificate"), "{err}");
    }

    #[test]
    fn rejects_incomplete_bundles() {
        let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256)
            .unwrap()
            .to_pem();
        let cert = pem::encode(pem::CERTIFICATE, b"leaf");
        assert!(CredentialBundle::parse(key.as_bytes()).is_err());
        assert!(CredentialBundle::parse(cert.as_bytes()).is_err());
        let odd = format!("{key}{cert}{}", pem::encode("RSA PRIVATE KEY", b"x"));
        assert!(CredentialBundle::parse(odd.as_bytes()).is_err());
    }

    #[test]
    fn trust_anchor_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca.pem");
        std::fs::write(&path, "nothing here").unwrap();
        assert!(load_trust_anchors(&path).is_err());
        std::fs::write(&path, pem::encode(pem::CERTIFICATE, b"root")).unwrap();
        assert_eq!(load_trust_anchors(&path).unwrap(), vec![b"root".to_vec()]);
    }
}

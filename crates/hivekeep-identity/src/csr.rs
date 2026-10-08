//! Certificate signing requests.

use x509_parser::certification_request::X509CertificationRequest;
use x509_parser::prelude::FromDer;

use crate::error::{IdentityError, Result};
use crate::key::PublicKey;

/// Largest CSR accepted, matching the API's limit.
pub const MAX_CSR_BYTES: usize = 16 * 1024;

/// Parses a DER CSR and checks its self-signature, proving the requester
/// holds the private key. Returns the key to certify; everything else the
/// CSR asks for is ignored, since the issuer decides the certificate's
/// contents.
pub fn verified_public_key(csr_der: &[u8]) -> Result<PublicKey> {
    if csr_der.len() > MAX_CSR_BYTES {
        return Err(IdentityError::Csr(format!(
            "larger than {MAX_CSR_BYTES} bytes"
        )));
    }
    let (rest, csr) = X509CertificationRequest::from_der(csr_der)
        .map_err(|e| IdentityError::Csr(format!("parsing: {e}")))?;
    if !rest.is_empty() {
        return Err(IdentityError::Csr("trailing data".into()));
    }
    csr.verify_signature()
        .map_err(|e| IdentityError::Csr(format!("verifying signature: {e}")))?;
    PublicKey::from_spki_der(csr.certification_request_info.subject_pki.raw.to_vec())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::key::{KeyAlgorithm, PrivateKey};

    /// A CSR for `key`, as a workload would send it.
    pub(crate) fn csr_for(key: &PrivateKey) -> Vec<u8> {
        let params = rcgen::CertificateParams::new(vec!["ignored.example".to_string()]).unwrap();
        params
            .serialize_request(key.key_pair())
            .unwrap()
            .der()
            .to_vec()
    }

    #[test]
    fn accepts_a_signed_request() {
        for alg in [
            KeyAlgorithm::EcdsaP256,
            KeyAlgorithm::Ed25519,
            KeyAlgorithm::Rsa2048,
        ] {
            let key = PrivateKey::generate(alg).unwrap();
            let got = verified_public_key(&csr_for(&key)).unwrap();
            assert_eq!(got, key.public_key(), "{alg:?}");
        }
    }

    #[test]
    fn rejects_tampering_and_junk() {
        let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
        let mut csr = csr_for(&key);
        // Flip a byte inside the signed subject alternative name.
        let i = csr.windows(7).position(|w| w == b"ignored").unwrap();
        csr[i] ^= 1;
        assert!(verified_public_key(&csr).is_err());
        assert!(verified_public_key(b"junk").is_err());
        assert!(verified_public_key(&vec![0u8; MAX_CSR_BYTES + 1]).is_err());
    }
}

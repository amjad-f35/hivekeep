//! The subject public key of a request.

use hivekeep_identity::csr;
use hivekeep_identity::key::PublicKey;

use super::PodCertificateRequest;
use crate::error::SignerError;

/// The key a request asks to certify: from the stub PKCS#10 request when
/// present (its self-signature is checked; nothing else in it is used),
/// else from the legacy PKIX public key field.
pub fn public_key(pcr: &PodCertificateRequest) -> Result<PublicKey, SignerError> {
    let stub = &pcr.spec.stub_pkc_s10_request.0;
    if !stub.is_empty() {
        return csr::verified_public_key(stub).map_err(SignerError::StubRequest);
    }
    match pcr.spec.pkix_public_key.as_ref() {
        Some(pkix) if !pkix.0.is_empty() => {
            PublicKey::from_spki_der(pkix.0.clone()).map_err(SignerError::PkixPublicKey)
        }
        _ => Err(SignerError::NoPublicKey),
    }
}

#[cfg(test)]
mod tests {
    use k8s_openapi::ByteString;

    use super::*;
    use crate::test_support::{spki, stub_csr, subject_key};

    #[test]
    fn public_key_from_stub_pkcs10_request() {
        let key = subject_key();
        let mut pcr = PodCertificateRequest::default();
        pcr.spec.stub_pkc_s10_request = ByteString(stub_csr(&key));
        let got = public_key(&pcr).unwrap();
        assert_eq!(got.spki_der(), spki(&key));
    }

    #[test]
    fn public_key_falls_back_to_pkix_public_key() {
        let key = subject_key();
        let mut pcr = PodCertificateRequest::default();
        pcr.spec.pkix_public_key = Some(ByteString(spki(&key)));
        let got = public_key(&pcr).unwrap();
        assert_eq!(got.spki_der(), spki(&key));
    }

    #[test]
    fn public_key_requires_key_material() {
        assert!(matches!(
            public_key(&PodCertificateRequest::default()),
            Err(SignerError::NoPublicKey)
        ));
    }

    #[test]
    fn public_key_rejects_malformed_material() {
        let mut pcr = PodCertificateRequest::default();
        pcr.spec.stub_pkc_s10_request = ByteString(b"invalid".to_vec());
        assert!(matches!(public_key(&pcr), Err(SignerError::StubRequest(_))));
        let mut pcr = PodCertificateRequest::default();
        pcr.spec.pkix_public_key = Some(ByteString(b"invalid".to_vec()));
        assert!(matches!(
            public_key(&pcr),
            Err(SignerError::PkixPublicKey(_))
        ));
    }
}

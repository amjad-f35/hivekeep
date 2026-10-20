//! Lifetime and timing shared by the signers' leaf certificates.

use std::time::{Duration, SystemTime};

use hivekeep_identity::ca::{CertificateAuthority, LeafSpec};
use hivekeep_identity::key::PublicKey;
use hivekeep_identity::pem;
use hivekeep_identity::pod_cert::{IssuedPodCertificate, MAX_POD_CERT_LIFETIME};

use crate::error::SignerError;
use crate::pod_certificate::PodCertificateRequest;

/// How far validity starts in the past, to tolerate clock skew.
pub const BACKDATE: Duration = Duration::from_secs(2 * 60);

/// How long before expiry the kubelet should start refreshing.
pub const REFRESH_BEFORE_EXPIRY: Duration = Duration::from_secs(30 * 60);

/// The lifetime the request allows, if it states one.
pub fn requested_lifetime(pcr: &PodCertificateRequest) -> Option<Duration> {
    pcr.spec
        .max_expiration_seconds
        .map(|s| Duration::from_secs(u64::try_from(s).unwrap_or(0)))
}

/// The lifetime to issue: what the request allows, capped at
/// [`MAX_POD_CERT_LIFETIME`] (which is also the default).
pub fn capped_lifetime(pcr: &PodCertificateRequest) -> Duration {
    requested_lifetime(pcr)
        .unwrap_or(MAX_POD_CERT_LIFETIME)
        .min(MAX_POD_CERT_LIFETIME)
}

/// Signs a leaf for `subject` valid for `lifetime` from [`BACKDATE`]
/// before `now`, after `customize` filled in its names and usages.
pub fn sign_leaf(
    ca: &dyn CertificateAuthority,
    subject: &PublicKey,
    lifetime: Duration,
    now: SystemTime,
    customize: impl FnOnce(&mut LeafSpec),
) -> Result<IssuedPodCertificate, SignerError> {
    let not_before = now - BACKDATE;
    let not_after = not_before + lifetime;
    let mut leaf = LeafSpec::new(not_before, not_after);
    customize(&mut leaf);
    let chain = ca.sign(&leaf, subject).map_err(SignerError::Sign)?;
    Ok(IssuedPodCertificate {
        cert_chain_pem: pem::encode_certificates(chain.iter().map(Vec::as_slice)),
        not_before,
        not_after,
        begin_refresh_at: not_after - REFRESH_BEFORE_EXPIRY,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_max(seconds: Option<i32>) -> PodCertificateRequest {
        let mut pcr = PodCertificateRequest::default();
        pcr.spec.max_expiration_seconds = seconds;
        pcr
    }

    #[test]
    fn lifetime_is_capped_and_defaulted() {
        let day = Duration::from_secs(86400);
        assert_eq!(capped_lifetime(&with_max(None)), day);
        assert_eq!(capped_lifetime(&with_max(Some(7 * 86400))), day);
        assert_eq!(
            capped_lifetime(&with_max(Some(3600))),
            Duration::from_secs(3600)
        );
        assert_eq!(
            requested_lifetime(&with_max(Some(-5))),
            Some(Duration::ZERO)
        );
    }
}

//! Workload certificates for Kubernetes Pods, issued in answer to Pod
//! certificate requests. They carry the Pod's SPIFFE ID in the cluster
//! trust domain and a [`PodIdentity`] extension naming the Pod, its service
//! account and its Node.

use std::time::{Duration, SystemTime};

use crate::ca::{CertificateAuthority, LeafSpec};
use crate::error::{IdentityError, Result};
use crate::extension::{IdentityExtension, PodIdentity};
use crate::key::PublicKey;
use crate::pem;

/// Signer name Pods request these certificates from.
pub const POD_IDENTITY_SIGNER: &str = "podidentity.podcert.hivekeep.io/identity";

/// Prefix of the ClusterTrustBundles that publish the signer's roots.
pub const POD_IDENTITY_TRUST_BUNDLE_PREFIX: &str = "podidentity.podcert.hivekeep.io:identity:";

/// Trust domain of Pod identities.
pub const CLUSTER_TRUST_DOMAIN: &str = "cluster.local";

/// Longest lifetime issued, whatever the request allows.
pub const MAX_POD_CERT_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);

const BACKDATE: Duration = Duration::from_secs(2 * 60);
const REFRESH_BEFORE_EXPIRY: Duration = Duration::from_secs(30 * 60);

/// An issued Pod certificate and its timing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedPodCertificate {
    /// The chain, leaf first, PEM.
    pub cert_chain_pem: String,
    /// Start of validity.
    pub not_before: SystemTime,
    /// End of validity.
    pub not_after: SystemTime,
    /// When the kubelet should start refreshing.
    pub begin_refresh_at: SystemTime,
}

/// The SPIFFE ID of a service account.
pub fn service_account_spiffe_id(namespace: &str, service_account: &str) -> String {
    format!("spiffe://{CLUSTER_TRUST_DOMAIN}/ns/{namespace}/sa/{service_account}")
}

/// Signs a certificate for `pod`'s key, valid for at most
/// `max_expiration` and never more than [`MAX_POD_CERT_LIFETIME`].
pub fn mint_pod_certificate(
    ca: &dyn CertificateAuthority,
    pod: &PodIdentity,
    subject: &PublicKey,
    max_expiration: Duration,
    now: SystemTime,
) -> Result<IssuedPodCertificate> {
    pod.validate()?;
    if max_expiration < REFRESH_BEFORE_EXPIRY {
        return Err(IdentityError::Refused(format!(
            "requested lifetime {max_expiration:?} is shorter than the refresh window"
        )));
    }
    let lifetime = max_expiration.min(MAX_POD_CERT_LIFETIME);
    let not_before = now - BACKDATE;
    let not_after = not_before + lifetime;
    let mut leaf = LeafSpec::new(not_before, not_after);
    leaf.common_name = Some(uuid::Uuid::new_v4().simple().to_string());
    leaf.uris.push(service_account_spiffe_id(
        &pod.namespace,
        &pod.service_account_name,
    ));
    leaf.client_auth = true;
    leaf.server_auth = true;
    leaf.extensions.push(pod.to_extension()?);
    let chain = ca.sign(&leaf, subject)?;
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
    use crate::ca::tests::test_pool;
    use crate::key::{KeyAlgorithm, PrivateKey};

    fn pod() -> PodIdentity {
        PodIdentity {
            namespace: "ns".into(),
            service_account_name: "sa".into(),
            service_account_uid: "sa-uid".into(),
            pod_name: "p".into(),
            pod_uid: "p-uid".into(),
            node_name: "n".into(),
            node_uid: "n-uid".into(),
        }
    }

    #[test]
    fn caps_lifetime_and_carries_the_pod() {
        let pool = test_pool();
        let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256)
            .unwrap()
            .public_key();
        let now = SystemTime::now();
        let long =
            mint_pod_certificate(&pool, &pod(), &key, Duration::from_secs(7 * 86400), now).unwrap();
        assert_eq!(
            long.not_after.duration_since(long.not_before).unwrap(),
            MAX_POD_CERT_LIFETIME
        );
        let short =
            mint_pod_certificate(&pool, &pod(), &key, Duration::from_secs(3600), now).unwrap();
        assert_eq!(
            short.not_after.duration_since(short.not_before).unwrap(),
            Duration::from_secs(3600)
        );
        assert_eq!(
            short.begin_refresh_at,
            short.not_after - Duration::from_secs(1800)
        );
        let der = &pem::decode_certificates(short.cert_chain_pem.as_bytes()).unwrap()[0];
        assert_eq!(PodIdentity::from_certificate(der).unwrap(), Some(pod()));
        assert_eq!(
            service_account_spiffe_id("ns", "sa"),
            "spiffe://cluster.local/ns/ns/sa/sa"
        );
    }

    #[test]
    fn refuses_incomplete_pods_and_tiny_lifetimes() {
        let pool = test_pool();
        let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256)
            .unwrap()
            .public_key();
        let now = SystemTime::now();
        let mut p = pod();
        p.node_uid.clear();
        assert!(mint_pod_certificate(&pool, &p, &key, Duration::from_secs(3600), now).is_err());
        assert!(mint_pod_certificate(&pool, &pod(), &key, Duration::from_secs(60), now).is_err());
    }
}

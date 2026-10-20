//! `podidentity.podcert.hivekeep.io/identity`: certificates equivalent to
//! service account tokens, backed by a local CA pool.

use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_identity::ca::CertificateAuthority;
use hivekeep_identity::extension::PodIdentity;
use hivekeep_identity::pod_cert::{self, MAX_POD_CERT_LIFETIME};
use hivekeep_k8s::cluster_trust_bundle::ClusterTrustBundle;
use hivekeep_node::clock::SharedClock;

use crate::error::SignerError;
use crate::pod_certificate::{public_key, PcrClient, PodCertificateRequest};
use crate::signer::issue::requested_lifetime;
use crate::signer::status::with_issued;
use crate::signer::trust_bundle::primary_bundle;
use crate::signer::SignerImpl;

/// The signer name.
pub const NAME: &str = pod_cert::POD_IDENTITY_SIGNER;

/// Prefix of the signer's ClusterTrustBundles.
pub const CTB_PREFIX: &str = pod_cert::POD_IDENTITY_TRUST_BUNDLE_PREFIX;

/// Issues SPIFFE certificates for the requesting Pod's service account.
pub struct PodIdentitySigner {
    ca: Arc<dyn CertificateAuthority>,
    pcrs: Arc<PcrClient>,
    clock: SharedClock,
}

impl PodIdentitySigner {
    /// A signer issuing from `ca` and answering through `pcrs`.
    pub fn new(
        ca: Arc<dyn CertificateAuthority>,
        pcrs: Arc<PcrClient>,
        clock: SharedClock,
    ) -> Self {
        PodIdentitySigner { ca, pcrs, clock }
    }
}

/// The identity a request names. It comes from the request alone:
/// kube-apiserver attests it for kubelet-created requests, and anyone else
/// needs permission to create requests and may name any identity (which is
/// how clients other than the kubelet obtain certificates).
pub fn pod_identity(pcr: &PodCertificateRequest) -> PodIdentity {
    let spec = &pcr.spec;
    PodIdentity {
        namespace: pcr.metadata.namespace.clone().unwrap_or_default(),
        service_account_name: spec.service_account_name.clone(),
        service_account_uid: spec.service_account_uid.clone(),
        pod_name: spec.pod_name.clone(),
        pod_uid: spec.pod_uid.clone(),
        node_name: spec.node_name.clone(),
        node_uid: spec.node_uid.clone(),
    }
}

#[async_trait]
impl SignerImpl for PodIdentitySigner {
    fn signer_name(&self) -> &str {
        NAME
    }

    fn desired_cluster_trust_bundles(&self) -> Result<Vec<ClusterTrustBundle>, SignerError> {
        Ok(vec![primary_bundle(CTB_PREFIX, NAME, self.ca.as_ref())?])
    }

    async fn make_cert(&self, pcr: &PodCertificateRequest) -> Result<(), SignerError> {
        let subject = public_key(pcr)?;
        let now = self.clock.now();
        let issued = pod_cert::mint_pod_certificate(
            self.ca.as_ref(),
            &pod_identity(pcr),
            &subject,
            requested_lifetime(pcr).unwrap_or(MAX_POD_CERT_LIFETIME),
            now.into(),
        )
        .map_err(SignerError::Sign)?;
        self.pcrs
            .update_status(&with_issued(pcr, &issued, now))
            .await
            .map_err(|e| SignerError::UpdateStatus(Box::new(e)))?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "pod_identity_signer_tests.rs"]
mod tests;

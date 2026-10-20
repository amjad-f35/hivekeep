//! What every signer provides, and the controller that drives one.

pub mod controller;
pub mod issue;
pub mod status;
pub mod trust_bundle;

use async_trait::async_trait;
use hivekeep_k8s::cluster_trust_bundle::ClusterTrustBundle;

use crate::error::SignerError;
use crate::pod_certificate::PodCertificateRequest;

/// One signer: its name, the trust bundles publishing its roots, and how
/// it answers a request.
#[async_trait]
pub trait SignerImpl: Send + Sync {
    /// The signer name requests address.
    fn signer_name(&self) -> &str;

    /// The ClusterTrustBundles that should exist for this signer.
    fn desired_cluster_trust_bundles(&self) -> Result<Vec<ClusterTrustBundle>, SignerError>;

    /// Issues (or denies) a certificate for `pcr` and records it in the
    /// request's status. An error leaves the request unanswered, to be
    /// retried.
    async fn make_cert(&self, pcr: &PodCertificateRequest) -> Result<(), SignerError>;
}

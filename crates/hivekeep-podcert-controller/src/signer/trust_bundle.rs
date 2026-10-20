//! The ClusterTrustBundle publishing a signer's roots.

use hivekeep_identity::ca::CertificateAuthority;
use hivekeep_k8s::cluster_trust_bundle::ClusterTrustBundle;
use k8s_openapi::api::certificates::v1beta1::ClusterTrustBundleSpec;

use crate::error::SignerError;

/// Label marking which bundles consumers should trust.
pub const CANARYING_LABEL: &str = "podcert.hivekeep.io/canarying";

/// [`CANARYING_LABEL`] value of bundles in use.
pub const CANARYING_LIVE: &str = "live";

/// Name suffix of a signer's only bundle.
pub const PRIMARY_BUNDLE: &str = "primary-bundle";

/// The bundle `<prefix>primary-bundle` for `signer`, holding every root of
/// `ca` (PEM, in pool order) so certificates from any CA in the pool,
/// including one being rotated in or out, verify.
pub fn primary_bundle(
    prefix: &str,
    signer: &str,
    ca: &dyn CertificateAuthority,
) -> Result<ClusterTrustBundle, SignerError> {
    let mut bundle = ClusterTrustBundle {
        spec: ClusterTrustBundleSpec {
            signer_name: Some(signer.into()),
            trust_bundle: ca.trust_bundle_pem().map_err(SignerError::TrustAnchors)?,
        },
        ..Default::default()
    };
    bundle.metadata.name = Some(format!("{prefix}{PRIMARY_BUNDLE}"));
    bundle.metadata.labels =
        Some([(CANARYING_LABEL.to_string(), CANARYING_LIVE.to_string())].into());
    Ok(bundle)
}

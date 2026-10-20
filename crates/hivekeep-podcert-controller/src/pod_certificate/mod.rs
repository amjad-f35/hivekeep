//! PodCertificateRequests: access through the served API version, and the
//! subject key a request asks to certify.

pub mod client;
pub mod public_key;

pub use client::PcrClient;
pub use public_key::public_key;

/// The in-memory representation of a request, whatever version is served.
/// The beta type is used because it still carries the legacy PKIX key.
pub type PodCertificateRequest = k8s_openapi::api::certificates::v1beta1::PodCertificateRequest;

/// Condition type of an answered request.
pub const CONDITION_ISSUED: &str = "Issued";
/// Condition type of a refused request.
pub const CONDITION_DENIED: &str = "Denied";
/// Condition type of a request the signer gave up on.
pub const CONDITION_FAILED: &str = "Failed";
/// Denial reason for requests whose user annotations are unacceptable.
pub const REASON_INVALID_USER_CONFIG: &str = "InvalidUserConfig";

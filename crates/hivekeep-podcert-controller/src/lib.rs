//! Pod certificate controller for Hivekeep.
//!
//! Watches PodCertificateRequests and answers those addressed to three
//! signers, each backed by its own local CA pool:
//!
//! * [`service_dns_signer`]: serving certificates naming the Services that
//!   select the requesting Pod;
//! * [`pod_identity_signer`]: certificates carrying the Pod's service
//!   account SPIFFE ID and a Pod identity extension;
//! * [`postgres_signer`]: PostgreSQL client-login certificates for the
//!   control plane's service account.
//!
//! Every signer runs in a [`signer::controller::Controller`], which also
//! publishes the signer's roots as a ClusterTrustBundle. Replicas split the
//! work with lease-based rendezvous hashing ([`rendezvous`]).

pub mod cli;
pub mod error;
pub mod pod_certificate;
pub mod rendezvous;
pub mod signer;

#[cfg(test)]
pub(crate) mod test_support;

pub use error::SignerError;

//! Identity for Hivekeep Actors and the components acting for them.
//!
//! * X.509: a [`ca::CaPool`] of self-signed CAs mints short-lived SPIFFE
//!   certificates from verified CSRs ([`actor_cert`]), server certificates
//!   for intercepted hosts ([`host_cert`]) and Pod workload certificates
//!   ([`pod_cert`]). Minted identities also travel in private extensions
//!   ([`extension`]).
//! * JWT: a [`jwt::authority::JwtPool`] signs Actor and worker-for-actor
//!   tokens ([`jwt::claims`]), publishes its keys ([`jwt::jwks`], [`oidc`])
//!   and [`jwt::verifier::Verifier`] checks them.
//! * Trust: bundle normalization ([`pem`]), named bundles
//!   ([`trust_bundle`]) and credential bundle files ([`cred_bundle`]),
//!   cached against file status for per-handshake lookups ([`cred_loader`]).
//!
//! Pools load from state files and reload periodically ([`state_file`]),
//! so keys rotate without restarts. Everything here is offline: fetching
//! keys over HTTP and talking to Kubernetes are the callers' business.

pub mod ca;
pub mod csr;
pub mod error;
pub mod extension;
pub mod key;
pub mod pem;
pub mod state_file;

pub use error::{IdentityError, Result};

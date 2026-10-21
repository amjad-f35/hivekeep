//! The actor JWT identity provider: serves the OpenID Connect discovery
//! document and JWK set relying parties use to verify actor JWTs, built
//! from the authority pool hivekeep-api signs with.
//!
//! * [`server`]: the HTTP routes over an [`IdentityProvider`](hivekeep_identity::oidc::IdentityProvider).
//! * [`flags`]: required-flag checking.
//! * [`tls`]: a serving certificate re-read from its credential bundle.
//! * [`serve`]: the HTTPS accept loop with graceful shutdown.

pub mod flags;
pub mod serve;
pub mod server;
pub mod tls;

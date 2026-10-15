//! The egress gateway's policy enforcement point: authenticating Actor
//! tunnels, deciding requests against Actor egress policies, and
//! injecting credentials outside the sandbox.

pub mod actor_jwt;
pub mod api;
pub mod handler;
pub mod identity;
pub mod policy_cache;
pub mod provider;
#[cfg(test)]
pub(crate) mod test_support;

pub use api::{CredentialProvider, EgressApi};
pub use handler::{Handler, CONNECT_DENIED_BODY, DENIED_BODY, WEBSOCKET_DENIED_BODY};
pub use identity::RootsLoader;
pub use policy_cache::DEFAULT_POLICY_CACHE_TTL;
pub use provider::{dial_provider, provider_name, ProviderDialConfig, CREDENTIAL_URI_SCHEME};

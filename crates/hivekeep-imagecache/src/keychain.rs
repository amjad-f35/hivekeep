//! Where pull credentials come from.
//!
//! A keychain returns every credential worth trying for a repository, most
//! specific first; the store tries them in order and pulls anonymously when
//! there are none. On a node the keychain is the keeper's kubelet credential
//! provider [`Keychain`](hivekeep_node::credential_provider::Keychain), so
//! pulls authenticate exactly as the kubelet's would.

use async_trait::async_trait;
use hivekeep_node::context::Ctx;

use crate::auth::Credential;
use crate::error::{Error, Result};

/// A source of registry credentials.
#[async_trait]
pub trait Keychain: Send + Sync + 'static {
    /// Credentials to try for `repository` (`registry/path`), in order.
    /// Empty means pull anonymously.
    async fn candidates(&self, ctx: &Ctx, repository: &str) -> Result<Vec<Credential>>;
}

#[async_trait]
impl Keychain for hivekeep_node::credential_provider::Keychain {
    async fn candidates(&self, ctx: &Ctx, repository: &str) -> Result<Vec<Credential>> {
        let auths = hivekeep_node::credential_provider::Keychain::candidates(self, ctx, repository)
            .await
            .map_err(Error::msg)?;
        Ok(auths.into_iter().map(Credential::from).collect())
    }
}

/// The same credentials for every repository.
#[derive(Debug, Clone, Default)]
pub struct StaticKeychain(pub Vec<Credential>);

#[async_trait]
impl Keychain for StaticKeychain {
    async fn candidates(&self, _: &Ctx, _: &str) -> Result<Vec<Credential>> {
        Ok(self.0.clone())
    }
}

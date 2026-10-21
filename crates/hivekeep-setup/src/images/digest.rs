//! Looking up the digest a registry tag currently names.

use super::reference::ImageRef;
use super::registry::RegistryClient;
use super::ImageError;
use async_trait::async_trait;

/// Resolves a tagged reference to the digest it names (`sha256:...`).
/// [`super::Prebuilt`] takes one so tests can rewrite manifests without a
/// registry.
#[async_trait]
pub trait Digester: Send + Sync {
    async fn digest(&self, reference: &str) -> Result<String, ImageError>;
}

/// Asks the reference's registry which manifest the tag names: one HEAD
/// request needing only read access. A local registry is contacted over
/// plain HTTP, which is what a Kind cluster's local registry serves.
#[derive(Clone, Debug, Default)]
pub struct RemoteDigest;

/// [`RemoteDigest`] as a free function.
pub async fn remote_digest(reference: &str) -> Result<String, ImageError> {
    let parsed = ImageRef::parse(reference)?;
    let mut client = RegistryClient::new(parsed)?;
    client
        .head_digest()
        .await
        .map_err(|e| ImageError::Registry(format!("resolving {reference} to a digest: {e}")))
}

#[async_trait]
impl Digester for RemoteDigest {
    async fn digest(&self, reference: &str) -> Result<String, ImageError> {
        remote_digest(reference).await
    }
}

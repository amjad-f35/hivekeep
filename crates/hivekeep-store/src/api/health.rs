use async_trait::async_trait;

use crate::Result;

/// Readiness of the backend.
#[async_trait]
pub trait HealthCheck: Send + Sync {
    /// Succeeds when the backend can serve requests.
    async fn ping(&self) -> Result<()>;
}

use async_trait::async_trait;

use crate::{Lease, Result};

/// Distributed leases for leader-style exclusive work.
#[async_trait]
pub trait LeaseStore: Send + Sync {
    /// Acquires the lease on `key`, held and renewed until it is closed.
    /// [`LeaseConflict`](crate::StoreError::LeaseConflict) if anyone,
    /// including this caller, already holds it.
    async fn acquire_lease(&self, key: &str) -> Result<Lease>;
}

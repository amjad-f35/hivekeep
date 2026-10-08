//! The store contract against the in-memory backend.

mod contract;

use std::sync::Arc;

use contract::Fresh;
use hivekeep_store::MemoryStore;

async fn setup() -> Fresh {
    (Arc::new(MemoryStore::new()), Box::pin(async {}))
}

contract_tests!(crate::setup, allow(clippy::all));

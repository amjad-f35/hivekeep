use std::sync::Arc;

use hivekeep_store::*;

type S = Arc<dyn Store>;

pub async fn acquire(s: S) {
    let lease = s.acquire_lease("test-lease").await.unwrap();
    assert!(lease.is_held(), "new lease is already lost");
    assert_eq!(lease.key(), "test-lease");
    lease.close().await;
    assert!(!lease.is_held());
}

pub async fn conflict(s: S) {
    let lease = s.acquire_lease("test-lease").await.unwrap();
    assert!(matches!(
        s.acquire_lease("test-lease").await,
        Err(StoreError::LeaseConflict)
    ));
    // Other keys are independent.
    s.acquire_lease("other-lease").await.unwrap().close().await;
    lease.close().await;
}

/// The holder itself cannot take the lease again.
pub async fn non_reentrant(s: S) {
    let lease = s.acquire_lease("test-lease").await.unwrap();
    assert!(matches!(
        s.acquire_lease("test-lease").await,
        Err(StoreError::LeaseConflict)
    ));
    lease.close().await;
}

pub async fn close_releases(s: S) {
    s.acquire_lease("test-lease").await.unwrap().close().await;
    s.acquire_lease("test-lease").await.unwrap().close().await;
}

pub async fn close_idempotent(s: S) {
    let lease = s.acquire_lease("test-lease").await.unwrap();
    lease.close().await;
    lease.close().await;
    // Dropping a closed lease releases nothing twice.
    drop(lease);
    s.acquire_lease("test-lease").await.unwrap().close().await;
}

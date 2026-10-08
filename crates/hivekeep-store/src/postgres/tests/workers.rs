//! Guarded Worker writes inside assignment transactions.

use super::super::workers::save_worker;
use super::*;
use crate::api::WorkerStore;
use crate::{DeletePreconditions, Precondition, StoreError};

/// Writing back a Worker read before a concurrent update loses.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn save_worker_rejects_a_stale_write() {
    let db = fresh("save_stale").await;
    let s = &db.store;
    let created = s
        .create_worker(&worker("stale-write-worker", "pod"))
        .await
        .unwrap();
    s.update_worker("stale-write-worker", Precondition::of(&created), &mut |w| {
        w.labels = [("tier".to_string(), "batch".to_string())].into();
        Ok(())
    })
    .await
    .unwrap();

    let mut tx = db.pool().begin().await.unwrap();
    let mut stale = created.clone();
    assert!(matches!(
        save_worker(&mut tx, &mut stale).await,
        Err(StoreError::VersionConflict)
    ));
    tx.rollback().await.unwrap();
    db.finish().await;
}

/// Writing back a Worker deleted since it was read loses too.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn save_worker_rejects_a_vanished_worker() {
    let db = fresh("save_vanished").await;
    let s = &db.store;
    let created = s
        .create_worker(&worker("vanished-worker", "pod"))
        .await
        .unwrap();
    s.delete_worker("vanished-worker", DeletePreconditions::none())
        .await
        .unwrap();

    let mut tx = db.pool().begin().await.unwrap();
    let mut gone = created.clone();
    assert!(matches!(
        save_worker(&mut tx, &mut gone).await,
        Err(StoreError::VersionConflict)
    ));
    tx.rollback().await.unwrap();
    db.finish().await;
}

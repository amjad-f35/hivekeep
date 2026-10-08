//! Lease acquisition, expiry and cleanup.

use std::sync::Arc;
use std::time::Duration;

use super::super::lease::{cleanup_expired, CLEANUP_BATCH};
use super::*;
use crate::api::LeaseStore;
use crate::StoreError;

async fn count(pool: &PgPool, key: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM leases WHERE key = $1")
        .bind(key)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Acquiring one key leaves other keys' expired rows to maintenance rather
/// than adding a delete to every acquisition.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn acquire_leaves_other_keys_expired_rows() {
    let db = fresh("lease_other").await;
    exec(
        db.pool(),
        "INSERT INTO leases (key, token, expires_at) VALUES ('expired', 'old', clock_timestamp() - interval '1 minute')",
    )
    .await;
    let lease = db.store.acquire_lease("new").await.unwrap();
    assert_eq!(count(db.pool(), "expired").await, 1);
    lease.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn cleanup_removes_only_expired_rows() {
    let db = fresh("lease_cleanup").await;
    exec(
        db.pool(),
        "INSERT INTO leases (key, token, expires_at) VALUES \
         ('expired', 'old', clock_timestamp() - interval '1 minute'), \
         ('active', 'live', clock_timestamp() + interval '1 hour')",
    )
    .await;
    assert_eq!(cleanup_expired(db.pool()).await.unwrap(), 1);
    assert_eq!(count(db.pool(), "expired").await, 0);
    assert_eq!(count(db.pool(), "active").await, 1);
    db.finish().await;
}

/// One pass keeps deleting until no expired row is left, however many
/// batches that takes.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn cleanup_drains_across_batches() {
    let db = fresh("lease_batches").await;
    let seeded = CLEANUP_BATCH * 2 + 7;
    sqlx::query(
        "INSERT INTO leases (key, token, expires_at) \
         SELECT 'expired-' || i, 'old', clock_timestamp() - interval '1 minute' \
         FROM generate_series(1, $1) AS i",
    )
    .bind(seeded)
    .execute(db.pool())
    .await
    .unwrap();
    assert_eq!(cleanup_expired(db.pool()).await.unwrap(), seeded as u64);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM leases")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(left, 0);
    db.finish().await;
}

/// A row locked by someone else (a concurrent acquire reclaiming it, another
/// replica's pass) is skipped rather than waited on, and taken by the next
/// pass once free.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn cleanup_skips_locked_rows() {
    let db = fresh("lease_locked").await;
    exec(
        db.pool(),
        "INSERT INTO leases (key, token, expires_at) VALUES \
         ('locked', 'old', clock_timestamp() - interval '1 minute'), \
         ('free', 'old', clock_timestamp() - interval '1 minute')",
    )
    .await;
    let mut holder = db.pool().begin().await.unwrap();
    sqlx::query("SELECT 1 FROM leases WHERE key = 'locked' FOR UPDATE")
        .execute(&mut *holder)
        .await
        .unwrap();

    let deleted = tokio::time::timeout(PATIENCE, cleanup_expired(db.pool()))
        .await
        .expect("cleanup waited on a locked row")
        .unwrap();
    assert_eq!(deleted, 1);
    assert_eq!(count(db.pool(), "locked").await, 1);
    assert_eq!(count(db.pool(), "free").await, 0);

    holder.commit().await.unwrap();
    assert_eq!(cleanup_expired(db.pool()).await.unwrap(), 1);
    assert_eq!(count(db.pool(), "locked").await, 0);
    db.finish().await;
}

/// A holder that stops renewing without releasing (a process that went
/// away) loses the lease once its TTL runs out, and the key can be taken.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn acquire_after_holder_stops_and_lease_expires() {
    let db = fresh("lease_expiry").await;
    let short = db.store.clone().with_lease_ttl(Duration::from_millis(200));
    let lease = short.acquire_lease("test-lease").await.unwrap();
    // Stop renewal without releasing.
    lease.token().cancel();
    tokio::time::timeout(Duration::from_secs(1), lease.lost())
        .await
        .expect("lease not marked lost with its holder");
    assert!(!lease.is_held());

    tokio::time::sleep(Duration::from_millis(700)).await;
    let next = db.store.acquire_lease("test-lease").await.unwrap();
    next.close().await;
    drop(lease);
    db.finish().await;
}

/// Many contenders for an expired lease: exactly one takes it over.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn acquire_concurrent_takeover_has_one_winner() {
    let db = fresh("lease_takeover").await;
    let initial = db
        .store
        .clone()
        .with_lease_ttl(Duration::from_millis(1))
        .acquire_lease("contested-lease")
        .await
        .unwrap();
    initial.token().cancel();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let store = Arc::new(db.store.clone().with_lease_ttl(Duration::from_secs(10)));
    let racers: Vec<_> = (0..20)
        .map(|_| {
            let s = store.clone();
            tokio::spawn(async move { s.acquire_lease("contested-lease").await })
        })
        .collect();
    let mut winners = Vec::new();
    for r in racers {
        match r.await.unwrap() {
            // Held until every racer has tried, so later racers cannot win
            // one after another.
            Ok(lease) => winners.push(lease),
            Err(StoreError::LeaseConflict) => {}
            Err(e) => panic!("acquire failed: {e}"),
        }
    }
    assert_eq!(winners.len(), 1);
    for w in winners {
        w.close().await;
    }
    drop(initial);
    db.finish().await;
}

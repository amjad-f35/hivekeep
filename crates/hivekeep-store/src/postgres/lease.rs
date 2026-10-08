//! Leases as rows with an expiry, renewed in the background.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::codec::db;
use super::PgStore;
use crate::api::LeaseStore;
use crate::{Lease, Result, StoreError};

/// Expired leases removed per statement by maintenance.
pub(super) const CLEANUP_BATCH: i64 = 1000;

async fn try_acquire(pool: &PgPool, key: &str, token: &str, ttl: Duration) -> Result<bool> {
    let row = sqlx::query(
        "INSERT INTO leases (key, token, expires_at) \
         VALUES ($1, $2, clock_timestamp() + make_interval(secs => $3)) \
         ON CONFLICT (key) DO UPDATE SET token = EXCLUDED.token, expires_at = EXCLUDED.expires_at \
         WHERE leases.expires_at <= clock_timestamp() \
         RETURNING key",
    )
    .bind(key)
    .bind(token)
    .bind(ttl.as_secs_f64())
    .fetch_optional(pool)
    .await
    .map_err(db)?;
    Ok(row.is_some())
}

async fn renew(pool: &PgPool, key: &str, token: &str, ttl: Duration) -> Result<bool> {
    let row = sqlx::query(
        "UPDATE leases SET expires_at = clock_timestamp() + make_interval(secs => $3) \
         WHERE key = $1 AND token = $2 AND expires_at > clock_timestamp() RETURNING key",
    )
    .bind(key)
    .bind(token)
    .bind(ttl.as_secs_f64())
    .fetch_optional(pool)
    .await
    .map_err(db)?;
    Ok(row.is_some())
}

async fn release(pool: &PgPool, key: &str, token: &str) -> Result<()> {
    sqlx::query("DELETE FROM leases WHERE key = $1 AND token = $2")
        .bind(key)
        .bind(token)
        .execute(pool)
        .await
        .map_err(db)?;
    Ok(())
}

/// Removes expired leases. Returns how many went.
pub(super) async fn cleanup_expired(pool: &PgPool) -> Result<u64> {
    let mut total = 0;
    loop {
        let n = sqlx::query(
            "DELETE FROM leases WHERE key IN ( \
                 SELECT key FROM leases WHERE expires_at <= clock_timestamp() \
                 LIMIT $1 FOR UPDATE SKIP LOCKED)",
        )
        .bind(CLEANUP_BATCH)
        .execute(pool)
        .await
        .map_err(db)?
        .rows_affected();
        total += n;
        if (n as i64) < CLEANUP_BATCH {
            return Ok(total);
        }
    }
}

/// Renews every `ttl / 3`. A failed renewal is retried every `ttl / 10`
/// until two thirds of the TTL have passed since the last success, at which
/// point the lease is presumed lost.
async fn renew_loop(
    pool: PgPool,
    key: String,
    token: String,
    ttl: Duration,
    held: CancellationToken,
) {
    let interval = ttl / 3;
    let retry = ttl / 10;
    let deadline = ttl * 2 / 3;
    let mut last = Instant::now();
    loop {
        tokio::select! {
            _ = held.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        loop {
            match renew(&pool, &key, &token, ttl).await {
                Ok(true) => {
                    last = Instant::now();
                    break;
                }
                Ok(false) => {
                    tracing::warn!(key, "lease is no longer ours");
                    held.cancel();
                    return;
                }
                Err(e) => {
                    if last.elapsed() + retry >= deadline {
                        tracing::warn!(key, error = %e, "could not renew lease before its deadline");
                        held.cancel();
                        return;
                    }
                    tracing::warn!(key, error = %e, "lease renewal failed; retrying");
                    tokio::select! {
                        _ = held.cancelled() => return,
                        _ = tokio::time::sleep(retry) => {}
                    }
                }
            }
        }
    }
}

#[async_trait]
impl LeaseStore for PgStore {
    async fn acquire_lease(&self, key: &str) -> Result<Lease> {
        let token = uuid::Uuid::new_v4().to_string();
        let ttl = self.lease_ttl;
        if !try_acquire(&self.pool, key, &token, ttl).await? {
            return Err(StoreError::LeaseConflict);
        }
        let held = CancellationToken::new();
        let renewal = tokio::spawn(renew_loop(
            self.pool.clone(),
            key.to_string(),
            token.clone(),
            ttl,
            held.clone(),
        ));
        let pool = self.pool.clone();
        let owned_key = key.to_string();
        let release = async move {
            let _ = renewal.await;
            let r =
                tokio::time::timeout(Duration::from_secs(5), release(&pool, &owned_key, &token))
                    .await;
            if !matches!(r, Ok(Ok(()))) {
                tracing::warn!(key = owned_key, "could not release lease; it will expire");
            }
        };
        Ok(Lease::new(key, held, release))
    }
}

//! PostgreSQL [`Store`](crate::Store) backend.
//!
//! Records are stored as encoded messages next to their key and
//! `(uid, version)` columns. Optimistic writes are guarded in SQL; Worker
//! writes lock the Worker row and append to a transactional outbox that feeds
//! watches on every replica. Queries are built at run time, so no database is
//! needed to compile.
//!
//! A background task keeps the outbox within its retention period and
//! removes expired leases.

mod codec;
mod lease;
mod migrate;
mod outbox;
mod resources;
pub mod setup;
mod table;
mod workers;

#[cfg(test)]
mod tests;

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, Executor, PgPool, Postgres};
use tokio_util::sync::CancellationToken;

pub use migrate::MIGRATOR;

use crate::api::HealthCheck;
use crate::{Result, StoreError};
use codec::db;

/// How long an acquired lease lives without renewal.
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);
/// Pace of the maintenance task.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);
/// Connections reserved for watch polling.
const WATCH_POOL_MAX: u32 = 3;
/// Connections of the owner pool, used for schema changes.
const OWNER_POOL_MAX: u32 = 2;
/// Connections of the health-check pool. Nothing else borrows it, so a
/// saturated request pool cannot fail a readiness probe.
const PROBE_POOL_MAX: u32 = 1;
/// How long a health check waits for its connection.
const PROBE_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// Connection settings.
#[derive(Clone, Default)]
pub struct PgConfig {
    /// Connection URL of the read/write user, which serves requests.
    pub read_write_url: String,
    /// Connection URL of the schema owner, which creates the schema and runs
    /// migrations. Defaults to `read_write_url`.
    pub owner_url: Option<String>,
    /// Role assumed by read/write connections (`SET ROLE`), if any.
    pub read_write_role: Option<String>,
    /// Role assumed by owner connections, if any.
    pub owner_role: Option<String>,
    /// Schema holding the tables; it becomes every connection's search path.
    pub schema: String,
    /// Cap on read/write connections; the driver default when `None`.
    pub max_connections: Option<u32>,
}

impl PgConfig {
    /// Rejects settings that cannot work: an empty schema, an empty role
    /// name, or an empty owner connection URL.
    fn validate(&self) -> Result<()> {
        let invalid = |what: &str| {
            Err(StoreError::InvalidArgument(format!(
                "PostgreSQL {what} must not be empty"
            )))
        };
        if self.schema.is_empty() {
            return invalid("schema");
        }
        if self.read_write_role.as_deref() == Some("") {
            return invalid("read/write role");
        }
        if self.owner_role.as_deref() == Some("") {
            return invalid("owner role");
        }
        if self.owner_url.as_deref() == Some("") {
            return invalid("owner connection string");
        }
        Ok(())
    }
}

impl std::fmt::Debug for PgConfig {
    // URLs can carry passwords, so they are left out.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgConfig")
            .field("schema", &self.schema)
            .field("read_write_role", &self.read_write_role)
            .field("owner_role", &self.owner_role)
            .field("max_connections", &self.max_connections)
            .finish_non_exhaustive()
    }
}

/// A PostgreSQL-backed store. Cloning shares the pools.
#[derive(Clone)]
pub struct PgStore {
    pool: PgPool,
    watch_pool: PgPool,
    probe_pool: PgPool,
    lease_ttl: Duration,
    poll_failure_close_after: Duration,
    shutdown: CancellationToken,
    maintenance: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    _guard: Arc<ShutdownGuard>,
}

struct ShutdownGuard(CancellationToken);

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl std::fmt::Debug for PgStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgStore").finish_non_exhaustive()
    }
}

fn options(url: &str, schema: &str) -> Result<PgConnectOptions> {
    let opts = PgConnectOptions::from_str(url)
        .map_err(|_| StoreError::InvalidArgument("unparseable PostgreSQL connection URL".into()))?;
    Ok(if schema.is_empty() {
        opts
    } else {
        opts.options([("search_path", migrate::quote_ident(schema))])
    })
}

fn pool_options(role: Option<String>) -> PgPoolOptions {
    PgPoolOptions::new().after_connect(move |conn, _meta| {
        let role = role.clone();
        Box::pin(async move {
            if let Some(role) = role {
                conn.execute(AssertSqlSafe(format!(
                    "SET ROLE {}",
                    migrate::quote_ident(&role)
                )))
                .await?;
            }
            Ok(())
        })
    })
}

impl PgStore {
    /// Connects, creates the schema if needed, applies migrations and starts
    /// maintenance. [`StoreError::Unavailable`] if the server cannot be
    /// reached, so callers can retry.
    pub async fn connect(config: &PgConfig) -> Result<Self> {
        config.validate()?;
        let owner_url = config
            .owner_url
            .as_deref()
            .unwrap_or(&config.read_write_url);
        // The schema may not exist yet, so the owner first connects without
        // a search path.
        let bootstrap = pool_options(config.owner_role.clone())
            .max_connections(1)
            .connect_with(options(owner_url, "")?)
            .await
            .map_err(db)?;
        migrate::create_schema(&bootstrap, &config.schema).await?;
        bootstrap.close().await;

        let owner = pool_options(config.owner_role.clone())
            .max_connections(OWNER_POOL_MAX)
            .connect_with(options(owner_url, &config.schema)?)
            .await
            .map_err(db)?;
        migrate::migrate(&owner).await?;
        owner.close().await;

        let rw = options(&config.read_write_url, &config.schema)?;
        let mut pool_opts = pool_options(config.read_write_role.clone());
        if let Some(n) = config.max_connections {
            pool_opts = pool_opts.max_connections(n);
        }
        let pool = pool_opts.connect_with(rw.clone()).await.map_err(db)?;
        let watch_pool = pool_options(config.read_write_role.clone())
            .max_connections(WATCH_POOL_MAX)
            .connect_with(rw.clone())
            .await
            .map_err(db)?;
        // Dials on the first probe.
        let probe_pool = pool_options(config.read_write_role.clone())
            .max_connections(PROBE_POOL_MAX)
            .min_connections(0)
            .acquire_timeout(PROBE_ACQUIRE_TIMEOUT)
            .connect_lazy_with(rw);
        Ok(Self::start(pool, watch_pool, probe_pool))
    }

    /// Wraps an existing pool whose connections already use the right schema,
    /// applying migrations first. Used by tests and embedders that manage
    /// their own pool.
    pub async fn from_pool(pool: PgPool) -> Result<Self> {
        migrate::migrate(&pool).await?;
        Ok(Self::start(pool.clone(), pool.clone(), pool))
    }

    fn start(pool: PgPool, watch_pool: PgPool, probe_pool: PgPool) -> Self {
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(maintenance(pool.clone(), shutdown.clone()));
        PgStore {
            pool,
            watch_pool,
            probe_pool,
            lease_ttl: DEFAULT_LEASE_TTL,
            poll_failure_close_after: outbox::POLL_FAILURE_CLOSE_AFTER,
            shutdown: shutdown.clone(),
            maintenance: Arc::new(tokio::sync::Mutex::new(Some(task))),
            _guard: Arc::new(ShutdownGuard(shutdown)),
        }
    }

    /// Overrides the lease TTL (mainly for tests).
    pub fn with_lease_ttl(mut self, ttl: Duration) -> Self {
        self.lease_ttl = ttl;
        self
    }

    /// The read/write pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Stops maintenance and watches, waits for the maintenance task to
    /// exit, and closes the pools.
    pub async fn close(&self) {
        self.shutdown.cancel();
        if let Some(task) = self.maintenance.lock().await.take() {
            let _ = task.await;
        }
        self.watch_pool.close().await;
        self.probe_pool.close().await;
        self.pool.close().await;
    }

    async fn conn(&self) -> Result<PoolConnection<Postgres>> {
        self.pool.acquire().await.map_err(db)
    }
}

/// What one maintenance pass removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceReport {
    /// Worker events past retention.
    pub expired_events: u64,
    /// Expired leases.
    pub expired_leases: u64,
}

impl PgStore {
    /// Runs one maintenance pass now: trims the worker event outbox and
    /// removes expired leases. The background task does this every minute.
    pub async fn run_maintenance(&self) -> Result<MaintenanceReport> {
        maintain(&self.pool).await
    }
}

async fn maintain(pool: &PgPool) -> Result<MaintenanceReport> {
    let mut conn = pool.acquire().await.map_err(db)?;
    let expired_events = outbox::trim(&mut conn).await?;
    drop(conn);
    let expired_leases = lease::cleanup_expired(pool).await?;
    Ok(MaintenanceReport {
        expired_events,
        expired_leases,
    })
}

async fn maintenance(pool: PgPool, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(MAINTENANCE_INTERVAL) => {}
        }
        match maintain(&pool).await {
            Ok(r) if r.expired_leases > 0 => {
                tracing::info!(removed = r.expired_leases, "removed expired leases")
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "store maintenance failed"),
        }
    }
}

#[async_trait]
impl HealthCheck for PgStore {
    /// Answers over the dedicated probe connection, so it fails only when the
    /// database does not answer, never because requests hold every
    /// connection.
    async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.probe_pool)
            .await
            .map_err(db)?;
        Ok(())
    }
}

//! Tests of the PostgreSQL backend that need its internals or raw SQL access.
//!
//! Every test that touches a database is ignored by default: set
//! `HK_TEST_DATABASE_URL` to a database whose user may create schemas (and,
//! for the role and setup-script tests, roles), then run with `--ignored`.
//! Each test works in schemas of its own, dropped when it passes.

mod connect;
mod lease;
mod listing;
mod migrations;
mod outbox;
mod ping;
mod resources;
mod setup_script;
mod unknown_fields;
mod workers;

use std::time::Duration;

use hivekeep_proto::api::v1 as pb;
use sqlx::{AssertSqlSafe, Executor, PgPool};

use super::{migrate::quote_ident, PgConfig, PgStore};
use crate::api::HiveStore;

/// The test database's URL.
pub(super) fn url() -> String {
    std::env::var("HK_TEST_DATABASE_URL").expect("HK_TEST_DATABASE_URL must name a test database")
}

/// A schema name no other test uses.
pub(super) fn unique_schema(tag: &str) -> String {
    format!(
        "hk_test_{tag}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

/// Connection settings for `schema` with the test user.
pub(super) fn config(schema: &str) -> PgConfig {
    PgConfig {
        read_write_url: url(),
        schema: schema.into(),
        ..Default::default()
    }
}

/// A pool on the test database with no schema of its own.
pub(super) async fn admin() -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&url())
        .await
        .expect("connecting to the test database")
}

/// Runs `sql` (which may hold several statements) on `pool`.
pub(super) async fn exec(pool: &PgPool, sql: &str) {
    pool.execute(AssertSqlSafe(sql.to_string()))
        .await
        .unwrap_or_else(|e| panic!("running {sql:?}: {e}"));
}

/// Drops `schema` and everything in it.
pub(super) async fn drop_schema(schema: &str) {
    let admin = admin().await;
    exec(
        &admin,
        &format!("DROP SCHEMA IF EXISTS {} CASCADE", quote_ident(schema)),
    )
    .await;
    admin.close().await;
}

/// A connected store on a fresh schema.
pub(super) struct Db {
    pub store: PgStore,
    pub schema: String,
}

impl Db {
    /// The store's read/write pool.
    pub fn pool(&self) -> &PgPool {
        self.store.pool()
    }

    /// Closes the store and drops its schema.
    pub async fn finish(self) {
        self.store.close().await;
        drop_schema(&self.schema).await;
    }
}

/// Connects a store to a new schema.
pub(super) async fn fresh(tag: &str) -> Db {
    let schema = unique_schema(tag);
    let store = PgStore::connect(&config(&schema))
        .await
        .expect("connecting to the test database");
    Db { store, schema }
}

pub(super) fn md(hive: &str, name: &str) -> Option<pb::ResourceMetadata> {
    Some(pb::ResourceMetadata {
        hive: hive.into(),
        name: name.into(),
        ..Default::default()
    })
}

pub(super) async fn mk_hive(store: &PgStore, name: &str) {
    store
        .create_hive(&pb::Hive {
            metadata: md("", name),
        })
        .await
        .unwrap_or_else(|e| panic!("create_hive({name}): {e}"));
}

pub(super) fn suspended(hive: &str, name: &str) -> pb::Actor {
    pb::Actor {
        metadata: md(hive, name),
        status: Some(pb::ActorStatus {
            state: pb::ActorState::Suspended as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub(super) fn worker(name: &str, pod: &str) -> pb::Worker {
    pb::Worker {
        metadata: md("", name),
        worker_namespace: "ns".into(),
        worker_pool: "pool".into(),
        worker_pod: pod.into(),
        ..Default::default()
    }
}

/// Runs `fut` to completion from inside a synchronous store callback (a
/// `mutate` closure), which is how a test interleaves a concurrent write
/// with a read-modify-write in flight. Needs the multi-threaded runtime.
pub(super) fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
}

/// How long a test waits for something that should happen.
pub(super) const PATIENCE: Duration = Duration::from_secs(5);

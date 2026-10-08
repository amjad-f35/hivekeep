//! The store contract against PostgreSQL.
//!
//! Ignored by default: set `HK_TEST_DATABASE_URL` to a database whose user
//! may create schemas, then run with `--ignored`. Each test gets its own
//! schema, dropped when the test passes.

mod contract;

use std::sync::Arc;

use contract::Fresh;
use hivekeep_store::{PgConfig, PgStore};
use sqlx::{AssertSqlSafe, Executor};

async fn setup() -> Fresh {
    let url = std::env::var("HK_TEST_DATABASE_URL")
        .expect("HK_TEST_DATABASE_URL must name a test database");
    let schema = format!("hk_test_{}", uuid::Uuid::new_v4().simple());
    let store = PgStore::connect(&PgConfig {
        read_write_url: url.clone(),
        schema: schema.clone(),
        ..Default::default()
    })
    .await
    .expect("connecting to the test database");
    let handle = store.clone();
    let cleanup = Box::pin(async move {
        handle.close().await;
        let admin = sqlx::PgPool::connect(&url)
            .await
            .expect("reconnecting for cleanup");
        admin
            .execute(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
            .await
            .expect("dropping the test schema");
        admin.close().await;
    });
    (Arc::new(store), cleanup)
}

contract_tests!(crate::setup, ignore = "needs HK_TEST_DATABASE_URL");

/// Maintenance runs cleanly against a live schema, and a lease that expired
/// unrenewed can be taken over.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn maintenance_and_lease_expiry() {
    use hivekeep_store::LeaseStore;
    use std::time::Duration;

    let url = std::env::var("HK_TEST_DATABASE_URL").expect("HK_TEST_DATABASE_URL");
    let schema = format!("hk_test_{}", uuid::Uuid::new_v4().simple());
    let config = PgConfig {
        read_write_url: url.clone(),
        schema: schema.clone(),
        ..Default::default()
    };
    let store = PgStore::connect(&config).await.unwrap();
    let report = store.run_maintenance().await.unwrap();
    assert_eq!(report.expired_leases, 0);

    // A held lease survives several renewal periods.
    let short = store.clone().with_lease_ttl(Duration::from_millis(600));
    let lease = short.acquire_lease("k").await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(lease.is_held(), "renewal let the lease lapse");
    lease.close().await;
    // Once released, another holder gets it immediately.
    short.acquire_lease("k").await.unwrap().close().await;

    store.close().await;
    let admin = sqlx::PgPool::connect(&url).await.unwrap();
    admin
        .execute(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
        .await
        .unwrap();
}

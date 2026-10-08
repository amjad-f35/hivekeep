//! Schema migrations: authoring policy, concurrent startup, locking, and
//! schemas in unusual states.

use std::path::{Path, PathBuf};
use std::time::Duration;

use sqlx::migrate::{Migrate, Migrator};

use super::super::migrate::{migrate_with, LEDGER, MIGRATOR};
use super::super::options;
use super::*;

fn migrations_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations")
}

/// Every migration is forward-only, runs inside the migrator's transaction
/// and does not paper over drift with `IF NOT EXISTS`.
#[test]
fn migration_policy() {
    let mut checked = 0;
    for entry in std::fs::read_dir(migrations_dir()).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if !name.ends_with(".sql") {
            continue;
        }
        checked += 1;
        assert!(
            !name.ends_with(".down.sql") && !name.ends_with(".up.sql"),
            "{name}: migrations are forward-only"
        );
        let sql = std::fs::read_to_string(&path).unwrap();
        let upper = sql.to_uppercase();
        assert!(
            !upper.contains("-- NO-TRANSACTION"),
            "{name} must run in a transaction"
        );
        assert!(
            !upper.contains("IF NOT EXISTS"),
            "{name} must not contain an IF NOT EXISTS guard"
        );
        for line in upper.lines() {
            let stmt = line.trim_start();
            for control in ["BEGIN;", "START TRANSACTION", "COMMIT;", "ROLLBACK;"] {
                assert!(
                    !stmt.starts_with(control),
                    "{name} must leave the transaction to the migrator: {line}"
                );
            }
        }
    }
    assert!(checked > 0, "no migrations found");
    assert_eq!(checked, MIGRATOR.iter().count());
}

async fn schema_pool(schema: &str) -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options(&url(), schema).unwrap())
        .await
        .unwrap()
}

async fn applied_versions(pool: &PgPool) -> Vec<i64> {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT version FROM {LEDGER} WHERE success ORDER BY version"
    )))
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Two replicas starting at once on an empty schema apply each migration
/// exactly once.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn migrations_concurrent_startup() {
    let schema = unique_schema("concurrent_startup");
    let cfg = config(&schema);
    let (a, b) = tokio::join!(PgStore::connect(&cfg), PgStore::connect(&cfg));
    let (a, b) = (a.unwrap(), b.unwrap());
    let want: Vec<i64> = MIGRATOR.iter().map(|m| m.version).collect();
    assert_eq!(applied_versions(a.pool()).await, want);
    a.close().await;
    b.close().await;
    drop_schema(&schema).await;
}

/// A migration started while another holds the migration lock waits for it
/// instead of failing or running alongside.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn migrations_wait_for_in_progress_migration() {
    let schema = unique_schema("lock_wait");
    let admin = admin().await;
    exec(&admin, &format!("CREATE SCHEMA {}", quote_ident(&schema))).await;
    let pool = schema_pool(&schema).await;

    let mut holder = admin.acquire().await.unwrap();
    holder.lock().await.unwrap();
    let run = {
        let pool = pool.clone();
        tokio::spawn(async move { migrate_with(&pool, &MIGRATOR).await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !run.is_finished(),
        "migration returned while the migration lock was held"
    );
    holder.unlock().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("migration did not finish after the lock was released")
        .unwrap()
        .expect("migration failed after the lock was released");

    drop(holder);
    pool.close().await;
    admin.close().await;
    drop_schema(&schema).await;
}

/// A schema migrated by a newer build (a migration this build does not
/// know) still opens, so a rollback works.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn migration_schema_ahead_opens() {
    let schema = unique_schema("ahead");
    PgStore::connect(&config(&schema))
        .await
        .unwrap()
        .close()
        .await;
    let pool = schema_pool(&schema).await;
    exec(
        &pool,
        &format!(
            "INSERT INTO {LEDGER} (version, description, success, checksum, execution_time) \
             SELECT max(version) + 1, 'from a newer build', true, '\\x00'::bytea, 0 FROM {LEDGER}"
        ),
    )
    .await;
    pool.close().await;
    PgStore::connect(&config(&schema))
        .await
        .expect("connecting to a schema migrated ahead")
        .close()
        .await;
    drop_schema(&schema).await;
}

/// Tables without a migration ledger were not made by these migrations, so
/// the store refuses them and leaves the schema untouched.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn migration_tables_without_ledger_refused() {
    let schema = unique_schema("legacy");
    let admin = admin().await;
    let q = quote_ident(&schema);
    exec(
        &admin,
        &format!("CREATE SCHEMA {q}; CREATE TABLE {q}.hives (id integer)"),
    )
    .await;
    let err = PgStore::connect(&config(&schema)).await.unwrap_err();
    assert!(
        err.to_string().contains("without a migration ledger"),
        "{err}"
    );
    let ledger: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(format!("{q}.{LEDGER}"))
        .fetch_one(&admin)
        .await
        .unwrap();
    assert!(!ledger, "a ledger was created for an unmanaged schema");
    admin.close().await;
    drop_schema(&schema).await;
}

/// A failing migration leaves the ones before it applied and none of its
/// own statements; once fixed, migration resumes where it stopped.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn migration_failure_leaves_completed_prefix_and_resumes() {
    let schema = unique_schema("resume");
    let admin = admin().await;
    exec(&admin, &format!("CREATE SCHEMA {}", quote_ident(&schema))).await;
    let pool = schema_pool(&schema).await;
    let dir = std::env::temp_dir().join(&schema);
    std::fs::create_dir_all(&dir).unwrap();
    let write = |name: &str, sql: &str| std::fs::write(dir.join(name), sql).unwrap();

    write(
        "0001_create.sql",
        "CREATE TABLE resume_test (id integer PRIMARY KEY);",
    );
    migrate_with(&pool, &Migrator::new(dir.as_path()).await.unwrap())
        .await
        .unwrap();

    write(
        "0002_add_value.sql",
        "ALTER TABLE resume_test ADD COLUMN value text;",
    );
    write(
        "0003_fail.sql",
        "CREATE TABLE failed_transaction (id integer);\nALTER TABLE missing_table ADD COLUMN value text;",
    );
    let err = migrate_with(&pool, &Migrator::new(dir.as_path()).await.unwrap()).await;
    assert!(err.is_err(), "migration 3 should fail");
    assert_eq!(applied_versions(&pool).await, vec![1, 2]);
    let value_column: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = 'resume_test' AND column_name = 'value')",
    )
    .bind(&schema)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(value_column, "migration 2 was not left applied");
    let leaked: bool = sqlx::query_scalar("SELECT to_regclass('failed_transaction') IS NOT NULL")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!leaked, "SQL from the failed migration was committed");

    write(
        "0003_fail.sql",
        "CREATE TABLE resumed_migration (id integer);",
    );
    migrate_with(&pool, &Migrator::new(dir.as_path()).await.unwrap())
        .await
        .unwrap();
    assert_eq!(applied_versions(&pool).await, vec![1, 2, 3]);
    let resumed: bool = sqlx::query_scalar("SELECT to_regclass('resumed_migration') IS NOT NULL")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(resumed, "migration 3 was not applied after the fix");

    std::fs::remove_dir_all(&dir).unwrap();
    pool.close().await;
    admin.close().await;
    drop_schema(&schema).await;
}

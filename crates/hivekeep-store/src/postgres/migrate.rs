//! Schema creation and migrations.

use sqlx::migrate::Migrator;
use sqlx::{AssertSqlSafe, PgPool, Row};

use super::codec::db;
use crate::{Result, StoreError};

/// The embedded migrations under `migrations/`.
///
/// Applied migrations this build does not know are tolerated, so a replica
/// can be rolled back after a newer one has migrated the schema forward.
pub static MIGRATOR: Migrator = Migrator {
    ignore_missing: true,
    ..sqlx::migrate!("./migrations")
};

/// Ledger table the migrator keeps in the schema.
pub(super) const LEDGER: &str = "_sqlx_migrations";

/// Oldest supported server (`xid8`, `pg_current_snapshot`).
const MIN_SERVER_VERSION: i32 = 130000;

/// Creates `schema` if it does not exist, serialized across replicas.
pub(super) async fn create_schema(pool: &PgPool, schema: &str) -> Result<()> {
    let mut tx = pool.begin().await.map_err(db)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("hivekeep:create-schema:{schema}"))
        .execute(&mut *tx)
        .await
        .map_err(db)?;
    let exists: bool = sqlx::query("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
        .bind(schema)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?
        .try_get(0)
        .map_err(db)?;
    if !exists {
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            quote_ident(schema)
        )))
        .execute(&mut *tx)
        .await
        .map_err(db)?;
    }
    tx.commit().await.map_err(db)
}

/// Checks the server version and applies pending migrations.
pub(super) async fn migrate(pool: &PgPool) -> Result<()> {
    migrate_with(pool, &MIGRATOR).await
}

/// [`migrate`] with an explicit set of migrations.
pub(super) async fn migrate_with(pool: &PgPool, migrator: &Migrator) -> Result<()> {
    let version: i32 = sqlx::query("SELECT current_setting('server_version_num')::int")
        .fetch_one(pool)
        .await
        .map_err(db)?
        .try_get(0)
        .map_err(db)?;
    if version < MIN_SERVER_VERSION {
        return Err(StoreError::Unavailable(format!(
            "PostgreSQL 13 or newer is required; server_version_num is {version}"
        )));
    }
    // Tables without a ledger were not created by these migrations; adopting
    // them would mean guessing which migrations they reflect.
    let unledgered: bool =
        sqlx::query("SELECT to_regclass('hives') IS NOT NULL AND to_regclass($1) IS NULL")
            .bind(LEDGER)
            .fetch_one(pool)
            .await
            .map_err(db)?
            .try_get(0)
            .map_err(db)?;
    if unledgered {
        return Err(StoreError::backend(
            "Hivekeep tables exist without a migration ledger; refusing to migrate an unmanaged schema",
        ));
    }
    migrator.run(pool).await.map_err(StoreError::backend)
}

/// Quotes an SQL identifier.
pub(super) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting() {
        assert_eq!(quote_ident("hivekeep"), "\"hivekeep\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn migrations_are_embedded() {
        assert!(MIGRATOR.iter().any(|m| m.description == "initial"));
    }
}

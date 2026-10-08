//! Connection settings: validation, credentials, schema and roles.

use super::super::{options, PROBE_POOL_MAX};
use super::*;
use crate::{api::HiveStore, StoreError};

fn invalid(r: crate::Result<PgStore>) -> String {
    match r {
        Err(StoreError::InvalidArgument(why)) => why,
        Err(e) => panic!("want InvalidArgument, got {e}"),
        Ok(_) => panic!("connect accepted an invalid configuration"),
    }
}

/// Settings that cannot work are refused before anything is dialled.
#[tokio::test]
async fn connect_rejects_empty_settings() {
    let base = PgConfig {
        read_write_url: "postgres://unused@127.0.0.1:1/unused".into(),
        schema: "hivekeep".into(),
        ..Default::default()
    };
    let cases = [
        (
            "read/write role",
            PgConfig {
                read_write_role: Some(String::new()),
                owner_role: Some("owner".into()),
                ..base.clone()
            },
            "read/write role must not be empty",
        ),
        (
            "owner role",
            PgConfig {
                read_write_role: Some("readwrite".into()),
                owner_role: Some(String::new()),
                ..base.clone()
            },
            "owner role must not be empty",
        ),
        (
            "owner connection string",
            PgConfig {
                owner_url: Some(String::new()),
                ..base.clone()
            },
            "owner connection string must not be empty",
        ),
        (
            "schema",
            PgConfig {
                schema: String::new(),
                ..base.clone()
            },
            "schema must not be empty",
        ),
    ];
    for (name, cfg, want) in cases {
        let why = invalid(PgStore::connect(&cfg).await);
        assert!(why.contains(want), "{name}: {why:?} does not say {want:?}");
    }
}

/// Client certificate, key and roots named in the URL are read when a
/// connection is made, not when the URL is parsed, so every new connection
/// presents what is on disk then and a rotated certificate is picked up
/// without a restart.
#[test]
fn credentials_are_read_per_connection() {
    let dir = std::env::temp_dir().join(unique_schema("rotation"));
    let bundle = dir.join("credential-bundle.pem");
    let root = dir.join("trust-bundle.pem");
    let url = format!(
        "postgres://postgres@postgres.hivekeep-system.svc:5432/hivekeep?sslmode=verify-full&sslrootcert={}&sslcert={}&sslkey={}",
        root.display(),
        bundle.display(),
        bundle.display()
    );
    // Nothing exists yet: parsing must not have needed the files.
    assert!(!bundle.exists() && !root.exists());
    options(&url, "hivekeep").expect("parsing a URL whose credentials are not on disk yet");
}

/// The configured schema wins over a search path in the URL, the migration
/// ledger lives in it, and unrelated tables are left alone.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn connect_uses_configured_schema() {
    let schema = unique_schema("configured");
    let admin = admin().await;
    exec(
        &admin,
        "CREATE TABLE IF NOT EXISTS public.hk_schema_test_marker (id integer)",
    )
    .await;
    let sep = if url().contains('?') { '&' } else { '?' };
    let store = PgStore::connect(&PgConfig {
        read_write_url: format!("{}{sep}options=-c%20search_path%3Dpublic", url()),
        owner_url: Some(url()),
        ..config(&schema)
    })
    .await
    .unwrap();
    mk_hive(&store, "schema-test").await;
    for table in ["hives", LEDGER_TABLE] {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
        )
        .bind(&schema)
        .bind(table)
        .fetch_one(&admin)
        .await
        .unwrap();
        assert!(exists, "{schema}.{table} is missing");
    }
    let in_schema: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {}.hives",
        quote_ident(&schema)
    )))
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(
        in_schema, 1,
        "the write did not land in the configured schema"
    );
    let marker: bool =
        sqlx::query_scalar("SELECT to_regclass('public.hk_schema_test_marker') IS NOT NULL")
            .fetch_one(&admin)
            .await
            .unwrap();
    assert!(marker, "migration removed an unrelated table");
    exec(&admin, "DROP TABLE public.hk_schema_test_marker").await;
    store.close().await;
    admin.close().await;
    drop_schema(&schema).await;
}

const LEDGER_TABLE: &str = super::super::migrate::LEDGER;

/// Swaps the user (and password) of a connection URL.
fn with_login(url: &str, user: &str, password: &str) -> String {
    let (scheme, rest) = url.split_once("://").expect("URL has a scheme");
    let (_, host) = rest.split_once('@').expect("URL names a user");
    format!("{scheme}://{user}:{password}@{host}")
}

/// Requests run as a role that may read and write but not change the
/// schema; migrations run as the owner role.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL (with CREATEROLE)"]
async fn connect_separates_runtime_and_ddl_privileges() {
    let schema = unique_schema("roles");
    let tag = &schema["hk_test_roles_".len()..];
    let runtime_role = format!("hk_runtime_{tag}");
    let ddl_role = format!("hk_ddl_{tag}");
    let runtime_login = format!("hk_runtime_login_{tag}");
    let ddl_login = format!("hk_ddl_login_{tag}");
    const PASSWORD: &str = "test-password";
    let [s, rr, dr, rl, dl] = [
        &schema,
        &runtime_role,
        &ddl_role,
        &runtime_login,
        &ddl_login,
    ]
    .map(|n| quote_ident(n));
    let admin = admin().await;
    let db: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&admin)
        .await
        .unwrap();
    let dbq = quote_ident(&db);
    exec(
        &admin,
        &format!(
            "CREATE ROLE {rr} NOLOGIN; CREATE ROLE {dr} NOLOGIN; \
             CREATE ROLE {rl} LOGIN PASSWORD '{PASSWORD}'; CREATE ROLE {dl} LOGIN PASSWORD '{PASSWORD}'; \
             GRANT {rr} TO {rl}; GRANT {dr} TO {dl}; GRANT CREATE ON DATABASE {dbq} TO {dr}; \
             CREATE SCHEMA {s} AUTHORIZATION {dr}; GRANT USAGE ON SCHEMA {s} TO {rr}; \
             ALTER DEFAULT PRIVILEGES FOR ROLE {dr} IN SCHEMA {s} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {rr}"
        ),
    )
    .await;

    let store = PgStore::connect(&PgConfig {
        read_write_url: with_login(&url(), &runtime_login, PASSWORD),
        owner_url: Some(with_login(&url(), &ddl_login, PASSWORD)),
        read_write_role: Some(runtime_role.clone()),
        owner_role: Some(ddl_role.clone()),
        schema: schema.clone(),
        max_connections: Some(20),
    })
    .await
    .unwrap();
    assert_eq!(store.pool.options().get_max_connections(), 20);
    assert_eq!(
        store.probe_pool.options().get_max_connections(),
        PROBE_POOL_MAX
    );
    let probe_user: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&store.probe_pool)
        .await
        .unwrap();
    assert_eq!(
        probe_user, runtime_role,
        "the probe pool runs as another role"
    );

    mk_hive(&store, "runtime-write").await;
    assert!(
        sqlx::query("CREATE TABLE forbidden (id integer)")
            .execute(store.pool())
            .await
            .is_err(),
        "the read/write role created a table"
    );
    let ledger: bool = sqlx::query_scalar("SELECT has_table_privilege(current_user, $1, 'UPDATE')")
        .bind(LEDGER_TABLE)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert!(
        ledger,
        "the read/write role lacks default privileges on the ledger"
    );
    store
        .run_maintenance()
        .await
        .expect("maintenance under the read/write role");
    let owner: String = sqlx::query_scalar(
        "SELECT pg_get_userbyid(relowner)::text FROM pg_class WHERE oid = $1::regclass",
    )
    .bind(format!("{s}.hives"))
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(owner, ddl_role, "tables are not owned by the owner role");
    assert!(store.get_hive("runtime-write").await.is_ok());

    store.close().await;
    exec(
        &admin,
        &format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {dr} IN SCHEMA {s} REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM {rr}; \
             DROP SCHEMA {s} CASCADE; REVOKE ALL ON DATABASE {dbq} FROM {dr}; \
             DROP ROLE {rl}; DROP ROLE {dl}; DROP ROLE {rr}; DROP ROLE {dr}"
        ),
    )
    .await;
    admin.close().await;
}

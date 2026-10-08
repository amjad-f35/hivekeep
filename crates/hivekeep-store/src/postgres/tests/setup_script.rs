//! The bootstrap script, run through psql as an operator would.

use super::super::pool_options;
use super::super::setup::SetupConfig;
use super::*;

/// Runs the bootstrap script for `config`, returning psql's output on
/// failure.
fn run_setup(script: &std::path::Path, config: &SetupConfig) -> Result<(), String> {
    let out = std::process::Command::new("psql")
        .arg(url())
        .args(["--no-psqlrc", "--set=ON_ERROR_STOP=1"])
        .args(config.psql_args())
        .arg(format!("--file={}", script.display()))
        .output()
        .map_err(|e| format!("running psql: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "psql exited with {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

/// Removes everything a setup run creates.
async fn reset(admin: &PgPool, config: &SetupConfig) {
    exec(
        admin,
        &format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(&config.schema)
        ),
    )
    .await;
    let roles = [
        &config.owner_user,
        &config.read_write_user,
        &config.owner_role,
        &config.read_write_role,
    ];
    for role in roles {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT FROM pg_roles WHERE rolname = $1)")
                .bind(role)
                .fetch_one(admin)
                .await
                .unwrap();
        if exists {
            exec(admin, &format!("DROP OWNED BY {}", quote_ident(role))).await;
        }
    }
    for role in roles {
        exec(admin, &format!("DROP ROLE IF EXISTS {}", quote_ident(role))).await;
    }
}

/// A pool logged in as `user`, acting as `role` in `schema`.
async fn login(user: &str, password: &str, role: &str, schema: &str) -> PgPool {
    let opts = super::super::options(&url(), schema)
        .unwrap()
        .username(user)
        .password(password);
    pool_options(Some(role.to_string()))
        .max_connections(1)
        .connect_with(opts)
        .await
        .unwrap_or_else(|e| panic!("connecting as {user}: {e}"))
}

async fn check(script: &std::path::Path, config: &SetupConfig) {
    let admin = admin().await;
    reset(&admin, config).await;

    // A pre-existing role with the wrong attributes is not adopted.
    let owner = quote_ident(&config.owner_role);
    exec(&admin, &format!("CREATE ROLE {owner} LOGIN")).await;
    let err = run_setup(script, config).unwrap_err();
    assert!(
        err.contains("conflicts with the required attributes"),
        "{err}"
    );
    exec(&admin, &format!("DROP ROLE {owner}")).await;

    // Nor is a schema someone else owns.
    let schema = quote_ident(&config.schema);
    exec(&admin, &format!("CREATE SCHEMA {schema}")).await;
    let err = run_setup(script, config).unwrap_err();
    assert!(err.contains("is owned by"), "{err}");
    exec(&admin, &format!("DROP SCHEMA {schema}")).await;

    // Idempotent.
    for i in 1..=2 {
        run_setup(script, config).unwrap_or_else(|e| panic!("setup run {i}: {e}"));
    }

    let owner = login(
        &config.owner_user,
        &config.owner_password,
        &config.owner_role,
        &config.schema,
    )
    .await;
    let rw = login(
        &config.read_write_user,
        &config.read_write_password,
        &config.read_write_role,
        &config.schema,
    )
    .await;
    exec(
        &owner,
        "CREATE TABLE setup_permissions (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, value text)",
    )
    .await;
    exec(
        &rw,
        "INSERT INTO setup_permissions (value) VALUES ('before')",
    )
    .await;
    exec(&rw, "UPDATE setup_permissions SET value = 'after'").await;
    let value: String = sqlx::query_scalar("SELECT value FROM setup_permissions")
        .fetch_one(&rw)
        .await
        .unwrap();
    assert_eq!(value, "after");
    exec(&rw, "DELETE FROM setup_permissions").await;
    assert!(
        sqlx::query("CREATE TABLE forbidden (id integer)")
            .execute(&rw)
            .await
            .is_err(),
        "the read/write role created a table"
    );
    owner.close().await;
    rw.close().await;
    reset(&admin, config).await;
    admin.close().await;
}

/// Bundled names and operator-chosen ones (including a password that needs
/// quoting) both provision working, correctly privileged roles, and the
/// script refuses to adopt conflicting roles or schemas.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL (superuser) and psql on PATH"]
async fn postgres_setup_script() {
    let dir = std::env::temp_dir().join(unique_schema("setup"));
    std::fs::create_dir_all(&dir).unwrap();
    let script = dir.join("hivekeep-postgres-setup.sql");
    std::fs::write(&script, SetupConfig::script()).unwrap();

    let bundled = SetupConfig {
        owner_password: "test-owner-password".into(),
        read_write_password: "test-runtime-password".into(),
        ..Default::default()
    };
    let operator = SetupConfig {
        schema: "custom-hivekeep".into(),
        owner_role: "custom_hivekeep_owner".into(),
        owner_user: "custom_hivekeep_owner_user".into(),
        owner_password: "custom owner's password".into(),
        read_write_role: "custom_hivekeep_readwrite".into(),
        read_write_user: "custom_hivekeep_readwrite_user".into(),
        read_write_password: "custom runtime password".into(),
    };
    for config in [bundled, operator] {
        check(&script, &config).await;
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

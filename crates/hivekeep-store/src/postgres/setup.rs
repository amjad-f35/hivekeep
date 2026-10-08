//! Database bootstrap: roles, login users and the schema, applied once by an
//! administrator with psql (see `setup.sql`).

/// Default schema name.
pub const SCHEMA: &str = "hivekeep";
/// Default owner role: owns the schema and runs migrations.
pub const OWNER_ROLE: &str = "hivekeep_owner";
/// Default login user granted [`OWNER_ROLE`].
pub const OWNER_USER: &str = "hivekeep_owner_user";
/// Default read/write role: serves requests.
pub const READ_WRITE_ROLE: &str = "hivekeep_readwrite";
/// Default login user granted [`READ_WRITE_ROLE`].
pub const READ_WRITE_USER: &str = "hivekeep_readwrite_user";

/// The bootstrap SQL, expecting the psql variables of [`SetupConfig::psql_args`].
pub const SETUP_SQL: &str = include_str!("setup.sql");

/// Names and passwords for the bootstrap script.
#[derive(Clone, PartialEq, Eq)]
pub struct SetupConfig {
    /// Schema to create.
    pub schema: String,
    /// Owner role.
    pub owner_role: String,
    /// Owner login user.
    pub owner_user: String,
    /// Owner user's password; empty disables password login.
    pub owner_password: String,
    /// Read/write role.
    pub read_write_role: String,
    /// Read/write login user.
    pub read_write_user: String,
    /// Read/write user's password; empty disables password login.
    pub read_write_password: String,
}

impl std::fmt::Debug for SetupConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetupConfig")
            .field("schema", &self.schema)
            .field("owner_role", &self.owner_role)
            .field("owner_user", &self.owner_user)
            .field("read_write_role", &self.read_write_role)
            .field("read_write_user", &self.read_write_user)
            .finish_non_exhaustive()
    }
}

impl Default for SetupConfig {
    fn default() -> Self {
        SetupConfig {
            schema: SCHEMA.into(),
            owner_role: OWNER_ROLE.into(),
            owner_user: OWNER_USER.into(),
            owner_password: String::new(),
            read_write_role: READ_WRITE_ROLE.into(),
            read_write_user: READ_WRITE_USER.into(),
            read_write_password: String::new(),
        }
    }
}

impl SetupConfig {
    /// `--set` arguments for psql supplying the script's variables.
    pub fn psql_args(&self) -> Vec<String> {
        [
            ("hivekeep_schema", &self.schema),
            ("hivekeep_owner_role", &self.owner_role),
            ("hivekeep_owner_user", &self.owner_user),
            ("hivekeep_owner_password", &self.owner_password),
            ("hivekeep_readwrite_role", &self.read_write_role),
            ("hivekeep_readwrite_user", &self.read_write_user),
            ("hivekeep_readwrite_password", &self.read_write_password),
        ]
        .into_iter()
        .map(|(k, v)| format!("--set={k}={v}"))
        .collect()
    }

    /// The script wrapped in a single transaction.
    pub fn script() -> String {
        format!("BEGIN;\n{SETUP_SQL}\nCOMMIT;\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variable_is_supplied() {
        let args = SetupConfig::default().psql_args();
        for var in [
            "hivekeep_schema",
            "hivekeep_owner_role",
            "hivekeep_owner_user",
            "hivekeep_owner_password",
            "hivekeep_readwrite_role",
            "hivekeep_readwrite_user",
            "hivekeep_readwrite_password",
        ] {
            assert!(SETUP_SQL.contains(&format!(":'{var}'")), "{var} unused");
            assert!(
                args.iter().any(|a| a.starts_with(&format!("--set={var}="))),
                "{var} missing"
            );
        }
        assert!(SetupConfig::script().starts_with("BEGIN;"));
    }
}

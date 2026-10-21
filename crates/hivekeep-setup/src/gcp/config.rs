//! The provisioning settings, their environment fallbacks, and the identities
//! derived from them.

use crate::installdefaults;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::OnceLock;

/// Default node machine type.
pub const DEFAULT_MACHINE_TYPE: &str = "c3-standard-4";
/// Default region, cluster name and location.
pub const DEFAULT_REGION: &str = "us-west1";
pub const DEFAULT_CLUSTER_NAME: &str = "hivekeep-poc";
pub const DEFAULT_CLUSTER_LOCATION: &str = "us-west1-c";
/// Name of the node pool this tool creates.
pub const NODE_POOL_NAME: &str = "hivekeep-node-pool";
/// Cloud SQL defaults.
pub const DEFAULT_CLOUDSQL_INSTANCE: &str = "hivekeep-pg";
pub const DEFAULT_CLOUDSQL_TIER: &str = "db-custom-2-8192";
pub const DEFAULT_CLOUDSQL_EDITION: &str = "enterprise";
pub const DEFAULT_CLOUDSQL_GSA_NAME: &str = "hivekeep-api";

/// The dashboards directory shipped with this crate.
pub fn default_dashboard_dir() -> String {
    concat!(env!("CARGO_MANIFEST_DIR"), "/dashboards").to_string()
}

/// Everything the subcommands may need. Each subcommand fills the fields its
/// flags cover.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcpConfig {
    pub project_id: String,
    pub project_number: String,
    pub region: String,

    pub cluster_name: String,
    pub cluster_location: String,
    pub cluster_version: String,

    pub network: String,
    pub subnetwork: String,
    pub enable_dataplane_v2: bool,

    pub machine_type: String,
    pub enable_nested_virtualization: bool,
    pub boot_disk_size_gb: i32,
    pub boot_disk_type: String,

    pub bucket_name: String,
    /// Name of the snapshot transfer service account
    /// (`SNAPSHOT_TRANSFER_GSA_NAME`, which the installer also reads).
    pub transfer_gsa_name: String,

    pub cloudsql_instance: String,
    pub cloudsql_tier: String,
    pub cloudsql_edition: String,
    pub cloudsql_storage_gb: i64,
    pub cloudsql_gsa_name: String,

    pub dashboard_dir: String,
}

impl GcpConfig {
    /// The bucket for micro-VM sandbox assets.
    pub fn asset_bucket_name(&self) -> String {
        format!("{}-assets", self.bucket_name)
    }
}

/// The IAM member for a Kubernetes service account in the install namespace
/// under GKE Workload Identity Federation.
pub fn workload_identity_principal(cfg: &GcpConfig, ksa: &str) -> String {
    format!(
        "principal://iam.googleapis.com/projects/{}/locations/global/workloadIdentityPools/{}.svc.id.goog/subject/ns/{}/sa/{ksa}",
        cfg.project_number,
        cfg.project_id,
        installdefaults::SYSTEM_NAMESPACE
    )
}

/// The node machine type from `NODE_MACHINE_TYPE`, falling back to the
/// deprecated `GVISOR_NODE_MACHINE_TYPE`, then the default. An empty value
/// counts as unset. The bool reports that the deprecated name supplied it.
pub fn machine_type_from(new: Option<&str>, old: Option<&str>) -> (String, bool) {
    if let Some(v) = new.filter(|v| !v.is_empty()) {
        return (v.to_string(), false);
    }
    if let Some(v) = old.filter(|v| !v.is_empty()) {
        return (v.to_string(), true);
    }
    (DEFAULT_MACHINE_TYPE.to_string(), false)
}

fn machine_type_from_env() -> (String, bool) {
    let new = std::env::var("NODE_MACHINE_TYPE").ok();
    let old = std::env::var("GVISOR_NODE_MACHINE_TYPE").ok();
    machine_type_from(new.as_deref(), old.as_deref())
}

/// The `--machine-type` default.
pub fn resolve_machine_type_default() -> String {
    machine_type_from_env().0
}

/// The deprecation warning to print, if the deprecated variable chose the
/// machine type and no flag overrode it.
pub fn deprecated_machine_type_warning(
    flag_set: bool,
    new: Option<&str>,
    old: Option<&str>,
) -> Option<&'static str> {
    if flag_set {
        return None;
    }
    machine_type_from(new, old)
        .1
        .then_some("GVISOR_NODE_MACHINE_TYPE is deprecated; use NODE_MACHINE_TYPE instead")
}

/// Prints the deprecation warning when it applies.
pub fn warn_deprecated_machine_type_env(flag_set: bool) {
    let new = std::env::var("NODE_MACHINE_TYPE").ok();
    let old = std::env::var("GVISOR_NODE_MACHINE_TYPE").ok();
    if let Some(w) = deprecated_machine_type_warning(flag_set, new.as_deref(), old.as_deref()) {
        crate::log::warn(w);
    }
}

/// A type an environment variable can be read as.
pub trait EnvValue: Sized + Clone + std::fmt::Debug {
    fn parse_env(raw: &str) -> Result<Self, String>;
}

impl EnvValue for String {
    fn parse_env(raw: &str) -> Result<Self, String> {
        Ok(raw.to_string())
    }
}

impl EnvValue for bool {
    /// The spellings `strconv.ParseBool`-style parsers accept; "off" and "no"
    /// are rejected.
    fn parse_env(raw: &str) -> Result<Self, String> {
        match raw {
            "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
            "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
            _ => Err(format!("invalid boolean {raw:?}")),
        }
    }
}

impl EnvValue for i64 {
    fn parse_env(raw: &str) -> Result<Self, String> {
        raw.parse().map_err(|e| format!("{e}"))
    }
}

impl EnvValue for i32 {
    fn parse_env(raw: &str) -> Result<Self, String> {
        raw.parse().map_err(|e| format!("{e}"))
    }
}

fn warned() -> &'static Mutex<HashSet<String>> {
    static W: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    W.get_or_init(Mutex::default)
}

/// Reads `key` as `T`, falling back when it is unset or unparsable. An
/// unparsable value is reported through `warn`, once per key: several
/// commands register flags for the same variable.
pub fn get_env_with<T: EnvValue>(key: &str, fallback: T, warn: &mut dyn FnMut(String)) -> T {
    let Ok(raw) = std::env::var(key) else {
        return fallback;
    };
    match T::parse_env(&raw) {
        Ok(v) => v,
        Err(e) => {
            if warned().lock().insert(key.to_string()) {
                warn(format!(
                    "ignoring unparsable environment variable {key}={raw:?}, using {fallback:?}: {e}"
                ));
            }
            fallback
        }
    }
}

/// [`get_env_with`], warning on stderr.
pub fn get_env<T: EnvValue>(key: &str, fallback: T) -> T {
    get_env_with(key, fallback, &mut |m| crate::log::warn(&m))
}

/// Forgets that a key was warned about, for tests.
pub fn reset_warned(key: &str) {
    warned().lock().remove(key);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_machine_type_default_cases() {
        let cases: [(&str, Option<&str>, Option<&str>, &str); 6] = [
            ("neither set", None, None, "c3-standard-4"),
            ("new name set", Some("n4-standard-8"), None, "n4-standard-8"),
            (
                "old name still honored",
                None,
                Some("n2-standard-8"),
                "n2-standard-8",
            ),
            (
                "new name wins over old",
                Some("n4-standard-8"),
                Some("n2-standard-8"),
                "n4-standard-8",
            ),
            (
                "empty new name does not suppress the old one",
                Some(""),
                Some("n2-standard-8"),
                "n2-standard-8",
            ),
            (
                "empty values fall back to the default",
                Some(""),
                Some(""),
                "c3-standard-4",
            ),
        ];
        for (name, new, old, want) in cases {
            assert_eq!(machine_type_from(new, old).0, want, "{name}");
        }
    }

    #[test]
    fn warn_deprecated_machine_type_env_cases() {
        type Case<'a> = (&'a str, Option<&'a str>, Option<&'a str>, bool, bool);
        let cases: [Case; 5] = [
            (
                "old name alone warns",
                None,
                Some("n2-standard-8"),
                false,
                true,
            ),
            ("neither set is quiet", None, None, false, false),
            (
                "new name alone is quiet",
                Some("n4-standard-8"),
                None,
                false,
                false,
            ),
            (
                "new name takes precedence, so no warning",
                Some("n4-standard-8"),
                Some("n2-standard-8"),
                false,
                false,
            ),
            (
                "explicit flag makes the variable moot",
                None,
                Some("n2-standard-8"),
                true,
                false,
            ),
        ];
        for (name, new, old, flag, want) in cases {
            let got = deprecated_machine_type_warning(flag, new, old);
            assert_eq!(got.is_some(), want, "{name}");
            if let Some(w) = got {
                assert!(w.contains("GVISOR_NODE_MACHINE_TYPE is deprecated"));
            }
        }
    }

    fn quiet() -> impl FnMut(String) {
        |_| {}
    }

    #[test]
    fn get_env_string() {
        let key = "HK_TEST_ENV_STRING_VAR";
        std::env::remove_var(key);
        assert_eq!(
            get_env_with(key, "default".to_string(), &mut quiet()),
            "default"
        );
        std::env::set_var(key, "hello");
        assert_eq!(
            get_env_with(key, "default".to_string(), &mut quiet()),
            "hello"
        );
        std::env::remove_var(key);
    }

    #[test]
    fn get_env_bool() {
        let key = "HK_TEST_ENV_BOOL_VAR";
        std::env::remove_var(key);
        assert!(get_env_with(key, true, &mut quiet()));
        assert!(!get_env_with(key, false, &mut quiet()));
        for (raw, fallback, want) in [
            ("true", false, true),
            ("TRUE", false, true),
            ("1", false, true),
            ("t", false, true),
            ("T", false, true),
            ("false", true, false),
            ("FALSE", true, false),
            ("0", true, false),
            ("f", true, false),
            ("F", true, false),
        ] {
            std::env::set_var(key, raw);
            assert_eq!(get_env_with(key, fallback, &mut quiet()), want, "{raw}");
        }
        std::env::set_var(key, "invalid_bool");
        assert!(get_env_with(key, true, &mut quiet()));
        assert!(!get_env_with(key, false, &mut quiet()));
        std::env::remove_var(key);
    }

    #[test]
    fn get_env_warns_on_unparsable_value() {
        let key = "HK_TEST_ENV_UNPARSABLE_VAR";
        std::env::set_var(key, "off");
        reset_warned(key);
        let mut log = Vec::new();
        assert!(get_env_with(key, true, &mut |m| log.push(m)));
        assert_eq!(log.len(), 1);
        assert!(log[0].contains(key), "{log:?}");
        // The warning does not repeat once per registration.
        assert!(get_env_with(key, true, &mut |m| log.push(m)));
        assert_eq!(log.len(), 1, "{log:?}");
        std::env::remove_var(key);
        reset_warned(key);
    }

    #[test]
    fn get_env_int32() {
        let key = "HK_TEST_ENV_INT32_VAR";
        std::env::remove_var(key);
        assert_eq!(get_env_with(key, 500i32, &mut quiet()), 500);
        std::env::set_var(key, "250");
        assert_eq!(get_env_with(key, 500i32, &mut quiet()), 250);
        std::env::set_var(key, "invalid");
        assert_eq!(get_env_with(key, 500i32, &mut quiet()), 500);
        std::env::set_var(key, "5000000000");
        assert_eq!(get_env_with(key, 500i32, &mut quiet()), 500);
        std::env::remove_var(key);
    }

    #[test]
    fn get_env_int64() {
        let key = "HK_TEST_ENV_INT64_VAR";
        std::env::set_var(key, "1234");
        assert_eq!(get_env_with(key, 0i64, &mut quiet()), 1234);
        std::env::set_var(key, "not-a-number");
        assert_eq!(get_env_with(key, 7i64, &mut quiet()), 7);
        std::env::remove_var(key);
    }
}

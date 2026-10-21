//! Test fixtures for the configuration machinery.
//!
//! The process environment is shared by every test thread, so a test that
//! reads or writes it holds [`TestEnv`], which serialises those tests and
//! restores every variable it touched. Command-registered settings are kept
//! per thread in test builds, and the guard restores them too.

use super::load::{load, LoadOptions, CONFIG_PATH_ENV};
use super::registry::{all, register_command, restore_scoped, snapshot_scoped};
use super::resolve::Flags;
use super::setting::{Setting, ValueKind};
use super::{Config, ConfigError, FILE_API_VERSION, FILE_KIND};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

static ENV_LOCK: Mutex<()> = Mutex::new(());

/// The header of a valid configuration document.
pub const VALID_HEADER: &str = "apiVersion: install.hivekeep.io/v1alpha1\nkind: HivekeepInstall\n";

/// Serialises environment access and restores what a test changed.
pub struct TestEnv {
    _lock: MutexGuard<'static, ()>,
    saved_env: BTreeMap<String, Option<OsString>>,
    saved_scoped: Vec<Setting>,
}

impl TestEnv {
    pub fn new() -> Self {
        debug_assert_eq!(
            VALID_HEADER,
            format!("apiVersion: {FILE_API_VERSION}\nkind: {FILE_KIND}\n")
        );
        Self {
            _lock: ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner()),
            saved_env: BTreeMap::new(),
            saved_scoped: snapshot_scoped(),
        }
    }

    fn remember(&mut self, name: &str) {
        self.saved_env
            .entry(name.to_string())
            .or_insert_with(|| std::env::var_os(name));
    }

    pub fn set_env(&mut self, name: &str, value: &str) {
        self.remember(name);
        std::env::set_var(name, value);
    }

    pub fn unset_env(&mut self, name: &str) {
        self.remember(name);
        std::env::remove_var(name);
    }

    /// Isolates loading from the ambient environment: every variable a
    /// setting reads is unset (unset, not blanked -- an exported empty value
    /// is a value), and the dev env file is skipped.
    pub fn load_env(&mut self) {
        self.set_env("NO_DEV_ENV", "1");
        let mut names: Vec<String> = all().into_iter().map(|s| s.env).collect();
        names.extend(["MEMORYSTORE_INSTANCE", CONFIG_PATH_ENV].map(String::from));
        for n in names {
            if !n.is_empty() {
                self.unset_env(&n);
            }
        }
    }

    pub fn use_settings(&mut self, settings: Vec<Setting>) {
        register_command(settings);
    }

    pub fn use_fixtures(&mut self) {
        self.use_settings(fixture_settings());
    }
}

impl Default for TestEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        for (k, v) in std::mem::take(&mut self.saved_env) {
            match v {
                Some(v) => std::env::set_var(&k, v),
                None => std::env::remove_var(&k),
            }
        }
        restore_scoped(std::mem::take(&mut self.saved_scoped));
    }
}

/// One setting of every kind, plus a default, a secret, and a key nested
/// more than one level deep.
pub fn fixture_settings() -> Vec<Setting> {
    use ValueKind::*;
    vec![
        Setting::new("fixture.name", "FIXTURE_NAME", "fixture-name")
            .usage("a string with no default"),
        Setting::new("fixture.mode", "FIXTURE_MODE", "fixture-mode")
            .default_value("alpha")
            .usage("a string with a default"),
        Setting::new("fixture.enabled", "FIXTURE_ENABLED", "fixture-enabled")
            .kind(Bool)
            .default_value("false")
            .usage("a bool"),
        Setting::new("fixture.count", "FIXTURE_COUNT", "fixture-count")
            .kind(Int)
            .default_value("1")
            .usage("an int"),
        Setting::new("fixture.timeout", "FIXTURE_TIMEOUT", "fixture-timeout")
            .kind(Duration)
            .default_value("1m0s")
            .usage("a duration"),
        Setting::new("fixture.secret", "FIXTURE_SECRET", "fixture-secret")
            .secret()
            .usage("a secret"),
        Setting::new(
            "fixture.nested.deep.key",
            "FIXTURE_NESTED_DEEP_KEY",
            "fixture-nested-deep-key",
        )
        .usage("a nested key"),
    ]
}

/// A plain string setting with no default.
pub fn scratch_setting() -> Setting {
    Setting::new("scratch.str", "SCRATCH_STR", "scratch-str").usage("a string")
}

pub fn test_setting(key: &str, env: &str, flag: &str, commands: &[&str]) -> Setting {
    Setting::new(key, env, flag)
        .usage("test setting")
        .commands(commands)
}

pub fn env_map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Writes a configuration document to a fresh temporary directory.
pub fn write_config(content: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("install.yaml");
    std::fs::write(&p, content).unwrap();
    (dir, p)
}

/// A configuration stated as raw flag values, the way a test means it.
/// A zero field is left out, so a bool cannot be passed explicitly false.
#[derive(Clone, Debug, Default)]
pub struct Options {
    pub kind: bool,
    pub kubeconfig: String,
    pub context: String,
    pub router: String,
    pub rollout_timeout: String,
    pub podcert_workers_per_signer: i64,
    pub envoy_concurrency: i64,
    pub cluster_size: String,
    pub cordon_system_infra_components: bool,
    pub fake_data_plane: bool,
    pub e2e_tuning: bool,
    pub additional_egress_extproc_service: String,
    pub credential_provider: String,
    pub otlp_endpoint: String,
    pub image_repo: String,
    pub image_tag: String,
}

/// The flags `opts` sets.
pub fn options_flags(opts: &Options) -> Flags {
    let mut f = Flags::default();
    for (flag, val) in [
        ("kubeconfig", &opts.kubeconfig),
        ("context", &opts.context),
        ("gate-dataplane", &opts.router),
        ("rollout-timeout", &opts.rollout_timeout),
        ("cluster-size", &opts.cluster_size),
        (
            "experimental-additional-egress-extproc-service",
            &opts.additional_egress_extproc_service,
        ),
        ("credential-provider", &opts.credential_provider),
        ("otlp-endpoint", &opts.otlp_endpoint),
        ("image-repo", &opts.image_repo),
        ("image-tag", &opts.image_tag),
    ] {
        if !val.is_empty() {
            f.set(flag, val);
        }
    }
    for (flag, on) in [
        ("kind", opts.kind),
        (
            "cordon-system-infra-components",
            opts.cordon_system_infra_components,
        ),
        ("fake-data-plane", opts.fake_data_plane),
        ("e2e-tuning", opts.e2e_tuning),
    ] {
        if on {
            f.set(flag, "true");
        }
    }
    if opts.podcert_workers_per_signer != 0 {
        f.set(
            "podcert-workers-per-signer",
            &opts.podcert_workers_per_signer.to_string(),
        );
    }
    if opts.envoy_concurrency != 0 {
        f.set("e2e-envoy-concurrency", &opts.envoy_concurrency.to_string());
    }
    f
}

/// The repository root tests load from.
pub fn test_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

/// Loads `opts` through the same path the command line takes.
pub fn load_options(opts: &Options) -> Result<Config, ConfigError> {
    load(
        &options_flags(opts),
        LoadOptions {
            root: Some(test_root()),
            ..Default::default()
        },
    )
}

/// [`Config::script_env`] as a map; absence and emptiness are distinct.
pub fn script_env_map(cfg: &Config) -> BTreeMap<String, String> {
    cfg.script_env()
        .into_iter()
        .map(|kv| {
            let (k, v) = kv.split_once('=').expect("KEY=VALUE");
            (k.to_string(), v.to_string())
        })
        .collect()
}

//! The environment the installer installs into.
//!
//! Settings are declared once ([`registry`]) and reach the installer through
//! four channels, lowest precedence first: the developer's
//! `.hivekeep-dev-env.sh`, the process environment, a configuration document,
//! and command-line flags. [`resolve`] layers them, remembering where each
//! value came from; [`Config`] is the typed projection the install steps
//! read.

pub mod credentials;
pub mod document;
pub mod errors;
pub mod file;
pub mod flags;
pub mod kindcheck;
pub mod kubeconfig;
pub mod load;
pub mod record;
pub mod registry;
pub mod report;
pub mod resolve;
pub mod setting;
pub mod shellenv;
#[cfg(test)]
pub(crate) mod testutil;
mod yaml_text;

pub use document::{Document, DocumentMetadata, Outcome, DOCUMENT_METADATA_KEY};
pub use errors::ConfigError;
pub use file::{parse_file, File, FILE_API_VERSION, FILE_KIND};
pub use flags::{bind_command_flags, bind_flags};
pub use load::{
    apply_kind_defaults, build_config, environ, load, split_list, LoadOptions, CONFIG_PATH_ENV,
};
pub use record::{record_dir, record_failure, record_name, record_success, user_cache_dir};
pub use registry::{
    all, channels_for, keys, lookup, register_command, registry, validate, KIND_ENABLED_KEY,
};
pub use report::ReportOptions;
pub use resolve::{resolve, Flags, Origin, ResolveOptions, Resolved, Value};
pub use setting::{format_duration, parse_duration, Parsed, Setting, ValueKind};
pub use shellenv::{repo_root, source_shell_env};

use crate::images;
use crate::installdefaults;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

/// Gate dataplanes.
pub const ROUTER_ENVOY: &str = "envoy";

/// Sandbox classes.
pub const SANDBOX_CLASS_GVISOR: &str = "gvisor";
pub const SANDBOX_CLASS_MICROVM: &str = "microvm";

/// Cluster size profiles. size0 is the shipped footprint; size10 assumes a
/// dedicated node for PostgreSQL and raises the store, its client pool, and
/// the podcertificate controller's API rate limits to match.
pub const CLUSTER_SIZE_SIZE0: &str = "size0";
pub const CLUSTER_SIZE_SIZE10: &str = "size10";

/// Default wait for workload rollouts.
pub const DEFAULT_ROLLOUT_TIMEOUT: Duration = Duration::from_secs(60);

/// Appended to the bundled read/write connection on size10 clusters, so the
/// client pool opens the connections the server is provisioned for.
pub const SIZE10_POSTGRES_POOL_PARAMS: &str = "&pool_max_conns=64&pool_min_conns=4";

pub const DEFAULT_POSTGRES_SCHEMA: &str = hivekeep_store::postgres::setup::SCHEMA;
pub const DEFAULT_POSTGRES_READ_WRITE_ROLE: &str = hivekeep_store::postgres::setup::READ_WRITE_ROLE;
pub const DEFAULT_POSTGRES_OWNER_ROLE: &str = hivekeep_store::postgres::setup::OWNER_ROLE;

/// Cloud SQL Auth Proxy IP types.
pub const CLOUDSQL_IP_TYPE_PRIVATE: &str = "private";
pub const CLOUDSQL_IP_TYPE_PUBLIC: &str = "public";
pub const CLOUDSQL_IP_TYPE_PSC: &str = "psc";

/// The optional per-developer environment script at the repository root.
pub const DEV_ENV_FILE: &str = ".hivekeep-dev-env.sh";

/// Environment variable naming the manifests directory when the installer
/// runs outside a checkout.
pub const MANIFESTS_DIR_ENV: &str = "HK_MANIFESTS_DIR";

/// The bundled Kubernetes Secrets credential provider and its default
/// address, the Service in
/// `manifests/egress-credential-injection/k8s-credential-provider.yaml`.
pub const K8S_CREDENTIAL_PROVIDER_NAME: &str = "k8s.io";
pub const K8S_CREDENTIAL_PROVIDER_ADDRESS: &str =
    "k8s-credential-provider.hivekeep-system.svc:50051";

/// The setting the provider JSON arrives on.
pub const CREDENTIAL_PROVIDER_KEY: &str = "gate.egress.credentialProvider";

/// The accepted `--credential-provider` values.
pub const CREDENTIAL_PROVIDER_USAGE: &str =
    "{\"name\":\"k8s.io\"} for the bundled Kubernetes Secrets provider, \
{\"enabled\":false} to turn egress credential injection off, \
or {\"name\":\"<provider>\",\"address\":\"<host>:<port>\"} for a provider you deploy";

/// The operator's Cloud SQL intent.
///
/// `instance` is three-way: non-empty selects Cloud SQL, explicitly empty
/// removes it, and unset (`instance_set` false) adopts whatever the cluster
/// already records -- so a redeploy from a shell that never exported the
/// variable cannot tear the proxy sidecar out from under a working install.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CloudSqlConfig {
    /// Instance connection name, PROJECT:REGION:INSTANCE.
    pub instance: String,
    /// Whether any channel supplied the instance, empty value included.
    pub instance_set: bool,
    /// Google service account the proxy authenticates as; its email minus
    /// `.gserviceaccount.com` is the IAM database user.
    pub gsa: String,
    /// `"true"` or `"false"`; empty defaults to enabled.
    pub iam_auth: String,
    /// One of the `CLOUDSQL_IP_TYPE_*` values; empty defaults to private.
    pub ip_type: String,
}

/// The credential provider the egress gateway is pointed at. The default
/// value means injection is off.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CredentialProvider {
    /// Host of the `hk-secret://` credential URIs the provider serves.
    pub name: String,
    /// host:port the gateway dials the provider at.
    pub address: String,
}

impl CredentialProvider {
    pub fn enabled(&self) -> bool {
        !self.name.is_empty()
    }

    /// Whether the provider is the bundled one.
    pub fn kubernetes(&self) -> bool {
        self.name == K8S_CREDENTIAL_PROVIDER_NAME
    }

    /// The SAN expected on the provider's serving certificate: the address
    /// without its port.
    pub fn server_name(&self) -> &str {
        match self.address.rfind(':') {
            Some(i) => &self.address[..i],
            None => &self.address,
        }
    }
}

/// The fully resolved installation environment.
#[derive(Clone, Debug, Default)]
pub struct Config {
    /// Repository root, for builds and repo-relative paths.
    pub root: PathBuf,
    /// The manifests directory (`crates/hivekeep-setup/manifests`).
    pub manifests: PathBuf,
    /// Local Kind install profile.
    pub kind: bool,
    /// Namespace the control plane is installed into. The checked-in
    /// manifests name the default literally, so manifest-applying steps
    /// refuse any other value.
    pub namespace: String,
    /// Single kubeconfig file handed to the client; empty uses the default
    /// loading rules.
    pub kubeconfig: String,
    /// kubeconfig context; empty means the current one.
    pub context: String,
    pub project_id: String,
    pub cluster_name: String,
    pub cluster_location: String,
    /// Service account token issuer hivekeep-api trusts, overriding
    /// derivation and discovery.
    pub expected_jwt_issuer: String,
    /// Comma-separated principals granted owner on global:root.
    pub api_authz_bootstrap_owners: String,
    /// ES256 or RS256.
    pub actor_jwt_algorithm: String,
    /// `iss` of actor JWTs.
    pub actor_jwt_issuer: String,
    /// Snapshot bucket demos are templated with.
    pub bucket_name: String,
    /// hivekeep-api's cluster-wide `--snapshot-location`.
    pub snapshot_location: String,
    /// Name of the GCS snapshot transfer service account in `project_id`.
    pub transfer_gsa_name: String,
    /// Registry images built from source are pushed to.
    pub build_repo: String,
    /// Platforms images built from source target.
    pub build_platforms: String,
    /// Extra `docker buildx build` flags.
    pub docker_build_flags: Vec<String>,
    /// Where images come from; the default builds them from source.
    pub images: images::Source,
    /// Gate dataplane: [`ROUTER_ENVOY`].
    pub router: String,
    /// The read/write and owner connections; one configured connection serves
    /// both pools, both empty selects bundled PostgreSQL.
    pub postgres_read_write_connection_string: String,
    pub postgres_owner_connection_string: String,
    pub postgres_read_write_role: String,
    pub postgres_owner_role: String,
    /// Whether a role was chosen rather than defaulted.
    pub postgres_read_write_role_set: bool,
    pub postgres_owner_role_set: bool,
    /// Empty means [`DEFAULT_POSTGRES_SCHEMA`].
    pub postgres_schema: String,
    pub postgres_pool_max_conns: String,
    /// Local PEM of an external PostgreSQL's server CA, published as the
    /// postgres-server-ca Secret.
    pub postgres_server_ca_file: String,
    pub postgres_storage_class: String,
    pub cloud_sql: CloudSqlConfig,
    pub rollout_timeout: Duration,
    /// Whether the rollout timeout was asked for rather than defaulted.
    pub rollout_timeout_set: bool,
    pub podcert_workers_per_signer: i64,
    pub envoy_concurrency: i64,
    /// [`CLUSTER_SIZE_SIZE0`] or [`CLUSTER_SIZE_SIZE10`].
    pub cluster_size: String,
    pub cordon_system_infra_components: bool,
    pub fake_data_plane: bool,
    pub e2e_tuning: bool,
    pub ingress_allowed_spiffe_ids: Vec<String>,
    /// Optional NS/SVC:PORT external processor.
    pub additional_egress_extproc_service: String,
    pub credential_provider_json: String,
    pub anthropic_api_key: String,
    pub otlp_endpoint: String,
    pub preview_gates: Vec<String>,
    /// Empty keeps the level an existing install has.
    pub api_log_level: String,
    pub benchmark_actor_memory: String,
    /// What helper scripts see as `$KUBECONFIG`; may be a PATH-style list.
    pub kubeconfig_env: String,
    /// The process environment layered over the developer env file.
    pub shell_env: BTreeMap<String, String>,
    pub(crate) resolved: OnceLock<Resolved>,
}

/// GKE-specific variables removed from the script environment of a Kind
/// install, so a developer who sourced their dev env does not point a local
/// install at a cloud project.
const KIND_UNSET_VARS: [&str; 6] = [
    "GCE_REGION",
    "CLUSTER_LOCATION",
    "NETWORK",
    "SUBNETWORK",
    "MEMORYSTORE_INSTANCE",
    "PROJECT_ID",
];

impl Config {
    /// Every setting and its origin. A Config built directly (as tests do)
    /// reports declared defaults.
    pub fn resolved(&self) -> &Resolved {
        self.resolved.get_or_init(Resolved::defaults_only)
    }

    /// Attaches resolved settings to a Config built directly.
    pub fn set_resolved(&mut self, r: Resolved) {
        self.resolved = OnceLock::new();
        let _ = self.resolved.set(r);
    }

    /// The snapshot transfer service account's email; empty when either
    /// half is.
    pub fn transfer_service_account(&self) -> String {
        if self.transfer_gsa_name.is_empty() || self.project_id.is_empty() {
            return String::new();
        }
        format!(
            "{}@{}.iam.gserviceaccount.com",
            self.transfer_gsa_name, self.project_id
        )
    }

    /// Fails when hivekeep-api would be deployed on GCS without a transfer
    /// service account, which its credential exchange needs to start. Kind
    /// installs use S3 and need none.
    pub fn require_transfer_service_account(&self) -> Result<(), ConfigError> {
        if self.kind {
            return Ok(());
        }
        if self.transfer_gsa_name.is_empty() {
            return self.resolved().require("storage.transferGSAName");
        }
        if self.project_id.is_empty() {
            return self.resolved().require("gcp.projectID");
        }
        Ok(())
    }

    pub fn size10(&self) -> bool {
        self.cluster_size == CLUSTER_SIZE_SIZE10
    }

    /// The configured schema, or the default; hivekeep-api rejects empty.
    pub fn postgres_schema_name(&self) -> &str {
        if self.postgres_schema.is_empty() {
            DEFAULT_POSTGRES_SCHEMA
        } else {
            &self.postgres_schema
        }
    }

    /// How long to wait for a workload whose historical timeout was
    /// `historical`. The rollout timeout reaches every wait, but only when it
    /// was asked for: the slow bootstrap waits (podcertificate controller,
    /// CSI drivers) keep their longer budgets otherwise.
    pub fn wait_timeout(&self, historical: Duration) -> Duration {
        if self.rollout_timeout_set {
            self.rollout_timeout
        } else {
            historical
        }
    }

    /// A path under `manifests/install`.
    pub fn manifest(&self, parts: &[&str]) -> PathBuf {
        let mut p = self.manifests.join("install");
        for part in parts {
            p.push(part);
        }
        p
    }

    /// A path under the manifests directory.
    pub fn manifests_path(&self, parts: &[&str]) -> PathBuf {
        let mut p = self.manifests.clone();
        for part in parts {
            p.push(part);
        }
        p
    }

    /// A repo-relative path.
    pub fn path(&self, parts: &[&str]) -> PathBuf {
        let mut p = self.root.clone();
        for part in parts {
            p.push(part);
        }
        p
    }

    /// The environment for helper scripts: the dev env under the process
    /// environment, with the resolved configuration layered on top so flags
    /// reach them. Sorted `NAME=value` entries.
    pub fn script_env(&self) -> Vec<String> {
        let mut merged = self.shell_env.clone();
        if self.kind {
            for name in KIND_UNSET_VARS {
                merged.remove(name);
            }
            merged.insert("HK_INSTALL_KIND".into(), "true".into());
            merged.insert("NO_DEV_ENV".into(), "true".into());
        }
        for (name, value) in [
            ("KUBECTL_CONTEXT", &self.context),
            ("KUBECONFIG", &self.kubeconfig_env),
            ("BUCKET_NAME", &self.bucket_name),
            ("SNAPSHOT_LOCATION", &self.snapshot_location),
            ("HK_BUILD_REPO", &self.build_repo),
            ("HK_BUILD_PLATFORMS", &self.build_platforms),
            ("PROJECT_ID", &self.project_id),
            ("CLUSTER_NAME", &self.cluster_name),
            ("CLUSTER_LOCATION", &self.cluster_location),
            ("HK_OTLP_ENDPOINT", &self.otlp_endpoint),
        ] {
            // Empty means not configured; an empty variable would defeat
            // `${VAR:-default}` fallbacks.
            if value.is_empty() {
                merged.remove(name);
            } else {
                merged.insert(name.into(), value.clone());
            }
        }
        if !self.rollout_timeout.is_zero() {
            merged.insert(
                "HK_INSTALL_ROLLOUT_TIMEOUT".into(),
                format_duration(self.rollout_timeout),
            );
        }
        if self.podcert_workers_per_signer > 0 {
            merged.insert(
                "HK_INSTALL_PODCERT_WORKERS_PER_SIGNER".into(),
                self.podcert_workers_per_signer.to_string(),
            );
        }
        if self.envoy_concurrency > 0 {
            merged.insert(
                "HK_E2E_ENVOY_CONCURRENCY".into(),
                self.envoy_concurrency.to_string(),
            );
        }
        // Resolved values replace whatever the environment carried.
        merged.remove("HK_INSTALL_CLUSTER_SIZE");
        if !self.cluster_size.is_empty() && self.cluster_size != CLUSTER_SIZE_SIZE0 {
            merged.insert("HK_INSTALL_CLUSTER_SIZE".into(), self.cluster_size.clone());
        }
        merged.remove("HK_PREVIEW");
        if !self.preview_gates.is_empty() {
            merged.insert("HK_PREVIEW".into(), self.preview_gates.join(","));
        }
        for (name, on) in [
            (
                "HK_INSTALL_CORDON_SYSTEM_INFRA_COMPONENTS",
                self.cordon_system_infra_components,
            ),
            ("HK_INSTALL_FAKE_DATA_PLANE", self.fake_data_plane),
            ("HK_INSTALL_E2E_TUNING", self.e2e_tuning),
        ] {
            merged.remove(name);
            if on {
                merged.insert(name.into(), "true".into());
            }
        }
        merged.remove("HK_INGRESS_ALLOWED_SPIFFE_IDS");
        if !self.ingress_allowed_spiffe_ids.is_empty() {
            merged.insert(
                "HK_INGRESS_ALLOWED_SPIFFE_IDS".into(),
                self.ingress_allowed_spiffe_ids.join(","),
            );
        }
        if !self.additional_egress_extproc_service.is_empty() {
            merged.insert(
                "HK_ADDITIONAL_EGRESS_EXTPROC_SERVICE".into(),
                self.additional_egress_extproc_service.clone(),
            );
        }
        if !self.credential_provider_json.is_empty() {
            merged.insert(
                "HK_CREDENTIAL_PROVIDER".into(),
                self.credential_provider_json.clone(),
            );
        }
        merged
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect()
    }

    /// Environment overrides the source image builder needs.
    pub fn build_env(&self) -> Vec<String> {
        let mut env = Vec::new();
        if !self.build_repo.is_empty() {
            env.push(format!("HK_BUILD_REPO={}", self.build_repo));
        }
        if !self.build_platforms.is_empty() {
            env.push(format!("HK_BUILD_PLATFORMS={}", self.build_platforms));
        }
        env
    }

    /// Parses `--credential-provider`: a JSON object that turns injection off
    /// with `enabled: false`, or names a provider and its address. Only the
    /// bundled provider has a default address. A missing value is reported
    /// here because only the deploys that render the egress gateway need one.
    pub fn credential_provider(&self) -> Result<CredentialProvider, ConfigError> {
        parse_credential_provider(&self.credential_provider_json)
    }
}

/// See [`Config::credential_provider`].
pub fn parse_credential_provider(raw: &str) -> Result<CredentialProvider, ConfigError> {
    if raw.is_empty() {
        return Err(ConfigError::Other(format!(
            "a credential provider is required; set one of {}: {CREDENTIAL_PROVIDER_USAGE}",
            channels_for(CREDENTIAL_PROVIDER_KEY)
        )));
    }
    let invalid =
        |why: String| ConfigError::Other(format!("invalid --credential-provider '{raw}': {why}"));

    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Spec {
        enabled: Option<bool>,
        #[serde(default)]
        name: String,
        #[serde(default)]
        address: String,
    }
    let spec: Spec = match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(v) if v.is_object() => serde_json::from_value(v)
            .map_err(|e| invalid(format!("{e}; want {CREDENTIAL_PROVIDER_USAGE}")))?,
        Ok(_) => return Err(invalid("want a single JSON object".into())),
        Err(e) if e.is_syntax() && e.to_string().contains("trailing") => {
            return Err(invalid("want a single JSON object".into()))
        }
        Err(e) => return Err(invalid(format!("{e}; want {CREDENTIAL_PROVIDER_USAGE}"))),
    };

    if spec.enabled == Some(false) {
        if !spec.name.is_empty() || !spec.address.is_empty() {
            return Err(invalid("enabled false takes no name or address".into()));
        }
        return Ok(CredentialProvider::default());
    }
    if spec.name.is_empty() {
        return Err(invalid(format!(
            "name is required; want {CREDENTIAL_PROVIDER_USAGE}"
        )));
    }
    if let Err(errs) = dns1123_subdomain(&spec.name) {
        return Err(invalid(format!(
            "name {:?} is not a valid DNS name: {}",
            spec.name,
            errs.join("; ")
        )));
    }
    let address = if spec.address.is_empty() {
        if spec.name != K8S_CREDENTIAL_PROVIDER_NAME {
            return Err(invalid(
                "a provider you deploy needs an address as host:port".into(),
            ));
        }
        K8S_CREDENTIAL_PROVIDER_ADDRESS.to_string()
    } else {
        match split_host_port(&spec.address) {
            Some((h, p)) if !h.is_empty() && !p.is_empty() => spec.address.clone(),
            _ => {
                return Err(invalid(format!(
                    "address must be host:port, got {:?}",
                    spec.address
                )))
            }
        }
    };
    Ok(CredentialProvider {
        name: spec.name,
        address,
    })
}

/// Splits `host:port`, accepting a bracketed IPv6 host.
pub fn split_host_port(s: &str) -> Option<(&str, &str)> {
    if let Some(rest) = s.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        let port = after.strip_prefix(':')?;
        return Some((host, port));
    }
    let (host, port) = s.rsplit_once(':')?;
    if host.contains(':') {
        return None;
    }
    Some((host, port))
}

/// Kubernetes' DNS-1123 subdomain rule: at most 253 characters of
/// dot-separated lowercase alphanumeric labels that may contain `-` inside.
pub fn dns1123_subdomain(name: &str) -> Result<(), Vec<String>> {
    let mut errs = Vec::new();
    if name.len() > 253 {
        errs.push("must be no more than 253 characters".to_string());
    }
    let label_ok = |l: &str| {
        !l.is_empty()
            && l.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
            && !l.starts_with('-')
            && !l.ends_with('-')
    };
    if !name.split('.').all(label_ok) {
        errs.push(
            "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', \
             and must start and end with an alphanumeric character"
                .to_string(),
        );
    }
    if errs.is_empty() {
        Ok(())
    } else {
        Err(errs)
    }
}

/// Checks an `--experimental-additional-egress-extproc-service` value,
/// `<namespace>/<service>:<port>`, naming the channel it came from.
pub fn validate_extproc_service(v: &Value) -> Result<(), ConfigError> {
    const FORM: &str = "<namespace>/<service>:<port>";
    let parts: Vec<&str> = v.raw.split('/').collect();
    if parts.len() != 2 {
        return Err(ConfigError::invalid(v, FORM));
    }
    let svc_port: Vec<&str> = parts[1].split(':').collect();
    if svc_port.len() != 2 || parts[0].is_empty() || svc_port[0].is_empty() {
        return Err(ConfigError::invalid(v, FORM));
    }
    match svc_port[1].parse::<u32>() {
        Ok(p) if (1..=65535).contains(&p) => Ok(()),
        _ => Err(ConfigError::invalid(
            v,
            format!("{FORM}, with a port of 1-65535"),
        )),
    }
}

/// Locates the manifests directory: `$HK_MANIFESTS_DIR`, else
/// `crates/hivekeep-setup/manifests` above the working directory, else the
/// directory this crate was built from.
pub fn manifests_root() -> PathBuf {
    if let Ok(dir) = std::env::var(MANIFESTS_DIR_ENV) {
        if !dir.is_empty() {
            return PathBuf::from(dir);
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        for dir in cwd.ancestors() {
            let candidate = dir.join("crates/hivekeep-setup/manifests");
            if candidate.join("install").is_dir() {
                return candidate;
            }
        }
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join("manifests")
}

/// A Config for tests: defaults everywhere, with the checked-in manifests.
pub fn test_config() -> Config {
    Config {
        root: Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."),
        manifests: Path::new(env!("CARGO_MANIFEST_DIR")).join("manifests"),
        namespace: installdefaults::SYSTEM_NAMESPACE.into(),
        router: ROUTER_ENVOY.into(),
        cluster_size: CLUSTER_SIZE_SIZE0.into(),
        actor_jwt_algorithm: "ES256".into(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests;

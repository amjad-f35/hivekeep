//! Resolving the effective configuration: the channels layered, the Kind
//! profile applied, and the result validated and projected onto [`Config`].

use super::errors::ConfigError;
use super::file::{parse_file, File};
use super::registry::{lookup, KIND_ENABLED_KEY};
use super::resolve::{resolve, Flags, Origin, ResolveOptions, Resolved, Value};
use super::setting::Parsed;
use super::shellenv::{repo_root, source_shell_env};
use super::{
    manifests_root, validate_extproc_service, CloudSqlConfig, Config, CLOUDSQL_IP_TYPE_PRIVATE,
    CLOUDSQL_IP_TYPE_PSC, CLOUDSQL_IP_TYPE_PUBLIC, CLUSTER_SIZE_SIZE0, CLUSTER_SIZE_SIZE10,
    DEV_ENV_FILE, ROUTER_ENVOY,
};
use crate::images;
use crate::installdefaults;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Names the configuration document when `--config` is not given, so the
/// file channel survives wrappers that forward only the environment.
pub const CONFIG_PATH_ENV: &str = "HK_CONFIG";

/// Inputs that select where configuration comes from, as opposed to the
/// settings themselves.
#[derive(Clone, Debug, Default)]
pub struct LoadOptions {
    /// The `--config` document. Empty falls back to `$HK_CONFIG`, then to no
    /// file layer.
    pub config_path: Option<PathBuf>,
    /// Skip sourcing the developer env file even when it exists.
    pub no_dev_env: bool,
    /// The repository root; `None` finds it from the working directory.
    pub root: Option<PathBuf>,
}

/// The process environment as a map.
pub fn environ() -> BTreeMap<String, String> {
    std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect()
}

/// Resolves the effective configuration.
///
/// Precedence, lowest to highest: the developer env file, the process
/// environment, the configuration document, flags. The document outranks the
/// environment because it is the artifact an operator owns and reviews.
pub fn load(flags: &Flags, opts: LoadOptions) -> Result<Config, ConfigError> {
    let root = match opts.root.clone() {
        Some(r) => r,
        None => repo_root()?,
    };
    let mut env = environ();

    // Parsed before the Kind question is answered, because it is one of the
    // channels that answers it.
    let file = match config_path_from(&opts) {
        Some(path) => Some(parse_file(&path)?),
        None => None,
    };

    let kind = resolve_kind(flags, file.as_ref(), &env);
    merge_dev_env(&root, &mut env, opts.no_dev_env, kind)?;

    let r = resolve(
        Some(flags),
        ResolveOptions {
            file: file.as_ref(),
            env: Some(&env),
        },
    )?;
    build_config(&root, &manifests_root(), env, r)
}

/// The document to read, flag over environment.
fn config_path_from(o: &LoadOptions) -> Option<PathBuf> {
    if let Some(p) = o.config_path.as_ref().filter(|p| !p.as_os_str().is_empty()) {
        return Some(p.clone());
    }
    std::env::var_os(CONFIG_PATH_ENV)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// Answers the Kind question before the dev env is sourced, layering the
/// same channels in the same order resolution does. Answering from the flag
/// and the environment alone would make a document that selects Kind still
/// source the dev env, pointing a local install at a cloud project.
fn resolve_kind(flags: &Flags, file: Option<&File>, env: &BTreeMap<String, String>) -> bool {
    let Some(s) = lookup(KIND_ENABLED_KEY) else {
        return false;
    };
    let as_bool = |raw: &str| match s.parse(raw) {
        Ok(Parsed::Bool(b)) => Some(b),
        _ => None,
    };
    // Read even when false: --kind=false overrides an inherited variable.
    if let Some(b) = flags.changed(&s.flag).and_then(as_bool) {
        return b;
    }
    if let Some(b) = file.and_then(|f| f.get(&s.key)).and_then(as_bool) {
        return b;
    }
    env.get(&s.env)
        .and_then(|raw| as_bool(raw))
        .unwrap_or(false)
}

/// Layers the dev env file underneath the process environment. Skipped for
/// Kind installs: its cloud-shaped variables would point a local install at
/// a cloud project.
fn merge_dev_env(
    root: &Path,
    env: &mut BTreeMap<String, String>,
    no_dev_env: bool,
    kind: bool,
) -> Result<(), ConfigError> {
    if no_dev_env || kind || std::env::var_os("NO_DEV_ENV").is_some_and(|v| !v.is_empty()) {
        return Ok(());
    }
    let path = root.join(DEV_ENV_FILE);
    if !path.exists() {
        return Ok(());
    }
    let sourced = source_shell_env(&path, root)
        .map_err(|e| ConfigError::Other(format!("while sourcing {DEV_ENV_FILE}: {e}")))?;
    // An explicitly exported variable still wins over the file.
    for (k, v) in sourced {
        env.entry(k).or_insert(v);
    }
    Ok(())
}

/// Splits a comma-separated value, dropping empty items.
pub fn split_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Divides the kubeconfig setting into the single path handed to the client
/// and the value exported to helper scripts. A PATH-style list from the
/// environment is left to the default loading rules, which merge it.
fn split_kubeconfig(r: &Resolved) -> (String, String) {
    let v = r.value_or_default("kubeconfig");
    if matches!(v.from, Origin::Flag | Origin::File) {
        return (v.raw.clone(), v.raw);
    }
    let sep = if cfg!(windows) { ';' } else { ':' };
    if v.raw.contains(sep) {
        return (String::new(), v.raw);
    }
    (v.raw.clone(), v.raw)
}

/// The Docker platform architecture of this host.
pub fn host_platform_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "arm" => "arm",
        "powerpc64" => "ppc64le",
        "s390x" => "s390x",
        other => other,
    }
}

/// Whether a flag or document supplied the setting, as opposed to the
/// ambient environment or a default.
fn named_explicitly(r: &Resolved, key: &str) -> bool {
    r.value(key)
        .is_some_and(|v| matches!(v.from, Origin::Flag | Origin::File))
}

/// Fills the Kind-only settings, leaving alone any a channel supplied. The
/// values go into the resolved set so the report and the record name what
/// the install uses.
pub fn apply_kind_defaults(r: &mut Resolved) {
    // A Kind cluster has no GCP project; cleared only when something named
    // one, so the profile is not reported as changing an empty setting.
    for key in [
        "gcp.projectID",
        "gcp.clusterLocation",
        "storage.transferGSAName",
    ] {
        if !r.string(key).is_empty() {
            r.set(key, "", Origin::KindProfile);
        }
    }
    if r.string("context").is_empty() {
        let ctx = format!("kind-{}", r.string("kindCluster.name"));
        r.set("context", &ctx, Origin::KindProfile);
    }
    if r.string("build.repo").is_empty() {
        r.set("build.repo", "localhost:5001", Origin::KindProfile);
    }
    // Ambient values point at cloud registries and buckets a local install
    // must not inherit; values named on this run are kept.
    if !named_explicitly(r, "build.platforms") {
        r.set(
            "build.platforms",
            &format!("linux/{}", host_platform_arch()),
            Origin::KindProfile,
        );
    }
    if !named_explicitly(r, "storage.bucketName") {
        r.set(
            "storage.bucketName",
            "hivekeep-snapshots",
            Origin::KindProfile,
        );
    }
    if !named_explicitly(r, "storage.snapshotLocation") {
        let loc = format!("gs://{}/", r.string("storage.bucketName"));
        r.set("storage.snapshotLocation", &loc, Origin::KindProfile);
    }
}

/// Projects the resolved settings onto [`Config`] and validates them.
pub fn build_config(
    root: &Path,
    manifests: &Path,
    env: BTreeMap<String, String>,
    mut r: Resolved,
) -> Result<Config, ConfigError> {
    if r.bool(KIND_ENABLED_KEY) {
        apply_kind_defaults(&mut r);
    }

    let rollout_timeout = r.duration("rolloutTimeout");
    if r.supplied("rolloutTimeout") && rollout_timeout.is_zero() {
        // A zero wait is a single probe against a workload not yet started;
        // there is no unbounded wait to offer either.
        return Err(ConfigError::invalid(
            &r.value_or_default("rolloutTimeout"),
            "a positive duration",
        ));
    }
    let podcert_workers = r.int("podcert.workersPerSigner");
    if r.supplied("podcert.workersPerSigner") && podcert_workers < 1 {
        return Err(ConfigError::invalid(
            &r.value_or_default("podcert.workersPerSigner"),
            "a positive integer",
        ));
    }
    let envoy_concurrency = r.int("e2e.envoyConcurrency");
    if r.supplied("e2e.envoyConcurrency") && envoy_concurrency < 1 {
        return Err(ConfigError::invalid(
            &r.value_or_default("e2e.envoyConcurrency"),
            "a positive integer",
        ));
    }

    let (kubeconfig, kubeconfig_env) = split_kubeconfig(&r);
    let read_write = r.string("api.postgres.readWrite.connectionString");
    let mut owner = r.string("api.postgres.owner.connectionString");
    if owner.is_empty() {
        owner = read_write.clone();
    }
    let instance = r.value_or_default("api.postgres.cloudsql.instance");
    let namespace = r.string("namespace");
    let mut actor_jwt_issuer = r.string("actorJWT.issuer");
    if actor_jwt_issuer.is_empty() {
        actor_jwt_issuer = installdefaults::actor_jwt_issuer(&namespace);
    }
    // hivectl and the demo steps fall back to tokens minted from the client
    // ServiceAccount, so it is the default bootstrap owner.
    let mut bootstrap_owners = r.string("api.authz.bootstrapOwners");
    if bootstrap_owners.is_empty() {
        bootstrap_owners = installdefaults::client_principal(&namespace);
    }

    let mut cfg = Config {
        root: root.to_path_buf(),
        manifests: manifests.to_path_buf(),
        kind: r.bool(KIND_ENABLED_KEY),
        namespace,
        kubeconfig,
        context: r.string("context"),
        project_id: r.string("gcp.projectID"),
        cluster_name: r.string("gcp.clusterName"),
        cluster_location: r.string("gcp.clusterLocation"),
        expected_jwt_issuer: r.string("api.expectedJWTIssuer"),
        api_authz_bootstrap_owners: bootstrap_owners,
        actor_jwt_algorithm: r.string("actorJWT.algorithm"),
        actor_jwt_issuer,
        bucket_name: r.string("storage.bucketName"),
        snapshot_location: r.string("storage.snapshotLocation"),
        transfer_gsa_name: r.string("storage.transferGSAName"),
        build_repo: r.string("build.repo"),
        build_platforms: r.string("build.platforms"),
        docker_build_flags: r
            .string("docker.buildFlags")
            .split_whitespace()
            .map(str::to_string)
            .collect(),
        images: images::Source {
            repo: r.string("images.repo").trim_end_matches('/').to_string(),
            tag: r.string("images.tag"),
        },
        router: r.string("gate.dataplane"),
        postgres_read_write_connection_string: read_write,
        postgres_owner_connection_string: owner,
        postgres_read_write_role: r.string("api.postgres.readWrite.role"),
        postgres_owner_role: r.string("api.postgres.owner.role"),
        postgres_read_write_role_set: r.supplied("api.postgres.readWrite.role"),
        postgres_owner_role_set: r.supplied("api.postgres.owner.role"),
        postgres_schema: r.string("api.postgres.schema"),
        postgres_pool_max_conns: r.string("api.postgres.poolMaxConns"),
        postgres_server_ca_file: r.string("api.postgres.serverCAFile"),
        postgres_storage_class: r.string("api.postgres.storageClass"),
        cloud_sql: CloudSqlConfig {
            instance: instance.raw.clone(),
            // Any channel may say "explicitly empty", so a file can clear it.
            instance_set: instance.supplied,
            gsa: r.string("api.postgres.cloudsql.gsa"),
            iam_auth: r.string("api.postgres.cloudsql.iamAuth"),
            ip_type: r.string("api.postgres.cloudsql.ipType"),
        },
        rollout_timeout,
        rollout_timeout_set: r.supplied("rolloutTimeout"),
        podcert_workers_per_signer: podcert_workers,
        envoy_concurrency,
        cluster_size: r.string("clusterSize"),
        cordon_system_infra_components: r.bool("cordonSystemInfraComponents"),
        fake_data_plane: r.bool("fakeDataPlane.enabled"),
        e2e_tuning: r.bool("e2e.tuning"),
        ingress_allowed_spiffe_ids: split_list(&r.string("ingressAllowedSPIFFEIDs")),
        additional_egress_extproc_service: r.string("gate.egress.additionalExtprocService"),
        credential_provider_json: r.string("gate.egress.credentialProvider"),
        anthropic_api_key: r.string("demo.anthropicAPIKey"),
        otlp_endpoint: r.string("otlpEndpoint"),
        preview_gates: split_list(&r.string("preview")),
        api_log_level: r.string("api.logLevel"),
        benchmark_actor_memory: r.string("benchmark.actorMemory"),
        kubeconfig_env,
        shell_env: env,
        ..Default::default()
    };
    validate_resolved(&cfg, &r)?;
    cfg.set_resolved(r);
    Ok(cfg)
}

fn from_desc(v: &Value) -> String {
    v.from.describe(&v.setting)
}

/// Rejects invalid and conflicting settings, naming the channel each value
/// came from.
fn validate_resolved(cfg: &Config, r: &Resolved) -> Result<(), ConfigError> {
    let val = |k: &str| r.value_or_default(k);

    // Checked here rather than on the source because only here is the
    // channel of each half known; a tag alone would otherwise be dropped in
    // silence, leaving a source build that looks like a release.
    let (repo, tag) = (val("images.repo"), val("images.tag"));
    if cfg.images.is_prebuilt() && cfg.images.tag.is_empty() {
        return Err(ConfigError::conflict(
            &repo,
            &tag,
            "a prebuilt image repository needs a tag",
        ));
    }
    if !cfg.images.is_prebuilt() && !cfg.images.tag.is_empty() {
        return Err(ConfigError::conflict(
            &tag,
            &repo,
            "without a repository, images are built from source and the tag names nothing",
        ));
    }

    let dataplane = val("gate.dataplane");
    if cfg.router != ROUTER_ENVOY {
        return Err(ConfigError::invalid(&dataplane, ROUTER_ENVOY));
    }

    let ip_type = val("api.postgres.cloudsql.ipType");
    if !ip_type.raw.is_empty()
        && ![
            CLOUDSQL_IP_TYPE_PRIVATE,
            CLOUDSQL_IP_TYPE_PUBLIC,
            CLOUDSQL_IP_TYPE_PSC,
        ]
        .contains(&ip_type.raw.as_str())
    {
        return Err(ConfigError::invalid(
            &ip_type,
            [
                CLOUDSQL_IP_TYPE_PRIVATE,
                CLOUDSQL_IP_TYPE_PUBLIC,
                CLOUDSQL_IP_TYPE_PSC,
            ]
            .join(", "),
        ));
    }

    if cfg.cluster_size != CLUSTER_SIZE_SIZE0 && cfg.cluster_size != CLUSTER_SIZE_SIZE10 {
        return Err(ConfigError::invalid(
            &val("clusterSize"),
            format!("{CLUSTER_SIZE_SIZE0} or {CLUSTER_SIZE_SIZE10}"),
        ));
    }

    // The router rejects a malformed ID at startup; catch it first.
    for id in &cfg.ingress_allowed_spiffe_ids {
        if let Err(e) = hivekeep_core::spiffe::validate_spiffe_id(id) {
            return Err(ConfigError::invalid(
                &val("ingressAllowedSPIFFEIDs"),
                format!("a comma-separated list of SPIFFE IDs ({id:?}: {e})"),
            ));
        }
    }

    if !["", "debug", "info", "warn", "error"].contains(&cfg.api_log_level.as_str()) {
        return Err(ConfigError::invalid(
            &val("api.logLevel"),
            "debug, info, warn, or error",
        ));
    }

    if cfg.actor_jwt_algorithm != "ES256" && cfg.actor_jwt_algorithm != "RS256" {
        return Err(ConfigError::invalid(
            &val("actorJWT.algorithm"),
            "ES256 or RS256",
        ));
    }
    if let Err(e) = hivekeep_identity::jwt::claims::validate_issuer(&cfg.actor_jwt_issuer) {
        let issuer = val("actorJWT.issuer");
        return Err(ConfigError::Other(format!(
            "{}: {e} (from {})",
            issuer.setting.key,
            from_desc(&issuer)
        )));
    }

    if !cfg.additional_egress_extproc_service.is_empty() {
        let extproc = val("gate.egress.additionalExtprocService");
        validate_extproc_service(&extproc)?;
        if cfg.router != ROUTER_ENVOY {
            return Err(ConfigError::conflict(
                &extproc,
                &dataplane,
                format!("an additional ext_proc filter requires dataplane {ROUTER_ENVOY}"),
            ));
        }
    }

    // An unknown gate makes every component refuse to start.
    if let Err(e) = hivekeep_core::preview::PreviewGates::from_flags(&cfg.preview_gates) {
        let gates = val("preview");
        return Err(ConfigError::Other(format!(
            "{e} (from {})",
            from_desc(&gates)
        )));
    }

    // Parsed here so a malformed provider is rejected with the rest; whether
    // one is required at all is the deploy's question.
    if !cfg.credential_provider_json.is_empty() {
        if let Err(e) = cfg.credential_provider() {
            let provider = val("gate.egress.credentialProvider");
            return Err(ConfigError::Other(format!(
                "{e} (from {})",
                from_desc(&provider)
            )));
        }
    }

    // hivekeep-api exits without a bootstrap owner.
    if cfg
        .api_authz_bootstrap_owners
        .trim_matches([' ', ','])
        .is_empty()
    {
        return Err(ConfigError::invalid(
            &val("api.authz.bootstrapOwners"),
            "a list of at least one bootstrap owner",
        ));
    }
    Ok(())
}

//! Every setting the installer knows: the installer-owned registry plus the
//! settings commands register for themselves.

use super::setting::{Setting, ValueKind};
use super::{
    CLUSTER_SIZE_SIZE0, CREDENTIAL_PROVIDER_USAGE, DEFAULT_POSTGRES_OWNER_ROLE,
    DEFAULT_POSTGRES_READ_WRITE_ROLE, DEFAULT_ROLLOUT_TIMEOUT, ROUTER_ENVOY,
};
use crate::installdefaults;
use std::sync::OnceLock;

/// Resolved before the rest, because whether to source the developer
/// environment depends on it.
pub const KIND_ENABLED_KEY: &str = "kindCluster.enabled";

fn s(key: &str, env: &str, flag: &str, usage: &str) -> Setting {
    Setting::new(key, env, flag).usage(usage)
}

/// The settings the installer itself owns, in declaration order.
pub fn registry() -> &'static [Setting] {
    static REGISTRY: OnceLock<Vec<Setting>> = OnceLock::new();
    REGISTRY.get_or_init(build_registry)
}

fn build_registry() -> Vec<Setting> {
    use ValueKind::*;
    let rollout_default = super::setting::format_duration(DEFAULT_ROLLOUT_TIMEOUT);
    vec![
        // Not "kind": a top-level kind would collide with the document's own
        // kind header field.
        s(KIND_ENABLED_KEY, "HK_INSTALL_KIND", "kind",
            "Target a local Kind cluster: kind overlays, the local registry, and host-architecture builds")
            .kind(Bool).default_value("false"),
        s("kindCluster.name", "KIND_CLUSTER_NAME", "kind-cluster-name", "Name of the Kind cluster to target")
            .default_value("kind"),
        s("namespace", "HK_NAMESPACE", "namespace", "Namespace the control plane is installed into")
            .default_value(installdefaults::SYSTEM_NAMESPACE),
        s("kubeconfig", "KUBECONFIG", "kubeconfig", "Path to the kubeconfig file"),
        s("context", "KUBECTL_CONTEXT", "context", "Name of the kubeconfig context to use"),
        s("gcp.projectID", "PROJECT_ID", "gcp-project-id", "GCP project holding the target cluster"),
        s("gcp.clusterName", "CLUSTER_NAME", "gcp-cluster-name",
            "GKE cluster name, used to fetch credentials and derive the JWT issuer"),
        s("gcp.clusterLocation", "CLUSTER_LOCATION", "gcp-cluster-location", "GKE cluster location"),
        s("build.repo", "HK_BUILD_REPO", "build-repo", "Registry images built from source are pushed to"),
        s("build.platforms", "HK_BUILD_PLATFORMS", "build-platforms",
            "Platforms images built from source are built for, comma-separated (linux/amd64,linux/arm64)"),
        s("actorJWT.algorithm", "ACTOR_JWT_ALGORITHM", "actor-jwt-algorithm",
            "Signing algorithm of the key in a new actor JWT pool: ES256 or RS256")
            .default_value("ES256"),
        // The default depends on the namespace, so building the Config applies it.
        s("actorJWT.issuer", "ACTOR_JWT_ISSUER", "actor-jwt-issuer",
            "iss claim of actor JWTs, as https://<host> with no path. Relying parties fetch \
             <issuer>/.well-known/openid-configuration (default https://idp.<namespace>.svc)"),
        // Whitespace-separated, as the environment variable is.
        s("docker.buildFlags", "DOCKER_BUILD_FLAGS", "docker-build-flags",
            "Extra docker buildx build flags for image builds, whitespace-separated"),
        s("images.repo", "HK_IMAGE_REPO", "image-repo",
            "Install pre-built images from this registry path instead of building from source"),
        s("images.tag", "HK_IMAGE_TAG", "image-tag", "Tag the pre-built images carry (required with --image-repo)"),
        s("storage.bucketName", "BUCKET_NAME", "storage-bucket-name", "Snapshot bucket the demos are templated with"),
        s("storage.snapshotLocation", "SNAPSHOT_LOCATION", "snapshot-location",
            "Cluster-wide object-storage URI hivekeep-api stores snapshots under, for example gs://BUCKET/PREFIX/ \
             (required if the PerTemplateSnapshotLocation preview gate is not enabled)"),
        s("storage.transferGSAName", "SNAPSHOT_TRANSFER_GSA_NAME", "snapshot-transfer-gsa-name",
            "Name of the Google service account snapshot transfers run as on GCS, in gcp.projectID. \
             hivekeep-api's credential exchange impersonates it (required to deploy hivekeep-api on GKE, unused on kind)"),
        s("gate.dataplane", "HK_GATE_DATAPLANE", "gate-dataplane",
            "Gate ingress and egress dataplane: envoy")
            .default_value(ROUTER_ENVOY),
        s("gate.egress.additionalExtprocService", "HK_ADDITIONAL_EGRESS_EXTPROC_SERVICE",
            "experimental-additional-egress-extproc-service",
            "Run an additional ext_proc authorization filter served by NS/SVC:PORT (requires --gate-dataplane=envoy)"),
        // No default: an unset level keeps the one an existing install has.
        s("api.logLevel", "HK_API_LOG_LEVEL", "api-log-level",
            "hivekeep-api log level: debug, info, warn, or error (default: keep the installed level, else info)"),
        s("api.expectedJWTIssuer", "EXPECTED_JWT_ISSUER", "api-expected-jwt-issuer",
            "Service account token issuer hivekeep-api trusts, overriding derivation and discovery"),
        s("api.authz.bootstrapOwners", "HK_API_AUTHZ_BOOTSTRAP_OWNERS", "api-authz-bootstrap-owners",
            "Comma-separated principals that always hold owner on global:root. Unset means hivectl's \
             ServiceAccount, system:serviceaccount:<namespace>:hivekeep-client"),
        // A JSON object carried as one value on one flag, so every channel
        // accepts the same text.
        s("gate.egress.credentialProvider", "HK_CREDENTIAL_PROVIDER", "credential-provider",
            &format!("Credential provider the egress gateway injects credentials from, as JSON; required by \
                      deploy hivekeep-system and deploy gate. {CREDENTIAL_PROVIDER_USAGE}. A provider requires \
                      --gate-dataplane=envoy")),
        // Both pools use the read-write connection when only it is set; both
        // empty selects the bundled PostgreSQL.
        Setting::new("api.postgres.readWrite.connectionString", "HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING", "")
            .secret()
            .usage("PostgreSQL connection string hivekeep-api uses for normal application access"),
        Setting::new("api.postgres.owner.connectionString", "HK_API_POSTGRES_OWNER_CONNECTION_STRING", "")
            .secret()
            .usage("PostgreSQL connection string hivekeep-api uses for schema changes, defaulting to the read-write one"),
        s("api.postgres.readWrite.role", "HK_API_POSTGRES_READ_WRITE_ROLE", "api-postgres-read-write-role",
            "PostgreSQL role owning normal application access")
            .default_value(DEFAULT_POSTGRES_READ_WRITE_ROLE),
        s("api.postgres.owner.role", "HK_API_POSTGRES_OWNER_ROLE", "api-postgres-owner-role",
            "PostgreSQL role owning the schema")
            .default_value(DEFAULT_POSTGRES_OWNER_ROLE),
        s("api.postgres.schema", "HK_API_POSTGRES_SCHEMA", "api-postgres-schema",
            "PostgreSQL schema holding the Hivekeep tables"),
        s("api.postgres.poolMaxConns", "HK_API_POSTGRES_POOL_MAX_CONNS", "api-postgres-pool-max-conns",
            "Maximum connections in hivekeep-api's database pool"),
        s("api.postgres.serverCAFile", "HK_API_POSTGRES_SERVER_CA_FILE", "api-postgres-server-ca-file",
            "Local PEM file holding an external PostgreSQL server CA"),
        s("api.postgres.storageClass", "HK_API_POSTGRES_STORAGE_CLASS", "api-postgres-storage-class",
            "StorageClass for the bundled PostgreSQL volume, instead of the cluster default. An existing install \
             must delete the postgres StatefulSet and its claim first"),
        s("api.postgres.cloudsql.instance", "HK_API_POSTGRES_CLOUDSQL_INSTANCE", "api-postgres-cloudsql-instance",
            "Cloud SQL instance connection name, PROJECT:REGION:INSTANCE"),
        s("api.postgres.cloudsql.gsa", "HK_API_POSTGRES_CLOUDSQL_GSA", "api-postgres-cloudsql-gsa",
            "Google service account the Cloud SQL Auth Proxy authenticates as"),
        s("api.postgres.cloudsql.iamAuth", "HK_API_POSTGRES_CLOUDSQL_IAM_AUTH", "api-postgres-cloudsql-iam-auth",
            "Enable automatic IAM database authentication: true or false"),
        s("api.postgres.cloudsql.ipType", "HK_API_POSTGRES_CLOUDSQL_IP_TYPE", "api-postgres-cloudsql-ip-type",
            "Instance address the Cloud SQL proxy dials: private, public or psc"),
        s("podcert.workersPerSigner", "HK_INSTALL_PODCERT_WORKERS_PER_SIGNER", "podcert-workers-per-signer",
            "Workers per signer in podcertificate-controller")
            .kind(Int).default_value("0"),
        s("e2e.tuning", "HK_INSTALL_E2E_TUNING", "e2e-tuning",
            "Apply the e2e test component (short keeper snapshot delays for end-to-end test runs)")
            .kind(Bool).default_value("false"),
        s("e2e.envoyConcurrency", "HK_E2E_ENVOY_CONCURRENCY", "e2e-envoy-concurrency",
            "Worker threads (--concurrency) for the Envoy container in gate-egress")
            .kind(Int).default_value("0"),
        s("rolloutTimeout", "HK_INSTALL_ROLLOUT_TIMEOUT", "rollout-timeout",
            "Timeout for workload rollouts, as a duration string")
            .kind(Duration).default_value(&rollout_default),
        s("clusterSize", "HK_INSTALL_CLUSTER_SIZE", "cluster-size", "Coarse-grained sizing of Hivekeep: size0 or size10")
            .default_value(CLUSTER_SIZE_SIZE0),
        s("cordonSystemInfraComponents", "HK_INSTALL_CORDON_SYSTEM_INFRA_COMPONENTS", "cordon-system-infra-components",
            "Keep the system infra components off the worker nodes. Assumes a small shared pool labeled and tainted \
             hivekeep.io/workloadType=hivekeep-system-infra:NoSchedule, and a one-node pool labeled and tainted \
             hivekeep.io/workloadType=hivekeep-postgres:NoSchedule for postgres alone")
            .kind(Bool).default_value("false"),
        s("fakeDataPlane.enabled", "HK_INSTALL_FAKE_DATA_PLANE", "fake-data-plane",
            "Install the fake data plane for control-plane benchmarks: fake-keeper in place of keeper, and \
             fake-workersync in place of hivekeep-controller. Actors do not exist; never use it on a cluster \
             that serves real actors")
            .kind(Bool).default_value("false"),
        // Comma-separated, as the router's own flag is.
        s("ingressAllowedSPIFFEIDs", "HK_INGRESS_ALLOWED_SPIFFE_IDS", "ingress-allowed-spiffe-ids",
            "SPIFFE IDs (exact match, comma-separated) of the clients the ingress router lets reach actors; any \
             listed client may reach any actor. Unset, no client may reach actors through the router. The \
             podidentity signer issues spiffe://cluster.local/ns/<namespace>/sa/<service-account>"),
        s("otlpEndpoint", "HK_OTLP_ENDPOINT", "otlp-endpoint",
            "Send control plane telemetry to this OTLP collector instead of the cluster default"),
        // Comma-separated, as the components' own --preview flag is.
        s("preview", "HK_PREVIEW", "preview",
            "Preview gates every Hivekeep component runs with, comma-separated; ALL enables all"),
        s("benchmark.actorMemory", "BENCHMARK_ACTOR_MEMORY", "benchmark-actor-memory", "Memory limit for benchmark actors"),
        s("record.dir", "HK_RECORD_DIR", "record-dir",
            "Directory install records are written to (default: the user cache directory)"),
        Setting::new("demo.anthropicAPIKey", "ANTHROPIC_API_KEY", "")
            .secret()
            .usage("API key required by the claude-code-multiplex demo"),
    ]
}

/// Runs `f` over the command-registered settings.
///
/// Test builds keep them per thread: each test thread starts empty and its
/// registrations cannot leak into a test running beside it.
#[cfg(not(test))]
fn with_scoped<R>(f: impl FnOnce(&mut Vec<Setting>) -> R) -> R {
    static SCOPED: OnceLock<std::sync::Mutex<Vec<Setting>>> = OnceLock::new();
    let mut guard = SCOPED
        .get_or_init(std::sync::Mutex::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    f(&mut guard)
}

#[cfg(test)]
fn with_scoped<R>(f: impl FnOnce(&mut Vec<Setting>) -> R) -> R {
    thread_local! {
        static SCOPED: std::cell::RefCell<Vec<Setting>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    SCOPED.with(|s| f(&mut s.borrow_mut()))
}

/// The command-registered settings, for a test to restore afterwards.
#[cfg(test)]
pub(crate) fn snapshot_scoped() -> Vec<Setting> {
    with_scoped(|s| s.clone())
}

#[cfg(test)]
pub(crate) fn restore_scoped(saved: Vec<Setting>) {
    with_scoped(|s| *s = saved);
}

/// Records settings owned by one command, to be called while the command
/// tree is built. A declaration identical to one already registered is
/// skipped, so building the tree twice is harmless; a different declaration
/// under a taken name is kept, and [`validate`] reports the clash.
pub fn register_command(settings: impl IntoIterator<Item = Setting>) {
    with_scoped(|list| {
        for s in settings {
            if !list.contains(&s) {
                list.push(s);
            }
        }
    });
}

/// Every setting, installer-owned then command-registered, in declaration
/// order. The file and the environment read this, so a document parses the
/// same whichever subcommand runs.
pub fn all() -> Vec<Setting> {
    let mut out = registry().to_vec();
    with_scoped(|s| out.extend(s.iter().cloned()));
    out
}

/// The setting with the canonical key. The latest registration of a key
/// wins, command-registered over installer-owned.
pub fn lookup(key: &str) -> Option<Setting> {
    if let Some(s) = with_scoped(|list| list.iter().rev().find(|s| s.key == key).cloned()) {
        return Some(s);
    }
    registry().iter().find(|s| s.key == key).cloned()
}

/// The channels of a registered key, or empty for an unknown one.
pub fn channels_for(key: &str) -> String {
    lookup(key).map(|s| s.channels()).unwrap_or_default()
}

/// Every canonical key, sorted.
pub fn keys() -> Vec<String> {
    let mut out: Vec<String> = all().into_iter().map(|s| s.key).collect();
    out.sort();
    out
}

/// Reports duplicate names, missing channels and secrets that declare a
/// flag across the merged set.
pub fn validate() -> Result<(), String> {
    validate_settings(&all())
}

/// [`validate`] over an explicit list.
pub fn validate_settings(settings: &[Setting]) -> Result<(), String> {
    let mut problems = Vec::new();
    type Field = (&'static str, fn(&Setting) -> &str, fn(&Setting) -> bool);
    let fields: [Field; 3] = [
        ("key", |s| &s.key, |_| false),
        ("environment variable", |s| &s.env, |_| false),
        ("flag", |s| &s.flag, |s| s.secret),
    ];
    for (name, get, omittable) in fields {
        let mut seen: std::collections::HashMap<String, String> = Default::default();
        for s in settings {
            let v = get(s);
            if v.is_empty() {
                if !omittable(s) {
                    problems.push(format!("setting {:?} has no {name}", s.key));
                }
                continue;
            }
            if let Some(prev) = seen.get(v) {
                problems.push(format!(
                    "{name} {v:?} is claimed by both {prev} and {}",
                    s.key
                ));
            }
            seen.insert(v.to_string(), s.key.clone());
        }
    }
    for s in settings {
        if s.secret && !s.flag.is_empty() {
            problems.push(format!(
                "secret setting {:?} declares flag {:?}; a credential must not be a flag",
                s.key, s.flag
            ));
        }
    }
    if problems.is_empty() {
        return Ok(());
    }
    problems.sort();
    Err(format!(
        "invalid setting registry:\n  {}",
        problems.join("\n  ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::document::DOCUMENT_METADATA_KEY;
    use crate::config::file::parse_file;
    use crate::config::setting::Parsed;
    use crate::config::testutil::{test_setting, write_config, TestEnv, VALID_HEADER};

    #[test]
    fn registry_covers_every_channel() {
        for s in registry() {
            assert!(!s.key.is_empty());
            assert!(!s.env.is_empty(), "{} has no environment variable", s.key);
            assert!(!s.flag.is_empty() || s.secret, "{} has no flag", s.key);
            assert!(!s.usage.is_empty(), "{} has no usage", s.key);
        }
    }

    #[test]
    fn registry_names_are_unique() {
        type Get = fn(&Setting) -> &str;
        let getters: [(&str, Get); 3] = [
            ("key", |s| &s.key),
            ("env", |s| &s.env),
            ("flag", |s| &s.flag),
        ];
        for (field, get) in getters {
            let mut seen = std::collections::HashMap::new();
            for s in registry() {
                let v = get(s);
                if v.is_empty() {
                    continue;
                }
                if let Some(prev) = seen.insert(v.to_string(), s.key.clone()) {
                    panic!("{field} {v:?} used by both {prev} and {}", s.key);
                }
            }
        }
    }

    #[test]
    fn registry_defaults_parse() {
        for s in registry() {
            if !s.default.is_empty() {
                s.parse(&s.default)
                    .unwrap_or_else(|e| panic!("{}: {e}", s.key));
            }
        }
    }

    #[test]
    fn lookup_finds_declared_settings() {
        assert!(lookup("gate.dataplane").is_some());
        assert!(lookup("no.such.setting").is_none());
    }

    #[test]
    fn setting_parse() {
        use ValueKind::*;
        let cases: Vec<(&str, ValueKind, &str, Option<Parsed>)> = vec![
            (
                "string passes through",
                String,
                "envoy",
                Some(Parsed::String("envoy".into())),
            ),
            ("empty string", String, "", Some(Parsed::String("".into()))),
            ("bool true", Bool, "true", Some(Parsed::Bool(true))),
            ("bool 1", Bool, "1", Some(Parsed::Bool(true))),
            ("bool false", Bool, "false", Some(Parsed::Bool(false))),
            ("bool 0", Bool, "0", Some(Parsed::Bool(false))),
            ("bool empty is false", Bool, "", Some(Parsed::Bool(false))),
            ("bool rejects other", Bool, "yes", None),
            ("int", Int, "4", Some(Parsed::Int(4))),
            ("int rejects words", Int, "four", None),
            ("int rejects a trailing suffix", Int, "3x", None),
            ("int rejects a decimal", Int, "2.5", None),
            ("int rejects hex", Int, "0x10", None),
            ("int rejects exponent notation", Int, "1e3", None),
            ("int rejects a second number", Int, "12 34", None),
            (
                "duration",
                Duration,
                "90s",
                Some(Parsed::Duration(std::time::Duration::from_secs(90))),
            ),
            ("duration rejects bare number", Duration, "90", None),
        ];
        for (name, kind, raw, want) in cases {
            let s = Setting {
                key: "test.key".into(),
                kind,
                ..Default::default()
            };
            match (s.parse(raw), want) {
                (Ok(got), Some(want)) => assert_eq!(got, want, "{name}"),
                (Err(e), None) => assert!(e.contains("test.key"), "{name}: {e}"),
                (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
            }
        }
    }

    #[test]
    fn keys_are_sorted() {
        let keys = keys();
        assert_eq!(keys.len(), registry().len());
        assert!(keys.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn registry_keys_do_not_collide_with_document_header() {
        for reserved in ["apiVersion", "kind", DOCUMENT_METADATA_KEY] {
            for s in registry() {
                assert_ne!(s.key.split('.').next().unwrap(), reserved, "{}", s.key);
            }
        }
    }

    #[test]
    fn secret_settings_have_no_flag() {
        for s in all() {
            if s.secret {
                assert!(s.flag.is_empty(), "--{} exposes a credential", s.flag);
                assert!(!s.env.is_empty(), "{}", s.key);
            }
        }
    }

    #[test]
    fn register_command_extends_all() {
        let mut t = TestEnv::new();
        let before = all().len();
        t.use_settings(vec![test_setting(
            "test.scoped",
            "HK_TEST_SCOPED",
            "test-scoped",
            &["deploy thing"],
        )]);
        assert_eq!(all().len(), before + 1);
        assert!(lookup("test.scoped").is_some());
        // Registering the same declaration again is harmless.
        register_command(vec![test_setting(
            "test.scoped",
            "HK_TEST_SCOPED",
            "test-scoped",
            &["deploy thing"],
        )]);
        assert_eq!(all().len(), before + 1);
    }

    #[test]
    fn scoped_settings_are_visible_to_the_file_channel() {
        let mut t = TestEnv::new();
        t.use_settings(vec![test_setting(
            "test.scoped",
            "HK_TEST_SCOPED",
            "test-scoped",
            &["deploy thing"],
        )]);
        let (_d, path) = write_config(&format!("{VALID_HEADER}test:\n  scoped: value\n"));
        assert_eq!(parse_file(&path).unwrap().get("test.scoped"), Some("value"));
    }

    #[test]
    fn validate_cases() {
        let cases: Vec<(&str, Vec<Setting>, Option<&str>)> = vec![
            (
                "clean",
                vec![test_setting(
                    "test.ok",
                    "HK_TEST_OK",
                    "test-ok",
                    &["deploy thing"],
                )],
                None,
            ),
            (
                "duplicate key",
                vec![
                    test_setting("test.dup", "HK_TEST_A", "test-a", &["deploy thing"]),
                    test_setting("test.dup", "HK_TEST_B", "test-b", &["deploy thing"]),
                ],
                Some("key \"test.dup\""),
            ),
            (
                "duplicate environment variable",
                vec![
                    test_setting("test.a", "HK_TEST_SAME", "test-a", &["deploy thing"]),
                    test_setting("test.b", "HK_TEST_SAME", "test-b", &["deploy thing"]),
                ],
                Some("environment variable \"HK_TEST_SAME\""),
            ),
            (
                "duplicate flag",
                vec![
                    test_setting("test.a", "HK_TEST_A", "test-same", &["deploy thing"]),
                    test_setting("test.b", "HK_TEST_B", "test-same", &["deploy thing"]),
                ],
                Some("flag \"test-same\""),
            ),
            (
                "collides with a setting the installer owns",
                vec![test_setting(
                    "test.a",
                    "HK_NAMESPACE",
                    "test-a",
                    &["deploy thing"],
                )],
                Some("namespace"),
            ),
            (
                "missing environment variable",
                vec![Setting::new("test.a", "", "test-a").usage("u")],
                Some("no environment variable"),
            ),
        ];
        for (name, add, want) in cases {
            let mut t = TestEnv::new();
            t.use_settings(add);
            match (validate(), want) {
                (Ok(()), None) => {}
                (Err(e), Some(w)) => assert!(e.contains(w), "{name}: {e}"),
                (got, _) => panic!("{name}: {got:?}"),
            }
        }
    }
}

//! Loading: precedence across channels, the Kind profile, validation, the
//! script environment, and the dev env file.
#![allow(clippy::type_complexity)]

use super::load::{build_config, host_platform_arch, load, LoadOptions, CONFIG_PATH_ENV};
use super::record::record_success;
use super::resolve::{resolve, Flags, ResolveOptions};
use super::setting::ValueKind;
use super::testutil::{
    load_options, options_flags, script_env_map, test_root, write_config, Options, TestEnv,
    VALID_HEADER,
};
use super::*;
use std::time::Duration;

fn opts() -> Options {
    Options::default()
}

#[test]
fn load_defaults() {
    let mut t = TestEnv::new();
    t.load_env();
    let cfg = load_options(&opts()).unwrap();
    assert_eq!(cfg.router, ROUTER_ENVOY);
    assert!(
        cfg.postgres_read_write_connection_string.is_empty()
            && cfg.postgres_owner_connection_string.is_empty()
    );
    assert_eq!(
        cfg.postgres_read_write_role,
        DEFAULT_POSTGRES_READ_WRITE_ROLE
    );
    assert_eq!(cfg.postgres_owner_role, DEFAULT_POSTGRES_OWNER_ROLE);
    assert_eq!(cfg.rollout_timeout, DEFAULT_ROLLOUT_TIMEOUT);
    assert_eq!(cfg.cluster_size, CLUSTER_SIZE_SIZE0);
    assert!(!cfg.cordon_system_infra_components);
}

#[test]
fn load_cluster_size() {
    let mut t = TestEnv::new();
    let cases: Vec<(&str, Options, Vec<(&str, &str)>, &str)> = vec![
        (
            "flag",
            Options {
                cluster_size: CLUSTER_SIZE_SIZE10.into(),
                ..opts()
            },
            vec![],
            CLUSTER_SIZE_SIZE10,
        ),
        (
            "environment",
            opts(),
            vec![("HK_INSTALL_CLUSTER_SIZE", CLUSTER_SIZE_SIZE10)],
            CLUSTER_SIZE_SIZE10,
        ),
        (
            "flag beats the environment",
            Options {
                cluster_size: CLUSTER_SIZE_SIZE0.into(),
                ..opts()
            },
            vec![("HK_INSTALL_CLUSTER_SIZE", CLUSTER_SIZE_SIZE10)],
            CLUSTER_SIZE_SIZE0,
        ),
        (
            "explicit connection string is untouched",
            Options {
                cluster_size: CLUSTER_SIZE_SIZE10.into(),
                ..opts()
            },
            vec![(
                "HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING",
                "postgresql://someone@db.example:5432/hkpg",
            )],
            CLUSTER_SIZE_SIZE10,
        ),
    ];
    for (name, o, env, want) in cases {
        t.load_env();
        for (k, v) in env {
            t.set_env(k, v);
        }
        let cfg = load_options(&o).unwrap();
        assert_eq!(cfg.cluster_size, want, "{name}");
        assert_eq!(cfg.size10(), want == CLUSTER_SIZE_SIZE10, "{name}");
        let env = script_env_map(&cfg);
        if want == CLUSTER_SIZE_SIZE10 {
            assert_eq!(
                env.get("HK_INSTALL_CLUSTER_SIZE").map(String::as_str),
                Some(CLUSTER_SIZE_SIZE10),
                "{name}"
            );
        } else {
            assert!(!env.contains_key("HK_INSTALL_CLUSTER_SIZE"), "{name}");
        }
    }
}

fn bool_profile_cases(
    t: &mut TestEnv,
    var: &str,
    set_flag: fn(&mut Options),
    read: fn(&Config) -> bool,
    cases: &[(&str, bool, &str, bool)],
) {
    for (name, flag, env, want) in cases {
        t.load_env();
        if !env.is_empty() {
            t.set_env(var, env);
        }
        let mut o = opts();
        if *flag {
            set_flag(&mut o);
        }
        let cfg = load_options(&o).unwrap();
        assert_eq!(read(&cfg), *want, "{name}");
        assert_eq!(
            script_env_map(&cfg).contains_key(var),
            *want,
            "{name}: script env"
        );
    }
}

#[test]
fn load_cordon_system_infra_components() {
    let mut t = TestEnv::new();
    bool_profile_cases(
        &mut t,
        "HK_INSTALL_CORDON_SYSTEM_INFRA_COMPONENTS",
        |o| o.cordon_system_infra_components = true,
        |c| c.cordon_system_infra_components,
        &[
            ("flag", true, "", true),
            ("environment true", false, "true", true),
            ("environment false", false, "false", false),
        ],
    );
}

#[test]
fn load_fake_data_plane() {
    let mut t = TestEnv::new();
    bool_profile_cases(
        &mut t,
        "HK_INSTALL_FAKE_DATA_PLANE",
        |o| o.fake_data_plane = true,
        |c| c.fake_data_plane,
        &[
            ("off by default", false, "", false),
            ("flag", true, "", true),
            ("environment", false, "true", true),
        ],
    );
}

#[test]
fn load_e2e_tuning() {
    let mut t = TestEnv::new();
    bool_profile_cases(
        &mut t,
        "HK_INSTALL_E2E_TUNING",
        |o| o.e2e_tuning = true,
        |c| c.e2e_tuning,
        &[
            ("off by default", false, "", false),
            ("flag", true, "", true),
            ("environment", false, "true", true),
        ],
    );
}

#[test]
fn load_ingress_allowed_spiffe_ids() {
    let mut t = TestEnv::new();
    let (a, b) = (
        "spiffe://cluster.local/ns/a/sa/a",
        "spiffe://cluster.local/ns/b/sa/b",
    );
    let several = format!(" {a},,{b}, ");
    let bad = format!("{a},https://example.com/x");
    let cases: Vec<(&str, &str, Vec<&str>, &str)> = vec![
        ("unset", "", vec![], ""),
        ("one", a, vec![a], ""),
        ("several, with stray separators", &several, vec![a, b], ""),
        (
            "not a SPIFFE ID",
            &bad,
            vec![],
            "ingressAllowedSPIFFEIDs must be a comma-separated list of SPIFFE IDs",
        ),
    ];
    for (name, env, want, want_err) in cases {
        t.load_env();
        if !env.is_empty() {
            t.set_env("HK_INGRESS_ALLOWED_SPIFFE_IDS", env);
        }
        let res = load_options(&opts());
        if !want_err.is_empty() {
            let err = res.expect_err(name).to_string();
            assert!(err.contains(want_err), "{name}: {err}");
            continue;
        }
        let cfg = res.unwrap();
        assert_eq!(cfg.ingress_allowed_spiffe_ids, want, "{name}");
        let joined = want.join(",");
        assert_eq!(
            script_env_map(&cfg)
                .get("HK_INGRESS_ALLOWED_SPIFFE_IDS")
                .cloned(),
            (!joined.is_empty()).then_some(joined),
            "{name}"
        );
    }
}

#[test]
fn require_transfer_service_account() {
    let cases = [
        (
            "GKE without a name",
            Config {
                project_id: "proj".into(),
                ..Default::default()
            },
            Some("storage.transferGSAName"),
        ),
        (
            "GKE without a project",
            Config {
                transfer_gsa_name: "xfer".into(),
                ..Default::default()
            },
            Some("gcp.projectID"),
        ),
        (
            "GKE with both",
            Config {
                transfer_gsa_name: "xfer".into(),
                project_id: "proj".into(),
                ..Default::default()
            },
            None,
        ),
        (
            "kind without either",
            Config {
                kind: true,
                ..Default::default()
            },
            None,
        ),
    ];
    for (name, cfg, want) in cases {
        match (cfg.require_transfer_service_account(), want) {
            (Ok(()), None) => {}
            (Err(ConfigError::Required { setting }), Some(key)) => {
                assert_eq!(setting.key, key, "{name}")
            }
            (got, _) => panic!("{name}: {got:?}"),
        }
    }
}

#[test]
fn load_transfer_service_account() {
    let mut t = TestEnv::new();
    let cases: Vec<(&str, Options, Vec<(&str, &str)>, &str, &str)> = vec![
        (
            "built from the name and the project",
            opts(),
            vec![
                ("SNAPSHOT_TRANSFER_GSA_NAME", "xfer"),
                ("PROJECT_ID", "proj"),
            ],
            "xfer",
            "xfer@proj.iam.gserviceaccount.com",
        ),
        (
            "no default name",
            opts(),
            vec![("PROJECT_ID", "proj")],
            "",
            "",
        ),
        (
            "kind ignores the environment",
            Options {
                kind: true,
                ..opts()
            },
            vec![
                ("SNAPSHOT_TRANSFER_GSA_NAME", "xfer"),
                ("PROJECT_ID", "proj"),
            ],
            "",
            "",
        ),
    ];
    for (name, o, env, want_name, want) in cases {
        t.load_env();
        for (k, v) in env {
            t.set_env(k, v);
        }
        let cfg = load_options(&o).unwrap();
        assert_eq!(cfg.transfer_gsa_name, want_name, "{name}");
        assert_eq!(cfg.transfer_service_account(), want, "{name}");
    }
}

#[test]
fn load_docker_build_flags() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env(
        "DOCKER_BUILD_FLAGS",
        " --cache-from type=gha  --cache-to type=gha,mode=max ",
    );
    let cfg = load_options(&opts()).unwrap();
    assert_eq!(
        cfg.docker_build_flags,
        [
            "--cache-from",
            "type=gha",
            "--cache-to",
            "type=gha,mode=max"
        ]
    );
}

#[test]
fn load_preview_gates() {
    // Real gate names stand in for two arbitrary gates.
    const A: &str = "ExternalVolumes";
    const B: &str = "GoldenEgressPolicy";
    let ab = format!("{A},{B}");
    let ab_spaced = format!("{A}, {B}");
    let a_padded = format!(",{A},,");
    let mut t = TestEnv::new();
    type Case<'a> = (
        &'a str,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Vec<&'a str>,
    );
    let cases: Vec<Case> = vec![
        ("default", None, None, None, vec![]),
        ("flag all", Some("ALL"), None, None, vec!["ALL"]),
        ("flag list", Some(&ab), None, None, vec![A, B]),
        ("file", None, Some(&ab_spaced), None, vec![A, B]),
        ("environment", None, None, Some("ALL"), vec!["ALL"]),
        ("empty items dropped", None, None, Some(&a_padded), vec![A]),
        (
            "file beats environment",
            None,
            Some(A),
            Some("ALL"),
            vec![A],
        ),
        ("flag beats file", Some(B), Some(A), None, vec![B]),
        (
            "empty flag beats environment",
            Some(""),
            None,
            Some("ALL"),
            vec![],
        ),
        (
            "empty file beats environment",
            None,
            Some(""),
            Some("ALL"),
            vec![],
        ),
    ];
    for (name, flag, file, env, want) in cases {
        t.load_env();
        if let Some(e) = env {
            t.set_env("HK_PREVIEW", e);
        }
        let mut flags = Flags::default();
        if let Some(f) = flag {
            flags.set("preview", f);
        }
        let mut lo = LoadOptions {
            root: Some(test_root()),
            ..Default::default()
        };
        let _dir;
        if let Some(f) = file {
            let (d, p) = write_config(&format!("{VALID_HEADER}preview: {f:?}\n"));
            _dir = d;
            lo.config_path = Some(p);
        }
        let cfg = load(&flags, lo).unwrap();
        assert_eq!(cfg.preview_gates, want, "{name}");
        let joined = want.join(",");
        assert_eq!(
            script_env_map(&cfg).get("HK_PREVIEW").cloned(),
            (!joined.is_empty()).then_some(joined),
            "{name}"
        );
    }
}

#[test]
fn load_rejects_unknown_preview_gates() {
    let mut t = TestEnv::new();
    for (name, env, want_err) in [
        ("all", "ALL", false),
        ("known gate", "ExternalVolumes", false),
        ("unknown gate", "NoSuchGate", true),
        ("unknown among known", "ALL,NoSuchGate", true),
    ] {
        t.load_env();
        t.set_env("HK_PREVIEW", env);
        let res = load_options(&opts());
        if !want_err {
            res.unwrap();
            continue;
        }
        let err = res.expect_err(name).to_string();
        assert!(
            err.contains("HK_PREVIEW") && err.contains("NoSuchGate"),
            "{name}: {err}"
        );
    }
}

#[test]
fn load_flags_beat_environment() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("HK_INSTALL_ROLLOUT_TIMEOUT", "30s");
    let cfg = load_options(&Options {
        rollout_timeout: "120s".into(),
        ..opts()
    })
    .unwrap();
    assert_eq!(cfg.rollout_timeout, Duration::from_secs(120));
}

#[test]
fn load_postgres_connection_strings() {
    let mut t = TestEnv::new();
    t.load_env();
    let dsn = "postgresql://someone@db.example:5432/hkpg?sslmode=disable";
    t.set_env("HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING", dsn);
    let cfg = load_options(&opts()).unwrap();
    assert_eq!(cfg.postgres_read_write_connection_string, dsn);
    assert_eq!(cfg.postgres_owner_connection_string, dsn);
    t.set_env("HK_API_POSTGRES_OWNER_CONNECTION_STRING", "owner-dsn");
    let cfg = load_options(&opts()).unwrap();
    assert_eq!(cfg.postgres_read_write_connection_string, dsn);
    assert_eq!(cfg.postgres_owner_connection_string, "owner-dsn");
}

#[test]
fn load_postgres_identity_overrides() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env(
        "HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING",
        "readwrite-dsn",
    );
    t.set_env("HK_API_POSTGRES_OWNER_CONNECTION_STRING", "owner-dsn");
    t.set_env("HK_API_POSTGRES_READ_WRITE_ROLE", "tenant_readwrite");
    t.set_env("HK_API_POSTGRES_OWNER_ROLE", "tenant_owner");
    let cfg = load_options(&opts()).unwrap();
    assert_eq!(cfg.postgres_read_write_connection_string, "readwrite-dsn");
    assert_eq!(cfg.postgres_owner_connection_string, "owner-dsn");
    assert_eq!(cfg.postgres_read_write_role, "tenant_readwrite");
    assert_eq!(cfg.postgres_owner_role, "tenant_owner");
    assert!(cfg.postgres_read_write_role_set && cfg.postgres_owner_role_set);
}

#[test]
fn load_postgres_schema() {
    let mut t = TestEnv::new();
    t.load_env();
    assert_eq!(
        load_options(&opts()).unwrap().postgres_schema_name(),
        DEFAULT_POSTGRES_SCHEMA
    );
    t.set_env("HK_API_POSTGRES_SCHEMA", "tenant_schema");
    assert_eq!(
        load_options(&opts()).unwrap().postgres_schema_name(),
        "tenant_schema"
    );
}

#[test]
fn load_postgres_tuning() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("HK_API_POSTGRES_POOL_MAX_CONNS", "50");
    t.set_env("HK_API_POSTGRES_SERVER_CA_FILE", "/etc/ssl/server-ca.pem");
    let cfg = load_options(&opts()).unwrap();
    assert_eq!(cfg.postgres_pool_max_conns, "50");
    assert_eq!(cfg.postgres_server_ca_file, "/etc/ssl/server-ca.pem");
}

#[test]
fn load_cloud_sql() {
    let mut t = TestEnv::new();
    // Unset: the cluster's record is adopted.
    t.load_env();
    assert!(!load_options(&opts()).unwrap().cloud_sql.instance_set);

    // Exported but empty removes Cloud SQL.
    t.load_env();
    t.set_env("HK_API_POSTGRES_CLOUDSQL_INSTANCE", "");
    let c = load_options(&opts()).unwrap().cloud_sql;
    assert!(c.instance_set && c.instance.is_empty());

    // Fully specified.
    t.load_env();
    t.set_env("HK_API_POSTGRES_CLOUDSQL_INSTANCE", "p:r:i");
    t.set_env(
        "HK_API_POSTGRES_CLOUDSQL_GSA",
        "hk@p.iam.gserviceaccount.com",
    );
    t.set_env("HK_API_POSTGRES_CLOUDSQL_IAM_AUTH", "false");
    t.set_env("HK_API_POSTGRES_CLOUDSQL_IP_TYPE", CLOUDSQL_IP_TYPE_PSC);
    assert_eq!(
        load_options(&opts()).unwrap().cloud_sql,
        CloudSqlConfig {
            instance: "p:r:i".into(),
            instance_set: true,
            gsa: "hk@p.iam.gserviceaccount.com".into(),
            iam_auth: "false".into(),
            ip_type: CLOUDSQL_IP_TYPE_PSC.into(),
        }
    );

    // An unknown IP type would silently dial the public address.
    t.load_env();
    t.set_env("HK_API_POSTGRES_CLOUDSQL_IP_TYPE", "internal");
    let err = load_options(&opts()).unwrap_err().to_string();
    assert!(err.contains("HK_API_POSTGRES_CLOUDSQL_IP_TYPE"), "{err}");
}

#[test]
fn load_expected_jwt_issuer() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("EXPECTED_JWT_ISSUER", "https://issuer.example.com");
    assert_eq!(
        load_options(&opts()).unwrap().expected_jwt_issuer,
        "https://issuer.example.com"
    );
}

#[test]
fn load_api_log_level() {
    let mut t = TestEnv::new();
    for (name, env, flag, want, want_err) in [
        ("unset", "", "", "", false),
        ("environment", "debug", "", "debug", false),
        ("flag beats environment", "debug", "warn", "warn", false),
        ("unsupported", "verbose", "", "", true),
    ] {
        t.load_env();
        t.set_env("HK_API_LOG_LEVEL", env);
        let mut flags = Flags::default();
        if !flag.is_empty() {
            flags.set("api-log-level", flag);
        }
        let res = load(
            &flags,
            LoadOptions {
                no_dev_env: true,
                root: Some(test_root()),
                ..Default::default()
            },
        );
        if want_err {
            assert!(res.is_err(), "{name}");
            continue;
        }
        assert_eq!(res.unwrap().api_log_level, want, "{name}");
    }
}

#[test]
fn load_api_authz() {
    let mut t = TestEnv::new();
    let client_owner = "system:serviceaccount:hivekeep-system:hivekeep-client";
    let owners = "alice@example.com,bob@example.com";
    let cases: Vec<(&str, Vec<(&str, &str)>, &str, bool)> = vec![
        (
            "owned by the client ServiceAccount by default",
            vec![],
            client_owner,
            false,
        ),
        (
            "default owner follows the namespace",
            vec![("HK_NAMESPACE", "custom")],
            "system:serviceaccount:custom:hivekeep-client",
            false,
        ),
        (
            "explicit owners replace the default",
            vec![("HK_API_AUTHZ_BOOTSTRAP_OWNERS", owners)],
            owners,
            false,
        ),
        (
            "only separators",
            vec![("HK_API_AUTHZ_BOOTSTRAP_OWNERS", " , ")],
            "",
            true,
        ),
    ];
    for (name, env, want, want_err) in cases {
        t.load_env();
        for (k, v) in env {
            t.set_env(k, v);
        }
        let res = load_options(&opts());
        if want_err {
            let err = res.expect_err(name).to_string();
            assert!(err.contains("bootstrap owner"), "{name}: {err}");
            continue;
        }
        assert_eq!(res.unwrap().api_authz_bootstrap_owners, want, "{name}");
    }
}

#[test]
fn load_actor_jwt_algorithm() {
    let mut t = TestEnv::new();
    for (name, env, want, want_err) in [
        ("unset", "", "ES256", false),
        ("RS256", "RS256", "RS256", false),
        ("unsupported", "HS256", "", true),
    ] {
        t.load_env();
        if !env.is_empty() {
            t.set_env("ACTOR_JWT_ALGORITHM", env);
        }
        let res = load_options(&opts());
        if want_err {
            assert!(res.is_err(), "{name}");
            continue;
        }
        assert_eq!(res.unwrap().actor_jwt_algorithm, want, "{name}");
    }
}

#[test]
fn load_actor_jwt_issuer() {
    let mut t = TestEnv::new();
    for (name, env, namespace, want, want_err) in [
        ("unset", "", "", "https://idp.hivekeep-system.svc", false),
        (
            "unset in a relocated install",
            "",
            "team-a",
            "https://idp.team-a.svc",
            false,
        ),
        (
            "set is used as given",
            "https://idp.example.com",
            "",
            "https://idp.example.com",
            false,
        ),
        ("not https", "http://idp.example.com", "", "", true),
        ("path", "https://idp.example.com/prod", "", "", true),
    ] {
        t.load_env();
        if !env.is_empty() {
            t.set_env("ACTOR_JWT_ISSUER", env);
        }
        if !namespace.is_empty() {
            t.set_env("HK_NAMESPACE", namespace);
        }
        let res = load_options(&opts());
        if want_err {
            assert!(res.is_err(), "{name}");
            continue;
        }
        assert_eq!(res.unwrap().actor_jwt_issuer, want, "{name}");
    }
}

#[test]
fn load_otlp_endpoint() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("HK_OTLP_ENDPOINT", "http://from-environment:4317");
    assert_eq!(
        load_options(&opts()).unwrap().otlp_endpoint,
        "http://from-environment:4317"
    );
    let cfg = load_options(&Options {
        otlp_endpoint: "http://from-flag:4317".into(),
        ..opts()
    })
    .unwrap();
    assert_eq!(cfg.otlp_endpoint, "http://from-flag:4317");
    assert_eq!(
        script_env_map(&cfg)["HK_OTLP_ENDPOINT"],
        "http://from-flag:4317"
    );
}

#[test]
fn load_kind_from_environment() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("HK_INSTALL_KIND", "true");
    t.set_env("PROJECT_ID", "some-project");
    let cfg = load_options(&opts()).unwrap();
    assert!(cfg.kind);
    assert_eq!(cfg.build_repo, "localhost:5001");
    assert_eq!(cfg.project_id, "");
}

#[test]
fn load_kubeconfig_falls_back_to_environment() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("KUBECONFIG", "/home/dev/clusters/dev.yaml");
    let cfg = load_options(&opts()).unwrap();
    assert_eq!(cfg.kubeconfig, "/home/dev/clusters/dev.yaml");
    assert!(cfg
        .script_env()
        .contains(&"KUBECONFIG=/home/dev/clusters/dev.yaml".to_string()));
    let cfg = load_options(&Options {
        kubeconfig: "/tmp/explicit.yaml".into(),
        ..opts()
    })
    .unwrap();
    assert_eq!(cfg.kubeconfig, "/tmp/explicit.yaml");
}

#[test]
fn load_kubeconfig_list_stays_with_the_loading_rules() {
    let mut t = TestEnv::new();
    t.load_env();
    let list = std::env::join_paths(["/home/dev/a.yaml", "/home/dev/b.yaml"])
        .unwrap()
        .into_string()
        .unwrap();
    t.set_env("KUBECONFIG", &list);
    let cfg = load_options(&opts()).unwrap();
    assert_eq!(cfg.kubeconfig, "");
    assert!(cfg.script_env().contains(&format!("KUBECONFIG={list}")));
    let cfg = load_options(&Options {
        kubeconfig: "/tmp/explicit.yaml".into(),
        ..opts()
    })
    .unwrap();
    assert_eq!(cfg.kubeconfig, "/tmp/explicit.yaml");
    assert!(cfg
        .script_env()
        .contains(&"KUBECONFIG=/tmp/explicit.yaml".to_string()));
}

#[test]
fn wait_timeout() {
    let mut t = TestEnv::new();
    let historical = Duration::from_secs(120);
    for (name, rollout, env, want) in [
        (
            "default leaves the historical timeout alone",
            "",
            "",
            historical,
        ),
        (
            "a longer flag raises it",
            "5m",
            "",
            Duration::from_secs(300),
        ),
        (
            "a shorter flag lowers it",
            "30s",
            "",
            Duration::from_secs(30),
        ),
        (
            "the environment counts as asking too",
            "",
            "10m",
            Duration::from_secs(600),
        ),
    ] {
        t.load_env();
        if !env.is_empty() {
            t.set_env("HK_INSTALL_ROLLOUT_TIMEOUT", env);
        }
        let cfg = load_options(&Options {
            rollout_timeout: rollout.into(),
            ..opts()
        })
        .unwrap();
        assert_eq!(cfg.wait_timeout(historical), want, "{name}");
    }
}

#[test]
fn load_rejects_invalid_values() {
    let mut t = TestEnv::new();
    t.load_env();
    let p = |s: &str| Options {
        credential_provider: s.into(),
        ..opts()
    };
    let cases: Vec<(&str, Options)> = vec![
        (
            "router",
            Options {
                router: "nginx".into(),
                ..opts()
            },
        ),
        (
            "rollout timeout",
            Options {
                rollout_timeout: "invalid".into(),
                ..opts()
            },
        ),
        (
            "zero rollout timeout",
            Options {
                rollout_timeout: "0s".into(),
                ..opts()
            },
        ),
        (
            "negative rollout timeout",
            Options {
                rollout_timeout: "-30s".into(),
                ..opts()
            },
        ),
        (
            "podcert workers",
            Options {
                podcert_workers_per_signer: -1,
                ..opts()
            },
        ),
        (
            "cluster size",
            Options {
                cluster_size: "size5".into(),
                ..opts()
            },
        ),
        (
            "extproc invalid format",
            Options {
                additional_egress_extproc_service: "extproc:50051".into(),
                ..opts()
            },
        ),
        ("provider not JSON", p("k8s.io")),
        ("provider as a JSON string", p("\"off\"")),
        ("provider null", p("null")),
        (
            "provider without a name",
            p(r#"{"address":"vault.hivekeep-system.svc:8200"}"#),
        ),
        (
            "provider with an unknown key",
            p(r#"{"name":"k8s.io","adress":"x:1"}"#),
        ),
        (
            "provider with trailing data",
            p(r#"{"enabled":false} {"name":"k8s.io"}"#),
        ),
        (
            "provider name with a scheme",
            p(r#"{"name":"hk-secret://k8s.io"}"#),
        ),
        (
            "provider name not lowercase",
            p(r#"{"name":"Vault.example.com","address":"vault.hivekeep-system.svc:8200"}"#),
        ),
        ("provider name with a port", p(r#"{"name":"k8s.io:443"}"#)),
        (
            "provider name with a path",
            p(r#"{"name":"k8s.io/default"}"#),
        ),
        ("provider name with a query", p(r#"{"name":"k8s.io?x=1"}"#)),
        (
            "disabled with a name",
            p(r#"{"enabled":false,"name":"k8s.io"}"#),
        ),
        (
            "disabled with an address",
            p(r#"{"enabled":false,"address":"stale.hivekeep-system.svc:50051"}"#),
        ),
        ("enabled without a name", p(r#"{"enabled":true}"#)),
        (
            "kubernetes address without a port",
            p(r#"{"name":"k8s.io","address":"secrets.hivekeep-system.svc"}"#),
        ),
        (
            "other provider without an address",
            p(r#"{"name":"vault.example.com"}"#),
        ),
        (
            "other provider address without a port",
            p(r#"{"name":"vault.example.com","address":"vault.hivekeep-system.svc"}"#),
        ),
    ];
    for (name, o) in cases {
        assert!(load_options(&o).is_err(), "{name}: load succeeded");
    }
}

#[test]
fn credential_provider_cases() {
    let mut t = TestEnv::new();
    let bundled = CredentialProvider {
        name: K8S_CREDENTIAL_PROVIDER_NAME.into(),
        address: K8S_CREDENTIAL_PROVIDER_ADDRESS.into(),
    };
    let vault = CredentialProvider {
        name: "vault.example.com".into(),
        address: "vault.hivekeep-system.svc:50051".into(),
    };
    let p = |s: &str| Options {
        credential_provider: s.into(),
        ..opts()
    };
    let vault_json = r#"{"name":"vault.example.com","address":"vault.hivekeep-system.svc:50051"}"#;
    type Case<'a> = (
        &'a str,
        Options,
        Vec<(&'a str, &'a str)>,
        Option<CredentialProvider>,
    );
    let cases: Vec<Case> = vec![
        ("absent is an error", opts(), vec![], None),
        (
            "disabled",
            p(r#"{"enabled":false}"#),
            vec![],
            Some(CredentialProvider::default()),
        ),
        (
            "enabled true names a provider",
            p(r#"{"enabled":true,"name":"k8s.io"}"#),
            vec![],
            Some(bundled.clone()),
        ),
        (
            "kubernetes gets the bundled address",
            p(r#"{"name":"k8s.io"}"#),
            vec![],
            Some(bundled.clone()),
        ),
        (
            "kubernetes keeps a given address",
            p(r#"{"name":"k8s.io","address":"secrets.hivekeep-system.svc:443"}"#),
            vec![],
            Some(CredentialProvider {
                name: "k8s.io".into(),
                address: "secrets.hivekeep-system.svc:443".into(),
            }),
        ),
        (
            "another provider",
            p(vault_json),
            vec![],
            Some(vault.clone()),
        ),
        (
            "whitespace is allowed",
            p(r#" { "name" : "k8s.io" } "#),
            vec![],
            Some(bundled.clone()),
        ),
        (
            "the environment selects too",
            opts(),
            vec![("HK_CREDENTIAL_PROVIDER", r#"{"name":"k8s.io"}"#)],
            Some(bundled.clone()),
        ),
        (
            "the flag overrides the environment",
            p(r#"{"enabled":false}"#),
            vec![("HK_CREDENTIAL_PROVIDER", vault_json)],
            Some(CredentialProvider::default()),
        ),
    ];
    for (name, o, env, want) in cases {
        t.load_env();
        for (k, v) in env {
            t.set_env(k, v);
        }
        let cfg = load_options(&o).unwrap();
        match (cfg.credential_provider(), want) {
            (Err(_), None) => {}
            (Ok(got), Some(want)) => {
                assert_eq!(got.enabled(), !want.name.is_empty(), "{name}");
                assert_eq!(got, want, "{name}");
            }
            (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
        }
    }
}

#[test]
fn credential_provider_server_name() {
    for (addr, want) in [
        (
            "k8s-credential-provider.hivekeep-system.svc:50051",
            "k8s-credential-provider.hivekeep-system.svc",
        ),
        ("vault.example.com", "vault.example.com"),
    ] {
        assert_eq!(
            CredentialProvider {
                address: addr.into(),
                ..Default::default()
            }
            .server_name(),
            want
        );
    }
}

#[test]
fn load_rejects_invalid_rollout_timeout_env() {
    let mut t = TestEnv::new();
    for val in ["0", "0s", "-1m", "forever"] {
        t.load_env();
        t.set_env("HK_INSTALL_ROLLOUT_TIMEOUT", val);
        assert!(load_options(&opts()).is_err(), "{val}");
    }
}

#[test]
fn load_rejects_invalid_podcert_workers_env() {
    let mut t = TestEnv::new();
    for val in ["0", "-1", "abc"] {
        t.load_env();
        t.set_env("HK_INSTALL_PODCERT_WORKERS_PER_SIGNER", val);
        assert!(load_options(&opts()).is_err(), "{val}");
    }
}

#[test]
fn load_envoy_concurrency() {
    let mut t = TestEnv::new();
    t.load_env();
    let cfg = load_options(&Options {
        envoy_concurrency: 4,
        ..opts()
    })
    .unwrap();
    assert_eq!(cfg.envoy_concurrency, 4);
    assert_eq!(script_env_map(&cfg)["HK_E2E_ENVOY_CONCURRENCY"], "4");
    for val in ["0", "-1", "abc"] {
        t.load_env();
        t.set_env("HK_E2E_ENVOY_CONCURRENCY", val);
        assert!(load_options(&opts()).is_err(), "{val}");
    }
}

#[test]
fn load_kind_profile() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("PROJECT_ID", "some-gke-project");
    t.set_env("CLUSTER_LOCATION", "us-central1-c");
    t.set_env("BUCKET_NAME", "ambient-cloud-bucket");
    let cfg = load_options(&Options {
        kind: true,
        ..opts()
    })
    .unwrap();
    assert_eq!(cfg.context, "kind-kind");
    assert_eq!(cfg.project_id, "");
    assert_eq!(cfg.cluster_location, "");
    assert_eq!(cfg.build_repo, "localhost:5001");
    assert_eq!(
        cfg.build_platforms,
        format!("linux/{}", host_platform_arch())
    );
    assert_eq!(cfg.bucket_name, "hivekeep-snapshots");
}

#[test]
fn script_env_carries_resolved_config() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("BUCKET_NAME", "from-environment");
    let cfg = load_options(&Options {
        context: "gke_demo".into(),
        kubeconfig: "/tmp/kubeconfig".into(),
        ..opts()
    })
    .unwrap();
    let env = script_env_map(&cfg);
    assert_eq!(env["KUBECTL_CONTEXT"], "gke_demo");
    assert_eq!(env["KUBECONFIG"], "/tmp/kubeconfig");
    assert_eq!(env["BUCKET_NAME"], "from-environment");
    assert!(!env.contains_key("PROJECT_ID"));
}

#[test]
fn script_env_kind_profile() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("PROJECT_ID", "some-gke-project");
    t.set_env("CLUSTER_LOCATION", "us-central1-c");
    t.set_env("MEMORYSTORE_INSTANCE", "some-instance");
    let cfg = load_options(&Options {
        kind: true,
        ..opts()
    })
    .unwrap();
    let env = script_env_map(&cfg);
    for name in ["PROJECT_ID", "CLUSTER_LOCATION", "MEMORYSTORE_INSTANCE"] {
        assert!(!env.contains_key(name), "{name}");
    }
    assert_eq!(env["HK_INSTALL_KIND"], "true");
    assert_eq!(env["HK_BUILD_REPO"], "localhost:5001");
}

#[test]
fn load_image_source() {
    let mut t = TestEnv::new();
    let o = |repo: &str, tag: &str| Options {
        image_repo: repo.into(),
        image_tag: tag.into(),
        ..opts()
    };
    type Case<'a> = (
        &'a str,
        Vec<(&'a str, &'a str)>,
        Options,
        (&'a str, &'a str),
        &'a str,
    );
    let cases: Vec<Case> = vec![
        ("unset builds from source", vec![], opts(), ("", ""), ""),
        (
            "flags",
            vec![],
            o("example.com/hivekeep", "v1.2.3"),
            ("example.com/hivekeep", "v1.2.3"),
            "",
        ),
        (
            "environment",
            vec![("HK_IMAGE_REPO", "example.com/env"), ("HK_IMAGE_TAG", "v9")],
            opts(),
            ("example.com/env", "v9"),
            "",
        ),
        (
            "flags beat the environment",
            vec![("HK_IMAGE_REPO", "example.com/env"), ("HK_IMAGE_TAG", "v9")],
            o("example.com/flag", "v1"),
            ("example.com/flag", "v1"),
            "",
        ),
        (
            "trailing slash is trimmed",
            vec![],
            o("example.com/hivekeep/", "v1"),
            ("example.com/hivekeep", "v1"),
            "",
        ),
        (
            "a repo needs a tag",
            vec![],
            o("example.com/hivekeep", ""),
            ("", ""),
            r#"images.repo="example.com/hivekeep" (from --image-repo) conflicts with images.tag="" (from default)"#,
        ),
        (
            "a tag needs a repo",
            vec![],
            o("", "v1"),
            ("", ""),
            r#"images.tag="v1" (from --image-tag) conflicts with images.repo="" (from default)"#,
        ),
        (
            "a tag from the environment needs a repo",
            vec![("HK_IMAGE_TAG", "v1")],
            opts(),
            ("", ""),
            r#"images.tag="v1" (from HK_IMAGE_TAG) conflicts with images.repo="" (from default)"#,
        ),
    ];
    for (name, env, o, (repo, tag), want_err) in cases {
        t.load_env();
        for (k, v) in env {
            t.set_env(k, v);
        }
        let res = load_options(&o);
        if !want_err.is_empty() {
            let err = res.expect_err(name).to_string();
            assert!(err.contains(want_err), "{name}: {err}");
            continue;
        }
        let cfg = res.unwrap();
        assert_eq!(
            (cfg.images.repo.as_str(), cfg.images.tag.as_str()),
            (repo, tag),
            "{name}"
        );
        assert_eq!(cfg.images.is_prebuilt(), !repo.is_empty(), "{name}");
    }
}

// ---- coverage ----

/// A value the setting accepts; a few validate their contents.
fn value_for(key: &str) -> &'static str {
    match key {
        "gate.dataplane" => ROUTER_ENVOY,
        "clusterSize" => CLUSTER_SIZE_SIZE0,
        "ingressAllowedSPIFFEIDs" => "spiffe://cluster.local/ns/a/sa/b",
        "api.postgres.cloudsql.ipType" => CLOUDSQL_IP_TYPE_PSC,
        "gate.egress.additionalExtprocService" => "ns/svc:8080",
        "gate.egress.credentialProvider" => r#"{"name":"k8s.io"}"#,
        "actorJWT.algorithm" => "ES256",
        "actorJWT.issuer" => "https://idp.example.com",
        "api.logLevel" => "debug",
        "csi.setup" => "none",
        "preview" => "ALL",
        _ => "set",
    }
}

/// With every setting supplied, every Config field a setting feeds is
/// populated. Rust has no field reflection, so the fields are listed; a new
/// field must be added here (or to the exempt list) to be covered.
#[test]
fn every_config_field_is_fed_by_some_setting() {
    let mut env = std::collections::BTreeMap::new();
    for s in all() {
        let v = match s.kind {
            // A Kind install clears the GCP coordinates by design, so the
            // cloud path exercises every field.
            ValueKind::Bool => (s.key != KIND_ENABLED_KEY).to_string(),
            ValueKind::Int => "7".into(),
            ValueKind::Duration => "7m0s".into(),
            ValueKind::String => value_for(&s.key).into(),
        };
        env.insert(s.env.clone(), v);
    }
    let r = resolve(
        None,
        ResolveOptions {
            env: Some(&env),
            file: None,
        },
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cfg = build_config(dir.path(), dir.path(), env.clone(), r).unwrap();
    let fields: Vec<(&str, bool)> = vec![
        ("namespace", !cfg.namespace.is_empty()),
        ("kubeconfig", !cfg.kubeconfig.is_empty()),
        ("context", !cfg.context.is_empty()),
        ("project_id", !cfg.project_id.is_empty()),
        ("cluster_name", !cfg.cluster_name.is_empty()),
        ("cluster_location", !cfg.cluster_location.is_empty()),
        ("expected_jwt_issuer", !cfg.expected_jwt_issuer.is_empty()),
        (
            "api_authz_bootstrap_owners",
            !cfg.api_authz_bootstrap_owners.is_empty(),
        ),
        ("actor_jwt_algorithm", !cfg.actor_jwt_algorithm.is_empty()),
        ("actor_jwt_issuer", !cfg.actor_jwt_issuer.is_empty()),
        ("bucket_name", !cfg.bucket_name.is_empty()),
        ("snapshot_location", !cfg.snapshot_location.is_empty()),
        ("transfer_gsa_name", !cfg.transfer_gsa_name.is_empty()),
        ("build_repo", !cfg.build_repo.is_empty()),
        ("build_platforms", !cfg.build_platforms.is_empty()),
        ("docker_build_flags", !cfg.docker_build_flags.is_empty()),
        ("router", !cfg.router.is_empty()),
        (
            "postgres_read_write_connection_string",
            !cfg.postgres_read_write_connection_string.is_empty(),
        ),
        (
            "postgres_owner_connection_string",
            !cfg.postgres_owner_connection_string.is_empty(),
        ),
        (
            "postgres_read_write_role",
            !cfg.postgres_read_write_role.is_empty(),
        ),
        ("postgres_owner_role", !cfg.postgres_owner_role.is_empty()),
        ("postgres_schema", !cfg.postgres_schema.is_empty()),
        (
            "postgres_pool_max_conns",
            !cfg.postgres_pool_max_conns.is_empty(),
        ),
        (
            "postgres_server_ca_file",
            !cfg.postgres_server_ca_file.is_empty(),
        ),
        (
            "postgres_storage_class",
            !cfg.postgres_storage_class.is_empty(),
        ),
        ("rollout_timeout", !cfg.rollout_timeout.is_zero()),
        ("rollout_timeout_set", cfg.rollout_timeout_set),
        (
            "podcert_workers_per_signer",
            cfg.podcert_workers_per_signer != 0,
        ),
        ("envoy_concurrency", cfg.envoy_concurrency != 0),
        ("cluster_size", !cfg.cluster_size.is_empty()),
        (
            "cordon_system_infra_components",
            cfg.cordon_system_infra_components,
        ),
        ("fake_data_plane", cfg.fake_data_plane),
        ("e2e_tuning", cfg.e2e_tuning),
        (
            "ingress_allowed_spiffe_ids",
            !cfg.ingress_allowed_spiffe_ids.is_empty(),
        ),
        (
            "additional_egress_extproc_service",
            !cfg.additional_egress_extproc_service.is_empty(),
        ),
        (
            "credential_provider_json",
            !cfg.credential_provider_json.is_empty(),
        ),
        ("anthropic_api_key", !cfg.anthropic_api_key.is_empty()),
        ("otlp_endpoint", !cfg.otlp_endpoint.is_empty()),
        ("preview_gates", !cfg.preview_gates.is_empty()),
        ("api_log_level", !cfg.api_log_level.is_empty()),
        (
            "benchmark_actor_memory",
            !cfg.benchmark_actor_memory.is_empty(),
        ),
        ("kubeconfig_env", !cfg.kubeconfig_env.is_empty()),
        ("shell_env", !cfg.shell_env.is_empty()),
        ("images", cfg.images.is_prebuilt()),
        ("cloud_sql", cfg.cloud_sql.instance_set),
    ];
    for (name, populated) in fields {
        assert!(
            populated,
            "Config.{name} is empty with every setting supplied; no registry entry feeds it"
        );
    }
}

// ---- developer environment ----

const DEV_ENV_MARKER: &str = "HK_TEST_DEV_ENV_MARKER";

/// A directory standing in for the repository root, holding a dev env file.
fn fake_repo(t: &mut TestEnv) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("Cargo.toml"), "[workspace]\n").unwrap();
    std::fs::write(
        dir.path().join(DEV_ENV_FILE),
        format!("export {DEV_ENV_MARKER}=sourced\n"),
    )
    .unwrap();
    t.unset_env("NO_DEV_ENV");
    t.unset_env(DEV_ENV_MARKER);
    dir
}

#[test]
fn kind_suppresses_the_dev_environment_from_every_channel() {
    let mut t = TestEnv::new();
    type Case<'a> = (&'a str, Vec<(&'a str, &'a str)>, &'a str, bool, bool);
    let cases: Vec<Case> = vec![
        ("not selected", vec![], "", false, false),
        ("flag", vec![], "", true, true),
        (
            "environment",
            vec![("HK_INSTALL_KIND", "true")],
            "",
            false,
            true,
        ),
        (
            "file",
            vec![],
            "kindCluster:\n  enabled: true\n",
            false,
            true,
        ),
        (
            "environment spelled 1",
            vec![("HK_INSTALL_KIND", "1")],
            "",
            false,
            true,
        ),
    ];
    for (name, env, file, flag, want_kind) in cases {
        t.load_env();
        let repo = fake_repo(&mut t);
        for (k, v) in env {
            t.set_env(k, v);
        }
        let _doc;
        if !file.is_empty() {
            let (d, p) = write_config(&format!("{VALID_HEADER}{file}"));
            _doc = d;
            t.set_env(CONFIG_PATH_ENV, p.to_str().unwrap());
        }
        let flags = options_flags(&Options {
            kind: flag,
            ..opts()
        });
        let cfg = load(
            &flags,
            LoadOptions {
                root: Some(repo.path().to_path_buf()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(cfg.kind, want_kind, "{name}");
        let sourced = cfg.shell_env.get(DEV_ENV_MARKER).map(String::as_str) == Some("sourced");
        assert_eq!(sourced, !want_kind, "{name}: dev env sourced = {sourced}");
    }
}

#[test]
fn kind_false_flag_overrides_the_environment() {
    let mut t = TestEnv::new();
    t.load_env();
    let repo = fake_repo(&mut t);
    t.set_env("HK_INSTALL_KIND", "true");
    let mut flags = Flags::default();
    flags.set("kind", "false");
    let cfg = load(
        &flags,
        LoadOptions {
            root: Some(repo.path().to_path_buf()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(!cfg.kind);
    assert_eq!(
        cfg.shell_env.get(DEV_ENV_MARKER).map(String::as_str),
        Some("sourced")
    );
}

// ---- Kind defaults ----

fn kind_defaults() -> Vec<(&'static str, String)> {
    vec![
        ("context", "kind-kind".into()),
        ("build.repo", "localhost:5001".into()),
        ("storage.bucketName", "hivekeep-snapshots".into()),
        (
            "storage.snapshotLocation",
            "gs://hivekeep-snapshots/".into(),
        ),
        ("build.platforms", format!("linux/{}", host_platform_arch())),
    ]
}

#[test]
fn kind_defaults_reach_the_report() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("HK_INSTALL_KIND", "true");
    let cfg = load_options(&opts()).unwrap();
    let r = cfg.resolved();
    for (key, want) in kind_defaults() {
        assert_eq!(r.string(key), want, "{key}");
        assert!(r.supplied(key), "{key} not marked supplied");
    }
}

#[test]
fn kind_install_records_what_it_used() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("HK_INSTALL_KIND", "true");
    let cfg = load_options(&opts()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (path, _) = record_success(dir.path(), &cfg.context, "", cfg.resolved()).unwrap();
    let f = parse_file(&path).unwrap();
    for (key, want) in kind_defaults() {
        assert_eq!(f.get(key), Some(want.as_str()), "{key}");
    }
}

#[test]
fn kind_record_replays_to_the_same_install() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("HK_INSTALL_KIND", "true");
    let cfg = load_options(&opts()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (path, _) = record_success(dir.path(), &cfg.context, "", cfg.resolved()).unwrap();
    t.load_env();
    t.set_env(CONFIG_PATH_ENV, path.to_str().unwrap());
    let replayed = load_options(&opts()).unwrap();
    assert_eq!(replayed.context, cfg.context);
    assert_eq!(replayed.build_repo, cfg.build_repo);
    assert_eq!(replayed.build_platforms, cfg.build_platforms);
    assert_eq!(replayed.bucket_name, cfg.bucket_name);
    assert_eq!(replayed.snapshot_location, cfg.snapshot_location);
}

// ---- loading with documents ----

#[test]
fn kind_keeps_explicit_values_but_not_ambient_ones() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("BUCKET_NAME", "ambient-cloud-bucket");
    t.set_env("HK_BUILD_PLATFORMS", "linux/ambient");
    t.set_env("SNAPSHOT_LOCATION", "gs://ambient-cloud-bucket/");

    let cfg = load_options(&Options {
        kind: true,
        ..opts()
    })
    .unwrap();
    assert_eq!(cfg.bucket_name, "hivekeep-snapshots");
    assert_eq!(cfg.snapshot_location, "gs://hivekeep-snapshots/");
    assert_eq!(
        cfg.build_platforms,
        format!("linux/{}", host_platform_arch())
    );

    let (_d, path) = write_config(&format!(
        "{VALID_HEADER}storage:\n  bucketName: from-file\n  snapshotLocation: gs://from-file/prefix/\nbuild:\n  platforms: linux/from-file\n"
    ));
    let flags = options_flags(&Options {
        kind: true,
        ..opts()
    });
    let cfg = load(
        &flags,
        LoadOptions {
            config_path: Some(path),
            root: Some(test_root()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(cfg.bucket_name, "from-file");
    assert_eq!(cfg.snapshot_location, "gs://from-file/prefix/");
    assert_eq!(cfg.build_platforms, "linux/from-file");
}

fn platforms_doc(platforms: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    write_config(&format!("{VALID_HEADER}build:\n  platforms: {platforms}\n"))
}

#[test]
fn load_file_outranks_environment() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("HK_BUILD_PLATFORMS", "linux/from-env");
    let (_d, path) = platforms_doc("linux/from-file");
    let cfg = load(
        &Flags::default(),
        LoadOptions {
            config_path: Some(path),
            root: Some(test_root()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(cfg.build_platforms, "linux/from-file");
}

#[test]
fn load_errors_name_the_supplying_channel() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env("HK_GATE_DATAPLANE", "nginx");
    let err = load_options(&opts()).unwrap_err().to_string();
    assert!(err.contains("HK_GATE_DATAPLANE"), "{err}");
}

#[test]
fn load_rejects_bad_config_path() {
    let mut t = TestEnv::new();
    t.load_env();
    let d = tempfile::tempdir().unwrap();
    let res = load(
        &Flags::default(),
        LoadOptions {
            config_path: Some(d.path().join("absent.yaml")),
            root: Some(test_root()),
            ..Default::default()
        },
    );
    assert!(res.is_err());
}

#[test]
fn config_path_from_environment() {
    let mut t = TestEnv::new();
    t.load_env();
    let unset = load_options(&opts()).unwrap().build_platforms;
    let (_d, path) = platforms_doc("linux/from-env-doc");

    // The environment supplies the document.
    t.set_env(CONFIG_PATH_ENV, path.to_str().unwrap());
    assert_eq!(
        load_options(&opts()).unwrap().build_platforms,
        "linux/from-env-doc"
    );

    // The flag outranks the environment.
    let (_o, other) = platforms_doc("linux/from-flag-doc");
    let cfg = load(
        &Flags::default(),
        LoadOptions {
            config_path: Some(other),
            root: Some(test_root()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(cfg.build_platforms, "linux/from-flag-doc");

    // Unset means no file layer.
    t.unset_env(CONFIG_PATH_ENV);
    assert_eq!(load_options(&opts()).unwrap().build_platforms, unset);
}

#[test]
fn missing_config_error_names_absolute_path() {
    let mut t = TestEnv::new();
    t.load_env();
    t.set_env(CONFIG_PATH_ENV, "no-such-config.yaml");
    let err = load_options(&opts()).unwrap_err().to_string();
    let rest = err.split("reading config ").nth(1).unwrap_or("");
    assert!(rest.starts_with('/'), "{err}");
}

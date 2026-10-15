use std::time::Duration;

use clap::Parser;

use super::*;
use crate::router::ingress::DEFAULT_PARKED_REQUEST_BUDGET;
use crate::router::xds::{XdsServer, DEFAULT_ROUTE_TIMEOUT};

const CERT_PATH: &str = "/run/servicedns.podcert.hivekeep.io/credential-bundle.pem";

fn park(max: usize) -> ParkedRequestConfig {
    ParkedRequestConfig {
        max,
        ..ParkedRequestConfig::disabled()
    }
}

fn cfg(mode: &str) -> RouterConfig {
    RouterConfig {
        mode: mode.to_string(),
        ..Default::default()
    }
}

fn valid_ingress_auth() -> IngressAuthConfig {
    IngressAuthConfig {
        client_ca_file: "/run/podidentity.podcert.hivekeep.io/trust-bundle.pem".to_string(),
        allowed_spiffe_ids: vec!["spiffe://cluster.local/ns/demo/sa/client".to_string()],
    }
}

fn valid_ingress_config() -> RouterConfig {
    RouterConfig {
        mode: "ingress".to_string(),
        parked_request: park(DEFAULT_PARKED_REQUEST_MAX),
        https_port: 8443,
        connect_tls_port: 8444,
        envoy_cert_path: CERT_PATH.to_string(),
        ingress_auth: valid_ingress_auth(),
        ..Default::default()
    }
}

fn check(name: &str, cfg: &RouterConfig, want_err: Option<&str>) {
    match (cfg.validate(), want_err) {
        (Ok(()), None) => {}
        (Err(e), None) => panic!("{name}: unexpected error {e}"),
        (Ok(()), Some(want)) => panic!("{name}: accepted, want an error containing {want:?}"),
        (Err(e), Some(want)) => assert!(
            e.to_string().contains(want),
            "{name}: {e} does not contain {want:?}"
        ),
    }
}

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    args: RouterArgs,
}

#[test]
fn router_config_validate() {
    let with = |c: RouterConfig| RouterConfig {
        https_port: 8443,
        envoy_cert_path: CERT_PATH.to_string(),
        ingress_auth: valid_ingress_auth(),
        ..c
    };
    let ingress = |f: fn(&mut RouterConfig)| {
        let mut c = cfg("ingress");
        f(&mut c);
        with(c)
    };
    let cases: Vec<(&str, RouterConfig, Option<&str>)> = vec![
        (
            "defaults are valid (auto breaker, dataplane defaults to envoy)",
            ingress(|c| c.parked_request = park(DEFAULT_PARKED_REQUEST_MAX)),
            None,
        ),
        (
            "dataplane set to envoy is valid",
            ingress(|c| {
                c.dataplane = "envoy".into();
                c.parked_request = park(DEFAULT_PARKED_REQUEST_MAX);
            }),
            None,
        ),
        (
            "unknown dataplane rejected",
            ingress(|c| c.dataplane = "blah".into()),
            Some("--gate-dataplane must be"),
        ),
        (
            "negative extproc-max-requests rejected",
            ingress(|c| c.ext_proc_max_requests = -1),
            Some("must not be negative"),
        ),
        (
            "explicit breaker below the lot rejected",
            ingress(|c| {
                c.ext_proc_max_requests = 512;
                c.parked_request = park(1024);
            }),
            Some("must be >= --parked-request-max"),
        ),
        (
            "explicit breaker equal to the lot accepted",
            ingress(|c| {
                c.ext_proc_max_requests = 1024;
                c.parked_request = park(1024);
            }),
            None,
        ),
        (
            "parking disabled ignores the relation",
            ingress(|c| c.ext_proc_max_requests = 8),
            None,
        ),
        ("explicit ingress mode accepted", with(cfg("ingress")), None),
        ("explicit egress mode accepted", with(cfg("egress")), None),
        (
            "missing mode rejected",
            with(cfg("")),
            Some("--mode must be \"ingress\" or \"egress\""),
        ),
        (
            "all mode rejected",
            with(cfg("all")),
            Some("--mode must be \"ingress\" or \"egress\""),
        ),
        (
            "drain-timeout below the parking budget rejected",
            ingress(|c| {
                c.parked_request = ParkedRequestConfig::with(1024, Duration::from_secs(5));
                c.drain_timeout = Duration::from_secs(2);
            }),
            Some("must be >= --parked-request-budget"),
        ),
        (
            "drain-timeout equal to the parking budget accepted",
            ingress(|c| {
                c.parked_request = ParkedRequestConfig::with(1024, Duration::from_secs(5));
                c.drain_timeout = Duration::from_secs(5);
            }),
            None,
        ),
        (
            "drain-timeout above the parking budget accepted",
            ingress(|c| {
                c.parked_request = ParkedRequestConfig::with(1024, Duration::from_secs(5));
                c.drain_timeout = Duration::from_secs(30);
            }),
            None,
        ),
        (
            "short drain-timeout with parking disabled accepted",
            ingress(|c| c.drain_timeout = Duration::from_secs(1)),
            None,
        ),
        (
            "upstream-connect with the upstream credential bundle accepted",
            ingress(|c| {
                c.upstream_connect = true;
                c.upstream_credential_bundle_path = "/run/bundle.pem".into();
            }),
            None,
        ),
        (
            "upstream-connect without the upstream credential bundle rejected",
            ingress(|c| c.upstream_connect = true),
            Some("requires --upstream-credential-bundle"),
        ),
        (
            "egress mode ignores upstream-connect",
            with(RouterConfig {
                upstream_connect: true,
                ..cfg("egress")
            }),
            None,
        ),
    ];
    for (name, c, want_err) in &cases {
        check(name, c, *want_err);
    }

    // Negative durations cannot be represented, so the flags refuse them.
    for flag in ["--drain-timeout=-1s", "--drain-delay=-1s"] {
        assert!(
            Cli::try_parse_from(["router", "--mode=ingress", flag]).is_err(),
            "{flag} accepted"
        );
    }
}

#[test]
fn router_config_validate_ingress_auth() {
    let cases: Vec<(&str, fn(&mut RouterConfig), Option<&str>)> = vec![
        ("both TLS listeners accepted", |_| {}, None),
        (
            "only the HTTPS listener accepted",
            |c| c.connect_tls_port = 0,
            None,
        ),
        (
            "only the CONNECT-TLS listener accepted",
            |c| c.https_port = 0,
            None,
        ),
        (
            "egress-only instance ignores the ingress settings",
            |c| {
                c.mode = "egress".into();
                c.envoy_cert_path.clear();
                c.ingress_auth = IngressAuthConfig::default();
            },
            None,
        ),
        (
            "missing serving cert rejected",
            |c| c.envoy_cert_path.clear(),
            Some("--envoy-cert-path is required"),
        ),
        (
            "no TLS listener rejected",
            |c| {
                c.https_port = 0;
                c.connect_tls_port = 0;
            },
            Some("--port-https or --port-connect-tls is required"),
        ),
        (
            "missing client CA rejected",
            |c| c.ingress_auth.client_ca_file.clear(),
            Some("--ingress-client-ca-file is required"),
        ),
        (
            "empty allowlist accepted",
            |c| c.ingress_auth.allowed_spiffe_ids.clear(),
            None,
        ),
        (
            "non-SPIFFE ID rejected",
            |c| {
                c.ingress_auth
                    .allowed_spiffe_ids
                    .push("https://cluster.local/ns/demo/sa/client".into())
            },
            Some("is not a valid SPIFFE ID"),
        ),
        (
            "trailing slash rejected",
            |c| {
                c.ingress_auth.allowed_spiffe_ids =
                    vec!["spiffe://cluster.local/ns/demo/sa/client/".into()]
            },
            Some("is not a valid SPIFFE ID"),
        ),
    ];
    for (name, mutate, want_err) in cases {
        let mut c = valid_ingress_config();
        mutate(&mut c);
        check(name, &c, want_err);
    }
}

#[test]
fn router_config_dataplane() {
    for (name, value, want) in [
        ("default", "", Dataplane::Envoy),
        ("explicit envoy", "envoy", Dataplane::Envoy),
    ] {
        let c = RouterConfig {
            dataplane: value.into(),
            ..Default::default()
        };
        assert_eq!(c.dataplane(), Ok(want), "{name}");
    }
}

#[test]
fn mode_serves() {
    for (mode, ingress, egress) in [
        ("", false, false),
        ("ingress", true, false),
        ("egress", false, true),
    ] {
        let c = cfg(mode);
        assert_eq!(c.serves_ingress(), ingress, "{mode:?}");
        assert_eq!(c.serves_egress(), egress, "{mode:?}");
        if let Some(m) = c.mode() {
            assert_eq!((m.serves_ingress(), m.serves_egress()), (ingress, egress));
            assert_eq!(m.as_str(), mode);
        }
    }
}

#[test]
fn router_config_ext_proc_max_requests() {
    for (name, explicit, max, want) in [
        (
            "auto derives twice the default lot",
            0,
            DEFAULT_PARKED_REQUEST_MAX,
            2 * DEFAULT_PARKED_REQUEST_MAX,
        ),
        ("auto scales with a larger lot", 0, 4096, 8192),
        (
            "auto floors at Envoy's default when the lot is small",
            0,
            10,
            EXT_PROC_MAX_REQUESTS_FLOOR,
        ),
        (
            "auto floors when parking is disabled",
            0,
            0,
            EXT_PROC_MAX_REQUESTS_FLOOR,
        ),
        ("explicit value wins over derivation", 1500, 1024, 1500),
    ] {
        let c = RouterConfig {
            ext_proc_max_requests: explicit,
            parked_request: park(max),
            ..Default::default()
        };
        assert_eq!(c.ext_proc_max_requests(), want, "{name}");
    }
}

/// The drain delay the install manifest passes, and its grace period.
fn router_shutdown_budget() -> (Duration, Duration) {
    let m = crate::router::xds::upstream_connect_tests::router_manifest();
    let cli = Cli::try_parse_from(&m.args).expect("the manifest's router args do not parse");
    (cli.args.drain_delay, m.termination_grace_period)
}

#[test]
fn router_config_drain_timeout_independent_of_route_timeout() {
    let (drain_delay, grace) = router_shutdown_budget();
    let c = RouterConfig {
        route_timeout: DEFAULT_ROUTE_TIMEOUT,
        ..Default::default()
    };
    let got = c.drain_timeout(
        &ParkedRequestConfig::with(1024, DEFAULT_PARKED_REQUEST_BUDGET).normalized(),
    );
    assert!(
        got < DEFAULT_ROUTE_TIMEOUT,
        "drain timeout {got:?} follows the route timeout"
    );
    let sequence = drain_delay + (DRAIN_ROUTE_BUDGET + DRAIN_TIMEOUT_MARGIN) + got;
    assert!(
        sequence <= grace,
        "shutdown sequence {sequence:?} exceeds terminationGracePeriodSeconds {grace:?}"
    );
}

#[test]
fn router_config_drain_timeout() {
    for (name, explicit, park_cfg, want) in [
        (
            "auto derives budget + drain route budget + margin",
            Duration::ZERO,
            ParkedRequestConfig::with(1024, Duration::from_secs(5)).normalized(),
            Duration::from_secs(5) + DRAIN_ROUTE_BUDGET + DRAIN_TIMEOUT_MARGIN,
        ),
        (
            "auto scales with a larger budget",
            Duration::ZERO,
            ParkedRequestConfig::with(1024, Duration::from_secs(30)).normalized(),
            Duration::from_secs(30) + DRAIN_ROUTE_BUDGET + DRAIN_TIMEOUT_MARGIN,
        ),
        (
            "parking disabled still derives from the normalized default budget",
            Duration::ZERO,
            park(0).normalized(),
            DEFAULT_PARKED_REQUEST_BUDGET + DRAIN_ROUTE_BUDGET + DRAIN_TIMEOUT_MARGIN,
        ),
        (
            "explicit value wins over derivation",
            Duration::from_secs(42),
            ParkedRequestConfig::with(1024, Duration::from_secs(5)).normalized(),
            Duration::from_secs(42),
        ),
    ] {
        let c = RouterConfig {
            drain_timeout: explicit,
            ..Default::default()
        };
        assert_eq!(c.drain_timeout(&park_cfg), want, "{name}");
    }
}

#[test]
fn set_otlp_collector_or_disable() {
    for (name, addr, host, port) in [
        (
            "usable address is applied",
            "http://collector.otel-system.svc:4317",
            "collector.otel-system.svc",
            4317,
        ),
        (
            "https disables Envoy tracing",
            "https://collector.otel-system.svc:4317",
            "",
            0,
        ),
        (
            "unknown scheme disables Envoy tracing",
            "grpc://collector.otel-system.svc:4317",
            "",
            0,
        ),
        ("hostless URL disables Envoy tracing", "http://:4317", "", 0),
        (
            "non-numeric port disables Envoy tracing",
            "collector.otel-system.svc:grpc",
            "",
            0,
        ),
        ("empty disables Envoy tracing", "", "", 0),
    ] {
        let x = XdsServer::new(0);
        x.set_otlp_collector_or_disable(addr);
        let s = x.settings();
        assert_eq!((s.otlp_host.as_str(), s.otlp_port), (host, port), "{name}");
        assert_eq!(s.build_tracing().is_some(), !host.is_empty(), "{name}");
    }
}

#[test]
fn set_otlp_collector_clears_previous_collector() {
    let x = XdsServer::new(0);
    x.set_otlp_collector_or_disable("http://collector.otel-system.svc:4317");
    x.set_otlp_collector_or_disable("https://collector.otel-system.svc:4317");
    let s = x.settings();
    assert_eq!(
        (s.otlp_host.as_str(), s.otlp_port),
        ("", 0),
        "a rejected address left the previous collector in place"
    );
    assert!(s.build_tracing().is_none());
}

#[test]
fn flag_defaults() {
    let cfg = RouterConfig::from(
        Cli::try_parse_from(["router", "--otlp-collector-address="])
            .unwrap()
            .args,
    );
    assert_eq!(cfg.mode, "");
    assert_eq!(cfg.log_level, "info");
    assert_eq!(cfg.metrics_addr, ":9090");
    assert_eq!(cfg.health_addr, ":9091");
    assert_eq!(cfg.dataplane, "envoy");
    assert_eq!(cfg.namespace, "default");
    assert_eq!(cfg.router_service_name, DEFAULT_ROUTER_SERVICE_NAME);
    assert_eq!(cfg.hivekeep_api_addr, DEFAULT_HIVEKEEP_API_ADDRESS);
    assert_eq!(
        (
            cfg.connect_tls_port,
            cfg.xds_port,
            cfg.extproc_port,
            cfg.https_port
        ),
        (8444, 18000, 50051, 8443)
    );
    assert_eq!(cfg.extproc_addr, "127.0.0.1");
    assert_eq!(
        (cfg.status_port, cfg.status_addr.as_str()),
        (4040, "127.0.0.1")
    );
    assert_eq!(cfg.health_interval, Duration::from_secs(1));
    assert_eq!(
        cfg.upstream_credential_bundle_path,
        DEFAULT_UPSTREAM_CREDENTIAL_BUNDLE
    );
    assert_eq!(
        cfg.upstream_trust_bundle_path,
        DEFAULT_UPSTREAM_TRUST_BUNDLE
    );
    assert!(cfg.upstream_connect);
    assert_eq!(
        cfg.egress_policy_cache_ttl,
        crate::router::egress::DEFAULT_POLICY_CACHE_TTL
    );
    assert!(!cfg.credential_provider.insecure);
    assert_eq!(cfg.route_timeout, DEFAULT_ROUTE_TIMEOUT);
    assert_eq!(cfg.parked_request, ParkedRequestConfig::default());
    assert_eq!(cfg.ext_proc_max_requests, 0);
    assert_eq!(
        (cfg.drain_delay, cfg.drain_timeout),
        (Duration::from_secs(13), Duration::ZERO)
    );
    assert_eq!(cfg.envoy_admin_addr, "localhost:9901");
    assert_eq!(cfg.drain_complete_file, DEFAULT_DRAIN_COMPLETE_FILE);

    let cfg = RouterConfig::from(
        Cli::try_parse_from([
            "router",
            "--ingress-allowed-spiffe-ids=spiffe://a/x,spiffe://a/y",
            "--ingress-allowed-spiffe-ids",
            "spiffe://a/z",
            "--upstream-connect=false",
        ])
        .unwrap()
        .args,
    );
    assert_eq!(
        cfg.ingress_auth.allowed_spiffe_ids,
        vec!["spiffe://a/x", "spiffe://a/y", "spiffe://a/z"]
    );
    assert!(!cfg.upstream_connect);
}

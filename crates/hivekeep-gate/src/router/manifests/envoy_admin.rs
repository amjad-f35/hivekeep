//! The Envoy admin API of both gateways stays on loopback, and the one
//! listener that proxies it from the pod IP forwards only read-only paths.

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;

use serde_json::Value;

use super::support::*;

/// Everything the pod-IP-reachable envoy_metrics listener may forward to the
/// admin API: read-only, no topology, no secrets.
const ADMIN_PROXY_PATHS: [&str; 2] = ["/ready", "/stats/prometheus"];

struct AdminManifest {
    file: &'static str,
    config_map: &'static str,
    deployment: &'static str,
    /// The admin path the rest of the manifest depends on.
    required_path: &'static str,
}

const ADMIN_MANIFESTS: [AdminManifest; 2] = [
    // Scraped by the PodMonitoring.
    AdminManifest {
        file: ROUTER_MANIFEST,
        config_map: ROUTER_ENVOY_CONFIG,
        deployment: ROUTER_DEPLOYMENT,
        required_path: "/stats/prometheus",
    },
    // Probed by the kubelet.
    AdminManifest {
        file: EGRESS_MANIFEST,
        config_map: EGRESS_ENVOY_CONFIG,
        deployment: EGRESS_DEPLOYMENT,
        required_path: "/ready",
    },
];

fn bootstrap(m: &AdminManifest) -> Value {
    parse_yaml(&envoy_config_from(m.file, m.config_map))
}

fn admin_address(b: &Value) -> SocketAddress {
    SocketAddress::of(child(child(b, "admin"), "address"))
}

/// The endpoint addresses of a cluster's load assignment.
fn cluster_endpoints(c: &Value) -> Vec<SocketAddress> {
    list(child(c, "load_assignment"), "endpoints")
        .into_iter()
        .flat_map(|e| list(e, "lb_endpoints"))
        .map(|lb| SocketAddress::of(child(child(lb, "endpoint"), "address")))
        .collect()
}

/// One static-listener route whose cluster is the admin API.
struct AdminProxyRoute<'a> {
    listener: &'a str,
    listener_stat_prefix: &'a str,
    hcm_stat_prefix: &'a str,
    address: SocketAddress,
    route: &'a Value,
}

fn admin_proxy_routes(b: &Value) -> Vec<AdminProxyRoute<'_>> {
    let admin = admin_address(b);
    let admin_clusters: BTreeSet<&str> = clusters(b)
        .into_iter()
        .filter(|c| cluster_endpoints(c).contains(&admin))
        .map(|c| s(c, "name"))
        .collect();
    let mut out = Vec::new();
    for l in listeners(b) {
        for fc in list(l, "filter_chains") {
            for f in list(fc, "filters") {
                let tc = child(f, "typed_config");
                for vh in list(child(tc, "route_config"), "virtual_hosts") {
                    for r in list(vh, "routes") {
                        if admin_clusters.contains(s(child(r, "route"), "cluster")) {
                            out.push(AdminProxyRoute {
                                listener: s(l, "name"),
                                listener_stat_prefix: s(l, "stat_prefix"),
                                hcm_stat_prefix: s(tc, "stat_prefix"),
                                address: SocketAddress::of(child(l, "address")),
                                route: r,
                            });
                        }
                    }
                }
            }
        }
    }
    out
}

fn route_path(r: &Value) -> &str {
    s(child(r, "match"), "path")
}

/// The route must be an exact allowlisted path restricted to GET.
fn check_admin_route(listener: &str, r: &Value) {
    let m = child(r, "match");
    for widened in [
        "prefix",
        "path_separated_prefix",
        "safe_regex",
        "path_match_policy",
    ] {
        assert!(!is_set(m, widened), "listener {listener:?} forwards a {widened} route to the admin API; only exact {ADMIN_PROXY_PATHS:?}");
    }
    let path = s(m, "path");
    assert!(
        ADMIN_PROXY_PATHS.contains(&path),
        "listener {listener:?} forwards {path:?} to the admin API; only {ADMIN_PROXY_PATHS:?}"
    );
    let get_only = list(m, "headers")
        .into_iter()
        .any(|h| s(h, "name") == ":method" && s(child(h, "string_match"), "exact") == "GET");
    assert!(get_only, "listener {listener:?} forwards {path:?} to the admin API without restricting :method to GET");
}

/// Whether the kubelet, which probes the pod IP, can reach a listener bound
/// to a. A bare "::" is IPv6-only unless ipv4_compat is set.
fn reachable_on_pod_ip(a: &SocketAddress) -> bool {
    match a.ip() {
        Some(IpAddr::V4(ip)) => ip.is_unspecified(),
        Some(IpAddr::V6(ip)) => ip.is_unspecified() && a.ipv4_compat,
        None => false,
    }
}

#[test]
fn reachable_on_pod_ip_cases() {
    let sa = |address: &str, ipv4_compat| SocketAddress {
        address: address.into(),
        port_value: 1,
        ipv4_compat,
    };
    assert!(reachable_on_pod_ip(&sa("0.0.0.0", false)));
    assert!(reachable_on_pod_ip(&sa("::", true)));
    assert!(!reachable_on_pod_ip(&sa("::", false)));
    assert!(!reachable_on_pod_ip(&sa("127.0.0.1", false)));
    assert!(!reachable_on_pod_ip(&sa("localhost", false)));
}

/// The admin API is unauthenticated and can shut the gateway down or dump its
/// config, so it must bind loopback and nothing in the pod may point at it.
#[test]
fn envoy_admin_is_loopback_only() {
    for m in &ADMIN_MANIFESTS {
        let admin = admin_address(&bootstrap(m));
        assert!(
            admin.ip().is_some_and(|ip| ip.is_loopback()),
            "{}: Envoy admin binds {:?}; it must be a loopback IP",
            m.deployment,
            admin.address
        );
        assert_ne!(
            admin.port_value, 0,
            "{}: Envoy admin sets no port_value; the manifest changed shape",
            m.deployment
        );
        let pod = pod_spec(m.file, m.deployment);
        for c in all_containers(&pod) {
            let cname = s(c, "name");
            for p in list(c, "ports") {
                assert_ne!(
                    p.get("containerPort").and_then(Value::as_i64),
                    Some(admin.port_value),
                    "{}: container {cname:?} declares the admin port as {:?}",
                    m.deployment,
                    s(p, "name")
                );
            }
            for kind in ["readinessProbe", "livenessProbe", "startupProbe"] {
                let probe = child(c, kind);
                let port = if is_set(probe, "httpGet") {
                    child(child(probe, "httpGet"), "port")
                } else if is_set(probe, "tcpSocket") {
                    child(child(probe, "tcpSocket"), "port")
                } else {
                    continue;
                };
                assert_ne!(
                    resolve_port(port, c),
                    Some(admin.port_value),
                    "{}: container {cname:?}'s {kind} dials the loopback-only admin port",
                    m.deployment
                );
            }
        }
    }
}

/// The proxy listener forwards only exact GET paths, and its stat prefixes
/// name "admin" so the drainer ignores probe and scrape connections.
#[test]
fn envoy_admin_proxy_is_allowlisted() {
    for m in &ADMIN_MANIFESTS {
        let b = bootstrap(m);
        let mut seen = BTreeSet::new();
        for r in admin_proxy_routes(&b) {
            seen.insert(route_path(r.route));
            check_admin_route(r.listener, r.route);
            assert!(
                r.listener_stat_prefix.contains("admin") && r.hcm_stat_prefix.contains("admin"),
                "{}: listener {:?} forwards to the admin API but its stat prefixes ({:?}, {:?}) lack \"admin\", so the drain may never reach zero",
                m.deployment,
                r.listener,
                r.listener_stat_prefix,
                r.hcm_stat_prefix
            );
        }
        assert!(
            seen.contains(m.required_path),
            "{}: no static listener forwards {} to the admin API",
            m.deployment,
            m.required_path
        );
    }
}

/// Each gateway's envoy container is Ready only once Envoy's /ready says so,
/// reached through a listener on the pod IP that forwards it.
#[test]
fn envoy_readiness_probe_asks_envoy() {
    for m in &ADMIN_MANIFESTS {
        let b = bootstrap(m);
        let ready_ports: BTreeSet<i64> = admin_proxy_routes(&b)
            .into_iter()
            .filter(|r| route_path(r.route) == "/ready" && reachable_on_pod_ip(&r.address))
            .map(|r| r.address.port_value)
            .collect();
        let pod = pod_spec(m.file, m.deployment);
        let envoy = by_name(&list(&pod, "containers"), "envoy")
            .unwrap_or_else(|| panic!("{} has no container named envoy", m.deployment));
        let http = child(child(envoy, "readinessProbe"), "httpGet");
        assert!(
            !http.is_null(),
            "{}: the envoy container has no HTTP readiness probe",
            m.deployment
        );
        assert_eq!(
            s(http, "path"),
            "/ready",
            "{}: the envoy readiness probe must ask Envoy's /ready",
            m.deployment
        );
        assert!(
            s(http, "host").is_empty() && s(http, "scheme") != "HTTPS",
            "{}: the readiness probe must dial the pod IP over plain HTTP",
            m.deployment
        );
        let port = resolve_port(child(http, "port"), envoy).unwrap_or(0);
        assert!(ready_ports.contains(&port), "{}: the readiness probe dials port {port}, but no pod-IP listener there forwards /ready", m.deployment);
    }
}

/// The PodMonitoring scrapes a port the router's envoy container declares,
/// and not the loopback-only admin port.
#[test]
fn router_pod_monitoring_avoids_envoy_admin() {
    let admin = admin_address(&parse_yaml(&envoy_config_from(
        ROUTER_MANIFEST,
        ROUTER_ENVOY_CONFIG,
    )));
    let pod = pod_spec(ROUTER_MANIFEST, ROUTER_DEPLOYMENT);
    let envoy =
        by_name(&list(&pod, "containers"), "envoy").expect("router pod has no envoy container");
    let named: BTreeMap<&str, i64> = list(envoy, "ports")
        .into_iter()
        .filter_map(|p| Some((s(p, "name"), p.get("containerPort")?.as_i64()?)))
        .collect();
    let mon = parse_yaml(&read_manifest(ROUTER_MONITORING_MANIFEST));
    let endpoints = list(child(&mon, "spec"), "endpoints");
    assert!(
        !endpoints.is_empty(),
        "{ROUTER_MONITORING_MANIFEST} has no endpoints; the manifest changed shape"
    );
    for ep in endpoints {
        let port = s(ep, "port");
        let n = named.get(port).unwrap_or_else(|| panic!("PodMonitoring scrapes port {port:?}, which the router's envoy container does not declare"));
        assert_ne!(
            *n, admin.port_value,
            "PodMonitoring scrapes the loopback-only admin port"
        );
    }
}

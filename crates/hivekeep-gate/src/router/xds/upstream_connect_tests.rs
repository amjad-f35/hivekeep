use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use envoy_types::pb::envoy::config::cluster::v3::{cluster::DiscoveryType, Cluster};
use envoy_types::pb::envoy::config::core::v3::address;
use envoy_types::pb::envoy::config::core::v3::envoy_internal_address::AddressNameSpecifier;
use envoy_types::pb::envoy::config::listener::v3::{listener, Listener};
use envoy_types::pb::envoy::config::route::v3::{
    header_matcher::HeaderMatchSpecifier, RouteConfiguration,
};
use envoy_types::pb::envoy::extensions::filters::network::set_filter_state::v3::Config as NetworkSetFilterState;
use envoy_types::pb::envoy::extensions::filters::network::tcp_proxy::v3::TcpProxy;
use envoy_types::pb::envoy::extensions::transport_sockets::tls::v3::UpstreamTlsContext;
use envoy_types::pb::envoy::extensions::upstreams::http::v3::http_protocol_options::{
    explicit_http_config::ProtocolConfig, UpstreamProtocolOptions,
};
use envoy_types::pb::envoy::r#type::matcher::v3::string_matcher::MatchPattern;

use super::clusters::build_actor_tunnel_cluster;
use super::filters::{actor_tunnel_filter_state_filter, SharedWithUpstream};
use super::listeners::build_actor_connect_listener;
use super::tests::*;
use super::*;
use crate::headers::TARGET_ACTOR_HEADER;
use crate::router::config::{RouterArgs, RouterConfig};
use crate::router::extproc::ORIGINAL_DST_FILTER_STATE_KEY;
use crate::tunnel::ingress::DEFAULT_CONNECT_PORT;

fn upstream_connect_snapshot() -> Arc<Snapshot> {
    let server = XdsServer::new(18000);
    server.set_config(50053, "127.0.0.1");
    server.set_connect_tls_port(8444);
    set_ingress_tls(&server);
    server.set_upstream_tls("/run/bundle.pem", "/run/trust.pem");
    server.set_upstream_connect(true);
    publish(&server)
}

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    args: RouterArgs,
}

/// The xDS server the router builds from `cfg` for an Envoy dataplane.
fn envoy_xds_server(cfg: &RouterConfig) -> Arc<XdsServer> {
    let park = cfg.parked_request.normalized();
    let x = XdsServer::new(cfg.xds_port);
    x.set_config(cfg.extproc_port, &cfg.extproc_addr);
    x.set_connect_tls_port(cfg.connect_tls_port);
    x.set_otlp_collector_or_disable(&cfg.otlp_collector_address);
    x.set_route_timeout(cfg.route_timeout);
    x.set_ext_proc_max_requests(cfg.ext_proc_max_requests());
    if park.enabled() {
        x.set_ext_proc_message_timeout(park.budget + Duration::from_secs(5));
    }
    x.set_tls_config(cfg.https_port, &cfg.envoy_cert_path);
    x.set_downstream_client_auth(
        &cfg.ingress_auth.client_ca_file,
        &cfg.ingress_auth.allowed_spiffe_ids,
    );
    x.set_upstream_tls(
        &cfg.upstream_credential_bundle_path,
        &cfg.upstream_trust_bundle_path,
    );
    x.set_upstream_connect(cfg.upstream_connect);
    x.update_snapshot().expect("building the xDS snapshot");
    x
}

#[test]
fn upstream_connect_flag() {
    for (name, extra, want_err, want_tunnels) in [
        ("default tunnels to the sandbox tunnel", vec![], None, true),
        (
            "false reaches the tunnel over HTTPS",
            vec!["--upstream-connect=false"],
            None,
            false,
        ),
        (
            "false without upstream mTLS accepted",
            vec!["--upstream-connect=false", "--upstream-credential-bundle="],
            None,
            false,
        ),
        (
            "default without upstream mTLS rejected",
            vec!["--upstream-credential-bundle="],
            Some("--upstream-connect=false"),
            false,
        ),
    ] {
        let mut argv = vec![
            "router",
            "--mode=ingress",
            "--otlp-collector-address=",
            "--envoy-cert-path=/run/servicedns.podcert.hivekeep.io/credential-bundle.pem",
            "--ingress-client-ca-file=/run/podidentity.podcert.hivekeep.io/trust-bundle.pem",
            "--ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/demo/sa/client",
        ];
        argv.extend(extra);
        let cfg = RouterConfig::from(
            Cli::try_parse_from(argv)
                .unwrap_or_else(|e| panic!("{name}: {e}"))
                .args,
        );
        let res = cfg.validate();
        if let Some(want) = want_err {
            let err = res.expect_err(name).to_string();
            assert!(err.contains(want), "{name}: {err}");
            continue;
        }
        res.unwrap_or_else(|e| panic!("{name}: {e}"));

        let snap = envoy_xds_server(&cfg).snapshot().unwrap();
        snap.consistent().unwrap();
        let rc = &snap.decoded::<RouteConfiguration>()[ROUTE_NAME];
        let routed: Vec<String> = rc.virtual_hosts[0]
            .routes
            .iter()
            .map(|r| action_cluster(route_action(r)).to_string())
            .collect();
        let want: Vec<&str> = if want_tunnels {
            vec![
                ACTOR_TUNNEL_HTTP2_CLUSTER_NAME,
                ACTOR_TUNNEL_HTTP1_CLUSTER_NAME,
            ]
        } else {
            vec![ORIGINAL_DST_CLUSTER_NAME]
        };
        assert_eq!(routed, want, "{name}");
        let clusters = snap.decoded::<Cluster>();
        for c in &routed {
            assert!(
                clusters.contains_key(c),
                "{name}: route to missing cluster {c}"
            );
        }
        assert_eq!(
            snap.decoded::<Listener>().contains_key(ACTOR_CONNECT_NAME),
            want_tunnels,
            "{name}"
        );
    }
}

#[test]
fn upstream_connect_resources() {
    let snap = upstream_connect_snapshot();
    let clusters = snap.decoded::<Cluster>();
    for name in [
        CLUSTER_NAME,
        MAIN_INTERNAL_NAME,
        ACTOR_TUNNEL_HTTP1_CLUSTER_NAME,
        ACTOR_TUNNEL_HTTP2_CLUSTER_NAME,
        ACTOR_CONNECT_NAME,
    ] {
        assert!(clusters.contains_key(name), "cluster {name} missing");
    }
    assert!(
        !clusters.contains_key(ORIGINAL_DST_CLUSTER_NAME),
        "original_dst cluster built alongside the tunnels"
    );
    assert_eq!(clusters.len(), 5, "{:?}", clusters.keys());

    let listeners = snap.decoded::<Listener>();
    let l = &listeners[ACTOR_CONNECT_NAME];
    assert!(
        matches!(
            l.listener_specifier,
            Some(listener::ListenerSpecifier::InternalListener(_))
        ),
        "actor_connect is not internal"
    );
    for name in [INGRESS_HTTPS_LISTENER, MAIN_INTERNAL_NAME] {
        let filters = hcm_of(&listeners[name]).http_filters;
        let names: Vec<&str> = filters.iter().map(|f| f.name.as_str()).collect();
        let n = names.len();
        assert!(
            n >= 4
                && names[n - 4] == HTTP_EXT_PROC_FILTER_NAME
                && names[n - 3] == HTTP_SET_FILTER_STATE_NAME
                && names[n - 1] == HTTP_ROUTER_FILTER_NAME,
            "{name}: filters {names:?}"
        );
        let config = set_filter_state(&filters[n - 3]);
        assert_eq!(
            object_key(&config.on_request_headers[0]),
            TUNNEL_KEY_FILTER_STATE_KEY,
            "{name}"
        );
    }
}

#[test]
fn actor_tunnel_filter_state_filter_values() {
    const WORKER: &str = "%DYNAMIC_METADATA(envoy.filters.listener.original_dst:local)%";
    const ACTOR: &str = "%REQ(hk-target-actor)%";
    const PORT: &str = "%DYNAMIC_METADATA(envoy.filters.listener.original_dst:port)%";
    let key = format!("{WORKER}|{ACTOR}|{PORT}");
    let want = std::collections::BTreeMap::from([
        (
            TUNNEL_KEY_FILTER_STATE_KEY,
            ("envoy.hashable_string", key.as_str()),
        ),
        (TUNNEL_WORKER_FILTER_STATE_KEY, ("envoy.string", WORKER)),
        (TUNNEL_ACTOR_FILTER_STATE_KEY, ("envoy.string", ACTOR)),
        (TUNNEL_PORT_FILTER_STATE_KEY, ("envoy.string", PORT)),
    ]);
    let values = set_filter_state(&actor_tunnel_filter_state_filter()).on_request_headers;
    assert_eq!(values.len(), want.len());
    for v in &values {
        let (factory, format) = want
            .get(object_key(v))
            .unwrap_or_else(|| panic!("unexpected key {}", object_key(v)));
        assert_eq!(v.factory_key, *factory, "{}", object_key(v));
        assert_eq!(format_string(v), *format, "{}", object_key(v));
        assert_eq!(
            v.shared_with_upstream,
            SharedWithUpstream::Once as i32,
            "{}",
            object_key(v)
        );
    }
}

#[test]
fn actor_tunnel_routes() {
    let x = XdsServer::new(0);
    x.set_upstream_connect(true);
    let rc = x.settings().build_routes();
    let routes = &rc.virtual_hosts[0].routes;
    assert_eq!(routes.len(), 2);
    let (grpc, rest) = (&routes[0], &routes[1]);
    assert_eq!(
        action_cluster(route_action(grpc)),
        ACTOR_TUNNEL_HTTP2_CLUSTER_NAME
    );
    assert_eq!(
        action_cluster(route_action(rest)),
        ACTOR_TUNNEL_HTTP1_CLUSTER_NAME
    );
    assert!(
        rest.r#match.as_ref().unwrap().headers.is_empty(),
        "the fallback route matches headers"
    );

    let (mut method, mut content_type) = (String::new(), String::new());
    for h in &grpc.r#match.as_ref().unwrap().headers {
        let Some(HeaderMatchSpecifier::StringMatch(sm)) = &h.header_match_specifier else {
            continue;
        };
        match (h.name.as_str(), &sm.match_pattern) {
            (":method", Some(MatchPattern::Exact(m))) => method = m.clone(),
            ("content-type", Some(MatchPattern::SafeRegex(re))) => content_type = re.regex.clone(),
            _ => {}
        }
    }
    assert_eq!(method, "POST");
    let re = regex::Regex::new(&content_type).unwrap();
    for (ct, is_grpc) in [
        ("application/grpc", true),
        ("application/grpc+proto", true),
        ("application/grpc;charset=utf-8", true),
        ("Application/GRPC", true),
        ("application/grpc-web", false),
        ("application/json", false),
    ] {
        assert_eq!(re.is_match(ct), is_grpc, "{ct}");
    }
    for r in routes {
        assert_eq!(r.request_headers_to_remove, vec![TARGET_ACTOR_HEADER]);
        assert!(r.request_headers_to_add.is_empty());
        let a = route_action(r);
        assert_eq!(
            pb::to_std(a.timeout.as_ref().unwrap()),
            DEFAULT_ROUTE_TIMEOUT
        );
        assert_eq!(
            pb::to_std(a.idle_timeout.as_ref().unwrap()),
            x.settings().route_idle_timeout()
        );
    }
}

#[test]
fn actor_connect_listener() {
    let l = build_actor_connect_listener();
    let filters = &l.filter_chains[0].filters;
    assert_eq!(filters.len(), 2);
    let set_worker: NetworkSetFilterState = unpack(filter_any(&filters[0]));
    let values = &set_worker.on_new_connection;
    assert_eq!(values.len(), 1);
    assert_eq!(object_key(&values[0]), ORIGINAL_DST_FILTER_STATE_KEY);
    assert_eq!(
        format_string(&values[0]),
        "%FILTER_STATE(io.hivekeep.tunnel.worker:PLAIN)%"
    );

    let proxy: TcpProxy = unpack(filter_any(&filters[1]));
    assert_eq!(proxy.cluster_specifier, Some(envoy_types::pb::envoy::extensions::filters::network::tcp_proxy::v3::tcp_proxy::ClusterSpecifier::Cluster(ACTOR_CONNECT_NAME.into())));
    let tunnel = proxy.tunneling_config.unwrap();
    assert_eq!(
        tunnel.hostname,
        "actor:%FILTER_STATE(io.hivekeep.tunnel.port:PLAIN)%"
    );
    assert_eq!(tunnel.headers_to_add.len(), 1);
    let h = tunnel.headers_to_add[0].header.as_ref().unwrap();
    assert_eq!(
        (h.key.as_str(), h.value.as_str()),
        (
            TARGET_ACTOR_HEADER,
            "%FILTER_STATE(io.hivekeep.tunnel.actor:PLAIN)%"
        )
    );
    let idle = proxy
        .idle_timeout
        .expect("idle timeout unset: tcp_proxy would default it to 1h");
    assert_eq!(pb::to_std(&idle), Duration::ZERO);
}

fn check_circuit_breakers(c: &Cluster, want: u32) {
    let t = &c.circuit_breakers.as_ref().unwrap().thresholds[0];
    for (name, got) in [
        ("max_connections", t.max_connections.unwrap().value),
        (
            "max_pending_requests",
            t.max_pending_requests.unwrap().value,
        ),
        ("max_requests", t.max_requests.unwrap().value),
    ] {
        assert_eq!(got, want, "{}: {name}", c.name);
    }
}

#[test]
fn actor_tunnel_clusters() {
    for http2 in [false, true] {
        let c = build_actor_tunnel_cluster("tunnel", http2);
        let Some(address::Address::EnvoyInternalAddress(target)) = endpoint_address(&c)
            .as_ref()
            .and_then(|a| a.address.clone())
        else {
            panic!("tunnel cluster does not target an internal listener")
        };
        assert_eq!(
            target.address_name_specifier,
            Some(AddressNameSpecifier::ServerListenerName(
                ACTOR_CONNECT_NAME.into()
            ))
        );
        let opts = protocol_options(&c);
        let Some(UpstreamProtocolOptions::ExplicitHttpConfig(explicit)) =
            &opts.upstream_protocol_options
        else {
            panic!("no explicit HTTP config")
        };
        match (http2, &explicit.protocol_config) {
            (true, Some(ProtocolConfig::Http2ProtocolOptions(_)))
            | (false, Some(ProtocolConfig::HttpProtocolOptions(_))) => {}
            (_, other) => panic!("http2={http2}: protocol config {other:?}"),
        }
        let idle = opts
            .common_http_protocol_options
            .unwrap()
            .idle_timeout
            .unwrap();
        assert_eq!(pb::to_std(&idle), TUNNEL_IDLE_TIMEOUT);
        check_circuit_breakers(&c, TUNNEL_CIRCUIT_BREAKER_LIMIT);
    }

    let x = XdsServer::new(0);
    x.set_upstream_tls("/run/bundle.pem", "/run/trust.pem");
    let c = x.settings().build_actor_connect_cluster();
    assert_eq!(cluster_type(&c), Some(DiscoveryType::OriginalDst));
    let Some(envoy_types::pb::envoy::config::cluster::v3::cluster::LbConfig::OriginalDstLbConfig(
        lb,
    )) = &c.lb_config
    else {
        panic!("no original_dst lb config")
    };
    assert!(
        lb.metadata_key.is_none(),
        "actor_connect reads a metadata key; the worker address comes from filter state"
    );
    assert_eq!(
        lb.upstream_port_override.unwrap().value,
        u32::from(DEFAULT_CONNECT_PORT)
    );
    let utc: UpstreamTlsContext = unpack(socket_any(c.transport_socket.as_ref().unwrap()));
    assert!(
        !utc.common_tls_context
            .unwrap()
            .tls_certificate_sds_secret_configs
            .is_empty(),
        "actor_connect presents no client certificate; the tunnel requires mTLS"
    );
    let Some(UpstreamProtocolOptions::ExplicitHttpConfig(explicit)) =
        protocol_options(&c).upstream_protocol_options
    else {
        panic!("no explicit HTTP config")
    };
    assert!(
        matches!(
            explicit.protocol_config,
            Some(ProtocolConfig::Http2ProtocolOptions(_))
        ),
        "actor_connect is not HTTP/2; tunnels to one worker would each need a connection"
    );
    check_circuit_breakers(&c, ACTOR_CONNECT_CIRCUIT_BREAKER_LIMIT);
}

/// The router container's arguments and the pod's grace period, read from
/// the install manifest.
pub(crate) struct RouterManifest {
    pub args: Vec<String>,
    pub termination_grace_period: Duration,
    pub text: String,
}

/// Reads the gate-router Deployment. A small line scanner is enough for the
/// few fields checked, and keeps a YAML parser out of the dependencies.
pub(crate) fn router_manifest() -> RouterManifest {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../hivekeep-setup/manifests/install/gate-router.yaml");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    let deployment = text
        .split("\n---")
        .find(|doc| doc.contains("kind: Deployment"))
        .expect("no Deployment");
    let grace = deployment
        .lines()
        .find_map(|l| l.trim().strip_prefix("terminationGracePeriodSeconds:"))
        .map(|v| Duration::from_secs(v.trim().parse().unwrap()))
        .expect("no terminationGracePeriodSeconds");
    let mut args = Vec::new();
    let mut lines = deployment
        .lines()
        .skip_while(|l| l.trim() != "- name: gate-router")
        .skip_while(|l| l.trim() != "args:");
    lines.next();
    for l in lines {
        let t = l.trim();
        if t.starts_with('#') || t.is_empty() {
            continue;
        }
        let Some(item) = t.strip_prefix("- ") else {
            break;
        };
        args.push(item.trim_matches('"').to_string());
    }
    RouterManifest {
        args,
        termination_grace_period: grace,
        text,
    }
}

#[test]
fn xds_manifest_is_loopback_only() {
    let m = router_manifest();
    let xds_port: u16 = m
        .args
        .iter()
        .find_map(|a| a.strip_prefix("--port-xds="))
        .expect("the manifest does not set --port-xds")
        .parse()
        .unwrap();
    let cli = Cli::try_parse_from(&m.args).expect("the manifest's router args do not parse");
    assert_eq!(cli.args.port_xds, xds_port);

    // The bootstrap's xds_cluster dials that port on loopback.
    let cluster = m
        .text
        .split("- name: xds_cluster")
        .nth(1)
        .expect("no xds_cluster in the Envoy bootstrap");
    let mut addr = None;
    let mut port = None;
    for l in cluster.lines() {
        let t = l.trim();
        if let Some(a) = t.strip_prefix("address: ") {
            addr = Some(a.trim_matches('"').to_string());
        }
        if let Some(p) = t.strip_prefix("port_value: ") {
            port = Some(p.parse::<u16>().unwrap());
            break;
        }
    }
    let ip: std::net::IpAddr = addr.expect("xds_cluster has no address").parse().unwrap();
    assert!(
        ip.is_loopback(),
        "xds_cluster dials {ip}, which is not loopback"
    );
    assert_eq!(port, Some(xds_port));

    // No container port exposes it.
    for l in m.text.lines() {
        if let Some(p) = l.trim().strip_prefix("containerPort: ") {
            assert_ne!(
                p.parse::<u16>().unwrap(),
                xds_port,
                "the xDS port is exposed as a container port"
            );
        }
    }
}

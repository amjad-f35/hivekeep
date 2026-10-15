//! The egress gateway's chains, ext_proc filters and filter state, pinned to
//! the constants the egress handler dispatches on and answers with. A chain
//! renamed on one side only fails closed at runtime.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::support::*;
use crate::router::extproc::{self as ep, is_egress_filter_chain};

const EXT_PROC_FILTER: &str = "envoy.filters.http.ext_proc";
const SET_FILTER_STATE_FILTER: &str = "envoy.filters.http.set_filter_state";
const EXT_PROC_SERVER_CLUSTER: &str = "ext_proc_server";
const PASSTHROUGH_FORWARD_PROXY_CLUSTER: &str = "egress_forward_proxy_passthrough";
const DFP_CLUSTER_TYPE: &str = "envoy.clusters.dynamic_forward_proxy";
const DFP_HTTP_FILTER: &str = "envoy.filters.http.dynamic_forward_proxy";

/// The chains that decide per request and answer with a dial.
const REQUEST_LEGS: [&str; 2] = [
    ep::EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
    ep::EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
];

/// The egress listener's CONNECT chain.
fn outer_chain(tree: &Value) -> &Value {
    let egress = by_name(&listeners(tree), "egress").expect("no egress listener");
    by_name(&list(egress, "filter_chains"), ep::EGRESS_FILTER_CHAIN_NAME).unwrap_or_else(|| {
        panic!(
            "no {:?} chain on the egress listener",
            ep::EGRESS_FILTER_CHAIN_NAME
        )
    })
}

/// The inner listener the CONNECT leg hands tunnels to.
fn mitm_listener(tree: &Value) -> &Value {
    by_name(&listeners(tree), "mitm_listener").expect("no mitm_listener in the egress manifest")
}

/// A chain's http_filters, the ext_proc config and its index among them.
fn ext_proc_of(chain: &Value) -> (Option<&Value>, Option<usize>, Vec<&Value>) {
    let filters = list(hcm(chain), "http_filters");
    match filter_index(&filters, EXT_PROC_FILTER) {
        Some(i) => (Some(child(filters[i], "typed_config")), Some(i), filters),
        None => (None, None, filters),
    }
}

/// The on_request_headers entries of every set_filter_state filter, with the
/// filter's index.
fn set_filter_state_entries<'a>(filters: &[&'a Value]) -> Vec<(usize, &'a Value)> {
    filters
        .iter()
        .enumerate()
        .filter(|(_, f)| s(f, "name") == SET_FILTER_STATE_FILTER)
        .flat_map(|(i, f)| {
            list(child(f, "typed_config"), "on_request_headers")
                .into_iter()
                .map(move |v| (i, v))
        })
        .collect()
}

fn inline_format(entry: &Value) -> &str {
    s(
        child(child(entry, "format_string"), "text_format_source"),
        "inline_string",
    )
}

/// Every chain that calls ext_proc must be one the handler knows, and the
/// manifest must have exactly the legs its topology implies.
#[test]
fn egress_manifests_name_every_ext_proc_chain() {
    let tree = egress_bootstrap();
    let mut want = vec![
        ep::EGRESS_FILTER_CHAIN_NAME,
        ep::EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
        ep::EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
    ];
    let mut got = Vec::new();
    for (l, chain) in all_chains(&tree) {
        if filter_index(&list(hcm(chain), "http_filters"), EXT_PROC_FILTER).is_none() {
            continue;
        }
        let name = s(chain, "name");
        assert!(
            is_egress_filter_chain(name),
            "listener {:?} has a chain {name:?} that calls ext_proc but is not one the egress handler serves",
            s(l, "name")
        );
        got.push(name);
    }
    got.sort();
    want.sort();
    assert_eq!(got, want, "ext_proc-calling filter chains");
}

/// Whether n's upgrade_configs turn websocket off explicitly.
fn websocket_disabled(n: &Value) -> bool {
    list(n, "upgrade_configs")
        .into_iter()
        .find(|u| s(u, "upgrade_type") == "websocket")
        .is_some_and(|u| u.get("enabled").and_then(Value::as_bool) == Some(false))
}

#[test]
fn websocket_disabled_cases() {
    for (yaml, want) in [
        (
            "upgrade_configs: [{upgrade_type: websocket, enabled: false}]",
            true,
        ),
        (
            "upgrade_configs: [{upgrade_type: websocket, enabled: true}]",
            false,
        ),
        ("upgrade_configs: [{upgrade_type: websocket}]", false),
        (
            "upgrade_configs: [{upgrade_type: CONNECT, enabled: false}]",
            false,
        ),
        ("{}", false),
    ] {
        assert_eq!(websocket_disabled(&parse_yaml(yaml)), want, "{yaml}");
    }
}

#[test]
fn egress_manifests_disable_web_socket_upgrades() {
    let tree = egress_bootstrap();
    let chains = list(mitm_listener(&tree), "filter_chains");
    for name in REQUEST_LEGS {
        let chain = by_name(&chains, name)
            .unwrap_or_else(|| panic!("expected request-leg chain {name:?} is absent"));
        assert!(
            websocket_disabled(hcm(chain)),
            "chain {name:?} must explicitly disable websocket upgrades"
        );
        let routes: Vec<&Value> = list(child(hcm(chain), "route_config"), "virtual_hosts")
            .into_iter()
            .flat_map(|vh| list(vh, "routes"))
            .collect();
        assert!(!routes.is_empty(), "chain {name:?} has no routes");
        for (i, route) in routes.iter().enumerate() {
            let action = child(route, "route");
            assert!(
                !action.is_null(),
                "chain {name:?} route {i} has no route action"
            );
            assert!(
                websocket_disabled(action),
                "chain {name:?} route {i} must explicitly disable websocket upgrades"
            );
        }
    }
}

/// Every ext_proc fails closed, talks to the co-located sidecar, asks for the
/// attributes its leg reads and can rewrite nothing that routes.
#[test]
fn egress_manifests_ext_proc_filters() {
    let required: BTreeMap<&str, Vec<&str>> = BTreeMap::from([
        (
            ep::EGRESS_FILTER_CHAIN_NAME,
            vec![ep::FILTER_CHAIN_NAME_ATTRIBUTE],
        ),
        (
            ep::EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
            vec![
                ep::FILTER_CHAIN_NAME_ATTRIBUTE,
                ep::ACTOR_IDENTITY_FILTER_STATE_ATTRIBUTE,
                ep::CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE,
            ],
        ),
        (
            ep::EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
            vec![
                ep::FILTER_CHAIN_NAME_ATTRIBUTE,
                ep::ACTOR_IDENTITY_FILTER_STATE_ATTRIBUTE,
                ep::CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE,
                ep::REQUESTED_SERVER_NAME_ATTRIBUTE,
            ],
        ),
    ]);
    let tree = egress_bootstrap();
    let mut prefixes: BTreeMap<String, String> = BTreeMap::new();
    let mut seen = 0;
    for (_, chain) in all_chains(&tree) {
        let name = s(chain, "name");
        let Some(attrs) = required.get(name) else {
            continue;
        };
        seen += 1;
        let (cfg, i, filters) = ext_proc_of(chain);
        let (Some(cfg), Some(i)) = (cfg, i) else {
            panic!("chain {name:?} has no ext_proc filter")
        };
        assert_eq!(
            s(
                child(child(cfg, "grpc_service"), "envoy_grpc"),
                "cluster_name"
            ),
            EXT_PROC_SERVER_CLUSTER,
            "chain {name:?} ext_proc cluster"
        );
        assert!(!flag(cfg, "failure_mode_allow"), "chain {name:?} ext_proc has failure_mode_allow: true; a sidecar outage would let every request through");
        let prefix = s(cfg, "stat_prefix");
        assert!(
            !prefix.is_empty(),
            "chain {name:?} ext_proc has no stat_prefix"
        );
        if let Some(other) = prefixes.insert(prefix.to_string(), name.to_string()) {
            panic!("chain {name:?} ext_proc reuses stat_prefix {prefix:?} of chain {other:?}");
        }
        let got = strs(cfg, "request_attributes");
        for attr in attrs {
            assert!(got.contains(attr), "chain {name:?} ext_proc does not request {attr:?}; the handler would read it as absent and deny");
        }
        // The decision comes before routing, and before the forward proxy
        // resolves the Host it names.
        for later in [DFP_HTTP_FILTER, "envoy.filters.http.router"] {
            if let Some(j) = filter_index(&filters, later) {
                assert!(j >= i, "chain {name:?} runs {later} before ext_proc");
            }
        }
        // The name that was policed is the name that is dialed.
        let rules = child(cfg, "mutation_rules");
        assert!(
            flag(rules, "disallow_is_error"),
            "chain {name:?} ext_proc does not set mutation_rules.disallow_is_error"
        );
        assert!(
            flag(rules, "disallow_system") && !flag(rules, "allow_all_routing"),
            "chain {name:?} ext_proc may rewrite routing headers"
        );
        let admitted = strs(
            child(child(cfg, "metadata_options"), "receiving_namespaces"),
            "untyped",
        );
        assert!(
            admitted.contains(&ep::EGRESS_METADATA_NAMESPACE),
            "chain {name:?} ext_proc does not admit dynamic metadata in {:?}; its answer would be dropped",
            ep::EGRESS_METADATA_NAMESPACE
        );
    }
    assert_eq!(
        seen,
        required.len(),
        "not every egress leg is in the manifest"
    );
}

/// The identity and dialed authority cross the inner hop as filter state
/// shared with the upstream.
#[test]
fn egress_manifests_share_identity_with_the_inner_listener() {
    let tree = egress_bootstrap();
    let filters = list(hcm(outer_chain(&tree)), "http_filters");
    let entries = set_filter_state_entries(&filters);
    for key in [
        ep::ACTOR_IDENTITY_FILTER_STATE_KEY,
        ep::CONNECT_AUTHORITY_FILTER_STATE_KEY,
    ] {
        let (_, entry) = entries
            .iter()
            .find(|(_, v)| s(v, "object_key") == key)
            .unwrap_or_else(|| panic!("the egress chain never sets {key}"));
        assert!(
            !s(entry, "shared_with_upstream").is_empty(),
            "{key} is not shared with upstream; the inner legs would not see it"
        );
    }
}

/// http_inspector steers HTTP/1.0 onto the cleartext chain, so its codec has
/// to accept it.
#[test]
fn egress_manifests_cleartext_accepts_http10() {
    let tree = egress_bootstrap();
    let mut seen = false;
    for (_, chain) in all_chains(&tree) {
        if s(chain, "name") != ep::EGRESS_CLEARTEXT_FILTER_CHAIN_NAME {
            continue;
        }
        seen = true;
        assert!(
            flag(child(hcm(chain), "http_protocol_options"), "accept_http_10"),
            "{} does not accept HTTP/1.0",
            ep::EGRESS_CLEARTEXT_FILTER_CHAIN_NAME
        );
    }
    assert!(seen, "no cleartext chain");
}

/// A connection no tunnel holds keeps its inner pool alive until the idle
/// timeout, which must not be disabled.
#[test]
fn egress_manifests_time_out_idle_inner_connections() {
    let tree = egress_bootstrap();
    let mut internal = 0;
    for cluster in clusters(&tree) {
        if !json(cluster).contains("\"server_listener_name\"") {
            continue;
        }
        internal += 1;
        let tcp = child(
            child(cluster, "typed_extension_protocol_options"),
            "envoy.extensions.upstreams.tcp.v3.TcpProtocolOptions",
        );
        let got = s(tcp, "idle_timeout");
        assert!(
            !got.trim_matches(|c| matches!(c, '0' | '.' | 's'))
                .is_empty(),
            "internal cluster {:?} idle_timeout is {got:?}; want a non-zero timeout",
            s(cluster, "name")
        );
    }
    assert!(internal > 0, "no cluster targets an internal listener");
}

/// The dynamic_metadata matchers on a single key of the egress namespace.
fn egress_metadata_matches<'a>(m: &'a Value, key: &str) -> Vec<&'a Value> {
    list(m, "dynamic_metadata")
        .into_iter()
        .filter(|d| s(d, "filter") == ep::EGRESS_METADATA_NAMESPACE)
        .filter(|d| {
            let segs = list(d, "path");
            segs.len() == 1 && s(segs[0], "key") == key
        })
        .collect()
}

/// The dial a route's match requires, or "".
fn dial_match_of(m: &Value) -> &str {
    egress_metadata_matches(m, ep::EGRESS_DIAL_KEY)
        .first()
        .map_or("", |d| s(child(child(d, "value"), "string_match"), "exact"))
}

/// Whether a route matches only when the answer carries the name to dial.
fn requires_dial_host(m: &Value) -> bool {
    egress_metadata_matches(m, ep::EGRESS_DIAL_HOST_KEY)
        .iter()
        .any(|d| flag(child(d, "value"), "present_match"))
}

/// Whether a cluster takes the SNI and the certificate check from the Host.
fn auto_sni_and_san(cluster: &Value) -> bool {
    let Some(opts) = child(cluster, "typed_extension_protocol_options").as_object() else {
        return false;
    };
    opts.values().find(|v| v.is_object()).is_some_and(|m| {
        let up = child(m, "upstream_http_protocol_options");
        flag(up, "auto_sni") && flag(up, "auto_san_validation")
    })
}

/// A request leg's answer picks its route: every route matches dial=name with
/// no default, goes to a dynamic forward proxy cluster and requires the
/// answered host. The MITM leg re-originates TLS checked against the Host; the
/// cleartext leg does not wrap the actor's plaintext.
#[test]
fn egress_manifests_request_legs_route_by_the_dial() {
    let tree = egress_bootstrap();
    let all = clusters(&tree);
    for (_, chain) in all_chains(&tree) {
        let name = s(chain, "name");
        if !REQUEST_LEGS.contains(&name) {
            continue;
        }
        let mut routes = 0;
        for vh in list(child(hcm(chain), "route_config"), "virtual_hosts") {
            for r in list(vh, "routes") {
                let cluster_name = s(child(r, "route"), "cluster");
                let cluster = by_name(&all, cluster_name).unwrap_or_else(|| {
                    panic!(
                        "chain {name:?} routes to cluster {cluster_name:?}, which does not exist"
                    )
                });
                routes += 1;
                assert_eq!(
                    dial_match_of(child(r, "match")),
                    ep::EGRESS_DIAL_NAME,
                    "chain {name:?} route to {cluster_name:?} dial match"
                );
                assert_eq!(
                    s(child(cluster, "cluster_type"), "name"),
                    DFP_CLUSTER_TYPE,
                    "chain {name:?} cluster {cluster_name:?} type"
                );
                assert!(requires_dial_host(child(r, "match")), "chain {name:?} sends dial=name to {cluster_name:?} without requiring the dial host");
                let tls = is_set(cluster, "transport_socket");
                if name == ep::EGRESS_TLS_MITM_FILTER_CHAIN_NAME {
                    assert!(tls && auto_sni_and_san(cluster), "chain {name:?} routes to {cluster_name:?}, which does not re-originate TLS checked against the Host");
                }
                if name == ep::EGRESS_CLEARTEXT_FILTER_CHAIN_NAME {
                    assert!(!tls, "chain {name:?} routes to {cluster_name:?}, which wraps the actor's plaintext in TLS");
                }
            }
        }
        assert!(
            routes > 0,
            "chain {name:?} has no route for dial={}",
            ep::EGRESS_DIAL_NAME
        );
    }
}

/// Each request leg dials the answered name: a set_filter_state between
/// ext_proc and the forward proxy filter copies it into the dynamic host,
/// and the filter resolves that entry rather than the Host as sent.
#[test]
fn egress_manifests_request_legs_dial_the_answered_name() {
    let tree = egress_bootstrap();
    let key = ep::UPSTREAM_DYNAMIC_HOST_FILTER_STATE_KEY;
    assert_eq!(
        filter_state_writers(&tree, key).len(),
        REQUEST_LEGS.len(),
        "{key} writers, want one per request leg"
    );
    for leg in REQUEST_LEGS {
        let chain = by_name(&list(mitm_listener(&tree), "filter_chains"), leg)
            .unwrap_or_else(|| panic!("no {leg:?} chain on mitm_listener"));
        let (_, ext_proc_at, filters) = ext_proc_of(chain);
        let fp = filter_index(&filters, DFP_HTTP_FILTER)
            .unwrap_or_else(|| panic!("chain {leg:?} has no dynamic_forward_proxy filter"));
        assert!(
            flag(
                child(filters[fp], "typed_config"),
                "allow_dynamic_host_from_filter_state"
            ),
            "chain {leg:?}'s dynamic_forward_proxy resolves the Host as sent, not {key}"
        );
        let (at, entry) = set_filter_state_entries(&filters)
            .into_iter()
            .rfind(|(_, v)| s(v, "object_key") == key)
            .unwrap_or_else(|| panic!("chain {leg:?} does not set {key}"));
        let ext_proc_at = ext_proc_at.expect("request leg without ext_proc");
        assert!(at >= ext_proc_at && at <= fp, "chain {leg:?} sets {key} at [{at}], want between ext_proc [{ext_proc_at}] and dynamic_forward_proxy [{fp}]");
        assert_eq!(
            inline_format(entry),
            ep::EGRESS_DIAL_HOST_FORMAT,
            "chain {leg:?} {key} format"
        );
    }
}

/// The inner listener selects a chain on the egress-policy module's verdict,
/// maps every verdict, and maps "denied" to no chain.
#[test]
fn egress_manifests_inner_listener_selects_on_the_module_verdict() {
    let tree = egress_bootstrap();
    let l = mitm_listener(&tree);
    let filters = list(l, "listener_filters");
    let module = filter_index(&filters, "envoy.filters.listener.dynamic_modules").expect(
        "mitm_listener has no dynamic_modules listener filter; nothing would write the verdict",
    );
    for inspector in [
        "envoy.filters.listener.tls_inspector",
        "envoy.filters.listener.http_inspector",
    ] {
        let i = filter_index(&filters, inspector);
        assert!(i.is_some_and(|i| i < module), "{inspector} is at {i:?}, the module at [{module}]; the module would see no transport protocol or SNI");
    }
    let tree_node = child(child(l, "filter_chain_matcher"), "matcher_tree");
    assert_eq!(
        s(child(child(tree_node, "input"), "typed_config"), "key"),
        ep::EGRESS_FILTER_CHAIN_FILTER_STATE_KEY
    );
    let actions = child(child(tree_node, "exact_match_map"), "map");

    let mut chains = BTreeSet::new();
    for c in list(l, "filter_chains") {
        chains.insert(s(c, "name"));
        assert!(
            !is_set(c, "filter_chain_match"),
            "chain {:?} has a filter_chain_match, which Envoy ignores under a filter_chain_matcher",
            s(c, "name")
        );
    }
    for (verdict, want) in [
        (
            ep::EGRESS_FILTER_CHAIN_MITM,
            ep::EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
        ),
        (
            ep::EGRESS_FILTER_CHAIN_CLEARTEXT,
            ep::EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
        ),
        (ep::EGRESS_FILTER_CHAIN_PASSTHROUGH, "egress_passthrough"),
    ] {
        let got = s(
            child(child(child(actions, verdict), "action"), "typed_config"),
            "value",
        );
        assert_eq!(got, want, "verdict {verdict:?}");
        assert!(
            chains.contains(got),
            "verdict {verdict:?} selects chain {got:?}, which the listener does not have"
        );
    }
    let denied = child(child(actions, ep::EGRESS_FILTER_CHAIN_DENIED), "action");
    if !denied.is_null() {
        let got = s(child(denied, "typed_config"), "value");
        assert!(
            !chains.contains(got),
            "verdict denied selects chain {got:?}; a denied connection must match no chain"
        );
    }
}

/// The CONNECT leg admits the policy namespace and copies the policy hash,
/// after ext_proc, into filter state shared with the inner listener.
#[test]
fn egress_manifests_connect_leg_hands_the_sni_rules_hash_to_the_inner_listener() {
    let tree = egress_bootstrap();
    let (cfg, ext_proc_at, filters) = ext_proc_of(outer_chain(&tree));
    let cfg = cfg.unwrap_or_else(|| {
        panic!(
            "chain {:?} has no ext_proc filter",
            ep::EGRESS_FILTER_CHAIN_NAME
        )
    });
    let admitted = strs(
        child(child(cfg, "metadata_options"), "receiving_namespaces"),
        "untyped",
    );
    assert!(
        admitted.contains(&ep::EGRESS_POLICY_METADATA_NAMESPACE),
        "the CONNECT leg's ext_proc does not admit {}",
        ep::EGRESS_POLICY_METADATA_NAMESPACE
    );

    let hash_key = ep::EGRESS_POLICY_HASH_FILTER_STATE_KEY;
    let writers = filter_state_writers(&tree, hash_key);
    assert_eq!(
        writers.len(),
        1,
        "{hash_key} writers, want exactly the CONNECT leg's"
    );
    let entry = writers[0];

    let mut got_keys = Vec::new();
    let mut transitive = Vec::new();
    let mut by_key = BTreeMap::new();
    let mut at = None;
    for (i, v) in set_filter_state_entries(&filters) {
        let key = s(v, "object_key");
        got_keys.push(key);
        by_key.insert(key, v);
        if key == hash_key {
            at = Some(i);
        }
        match s(v, "shared_with_upstream") {
            "TRANSITIVE" => transitive.push(key),
            "ONCE" => {}
            other => panic!("{key} has shared_with_upstream {other:?}, want ONCE or TRANSITIVE"),
        }
    }
    assert_eq!(
        got_keys,
        [
            ep::ACTOR_IDENTITY_FILTER_STATE_KEY,
            ep::CONNECT_AUTHORITY_FILTER_STATE_KEY,
            ep::UPSTREAM_DYNAMIC_PORT_FILTER_STATE_KEY,
            hash_key
        ],
        "outer chain set_filter_state keys"
    );
    // These partition origin pools per actor and port.
    assert_eq!(
        transitive,
        [
            ep::ACTOR_IDENTITY_FILTER_STATE_KEY,
            ep::CONNECT_AUTHORITY_FILTER_STATE_KEY
        ],
        "TRANSITIVE keys"
    );
    // Pool selection hashes these, so their factory must be hashable.
    for key in [
        ep::ACTOR_IDENTITY_FILTER_STATE_KEY,
        ep::CONNECT_AUTHORITY_FILTER_STATE_KEY,
        hash_key,
    ] {
        assert_eq!(
            s(by_key[key], "factory_key"),
            "envoy.hashable_string",
            "{key} factory"
        );
    }
    let at = at.unwrap_or_else(|| panic!("the writer of {hash_key} is not on the outer chain"));
    let ext_proc_at = ext_proc_at.unwrap();
    assert!(
        at >= ext_proc_at,
        "{hash_key} is set at [{at}], before ext_proc at [{ext_proc_at}]"
    );
    assert_eq!(
        inline_format(entry),
        ep::EGRESS_POLICY_HASH_FORMAT,
        "{hash_key} format"
    );
    assert!(
        flag(entry, "skip_if_empty"),
        "{hash_key} is not skip_if_empty; an absent hash would be written as an empty one"
    );
}

/// Denied connections match no chain, so the listener access log is their
/// only record: NR only, with the SNI, actor and verdict.
#[test]
fn egress_manifests_inner_listener_logs_denied_connections() {
    let tree = egress_bootstrap();
    let logs = list(mitm_listener(&tree), "access_log");
    assert!(
        !logs.is_empty(),
        "mitm_listener has no access_log; a denied ClientHello would leave no record"
    );
    for (i, al) in logs.iter().enumerate() {
        let flags = strs(child(child(al, "filter"), "response_flag_filter"), "flags");
        assert!(
            flags.contains(&"NR"),
            "access_log[{i}] is not confined to response flag NR (got {flags:?})"
        );
        let format = child(
            child(child(al, "typed_config"), "log_format"),
            "json_format",
        );
        let values: Vec<&str> = format
            .as_object()
            .map(|m| m.values().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        for want in [
            "%REQUESTED_SERVER_NAME%".to_string(),
            format!(
                "%FILTER_STATE({}:PLAIN)%",
                ep::ACTOR_IDENTITY_FILTER_STATE_KEY
            ),
            format!(
                "%FILTER_STATE({}:PLAIN)%",
                ep::EGRESS_FILTER_CHAIN_FILTER_STATE_KEY
            ),
        ] {
            assert!(
                values.iter().any(|v| v.contains(&want)),
                "access_log[{i}] does not log {want}"
            );
        }
    }
}

/// The passthrough chain relays TLS unread to the resolved SNI through a raw
/// forward proxy cluster only it may use, and nothing dials the address the
/// actor chose.
#[test]
fn egress_manifests_passthrough_chain_dials_the_resolved_sni() {
    let tree = egress_bootstrap();
    let chain = by_name(
        &list(mitm_listener(&tree), "filter_chains"),
        "egress_passthrough",
    )
    .expect("no egress_passthrough chain on mitm_listener");
    let filters = list(chain, "filters");
    let names: Vec<&str> = filters.iter().map(|f| s(f, "name")).collect();
    assert_eq!(
        names,
        [
            "envoy.filters.network.sni_dynamic_forward_proxy",
            "envoy.filters.network.tcp_proxy"
        ],
        "egress_passthrough filters"
    );
    let sni = child(filters[0], "typed_config");
    let proxy = child(filters[1], "typed_config");
    assert_eq!(s(proxy, "cluster"), PASSTHROUGH_FORWARD_PROXY_CLUSTER);
    assert!(
        !is_set(proxy, "tunneling_config"),
        "egress_passthrough wraps the connection in a CONNECT; it relays bytes as they are"
    );

    let all = clusters(&tree);
    let cluster = by_name(&all, PASSTHROUGH_FORWARD_PROXY_CLUSTER)
        .unwrap_or_else(|| panic!("no {PASSTHROUGH_FORWARD_PROXY_CLUSTER} cluster"));
    let cluster_type = child(child(cluster, "cluster_type"), "typed_config");
    assert!(
        s(cluster_type, "@type").contains("dynamic_forward_proxy"),
        "{PASSTHROUGH_FORWARD_PROXY_CLUSTER} is not a dynamic forward proxy"
    );
    // Envoy rejects two differing configs of one DNS cache.
    let want = child(sni, "dns_cache_config");
    assert!(
        !want.is_null(),
        "sni_dynamic_forward_proxy has no dns_cache_config"
    );
    assert_eq!(
        want,
        child(cluster_type, "dns_cache_config"),
        "sni_dynamic_forward_proxy and cluster dns_cache_config differ"
    );
    assert!(
        !is_set(cluster, "transport_socket"),
        "{PASSTHROUGH_FORWARD_PROXY_CLUSTER} wraps the actor's own TLS"
    );
    assert!(
        !is_set(cluster, "typed_extension_protocol_options"),
        "{PASSTHROUGH_FORWARD_PROXY_CLUSTER} has HTTP protocol options"
    );

    for (_, c) in all_chains(&tree) {
        if s(c, "name") != "egress_passthrough" {
            assert!(
                !json(c).contains(PASSTHROUGH_FORWARD_PROXY_CLUSTER),
                "chain {:?} references {PASSTHROUGH_FORWARD_PROXY_CLUSTER}",
                s(c, "name")
            );
        }
    }
    for c in &all {
        let name = s(c, "name");
        let j = json(c);
        assert!(
            name == PASSTHROUGH_FORWARD_PROXY_CLUSTER || !j.contains("dynamic_forward_proxy") || j.contains("\"typed_extension_protocol_options\""),
            "cluster {name:?} is a dynamic forward proxy without HTTP protocol options; a raw by-name dial has no leg to authorize it"
        );
        assert_ne!(
            s(c, "type"),
            "ORIGINAL_DST",
            "cluster {name:?} is ORIGINAL_DST; the gateway never dials the actor's address"
        );
    }
    assert!(
        filter_state_writers(&tree, ep::ORIGINAL_DST_FILTER_STATE_KEY).is_empty(),
        "the gateway carries the actor's dialed address to the inner listener"
    );
}

/// The dialed port reaches the passthrough chain as filter state the outer
/// chain copies from the CONNECT leg's answer, after ext_proc.
#[test]
fn egress_manifests_connect_leg_hands_the_dialed_port_to_the_inner_listener() {
    let tree = egress_bootstrap();
    let key = ep::UPSTREAM_DYNAMIC_PORT_FILTER_STATE_KEY;
    let writers = filter_state_writers(&tree, key);
    assert_eq!(
        writers.len(),
        1,
        "{key} writers, want exactly the CONNECT leg's"
    );
    let entry = writers[0];
    let (_, ext_proc_at, filters) = ext_proc_of(outer_chain(&tree));
    let at = set_filter_state_entries(&filters)
        .into_iter()
        .rfind(|(_, v)| s(v, "object_key") == key)
        .map(|(i, _)| i)
        .unwrap_or_else(|| panic!("the writer of {key} is not on the outer chain"));
    let ext_proc_at = ext_proc_at.unwrap();
    assert!(
        at >= ext_proc_at,
        "{key} is set at [{at}], before ext_proc at [{ext_proc_at}]"
    );
    assert_eq!(inline_format(entry), ep::EGRESS_DIALED_PORT_FORMAT);
    assert_eq!(
        s(entry, "factory_key"),
        "",
        "{key} names a factory; the factory of the same name parses the port"
    );
    assert!(
        !s(entry, "shared_with_upstream").is_empty(),
        "{key} is not shared with upstream"
    );
}

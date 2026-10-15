//! Loading and navigating the install manifests as generic JSON trees.

use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;
use serde_json::Value;

use crate::router::config::RouterArgs;

pub const ROUTER_MANIFEST: &str = "gate-router.yaml";
pub const EGRESS_MANIFEST: &str = "gate-egress.yaml";
pub const ROUTER_MONITORING_MANIFEST: &str = "gate-router-monitoring.yaml";
pub const ROUTER_DEPLOYMENT: &str = "gate-router";
pub const ROUTER_CONTAINER: &str = "gate-router";
pub const ROUTER_ENVOY_CONFIG: &str = "gate-router-envoy-config";
pub const EGRESS_DEPLOYMENT: &str = "gate-egress";
pub const EGRESS_ENVOY_CONFIG: &str = "gate-egress";

static NULL: Value = Value::Null;

pub fn manifest_path(file: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../hivekeep-setup/manifests/install")
        .join(file)
}

pub fn read_manifest(file: &str) -> String {
    let path = manifest_path(file);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

/// Every document of a (possibly multi-document) YAML file.
pub fn documents(file: &str) -> Vec<Value> {
    let raw = read_manifest(file);
    serde_yaml::Deserializer::from_str(&raw)
        .map(|d| {
            Value::deserialize(d).unwrap_or_else(|e| panic!("parsing a document of {file}: {e}"))
        })
        .filter(|v| !v.is_null())
        .collect()
}

pub fn parse_yaml(raw: &str) -> Value {
    serde_yaml::from_str(raw).unwrap_or_else(|e| panic!("parsing YAML: {e}"))
}

/// The object of the given kind and name in a manifest.
pub fn object(file: &str, kind: &str, name: &str) -> Value {
    documents(file)
        .into_iter()
        .find(|d| s(d, "kind") == kind && s(child(d, "metadata"), "name") == name)
        .unwrap_or_else(|| panic!("{file} has no {kind} named {name}"))
}

/// The pod spec of a Deployment.
pub fn pod_spec(file: &str, deployment: &str) -> Value {
    child(
        child(
            child(&object(file, "Deployment", deployment), "spec"),
            "template",
        ),
        "spec",
    )
    .clone()
}

/// The envoy.yaml a ConfigMap ships.
pub fn envoy_config_from(file: &str, config_map: &str) -> String {
    let cm = object(file, "ConfigMap", config_map);
    child(&cm, "data")
        .get("envoy.yaml")
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("the {config_map} ConfigMap in {file} has no envoy.yaml key"))
        .to_string()
}

/// The egress gateway's Envoy bootstrap as a tree.
pub fn egress_bootstrap() -> Value {
    parse_yaml(&envoy_config_from(EGRESS_MANIFEST, EGRESS_ENVOY_CONFIG))
}

pub fn child<'a>(n: &'a Value, key: &str) -> &'a Value {
    n.get(key).unwrap_or(&NULL)
}

/// The objects in the list at key.
pub fn list<'a>(n: &'a Value, key: &str) -> Vec<&'a Value> {
    n.get(key)
        .and_then(Value::as_array)
        .map(|a| a.iter().filter(|v| v.is_object()).collect())
        .unwrap_or_default()
}

pub fn s<'a>(n: &'a Value, key: &str) -> &'a str {
    n.get(key).and_then(Value::as_str).unwrap_or("")
}

pub fn strs<'a>(n: &'a Value, key: &str) -> Vec<&'a str> {
    n.get(key)
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

pub fn flag(n: &Value, key: &str) -> bool {
    n.get(key).and_then(Value::as_bool).unwrap_or(false)
}

pub fn is_set(n: &Value, key: &str) -> bool {
    n.get(key).is_some_and(|v| !v.is_null())
}

pub fn by_name<'a>(items: &[&'a Value], name: &str) -> Option<&'a Value> {
    items.iter().copied().find(|i| s(i, "name") == name)
}

/// Init containers then containers.
pub fn all_containers(pod: &Value) -> Vec<&Value> {
    let mut out = list(pod, "initContainers");
    out.extend(list(pod, "containers"));
    out
}

pub fn container<'a>(pod: &'a Value, name: &str) -> &'a Value {
    by_name(&all_containers(pod), name)
        .unwrap_or_else(|| panic!("the pod has no container named {name:?}"))
}

pub fn args(c: &Value) -> Vec<String> {
    strs(c, "args").into_iter().map(str::to_string).collect()
}

pub fn container_port(c: &Value, name: &str) -> Option<i64> {
    list(c, "ports")
        .into_iter()
        .find(|p| s(p, "name") == name)
        .and_then(|p| p.get("containerPort")?.as_i64())
}

/// Resolves a probe or service port that may be a number or a port name.
pub fn resolve_port(port: &Value, c: &Value) -> Option<i64> {
    match port {
        Value::Number(n) => n.as_i64(),
        Value::String(name) => container_port(c, name),
        _ => None,
    }
}

#[derive(Parser)]
struct RouterCli {
    #[command(flatten)]
    args: RouterArgs,
}

/// The router container's args after the subcommand, parsed as the binary
/// would parse them.
pub fn router_manifest_args() -> (Vec<String>, RouterArgs) {
    let pod = pod_spec(ROUTER_MANIFEST, ROUTER_DEPLOYMENT);
    let a = args(container(&pod, ROUTER_CONTAINER));
    assert!(
        a.first().map(String::as_str) == Some("router"),
        "the {ROUTER_CONTAINER} container's args are {a:?}; they should invoke the router subcommand"
    );
    let rest = a[1..].to_vec();
    let parsed = parse_router_args(&rest);
    (rest, parsed)
}

pub fn parse_router_args(rest: &[String]) -> RouterArgs {
    RouterCli::try_parse_from(std::iter::once("router".to_string()).chain(rest.iter().cloned()))
        .unwrap_or_else(|e| {
            panic!("the {ROUTER_CONTAINER} flags are not ones the binary accepts: {e}")
        })
        .args
}

/// An Envoy socket address.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SocketAddress {
    pub address: String,
    pub port_value: i64,
    pub ipv4_compat: bool,
}

impl SocketAddress {
    pub fn of(n: &Value) -> Self {
        let sa = child(n, "socket_address");
        SocketAddress {
            address: s(sa, "address").to_string(),
            port_value: sa.get("port_value").and_then(Value::as_i64).unwrap_or(0),
            ipv4_compat: flag(sa, "ipv4_compat"),
        }
    }

    pub fn ip(&self) -> Option<std::net::IpAddr> {
        self.address.parse().ok()
    }
}

pub fn listeners(tree: &Value) -> Vec<&Value> {
    list(child(tree, "static_resources"), "listeners")
}

pub fn clusters(tree: &Value) -> Vec<&Value> {
    list(child(tree, "static_resources"), "clusters")
}

/// Every filter chain in the bootstrap with the listener it belongs to.
pub fn all_chains(tree: &Value) -> Vec<(&Value, &Value)> {
    listeners(tree)
        .into_iter()
        .flat_map(|l| list(l, "filter_chains").into_iter().map(move |c| (l, c)))
        .collect()
}

/// The http_connection_manager config of a filter chain, or null for a chain
/// without one (a tcp_proxy chain).
pub fn hcm(chain: &Value) -> &Value {
    list(chain, "filters")
        .into_iter()
        .find(|f| s(f, "name") == "envoy.filters.network.http_connection_manager")
        .map_or(&NULL, |f| child(f, "typed_config"))
}

/// The position of the named filter, if present.
pub fn filter_index(filters: &[&Value], name: &str) -> Option<usize> {
    filters.iter().position(|f| s(f, "name") == name)
}

/// Every set_filter_state entry anywhere in the tree that writes key. Only
/// object_key counts; access-log reads do not.
pub fn filter_state_writers<'a>(v: &'a Value, key: &str) -> Vec<&'a Value> {
    let mut found = Vec::new();
    match v {
        Value::Object(m) => {
            if s(v, "object_key") == key {
                found.push(v);
            }
            for sub in m.values() {
                found.extend(filter_state_writers(sub, key));
            }
        }
        Value::Array(a) => {
            for sub in a {
                found.extend(filter_state_writers(sub, key));
            }
        }
        _ => {}
    }
    found
}

/// The compact JSON form of a subtree.
pub fn json(v: &Value) -> String {
    serde_json::to_string(v).unwrap()
}

//! Test helpers: an [`Env`] over an in-memory cluster, and builders for the
//! objects the steps read.

use super::Env;
use crate::config::Config;
use crate::kube::fake::FakeCluster;
use crate::kube::{Client, Object};
use base64::Engine;
use serde_json::{json, Value};
use std::sync::Arc;

/// An Env over a fake cluster seeded with `objs`.
pub(crate) fn fake_env(cfg: Config, objs: Vec<Object>) -> (Env, FakeCluster) {
    let f = FakeCluster::with_objects(objs);
    let client = Client::new(Arc::new(f.clone())).with_fast_timings();
    (Env::with_client(cfg, client), f)
}

pub(crate) fn namespace_object(name: &str) -> Object {
    json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}})
}

/// A Secret with base64 `data`.
pub(crate) fn secret_object(namespace: &str, name: &str, data: &[(&str, Vec<u8>)]) -> Object {
    let mut map = serde_json::Map::new();
    for (k, v) in data {
        map.insert(
            k.to_string(),
            Value::String(base64::engine::general_purpose::STANDARD.encode(v)),
        );
    }
    json!({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": namespace}, "data": map})
}

pub(crate) fn config_map_object(namespace: &str, name: &str, data: &[(&str, &str)]) -> Object {
    let map: serde_json::Map<String, Value> = data
        .iter()
        .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
        .collect();
    json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name, "namespace": namespace}, "data": map})
}

pub(crate) fn service_account_object(
    namespace: &str,
    name: &str,
    annotations: &[(&str, &str)],
) -> Object {
    let map: serde_json::Map<String, Value> = annotations
        .iter()
        .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
        .collect();
    json!({"apiVersion": "v1", "kind": "ServiceAccount",
           "metadata": {"name": name, "namespace": namespace, "annotations": map}})
}

pub(crate) fn node_object(name: &str, labels: &[(&str, &str)]) -> Object {
    let map: serde_json::Map<String, Value> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
        .collect();
    json!({"apiVersion": "v1", "kind": "Node", "metadata": {"name": name, "labels": map}})
}

/// The `stringData` of a stored Secret (what an apply wrote), by key.
pub(crate) fn string_data(
    f: &FakeCluster,
    namespace: &str,
    name: &str,
    key: &str,
) -> Option<String> {
    let s = f.object(&crate::kube::Gvk::new("", "v1", "Secret"), namespace, name)?;
    s.pointer(&format!("/stringData/{key}"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// The decoded `data` of a stored Secret, by key.
pub(crate) fn secret_data(
    f: &FakeCluster,
    namespace: &str,
    name: &str,
    key: &str,
) -> Option<String> {
    let s = f.object(&crate::kube::Gvk::new("", "v1", "Secret"), namespace, name)?;
    let b64 = s
        .pointer(&format!("/data/{key}"))
        .and_then(|v| v.as_str())?;
    let raw = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    Some(String::from_utf8_lossy(&raw).into_owned())
}

/// The `data` of a stored ConfigMap.
pub(crate) fn config_map_data(
    f: &FakeCluster,
    namespace: &str,
    name: &str,
) -> serde_json::Map<String, Value> {
    f.object(
        &crate::kube::Gvk::new("", "v1", "ConfigMap"),
        namespace,
        name,
    )
    .and_then(|o| o.get("data").and_then(|d| d.as_object()).cloned())
    .unwrap_or_default()
}

//! Every dynamic forward proxy DNS cache the install ships resolves both
//! address families (`dns_lookup_family: ALL`); gate-egress.yaml says why.
// Tests build a default Config and adjust the fields under test.
#![allow(clippy::field_reassign_with_default)]

use super::overlay::system_overlay;
use super::overlay_tests::kustomize_missing;
use super::yamldoc::split_documents;
use super::Env;
use crate::config::{test_config, Config};
use serde_json::Value;
use std::path::{Path, PathBuf};

fn manifests_root() -> PathBuf {
    test_config().manifests
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            walk(&path, out);
        } else {
            out.push(path);
        }
    }
}

/// Every YAML file that ships, not a fixture list.
fn manifest_paths() -> Vec<PathBuf> {
    let mut all = Vec::new();
    walk(&manifests_root(), &mut all);
    all.retain(|p| p.extension().is_some_and(|e| e == "yaml" || e == "yml"));
    all.sort();
    all
}

/// Every buildable kustomization root; components only render through an
/// overlay that includes them.
fn overlay_dirs() -> Vec<PathBuf> {
    manifest_paths()
        .into_iter()
        .filter(|p| p.file_name().is_some_and(|n| n == "kustomization.yaml"))
        .filter(|p| {
            let head: Value = serde_yaml::from_str(&std::fs::read_to_string(p).unwrap()).unwrap();
            head.get("kind").and_then(Value::as_str) != Some("Component")
        })
        .map(|p| p.parent().unwrap().to_path_buf())
        .collect()
}

fn collect_dns_cache_configs(node: &Value, out: &mut Vec<Value>) {
    match node {
        Value::Object(m) => {
            for (k, v) in m {
                if k == "dns_cache_config" && v.is_object() {
                    out.push(v.clone());
                }
                collect_dns_cache_configs(v, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|v| collect_dns_cache_configs(v, out)),
        _ => {}
    }
}

fn dns_cache_configs_in(text: &str) -> Vec<Value> {
    let mut caches = Vec::new();
    for doc in split_documents(text) {
        let Ok(obj) = serde_yaml::from_str::<Value>(&doc) else {
            panic!("unparseable document")
        };
        if obj.get("kind").and_then(Value::as_str) != Some("ConfigMap") {
            continue;
        }
        for v in obj
            .get("data")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
            .filter_map(|(_, v)| v.as_str())
        {
            // Not every ConfigMap value is YAML.
            if let Ok(parsed) = serde_yaml::from_str::<Value>(v) {
                collect_dns_cache_configs(&parsed, &mut caches);
            }
        }
    }
    caches
}

fn dns_cache_configs(path: &Path) -> Vec<Value> {
    dns_cache_configs_in(&std::fs::read_to_string(path).unwrap())
}

fn require_lookup_family_all(what: &str, caches: &[Value]) {
    for cache in caches {
        assert_eq!(
            cache.get("dns_lookup_family").and_then(Value::as_str),
            Some("ALL"),
            "{what}: {:?}",
            cache.get("name")
        );
    }
}

#[test]
fn egress_dns_lookup_family() {
    for path in manifest_paths() {
        require_lookup_family_all(&path.display().to_string(), &dns_cache_configs(&path));
    }
}

#[test]
fn rendered_overlays_dns_lookup_family() {
    if kustomize_missing() {
        return;
    }
    for dir in overlay_dirs() {
        let rendered = crate::kustomize::build(&dir)
            .unwrap_or_else(|e| panic!("rendering {}: {e}", dir.display()));
        require_lookup_family_all(
            &dir.display().to_string(),
            &dns_cache_configs_in(&String::from_utf8(rendered).unwrap()),
        );
    }
}

#[test]
fn egress_manifests_carry_dns_caches() {
    let e = Env::local(test_config());
    assert_eq!(dns_cache_configs(&e.gate_egress_manifest_path()).len(), 7);
}

#[test]
fn overlay_discovery_covers_the_installer() {
    let root = manifests_root();
    let discovered = overlay_dirs();
    for kind in [false, true] {
        let mut cfg = Config::default();
        cfg.kind = kind;
        let dir = root.join(system_overlay(&cfg));
        assert!(
            discovered.contains(&dir),
            "the installer renders {}, but discovery did not find it",
            dir.display()
        );
    }
}

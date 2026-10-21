//! Tests of the control plane renders, over the checked-in manifests.
// Tests build a default Config and adjust the fields under test.
#![allow(clippy::field_reassign_with_default)]

use super::credprovider::{K8S_CREDENTIAL_PROVIDER_DEPLOYMENT, K8S_CREDENTIAL_PROVIDER_MANIFEST};
use super::deploy::FAKE_WORKERSYNC_DEPLOYMENT;
use super::env::NAMESPACE_SYSTEM;
use super::overlay::*;
use super::yamldoc::decode_all;
use super::Env;
use crate::config::{
    test_config, Config, CredentialProvider, CLUSTER_SIZE_SIZE10, K8S_CREDENTIAL_PROVIDER_ADDRESS,
    K8S_CREDENTIAL_PROVIDER_NAME, ROUTER_ENVOY,
};
use crate::images::{Digester, ImageError, Source};
use crate::kube::fake::FakeCluster;
use crate::kube::{self, stub, Gvk, Object, ObjectExt};
use async_trait::async_trait;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment};
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const DIGEST: &str = "sha256:2222222222222222222222222222222222222222222222222222222222222222";

pub(crate) fn kustomize_missing() -> bool {
    if !crate::kustomize::available() {
        eprintln!("skipping: no kustomize renderer (kubectl) on PATH");
        return true;
    }
    false
}

/// Answers every lookup with a fixed digest, recording what was asked.
#[derive(Default)]
pub(crate) struct FixedDigest {
    pub looked: Mutex<Vec<String>>,
    pub fail: bool,
}

#[async_trait]
impl Digester for FixedDigest {
    async fn digest(&self, reference: &str) -> Result<String, ImageError> {
        self.looked.lock().push(reference.to_string());
        if self.fail {
            return Err(ImageError::Registry(format!(
                "resolving {reference} to a digest: MANIFEST_UNKNOWN"
            )));
        }
        Ok(DIGEST.to_string())
    }
}

fn prebuilt_source() -> Source {
    Source {
        repo: "example.com/hivekeep".into(),
        tag: "v1.2.3".into(),
    }
}

/// An Env installing pre-built images whose digests are fixed.
fn prebuilt_env(mut cfg: Config) -> Env {
    cfg.images = prebuilt_source();
    Env::local(cfg).with_digester(Arc::new(FixedDigest::default()))
}

/// An Env over an in-memory cluster holding `objs`.
pub(crate) fn fake_env(cfg: Config, objs: Vec<Object>) -> (Env, FakeCluster) {
    let fake = FakeCluster::with_objects(objs);
    let client = kube::Client::new(Arc::new(fake.clone())).with_fast_timings();
    (Env::with_client(cfg, client), fake)
}

fn docs(manifest: &[u8]) -> Vec<Value> {
    decode_all(manifest).unwrap()
}

fn kind_of(o: &Value) -> &str {
    o.get("kind").and_then(Value::as_str).unwrap_or("")
}

fn name_of(o: &Value) -> &str {
    o.pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap_or("")
}

/// The ConfigMap data of the first ConfigMap in a manifest.
fn first_config_map_data(manifest: &[u8]) -> BTreeMap<String, String> {
    let cm = docs(manifest)
        .into_iter()
        .find(|d| kind_of(d) == "ConfigMap")
        .expect("a ConfigMap");
    serde_json::from_value::<ConfigMap>(cm)
        .unwrap()
        .data
        .unwrap_or_default()
}

fn assert_egress_manifest_parses(manifest: &[u8]) {
    let data = first_config_map_data(manifest);
    serde_yaml::from_str::<serde_yaml::Value>(data.get("envoy.yaml").expect("envoy.yaml"))
        .expect("envoy.yaml parses");
}

#[test]
fn patch_gate_egress_inject_cases() {
    let e = Env::local(test_config());
    let raw = std::fs::read(e.gate_egress_manifest_path()).unwrap();
    let cases: Vec<(&str, CredentialProvider, Vec<&str>)> = vec![
        (
            "kubernetes",
            CredentialProvider {
                name: K8S_CREDENTIAL_PROVIDER_NAME.into(),
                address: K8S_CREDENTIAL_PROVIDER_ADDRESS.into(),
            },
            vec![
                "--credential-provider-name=k8s.io",
                "--credential-provider-address=k8s-credential-provider.hivekeep-system.svc:50051",
                "--credential-provider-server-name=k8s-credential-provider.hivekeep-system.svc",
                "--credential-provider-ca-file=",
                "--credential-provider-client-cert=",
            ],
        ),
        (
            "another provider",
            CredentialProvider {
                name: "vault.example.com".into(),
                address: "vault.hivekeep-system.svc:8200".into(),
            },
            vec![
                "--credential-provider-name=vault.example.com",
                "--credential-provider-address=vault.hivekeep-system.svc:8200",
                "--credential-provider-server-name=vault.hivekeep-system.svc",
            ],
        ),
        ("off", CredentialProvider::default(), vec![]),
    ];
    for (name, provider, want) in cases {
        let patched = patch_gate_egress_inject(&raw, &provider).unwrap();
        let text = String::from_utf8(patched.clone()).unwrap();
        for line in text.lines() {
            assert!(
                !line.trim().starts_with(EGRESS_INJECT_FLAGS_MARKER),
                "{name}: unreplaced marker {line:?}"
            );
        }
        for w in want {
            assert!(text.contains(w), "{name}: missing {w:?}");
        }
        if !provider.enabled() {
            assert!(
                !text.contains("- --credential-provider-"),
                "{name}: provider flags with injection off"
            );
        }
        assert_egress_manifest_parses(&patched);
    }
    // The marker is what the splice keys on; without it the install is broken.
    assert!(
        patch_gate_egress_inject(b"kind: ConfigMap\n", &CredentialProvider::default()).is_err()
    );
}

#[test]
fn emit_additional_egress_extproc_cluster_uses_sds() {
    let out = emit_additional_egress_extproc_cluster(
        "foo.hivekeep-system.svc.cluster.local",
        "50051",
        "foo.hivekeep-system.svc",
    );
    let clusters: Vec<Value> = serde_yaml::from_str(&out).expect("valid YAML");
    assert_eq!(clusters.len(), 1);
    assert_eq!(clusters[0]["name"], ADDITIONAL_EGRESS_EXTPROC_CLUSTER);
    for want in [
        "tls_certificate_sds_secret_configs",
        "path: /etc/envoy/sds-podidentity-cert.yaml",
        "combined_validation_context",
        "path: /etc/envoy/sds-servicedns-validation.yaml",
        "exact: foo.hivekeep-system.svc",
        "port_value: 50051",
        "address: foo.hivekeep-system.svc.cluster.local",
    ] {
        assert!(out.contains(want), "missing {want:?}");
    }
    assert!(!out.contains("tls_certificates:"));
    assert!(!out.contains("watched_directory"));
}

/// The http_filters of every HTTP connection manager in the bootstrap's
/// static listeners.
fn http_filter_chains(envoy_yaml: &str) -> Vec<Vec<Value>> {
    let bootstrap: Value = serde_yaml::from_str(envoy_yaml).expect("bootstrap parses");
    let mut chains = Vec::new();
    for l in bootstrap
        .pointer("/static_resources/listeners")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for fc in l
            .get("filter_chains")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            for f in fc
                .get("filters")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if f.get("name").and_then(Value::as_str)
                    == Some("envoy.filters.network.http_connection_manager")
                {
                    chains.push(
                        f.pointer("/typed_config/http_filters")
                            .and_then(Value::as_array)
                            .cloned()
                            .unwrap_or_default(),
                    );
                }
            }
        }
    }
    chains
}

fn ext_proc_cluster(filter: &Value) -> &str {
    if filter.get("name").and_then(Value::as_str) != Some("envoy.filters.http.ext_proc") {
        return "";
    }
    filter
        .pointer("/typed_config/grpc_service/envoy_grpc/cluster_name")
        .and_then(Value::as_str)
        .unwrap_or("")
}

#[test]
fn patch_gate_egress_manifest_splices_and_reparses() {
    let mut cfg = test_config();
    cfg.additional_egress_extproc_service = "hivekeep-system/foo:50051".into();
    let e = Env::local(cfg);
    let patched = e.patch_gate_egress_manifest().unwrap();
    let text = String::from_utf8(patched.clone()).unwrap();
    for line in text.lines() {
        assert!(
            !line.trim().starts_with("#HK_MITM_EXTPROC"),
            "unreplaced marker line {line:?}"
        );
    }
    let data = first_config_map_data(&patched);
    for key in [
        "envoy.yaml",
        "sds-servicedns-cert.yaml",
        "sds-podidentity-cert.yaml",
        "sds-servicedns-validation.yaml",
        "sds-actor-identity-validation.yaml",
    ] {
        let content = data
            .get(key)
            .unwrap_or_else(|| panic!("ConfigMap is missing {key}"));
        serde_yaml::from_str::<serde_yaml::Value>(content)
            .unwrap_or_else(|e| panic!("{key} is not YAML: {e}"));
    }
    let envoy = &data["envoy.yaml"];
    for want in [
        "path: /etc/envoy/sds-podidentity-cert.yaml",
        "path: /etc/envoy/sds-servicedns-validation.yaml",
        &format!("cluster_name: {ADDITIONAL_EGRESS_EXTPROC_CLUSTER}"),
    ] {
        assert!(envoy.contains(want), "envoy.yaml missing {want:?}");
    }
    for line in envoy.lines() {
        assert!(
            line.trim().starts_with('#') || !line.contains("watched_directory"),
            "watched_directory outside the SDS resource files: {line:?}"
        );
    }
    for key in [
        "sds-servicedns-cert.yaml",
        "sds-podidentity-cert.yaml",
        "sds-servicedns-validation.yaml",
        "sds-actor-identity-validation.yaml",
    ] {
        assert!(
            data[key].contains("watched_directory"),
            "{key} lacks watched_directory"
        );
    }
    // Below the policy ext_proc on both HTTP chains.
    let mut spliced = 0;
    for filters in http_filter_chains(envoy) {
        let policy_at = filters
            .iter()
            .position(|f| ext_proc_cluster(f) == "ext_proc_server");
        let Some(additional_at) = filters
            .iter()
            .position(|f| ext_proc_cluster(f) == ADDITIONAL_EGRESS_EXTPROC_CLUSTER)
        else {
            continue;
        };
        spliced += 1;
        assert!(
            policy_at.is_some_and(|p| p < additional_at),
            "additional ext_proc runs before the policy one"
        );
    }
    assert_eq!(spliced, 2);
}

#[test]
fn system_overlay_default_is_base() {
    let mut cfg = Config::default();
    cfg.router = ROUTER_ENVOY.into();
    assert_eq!(system_overlay(&cfg), "install/base");
}

/// Workloads carrying the cordon pinning, by pool, and every workload seen.
fn pinned_workloads(manifest: &[u8]) -> (BTreeMap<String, String>, Vec<String>) {
    let mut pinned = BTreeMap::new();
    let mut all = Vec::new();
    for d in docs(manifest) {
        if kind_of(&d) != "Deployment" && kind_of(&d) != "StatefulSet" {
            continue;
        }
        let name = name_of(&d).to_string();
        all.push(name.clone());
        let spec = d
            .pointer("/spec/template/spec")
            .cloned()
            .unwrap_or(Value::Null);
        let Some(pool) = spec
            .pointer("/nodeSelector/hivekeep.io~1workloadType")
            .and_then(Value::as_str)
        else {
            continue;
        };
        let tolerates = spec
            .get("tolerations")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|t| {
                t.get("key").and_then(Value::as_str) == Some("hivekeep.io/workloadType")
                    && t.get("value").and_then(Value::as_str) == Some(pool)
            });
        assert!(
            tolerates,
            "{name} selects pool {pool} but does not tolerate it"
        );
        assert!(
            spec.pointer("/affinity/podAntiAffinity").is_none(),
            "{name} has pod anti-affinity"
        );
        let spread = spec
            .get("topologySpreadConstraints")
            .and_then(Value::as_array)
            .is_some_and(|a| !a.is_empty());
        assert_eq!(
            spread,
            pool == "hivekeep-system-infra",
            "{name} in {pool}: topology spread = {spread}"
        );
        if pool == "hivekeep-system-infra" {
            assert!(
                d.pointer("/spec/template/metadata/labels/app").is_some(),
                "{name} has no app label"
            );
        }
        pinned.insert(name, pool.to_string());
    }
    (pinned, all)
}

#[test]
fn render_cordon_system_infra_components() {
    if kustomize_missing() {
        return;
    }
    type PathFn = fn(&Env) -> PathBuf;
    type Case<'a> = (&'a str, fn(&mut Config), PathFn, Vec<&'a str>);
    let cases: Vec<Case> = vec![
        (
            "base bundle",
            |c| c.router = ROUTER_ENVOY.into(),
            |e| e.system_overlay_path(),
            vec![
                "hivekeep-api",
                "hivekeep-idp-server",
                "hivekeep-controller",
                "gate-router",
            ],
        ),
        (
            "kind bundle",
            |c| c.kind = true,
            |e| e.system_overlay_path(),
            vec![
                "hivekeep-api",
                "hivekeep-idp-server",
                "hivekeep-controller",
                "gate-router",
            ],
        ),
        (
            "api server file",
            |_| {},
            |e| e.cfg.manifest(&["hivekeep-api.yaml"]),
            vec!["hivekeep-api"],
        ),
        (
            "kind api server overlay",
            |c| c.kind = true,
            |e| e.api_server_manifest_path(),
            vec!["hivekeep-api"],
        ),
        (
            "idp server file",
            |_| {},
            |e| e.idp_server_manifest_path(),
            vec!["hivekeep-idp-server"],
        ),
        (
            "podcert file",
            |_| {},
            |e| e.cfg.manifest(&["pod-certificate-controller.yaml"]),
            vec!["podcertificate-controller"],
        ),
        (
            "podcert size10 overlay",
            |c| c.cluster_size = CLUSTER_SIZE_SIZE10.into(),
            |e| e.cfg.manifest(&["podcert-size10"]),
            vec!["podcertificate-controller"],
        ),
        (
            "postgres file",
            |_| {},
            |e| e.postgres_manifest_path(),
            vec!["postgres"],
        ),
        (
            "egress file",
            |_| {},
            |e| e.gate_egress_manifest_path(),
            vec!["gate-egress"],
        ),
        (
            "credential provider file",
            |_| {},
            |e| e.k8s_credential_provider_path(K8S_CREDENTIAL_PROVIDER_MANIFEST),
            vec![K8S_CREDENTIAL_PROVIDER_DEPLOYMENT],
        ),
    ];
    for (name, setup, path, want) in cases {
        let mut cfg = test_config();
        setup(&mut cfg);
        cfg.cordon_system_infra_components = true;
        let e = Env::local(cfg);
        let rendered = e
            .render(&path(&e))
            .unwrap_or_else(|err| panic!("{name}: render: {err}"));
        let (pinned, all) = pinned_workloads(&rendered);
        for w in &want {
            assert!(
                all.iter().any(|a| a == w),
                "{name}: no workload {w} (found {all:?})"
            );
            let pool = if *w == "postgres" {
                "hivekeep-postgres"
            } else {
                "hivekeep-system-infra"
            };
            assert_eq!(
                pinned.get(*w).map(String::as_str),
                Some(pool),
                "{name}: {w}"
            );
        }
        for p in pinned.keys() {
            assert!(
                want.contains(&p.as_str()),
                "{name}: {p} is pinned but is not system infra"
            );
        }
    }
}

#[test]
fn render_bytes_cordon_system_infra_components() {
    if kustomize_missing() {
        return;
    }
    let mut cfg = test_config();
    cfg.cordon_system_infra_components = true;
    cfg.additional_egress_extproc_service = "hivekeep-system/foo:50051".into();
    let e = Env::local(cfg);
    let patched = e.patch_gate_egress_manifest().unwrap();
    let rendered = e.render_bytes(&patched).unwrap();
    let (pinned, _) = pinned_workloads(&rendered);
    assert_eq!(
        pinned.get(GATE_EGRESS).map(String::as_str),
        Some("hivekeep-system-infra")
    );
    assert!(String::from_utf8_lossy(&rendered).contains(ADDITIONAL_EGRESS_EXTPROC_CLUSTER));
}

#[test]
fn render_without_cordon_leaves_manifests_alone() {
    let e = Env::local(test_config());
    let path = e.cfg.manifest(&["hivekeep-api.yaml"]);
    let rendered = e.render(&path).unwrap();
    assert!(pinned_workloads(&rendered).0.is_empty());
    assert_eq!(
        rendered,
        std::fs::read(&path).unwrap(),
        "a plain render is not the file's bytes"
    );
}

fn data_plane_workloads(
    manifest: &[u8],
) -> (BTreeMap<String, DaemonSet>, BTreeMap<String, Deployment>) {
    let mut ds = BTreeMap::new();
    let mut deps = BTreeMap::new();
    for d in docs(manifest) {
        match kind_of(&d) {
            "DaemonSet" => {
                ds.insert(name_of(&d).to_string(), serde_json::from_value(d).unwrap());
            }
            "Deployment" => {
                deps.insert(name_of(&d).to_string(), serde_json::from_value(d).unwrap());
            }
            _ => {}
        }
    }
    (ds, deps)
}

#[test]
fn render_fake_data_plane() {
    if kustomize_missing() {
        return;
    }
    type PathFn = fn(&Env) -> PathBuf;
    struct Case {
        name: &'static str,
        setup: fn(&mut Config),
        path: PathFn,
        keeper: bool,
        controller: bool,
        storage: &'static str,
    }
    let cases = [
        Case {
            name: "base bundle",
            setup: |c| c.router = ROUTER_ENVOY.into(),
            path: |e| e.system_overlay_path(),
            keeper: true,
            controller: true,
            storage: "gcs",
        },
        Case {
            name: "kind bundle",
            setup: |c| c.kind = true,
            path: |e| e.system_overlay_path(),
            keeper: true,
            controller: true,
            storage: "s3",
        },
        Case {
            name: "keeper file",
            setup: |_| {},
            path: |e| e.cfg.manifest(&["keeper.yaml"]),
            keeper: true,
            controller: false,
            storage: "gcs",
        },
        Case {
            name: "kind keeper overlay",
            setup: |c| c.kind = true,
            path: |e| e.keeper_manifest_path(),
            keeper: true,
            controller: false,
            storage: "s3",
        },
        Case {
            name: "controller file",
            setup: |_| {},
            path: |e| e.cfg.manifest(&["hivekeep-controller.yaml"]),
            keeper: false,
            controller: true,
            storage: "",
        },
        Case {
            name: "controller file under cordon",
            setup: |c| c.cordon_system_infra_components = true,
            path: |e| e.cfg.manifest(&["hivekeep-controller.yaml"]),
            keeper: false,
            controller: true,
            storage: "",
        },
    ];
    for c in cases {
        let mut cfg = test_config();
        (c.setup)(&mut cfg);
        cfg.fake_data_plane = true;
        let cordon = cfg.cordon_system_infra_components;
        let e = Env::local(cfg);
        let rendered = e
            .render(&(c.path)(&e))
            .unwrap_or_else(|err| panic!("{}: {err}", c.name));
        let (daemon_sets, deployments) = data_plane_workloads(&rendered);
        let ws = deployments
            .get(FAKE_WORKERSYNC_DEPLOYMENT)
            .unwrap_or_else(|| panic!("{}: no fake-workersync", c.name));
        let strategy = ws
            .spec
            .as_ref()
            .and_then(|s| s.strategy.as_ref())
            .and_then(|s| s.type_.clone());
        assert_eq!(strategy.as_deref(), Some("Recreate"), "{}", c.name);

        assert_eq!(
            daemon_sets.contains_key("keeper"),
            c.keeper,
            "{}: keeper present",
            c.name
        );
        if let Some(ds) = daemon_sets.get("keeper") {
            let tmpl = ds.spec.as_ref().unwrap().template.clone();
            let meta = tmpl.metadata.unwrap_or_default();
            assert_eq!(
                meta.labels
                    .unwrap_or_default()
                    .get("hivekeep.io/fake-keeper")
                    .map(String::as_str),
                Some("true"),
                "{}",
                c.name
            );
            let pod = tmpl.spec.unwrap();
            assert!(
                pod.init_containers.as_ref().is_none_or(|i| i.is_empty()),
                "{}: keeps init containers",
                c.name
            );
            for v in pod.volumes.iter().flatten() {
                assert!(
                    v.host_path.is_none(),
                    "{}: keeps hostPath volume {}",
                    c.name,
                    v.name
                );
            }
            assert!(
                !meta
                    .annotations
                    .unwrap_or_default()
                    .contains_key("prometheus.io/scrape"),
                "{}: scrape annotation",
                c.name
            );
            assert_eq!(
                pod.service_account_name.as_deref(),
                Some("hivekeep-keeper"),
                "{}",
                c.name
            );
            let ctr = &pod.containers[0];
            assert_eq!(
                ctr.image.as_deref(),
                Some(crate::images::manifest_reference("hivekeep-fake-keeper").as_str()),
                "{}",
                c.name
            );
            assert_eq!(
                ctr.args.clone().unwrap_or_default(),
                vec![
                    "--api-address=hivekeep-api.hivekeep-system.svc:443",
                    "--api-server-name=hivekeep-api.hivekeep-system.svc"
                ],
                "{}: fake-keeper args",
                c.name
            );
            let env_from = ctr.env_from.clone().unwrap_or_default();
            let tuning = env_from
                .iter()
                .filter_map(|s| s.config_map_ref.as_ref())
                .find(|r| r.name == "fake-keeper-tuning");
            assert!(
                tuning.is_some_and(|t| t.optional == Some(true)),
                "{}: tuning envFrom {env_from:?}",
                c.name
            );
            assert!(
                env_from
                    .iter()
                    .filter_map(|s| s.config_map_ref.as_ref())
                    .any(|r| r.name == OTEL_CONFIG_MAP),
                "{}: lost otel envFrom",
                c.name
            );
            let ports = ctr.ports.clone().unwrap_or_default();
            for p in &ports {
                assert!(
                    p.host_port.is_none_or(|h| h == 0),
                    "{}: port {:?} keeps hostPort",
                    c.name,
                    p.name
                );
            }
            for probe in [&ctr.readiness_probe, &ctr.liveness_probe]
                .into_iter()
                .flatten()
            {
                if let Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(
                    port,
                )) = probe.http_get.as_ref().map(|h| h.port.clone())
                {
                    assert!(
                        ports
                            .iter()
                            .any(|p| p.name.as_deref() == Some(port.as_str())),
                        "{}: probe targets undeclared {port}",
                        c.name
                    );
                }
            }
            let env: BTreeMap<String, String> = ctr
                .env
                .iter()
                .flatten()
                .map(|v| (v.name.clone(), v.value.clone().unwrap_or_default()))
                .collect();
            assert_eq!(
                env.get("HK_STORAGE_BACKEND").map(String::as_str),
                Some(c.storage),
                "{}",
                c.name
            );
            if c.storage == "s3" {
                assert!(
                    env.get("AWS_ENDPOINT_URL").is_some_and(|v| !v.is_empty()),
                    "{}: no AWS_ENDPOINT_URL",
                    c.name
                );
            }
        }
        assert_eq!(
            deployments.contains_key("hivekeep-controller"),
            c.controller,
            "{}: controller present",
            c.name
        );
        if let Some(d) = deployments.get("hivekeep-controller") {
            assert_eq!(
                d.spec.as_ref().and_then(|s| s.replicas),
                Some(0),
                "{}",
                c.name
            );
        }
        if cordon {
            let (pinned, _) = pinned_workloads(&rendered);
            for n in ["hivekeep-controller", FAKE_WORKERSYNC_DEPLOYMENT] {
                assert_eq!(
                    pinned.get(n).map(String::as_str),
                    Some("hivekeep-system-infra"),
                    "{}: {n}",
                    c.name
                );
            }
        }
    }
}

fn keeper_args(manifest: &[u8]) -> Option<Vec<String>> {
    let (ds, _) = data_plane_workloads(manifest);
    ds.get("keeper").map(|d| {
        d.spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .args
            .clone()
            .unwrap_or_default()
    })
}

#[test]
fn render_e2e_tuning() {
    if kustomize_missing() {
        return;
    }
    let gke = vec![
        "--grpc-server-cred-bundle=/run/podidentity.podcert.hivekeep.io/credential-bundle.pem",
        "--snapshot-upload-delay=2s",
    ];
    let kind = vec![
        "--localhost-registry-replacement=kind-registry:5000",
        "--log-level=debug",
        "--snapshot-upload-delay=2s",
    ];
    type PathFn = fn(&Env) -> PathBuf;
    type Case<'a> = (&'a str, fn(&mut Config), PathFn, Option<Vec<&'a str>>);
    let cases: Vec<Case> = vec![
        (
            "base bundle",
            |c| c.router = ROUTER_ENVOY.into(),
            |e| e.system_overlay_path(),
            Some(gke.clone()),
        ),
        (
            "kind bundle",
            |c| c.kind = true,
            |e| e.system_overlay_path(),
            Some(kind.clone()),
        ),
        (
            "keeper file",
            |_| {},
            |e| e.cfg.manifest(&["keeper.yaml"]),
            Some(gke.clone()),
        ),
        (
            "kind keeper overlay",
            |c| c.kind = true,
            |e| e.keeper_manifest_path(),
            Some(kind.clone()),
        ),
        (
            "api server file without keeper",
            |_| {},
            |e| e.cfg.manifest(&["hivekeep-api.yaml"]),
            None,
        ),
    ];
    for (name, setup, path, want) in cases {
        let mut cfg = test_config();
        setup(&mut cfg);
        cfg.e2e_tuning = true;
        let e = Env::local(cfg);
        let rendered = e.render(&path(&e)).unwrap();
        let args = keeper_args(&rendered);
        assert_eq!(args.is_some(), want.is_some(), "{name}: keeper present");
        if let (Some(args), Some(want)) = (args, want) {
            for w in want {
                assert!(
                    args.iter().any(|a| a == w),
                    "{name}: keeper args {args:?} missing {w}"
                );
            }
        }
    }

    // kind without e2e tuning keeps the default upload delay.
    let mut cfg = test_config();
    cfg.kind = true;
    let e = Env::local(cfg);
    let args = keeper_args(&e.render(&e.keeper_manifest_path()).unwrap()).unwrap();
    assert!(args.iter().any(|a| a == "--log-level=debug"), "{args:?}");
    assert!(
        !args
            .iter()
            .any(|a| a.starts_with("--snapshot-upload-delay")),
        "{args:?}"
    );

    // Composes with the fake data plane, which replaces every arg with the
    // hivekeep-api location.
    let mut cfg = test_config();
    cfg.e2e_tuning = true;
    cfg.fake_data_plane = true;
    let e = Env::local(cfg);
    let args = keeper_args(&e.render(&e.keeper_manifest_path()).unwrap()).unwrap();
    assert_eq!(
        args,
        [
            "--api-address=hivekeep-api.hivekeep-system.svc:443",
            "--api-server-name=hivekeep-api.hivekeep-system.svc"
        ],
        "fake-keeper args"
    );
}

fn router_args(manifest: &[u8]) -> Vec<String> {
    for d in docs(manifest) {
        if kind_of(&d) != "Deployment" || name_of(&d) != GATE_ROUTER {
            continue;
        }
        let dep: Deployment = serde_json::from_value(d).unwrap();
        for c in dep.spec.unwrap().template.spec.unwrap().containers {
            if c.name == GATE_ROUTER {
                return c.args.unwrap_or_default();
            }
        }
    }
    panic!("rendered manifest has no gate-router container");
}

#[tokio::test]
async fn render_router_allowed_spiffe_ids() {
    if kustomize_missing() {
        return;
    }
    const A: &str = "spiffe://cluster.local/ns/a/sa/a";
    const B: &str = "spiffe://cluster.local/ns/b/sa/b";
    for (name, kind, router_file) in [
        ("base bundle", false, false),
        ("kind bundle", true, false),
        ("router file", false, true),
    ] {
        for ids in [vec![A.to_string(), B.to_string()], vec![]] {
            let mut cfg = test_config();
            cfg.kind = kind;
            cfg.router = ROUTER_ENVOY.into();
            cfg.ingress_allowed_spiffe_ids = ids.clone();
            let e = prebuilt_env(cfg);
            let rendered = if router_file {
                e.render_gate_router_manifest().await.unwrap()
            } else {
                e.render_system_manifests().await.unwrap()
            };
            let got: Vec<String> = router_args(&rendered)
                .into_iter()
                .filter(|a| a.starts_with("--ingress-allowed-spiffe-ids"))
                .collect();
            let want: Vec<String> = if ids.is_empty() {
                vec![]
            } else {
                vec![format!("--ingress-allowed-spiffe-ids={A},{B}")]
            };
            assert_eq!(got, want, "{name} ids={ids:?}");
        }
    }
}

#[test]
fn render_data_plane_without_fake_is_render() {
    if kustomize_missing() {
        return;
    }
    let mut cfg = test_config();
    cfg.kind = true;
    let e = Env::local(cfg);
    let a = e.render(&e.system_overlay_path()).unwrap();
    let b = e.render(&e.system_overlay_path()).unwrap();
    assert_eq!(a, b);
    assert!(!data_plane_workloads(&a)
        .1
        .contains_key(FAKE_WORKERSYNC_DEPLOYMENT));
}

#[tokio::test]
async fn egress_mitm_ca_pool_secret() {
    let (e, _) = fake_env(test_config(), vec![]);
    let secret = super::create::SECRET_EGRESS_MITM_CA_POOL;
    assert!(
        std::fs::read_to_string(e.gate_egress_manifest_path())
            .unwrap()
            .contains(secret),
        "the egress gateway does not mount {secret}"
    );
    e.ensure_egress_mitm_ca_pool_secret().await.unwrap();
    assert!(
        e.kube()
            .unwrap()
            .secret_exists(NAMESPACE_SYSTEM, secret)
            .await
            .unwrap(),
        "no {secret} generated"
    );
}

fn otel_config(endpoint: &str) -> Object {
    json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"namespace": NAMESPACE_SYSTEM, "name": OTEL_CONFIG_MAP},
        "data": {OTEL_ENDPOINT_KEY: endpoint},
    })
}

fn restarted(fake: &FakeCluster, gvk: &Gvk, name: &str) -> bool {
    fake.object(gvk, NAMESPACE_SYSTEM, name)
        .and_then(|o| {
            o.pointer("/spec/template/metadata/annotations/kubectl.kubernetes.io~1restartedAt")
                .cloned()
        })
        .is_some()
}

#[tokio::test]
async fn apply_otel_endpoint_override() {
    const ENDPOINT: &str = "http://collector.benchmark.svc:4317";
    let dep = Gvk::of::<Deployment>();
    let ds = Gvk::of::<DaemonSet>();
    let api = || stub(&dep, NAMESPACE_SYSTEM, "hivekeep-api");

    // No endpoint configured is a no-op.
    let (e, fake) = fake_env(Config::default(), vec![otel_config("http://default:4317")]);
    e.apply_otel_endpoint_override().await.unwrap();
    let cm = fake
        .object(&Gvk::of::<ConfigMap>(), NAMESPACE_SYSTEM, OTEL_CONFIG_MAP)
        .unwrap();
    assert_eq!(cm["data"][OTEL_ENDPOINT_KEY], "http://default:4317");

    // Already correct restarts nothing, so no restart races the rollout wait.
    let mut cfg = Config::default();
    cfg.otlp_endpoint = ENDPOINT.into();
    let (e, fake) = fake_env(cfg.clone(), vec![otel_config(ENDPOINT), api()]);
    e.apply_otel_endpoint_override().await.unwrap();
    assert!(!restarted(&fake, &dep, "hivekeep-api"));

    // Patches and restarts the consumers; absent ones are no error.
    let (e, fake) = fake_env(
        cfg,
        vec![
            otel_config("http://default:4317"),
            api(),
            stub(&ds, NAMESPACE_SYSTEM, "keeper"),
        ],
    );
    e.apply_otel_endpoint_override().await.unwrap();
    let cm = fake
        .object(&Gvk::of::<ConfigMap>(), NAMESPACE_SYSTEM, OTEL_CONFIG_MAP)
        .unwrap();
    assert_eq!(cm["data"][OTEL_ENDPOINT_KEY], ENDPOINT);
    assert!(restarted(&fake, &dep, "hivekeep-api"));
    assert!(restarted(&fake, &ds, "keeper"));
}

#[tokio::test]
async fn render_gate_egress_manifest_prebuilt() {
    if kustomize_missing() {
        return;
    }
    let digest = Arc::new(FixedDigest::default());
    let mut cfg = test_config();
    cfg.router = ROUTER_ENVOY.into();
    cfg.images = prebuilt_source();
    cfg.ingress_allowed_spiffe_ids = vec!["spiffe://cluster.local/ns/a/sa/a".into()];
    let e = Env::local(cfg).with_digester(digest.clone());
    let want = format!("example.com/hivekeep/hivekeep-envoy-dataplane:v1.2.3@{DIGEST}");
    for case in ["egress", "router", "system"] {
        let out = match case {
            "egress" => {
                e.render_gate_egress_manifest(&CredentialProvider::default())
                    .await
            }
            "router" => e.render_gate_router_manifest().await,
            _ => e.render_system_manifests().await,
        }
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(
            text.contains(&format!("image: {want}")),
            "{case}: does not install {want}"
        );
        for leftover in [
            ENVOY_DATAPLANE_PLACEHOLDER,
            crate::images::REGISTRY_PLACEHOLDER,
        ] {
            assert!(
                !text.contains(leftover),
                "{case}: still contains {leftover}"
            );
        }
    }
    assert!(
        digest
            .looked
            .lock()
            .iter()
            .any(|r| r == "example.com/hivekeep/hivekeep-envoy-dataplane:v1.2.3"),
        "lookups = {:?}",
        digest.looked.lock()
    );
}

#[tokio::test]
async fn dockerfile_image_prebuilt_not_published() {
    let mut cfg = Config::default();
    cfg.images = prebuilt_source();
    let e = Env::local(cfg).with_digester(Arc::new(FixedDigest {
        fail: true,
        ..Default::default()
    }));
    let err = e
        .dockerfile_image(crate::images::ENVOY_DATAPLANE)
        .await
        .unwrap_err()
        .to_string();
    for want in [
        "hivekeep-envoy-dataplane is built from a Dockerfile, and the image repository has no readable copy",
        "resolving example.com/hivekeep/hivekeep-envoy-dataplane:v1.2.3 to a digest: MANIFEST_UNKNOWN",
    ] {
        assert!(err.contains(want), "error is missing {want:?}:\n{err}");
    }
}

#[test]
fn patch_envoy_dataplane_image_substitutes() {
    let raw = b"containers:\n- name: envoy\n  image: ${ENVOY_DATAPLANE_IMAGE}\n";
    let got = patch_envoy_dataplane_image(raw, "gcr.io/example/envoy-dataplane@sha256:abc123");
    assert_eq!(
        String::from_utf8(got).unwrap(),
        "containers:\n- name: envoy\n  image: gcr.io/example/envoy-dataplane@sha256:abc123\n"
    );
}

fn envoy_args(manifest: &[u8]) -> Vec<String> {
    for d in docs(manifest) {
        if kind_of(&d) != "Deployment" || name_of(&d) != GATE_EGRESS {
            continue;
        }
        let dep: Deployment = serde_json::from_value(d).unwrap();
        for c in dep.spec.unwrap().template.spec.unwrap().containers {
            if c.name == "envoy" {
                return c.args.unwrap_or_default();
            }
        }
        panic!("gate-egress has no envoy container");
    }
    panic!("no gate-egress Deployment");
}

#[tokio::test]
async fn render_gate_egress_manifest_envoy_concurrency() {
    if kustomize_missing() {
        return;
    }
    let mut cfg = test_config();
    cfg.router = ROUTER_ENVOY.into();
    let e = prebuilt_env(cfg.clone());
    let args = envoy_args(
        &e.render_gate_egress_manifest(&CredentialProvider::default())
            .await
            .unwrap(),
    );
    assert!(!args.iter().any(|a| a == "--concurrency"), "{args:?}");

    cfg.envoy_concurrency = 4;
    let e = prebuilt_env(cfg);
    let args = envoy_args(
        &e.render_gate_egress_manifest(&CredentialProvider::default())
            .await
            .unwrap(),
    );
    assert_eq!(
        args,
        [
            "-c",
            "/etc/envoy/envoy.yaml",
            "--service-node",
            "gate-egress",
            "--service-cluster",
            "gate-egress",
            "--concurrency",
            "4"
        ]
    );
}

#[tokio::test]
async fn wait_fake_workersync_without_the_flag_is_a_noop() {
    let (e, _) = fake_env(test_config(), vec![]);
    e.wait_fake_workersync().await.unwrap();
}

#[tokio::test]
async fn stop_fake_workersync() {
    for running in [true, false] {
        let mut objs = vec![];
        if running {
            objs.push(stub(
                &Gvk::of::<Deployment>(),
                NAMESPACE_SYSTEM,
                FAKE_WORKERSYNC_DEPLOYMENT,
            ));
        }
        let mut cfg = test_config();
        cfg.rollout_timeout = Duration::from_secs(1);
        let (e, _) = fake_env(cfg, objs);
        e.stop_fake_workersync().await.unwrap();
        assert!(!e
            .kube()
            .unwrap()
            .deployment_exists(NAMESPACE_SYSTEM, FAKE_WORKERSYNC_DEPLOYMENT)
            .await
            .unwrap());
    }
}

#[tokio::test]
async fn require_no_worker_pods() {
    let pod_gvk = Gvk::of::<Pod>();
    let worker_pod = |name: &str, terminating: bool| {
        let mut p = stub(&pod_gvk, "benchmark-workloads", name);
        p["metadata"]["labels"] = json!({super::deploy::WORKER_POOL_LABEL: "benchmark-worker"});
        p["status"] = json!({"phase": "Running"});
        if terminating {
            p["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
            p["metadata"]["finalizers"] = json!(["test"]);
        }
        p
    };
    let mut scaled_to_zero = stub(&Gvk::of::<Deployment>(), "benchmark-workloads", "idle");
    scaled_to_zero["metadata"]["labels"] = json!({super::deploy::WORKER_POOL_LABEL: "idle"});
    let mut evicted = worker_pod("w-evicted", false);
    evicted["status"] = json!({"phase": "Failed", "reason": "Evicted"});
    let mut other = stub(&pod_gvk, NAMESPACE_SYSTEM, "hivekeep-api-0");
    other["metadata"]["labels"] = json!({"app": "hivekeep-api"});
    let cases = vec![
        (
            "real install with worker pods",
            false,
            vec![worker_pod("w-0", false), other.clone()],
            false,
        ),
        (
            "fake install without worker pods",
            true,
            vec![other.clone()],
            false,
        ),
        (
            "fake install beside a pool scaled to zero",
            true,
            vec![scaled_to_zero, other.clone()],
            false,
        ),
        (
            "fake install beside evicted worker pods",
            true,
            vec![evicted, other.clone()],
            false,
        ),
        (
            "fake install with worker pods",
            true,
            vec![
                worker_pod("w-0", false),
                worker_pod("w-1", false),
                other.clone(),
            ],
            true,
        ),
        (
            "fake install with terminating worker pods",
            true,
            vec![worker_pod("w-0", true), other],
            true,
        ),
    ];
    for (name, fake, objs, want_err) in cases {
        let mut cfg = test_config();
        cfg.fake_data_plane = fake;
        let (e, _) = fake_env(cfg, objs);
        let res = e.require_no_worker_pods().await;
        assert_eq!(res.is_err(), want_err, "{name}: {res:?}");
        if let Err(err) = res {
            assert_eq!(
                err.to_string()
                    .matches("benchmark-workloads/benchmark-worker")
                    .count(),
                1,
                "{name}: {err}"
            );
        }
    }
}

#[test]
fn fake_workersync_path_is_the_components_manifest() {
    let e = Env::local(test_config());
    let objs = kube::load_path(&e.fake_workersync_path()).unwrap();
    let kinds: Vec<String> = objs
        .iter()
        .map(|o| format!("{}/{}", o.kind(), o.name()))
        .collect();
    for want in [
        format!("Deployment/{FAKE_WORKERSYNC_DEPLOYMENT}"),
        "ClusterRole/fake-workersync".to_string(),
        "ClusterRoleBinding/fake-workersync".to_string(),
    ] {
        assert!(kinds.contains(&want), "no {want} in {kinds:?}");
    }
    let component = std::fs::read_to_string(
        e.cfg
            .manifests_path(&[FAKE_DATA_PLANE_COMPONENT, "kustomization.yaml"]),
    )
    .unwrap();
    assert!(component.contains("- fake-workersync.yaml"));
}

#[tokio::test]
async fn require_installed_data_plane() {
    for (name, fake, installed, want_err) in [
        ("real on real", false, false, false),
        ("fake on fake", true, true, false),
        ("fake on real", true, false, true),
        ("real on fake", false, true, true),
    ] {
        let objs = if installed {
            vec![stub(
                &Gvk::of::<Deployment>(),
                NAMESPACE_SYSTEM,
                FAKE_WORKERSYNC_DEPLOYMENT,
            )]
        } else {
            vec![]
        };
        let mut cfg = test_config();
        cfg.fake_data_plane = fake;
        let (e, _) = fake_env(cfg, objs);
        assert_eq!(
            e.require_installed_data_plane("deploy keeper")
                .await
                .is_err(),
            want_err,
            "{name}"
        );
    }
}

#[tokio::test]
async fn remove_fake_workersync() {
    let rbac = |kind: &str| {
        stub(
            &Gvk::new("rbac.authorization.k8s.io", "v1", kind),
            "",
            FAKE_WORKERSYNC_DEPLOYMENT,
        )
    };
    for (name, fake, running, left, want_gone) in [
        ("real install over a fake one", false, true, true, true),
        (
            "real install over RBAC a failed run left",
            false,
            false,
            true,
            true,
        ),
        ("real install with nothing left", false, false, false, false),
        ("fake install keeps its own", true, true, true, false),
    ] {
        let mut objs = vec![];
        if running {
            objs.push(stub(
                &Gvk::of::<Deployment>(),
                NAMESPACE_SYSTEM,
                FAKE_WORKERSYNC_DEPLOYMENT,
            ));
        }
        if left {
            objs.push(rbac("ClusterRole"));
            objs.push(rbac("ClusterRoleBinding"));
        }
        let mut cfg = test_config();
        cfg.fake_data_plane = fake;
        cfg.rollout_timeout = Duration::from_secs(1);
        let (e, f) = fake_env(cfg, objs);
        e.remove_fake_workersync().await.unwrap();
        let exists = e
            .kube()
            .unwrap()
            .deployment_exists(NAMESPACE_SYSTEM, FAKE_WORKERSYNC_DEPLOYMENT)
            .await
            .unwrap();
        assert_eq!(exists, running && !want_gone, "{name}");
        if left {
            for kind in ["ClusterRole", "ClusterRoleBinding"] {
                let gone = f
                    .object(
                        &Gvk::new("rbac.authorization.k8s.io", "v1", kind),
                        "",
                        FAKE_WORKERSYNC_DEPLOYMENT,
                    )
                    .is_none();
                assert_eq!(gone, want_gone, "{name}: {kind}");
            }
        }
    }
}

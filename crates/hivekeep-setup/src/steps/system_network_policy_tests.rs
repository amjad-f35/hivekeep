//! The system namespace's NetworkPolicies, checked against every workload an
//! install of each configuration puts there: a default deny, every container
//! port either admitted or deliberately left out, named ports that resolve,
//! and least privilege on the sensitive ports.

use super::credprovider::K8S_CREDENTIAL_PROVIDER_MANIFEST;
use super::env::NAMESPACE_SYSTEM;
use super::overlay_tests::kustomize_missing;
use super::yamldoc::decode_all;
use super::Env;
use crate::config::{test_config, Config};
use crate::kube::{Object, ObjectExt};
use k8s_openapi::api::core::v1::{ContainerPort, PodTemplateSpec};
use k8s_openapi::api::networking::v1::{
    NetworkPolicy, NetworkPolicyIngressRule, NetworkPolicyPort,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use std::collections::{BTreeMap, BTreeSet};

type Labels = BTreeMap<String, String>;

/// The label the WorkerPool controller puts on worker pods.
const WORKER_POOL_LABEL_FOR_TEST: &str = "hivekeep.io/worker-pool";

/// Container ports no NetworkPolicy may admit, by the pod's app label and the
/// port's name. Every other port must be admitted by some policy, so a new
/// port fails the coverage test until someone decides which list it is on.
fn unadmitted_ports() -> BTreeMap<&'static str, Vec<&'static str>> {
    BTreeMap::from([
        // The status page exposes cross-tenant request history; reach it with
        // a port-forward.
        ("gate-router", vec!["status", "extproc"]),
        // ext_proc is served to the envoy container on loopback.
        ("gate-egress", vec!["extproc"]),
        // The provider ships its own policy admitting only the egress
        // gateway's gRPC; its probes come from the node.
        ("k8s-credential-provider", vec!["metrics", "health"]),
        // kind only: the rustfs web console.
        ("rustfs", vec!["console"]),
    ])
}

#[derive(Clone, Debug)]
struct SystemPod {
    workload: String,
    labels: Labels,
    ports: Vec<ContainerPort>,
}

struct SystemInstall {
    pods: Vec<SystemPod>,
    policies: Vec<NetworkPolicy>,
}

fn install_configs() -> Vec<Config> {
    let mut out = Vec::new();
    for kind in [false, true] {
        for fake in [false, true] {
            let mut c = test_config();
            c.kind = kind;
            c.fake_data_plane = fake;
            out.push(c);
        }
    }
    out
}

fn install_name(cfg: &Config) -> String {
    let env = if cfg.kind { "kind" } else { "gke" };
    let mut name = format!("{env}-{}", cfg.router);
    if cfg.fake_data_plane {
        name.push_str("-fake");
    }
    name
}

fn load_system_install(cfg: &Config) -> SystemInstall {
    let e = Env::local(cfg.clone());
    let mut objs: Vec<Object> = Vec::new();
    let mut add = |rendered: Vec<u8>| objs.extend(decode_all(&rendered).unwrap());
    add(e.render(&e.system_overlay_path()).unwrap());
    add(e.render(&e.postgres_manifest_path()).unwrap());
    objs.extend(decode_all(&std::fs::read(e.gate_egress_manifest_path()).unwrap()).unwrap());
    objs.extend(
        decode_all(
            &std::fs::read(e.k8s_credential_provider_path(K8S_CREDENTIAL_PROVIDER_MANIFEST))
                .unwrap(),
        )
        .unwrap(),
    );

    let mut install = SystemInstall {
        pods: vec![],
        policies: vec![],
    };
    for obj in objs {
        if obj.obj_namespace() != NAMESPACE_SYSTEM {
            continue;
        }
        match obj.kind() {
            "NetworkPolicy" => install
                .policies
                .push(serde_json::from_value(obj.clone()).unwrap()),
            "Deployment" | "DaemonSet" | "StatefulSet" | "Job" => {
                let raw = obj
                    .pointer("/spec/template")
                    .cloned()
                    .unwrap_or_else(|| panic!("{} has no pod template", obj.name()));
                let tmpl: PodTemplateSpec = serde_json::from_value(raw).unwrap();
                let mut labels: Labels = tmpl.metadata.and_then(|m| m.labels).unwrap_or_default();
                if obj.kind() == "Job" {
                    // The Job controller adds this label to its pods.
                    labels.insert("batch.kubernetes.io/job-name".into(), obj.name().into());
                }
                let spec = tmpl.spec.unwrap_or_default();
                let ports = spec
                    .init_containers
                    .unwrap_or_default()
                    .into_iter()
                    .chain(spec.containers)
                    .flat_map(|c| c.ports.unwrap_or_default())
                    .collect();
                install.pods.push(SystemPod {
                    workload: format!("{}/{}", obj.kind(), obj.name()),
                    labels,
                    ports,
                });
            }
            _ => {}
        }
    }
    assert!(
        !install.pods.is_empty(),
        "found no system workloads; the manifests changed shape"
    );
    install
}

/// Kubernetes label selector semantics.
fn selector_matches(sel: &LabelSelector, labels: &Labels) -> bool {
    for (k, v) in sel.match_labels.iter().flatten() {
        if labels.get(k) != Some(v) {
            return false;
        }
    }
    for req in sel.match_expressions.iter().flatten() {
        let values = req.values.clone().unwrap_or_default();
        let have = labels.get(&req.key);
        let ok = match req.operator.as_str() {
            "In" => have.is_some_and(|h| values.contains(h)),
            "NotIn" => !have.is_some_and(|h| values.contains(h)),
            "Exists" => have.is_some(),
            "DoesNotExist" => have.is_none(),
            other => panic!("invalid label selector operator {other:?}"),
        };
        if !ok {
            return false;
        }
    }
    true
}

fn selector_empty(sel: &LabelSelector) -> bool {
    sel.match_labels.as_ref().is_none_or(|m| m.is_empty())
        && sel.match_expressions.as_ref().is_none_or(|m| m.is_empty())
}

fn labels(pairs: &[(&str, &str)]) -> Labels {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

impl SystemInstall {
    /// Policies that select the pod for ingress.
    fn policies_for(&self, pod: &SystemPod) -> Vec<&NetworkPolicy> {
        self.policies
            .iter()
            .filter(|np| {
                let spec = np.spec.as_ref().unwrap();
                spec.policy_types.iter().flatten().any(|t| t == "Ingress")
                    && selector_matches(&spec.pod_selector.clone().unwrap_or_default(), &pod.labels)
            })
            .collect()
    }

    /// Ingress rules, across every policy selecting the pod, admitting the
    /// container port.
    fn admitting_rules(
        &self,
        pod: &SystemPod,
        cp: &ContainerPort,
    ) -> Vec<NetworkPolicyIngressRule> {
        let mut out = Vec::new();
        for np in self.policies_for(pod) {
            for rule in np.spec.as_ref().unwrap().ingress.iter().flatten() {
                let ports = rule.ports.clone().unwrap_or_default();
                if ports.is_empty() || ports.iter().any(|pp| port_matches(pp, cp)) {
                    out.push(rule.clone());
                }
            }
        }
        out
    }

    fn pod(&self, app: &str) -> &SystemPod {
        self.pods
            .iter()
            .find(|p| p.labels.get("app").map(String::as_str) == Some(app))
            .unwrap_or_else(|| panic!("no system workload with app={app}"))
    }

    fn admits_from(&self, pod: &SystemPod, cp: &ContainerPort, client: &SystemPod) -> bool {
        self.admitting_rules(pod, cp).iter().any(|rule| {
            let from = rule.from.clone().unwrap_or_default();
            from.is_empty()
                || from.iter().any(|peer| {
                    peer.namespace_selector.is_none()
                        && peer.ip_block.is_none()
                        && peer
                            .pod_selector
                            .as_ref()
                            .is_some_and(|s| selector_matches(s, &client.labels))
                })
        })
    }
}

fn port_matches(pp: &NetworkPolicyPort, cp: &ContainerPort) -> bool {
    let proto = pp.protocol.clone().unwrap_or_else(|| "TCP".into());
    let cproto = cp
        .protocol
        .clone()
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| "TCP".into());
    if proto != cproto {
        return false;
    }
    match &pp.port {
        None => true,
        Some(IntOrString::String(name)) => cp
            .name
            .as_deref()
            .is_some_and(|n| !n.is_empty() && n == name),
        Some(IntOrString::Int(n)) => *n == cp.container_port,
    }
}

fn named_port(pod: &SystemPod, name: &str) -> ContainerPort {
    pod.ports
        .iter()
        .find(|p| p.name.as_deref() == Some(name))
        .cloned()
        .unwrap_or_else(|| panic!("{} has no container port named {name:?}", pod.workload))
}

fn numbered_port(pod: &SystemPod, n: i32) -> ContainerPort {
    pod.ports
        .iter()
        .find(|p| p.container_port == n)
        .cloned()
        .unwrap_or_else(|| panic!("{} has no container port {n}", pod.workload))
}

/// Fails unless every rule admitting the port names only system pods whose
/// app label is one of `apps`.
fn require_only_from_apps(
    what: &str,
    inst: &SystemInstall,
    pod: &SystemPod,
    cp: &ContainerPort,
    apps: &[&str],
) {
    let rules = inst.admitting_rules(pod, cp);
    assert!(
        !rules.is_empty(),
        "{what}: {} port {} is admitted from nowhere, want from {apps:?}",
        pod.workload,
        cp.container_port
    );
    for rule in rules {
        let from = rule.from.unwrap_or_default();
        assert!(
            !from.is_empty(),
            "{what}: {} port {} is admitted from any source",
            pod.workload,
            cp.container_port
        );
        for peer in from {
            assert!(
                peer.namespace_selector.is_none()
                    && peer.ip_block.is_none()
                    && peer.pod_selector.is_some(),
                "{what}: {} port {} admits peer {peer:?}",
                pod.workload,
                cp.container_port
            );
            let sel = peer.pod_selector.unwrap();
            assert!(
                !selector_empty(&sel),
                "{what}: {} port {} admits every system pod",
                pod.workload,
                cp.container_port
            );
            for other in &inst.pods {
                if selector_matches(&sel, &other.labels) {
                    let app = other.labels.get("app").map(String::as_str).unwrap_or("");
                    assert!(
                        apps.contains(&app),
                        "{what}: {} port {} admits {}",
                        pod.workload,
                        cp.container_port,
                        other.workload
                    );
                }
            }
        }
    }
}

#[test]
fn system_network_policies_admit_fake_data_plane() {
    if kustomize_missing() {
        return;
    }
    for cfg in install_configs().into_iter().filter(|c| c.fake_data_plane) {
        let name = install_name(&cfg);
        let inst = load_system_install(&cfg);
        let workersync = inst.pod("fake-workersync");
        let api = inst.pod("hivekeep-api");
        assert!(
            inst.admits_from(api, &numbered_port(api, 443), workersync),
            "{name}: hivekeep-api 443 does not admit fake-workersync"
        );
        let keeper = inst.pod("keeper");
        assert!(
            inst.admits_from(keeper, &named_port(keeper, "relay"), workersync),
            "{name}: relay does not admit fake-workersync"
        );
    }
}

#[test]
fn system_network_policies_default_deny() {
    if kustomize_missing() {
        return;
    }
    for cfg in install_configs() {
        let inst = load_system_install(&cfg);
        let found = inst.policies.iter().any(|np| {
            let spec = np.spec.as_ref().unwrap();
            selector_empty(&spec.pod_selector.clone().unwrap_or_default())
                && spec.policy_types.as_deref() == Some(&["Ingress".to_string()][..])
                && spec.ingress.as_ref().is_none_or(|i| i.is_empty())
        });
        assert!(
            found,
            "{}: no NetworkPolicy denies ingress to every system pod",
            install_name(&cfg)
        );
    }
}

#[test]
fn system_network_policies_cover_every_port() {
    if kustomize_missing() {
        return;
    }
    let unadmitted = unadmitted_ports();
    for cfg in install_configs() {
        let name = install_name(&cfg);
        let inst = load_system_install(&cfg);
        for pod in &inst.pods {
            let app = pod.labels.get("app").map(String::as_str).unwrap_or("");
            for cp in &pod.ports {
                let denied = cp
                    .name
                    .as_deref()
                    .is_some_and(|n| unadmitted.get(app).is_some_and(|l| l.contains(&n)));
                let admitted = !inst.admitting_rules(pod, cp).is_empty();
                assert!(
                    !(denied && admitted),
                    "{name}: {} port {} ({:?}) must not be admitted",
                    pod.workload,
                    cp.container_port,
                    cp.name
                );
                assert!(
                    denied || admitted,
                    "{name}: {} port {} ({:?}) is admitted by no NetworkPolicy; admit it in \
                     manifests/install/hivekeep-system-networkpolicies.yaml or list it as unadmitted",
                    pod.workload,
                    cp.container_port,
                    cp.name
                );
            }
        }
    }
}

#[test]
fn system_network_policies_named_ports_resolve() {
    if kustomize_missing() {
        return;
    }
    let mut referenced = BTreeSet::new();
    let mut resolved = BTreeSet::new();
    for cfg in install_configs() {
        let inst = load_system_install(&cfg);
        for np in &inst.policies {
            let spec = np.spec.as_ref().unwrap();
            let sel = spec.pod_selector.clone().unwrap_or_default();
            for rule in spec.ingress.iter().flatten() {
                for pp in rule.ports.iter().flatten() {
                    let Some(IntOrString::String(port)) = &pp.port else {
                        continue;
                    };
                    let key = (np.metadata.name.clone().unwrap_or_default(), port.clone());
                    referenced.insert(key.clone());
                    for pod in inst
                        .pods
                        .iter()
                        .filter(|p| selector_matches(&sel, &p.labels))
                    {
                        if pod
                            .ports
                            .iter()
                            .any(|cp| cp.name.as_deref() == Some(port.as_str()))
                        {
                            resolved.insert(key.clone());
                        }
                    }
                }
            }
        }
    }
    assert!(
        !referenced.is_empty(),
        "no NetworkPolicy names a port; this test checks nothing"
    );
    let unresolved: Vec<_> = referenced.difference(&resolved).collect();
    assert!(
        unresolved.is_empty(),
        "networkpolicy ports no selected pod declares in any install: {unresolved:?}"
    );
}

#[test]
fn system_network_policies_least_privilege() {
    if kustomize_missing() {
        return;
    }
    for cfg in install_configs() {
        let name = install_name(&cfg);
        let inst = load_system_install(&cfg);

        let postgres = inst.pod("postgres");
        require_only_from_apps(
            &name,
            &inst,
            postgres,
            &named_port(postgres, "postgres"),
            &["hivekeep-api"],
        );

        let keeper = inst.pod("keeper");
        require_only_from_apps(
            &name,
            &inst,
            keeper,
            &named_port(keeper, "grpc"),
            &["hivekeep-api"],
        );
        if cfg.fake_data_plane {
            require_only_from_apps(
                &name,
                &inst,
                keeper,
                &named_port(keeper, "relay"),
                &["fake-workersync"],
            );
        }

        // The credential exchange beside the API is for the node plugins.
        let api = inst.pod("hivekeep-api");
        require_only_from_apps(&name, &inst, api, &numbered_port(api, 8445), &["keeper"]);
        // The API admits the control plane and opted-in namespaces only.
        for rule in inst.admitting_rules(api, &numbered_port(api, 443)) {
            let from = rule.from.unwrap_or_default();
            assert!(
                !from.is_empty(),
                "{name}: hivekeep-api 443 is admitted from any source"
            );
            for peer in from {
                assert!(
                    peer.ip_block.is_none(),
                    "{name}: hivekeep-api 443 admits an ipBlock"
                );
                if let Some(ns) = &peer.namespace_selector {
                    let want = labels(&[("hivekeep.io/api-access", "true")]);
                    assert!(
                        ns.match_labels.clone().unwrap_or_default() == want
                            && ns.match_expressions.as_ref().is_none_or(|m| m.is_empty()),
                        "{name}: hivekeep-api 443 admits namespaces {ns:?}"
                    );
                    // Worker pods in an opted-in namespace never call the API.
                    let sel = peer.pod_selector.clone().unwrap_or_else(|| {
                        panic!("{name}: admits every pod of opted-in namespaces")
                    });
                    assert!(
                        !selector_matches(&sel, &labels(&[(WORKER_POOL_LABEL_FOR_TEST, "any")])),
                        "{name}: admits worker pods"
                    );
                    assert!(
                        selector_matches(&sel, &labels(&[("app", "dashboard")])),
                        "{name}: excludes ordinary pods"
                    );
                    continue;
                }
                assert!(
                    peer.pod_selector
                        .as_ref()
                        .is_some_and(|s| !selector_empty(s)),
                    "{name}: hivekeep-api 443 admits every system pod"
                );
            }
        }

        // The egress gateway admits worker pods only, from every namespace.
        let egress = inst.pod("gate-egress");
        let rules = inst.admitting_rules(egress, &named_port(egress, "https"));
        assert!(
            !rules.is_empty(),
            "{name}: gate-egress https is admitted from nowhere"
        );
        for rule in rules {
            let from = rule.from.unwrap_or_default();
            assert!(
                !from.is_empty(),
                "{name}: gate-egress https is admitted from any source"
            );
            for peer in from {
                assert!(
                    peer.namespace_selector.as_ref().is_some_and(selector_empty),
                    "{name}: gate-egress peer {peer:?}"
                );
                let sel = peer.pod_selector.clone().unwrap_or_default();
                assert!(
                    peer.pod_selector.is_some()
                        && selector_matches(&sel, &labels(&[(WORKER_POOL_LABEL_FOR_TEST, "any")]))
                        && !selector_matches(&sel, &labels(&[("app", "probe")])),
                    "{name}: gate-egress https admits {peer:?}"
                );
            }
        }
    }
}

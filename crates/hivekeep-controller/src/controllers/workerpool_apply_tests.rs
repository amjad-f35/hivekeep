use std::collections::{BTreeMap, HashMap};

use hivekeep_core::preview::{Gate, PreviewGates};
use hivekeep_k8s::crd::{WorkerPoolSandboxClass, WorkerPoolSpec};
use hivekeep_k8s::install_defaults::{
    KEEPER_SERVICE_ACCOUNT, ROUTER_SERVICE_ACCOUNT, SYSTEM_NAMESPACE,
};
use k8s_openapi::api::core::v1::{EnvVar, PodSpec};

use super::super::worker_env::{
    DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL, WORKER_OTEL_RESOURCE_ATTRIBUTES,
};
use super::*;

fn settings() -> WorkerPodSettings {
    WorkerPodSettings {
        otel: WorkerOtelSettings::default(),
        system_namespace: SYSTEM_NAMESPACE.into(),
        keeper_service_account: KEEPER_SERVICE_ACCOUNT.into(),
        router_service_account: ROUTER_SERVICE_ACCOUNT.into(),
        dns_egress_policy: DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL.into(),
        preview: PreviewGates::none(),
    }
}

fn with_otel(otel: WorkerOtelSettings) -> WorkerPodSettings {
    WorkerPodSettings { otel, ..settings() }
}

/// The pool the apply tests build from: no sandbox class, so the API
/// default (gvisor) applies.
fn test_pool(tmpl: Option<WorkerPoolPodTemplate>) -> WorkerPool {
    let mut wp = WorkerPool::new(
        "pool",
        WorkerPoolSpec {
            replicas: 2,
            worker_image: "worker:v1".into(),
            template: tmpl,
            sandbox_classes: vec![],
        },
    );
    wp.metadata.namespace = Some("default".into());
    wp.metadata.uid = Some("uid".into());
    wp
}

fn with_class(mut wp: WorkerPool, class: Option<SandboxClass>) -> WorkerPool {
    wp.spec.sandbox_classes = class
        .map(|name| {
            vec![WorkerPoolSandboxClass {
                name,
                config_ref: None,
            }]
        })
        .unwrap_or_default();
    wp
}

fn class_toleration(class: &str) -> Toleration {
    Toleration {
        key: Some(SANDBOX_CLASS_TAINT_KEY.into()),
        operator: Some("Equal".into()),
        value: Some(class.into()),
        effect: Some("NoSchedule".into()),
        toleration_seconds: None,
    }
}

fn field_env(name: &str, path: &str) -> EnvVar {
    serde_json::from_value(
        serde_json::json!({"name": name, "valueFrom": {"fieldRef": {"fieldPath": path}}}),
    )
    .unwrap()
}

/// The Deployment of `test_pool(None)`, written out field by field, with
/// `mutate` applied to its pod spec.
fn expected_deployment(mutate: impl FnOnce(&mut PodSpec)) -> Deployment {
    let ro = |name: &str, path: &str| VolumeMount {
        name: name.into(),
        mount_path: path.into(),
        read_only: Some(true),
        ..Default::default()
    };
    let ctb = |signer: &str| VolumeProjection {
        cluster_trust_bundle: Some(ClusterTrustBundleProjection {
            signer_name: Some(signer.into()),
            label_selector: Some(LabelSelector {
                match_labels: Some(BTreeMap::from([(
                    "podcert.hivekeep.io/canarying".into(),
                    "live".into(),
                )])),
                match_expressions: None,
            }),
            path: "trust-bundle.pem".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let capacity_file = |path: &str, res: &str, div: &str| DownwardAPIVolumeFile {
        path: path.into(),
        resource_field_ref: Some(ResourceFieldSelector {
            container_name: Some("worker".into()),
            resource: res.into(),
            divisor: Some(Quantity(div.into())),
        }),
        ..Default::default()
    };
    let container = Container {
        name: "worker".into(),
        image: Some("worker:v1".into()),
        args: Some(vec![
            "--pod-uid=$(POD_UID)".into(),
            "--pod-ips=$(POD_IPS)".into(),
            "--tunnel-listen-address=:443".into(),
            "--tunnel-connect-listen-address=:8443".into(),
            "--tunnel-credential-bundle=/run/podidentity.podcert.hivekeep.io/credential-bundle.pem"
                .into(),
            "--tunnel-trust-bundle=/run/podidentity.podcert.hivekeep.io/trust-bundle.pem".into(),
            format!(
                "--tunnel-client-identity={}",
                install_defaults::router_spiffe_id(SYSTEM_NAMESPACE)
            ),
            "--tunnel-egress-listen-address=0.0.0.0:15001".into(),
            "--tunnel-egress-trust-bundle=/run/servicedns.podcert.hivekeep.io/trust-bundle.pem"
                .into(),
            "--dns-egress-policy=enforce-in-tunnel".into(),
            "--preview=".into(),
        ]),
        ports: Some(vec![
            ContainerPort {
                name: Some("https".into()),
                container_port: 443,
                protocol: Some("TCP".into()),
                ..Default::default()
            },
            ContainerPort {
                name: Some("connect".into()),
                container_port: 8443,
                protocol: Some("TCP".into()),
                ..Default::default()
            },
            ContainerPort {
                name: Some("readyz".into()),
                container_port: 8080,
                protocol: Some("TCP".into()),
                ..Default::default()
            },
        ]),
        readiness_probe: Some(Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/readyz".into()),
                port: IntOrString::String("readyz".into()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        security_context: Some(SecurityContext {
            run_as_user: Some(0),
            run_as_group: Some(0),
            privileged: Some(false),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".into()]),
                add: Some(
                    WORKER_GVISOR_CAPABILITIES
                        .iter()
                        .map(|c| c.to_string())
                        .collect(),
                ),
            }),
            app_armor_profile: Some(AppArmorProfile {
                type_: "Unconfined".into(),
                localhost_profile: None,
            }),
            seccomp_profile: Some(SeccompProfile {
                type_: "Unconfined".into(),
                localhost_profile: None,
            }),
            ..Default::default()
        }),
        env: Some(vec![
            field_env("POD_UID", "metadata.uid"),
            field_env("POD_NAMESPACE", "metadata.namespace"),
            field_env(
                "WORKER_POOL_NAME",
                "metadata.labels['hivekeep.io/worker-pool']",
            ),
            field_env("POD_NAME", "metadata.name"),
            field_env("NODE_NAME", "spec.nodeName"),
            field_env("POD_IPS", "status.podIPs"),
            EnvVar {
                name: "OTEL_RESOURCE_ATTRIBUTES".into(),
                value: Some(WORKER_OTEL_RESOURCE_ATTRIBUTES.into()),
                value_from: None,
            },
        ]),
        volume_mounts: Some(vec![
            ro("worker-capacity", "/run/worker-capacity"),
            VolumeMount {
                name: "hivekeep-base".into(),
                mount_path: "/var/lib/hivekeep".into(),
                mount_propagation: Some("HostToContainer".into()),
                ..Default::default()
            },
            ro("tunnel-identity", "/run/podidentity.podcert.hivekeep.io"),
            ro("tunnel-egress-trust", "/run/servicedns.podcert.hivekeep.io"),
        ]),
        resources: Some(ResourceRequirements::default()),
        ..Default::default()
    };
    let mut pod = PodSpec {
        security_context: Some(PodSecurityContext {
            run_as_user: Some(0),
            run_as_group: Some(0),
            ..Default::default()
        }),
        volumes: Some(vec![
            Volume {
                name: "worker-capacity".into(),
                downward_api: Some(DownwardAPIVolumeSource {
                    items: Some(vec![
                        capacity_file("cpu_millicores", "limits.cpu", "1m"),
                        capacity_file("memory_bytes", "limits.memory", "1"),
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Volume {
                name: "hivekeep-base".into(),
                host_path: Some(HostPathVolumeSource {
                    path: "/var/lib/hivekeep".into(),
                    type_: Some("DirectoryOrCreate".into()),
                }),
                ..Default::default()
            },
            Volume {
                name: "tunnel-identity".into(),
                projected: Some(ProjectedVolumeSource {
                    sources: Some(vec![
                        VolumeProjection {
                            pod_certificate: Some(PodCertificateProjection {
                                signer_name: "podidentity.podcert.hivekeep.io/identity".into(),
                                key_type: "ECDSAP256".into(),
                                credential_bundle_path: Some("credential-bundle.pem".into()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        ctb("podidentity.podcert.hivekeep.io/identity"),
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Volume {
                name: "tunnel-egress-trust".into(),
                projected: Some(ProjectedVolumeSource {
                    sources: Some(vec![ctb("servicedns.podcert.hivekeep.io/identity")]),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ]),
        containers: vec![container],
        tolerations: Some(vec![class_toleration("gvisor")]),
        priority_class_name: Some(String::new()),
        service_account_name: Some("hivekeep-worker".into()),
        affinity: Some(Affinity::default()),
        termination_grace_period_seconds: Some(3600),
        ..Default::default()
    };
    mutate(&mut pod);
    let pool_label = BTreeMap::from([("hivekeep.io/worker-pool".to_string(), "pool".to_string())]);
    Deployment {
        metadata: ObjectMeta {
            name: Some("pool".into()),
            namespace: Some("default".into()),
            labels: Some(pool_label.clone()),
            owner_references: Some(vec![OwnerReference {
                api_version: "hivekeep.io/v1".into(),
                kind: "WorkerPool".into(),
                name: "pool".into(),
                uid: "uid".into(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(2),
            strategy: Some(DeploymentStrategy {
                type_: Some("RollingUpdate".into()),
                rolling_update: Some(RollingUpdateDeployment {
                    max_surge: Some(IntOrString::Int(0)),
                    max_unavailable: Some(IntOrString::String("10%".into())),
                }),
            }),
            progress_deadline_seconds: Some(4800),
            selector: LabelSelector {
                match_labels: Some(pool_label.clone()),
                match_expressions: None,
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(pool_label),
                    ..Default::default()
                }),
                spec: Some(pod),
            },
            ..Default::default()
        }),
        status: None,
    }
}

fn pod_spec(d: &Deployment) -> &PodSpec {
    d.spec.as_ref().unwrap().template.spec.as_ref().unwrap()
}

fn worker(d: &Deployment) -> &Container {
    &pod_spec(d).containers[0]
}

fn requirement(key: &str, value: &str) -> NodeSelectorRequirement {
    NodeSelectorRequirement {
        key: key.into(),
        operator: "In".into(),
        values: Some(vec![value.into()]),
    }
}

fn quantities(pairs: &[(&str, &str)]) -> BTreeMap<String, Quantity> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Quantity(v.to_string())))
        .collect()
}

#[test]
fn build_deployment_apply_config() {
    let required = NodeAffinity {
        required_during_scheduling_ignored_during_execution: Some(NodeSelector {
            node_selector_terms: vec![NodeSelectorTerm {
                match_expressions: Some(vec![requirement("workload", "hivekeep")]),
                match_fields: None,
            }],
        }),
        preferred_during_scheduling_ignored_during_execution: None,
    };
    let preferred = NodeAffinity {
        required_during_scheduling_ignored_during_execution: None,
        preferred_during_scheduling_ignored_during_execution: Some(vec![PreferredSchedulingTerm {
            weight: 50,
            preference: NodeSelectorTerm {
                match_expressions: Some(vec![requirement("disk", "ssd")]),
                match_fields: None,
            },
        }]),
    };
    let toleration = Toleration {
        key: Some("dedicated".into()),
        operator: Some("Equal".into()),
        value: Some("workerpool".into()),
        effect: Some("NoSchedule".into()),
        toleration_seconds: Some(300),
    };
    let selector = BTreeMap::from([
        ("accelerator".to_string(), "gpu".to_string()),
        ("topology".into(), "high-mem".into()),
    ]);
    let resources = ResourceRequirements {
        requests: Some(quantities(&[("cpu", "500m"), ("memory", "1Gi")])),
        limits: Some(quantities(&[("cpu", "1"), ("memory", "2Gi")])),
        claims: None,
    };

    type Mutate = Box<dyn FnOnce(&mut PodSpec)>;
    let cases: Vec<(&str, Option<WorkerPoolPodTemplate>, Mutate)> = vec![
        ("default workerpool", None, Box::new(|_| {})),
        (
            "with node selector",
            Some(WorkerPoolPodTemplate {
                node_selector: selector.clone(),
                ..Default::default()
            }),
            Box::new({
                let s = selector.clone();
                move |p| p.node_selector = Some(s)
            }),
        ),
        (
            "with tolerations",
            Some(WorkerPoolPodTemplate {
                tolerations: vec![toleration.clone()],
                ..Default::default()
            }),
            // Template tolerations first; the sandbox class one after them.
            Box::new({
                let t = toleration.clone();
                move |p| p.tolerations = Some(vec![t, class_toleration("gvisor")])
            }),
        ),
        (
            "with node affinity",
            Some(WorkerPoolPodTemplate {
                node_affinity: Some(required.clone()),
                ..Default::default()
            }),
            Box::new({
                let a = required.clone();
                move |p| {
                    p.affinity = Some(Affinity {
                        node_affinity: Some(a),
                        ..Default::default()
                    })
                }
            }),
        ),
        (
            "with priority class name",
            Some(WorkerPoolPodTemplate {
                priority_class_name: "interactive-workerpool".into(),
                ..Default::default()
            }),
            Box::new(|p| p.priority_class_name = Some("interactive-workerpool".into())),
        ),
        (
            "with service account name",
            Some(WorkerPoolPodTemplate {
                service_account_name: Some("hivekeep-worker".into()),
                ..Default::default()
            }),
            Box::new(|p| p.service_account_name = Some("hivekeep-worker".into())),
        ),
        (
            "with resources",
            Some(WorkerPoolPodTemplate {
                resources: Some(resources.clone()),
                ..Default::default()
            }),
            Box::new({
                let r = resources.clone();
                move |p| p.containers[0].resources = Some(r)
            }),
        ),
        (
            "with combined scheduling fields",
            Some(WorkerPoolPodTemplate {
                node_selector: selector.clone(),
                tolerations: vec![toleration.clone()],
                priority_class_name: "interactive-workerpool".into(),
                node_affinity: Some(preferred.clone()),
                ..Default::default()
            }),
            Box::new({
                let (s, t, a) = (selector.clone(), toleration.clone(), preferred.clone());
                move |p| {
                    p.node_selector = Some(s);
                    p.tolerations = Some(vec![t, class_toleration("gvisor")]);
                    p.priority_class_name = Some("interactive-workerpool".into());
                    p.affinity = Some(Affinity {
                        node_affinity: Some(a),
                        ..Default::default()
                    });
                }
            }),
        ),
    ];
    for (name, tmpl, mutate) in cases {
        let got = build_deployment(&test_pool(tmpl), &settings());
        assert_eq!(got, expected_deployment(mutate), "{name}");
    }
}

#[test]
fn build_deployment_apply_config_metadata() {
    let wp = test_pool(Some(WorkerPoolPodTemplate {
        labels: BTreeMap::from([
            ("project".into(), "hivekeep".into()),
            ("team".into(), "compute".into()),
            ("hivekeep.io/worker-pool".into(), "incorrect".into()),
        ]),
        annotations: BTreeMap::from([(
            "policy.example.com/exemption".into(),
            "sandbox-host".into(),
        )]),
        ..Default::default()
    }));
    let got = build_deployment(&wp, &settings());
    let want_labels = BTreeMap::from([
        ("project".to_string(), "hivekeep".to_string()),
        ("team".into(), "compute".into()),
        ("hivekeep.io/worker-pool".into(), "pool".into()),
    ]);
    let want_annotations = BTreeMap::from([(
        "policy.example.com/exemption".to_string(),
        "sandbox-host".to_string(),
    )]);
    assert_eq!(got.metadata.labels.as_ref(), Some(&want_labels));
    assert_eq!(got.metadata.annotations.as_ref(), Some(&want_annotations));
    let tmeta = got
        .spec
        .as_ref()
        .unwrap()
        .template
        .metadata
        .as_ref()
        .unwrap();
    assert_eq!(tmeta.labels.as_ref(), Some(&want_labels));
    assert_eq!(tmeta.annotations.as_ref(), Some(&want_annotations));
}

#[test]
fn sandbox_class_toleration() {
    for (name, class, want) in [
        ("gvisor default", None, "gvisor"),
        ("gvisor explicit", Some(SandboxClass::Gvisor), "gvisor"),
        ("microvm", Some(SandboxClass::Microvm), "microvm"),
    ] {
        let wp = with_class(
            test_pool(Some(WorkerPoolPodTemplate {
                tolerations: vec![Toleration {
                    key: Some("dedicated".into()),
                    operator: Some("Exists".into()),
                    effect: Some("NoSchedule".into()),
                    ..Default::default()
                }],
                ..Default::default()
            })),
            class,
        );
        let d = build_deployment(&wp, &settings());
        let mut class_values = Vec::new();
        let mut has_template = false;
        for t in pod_spec(&d).tolerations.iter().flatten() {
            match t.key.as_deref() {
                Some(SANDBOX_CLASS_TAINT_KEY) => {
                    assert_eq!(t.operator.as_deref(), Some("Equal"), "{name}");
                    assert_eq!(t.effect.as_deref(), Some("NoSchedule"), "{name}");
                    class_values.push(t.value.clone().expect("value"));
                }
                Some("dedicated") => has_template = true,
                _ => {}
            }
        }
        assert_eq!(class_values, vec![want.to_string()], "{name}");
        assert!(
            has_template,
            "{name}: template toleration dropped; the class toleration is additive"
        );
    }
}

fn container_mounts_path(c: &Container, path: &str) -> bool {
    c.volume_mounts
        .iter()
        .flatten()
        .any(|m| m.mount_path == path)
}

fn device_limit(c: &Container, name: &str) -> Option<String> {
    c.resources
        .as_ref()?
        .limits
        .as_ref()?
        .get(name)
        .map(|q| q.0.clone())
}

#[test]
fn microvm_pod_shape() {
    for (name, class, want_microvm) in [
        ("gvisor default", None, false),
        ("gvisor explicit", Some(SandboxClass::Gvisor), false),
        ("microvm", Some(SandboxClass::Microvm), true),
    ] {
        let d = build_deployment(&with_class(test_pool(None), class), &settings());
        let ps = pod_spec(&d);
        // /dev/kvm must come from the device plugin, never a hostPath;
        // /dev/net/tun is allowed by default and bind-mounted.
        let mut has_tun = false;
        for v in ps.volumes.iter().flatten() {
            let Some(hp) = &v.host_path else { continue };
            if hp.path == TUN_DEVICE_PATH {
                has_tun = true;
                assert_eq!(hp.type_.as_deref(), Some("CharDevice"), "{name}");
            } else {
                assert!(
                    !hp.path.starts_with("/dev/"),
                    "{name}: {} must be a resource request",
                    hp.path
                );
            }
        }
        if has_tun {
            assert!(
                container_mounts_path(&ps.containers[0], TUN_DEVICE_PATH),
                "{name}"
            );
        }
        let kvm = device_limit(&ps.containers[0], RESOURCE_KVM);
        if let Some(q) = &kvm {
            assert_eq!(q, "1", "{name}");
        }
        assert!(
            !ps.node_selector
                .iter()
                .flatten()
                .any(|(k, _)| k == SANDBOX_CLASS_TAINT_KEY),
            "{name}: placement comes from the device request"
        );
        assert_eq!(kvm.is_some(), want_microvm, "{name}: device request");
        assert_eq!(has_tun, want_microvm, "{name}: tun mount");
    }
}

#[test]
fn microvm_device_requests_preserve_template_resources() {
    let wp = with_class(
        test_pool(Some(WorkerPoolPodTemplate {
            resources: Some(ResourceRequirements {
                limits: Some(quantities(&[("memory", "2Gi")])),
                ..Default::default()
            }),
            ..Default::default()
        })),
        Some(SandboxClass::Microvm),
    );
    let d = build_deployment(&wp, &settings());
    assert_eq!(device_limit(worker(&d), "memory").as_deref(), Some("2Gi"));
    assert_eq!(device_limit(worker(&d), RESOURCE_KVM).as_deref(), Some("1"));
}

#[test]
fn worker_security_context_by_class() {
    let gvisor: Vec<String> = WORKER_GVISOR_CAPABILITIES
        .iter()
        .map(|c| c.to_string())
        .collect();
    let microvm: Vec<String> = worker_microvm_capabilities()
        .iter()
        .map(|c| c.to_string())
        .collect();
    for (name, wp_class, want_caps) in [
        ("gvisor default", None, &gvisor),
        ("gvisor explicit", Some(SandboxClass::Gvisor), &gvisor),
        ("microvm", Some(SandboxClass::Microvm), &microvm),
    ] {
        let sc = worker_security_context(wp_class.unwrap_or_default());
        assert_eq!(sc.privileged, Some(false), "{name}");
        assert_eq!(
            (sc.run_as_user, sc.run_as_group),
            (Some(0), Some(0)),
            "{name}"
        );
        let caps = sc
            .capabilities
            .as_ref()
            .expect("capabilities set so the default set is dropped");
        assert_eq!(
            caps.drop.as_deref(),
            Some(&["ALL".to_string()][..]),
            "{name}"
        );
        assert_eq!(caps.add.as_ref(), Some(want_caps), "{name}");
        assert_eq!(
            sc.app_armor_profile.as_ref().map(|p| p.type_.as_str()),
            Some("Unconfined"),
            "{name}"
        );
        assert_eq!(
            sc.seccomp_profile.as_ref().map(|p| p.type_.as_str()),
            Some("Unconfined"),
            "{name}"
        );
    }
    let microvm_set: Vec<&str> = worker_microvm_capabilities();
    assert!(microvm_set.contains(&"FSETID") && microvm_set.contains(&"DAC_READ_SEARCH"));
}

#[test]
fn termination_grace_period_seconds() {
    let d = build_deployment(&test_pool(None), &settings());
    assert_eq!(pod_spec(&d).termination_grace_period_seconds, Some(3600));
}

#[test]
fn rollout_strategy() {
    let d = build_deployment(&test_pool(None), &settings());
    let spec = d.spec.as_ref().unwrap();
    let strategy = spec.strategy.as_ref().expect("strategy");
    assert_eq!(strategy.type_.as_deref(), Some("RollingUpdate"));
    let ru = strategy.rolling_update.as_ref().expect("rollingUpdate");
    assert_eq!(ru.max_surge, Some(IntOrString::Int(0)));
    assert_eq!(ru.max_unavailable, Some(IntOrString::String("10%".into())));
    let deadline = spec
        .progress_deadline_seconds
        .expect("progressDeadlineSeconds");
    assert!(i64::from(deadline) > WORKER_TERMINATION_GRACE_PERIOD_SECONDS);
}

struct EnvInfo {
    index: usize,
    value: String,
}

fn env_by_name(c: &Container) -> HashMap<String, EnvInfo> {
    c.env
        .iter()
        .flatten()
        .enumerate()
        .map(|(index, e)| {
            (
                e.name.clone(),
                EnvInfo {
                    index,
                    value: e.value.clone().unwrap_or_default(),
                },
            )
        })
        .collect()
}

/// The distinct `$(VAR)` references in `value`, first use first.
fn env_refs(value: &str) -> Vec<String> {
    let mut refs: Vec<String> = Vec::new();
    let mut rest = value;
    while let Some(start) = rest.find("$(") {
        let after = &rest[start + 2..];
        let Some(end) = after.find(')') else { break };
        let name = &after[..end];
        if !name.is_empty()
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !refs.iter().any(|r| r == name)
        {
            refs.push(name.to_string());
        }
        rest = &after[end + 1..];
    }
    refs
}

fn env_attr_keys(value: &str) -> Vec<String> {
    value
        .split(',')
        .filter_map(|p| p.split_once('=').map(|(k, _)| k.to_string()))
        .collect()
}

const ENDPOINT: &str = "http://collector.otel-system.svc:4317";

#[test]
fn build_deployment_apply_config_otel_endpoint() {
    for (name, endpoint, want) in [
        ("endpoint empty", "", false),
        ("endpoint set", ENDPOINT, true),
    ] {
        let d = build_deployment(
            &test_pool(None),
            &with_otel(WorkerOtelSettings {
                endpoint: endpoint.into(),
                ..Default::default()
            }),
        );
        let env = env_by_name(worker(&d));
        for k in ["POD_UID", "POD_NAMESPACE", "WORKER_POOL_NAME"] {
            assert!(env.contains_key(k), "{name}: {k} must always be set");
        }
        let got = env.get("OTEL_EXPORTER_OTLP_ENDPOINT");
        assert_eq!(got.is_some(), want, "{name}");
        assert_eq!(
            got.map(|e| e.value.as_str()).unwrap_or(""),
            endpoint,
            "{name}"
        );
        let attrs = &env["OTEL_RESOURCE_ATTRIBUTES"];
        assert_eq!(
            env_attr_keys(&attrs.value),
            [
                "k8s.namespace.name",
                "k8s.pod.name",
                "k8s.pod.uid",
                "k8s.node.name",
                "service.instance.id"
            ],
            "{name}"
        );
        let refs = env_refs(&attrs.value);
        assert!(!refs.is_empty(), "{name}: the identity substitutes nothing");
        for r in refs {
            let e = env
                .get(&r)
                .unwrap_or_else(|| panic!("{name}: {r} must be set for substitution"));
            assert!(
                e.index < attrs.index,
                "{name}: {r} must precede OTEL_RESOURCE_ATTRIBUTES"
            );
        }
    }
}

/// Checks that exactly the variables in `want` among `keys` are set, with
/// those values.
fn check_env(name: &str, otel: WorkerOtelSettings, keys: &[&str], want: &[(&str, &str)]) {
    let d = build_deployment(&test_pool(None), &with_otel(otel));
    let env = env_by_name(worker(&d));
    for k in keys {
        let w = want.iter().find(|(wk, _)| wk == k).map(|(_, v)| *v);
        assert_eq!(env.get(*k).map(|e| e.value.as_str()), w, "{name}: {k}");
    }
}

#[test]
fn build_deployment_apply_config_metric_export_tuning() {
    let keys = ["OTEL_METRIC_EXPORT_INTERVAL", "OTEL_METRIC_EXPORT_TIMEOUT"];
    let o = |interval: &str, timeout: &str, endpoint: &str| WorkerOtelSettings {
        endpoint: endpoint.into(),
        metric_export_interval: interval.into(),
        metric_export_timeout: timeout.into(),
        ..Default::default()
    };
    check_env("unset keeps SDK defaults", o("", "", ENDPOINT), &keys, &[]);
    check_env(
        "both set with endpoint",
        o("10000", "10000", ENDPOINT),
        &keys,
        &[
            ("OTEL_METRIC_EXPORT_INTERVAL", "10000"),
            ("OTEL_METRIC_EXPORT_TIMEOUT", "10000"),
        ],
    );
    check_env(
        "interval alone",
        o("10000", "", ENDPOINT),
        &keys,
        &[("OTEL_METRIC_EXPORT_INTERVAL", "10000")],
    );
    check_env(
        "timeout alone",
        o("", "10000", ENDPOINT),
        &keys,
        &[("OTEL_METRIC_EXPORT_TIMEOUT", "10000")],
    );
    check_env(
        "set without endpoint",
        o("10000", "10000", ""),
        &keys,
        &[
            ("OTEL_METRIC_EXPORT_INTERVAL", "10000"),
            ("OTEL_METRIC_EXPORT_TIMEOUT", "10000"),
        ],
    );
}

#[test]
fn build_deployment_apply_config_traces_sampler_propagation() {
    let keys = ["OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG"];
    let o = |sampler: &str, arg: &str, endpoint: &str| WorkerOtelSettings {
        endpoint: endpoint.into(),
        traces_sampler: sampler.into(),
        traces_sampler_arg: arg.into(),
        ..Default::default()
    };
    check_env(
        "unset keeps binary default",
        o("", "", ENDPOINT),
        &keys,
        &[],
    );
    check_env(
        "sampler and arg with endpoint",
        o("parentbased_traceidratio", "0.25", ENDPOINT),
        &keys,
        &[
            ("OTEL_TRACES_SAMPLER", "parentbased_traceidratio"),
            ("OTEL_TRACES_SAMPLER_ARG", "0.25"),
        ],
    );
    check_env(
        "sampler alone",
        o("parentbased_always_on", "", ENDPOINT),
        &keys,
        &[("OTEL_TRACES_SAMPLER", "parentbased_always_on")],
    );
    check_env("arg alone stays unset", o("", "0.25", ENDPOINT), &keys, &[]);
    check_env(
        "set without endpoint",
        o("parentbased_traceidratio", "0.25", ""),
        &keys,
        &[
            ("OTEL_TRACES_SAMPLER", "parentbased_traceidratio"),
            ("OTEL_TRACES_SAMPLER_ARG", "0.25"),
        ],
    );
}

#[test]
fn build_deployment_apply_config_traces_exporter_propagation() {
    let o = |v: &str, endpoint: &str| WorkerOtelSettings {
        endpoint: endpoint.into(),
        traces_exporter: v.into(),
        ..Default::default()
    };
    let keys = ["OTEL_TRACES_EXPORTER"];
    check_env("unset keeps binary default", o("", ENDPOINT), &keys, &[]);
    check_env(
        "none with endpoint",
        o("none", ENDPOINT),
        &keys,
        &[("OTEL_TRACES_EXPORTER", "none")],
    );
    check_env(
        "none without endpoint",
        o("none", ""),
        &keys,
        &[("OTEL_TRACES_EXPORTER", "none")],
    );
}

#[test]
fn build_deployment_apply_config_metrics_exporter_propagation() {
    let o = |v: &str, endpoint: &str| WorkerOtelSettings {
        endpoint: endpoint.into(),
        metrics_exporter: v.into(),
        ..Default::default()
    };
    let keys = ["OTEL_METRICS_EXPORTER"];
    check_env("unset keeps binary default", o("", ENDPOINT), &keys, &[]);
    check_env(
        "none with endpoint",
        o("none", ENDPOINT),
        &keys,
        &[("OTEL_METRICS_EXPORTER", "none")],
    );
    check_env(
        "none without endpoint",
        o("none", ""),
        &keys,
        &[("OTEL_METRICS_EXPORTER", "none")],
    );
}

#[test]
fn build_deployment_apply_config_logs_exporter() {
    let o = |v: &str, endpoint: &str| WorkerOtelSettings {
        endpoint: endpoint.into(),
        logs_exporter: v.into(),
        ..Default::default()
    };
    let keys = ["OTEL_LOGS_EXPORTER"];
    check_env(
        "set with endpoint",
        o("otlp", ENDPOINT),
        &keys,
        &[("OTEL_LOGS_EXPORTER", "otlp")],
    );
    check_env("unset keeps binary default", o("", ENDPOINT), &keys, &[]);
    check_env(
        "set without endpoint",
        o("otlp", ""),
        &keys,
        &[("OTEL_LOGS_EXPORTER", "otlp")],
    );
}

fn flag_value(c: &Container, flag: &str) -> Option<String> {
    c.args
        .iter()
        .flatten()
        .filter_map(|a| a.strip_prefix(flag).map(String::from))
        .next_back()
}

#[test]
fn build_deployment_tunnel_identities_relocated_namespace() {
    let s = WorkerPodSettings {
        system_namespace: "hivekeep-test".into(),
        ..settings()
    };
    let d = build_deployment(&test_pool(None), &s);
    assert_eq!(
        flag_value(worker(&d), "--tunnel-client-identity=").as_deref(),
        Some("spiffe://cluster.local/ns/hivekeep-test/sa/hivekeep-gate-router")
    );
    assert_eq!(
        flag_value(worker(&d), "--tunnel-broker-identity=").as_deref(),
        Some("spiffe://cluster.local/ns/hivekeep-test/sa/hivekeep-keeper")
    );
}

#[test]
fn build_deployment_tunnel_identities_prefixed_service_accounts() {
    let s = WorkerPodSettings {
        system_namespace: "acme-system".into(),
        keeper_service_account: "acme-keeper".into(),
        router_service_account: "acme-gate-router".into(),
        ..settings()
    };
    let d = build_deployment(&test_pool(None), &s);
    assert_eq!(
        flag_value(worker(&d), "--tunnel-client-identity=").as_deref(),
        Some("spiffe://cluster.local/ns/acme-system/sa/acme-gate-router")
    );
    assert_eq!(
        flag_value(worker(&d), "--tunnel-broker-identity=").as_deref(),
        Some("spiffe://cluster.local/ns/acme-system/sa/acme-keeper")
    );
}

#[test]
fn build_deployment_omits_broker_identity_for_canonical_install() {
    let d = build_deployment(&test_pool(None), &settings());
    assert_eq!(flag_value(worker(&d), "--tunnel-broker-identity="), None);
    assert_eq!(
        flag_value(worker(&d), "--tunnel-client-identity=").as_deref(),
        Some(install_defaults::router_spiffe_id(SYSTEM_NAMESPACE).as_str())
    );
}

#[test]
fn build_deployment_apply_config_worker_pool_env() {
    let d = build_deployment(&test_pool(None), &settings());
    let path = worker(&d)
        .env
        .iter()
        .flatten()
        .find(|e| e.name == "WORKER_POOL_NAME")
        .and_then(|e| {
            e.value_from
                .as_ref()?
                .field_ref
                .as_ref()
                .map(|f| f.field_path.clone())
        });
    assert_eq!(
        path,
        Some(format!("metadata.labels['{WORKER_POOL_LABEL}']"))
    );
    let tlabels = d
        .spec
        .as_ref()
        .unwrap()
        .template
        .metadata
        .as_ref()
        .unwrap()
        .labels
        .as_ref()
        .unwrap();
    assert!(tlabels.contains_key(WORKER_POOL_LABEL));
}

#[test]
fn build_deployment_preview() {
    for (name, gates, want) in [
        ("disabled", PreviewGates::none(), "--preview="),
        (
            "named",
            PreviewGates::none().with(Gate::ExternalVolumes),
            "--preview=ExternalVolumes",
        ),
        (
            "ALL",
            PreviewGates::from_flags(["ALL"]).unwrap(),
            "--preview=ExternalVolumes,GoldenEgressPolicy,PerTemplateSnapshotLocation",
        ),
    ] {
        let s = WorkerPodSettings {
            preview: gates,
            ..settings()
        };
        let d = build_deployment(&test_pool(None), &s);
        let got: Vec<&String> = worker(&d)
            .args
            .iter()
            .flatten()
            .filter(|a| a.starts_with("--preview"))
            .collect();
        assert_eq!(got, vec![want], "{name}");
    }
}

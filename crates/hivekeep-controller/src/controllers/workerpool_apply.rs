//! The worker Deployment a WorkerPool runs, as the server-side apply
//! configuration the WorkerPool controller owns.
//!
//! Only the fields set here are owned by the controller. Fields it sets to
//! an empty value (priority class, affinity, resources) are still owned,
//! so a value removed from the pool's template is removed from the
//! Deployment too.

use std::collections::BTreeMap;

use hivekeep_k8s::crd::{SandboxClass, WorkerPool, WorkerPoolPodTemplate, API_VERSION};
use hivekeep_k8s::install_defaults::{self, WORKER_SERVICE_ACCOUNT};
use k8s_openapi::api::apps::v1::{
    Deployment, DeploymentSpec, DeploymentStrategy, RollingUpdateDeployment,
};
use k8s_openapi::api::core::v1::{
    Affinity, AppArmorProfile, Capabilities, ClusterTrustBundleProjection, Container,
    ContainerPort, DownwardAPIVolumeFile, DownwardAPIVolumeSource, HTTPGetAction,
    HostPathVolumeSource, NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
    PodCertificateProjection, PodSecurityContext, PodSpec, PodTemplateSpec,
    PreferredSchedulingTerm, Probe, ProjectedVolumeSource, ResourceFieldSelector,
    ResourceRequirements, SeccompProfile, SecurityContext, Toleration, Volume, VolumeMount,
    VolumeProjection,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use super::meta::label;
use super::worker_env::{
    preview_arg, worker_container_env, WorkerOtelSettings, CANARYING_LABEL, NODE_BASE_PATH,
    POD_IDENTITY_SIGNER, RESOURCE_KVM, SANDBOX_CLASS_TAINT_KEY, SERVICE_DNS_SIGNER,
    WORKER_CAPACITY_CPU_FILE, WORKER_CAPACITY_MEMORY_FILE, WORKER_CAPACITY_MOUNT_PATH,
    WORKER_POOL_LABEL,
};
use hivekeep_core::preview::PreviewGates;

/// Pod termination grace period of worker pods: an hour, for the actors
/// on a worker to drain.
pub const WORKER_TERMINATION_GRACE_PERIOD_SECONDS: i64 = 3600;

/// A pool edit rolls its workers through the eviction path, so the rollout
/// never surges (a surge could stall a small pool) and takes down 10% at a
/// time (at least one pod).
pub const WORKER_ROLLOUT_MAX_SURGE: i32 = 0;
/// See [`WORKER_ROLLOUT_MAX_SURGE`].
pub const WORKER_ROLLOUT_MAX_UNAVAILABLE: &str = "10%";
/// Above the pod grace period with room for the replacement to start, so a
/// batch waiting out its drain is not reported as a failed rollout.
pub const WORKER_ROLLOUT_PROGRESS_DEADLINE_SECONDS: i32 = 4800;

/// Projected volume with the worker tunnel's credentials and the
/// pod-identity trust bundle.
pub const TUNNEL_IDENTITY_VOLUME: &str = "tunnel-identity";
/// Where [`TUNNEL_IDENTITY_VOLUME`] is mounted.
pub const TUNNEL_IDENTITY_MOUNT_PATH: &str = "/run/podidentity.podcert.hivekeep.io";
/// Projected volume with the roots the worker tunnel trusts for egress.
pub const TUNNEL_EGRESS_TRUST_VOLUME: &str = "tunnel-egress-trust";
/// Where [`TUNNEL_EGRESS_TRUST_VOLUME`] is mounted.
pub const TUNNEL_EGRESS_TRUST_MOUNT_PATH: &str = "/run/servicedns.podcert.hivekeep.io";
/// Downward-API volume with the worker container's own limits.
pub const WORKER_CAPACITY_VOLUME: &str = "worker-capacity";
/// Host-path volume of [`NODE_BASE_PATH`].
pub const NODE_BASE_VOLUME: &str = "hivekeep-base";
/// The tun device a micro-VM worker bind-mounts to build the guest's tap.
pub const TUN_DEVICE_VOLUME: &str = "dev-net-tun";
/// See [`TUN_DEVICE_VOLUME`].
pub const TUN_DEVICE_PATH: &str = "/dev/net/tun";

/// Capabilities an unprivileged gVisor worker needs: runsc's gofer maps a
/// full-range identity in a user namespace (SETUID/SETGID/SETPCAP/SETFCAP),
/// the sandbox pivots root and traces the application
/// (SYS_ADMIN/SYS_CHROOT/SYS_PTRACE), actor networking programs veths and
/// nftables (NET_ADMIN/NET_RAW), and OCI root filesystems are unpacked and
/// device nodes created over image-owned trees
/// (DAC_OVERRIDE/FOWNER/CHOWN/MKNOD).
pub const WORKER_GVISOR_CAPABILITIES: &[&str] = &[
    "NET_ADMIN",
    "SYS_ADMIN",
    "SYS_CHROOT",
    "SYS_PTRACE",
    "SETUID",
    "SETGID",
    "SETPCAP",
    "DAC_OVERRIDE",
    "FOWNER",
    "CHOWN",
    "MKNOD",
    "NET_RAW",
    "SETFCAP",
];

/// Capabilities an unprivileged micro-VM worker needs: the gVisor set plus
/// FSETID and DAC_READ_SEARCH, which virtiofsd re-applies to its sandboxed
/// child (the kernel refuses to raise a capability the bounding set
/// omits). The hypervisor devices come from the device plugin instead.
pub fn worker_microvm_capabilities() -> Vec<&'static str> {
    let mut caps = WORKER_GVISOR_CAPABILITIES.to_vec();
    caps.extend(["FSETID", "DAC_READ_SEARCH"]);
    caps
}

/// Divisors of the projected limits: the downward API reports
/// ceil(limit/divisor), and the default divisor of one core would round a
/// fractional CPU limit up.
const MILLI_CORES: &str = "1m";
const WHOLE_BYTES: &str = "1";

/// Everything besides the pool that shapes its worker pods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerPodSettings {
    /// Telemetry configuration for the worker.
    pub otel: WorkerOtelSettings,
    /// The namespace the control plane runs in.
    pub system_namespace: String,
    /// The keeper's ServiceAccount.
    pub keeper_service_account: String,
    /// The gate router's ServiceAccount.
    pub router_service_account: String,
    /// The worker's `--dns-egress-policy`.
    pub dns_egress_policy: String,
    /// The preview gates the worker gets.
    pub preview: PreviewGates,
}

/// The class the pool's workers run; the API default without one.
fn pool_class(wp: &WorkerPool) -> SandboxClass {
    wp.spec.default_sandbox_class().unwrap_or_default()
}

/// The worker Deployment of `wp`.
pub fn build_deployment(wp: &WorkerPool, settings: &WorkerPodSettings) -> Deployment {
    let name = wp.metadata.name.clone().unwrap_or_default();
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    let mut annotations: BTreeMap<String, String> = BTreeMap::new();
    if let Some(t) = &wp.spec.template {
        labels.extend(t.labels.clone());
        annotations.extend(t.annotations.clone());
    }
    labels.insert(WORKER_POOL_LABEL.into(), name.clone());
    let annotations = (!annotations.is_empty()).then_some(annotations);

    let class = pool_class(wp);
    let mut container = worker_container(wp, settings, class);
    let mut pod = PodSpec {
        security_context: Some(PodSecurityContext {
            run_as_user: Some(0),
            run_as_group: Some(0),
            ..Default::default()
        }),
        volumes: Some(worker_volumes()),
        ..Default::default()
    };
    apply_pod_template(&mut pod, &mut container, wp.spec.template.as_ref());
    apply_sandbox_class_toleration(&mut pod, class);
    apply_microvm_pod_shape(&mut pod, &mut container, class);
    pod.containers = vec![container];
    pod.termination_grace_period_seconds = Some(WORKER_TERMINATION_GRACE_PERIOD_SECONDS);

    Deployment {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: wp.metadata.namespace.clone(),
            labels: Some(labels.clone()),
            annotations: annotations.clone(),
            owner_references: Some(vec![owner_reference(wp)]),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(wp.spec.replicas),
            strategy: Some(DeploymentStrategy {
                type_: Some("RollingUpdate".into()),
                rolling_update: Some(RollingUpdateDeployment {
                    max_surge: Some(IntOrString::Int(WORKER_ROLLOUT_MAX_SURGE)),
                    max_unavailable: Some(IntOrString::String(
                        WORKER_ROLLOUT_MAX_UNAVAILABLE.into(),
                    )),
                }),
            }),
            progress_deadline_seconds: Some(WORKER_ROLLOUT_PROGRESS_DEADLINE_SECONDS),
            selector: LabelSelector {
                match_labels: Some(label(WORKER_POOL_LABEL, &name)),
                match_expressions: None,
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    annotations,
                    ..Default::default()
                }),
                spec: Some(pod),
            },
            ..Default::default()
        }),
        status: None,
    }
}

/// The controller reference a WorkerPool puts on what it owns.
pub fn owner_reference(wp: &WorkerPool) -> OwnerReference {
    OwnerReference {
        api_version: API_VERSION.into(),
        kind: "WorkerPool".into(),
        name: wp.metadata.name.clone().unwrap_or_default(),
        uid: wp.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// The worker container's flags. The tunnel authenticates peers that run
/// in the control plane's namespace, so the controller passes their
/// identities rather than letting the worker assume the default install.
pub fn worker_args(settings: &WorkerPodSettings) -> Vec<String> {
    let mut args = vec![
        "--pod-uid=$(POD_UID)".to_string(),
        "--pod-ips=$(POD_IPS)".to_string(),
        "--tunnel-listen-address=:443".to_string(),
        "--tunnel-connect-listen-address=:8443".to_string(),
        format!("--tunnel-credential-bundle={TUNNEL_IDENTITY_MOUNT_PATH}/credential-bundle.pem"),
        format!("--tunnel-trust-bundle={TUNNEL_IDENTITY_MOUNT_PATH}/trust-bundle.pem"),
        format!(
            "--tunnel-client-identity={}",
            install_defaults::spiffe_id(
                &settings.system_namespace,
                &settings.router_service_account
            )
        ),
    ];
    // A worker without --tunnel-broker-identity assumes the canonical keeper
    // identity, so the flag is only passed when the install differs; an
    // older worker pinned by a pool would exit on the unknown flag.
    let broker =
        install_defaults::spiffe_id(&settings.system_namespace, &settings.keeper_service_account);
    if broker != install_defaults::keeper_spiffe_id(install_defaults::SYSTEM_NAMESPACE) {
        args.push(format!("--tunnel-broker-identity={broker}"));
    }
    args.push("--tunnel-egress-listen-address=0.0.0.0:15001".into());
    args.push(format!(
        "--tunnel-egress-trust-bundle={TUNNEL_EGRESS_TRUST_MOUNT_PATH}/trust-bundle.pem"
    ));
    args.push(format!(
        "--dns-egress-policy={}",
        settings.dns_egress_policy
    ));
    args.push(preview_arg(&settings.preview));
    args
}

fn port(name: &str, port: i32) -> ContainerPort {
    ContainerPort {
        name: Some(name.into()),
        container_port: port,
        protocol: Some("TCP".into()),
        ..Default::default()
    }
}

fn mount(name: &str, path: &str) -> VolumeMount {
    VolumeMount {
        name: name.into(),
        mount_path: path.into(),
        ..Default::default()
    }
}

fn read_only_mount(name: &str, path: &str) -> VolumeMount {
    VolumeMount {
        read_only: Some(true),
        ..mount(name, path)
    }
}

fn worker_container(
    wp: &WorkerPool,
    settings: &WorkerPodSettings,
    class: SandboxClass,
) -> Container {
    Container {
        name: "worker".into(),
        image: Some(wp.spec.worker_image.clone()),
        args: Some(worker_args(settings)),
        ports: Some(vec![
            port("https", 443),
            port("connect", 8443),
            port("readyz", 8080),
        ]),
        readiness_probe: Some(Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/readyz".into()),
                port: IntOrString::String("readyz".into()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        security_context: Some(worker_security_context(class)),
        env: Some(worker_container_env(&settings.otel)),
        volume_mounts: Some(vec![
            read_only_mount(WORKER_CAPACITY_VOLUME, WORKER_CAPACITY_MOUNT_PATH),
            VolumeMount {
                mount_propagation: Some("HostToContainer".into()),
                ..mount(NODE_BASE_VOLUME, NODE_BASE_PATH)
            },
            read_only_mount(TUNNEL_IDENTITY_VOLUME, TUNNEL_IDENTITY_MOUNT_PATH),
            read_only_mount(TUNNEL_EGRESS_TRUST_VOLUME, TUNNEL_EGRESS_TRUST_MOUNT_PATH),
        ]),
        ..Default::default()
    }
}

/// Projects the worker container's `resource` limit to `path` in units of
/// `divisor`.
fn resource_field_ref_file(path: &str, resource: &str, divisor: &str) -> DownwardAPIVolumeFile {
    DownwardAPIVolumeFile {
        path: path.into(),
        resource_field_ref: Some(ResourceFieldSelector {
            container_name: Some("worker".into()),
            resource: resource.into(),
            divisor: Some(Quantity(divisor.into())),
        }),
        ..Default::default()
    }
}

fn live_trust_bundle(signer: &str) -> VolumeProjection {
    VolumeProjection {
        cluster_trust_bundle: Some(ClusterTrustBundleProjection {
            signer_name: Some(signer.into()),
            label_selector: Some(LabelSelector {
                match_labels: Some(label(CANARYING_LABEL, "live")),
                match_expressions: None,
            }),
            path: "trust-bundle.pem".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The pod volumes every worker has, in order.
pub fn worker_volumes() -> Vec<Volume> {
    vec![
        Volume {
            name: WORKER_CAPACITY_VOLUME.into(),
            downward_api: Some(DownwardAPIVolumeSource {
                items: Some(vec![
                    resource_field_ref_file(WORKER_CAPACITY_CPU_FILE, "limits.cpu", MILLI_CORES),
                    resource_field_ref_file(
                        WORKER_CAPACITY_MEMORY_FILE,
                        "limits.memory",
                        WHOLE_BYTES,
                    ),
                ]),
                ..Default::default()
            }),
            ..Default::default()
        },
        Volume {
            name: NODE_BASE_VOLUME.into(),
            host_path: Some(HostPathVolumeSource {
                path: NODE_BASE_PATH.into(),
                type_: Some("DirectoryOrCreate".into()),
            }),
            ..Default::default()
        },
        Volume {
            name: TUNNEL_IDENTITY_VOLUME.into(),
            projected: Some(ProjectedVolumeSource {
                sources: Some(vec![
                    VolumeProjection {
                        pod_certificate: Some(PodCertificateProjection {
                            signer_name: POD_IDENTITY_SIGNER.into(),
                            key_type: "ECDSAP256".into(),
                            credential_bundle_path: Some("credential-bundle.pem".into()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    live_trust_bundle(POD_IDENTITY_SIGNER),
                ]),
                ..Default::default()
            }),
            ..Default::default()
        },
        Volume {
            name: TUNNEL_EGRESS_TRUST_VOLUME.into(),
            projected: Some(ProjectedVolumeSource {
                sources: Some(vec![live_trust_bundle(SERVICE_DNS_SIGNER)]),
                ..Default::default()
            }),
            ..Default::default()
        },
    ]
}

/// The worker container's security context. No class runs privileged;
/// they differ in their capability set. Both runtimes mount inside the
/// worker, which the default AppArmor profile denies, and both sandbox
/// children `pivot_root()`, which the default seccomp profile denies, so
/// both are declared Unconfined explicitly rather than relying on the
/// cluster default.
pub fn worker_security_context(class: SandboxClass) -> SecurityContext {
    let caps: Vec<String> = match class {
        SandboxClass::Microvm => worker_microvm_capabilities(),
        SandboxClass::Gvisor => WORKER_GVISOR_CAPABILITIES.to_vec(),
    }
    .into_iter()
    .map(String::from)
    .collect();
    SecurityContext {
        run_as_user: Some(0),
        run_as_group: Some(0),
        privileged: Some(false),
        app_armor_profile: Some(AppArmorProfile {
            type_: "Unconfined".into(),
            localhost_profile: None,
        }),
        capabilities: Some(Capabilities {
            drop: Some(vec!["ALL".into()]),
            add: Some(caps),
        }),
        seccomp_profile: Some(SeccompProfile {
            type_: "Unconfined".into(),
            localhost_profile: None,
        }),
        ..Default::default()
    }
}

/// Lets worker pods onto nodes tainted for the pool's own sandbox class,
/// on top of the template's tolerations.
pub fn apply_sandbox_class_toleration(pod: &mut PodSpec, class: SandboxClass) {
    pod.tolerations
        .get_or_insert_with(Vec::new)
        .push(Toleration {
            key: Some(SANDBOX_CLASS_TAINT_KEY.into()),
            operator: Some("Equal".into()),
            value: Some(class.as_str().into()),
            effect: Some("NoSchedule".into()),
            toleration_seconds: None,
        });
}

/// Adds what a micro-VM worker needs on top of the template: `/dev/kvm`
/// as an extended resource from the keeper's device plugin (a host-path
/// mount carries no cgroup device rule, and the request also keeps the pod
/// off nodes without KVM), and a bind mount of `/dev/net/tun`, which the
/// runtime's default device allow-list already admits. No-op for other
/// classes.
pub fn apply_microvm_pod_shape(pod: &mut PodSpec, container: &mut Container, class: SandboxClass) {
    if class != SandboxClass::Microvm {
        return;
    }
    add_device_resource_limits(container, &[RESOURCE_KVM]);
    container
        .volume_mounts
        .get_or_insert_with(Vec::new)
        .push(mount(TUN_DEVICE_VOLUME, TUN_DEVICE_PATH));
    pod.volumes.get_or_insert_with(Vec::new).push(Volume {
        name: TUN_DEVICE_VOLUME.into(),
        host_path: Some(HostPathVolumeSource {
            path: TUN_DEVICE_PATH.into(),
            type_: Some("CharDevice".into()),
        }),
        ..Default::default()
    });
}

/// Requests one unit of each extended resource, merged into the limits the
/// template set.
fn add_device_resource_limits(container: &mut Container, names: &[&str]) {
    let resources = container
        .resources
        .get_or_insert_with(ResourceRequirements::default);
    let limits = resources.limits.get_or_insert_with(BTreeMap::new);
    for name in names {
        limits.insert((*name).into(), Quantity("1".into()));
    }
}

/// Applies the pool's pod template. Without one, the fields it can set are
/// still declared (empty), so the controller keeps owning them and a
/// cleared template clears the Deployment.
pub fn apply_pod_template(
    pod: &mut PodSpec,
    container: &mut Container,
    tmpl: Option<&WorkerPoolPodTemplate>,
) {
    pod.node_selector = None;
    pod.tolerations = None;
    pod.priority_class_name = Some(String::new());
    pod.service_account_name = Some(WORKER_SERVICE_ACCOUNT.into());
    pod.affinity = Some(Affinity::default());
    let mut resources = ResourceRequirements::default();

    if let Some(t) = tmpl {
        if !t.node_selector.is_empty() {
            pod.node_selector = Some(t.node_selector.clone());
        }
        if !t.tolerations.is_empty() {
            pod.tolerations = Some(t.tolerations.iter().map(normalize_toleration).collect());
        }
        pod.priority_class_name = Some(t.priority_class_name.clone());
        if let Some(sa) = &t.service_account_name {
            pod.service_account_name = Some(sa.clone());
        }
        if let Some(na) = &t.node_affinity {
            pod.affinity = Some(Affinity {
                node_affinity: Some(normalize_node_affinity(na)),
                ..Default::default()
            });
        }
        if let Some(r) = &t.resources {
            resources.requests = r.requests.clone();
            resources.limits = r.limits.clone();
        }
    }
    container.resources = Some(resources);
}

fn non_empty(s: &Option<String>) -> Option<String> {
    s.clone().filter(|s| !s.is_empty())
}

/// A toleration with its empty fields left out.
fn normalize_toleration(t: &Toleration) -> Toleration {
    Toleration {
        key: non_empty(&t.key),
        operator: non_empty(&t.operator),
        value: non_empty(&t.value),
        effect: non_empty(&t.effect),
        toleration_seconds: t.toleration_seconds,
    }
}

fn normalize_requirement(r: &NodeSelectorRequirement) -> NodeSelectorRequirement {
    NodeSelectorRequirement {
        key: r.key.clone(),
        operator: r.operator.clone(),
        values: r.values.clone().filter(|v| !v.is_empty()),
    }
}

fn normalize_term(t: &NodeSelectorTerm) -> NodeSelectorTerm {
    let list = |v: &Option<Vec<NodeSelectorRequirement>>| {
        v.as_ref()
            .filter(|v| !v.is_empty())
            .map(|v| v.iter().map(normalize_requirement).collect())
    };
    NodeSelectorTerm {
        match_expressions: list(&t.match_expressions),
        match_fields: list(&t.match_fields),
    }
}

/// A node affinity with its empty lists left out.
fn normalize_node_affinity(na: &NodeAffinity) -> NodeAffinity {
    NodeAffinity {
        required_during_scheduling_ignored_during_execution: na
            .required_during_scheduling_ignored_during_execution
            .as_ref()
            .map(|ns| NodeSelector {
                node_selector_terms: ns.node_selector_terms.iter().map(normalize_term).collect(),
            }),
        preferred_during_scheduling_ignored_during_execution: na
            .preferred_during_scheduling_ignored_during_execution
            .as_ref()
            .filter(|p| !p.is_empty())
            .map(|p| {
                p.iter()
                    .map(|t| PreferredSchedulingTerm {
                        weight: t.weight,
                        preference: normalize_term(&t.preference),
                    })
                    .collect()
            }),
    }
}

#[cfg(test)]
#[path = "workerpool_apply_tests.rs"]
mod tests;

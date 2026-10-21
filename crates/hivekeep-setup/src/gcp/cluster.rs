//! The GKE cluster: the spec of a new one, reconciling an existing one, and
//! the node pool checks that cannot be fixed in place.

use super::config::{GcpConfig, NODE_POOL_NAME};
use super::runner::{Gcloud, Runner};
use super::GcpError;
use crate::{info, warn};
use serde::Deserialize;
use std::collections::BTreeMap;

/// Beta APIs the pod identity machinery needs.
pub const REQUIRED_BETA_APIS: &[&str] = &[
    "certificates.k8s.io/v1beta1/podcertificaterequests",
    "certificates.k8s.io/v1beta1/clustertrustbundles",
];

/// Node sysctls for high actor density.
pub const REQUIRED_NODE_SYSCTLS: &[(&str, &str)] = &[
    ("net.ipv4.neigh.default.gc_thresh1", "4096"),
    ("net.ipv4.neigh.default.gc_thresh2", "16384"),
    ("net.ipv4.neigh.default.gc_thresh3", "32768"),
    ("fs.inotify.max_user_instances", "65536"),
    ("fs.inotify.max_user_watches", "1048576"),
];

/// Managed OpenTelemetry scope new and reconciled clusters get.
pub const MANAGED_OTEL_SCOPE: &str = "COLLECTION_AND_INSTRUMENTATION_COMPONENTS";

/// First Kubernetes 1.x minor whose kubelet serves pod certificate
/// projection without a feature gate.
pub const POD_CERTIFICATE_PROJECTION_GA_MINOR: u32 = 37;

pub fn required_node_sysctls() -> BTreeMap<String, String> {
    REQUIRED_NODE_SYSCTLS
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdvancedMachineFeatures {
    #[serde(default)]
    pub enable_nested_virtualization: Option<bool>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinuxNodeConfig {
    #[serde(default)]
    pub sysctls: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeConfig {
    #[serde(default)]
    pub machine_type: String,
    #[serde(default)]
    pub disk_size_gb: i32,
    #[serde(default)]
    pub disk_type: String,
    #[serde(default)]
    pub linux_node_config: Option<LinuxNodeConfig>,
    #[serde(default)]
    pub advanced_machine_features: Option<AdvancedMachineFeatures>,
}

impl NodeConfig {
    pub fn nested_virtualization(&self) -> bool {
        self.advanced_machine_features
            .as_ref()
            .and_then(|a| a.enable_nested_virtualization)
            .unwrap_or(false)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodePool {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub initial_node_count: i32,
    #[serde(default)]
    pub config: Option<NodeConfig>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkConfig {
    #[serde(default)]
    pub network: String,
    #[serde(default)]
    pub subnetwork: String,
    #[serde(default)]
    pub datapath_provider: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadIdentityConfig {
    #[serde(default)]
    pub workload_pool: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BetaApis {
    #[serde(default)]
    pub enabled_apis: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct OtelConfig {
    #[serde(default)]
    pub scope: String,
}

/// An existing cluster, as `gcloud container clusters describe` reports it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cluster {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub node_pools: Vec<NodePool>,
    #[serde(default)]
    pub network_config: Option<NetworkConfig>,
    #[serde(default)]
    pub workload_identity_config: Option<WorkloadIdentityConfig>,
    #[serde(default, rename = "enableK8sBetaApis")]
    pub enable_k8s_beta_apis: Option<BetaApis>,
    #[serde(default)]
    pub managed_opentelemetry_config: Option<OtelConfig>,
}

/// What a new cluster is created with.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterSpec {
    pub parent: String,
    pub name: String,
    pub initial_cluster_version: String,
    pub node_pools: Vec<NodePool>,
    pub enabled_beta_apis: Vec<String>,
    pub workload_pool: String,
    pub network: String,
    pub subnetwork: String,
    pub dataplane_v2: bool,
    pub managed_otel_scope: String,
    /// The Filestore CSI add-on; off for clusters this tool creates. An
    /// existing cluster's add-on is left alone.
    pub filestore_csi_enabled: bool,
}

/// The spec of a new cluster from the flags.
pub fn build_create_cluster_request(parent: &str, cfg: &GcpConfig) -> ClusterSpec {
    let mut node_config = NodeConfig {
        machine_type: cfg.machine_type.clone(),
        linux_node_config: Some(LinuxNodeConfig {
            sysctls: required_node_sysctls(),
        }),
        ..Default::default()
    };
    // A disabled knob leaves the field unset so GKE applies its default.
    if cfg.enable_nested_virtualization {
        node_config.advanced_machine_features = Some(AdvancedMachineFeatures {
            enable_nested_virtualization: Some(true),
        });
    }
    if cfg.boot_disk_size_gb > 0 {
        node_config.disk_size_gb = cfg.boot_disk_size_gb;
    }
    if !cfg.boot_disk_type.is_empty() {
        node_config.disk_type = cfg.boot_disk_type.clone();
    }
    ClusterSpec {
        parent: parent.into(),
        name: cfg.cluster_name.clone(),
        initial_cluster_version: cfg.cluster_version.clone(),
        node_pools: vec![NodePool {
            name: NODE_POOL_NAME.into(),
            version: String::new(),
            initial_node_count: 2,
            config: Some(node_config),
        }],
        enabled_beta_apis: REQUIRED_BETA_APIS.iter().map(|s| s.to_string()).collect(),
        workload_pool: format!("{}.svc.id.goog", cfg.project_id),
        network: cfg.network.clone(),
        subnetwork: cfg.subnetwork.clone(),
        dataplane_v2: cfg.enable_dataplane_v2,
        managed_otel_scope: MANAGED_OTEL_SCOPE.into(),
        filestore_csi_enabled: false,
    }
}

/// The node sysctls as a gcloud `--system-config-from-file` document.
pub fn system_config_yaml(sysctls: &BTreeMap<String, String>) -> String {
    let mut out = String::from("linuxConfig:\n  sysctl:\n");
    for (k, v) in sysctls {
        out.push_str(&format!("    {k}: '{v}'\n"));
    }
    out
}

/// The `gcloud container clusters create` arguments for a spec, with the
/// sysctls read from `system_config_path`. gcloud names the first pool
/// `default-pool`; the pool's other settings come from the spec.
pub fn create_cluster_args(
    spec: &ClusterSpec,
    cfg: &GcpConfig,
    system_config_path: &str,
) -> Vec<String> {
    let mut a = vec![
        "container".to_string(),
        "clusters".into(),
        "create".into(),
        spec.name.clone(),
        format!("--project={}", cfg.project_id),
        format!("--location={}", cfg.cluster_location),
        format!("--workload-pool={}", spec.workload_pool),
        format!("--network={}", spec.network),
        format!("--subnetwork={}", spec.subnetwork),
        format!(
            "--enable-kubernetes-unstable-apis={}",
            spec.enabled_beta_apis.join(",")
        ),
        format!("--managed-otel-scope={}", spec.managed_otel_scope),
    ];
    if !spec.initial_cluster_version.is_empty() {
        a.push(format!(
            "--cluster-version={}",
            spec.initial_cluster_version
        ));
    }
    if spec.dataplane_v2 {
        a.push("--enable-dataplane-v2".into());
    }
    if let Some(pool) = spec.node_pools.first() {
        a.push(format!("--num-nodes={}", pool.initial_node_count));
        if let Some(c) = &pool.config {
            a.push(format!("--machine-type={}", c.machine_type));
            if c.disk_size_gb > 0 {
                a.push(format!("--disk-size={}", c.disk_size_gb));
            }
            if !c.disk_type.is_empty() {
                a.push(format!("--disk-type={}", c.disk_type));
            }
            if c.nested_virtualization() {
                a.push("--enable-nested-virtualization".into());
            }
            if c.linux_node_config.is_some() {
                a.push(format!("--system-config-from-file={system_config_path}"));
            }
        }
    }
    a
}

/// Whether any node pool exposes /dev/kvm. Any pool, not every pool: KVM
/// nodes in a second, hand-added pool are a normal arrangement.
pub fn nested_virtualization_enabled(cluster: Option<&Cluster>) -> bool {
    cluster.is_some_and(|c| {
        c.node_pools.iter().any(|p| {
            p.config
                .as_ref()
                .is_some_and(NodeConfig::nested_virtualization)
        })
    })
}

/// Whether any node pool carries every required sysctl key.
pub fn node_sysctls_configured(cluster: Option<&Cluster>) -> bool {
    let Some(cluster) = cluster else { return false };
    cluster.node_pools.iter().any(|p| {
        let sysctls = p
            .config
            .as_ref()
            .and_then(|c| c.linux_node_config.as_ref())
            .map(|l| &l.sysctls);
        match sysctls {
            Some(s) if !s.is_empty() => REQUIRED_NODE_SYSCTLS
                .iter()
                .all(|(k, _)| s.contains_key(*k)),
            _ => false,
        }
    })
}

/// The minor of a GKE version such as `1.36.4-gke.1247000`; only 1.x.
pub fn kubernetes_minor(version: &str) -> Option<u32> {
    let (major, rest) = version.split_once('.')?;
    if major != "1" {
        return None;
    }
    let minor = rest.split('.').next().unwrap_or(rest);
    minor.parse().ok()
}

/// The pools whose nodes cannot mount pod certificate volumes after the beta
/// APIs are turned on in place: projection is a kubelet feature, and a
/// kubelet already running when the APIs appeared never picks it up. The
/// pool's version decides, and an unreadable one is reported, since a
/// spurious warning costs far less than a silent mount failure.
pub fn node_pools_without_pod_certificate_projection(cluster: &Cluster) -> Vec<&NodePool> {
    cluster
        .node_pools
        .iter()
        .filter(|p| {
            !kubernetes_minor(&p.version).is_some_and(|m| m >= POD_CERTIFICATE_PROJECTION_GA_MINOR)
        })
        .collect()
}

/// A name for the pool replacing `name`. GKE caps pool names at 40
/// characters.
pub fn replacement_node_pool_name(name: &str) -> String {
    const SUFFIX: &str = "-2";
    let mut base = name.to_string();
    if base.len() + SUFFIX.len() > 40 {
        base = base[..40 - SUFFIX.len()].trim_end_matches('-').to_string();
    }
    base + SUFFIX
}

/// The command that adds a pool to take over from one whose nodes predate
/// the beta APIs. A new pool is the one operation certain to produce new
/// kubelets. Anything else customised on the old pool (taints, service
/// account, autoscaling) has to be carried over by hand.
pub fn replacement_node_pool_command(cfg: &GcpConfig, pool: &NodePool) -> String {
    let nc = pool.config.clone().unwrap_or_default();
    let mut a = vec![
        format!(
            "gcloud container node-pools create {}",
            replacement_node_pool_name(&pool.name)
        ),
        format!("--cluster={}", cfg.cluster_name),
        format!("--project={}", cfg.project_id),
        format!("--location={}", cfg.cluster_location),
        format!("--node-version={}", pool.version),
        format!("--machine-type={}", nc.machine_type),
        format!("--num-nodes={}", pool.initial_node_count.max(1)),
    ];
    if nc.disk_size_gb > 0 {
        a.push(format!("--disk-size={}", nc.disk_size_gb));
    }
    if !nc.disk_type.is_empty() {
        a.push(format!("--disk-type={}", nc.disk_type));
    }
    if nc.nested_virtualization() {
        a.push("--enable-nested-virtualization".into());
    }
    a.join(" ")
}

/// The command that removes the old pool once its workloads have moved.
pub fn delete_node_pool_command(cfg: &GcpConfig, pool: &NodePool) -> String {
    format!(
        "gcloud container node-pools delete {} --cluster={} --project={} --location={}",
        pool.name, cfg.cluster_name, cfg.project_id, cfg.cluster_location
    )
}

/// Fails on a negative boot disk size.
pub fn validate_boot_disk(cfg: &GcpConfig) -> Result<(), GcpError> {
    if cfg.boot_disk_size_gb < 0 {
        return Err(GcpError::Usage(format!(
            "boot disk size {} is invalid: must be greater than or equal to 0",
            cfg.boot_disk_size_gb
        )));
    }
    Ok(())
}

/// Requires a region and a cluster location that is that region or a zone
/// in it.
pub fn validate_cluster_location(cfg: &mut GcpConfig) -> Result<(), GcpError> {
    cfg.region = cfg.region.trim().to_string();
    if cfg.region.is_empty() {
        return Err(GcpError::Usage("--region is required".into()));
    }
    cfg.cluster_location = cfg.cluster_location.trim().to_string();
    if cfg.cluster_location.is_empty() {
        return Err(GcpError::Usage("--cluster-location is required".into()));
    }
    if cfg.cluster_location != cfg.region
        && !cfg
            .cluster_location
            .starts_with(&format!("{}-", cfg.region))
    {
        return Err(GcpError::Usage(format!(
            "cluster location {:?} is not compatible with region {:?}: cluster location must be the region itself (for regional clusters) or a zone within that region (e.g. {}-c)",
            cfg.cluster_location, cfg.region, cfg.region
        )));
    }
    Ok(())
}

fn cluster_ref_args(verb: &str, cfg: &GcpConfig) -> Vec<String> {
    vec![
        "container".into(),
        "clusters".into(),
        verb.into(),
        cfg.cluster_name.clone(),
        format!("--project={}", cfg.project_id),
        format!("--location={}", cfg.cluster_location),
    ]
}

fn update_cluster(
    runner: &dyn Runner,
    cfg: &GcpConfig,
    what: &str,
    flag: String,
) -> Result<(), GcpError> {
    let mut a = cluster_ref_args("update", cfg);
    a.push(flag);
    a.push("--quiet".into());
    Gcloud { runner }.run(&format!("update cluster {what}"), &a)?;
    Ok(())
}

fn create_cluster(runner: &dyn Runner, cfg: &GcpConfig) -> Result<(), GcpError> {
    info!("Cluster {} does not exist. Creating...", cfg.cluster_name);
    let parent = format!(
        "projects/{}/locations/{}",
        cfg.project_id, cfg.cluster_location
    );
    let spec = build_create_cluster_request(&parent, cfg);
    let sysctls = spec.node_pools[0]
        .config
        .as_ref()
        .and_then(|c| c.linux_node_config.clone())
        .unwrap_or_default();
    let file = tempfile::Builder::new()
        .prefix("hivekeep-node-system-config-")
        .suffix(".yaml")
        .tempfile()
        .map_err(|e| GcpError::Other(format!("stage node system config: {e}")))?;
    std::fs::write(file.path(), system_config_yaml(&sysctls.sysctls))
        .map_err(|e| GcpError::Other(format!("stage node system config: {e}")))?;
    let a = create_cluster_args(&spec, cfg, &file.path().display().to_string());
    Gcloud { runner }.run("create cluster", &a)?;
    Ok(())
}

fn delete_cluster(runner: &dyn Runner, cfg: &GcpConfig) -> Result<(), GcpError> {
    info!("Deleting cluster {}", cfg.cluster_name);
    let mut a = cluster_ref_args("delete", cfg);
    a.push("--quiet".into());
    Gcloud { runner }.run("delete cluster", &a)?;
    Ok(())
}

fn recreate(runner: &dyn Runner, cfg: &GcpConfig) -> Result<(), GcpError> {
    delete_cluster(runner, cfg)?;
    create_cluster(runner, cfg)
}

/// Creates the cluster, or reconciles an existing one: a network, subnet or
/// dataplane mismatch recreates it; workload identity, the beta APIs and
/// managed OpenTelemetry are updated in place; settings fixed at node
/// creation are reported rather than silently left unapplied.
pub fn create_cluster_idempotent(runner: &dyn Runner, cfg: &mut GcpConfig) -> Result<(), GcpError> {
    validate_cluster_location(cfg)?;
    validate_boot_disk(cfg)?;
    let cfg = &*cfg;
    info!(
        "Checking if cluster {} exists in {}",
        cfg.cluster_name, cfg.cluster_location
    );
    let mut a = cluster_ref_args("describe", cfg);
    a.push("--format=json".into());
    let out = Gcloud { runner }.raw(&a)?;
    if !out.success {
        if !out.is_not_found() {
            return Err(GcpError::Other(format!(
                "getting cluster: {}",
                out.stderr.trim()
            )));
        }
        return create_cluster(runner, cfg);
    }
    let cluster: Cluster = serde_json::from_str(&out.stdout)
        .map_err(|e| GcpError::Other(format!("parse cluster: {e}")))?;
    info!(
        "Cluster {} exists. Checking attributes...",
        cfg.cluster_name
    );

    let net = cluster.network_config.clone().unwrap_or_default();
    let expected_network = format!(
        "projects/{}/global/networks/{}",
        cfg.project_id, cfg.network
    );
    if cluster.network_config.is_some()
        && !net.network.is_empty()
        && !net.network.ends_with(&expected_network)
    {
        info!(
            "Mismatch in network: current {}, expected {expected_network}",
            net.network
        );
        return recreate(runner, cfg);
    }
    let expected_subnetwork = format!(
        "projects/{}/regions/{}/subnetworks/{}",
        cfg.project_id, cfg.region, cfg.subnetwork
    );
    if cluster.network_config.is_some()
        && !net.subnetwork.is_empty()
        && !net.subnetwork.ends_with(&expected_subnetwork)
    {
        info!(
            "Mismatch in subnetwork: current {}, expected {expected_subnetwork}",
            net.subnetwork
        );
        return recreate(runner, cfg);
    }
    let current_v2 = net.datapath_provider == "ADVANCED_DATAPATH";
    if current_v2 != cfg.enable_dataplane_v2 {
        info!(
            "Mismatch in Dataplane V2 configuration: current {current_v2}, expected {}",
            cfg.enable_dataplane_v2
        );
        return recreate(runner, cfg);
    }

    let expected_pool = format!("{}.svc.id.goog", cfg.project_id);
    let current_pool = cluster
        .workload_identity_config
        .as_ref()
        .map(|w| w.workload_pool.clone())
        .unwrap_or_default();
    if current_pool != expected_pool {
        info!("Mismatch in workload pool: current {current_pool:?}, expected {expected_pool}; updating...");
        update_cluster(
            runner,
            cfg,
            "workload identity",
            format!("--workload-pool={expected_pool}"),
        )?;
    } else {
        info!("Cluster workload identity configuration matches.");
    }

    let enabled = cluster
        .enable_k8s_beta_apis
        .as_ref()
        .map(|b| b.enabled_apis.clone())
        .unwrap_or_default();
    if enabled.is_empty()
        || !REQUIRED_BETA_APIS
            .iter()
            .all(|a| enabled.iter().any(|e| e == a))
    {
        info!(
            "Mismatch in beta APIs: current {:?}, expected {:?}",
            enabled, REQUIRED_BETA_APIS
        );
        let mut combined: Vec<String> = Vec::new();
        for api in REQUIRED_BETA_APIS
            .iter()
            .map(|s| s.to_string())
            .chain(enabled)
        {
            if !combined.contains(&api) {
                combined.push(api);
            }
        }
        update_cluster(
            runner,
            cfg,
            "beta apis",
            format!("--enable-kubernetes-unstable-apis={}", combined.join(",")),
        )?;
        warn_node_pools_without_pod_certificate_projection(cfg, &cluster);
    } else {
        info!("Cluster beta APIs match.");
    }

    let scope = cluster
        .managed_opentelemetry_config
        .as_ref()
        .map(|o| o.scope.clone())
        .unwrap_or_default();
    if scope != MANAGED_OTEL_SCOPE {
        info!("Mismatch in managed OpenTelemetry scope: current {scope:?}, expected {MANAGED_OTEL_SCOPE}; updating...");
        update_cluster(
            runner,
            cfg,
            "managed opentelemetry",
            format!("--managed-otel-scope={MANAGED_OTEL_SCOPE}"),
        )?;
    } else {
        info!("Cluster managed OpenTelemetry configuration matches.");
    }

    // Fixed at node creation: report the drift rather than let the flag look
    // applied.
    if cfg.enable_nested_virtualization && !nested_virtualization_enabled(Some(&cluster)) {
        warn!(
            "cluster {} has no node pool with nested virtualization; --enable-nested-virtualization only applies to a \
             cluster this tool creates. Recreate the cluster, or add a node pool with nested virtualization",
            cfg.cluster_name
        );
    }
    if !node_sysctls_configured(Some(&cluster)) {
        warn!(
            "cluster {} has no node pool with high-density node sysctls; create cluster only sets them on a cluster \
             it creates. Run gcloud container node-pools update --system-config-from-file",
            cfg.cluster_name
        );
    }
    Ok(())
}

/// Warns about each pool that needs replacing after the beta APIs were
/// turned on in place. Replacing moves workloads, so it is left to the
/// operator.
pub fn warn_node_pools_without_pod_certificate_projection(cfg: &GcpConfig, cluster: &Cluster) {
    for pool in node_pools_without_pod_certificate_projection(cluster) {
        warn!(
            "node pool {} (version {:?}) of cluster {} predates the beta APIs and its nodes cannot mount pod \
             certificate volumes; replace it with a new pool, copying over any settings of its own such as taints, \
             service account, or autoscaling:\n  {}\nthen:\n  {}",
            pool.name,
            pool.version,
            cfg.cluster_name,
            replacement_node_pool_command(cfg, pool),
            delete_node_pool_command(cfg, pool)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(machine: &str) -> GcpConfig {
        GcpConfig {
            project_id: "test-project".into(),
            cluster_name: "test-cluster".into(),
            cluster_location: "us-west1-c".into(),
            machine_type: machine.into(),
            ..Default::default()
        }
    }

    const PARENT: &str = "projects/test-project/locations/us-west1-c";

    #[test]
    fn build_create_cluster_request_filestore_disabled() {
        let spec = build_create_cluster_request(PARENT, &base("c3-standard-4"));
        assert!(!spec.filestore_csi_enabled);
        assert_eq!(spec.parent, PARENT);
    }

    #[test]
    fn build_create_cluster_request_node_config() {
        let custom = GcpConfig {
            boot_disk_size_gb: 500,
            boot_disk_type: "hyperdisk-balanced".into(),
            ..base("c3-standard-8")
        };
        for (name, cfg, want_size, want_type) in [
            (
                "custom disk size and type",
                custom,
                500,
                "hyperdisk-balanced",
            ),
            ("unset disk size and type", base("c3-standard-4"), 0, ""),
        ] {
            let spec = build_create_cluster_request(PARENT, &cfg);
            let nc = spec.node_pools[0].config.clone().expect("node config");
            assert_eq!(nc.machine_type, cfg.machine_type, "{name}");
            assert_eq!(nc.disk_size_gb, want_size, "{name}");
            assert_eq!(nc.disk_type, want_type, "{name}");
            let sysctls = nc.linux_node_config.expect("linux config").sysctls;
            let mut parsed = BTreeMap::new();
            for (k, _) in REQUIRED_NODE_SYSCTLS {
                let v: i64 = sysctls
                    .get(*k)
                    .unwrap_or_else(|| panic!("missing {k}"))
                    .parse()
                    .unwrap();
                assert!(v > 0, "{k}");
                parsed.insert(*k, v);
            }
            let (t1, t2, t3) = (
                parsed["net.ipv4.neigh.default.gc_thresh1"],
                parsed["net.ipv4.neigh.default.gc_thresh2"],
                parsed["net.ipv4.neigh.default.gc_thresh3"],
            );
            assert!(1024 < t1 && t1 < t2 && t2 < t3, "{name}");
            assert!(parsed["fs.inotify.max_user_watches"] <= 1048576, "{name}");
        }
    }

    #[test]
    fn build_create_cluster_request_nested_virtualization() {
        let enabled = GcpConfig {
            enable_nested_virtualization: true,
            ..base("n2-standard-8")
        };
        for (name, cfg, want) in [
            ("enabled", enabled, true),
            ("disabled", base("c3-standard-4"), false),
        ] {
            let nc = build_create_cluster_request(PARENT, &cfg).node_pools[0]
                .config
                .clone()
                .unwrap();
            assert_eq!(nc.nested_virtualization(), want, "{name}");
            if !want {
                assert!(
                    nc.advanced_machine_features.is_none(),
                    "{name}: field must stay unset"
                );
            }
        }
    }

    fn pool_nv(enabled: Option<bool>) -> NodePool {
        NodePool {
            config: Some(NodeConfig {
                advanced_machine_features: Some(AdvancedMachineFeatures {
                    enable_nested_virtualization: enabled,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn nested_virtualization_enabled_cases() {
        let c = |pools: Vec<NodePool>| Cluster {
            node_pools: pools,
            ..Default::default()
        };
        let cases = [
            ("nil cluster", None, false),
            ("no node pools", Some(c(vec![])), false),
            (
                "pool without advanced machine features",
                Some(c(vec![NodePool {
                    config: Some(NodeConfig::default()),
                    ..Default::default()
                }])),
                false,
            ),
            (
                "single pool with nested virtualization",
                Some(c(vec![pool_nv(Some(true))])),
                true,
            ),
            (
                "second pool carries the KVM nodes",
                Some(c(vec![pool_nv(Some(false)), pool_nv(Some(true))])),
                true,
            ),
            (
                "every pool disabled",
                Some(c(vec![pool_nv(Some(false)), pool_nv(None)])),
                false,
            ),
        ];
        for (name, cluster, want) in cases {
            assert_eq!(
                nested_virtualization_enabled(cluster.as_ref()),
                want,
                "{name}"
            );
        }
    }

    #[test]
    fn node_sysctls_configured_cases() {
        let pool = |s: Option<BTreeMap<String, String>>| NodePool {
            config: Some(NodeConfig {
                linux_node_config: s.map(|sysctls| LinuxNodeConfig { sysctls }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut partial = required_node_sysctls();
        partial.remove("fs.inotify.max_user_watches");
        let c = |pools: Vec<NodePool>| Cluster {
            node_pools: pools,
            ..Default::default()
        };
        let cases = [
            ("nil cluster", None, false),
            ("no node pools", Some(c(vec![])), false),
            (
                "pool without linux node config",
                Some(c(vec![pool(None)])),
                false,
            ),
            (
                "pool missing required sysctl",
                Some(c(vec![pool(Some(partial))])),
                false,
            ),
            (
                "single pool with required sysctls",
                Some(c(vec![pool(Some(required_node_sysctls()))])),
                true,
            ),
            (
                "second pool carries required sysctls",
                Some(c(vec![
                    pool(Some(BTreeMap::new())),
                    pool(Some(required_node_sysctls())),
                ])),
                true,
            ),
        ];
        for (name, cluster, want) in cases {
            assert_eq!(node_sysctls_configured(cluster.as_ref()), want, "{name}");
        }
    }

    #[test]
    fn validate_boot_disk_cases() {
        for (name, size, want) in [
            ("valid positive boot disk size", 500, ""),
            ("zero boot disk size (unset/default)", 0, ""),
            (
                "negative boot disk size",
                -1,
                "boot disk size -1 is invalid: must be greater than or equal to 0",
            ),
        ] {
            let res = validate_boot_disk(&GcpConfig {
                boot_disk_size_gb: size,
                ..Default::default()
            });
            if want.is_empty() {
                res.unwrap_or_else(|e| panic!("{name}: {e}"));
            } else {
                assert_eq!(res.unwrap_err().to_string(), want, "{name}");
            }
        }
    }

    #[test]
    fn validate_cluster_location_cases() {
        let cases = [
            (
                "compatible default us-west1 and us-west1-c",
                "us-west1",
                "us-west1-c",
                "",
            ),
            (
                "compatible zone in region",
                "us-central1",
                "us-central1-a",
                "",
            ),
            (
                "compatible regional cluster location",
                "us-central1",
                "us-central1",
                "",
            ),
            (
                "incompatible zone and region",
                "us-central1",
                "us-west1-c",
                r#"cluster location "us-west1-c" is not compatible with region "us-central1""#,
            ),
            (
                "incompatible regions",
                "us-central1",
                "us-west1",
                r#"cluster location "us-west1" is not compatible with region "us-central1""#,
            ),
            (
                "similar prefix but different region number",
                "us-central1",
                "us-central2-c",
                r#"cluster location "us-central2-c" is not compatible with region "us-central1""#,
            ),
            (
                "missing region",
                "",
                "us-central1-c",
                "--region is required",
            ),
            (
                "missing cluster location",
                "us-central1",
                "",
                "--cluster-location is required",
            ),
        ];
        for (name, region, loc, want) in cases {
            let mut c = GcpConfig {
                region: region.into(),
                cluster_location: loc.into(),
                ..Default::default()
            };
            let res = validate_cluster_location(&mut c);
            if want.is_empty() {
                res.unwrap_or_else(|e| panic!("{name}: {e}"));
            } else {
                let err = res.unwrap_err().to_string();
                assert!(err.contains(want), "{name}: {err}");
            }
        }
    }

    #[test]
    fn node_pools_without_pod_certificate_projection_cases() {
        let p = |name: &str, v: &str| NodePool {
            name: name.into(),
            version: v.into(),
            ..Default::default()
        };
        let cases: Vec<(&str, Vec<NodePool>, Vec<&str>)> = vec![
            (
                "1.36 pool needs its nodes recreated",
                vec![p("default-pool", "1.36.4-gke.1247000")],
                vec!["default-pool"],
            ),
            (
                "1.37 pool is fine, projection is GA in its kubelet",
                vec![p("default-pool", "1.37.0-gke.3503000")],
                vec![],
            ),
            (
                "only the pools below 1.37 are reported",
                vec![
                    p("old", "1.36.4-gke.1247000"),
                    p("new", "1.37.0-gke.3503000"),
                ],
                vec!["old"],
            ),
            (
                "an unreadable version is reported rather than assumed fine",
                vec![p("mystery", "")],
                vec!["mystery"],
            ),
            ("no pools, nothing to report", vec![], vec![]),
        ];
        for (name, pools, want) in cases {
            let cluster = Cluster {
                node_pools: pools,
                ..Default::default()
            };
            let got: Vec<&str> = node_pools_without_pod_certificate_projection(&cluster)
                .iter()
                .map(|p| p.name.as_str())
                .collect();
            assert_eq!(got, want, "{name}");
        }
    }

    #[test]
    fn kubernetes_minor_cases() {
        for (v, want) in [
            ("1.36.4-gke.1247000", Some(36)),
            ("1.37", Some(37)),
            ("1.100.0", Some(100)),
            ("", None),
            ("latest", None),
            ("2.0.0", None),
            ("1.x.0", None),
        ] {
            assert_eq!(kubernetes_minor(v), want, "{v}");
        }
    }

    fn poc() -> GcpConfig {
        GcpConfig {
            project_id: "my-project".into(),
            cluster_name: "hivekeep-poc".into(),
            cluster_location: "us-west1-c".into(),
            ..Default::default()
        }
    }

    #[test]
    fn replacement_node_pool_command_cases() {
        let tool_pool = NodePool {
            name: "hivekeep-node-pool".into(),
            version: "1.36.4-gke.1247000".into(),
            initial_node_count: 2,
            config: Some(NodeConfig {
                machine_type: "c3-standard-4".into(),
                disk_size_gb: 500,
                disk_type: "pd-balanced".into(),
                advanced_machine_features: Some(AdvancedMachineFeatures {
                    enable_nested_virtualization: Some(true),
                }),
                ..Default::default()
            }),
        };
        let default_pool = NodePool {
            name: "default-pool".into(),
            version: "1.36.4-gke.1082000".into(),
            initial_node_count: 0,
            config: Some(NodeConfig {
                machine_type: "e2-standard-4".into(),
                ..Default::default()
            }),
        };
        assert_eq!(
            replacement_node_pool_command(&poc(), &tool_pool),
            "gcloud container node-pools create hivekeep-node-pool-2 --cluster=hivekeep-poc --project=my-project --location=us-west1-c --node-version=1.36.4-gke.1247000 --machine-type=c3-standard-4 --num-nodes=2 --disk-size=500 --disk-type=pd-balanced --enable-nested-virtualization"
        );
        // Autoscaled pools can report zero nodes; the replacement gets one.
        assert_eq!(
            replacement_node_pool_command(&poc(), &default_pool),
            "gcloud container node-pools create default-pool-2 --cluster=hivekeep-poc --project=my-project --location=us-west1-c --node-version=1.36.4-gke.1082000 --machine-type=e2-standard-4 --num-nodes=1"
        );
    }

    #[test]
    fn replacement_node_pool_name_fits_gke_limit() {
        let long = format!("{}-b", "a".repeat(39));
        let got = replacement_node_pool_name(&long);
        assert!(got.len() <= 40, "{got}");
        assert_ne!(got, long);
    }

    #[test]
    fn delete_node_pool_command_format() {
        let pool = NodePool {
            name: "hivekeep-node-pool".into(),
            ..Default::default()
        };
        assert_eq!(
            delete_node_pool_command(&poc(), &pool),
            "gcloud container node-pools delete hivekeep-node-pool --cluster=hivekeep-poc --project=my-project --location=us-west1-c"
        );
    }

    #[test]
    fn missing_cluster_is_created_with_the_spec() {
        let runner = crate::gcp::runner::FakeRunner::new();
        runner.respond(
            &["container", "clusters", "describe"],
            crate::gcp::runner::Output::fail(
                "ERROR: (gcloud.container.clusters.describe) NOT_FOUND",
            ),
        );
        let mut c = GcpConfig {
            region: "us-west1".into(),
            enable_dataplane_v2: true,
            enable_nested_virtualization: true,
            ..base("c3-standard-4")
        };
        create_cluster_idempotent(&runner, &mut c).unwrap();
        let create = runner
            .command_lines()
            .into_iter()
            .find(|l| l.contains("clusters create"))
            .expect("create");
        for want in [
            "--workload-pool=test-project.svc.id.goog",
            "--enable-dataplane-v2",
            "--enable-nested-virtualization",
            "--num-nodes=2",
            "--system-config-from-file=",
            "--enable-kubernetes-unstable-apis=certificates.k8s.io/v1beta1/podcertificaterequests,certificates.k8s.io/v1beta1/clustertrustbundles",
        ] {
            assert!(create.contains(want), "{want} missing from {create}");
        }
    }

    #[test]
    fn existing_cluster_gets_missing_beta_apis_merged() {
        let runner = crate::gcp::runner::FakeRunner::new();
        runner.respond(
            &["container", "clusters", "describe"],
            crate::gcp::runner::Output::ok(
                r#"{"name":"test-cluster","networkConfig":{"datapathProvider":"ADVANCED_DATAPATH"},
                    "workloadIdentityConfig":{"workloadPool":"test-project.svc.id.goog"},
                    "enableK8sBetaApis":{"enabledApis":["other/v1beta1/things"]},
                    "managedOpentelemetryConfig":{"scope":"COLLECTION_AND_INSTRUMENTATION_COMPONENTS"},
                    "nodePools":[{"name":"p","version":"1.37.1-gke.1"}]}"#,
            ),
        );
        let mut c = GcpConfig {
            region: "us-west1".into(),
            enable_dataplane_v2: true,
            ..base("c3-standard-4")
        };
        create_cluster_idempotent(&runner, &mut c).unwrap();
        let updates: Vec<String> = runner
            .command_lines()
            .into_iter()
            .filter(|l| l.contains("clusters update"))
            .collect();
        assert_eq!(updates.len(), 1, "{updates:?}");
        assert!(updates[0].contains(
            "--enable-kubernetes-unstable-apis=certificates.k8s.io/v1beta1/podcertificaterequests,certificates.k8s.io/v1beta1/clustertrustbundles,other/v1beta1/things"
        ));
    }
}

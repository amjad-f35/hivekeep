//! The contract between the controller and the worker pods it runs: pod
//! labels, mount paths, flags and environment.

use std::sync::OnceLock;

use hivekeep_core::preview::PreviewGates;
use k8s_openapi::api::core::v1::{EnvVar, EnvVarSource, ObjectFieldSelector};

/// Names a worker pod's WorkerPool. It is on the Deployment, the pod
/// template and every worker pod, and selects them.
pub const WORKER_POOL_LABEL: &str = "hivekeep.io/worker-pool";

/// Taint key a cluster puts on nodes reserved for one sandbox class, with
/// the class as value: `hivekeep.io/sandboxClass=<class>:NoSchedule`.
pub const SANDBOX_CLASS_TAINT_KEY: &str = "hivekeep.io/sandboxClass";

/// Host directory shared by the keeper and every worker pod.
pub const NODE_BASE_PATH: &str = "/var/lib/hivekeep";

/// Where the worker's own resource limits are projected.
pub const WORKER_CAPACITY_MOUNT_PATH: &str = "/run/worker-capacity";
/// File under [`WORKER_CAPACITY_MOUNT_PATH`] holding the CPU limit in
/// millicores.
pub const WORKER_CAPACITY_CPU_FILE: &str = "cpu_millicores";
/// File under [`WORKER_CAPACITY_MOUNT_PATH`] holding the memory limit in
/// bytes.
pub const WORKER_CAPACITY_MEMORY_FILE: &str = "memory_bytes";

/// Extended resource the keeper's device plugin serves for `/dev/kvm`.
pub const RESOURCE_KVM: &str = "hivekeep.io/kvm";

/// Signer of the projected volume serving the worker tunnel's own
/// credentials and the pod-identity trust bundle.
pub const POD_IDENTITY_SIGNER: &str = hivekeep_identity::pod_cert::POD_IDENTITY_SIGNER;
/// Signer whose roots the worker tunnel trusts for egress.
pub const SERVICE_DNS_SIGNER: &str = "servicedns.podcert.hivekeep.io/identity";
/// Label selecting the ClusterTrustBundles that are live.
pub const CANARYING_LABEL: &str = "podcert.hivekeep.io/canarying";

/// The `--dns-egress-policy` mode enforcing an Actor's egress policy on
/// DNS queries in the worker tunnel's DNS relay.
pub const DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL: &str = "enforce-in-tunnel";

/// Checks a `--dns-egress-policy` value.
pub fn validate_dns_egress_policy(mode: &str) -> Result<(), String> {
    match mode {
        "" => Err("--dns-egress-policy is required".into()),
        DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL => Ok(()),
        other => Err(format!(
            "unsupported --dns-egress-policy {other:?} (must be {DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL:?})"
        )),
    }
}

static PREVIEW_GATES: OnceLock<PreviewGates> = OnceLock::new();

/// Sets the preview gates the controller runs with, which worker pods
/// inherit. Only the first call takes effect; without one, no gate is
/// enabled.
pub fn init_preview_gates(gates: PreviewGates) {
    let _ = PREVIEW_GATES.set(gates);
}

/// The preview gates set by [`init_preview_gates`].
pub fn preview_gates() -> PreviewGates {
    PREVIEW_GATES.get().cloned().unwrap_or_default()
}

/// The `--preview` argument of the worker: the enabled gates, sorted, so
/// the pod template is stable.
pub fn preview_arg(gates: &PreviewGates) -> String {
    let names: Vec<&str> = gates.iter().map(|g| g.name()).collect();
    format!("--preview={}", names.join(","))
}

/// Telemetry configuration propagated to worker pods. Every field is the
/// raw value of the matching `OTEL_*` variable; an empty field leaves the
/// variable unset so the worker keeps its own default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkerOtelSettings {
    /// `OTEL_EXPORTER_OTLP_ENDPOINT`: the collector the worker exports to
    /// directly when no keeper relay is available.
    pub endpoint: String,
    /// `OTEL_METRIC_EXPORT_INTERVAL`, whole milliseconds.
    pub metric_export_interval: String,
    /// `OTEL_METRIC_EXPORT_TIMEOUT`, whole milliseconds.
    pub metric_export_timeout: String,
    /// `OTEL_TRACES_SAMPLER`.
    pub traces_sampler: String,
    /// `OTEL_TRACES_SAMPLER_ARG`; dropped unless a sampler is set.
    pub traces_sampler_arg: String,
    /// `OTEL_LOGS_EXPORTER`.
    pub logs_exporter: String,
    /// `OTEL_TRACES_EXPORTER`.
    pub traces_exporter: String,
    /// `OTEL_METRICS_EXPORTER`.
    pub metrics_exporter: String,
}

/// The worker's OTel resource identity. `service.instance.id` is the pod
/// UID, so every worker pod is a distinct telemetry source.
pub const WORKER_OTEL_RESOURCE_ATTRIBUTES: &str = "k8s.namespace.name=$(POD_NAMESPACE),k8s.pod.name=$(POD_NAME),k8s.pod.uid=$(POD_UID),k8s.node.name=$(NODE_NAME),service.instance.id=$(POD_UID)";

fn field_ref_env(name: &str, field_path: &str) -> EnvVar {
    EnvVar {
        name: name.into(),
        value: None,
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                api_version: None,
                field_path: field_path.into(),
            }),
            ..Default::default()
        }),
    }
}

fn value_env(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.into(),
        value: Some(value.into()),
        value_from: None,
    }
}

/// The worker container's environment. The downward-API variables come
/// first, so `OTEL_RESOURCE_ATTRIBUTES` can substitute them; the identity
/// is always set because the worker also exports through the keeper's
/// relay, and the endpoint only when one is configured.
pub fn worker_container_env(otel: &WorkerOtelSettings) -> Vec<EnvVar> {
    let mut env = vec![
        field_ref_env("POD_UID", "metadata.uid"),
        field_ref_env("POD_NAMESPACE", "metadata.namespace"),
        field_ref_env(
            "WORKER_POOL_NAME",
            &format!("metadata.labels['{WORKER_POOL_LABEL}']"),
        ),
        field_ref_env("POD_NAME", "metadata.name"),
        field_ref_env("NODE_NAME", "spec.nodeName"),
        // Resolved when the container starts, so a worker restarted in a
        // recreated pod sandbox registers the sandbox's new IPs.
        field_ref_env("POD_IPS", "status.podIPs"),
    ];
    if !otel.endpoint.is_empty() {
        env.push(value_env("OTEL_EXPORTER_OTLP_ENDPOINT", &otel.endpoint));
    }
    env.push(value_env(
        "OTEL_RESOURCE_ATTRIBUTES",
        WORKER_OTEL_RESOURCE_ATTRIBUTES,
    ));
    if !otel.metric_export_interval.is_empty() {
        env.push(value_env(
            "OTEL_METRIC_EXPORT_INTERVAL",
            &otel.metric_export_interval,
        ));
    }
    if !otel.metric_export_timeout.is_empty() {
        env.push(value_env(
            "OTEL_METRIC_EXPORT_TIMEOUT",
            &otel.metric_export_timeout,
        ));
    }
    if !otel.logs_exporter.is_empty() {
        env.push(value_env("OTEL_LOGS_EXPORTER", &otel.logs_exporter));
    }
    if !otel.traces_sampler.is_empty() {
        env.push(value_env("OTEL_TRACES_SAMPLER", &otel.traces_sampler));
        if !otel.traces_sampler_arg.is_empty() {
            env.push(value_env(
                "OTEL_TRACES_SAMPLER_ARG",
                &otel.traces_sampler_arg,
            ));
        }
    }
    if !otel.traces_exporter.is_empty() {
        env.push(value_env("OTEL_TRACES_EXPORTER", &otel.traces_exporter));
    }
    if !otel.metrics_exporter.is_empty() {
        env.push(value_env("OTEL_METRICS_EXPORTER", &otel.metrics_exporter));
    }
    env
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_core::preview::Gate;

    #[test]
    fn dns_egress_policy_modes() {
        assert!(validate_dns_egress_policy(DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL).is_ok());
        assert_eq!(
            validate_dns_egress_policy("").unwrap_err(),
            "--dns-egress-policy is required"
        );
        assert!(validate_dns_egress_policy("other")
            .unwrap_err()
            .contains("unsupported"));
    }

    #[test]
    fn preview_args() {
        assert_eq!(preview_arg(&PreviewGates::none()), "--preview=");
        assert_eq!(
            preview_arg(&PreviewGates::none().with(Gate::GoldenEgressPolicy)),
            "--preview=GoldenEgressPolicy"
        );
        assert_eq!(
            preview_arg(&PreviewGates::all()),
            "--preview=ExternalVolumes,GoldenEgressPolicy,PerTemplateSnapshotLocation"
        );
    }
}

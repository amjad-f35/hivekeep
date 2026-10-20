//! The Kubernetes controllers:
//!
//! - WorkerPool: the worker Deployment and ServiceAccount, SandboxConfig
//!   resolution and the pool's status;
//! - SandboxConfig protection: releases a deleting SandboxConfig once no
//!   pool uses it;
//! - NetworkPolicy: the ingress policy of each pool's workers;
//! - egress MITM trust: publishes the egress CA pool's roots as a
//!   ClusterTrustBundle.

pub mod egress_mitm_trust;
pub mod error;
pub mod events;
pub mod manager;
pub mod meta;
pub mod networkpolicy;
pub mod queue;
pub mod reader;
pub mod runner;
pub mod sandboxconfig_protection;
pub mod worker_env;
pub mod workerpool;
pub mod workerpool_apply;
pub mod workerpool_metrics;
pub mod workerpool_sandboxconfig;

#[cfg(test)]
mod fake_cluster;
#[cfg(test)]
mod test_support;

pub use worker_env::{init_preview_gates, WorkerOtelSettings};

/// Configuration of the controllers.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    /// The namespace the control plane runs in. It holds the egress MITM CA
    /// pool, and names the identities worker pods authenticate.
    pub system_namespace: String,
    /// The keeper's ServiceAccount.
    pub keeper_service_account: String,
    /// The gate router's ServiceAccount.
    pub router_service_account: String,
    /// The worker pods' `--dns-egress-policy`.
    pub dns_egress_policy: String,
    /// Telemetry configuration propagated to worker pods.
    pub otel: WorkerOtelSettings,
}

/// Runs all four controllers until `shutdown` fires. Fails if the
/// ClusterTrustBundle API cannot be discovered or the metrics cannot be
/// registered.
pub async fn run(
    client: kube::Client,
    cfg: ControllerConfig,
    registry: prometheus::Registry,
    shutdown: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    manager::run_controllers(client, cfg, registry, shutdown, manager::Controllers::all()).await
}

/// The (namespace, name) of the Secret holding the egress MITM CA pool.
pub fn egress_mitm_ca_pool_ref(system_namespace: &str) -> (String, String) {
    egress_mitm_trust::egress_mitm_ca_pool_ref(system_namespace)
}

/// Checks a `--dns-egress-policy` value.
pub fn validate_dns_egress_policy(v: &str) -> Result<(), String> {
    worker_env::validate_dns_egress_policy(v)
}

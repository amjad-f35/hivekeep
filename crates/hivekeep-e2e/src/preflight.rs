//! Checks that the cluster is ready for a suite before any test runs.

use std::time::Duration;

use hivekeep_proto::api::v1 as pb;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Namespace;
use kube::api::{Api, DynamicObject, ListParams};

use crate::clients::Clients;
use crate::env::{resource_name, system_namespace};
use crate::install;
use crate::sandbox::{is_microvm, SANDBOX_CLASS_ENV, SANDBOX_CLASS_MICROVM};

/// Verifies connectivity, that the control plane Deployments have ready
/// replicas, that the API answers, and that the micro-VM SandboxConfig
/// exists when the run targets it. Whether a node can host a micro-VM is
/// left to the scheduler and reported at runtime.
pub async fn preflight_checks(clients: &Clients) -> Result<(), String> {
    Api::<Namespace>::all(clients.kube())
        .list(&ListParams::default().limit(1))
        .await
        .map_err(|e| format!("failed to connect to Kubernetes API server: {e}"))?;

    let namespace = system_namespace();
    let deployments: Api<Deployment> = Api::namespaced(clients.kube(), &namespace);
    for name in [
        resource_name(install::CONTROLLER_DEPLOYMENT),
        resource_name(install::API_DEPLOYMENT),
    ] {
        let dep = deployments
            .get(&name)
            .await
            .map_err(|e| format!("deployment {namespace}/{name} is missing: {e}"))?;
        let status = dep.status.unwrap_or_default();
        if status.ready_replicas.unwrap_or(0) == 0 {
            return Err(format!(
                "deployment {namespace}/{name} has 0 ready replicas. Status: {status:?}"
            ));
        }
    }

    tokio::time::timeout(
        Duration::from_secs(10),
        clients.api().list_actors(pb::ListActorsRequest::default()),
    )
    .await
    .map_err(|_| "ListActors RPC failed: timed out".to_string())?
    .map_err(|e| format!("ListActors RPC failed: {e}"))?;

    if is_microvm() {
        let configs: Api<DynamicObject> =
            Api::all_with(clients.kube(), &crate::crd::sandbox_config_resource());
        configs.get(SANDBOX_CLASS_MICROVM).await.map_err(|e| {
            format!("{SANDBOX_CLASS_ENV}={SANDBOX_CLASS_MICROVM} but SandboxConfig/{SANDBOX_CLASS_MICROVM} is missing: {e}")
        })?;
    }
    Ok(())
}

//! The NetworkPolicy of each WorkerPool: worker pods accept connections
//! only from the gate router. Egress is not managed by NetworkPolicy.

use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_k8s::crd::WorkerPool;
use k8s_openapi::api::networking::v1::{
    NetworkPolicy, NetworkPolicyIngressRule, NetworkPolicyPeer, NetworkPolicySpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use kube::api::{Api, Patch, PatchParams};
use kube::{Client, ResourceExt};

use super::error::{api_err, Error, Result};
use super::events::{object_reference, EventRecorder, EventType};
use super::meta::{controller_of, label};
use super::queue::ObjectKey;
use super::reader::Reader;
use super::runner::{Action, Reconcile};
use super::worker_env::WORKER_POOL_LABEL;
use super::workerpool_apply::owner_reference;

/// Field manager of the NetworkPolicies.
pub const NETWORK_POLICY_FIELD_OWNER: &str = "hivekeep-networkpolicy";
/// `app` label of the gate router's pods.
pub const GATE_ROUTER_APP_NAME: &str = "gate-router";
/// Reason of the Warning Event a pool emits when its NetworkPolicy belongs
/// to something else.
pub const EVENT_REASON_NETWORK_POLICY_CONFLICT: &str = "NetworkPolicyConflict";
/// Action of that Event.
pub const EVENT_ACTION_APPLY_NETWORK_POLICY: &str = "ApplyNetworkPolicy";

/// The name of a pool's NetworkPolicy.
pub fn network_policy_name(pool: &str) -> String {
    format!("hivekeep-{pool}")
}

/// Reconciles the NetworkPolicy of each WorkerPool.
pub struct NetworkPolicyReconciler {
    client: Client,
    pools: Reader<WorkerPool>,
    policies: Reader<NetworkPolicy>,
    /// The namespace the gate router runs in. The ingress rule admits only
    /// that namespace, so a wrong value blocks every request to the pool.
    system_namespace: String,
    recorder: Arc<dyn EventRecorder>,
}

impl NetworkPolicyReconciler {
    /// A reconciler writing through `client` and reading from the readers.
    pub fn new(
        client: Client,
        pools: Reader<WorkerPool>,
        policies: Reader<NetworkPolicy>,
        system_namespace: impl Into<String>,
        recorder: Arc<dyn EventRecorder>,
    ) -> Self {
        NetworkPolicyReconciler {
            client,
            pools,
            policies,
            system_namespace: system_namespace.into(),
            recorder,
        }
    }

    /// A reconciler doing every read through `client`.
    pub fn live(
        client: Client,
        system_namespace: impl Into<String>,
        recorder: Arc<dyn EventRecorder>,
    ) -> Self {
        Self::new(
            client.clone(),
            Reader::live_namespaced(client.clone()),
            Reader::live_namespaced(client),
            system_namespace,
            recorder,
        )
    }

    pub(crate) async fn reconcile_impl(&self, wp: &WorkerPool) -> Result<()> {
        let np = self.build_network_policy(wp);
        let ns = np.metadata.namespace.clone().unwrap_or_default();
        let name = np.metadata.name.clone().unwrap_or_default();
        let existing = self
            .policies
            .get(&ns, &name)
            .await
            .map_err(api_err(format!("failed to get NetworkPolicy {ns}:{name}")))?;
        if let Some(existing) = existing {
            if let Err(err) = check_network_policy_owner(&existing, wp) {
                self.recorder.event(
                    object_reference(wp),
                    Some(object_reference(&existing)),
                    EventType::Warning,
                    EVENT_REASON_NETWORK_POLICY_CONFLICT,
                    EVENT_ACTION_APPLY_NETWORK_POLICY,
                    err.to_string(),
                );
                return Err(err);
            }
        }
        Api::<NetworkPolicy>::namespaced(self.client.clone(), &ns)
            .patch(
                &name,
                &PatchParams::apply(NETWORK_POLICY_FIELD_OWNER).force(),
                &Patch::Apply(&np),
            )
            .await
            .map_err(api_err(format!(
                "failed to apply NetworkPolicy {ns}:{name}"
            )))?;
        tracing::info!(namespace = %ns, name = %name, "Applied NetworkPolicy");
        Ok(())
    }

    /// The NetworkPolicy of `wp`: ingress on every port, only from the gate
    /// router's pods in the system namespace.
    pub fn build_network_policy(&self, wp: &WorkerPool) -> NetworkPolicy {
        let pool = wp.name_any();
        NetworkPolicy {
            metadata: ObjectMeta {
                name: Some(network_policy_name(&pool)),
                namespace: wp.namespace(),
                labels: Some(label(WORKER_POOL_LABEL, &pool)),
                owner_references: Some(vec![owner_reference(wp)]),
                ..Default::default()
            },
            spec: Some(NetworkPolicySpec {
                pod_selector: Some(LabelSelector {
                    match_labels: Some(label(WORKER_POOL_LABEL, &pool)),
                    match_expressions: None,
                }),
                policy_types: Some(vec!["Ingress".into()]),
                ingress: Some(vec![NetworkPolicyIngressRule {
                    from: Some(vec![NetworkPolicyPeer {
                        namespace_selector: Some(LabelSelector {
                            match_labels: Some(label(
                                "kubernetes.io/metadata.name",
                                &self.system_namespace,
                            )),
                            match_expressions: None,
                        }),
                        pod_selector: Some(LabelSelector {
                            match_labels: Some(label("app", GATE_ROUTER_APP_NAME)),
                            match_expressions: None,
                        }),
                        ip_block: None,
                    }]),
                    ports: None,
                }]),
                egress: None,
            }),
        }
    }
}

/// Refuses `np` unless it belongs to `wp`. The apply forces ownership, so
/// without this it would rewrite another pool's policy or one someone else
/// created under the name. A policy controlled by `wp` passes even with an
/// edited label (the apply restores it); one with no controller passes
/// only if it is labeled for `wp`.
pub fn check_network_policy_owner(np: &NetworkPolicy, wp: &WorkerPool) -> Result<()> {
    let ns = np.namespace().unwrap_or_default();
    let name = np.name_any();
    if let Some(owner) = controller_of(&np.metadata) {
        if Some(owner.uid.as_str()) == wp.metadata.uid.as_deref() {
            return Ok(());
        }
        return Err(Error::Refused(format!(
            "refusing to take over NetworkPolicy {ns}:{name}: controlled by {} {} (UID {})",
            owner.kind, owner.name, owner.uid
        )));
    }
    let got = np
        .labels()
        .get(WORKER_POOL_LABEL)
        .cloned()
        .unwrap_or_default();
    let want = wp.name_any();
    if got != want {
        return Err(Error::Refused(format!(
            "refusing to take over NetworkPolicy {ns}:{name}: label {WORKER_POOL_LABEL} is {got:?}, want {want:?}"
        )));
    }
    Ok(())
}

#[async_trait]
impl Reconcile for NetworkPolicyReconciler {
    async fn reconcile(&self, key: &ObjectKey) -> Result<Action> {
        let Some(wp) = self
            .pools
            .get(&key.namespace, &key.name)
            .await
            .map_err(api_err(format!("failed to get worker pool {key:?}")))?
        else {
            return Ok(Action::Done);
        };
        if wp.metadata.deletion_timestamp.is_some() {
            tracing::info!(pool = %key, "WorkerPool is being deleted; its NetworkPolicy goes with it");
            return Ok(Action::Done);
        }
        if let Err(err) = self.reconcile_impl(&wp).await {
            tracing::error!(%err, pool = %key, "Failed to reconcile NetworkPolicy");
            return Err(err);
        }
        Ok(Action::Done)
    }
}

#[cfg(test)]
#[path = "networkpolicy_tests.rs"]
mod tests;

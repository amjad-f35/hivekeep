//! What the cluster says about a node's workers: the seam between the
//! keeper's sandbox syncer and whatever watches the cluster.

use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::sandbox_config::SandboxConfig;

/// The label naming a worker pod's pool.
pub const WORKER_POOL_LABEL: &str = "hivekeep.io/worker-pool";
/// A worker pod on this node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerPod {
    /// The pod's UID.
    pub uid: String,
    /// The pod's namespace.
    pub namespace: String,
    /// The pod's name.
    pub name: String,
    /// The pool label, if set.
    pub pool: Option<String>,
    /// Whether the pod is being deleted.
    pub deleting: bool,
}

/// Running counts of the cluster changes a watching view has seen, split by
/// who acts on them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClusterChanges {
    /// A worker pod appeared on or left this node, or a WorkerPool
    /// appeared: only this node acts on it, so it is synced at once.
    pub local: u64,
    /// A WorkerPool's resolved SandboxConfig or a SandboxConfig's spec
    /// changed: every node sees it at the same moment, so the syncs it
    /// causes are spread out.
    pub fleet: u64,
}

/// The sending half of a view's change notifications.
#[derive(Debug, Clone)]
pub struct ClusterChangeNotifier(watch::Sender<ClusterChanges>);

impl Default for ClusterChangeNotifier {
    fn default() -> Self {
        ClusterChangeNotifier(watch::channel(ClusterChanges::default()).0)
    }
}

impl ClusterChangeNotifier {
    /// Records a change only this node acts on.
    pub fn local(&self) {
        self.0.send_modify(|c| c.local += 1);
    }
    /// Records a change every node acts on.
    pub fn fleet(&self) {
        self.0.send_modify(|c| c.fleet += 1);
    }
    /// A receiver seeing every later change.
    pub fn subscribe(&self) -> watch::Receiver<ClusterChanges> {
        self.0.subscribe()
    }
}

/// What the cluster says about the node's workers. Watching the cluster
/// is the implementation's business; the syncer resyncs periodically and,
/// when the view offers [`ClusterView::changes`], as soon as it changes.
#[async_trait]
pub trait ClusterView: Send + Sync + 'static {
    /// Change notifications, for a view that watches the cluster. A view
    /// without them (the default) is only read on the syncer's period.
    fn changes(&self) -> Option<watch::Receiver<ClusterChanges>> {
        None
    }
    /// The worker pods on this node.
    async fn worker_pods(&self) -> Result<Vec<WorkerPod>, String>;
    /// The sandbox config a pool resolved, by name; `None` until resolved.
    async fn pool_sandbox_config(
        &self,
        namespace: &str,
        pool: &str,
    ) -> Result<Option<String>, String>;
    /// A sandbox config.
    async fn sandbox_config(&self, name: &str) -> Result<Option<SandboxConfig>, String>;
}

/// A cluster view read from a JSON file, for development and tests.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticClusterView {
    /// Worker pods by UID: namespace and pool.
    #[serde(default)]
    pub pods: HashMap<String, (String, String)>,
    /// Each pool's sandbox config, keyed `namespace/pool`.
    #[serde(default)]
    pub pools: HashMap<String, String>,
    /// Sandbox configs.
    #[serde(default)]
    pub configs: Vec<SandboxConfig>,
}

#[async_trait]
impl ClusterView for StaticClusterView {
    async fn worker_pods(&self) -> Result<Vec<WorkerPod>, String> {
        Ok(self
            .pods
            .iter()
            .map(|(uid, (ns, pool))| WorkerPod {
                uid: uid.clone(),
                namespace: ns.clone(),
                name: uid.clone(),
                pool: Some(pool.clone()),
                deleting: false,
            })
            .collect())
    }
    async fn pool_sandbox_config(
        &self,
        namespace: &str,
        pool: &str,
    ) -> Result<Option<String>, String> {
        Ok(self.pools.get(&format!("{namespace}/{pool}")).cloned())
    }
    async fn sandbox_config(&self, name: &str) -> Result<Option<SandboxConfig>, String> {
        Ok(self.configs.iter().find(|c| c.name == name).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn notifier_counts_changes_by_kind() {
        let n = ClusterChangeNotifier::default();
        let mut rx = n.subscribe();
        assert!(StaticClusterView::default().changes().is_none());
        n.local();
        n.fleet();
        n.fleet();
        rx.changed().await.unwrap();
        assert_eq!(
            *rx.borrow_and_update(),
            ClusterChanges { local: 1, fleet: 2 }
        );
    }
}

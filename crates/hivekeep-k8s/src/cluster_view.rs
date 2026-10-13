//! A watch-backed [`ClusterView`] for the keeper: the worker pods on its
//! node, the WorkerPools they belong to, and the SandboxConfigs those pools
//! resolved.

use std::sync::Arc;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::api::Api;
use kube::runtime::watcher;
use kube::{Client, ResourceExt};

use hivekeep_node::cluster_view::{
    ClusterChangeNotifier, ClusterChanges, ClusterView, WorkerPod, WORKER_POOL_LABEL,
};
use hivekeep_node::sandbox_config as kv;

use crate::crd::{SandboxConfig, SandboxVersionState, WorkerPool};
use crate::informer::{Informer, InformerEvent};

/// Three informers: worker pods on one node (pods carrying the pool label,
/// narrowed by `spec.nodeName`), every WorkerPool, every SandboxConfig.
/// Reads fail until all three listed once, so a keeper never mistakes an
/// empty cache for a node without workers.
pub struct KubeClusterView {
    pods: Arc<Informer<Pod>>,
    pools: Arc<Informer<WorkerPool>>,
    configs: Arc<Informer<SandboxConfig>>,
    changes: ClusterChangeNotifier,
}

/// Who acts on an observed change, if anyone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeScope {
    /// Only this node.
    Local,
    /// Every node at once.
    Fleet,
}

/// A worker pod matters when it arrives or leaves; its updates change no
/// runtimes file.
pub fn pod_change(ev: &InformerEvent<Pod>) -> Option<ChangeScope> {
    match ev {
        InformerEvent::Added(_) | InformerEvent::Deleted(_) => Some(ChangeScope::Local),
        InformerEvent::Updated { .. } => None,
    }
}

/// A new pool's local pods sync at once; a pool whose resolved config
/// moved changes every node's files.
pub fn pool_change(ev: &InformerEvent<WorkerPool>) -> Option<ChangeScope> {
    match ev {
        InformerEvent::Added(_) => Some(ChangeScope::Local),
        InformerEvent::Updated { old, new }
            if old.resolved_sandbox_config() != new.resolved_sandbox_config() =>
        {
            Some(ChangeScope::Fleet)
        }
        _ => None,
    }
}

/// A config matters when it appears and when its spec changes (the
/// generation moves on spec changes only).
pub fn config_change(ev: &InformerEvent<SandboxConfig>) -> Option<ChangeScope> {
    match ev {
        InformerEvent::Added(_) => Some(ChangeScope::Fleet),
        InformerEvent::Updated { old, new }
            if old.metadata.generation != new.metadata.generation =>
        {
            Some(ChangeScope::Fleet)
        }
        _ => None,
    }
}

fn record(changes: &ClusterChangeNotifier, scope: Option<ChangeScope>) {
    match scope {
        Some(ChangeScope::Local) => changes.local(),
        Some(ChangeScope::Fleet) => changes.fleet(),
        None => {}
    }
}

impl KubeClusterView {
    /// A view of `node_name`'s workers. Call [`KubeClusterView::spawn`] to
    /// start watching.
    pub fn new(client: Client, node_name: &str) -> Self {
        let pods = Informer::new(
            Api::all(client.clone()),
            watcher::Config::default()
                .labels(WORKER_POOL_LABEL)
                .fields(&format!("spec.nodeName={node_name}")),
        );
        let pools = Informer::new(Api::all(client.clone()), watcher::Config::default());
        let configs = Informer::new(Api::all(client), watcher::Config::default());
        let changes = ClusterChangeNotifier::default();
        let c = changes.clone();
        pods.add_handler(move |ev| record(&c, pod_change(ev)));
        let c = changes.clone();
        pools.add_handler(move |ev| record(&c, pool_change(ev)));
        let c = changes.clone();
        configs.add_handler(move |ev| record(&c, config_change(ev)));
        KubeClusterView {
            pods,
            pools,
            configs,
            changes,
        }
    }

    /// Starts the informers on the current runtime.
    pub fn spawn(&self) -> Vec<tokio::task::JoinHandle<()>> {
        vec![self.pods.spawn(), self.pools.spawn(), self.configs.spawn()]
    }

    /// Calls `f` on every change to a worker pod, pool or config, so the
    /// caller can resync without waiting for its period.
    pub fn on_change(&self, f: impl Fn() + Send + Sync + 'static) {
        let f = Arc::new(f);
        let g = f.clone();
        self.pods.add_handler(move |_| g());
        let g = f.clone();
        self.pools.add_handler(move |_| g());
        self.configs.add_handler(move |_| f());
    }

    /// Whether every informer listed once.
    pub fn has_synced(&self) -> bool {
        self.pods.has_synced() && self.pools.has_synced() && self.configs.has_synced()
    }

    /// Waits until every informer listed once.
    pub async fn wait_synced(&self) {
        self.pods.wait_synced().await;
        self.pools.wait_synced().await;
        self.configs.wait_synced().await;
    }

    fn check_synced(&self) -> Result<(), String> {
        if self.has_synced() {
            Ok(())
        } else {
            Err("cluster view not synced yet".into())
        }
    }
}

/// The keeper's form of a SandboxConfig.
pub fn to_keeper_config(sc: &SandboxConfig) -> kv::SandboxConfig {
    kv::SandboxConfig {
        name: sc.name_any(),
        generation: sc.metadata.generation.unwrap_or_default(),
        sandbox_class: sc.spec.sandbox_class.as_str().to_string(),
        default_version: sc.spec.default_version.clone(),
        versions: sc
            .spec
            .versions
            .iter()
            .map(|v| kv::SandboxVersion {
                name: v.name.clone(),
                state: match v.state {
                    Some(SandboxVersionState::Disabled) => kv::VersionState::Disabled,
                    _ => kv::VersionState::Enabled,
                },
                assets: v
                    .assets
                    .iter()
                    .map(|(arch, files)| {
                        (
                            arch.clone(),
                            files
                                .iter()
                                .map(|(n, f)| {
                                    (
                                        n.clone(),
                                        kv::AssetFile {
                                            url: f.url.clone(),
                                            sha256: f.sha256.clone(),
                                        },
                                    )
                                })
                                .collect(),
                        )
                    })
                    .collect(),
            })
            .collect(),
    }
}

#[async_trait]
impl ClusterView for KubeClusterView {
    fn changes(&self) -> Option<tokio::sync::watch::Receiver<ClusterChanges>> {
        Some(self.changes.subscribe())
    }

    async fn worker_pods(&self) -> Result<Vec<WorkerPod>, String> {
        self.check_synced()?;
        Ok(self
            .pods
            .list()
            .iter()
            .filter_map(|p| {
                Some(WorkerPod {
                    uid: p.metadata.uid.clone()?,
                    namespace: p.namespace().unwrap_or_default(),
                    name: p.name_any(),
                    pool: p
                        .labels()
                        .get(WORKER_POOL_LABEL)
                        .filter(|v| !v.is_empty())
                        .cloned(),
                    deleting: p.metadata.deletion_timestamp.is_some(),
                })
            })
            .collect())
    }

    async fn pool_sandbox_config(
        &self,
        namespace: &str,
        pool: &str,
    ) -> Result<Option<String>, String> {
        self.check_synced()?;
        Ok(self
            .pools
            .get(namespace, pool)
            .and_then(|p| p.resolved_sandbox_config().map(String::from)))
    }

    async fn sandbox_config(&self, name: &str) -> Result<Option<kv::SandboxConfig>, String> {
        self.check_synced()?;
        Ok(self.configs.get("", name).map(|c| to_keeper_config(&c)))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::*;
    use crate::fake::FakeApiServer;

    fn pool_resolving(config: Option<&str>) -> Arc<WorkerPool> {
        let class = match config {
            Some(c) => json!({"name": "gvisor", "configRef": {"name": c}}),
            None => json!({"name": "gvisor"}),
        };
        Arc::new(
            serde_json::from_value(json!({
                "apiVersion": "hivekeep.io/v1", "kind": "WorkerPool",
                "metadata": {"name": "pool", "namespace": "ns"},
                "spec": {"replicas": 1, "workerImage": "i", "sandboxClasses": [{"name": "gvisor"}]},
                "status": {"replicas": 1, "sandboxClasses": [class]}
            }))
            .unwrap(),
        )
    }

    fn config_at(generation: i64) -> Arc<SandboxConfig> {
        Arc::new(
            serde_json::from_value(json!({
                "apiVersion": "hivekeep.io/v1", "kind": "SandboxConfig",
                "metadata": {"name": "c", "generation": generation},
                "spec": {"sandboxClass": "gvisor", "defaultVersion": "v1", "versions": [{"name": "v1"}]}
            }))
            .unwrap(),
        )
    }

    #[test]
    fn change_scopes() {
        use ChangeScope::{Fleet, Local};
        let p = Arc::new(pod("w", "u", "n", Some("pool")));
        assert_eq!(pod_change(&InformerEvent::Added(p.clone())), Some(Local));
        assert_eq!(pod_change(&InformerEvent::Deleted(p.clone())), Some(Local));
        assert_eq!(
            pod_change(&InformerEvent::Updated {
                old: p.clone(),
                new: p
            }),
            None
        );

        let (a, b, none) = (
            pool_resolving(Some("a")),
            pool_resolving(Some("b")),
            pool_resolving(None),
        );
        assert_eq!(pool_change(&InformerEvent::Added(a.clone())), Some(Local));
        assert_eq!(
            pool_change(&InformerEvent::Updated {
                old: a.clone(),
                new: b
            }),
            Some(Fleet)
        );
        assert_eq!(
            pool_change(&InformerEvent::Updated {
                old: none,
                new: a.clone()
            }),
            Some(Fleet)
        );
        assert_eq!(
            pool_change(&InformerEvent::Updated {
                old: a.clone(),
                new: a.clone()
            }),
            None
        );
        assert_eq!(pool_change(&InformerEvent::Deleted(a)), None);

        assert_eq!(
            config_change(&InformerEvent::Added(config_at(1))),
            Some(Fleet)
        );
        assert_eq!(
            config_change(&InformerEvent::Updated {
                old: config_at(1),
                new: config_at(2)
            }),
            Some(Fleet)
        );
        assert_eq!(
            config_change(&InformerEvent::Updated {
                old: config_at(1),
                new: config_at(1)
            }),
            None
        );
        assert_eq!(config_change(&InformerEvent::Deleted(config_at(1))), None);
    }

    fn pod(name: &str, uid: &str, node: &str, pool: Option<&str>) -> Pod {
        let mut labels = serde_json::Map::new();
        if let Some(p) = pool {
            labels.insert(WORKER_POOL_LABEL.into(), json!(p));
        }
        serde_json::from_value(json!({
            "metadata": {"name": name, "namespace": "ns", "uid": uid, "labels": labels},
            "spec": {"nodeName": node, "containers": [{"name": "w", "image": "i"}]}
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn view_reflects_cluster() {
        let fake = FakeApiServer::new();
        fake.insert(&pod("w1", "uid-1", "node-a", Some("pool")));
        fake.insert(&pod("w2", "uid-2", "node-b", Some("pool")));
        fake.insert(&pod("other", "uid-3", "node-a", None));
        let wp: WorkerPool = serde_json::from_value(json!({
            "apiVersion": "hivekeep.io/v1", "kind": "WorkerPool",
            "metadata": {"name": "pool", "namespace": "ns"},
            "spec": {"replicas": 1, "workerImage": "i", "sandboxClasses": [{"name": "gvisor"}]},
            "status": {"replicas": 1, "sandboxClasses": [{"name": "gvisor", "configRef": {"name": "gvisor-default"}}]}
        }))
        .unwrap();
        fake.insert(&wp);
        let sc: SandboxConfig = serde_json::from_value(json!({
            "apiVersion": "hivekeep.io/v1", "kind": "SandboxConfig",
            "metadata": {"name": "gvisor-default"},
            "spec": {"sandboxClass": "gvisor", "defaultVersion": "v1", "versions": [
                {"name": "v1", "assets": {"amd64": {"gvisor": {"url": "gs://b/g", "sha256": "ab"}}}},
                {"name": "v0", "state": "Disabled"}
            ]}
        }))
        .unwrap();
        fake.insert(&sc);

        let view = KubeClusterView::new(fake.client(), "node-a");
        assert!(
            view.worker_pods().await.is_err(),
            "unsynced view must not read as empty"
        );
        let tasks = view.spawn();
        tokio::time::timeout(Duration::from_secs(5), view.wait_synced())
            .await
            .unwrap();

        let pods = view.worker_pods().await.unwrap();
        assert_eq!(pods.len(), 1);
        assert_eq!(pods[0].uid, "uid-1");
        assert_eq!(pods[0].pool.as_deref(), Some("pool"));
        assert!(!pods[0].deleting);
        assert_eq!(
            view.pool_sandbox_config("ns", "pool")
                .await
                .unwrap()
                .as_deref(),
            Some("gvisor-default")
        );
        assert_eq!(
            view.pool_sandbox_config("ns", "missing").await.unwrap(),
            None
        );
        let cfg = view
            .sandbox_config("gvisor-default")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cfg.sandbox_class, "gvisor");
        assert_eq!(
            cfg.default_version_config().unwrap().assets["amd64"]["gvisor"].url,
            "gs://b/g"
        );
        assert_eq!(cfg.versions[1].state, kv::VersionState::Disabled);
        assert!(view.sandbox_config("nope").await.unwrap().is_none());

        // A pod arriving on the node is a local change.
        let mut changes = view.changes().unwrap();
        let before = *changes.borrow_and_update();
        fake.insert(&pod("w3", "uid-4", "node-a", Some("pool")));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                changes.changed().await.unwrap();
                if changes.borrow_and_update().local > before.local {
                    break;
                }
            }
        })
        .await
        .expect("a new worker pod is reported as a change");
        for t in tasks {
            t.abort();
        }
    }
}

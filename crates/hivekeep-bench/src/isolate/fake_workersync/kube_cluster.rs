//! The controller's view of Kubernetes: WorkerPools from an informer, the
//! fake-keeper pods and their nodes from the API.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use hivekeep_k8s::crd::WorkerPool;
use hivekeep_k8s::informer::Informer;
use k8s_openapi::api::core::v1::{Node as K8sNode, Pod};
use kube::api::{Api, ListParams, PostParams};
use kube::ResourceExt;
use parking_lot::Mutex;

use super::ports::{Cluster, Node};

/// The controller's Kubernetes.
pub struct KubeCluster {
    client: kube::Client,
    /// WorkerPools; `None` lists none.
    pools: Option<Arc<Informer<WorkerPool>>>,
    keeper_namespace: String,
    keeper_label: String,
    relay_port: u16,
    /// How long a node stays in the set with no fake-keeper pod.
    node_grace: Duration,
    /// The clock `node_grace` is measured on.
    pub(crate) now: Box<dyn Fn() -> SystemTime + Send + Sync>,
    /// When each node last had a fake-keeper pod scheduled to it.
    seen: Mutex<HashMap<String, SystemTime>>,
}

fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

impl KubeCluster {
    /// A view over `client` with WorkerPools from `pools`.
    pub fn new(
        client: kube::Client,
        pools: Option<Arc<Informer<WorkerPool>>>,
        keeper_namespace: &str,
        keeper_label: &str,
        relay_port: u16,
        node_grace: Duration,
    ) -> KubeCluster {
        KubeCluster {
            client,
            pools,
            keeper_namespace: keeper_namespace.into(),
            keeper_label: keeper_label.into(),
            relay_port,
            node_grace,
            now: Box::new(SystemTime::now),
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// Adds `nodes` to the set [`Cluster::nodes`] draws on, as if a
    /// fake-keeper pod had just been scheduled to each.
    pub fn remember(&self, nodes: &[String]) {
        let now = (self.now)();
        let mut seen = self.seen.lock();
        for n in nodes {
            seen.insert(n.clone(), now);
        }
    }

    async fn keeper_pods(&self) -> Result<Vec<Pod>, String> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), &self.keeper_namespace);
        api.list(&ListParams::default().labels(&self.keeper_label))
            .await
            .map(|l| l.items)
            .map_err(|e| e.to_string())
    }
}

#[async_trait]
impl Cluster for KubeCluster {
    async fn pools(&self) -> Result<Vec<WorkerPool>, String> {
        Ok(self
            .pools
            .as_ref()
            .map(|i| i.list().into_iter().map(|p| (*p).clone()).collect())
            .unwrap_or_default())
    }

    /// The nodes that still exist and have had a fake-keeper pod scheduled
    /// to them, ready or not, within the node grace. A node stays while its
    /// fake keeper is replaced, as in a DaemonSet rollout, so its Workers
    /// wait for the relay rather than moving; one that stops running the
    /// fake keeper for good leaves, and its Workers move.
    async fn nodes(&self) -> Result<Vec<Node>, String> {
        let pods = self.keeper_pods().await?;
        let list = Api::<K8sNode>::all(self.client.clone())
            .list(&ListParams::default())
            .await
            .map_err(|e| e.to_string())?
            .items;
        let now = (self.now)();
        let mut seen = self.seen.lock();
        for p in &pods {
            if let Some(n) = p
                .spec
                .as_ref()
                .and_then(|s| s.node_name.clone())
                .filter(|n| !n.is_empty())
            {
                seen.insert(n, now);
            }
        }
        let exists: Vec<String> = list.iter().map(|n| n.name_any()).collect();
        seen.retain(|name, at| {
            exists.contains(name) && now.duration_since(*at).unwrap_or_default() <= self.node_grace
        });
        let mut nodes: Vec<Node> = list
            .into_iter()
            .filter(|n| seen.contains_key(&n.name_any()))
            .map(|n| Node {
                name: n.name_any(),
                allocatable: n.status.and_then(|s| s.allocatable).unwrap_or_default(),
            })
            .collect();
        nodes.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(nodes)
    }

    async fn relays(&self) -> Result<HashMap<String, String>, String> {
        let mut out = HashMap::new();
        for p in self.keeper_pods().await? {
            let status = p.status.clone().unwrap_or_default();
            let ip = status.pod_ip.unwrap_or_default();
            if status.phase.as_deref() != Some("Running")
                || ip.is_empty()
                || p.metadata.deletion_timestamp.is_some()
            {
                continue;
            }
            let node = p.spec.and_then(|s| s.node_name).unwrap_or_default();
            out.insert(node, join_host_port(&ip, self.relay_port));
        }
        Ok(out)
    }

    async fn update_status(&self, wp: &WorkerPool) -> Result<(), String> {
        let api: Api<WorkerPool> =
            Api::namespaced(self.client.clone(), &wp.namespace().unwrap_or_default());
        api.replace_status(&wp.name_any(), &PostParams::default(), wp)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_k8s::fake::FakeApiServer;
    use kube::api::DeleteParams;
    use serde_json::json;

    const LABEL: &str = "hivekeep.io/fake-keeper=true";

    fn node(name: &str) -> K8sNode {
        serde_json::from_value(
            json!({"metadata": {"name": name}, "status": {"allocatable": {"memory": "8Gi"}}}),
        )
        .unwrap()
    }

    fn pod(name: &str, ns: &str, node: &str, fake_keeper: bool) -> Pod {
        let labels = if fake_keeper {
            json!({"app": "keeper", "hivekeep.io/fake-keeper": "true"})
        } else {
            json!({"app": "gate-router"})
        };
        let mut spec = json!({"containers": [{"name": "main", "image": "x"}]});
        if !node.is_empty() {
            spec["nodeName"] = json!(node);
        }
        serde_json::from_value(
            json!({"metadata": {"name": name, "namespace": ns, "labels": labels}, "spec": spec}),
        )
        .unwrap()
    }

    struct Env {
        fake: FakeApiServer,
        cl: KubeCluster,
        now: Arc<Mutex<SystemTime>>,
    }

    /// A cluster with a five-minute node grace on `objects`, and a clock
    /// the test advances.
    fn env(nodes: &[&str], pods: Vec<Pod>) -> Env {
        let fake = FakeApiServer::new();
        for n in nodes {
            fake.insert(&node(n));
        }
        for p in &pods {
            fake.insert(p);
        }
        let mut cl = KubeCluster::new(
            fake.client(),
            None,
            "hivekeep-system",
            LABEL,
            8086,
            Duration::from_secs(300),
        );
        let now = Arc::new(Mutex::new(
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_790_000_000),
        ));
        let n = now.clone();
        cl.now = Box::new(move || *n.lock());
        Env { fake, cl, now }
    }

    impl Env {
        fn advance(&self, d: Duration) {
            *self.now.lock() += d;
        }
        async fn names(&self) -> Vec<String> {
            let nodes = self.cl.nodes().await.unwrap();
            for n in &nodes {
                assert!(
                    crate::isolate::quantity::memory_bytes(Some(&n.allocatable)) > 0,
                    "{} has no memory",
                    n.name
                );
            }
            nodes.into_iter().map(|n| n.name).collect()
        }
        async fn delete_pod(&self, name: &str) {
            Api::<Pod>::namespaced(self.fake.client(), "hivekeep-system")
                .delete(name, &DeleteParams::default())
                .await
                .unwrap();
        }
        async fn delete_node(&self, name: &str) {
            Api::<K8sNode>::all(self.fake.client())
                .delete(name, &DeleteParams::default())
                .await
                .unwrap();
        }
    }

    // Workers go only to nodes that run a fake-keeper pod: a node without
    // one, such as a tainted control plane node, has no relay.
    #[tokio::test]
    async fn nodes_are_the_fake_keeper_nodes() {
        let e = env(
            &[
                "node-b",
                "node-a",
                "control-plane",
                "unscheduled",
                "other-ns",
            ],
            vec![
                pod("fake-keeper-b", "hivekeep-system", "node-b", true),
                pod("fake-keeper-a", "hivekeep-system", "node-a", true),
                pod("fake-keeper-pending", "hivekeep-system", "", true),
                pod("not-fake", "hivekeep-system", "control-plane", false),
                pod("fake-keeper-elsewhere", "default", "other-ns", true),
            ],
        );
        assert_eq!(e.names().await, vec!["node-a", "node-b"]);
    }

    // After a restart, a node whose fake keeper is being replaced has no
    // pod; the adopted Workers' nodes keep it in the set.
    #[tokio::test]
    async fn remembers_adopted_nodes() {
        let e = env(
            &["node-a", "node-b", "gone-later"],
            vec![pod("fake-keeper-a", "hivekeep-system", "node-a", true)],
        );
        e.cl.remember(&["node-b".into(), "missing".into()]);
        assert_eq!(e.names().await, vec!["node-a", "node-b"]);
    }

    // A DaemonSet rollout leaves a node without a fake-keeper pod for a
    // moment; the node stays. A deleted node goes.
    #[tokio::test]
    async fn nodes_survive_a_fake_keeper_replacement() {
        let e = env(
            &["node-a", "node-b"],
            vec![
                pod("fake-keeper-a", "hivekeep-system", "node-a", true),
                pod("fake-keeper-b", "hivekeep-system", "node-b", true),
            ],
        );
        assert_eq!(e.names().await, vec!["node-a", "node-b"]);
        e.delete_pod("fake-keeper-a").await;
        assert_eq!(e.names().await, vec!["node-a", "node-b"]);
        e.delete_node("node-b").await;
        assert_eq!(e.names().await, vec!["node-a"]);
    }

    // A node that stops running the fake keeper for good leaves the set once
    // the grace has passed.
    #[tokio::test]
    async fn nodes_expire_without_a_fake_keeper() {
        let e = env(
            &["node-a", "node-b"],
            vec![
                pod("fake-keeper-a", "hivekeep-system", "node-a", true),
                pod("fake-keeper-b", "hivekeep-system", "node-b", true),
            ],
        );
        e.names().await;
        e.delete_pod("fake-keeper-b").await;
        e.advance(Duration::from_secs(240));
        assert_eq!(e.names().await, vec!["node-a", "node-b"]);
        e.advance(Duration::from_secs(120));
        assert_eq!(e.names().await, vec!["node-a"]);
    }

    #[tokio::test]
    async fn relays_are_running_fake_keeper_pods() {
        let mut running = pod("fake-keeper-a", "hivekeep-system", "node-a", true);
        running.status =
            Some(serde_json::from_value(json!({"phase": "Running", "podIP": "10.0.0.1"})).unwrap());
        let mut v6 = pod("fake-keeper-c", "hivekeep-system", "node-c", true);
        v6.status =
            Some(serde_json::from_value(json!({"phase": "Running", "podIP": "fd00::1"})).unwrap());
        let pending = pod("fake-keeper-b", "hivekeep-system", "node-b", true);
        let e = env(&["node-a", "node-b", "node-c"], vec![running, pending, v6]);
        let relays = e.cl.relays().await.unwrap();
        assert_eq!(
            relays,
            HashMap::from([
                ("node-a".to_string(), "10.0.0.1:8086".to_string()),
                ("node-c".to_string(), "[fd00::1]:8086".to_string()),
            ])
        );
    }
}

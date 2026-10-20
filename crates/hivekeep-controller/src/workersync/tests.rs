use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hivekeep_k8s::crd::{SandboxClass, WorkerPool, WorkerPoolSandboxClass, WorkerPoolSpec};
use hivekeep_k8s::fake::{FakeApiServer, Reaction};
use hivekeep_k8s::informer::Informer;
use hivekeep_proto::api::v1 as pb;
use k8s_openapi::api::apps::v1::{Deployment, ReplicaSet};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{OwnerReference, Time};
use kube::api::{Api, DeleteParams, PostParams};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Status};

use super::fake_registry::FakeRegistry;
use super::informers::{worker_pod_informer, worker_pool_informer};
use super::registry::WorkerRegistry;
use super::syncer::WorkerPoolSyncer;
use super::{WorkerKey, WORKER_POD_LABEL};

const TEST_POD_UID: &str = "11111111-1111-1111-1111-111111111111";
const OTHER_POD_UID: &str = "22222222-2222-2222-2222-222222222222";

/// A pod the informer's selector matches, scheduled to a node. An empty ip
/// leaves it with no status, so not Ready.
fn worker_pod(ns: &str, name: &str, pool: &str, uid: &str, ip: &str) -> Pod {
    let mut v = json!({
        "metadata": {"name": name, "namespace": ns, "uid": uid, "labels": {WORKER_POD_LABEL: pool}},
        "spec": {"nodeName": "node1", "containers": [{"name": "main", "image": "nginx"}]}
    });
    if !ip.is_empty() {
        v["status"] = json!({
            "phase": "Running", "podIP": ip, "podIPs": [{"ip": ip}],
            "conditions": [{"type": "Ready", "status": "True"}]
        });
    }
    serde_json::from_value(v).unwrap()
}

fn set_ready(pod: &mut Pod, ready: Option<&str>) {
    let st = pod.status.get_or_insert_with(Default::default);
    st.conditions =
        ready.map(|r| vec![serde_json::from_value(json!({"type": "Ready", "status": r})).unwrap()]);
}

fn worker_pool(ns: &str, name: &str, labels: &[(&str, &str)]) -> WorkerPool {
    let mut wp = WorkerPool::new(
        name,
        WorkerPoolSpec {
            sandbox_classes: vec![WorkerPoolSandboxClass {
                name: SandboxClass::Gvisor,
                config_ref: None,
            }],
            ..Default::default()
        },
    );
    wp.metadata.namespace = Some(ns.into());
    if !labels.is_empty() {
        wp.metadata.labels = Some(
            labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
    }
    wp
}

fn registered_worker(ns: &str, pool: &str, pod: &str, uid: &str) -> pb::Worker {
    pb::Worker {
        metadata: Some(pb::ResourceMetadata {
            name: uid.into(),
            ..Default::default()
        }),
        worker_namespace: ns.into(),
        worker_pool: pool.into(),
        worker_pod: pod.into(),
        worker_pod_uid: uid.into(),
        node_name: "node1".into(),
        ..Default::default()
    }
}

fn state(w: &Option<pb::Worker>) -> pb::WorkerState {
    w.as_ref()
        .and_then(|w| w.status.as_ref())
        .and_then(|s| pb::WorkerState::try_from(s.state).ok())
        .unwrap_or(pb::WorkerState::Unspecified)
}

fn labels(w: &Option<pb::Worker>) -> BTreeMap<String, String> {
    w.as_ref()
        .map(|w| w.labels.clone().into_iter().collect())
        .unwrap_or_default()
}

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// A syncer whose caches are seeded directly, for driving reconciles
/// synchronously. Its Kubernetes client is a fake server the caches do not
/// watch.
struct ReconcileEnv {
    s: Arc<WorkerPoolSyncer>,
    pods: Arc<Informer<Pod>>,
    pools: Arc<Informer<WorkerPool>>,
    fake: FakeApiServer,
}

fn setup_reconcile(reg: &FakeRegistry, init_pools: Vec<WorkerPool>) -> ReconcileEnv {
    setup_reconcile_with(reg, init_pools, FakeApiServer::new())
}

fn setup_reconcile_with(
    reg: &FakeRegistry,
    init_pools: Vec<WorkerPool>,
    fake: FakeApiServer,
) -> ReconcileEnv {
    let pods = worker_pod_informer(fake.client());
    let pools = worker_pool_informer(fake.client());
    for p in init_pools {
        pools.cache_put(p);
    }
    let s = WorkerPoolSyncer::new(
        Arc::new(reg.clone()),
        fake.client(),
        pods.clone(),
        pools.clone(),
        2,
    );
    ReconcileEnv {
        s,
        pods,
        pools,
        fake,
    }
}

impl ReconcileEnv {
    fn seed(&self, pod: Pod) -> WorkerKey {
        let key = WorkerKey::new(
            pod.metadata.namespace.clone().unwrap(),
            pod.metadata.name.clone().unwrap(),
            pod.metadata.uid.clone().unwrap(),
        );
        self.pods.cache_put(pod);
        key
    }

    async fn must_reconcile(&self, key: &WorkerKey) {
        self.s
            .reconcile(key)
            .await
            .unwrap_or_else(|e| panic!("reconcile({key:?}): {e}"));
    }
}

/// A running syncer over a fake server holding `init_pools`.
struct SyncerEnv {
    fake: FakeApiServer,
    stop: CancellationToken,
}

impl Drop for SyncerEnv {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

fn setup_syncer(reg: &FakeRegistry, init_pools: Vec<WorkerPool>) -> SyncerEnv {
    let fake = FakeApiServer::new();
    for p in &init_pools {
        fake.insert(p);
    }
    let pods = worker_pod_informer(fake.client());
    let pools = worker_pool_informer(fake.client());
    let stop = CancellationToken::new();
    let s = WorkerPoolSyncer::new(
        Arc::new(reg.clone()),
        fake.client(),
        pods.clone(),
        pools.clone(),
        2,
    );
    // Start before the informers: their initial list seeds the queue.
    s.start(stop.clone());
    for inf in [pods.spawn(), pools.spawn()] {
        let stop = stop.clone();
        tokio::spawn(async move {
            stop.cancelled().await;
            inf.abort();
        });
    }
    SyncerEnv { fake, stop }
}

async fn wait_for_worker(
    reg: &FakeRegistry,
    name: &str,
    cond: impl Fn(&Option<pb::Worker>) -> bool,
) -> Option<pb::Worker> {
    let mut last = None;
    for _ in 0..500 {
        last = reg.get(name);
        if cond(&last) {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("waiting on worker {name}: last seen {last:?}");
}

fn active(w: &Option<pb::Worker>) -> bool {
    state(w) == pb::WorkerState::Active
}

/// Polls `check` for `window`, failing if it ever reports a problem.
async fn holds_for(window: Duration, check: impl Fn() -> Result<(), String>) {
    let deadline = tokio::time::Instant::now() + window;
    while tokio::time::Instant::now() < deadline {
        if let Err(e) = check() {
            panic!("{e}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn pods_api(fake: &FakeApiServer, ns: &str) -> Api<Pod> {
    Api::namespaced(fake.client(), ns)
}

#[tokio::test]
async fn lifecycle() {
    let (ns, pod_name, pool) = ("ns-syncer-lifecycle", "worker-unit-1", "pool1");
    let reg = FakeRegistry::new();
    let env = setup_syncer(&reg, vec![worker_pool(ns, pool, &[("foo", "bar")])]);
    let api = pods_api(&env.fake, ns);

    // An unscheduled pod is not registered: node_name is required.
    let mut pod = worker_pod(ns, pod_name, pool, TEST_POD_UID, "");
    pod.spec.as_mut().unwrap().node_name = None;
    api.create(&PostParams::default(), &pod).await.unwrap();
    holds_for(Duration::from_millis(300), || {
        let n = reg.names();
        if n.is_empty() {
            Ok(())
        } else {
            Err(format!("registry holds {n:?} for a pod with no node"))
        }
    })
    .await;

    // Scheduled and Ready: registered with the pool's labels.
    let mut current = api.get(pod_name).await.unwrap();
    current.spec.as_mut().unwrap().node_name = Some("node1".into());
    let current = api
        .replace(pod_name, &PostParams::default(), &current)
        .await
        .unwrap();
    let mut with_status = current.clone();
    with_status.status = worker_pod(ns, pod_name, pool, TEST_POD_UID, "127.0.0.1").status;
    api.replace_status(pod_name, &PostParams::default(), &with_status)
        .await
        .unwrap();
    let got = wait_for_worker(&reg, TEST_POD_UID, active).await;
    assert_eq!(labels(&got), map(&[("foo", "bar")]));

    api.delete(pod_name, &DeleteParams::default())
        .await
        .unwrap();
    wait_for_worker(&reg, TEST_POD_UID, Option::is_none).await;
}

#[tokio::test]
async fn omitted_fields() {
    let (ns, pool) = ("ns-syncer-omitted", "pool1");
    let reg = FakeRegistry::new();
    let env = setup_syncer(&reg, vec![worker_pool(ns, pool, &[])]);
    env.fake.insert(&worker_pod(
        ns,
        "worker-unit-1",
        pool,
        TEST_POD_UID,
        "127.0.0.1",
    ));
    let got = wait_for_worker(&reg, TEST_POD_UID, Option::is_some).await;
    assert!(
        labels(&got).is_empty(),
        "worker labels = {:?}, want none",
        labels(&got)
    );
}

#[tokio::test]
async fn does_not_infer_capacity_from_the_pod() {
    let (ns, pool) = ("ns-syncer-capacity", "pool1");
    let reg = FakeRegistry::new();
    let env = setup_syncer(&reg, vec![worker_pool(ns, pool, &[])]);
    let mut pod = worker_pod(ns, "worker-unit-1", pool, TEST_POD_UID, "127.0.0.1");
    pod.spec.as_mut().unwrap().containers.push(
        serde_json::from_value(json!({"name": "worker", "image": "worker", "resources": {"limits": {"cpu": "4", "memory": "8Gi"}}}))
            .unwrap(),
    );
    env.fake.insert(&pod);
    let got = wait_for_worker(&reg, TEST_POD_UID, Option::is_some)
        .await
        .unwrap();
    assert!(
        got.status.unwrap().capacity.is_none(),
        "the syncer wrote a capacity"
    );
}

#[tokio::test]
async fn soft_delete_marks_draining() {
    for (ns, ip) in [("ns-drain", "10.0.0.2"), ("ns-drain-noip", "")] {
        let reg = FakeRegistry::new();
        reg.put(registered_worker(ns, "pool1", "worker-drain", TEST_POD_UID));
        let env = setup_reconcile(&reg, vec![]);
        let mut pod = worker_pod(ns, "worker-drain", "pool1", TEST_POD_UID, ip);
        pod.metadata.deletion_timestamp =
            Some(Time(k8s_openapi::jiff::Timestamp::from_second(1).unwrap()));
        let key = env.seed(pod);
        env.must_reconcile(&key).await;
        let w = reg.get(TEST_POD_UID);
        assert!(w.is_some(), "worker should still exist while draining");
        assert_eq!(state(&w), pb::WorkerState::Draining, "ip {ip:?}");
    }
}

#[tokio::test]
async fn mark_worker_draining() {
    let key = WorkerKey::new("ns-mark", "worker-mark", TEST_POD_UID);

    // Worker not found returns Ok.
    let env = setup_reconcile(&FakeRegistry::new(), vec![]);
    env.s.mark_worker_draining(&key).await.unwrap();

    // Already draining returns Ok without a write.
    let reg = FakeRegistry::new();
    let mut w = registered_worker("ns-mark", "pool1", "worker-mark", TEST_POD_UID);
    w.status = Some(pb::WorkerStatus {
        state: pb::WorkerState::Draining as i32,
        ..Default::default()
    });
    let seeded = reg.put(w);
    let env = setup_reconcile(&reg, vec![]);
    env.s.mark_worker_draining(&key).await.unwrap();
    assert_eq!(
        reg.get(TEST_POD_UID).unwrap().metadata.unwrap().version,
        seeded.metadata.unwrap().version,
        "a repeated drain churned the version"
    );

    // Active worker marked draining.
    let reg = FakeRegistry::new();
    reg.put(registered_worker(
        "ns-mark",
        "pool1",
        "worker-mark",
        TEST_POD_UID,
    ));
    let env = setup_reconcile(&reg, vec![]);
    env.s.mark_worker_draining(&key).await.unwrap();
    assert_eq!(state(&reg.get(TEST_POD_UID)), pb::WorkerState::Draining);
}

#[tokio::test]
async fn reconcile_dead_worker() {
    let key = WorkerKey::new("ns-del", "worker-del", TEST_POD_UID);
    let reg = FakeRegistry::new();
    reg.put(registered_worker(
        "ns-del",
        "pool1",
        "worker-del",
        TEST_POD_UID,
    ));
    let env = setup_reconcile(&reg, vec![]);
    env.s.reconcile_dead_worker(&key).await.unwrap();
    assert!(reg.get(TEST_POD_UID).is_none(), "worker still registered");

    // An absent worker is success.
    let env = setup_reconcile(&FakeRegistry::new(), vec![]);
    env.s.reconcile_dead_worker(&key).await.unwrap();
}

#[tokio::test]
async fn reconcile_orphaned_workers() {
    let (ns, pool) = ("ns-recon", "pool1");
    let reg = FakeRegistry::new();
    reg.put(registered_worker(ns, pool, "worker-live", TEST_POD_UID));
    reg.put(registered_worker(ns, pool, "worker-orphan", OTHER_POD_UID));
    let env = setup_reconcile(&reg, vec![worker_pool(ns, pool, &[])]);
    env.seed(worker_pod(
        ns,
        "worker-live",
        pool,
        TEST_POD_UID,
        "10.0.0.9",
    ));

    env.s
        .enqueue_registered_workers(&CancellationToken::new())
        .await;
    for _ in 0..env.s.queue.len() {
        env.s.process_next_work_item().await;
    }
    assert!(
        reg.get(OTHER_POD_UID).is_none(),
        "orphan worker not removed"
    );
    assert!(
        reg.get(TEST_POD_UID).is_some(),
        "live worker was wrongly removed"
    );
}

#[tokio::test]
async fn enqueue_registered_workers_retries_transient_list_error() {
    let reg = FakeRegistry::new();
    reg.put(registered_worker(
        "ns-enq-retry",
        "pool1",
        "worker-1",
        TEST_POD_UID,
    ));
    let fails_left = Arc::new(AtomicUsize::new(1));
    let f = fails_left.clone();
    reg.set_list_hook(move |_| {
        if f.load(Ordering::SeqCst) > 0 {
            f.fetch_sub(1, Ordering::SeqCst);
            return Err(Status::unavailable("transient failure"));
        }
        Ok(())
    });
    let env = setup_reconcile(&reg, vec![]);
    let mut s = Arc::try_unwrap(env.s).ok().unwrap();
    s.list_backoff = Duration::from_millis(1);
    s.enqueue_registered_workers(&CancellationToken::new())
        .await;
    assert_eq!(
        fails_left.load(Ordering::SeqCst),
        0,
        "ListWorkers should have been retried"
    );
    assert_eq!(
        s.queue.len(),
        1,
        "the worker must be enqueued after the retry"
    );
}

#[tokio::test]
async fn list_workers_page_with_retry_stops_on_cancel() {
    let reg = FakeRegistry::new();
    reg.set_list_hook(|_| Err(Status::unavailable("still down")));
    let env = setup_reconcile(&reg, vec![]);
    let mut s = Arc::try_unwrap(env.s).ok().unwrap();
    s.list_backoff = Duration::from_millis(1);
    let stop = CancellationToken::new();
    let c = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        c.cancel();
    });
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        s.list_workers_page_with_retry(&stop, ""),
    )
    .await;
    let err = r.expect("returned after cancel").unwrap_err();
    assert!(err.contains("aborted"), "{err}");
}

#[tokio::test]
async fn enqueue_registered_workers_streams_pages_and_retries_late_page() {
    let reg = FakeRegistry::new();
    reg.set_list_page_size(1);
    for (i, uid) in [
        "aaaaaaaa-0000-0000-0000-000000000000",
        "bbbbbbbb-0000-0000-0000-000000000000",
        "cccccccc-0000-0000-0000-000000000000",
    ]
    .iter()
    .enumerate()
    {
        reg.put(registered_worker("ns", "p", &format!("w{i}"), uid));
    }
    let failed = Arc::new(AtomicBool::new(false));
    let f = failed.clone();
    reg.set_list_hook(move |token| {
        if token == "1" && !f.swap(true, Ordering::SeqCst) {
            return Err(Status::unavailable("transient failure on page"));
        }
        Ok(())
    });
    let env = setup_reconcile(&reg, vec![]);
    let mut s = Arc::try_unwrap(env.s).ok().unwrap();
    s.list_backoff = Duration::from_millis(1);
    s.enqueue_registered_workers(&CancellationToken::new())
        .await;
    assert!(
        failed.load(Ordering::SeqCst),
        "the page-2 failure was not exercised"
    );
    assert_eq!(
        s.queue.len(),
        3,
        "every worker across all pages must be enqueued"
    );
}

#[tokio::test]
async fn create_worker_before_pod_ip() {
    let (ns, pool) = ("ns-syncer-no-ip", "pool-no-ip");
    let reg = FakeRegistry::new();
    let env = setup_reconcile(&reg, vec![worker_pool(ns, pool, &[])]);
    let key = env.seed(worker_pod(ns, "worker-unit-no-ip", pool, TEST_POD_UID, ""));
    env.must_reconcile(&key).await;
    let got = reg
        .get(TEST_POD_UID)
        .expect("no worker registered for a scheduled pod with no IP");
    assert_eq!(got.node_name, "node1");
    assert_eq!(state(&Some(got.clone())), pb::WorkerState::Unavailable);
    assert!(
        got.status.unwrap().ips.is_empty(),
        "IPs are the worker's to report"
    );
}

#[tokio::test]
async fn create_worker_spiffe_id() {
    let (ns, pool) = ("ns-syncer-sa", "pool-sa");
    let reg = FakeRegistry::new();
    let env = setup_reconcile(&reg, vec![worker_pool(ns, pool, &[])]);
    let mut pod = worker_pod(ns, "worker-unit-sa", pool, TEST_POD_UID, "10.0.0.1");
    pod.spec.as_mut().unwrap().service_account_name = Some("hivekeep-worker".into());
    let key = env.seed(pod);
    env.must_reconcile(&key).await;
    assert_eq!(
        reg.get(TEST_POD_UID).unwrap().worker_spiffe_id,
        "spiffe://cluster.local/ns/ns-syncer-sa/sa/hivekeep-worker"
    );
}

#[tokio::test]
async fn update_worker_retry_on_version_conflict() {
    let (ns, pool) = ("ns-syncer-conflict", "pool-conflict");
    let reg = FakeRegistry::new();
    let env = setup_reconcile(&reg, vec![worker_pool(ns, pool, &[("foo", "bar")])]);
    let key = env.seed(worker_pod(
        ns,
        "worker-unit-conflict",
        pool,
        TEST_POD_UID,
        "10.0.0.1",
    ));
    env.must_reconcile(&key).await;
    assert!(reg.get(TEST_POD_UID).is_some(), "worker not registered");

    env.pools
        .cache_put(worker_pool(ns, pool, &[("foo", "baz")]));
    // Land a concurrent drain the moment the syncer updates: it is mutable
    // and not written by the update path, so it survives only if the retry
    // re-reads.
    let conflicted = Arc::new(AtomicBool::new(false));
    let (c, r) = (conflicted.clone(), reg.clone());
    reg.set_update_hook_once(move |name| {
        c.store(true, Ordering::SeqCst);
        let r = r.clone();
        let name = name.to_string();
        // The fake's drain is synchronous under its lock.
        futures::executor::block_on(r.drain_worker(&name)).unwrap();
    });
    let err = env.s.reconcile(&key).await.unwrap_err();
    assert_eq!(err.code(), Some(Code::Aborted), "{err}");
    assert!(
        conflicted.load(Ordering::SeqCst),
        "UpdateWorker never called"
    );

    env.must_reconcile(&key).await;
    let got = reg.get(TEST_POD_UID);
    assert_eq!(labels(&got)["foo"], "baz");
    assert_eq!(
        state(&got),
        pb::WorkerState::Draining,
        "the concurrent drain must survive the retry"
    );
}

#[tokio::test]
async fn missing_worker_pool() {
    let (ns, pool) = ("ns-syncer-missingpool", "pool-orphan");
    let reg = FakeRegistry::new();
    let env = setup_reconcile(&reg, vec![]);
    let key = env.seed(worker_pod(
        ns,
        "worker-orphan-1",
        pool,
        TEST_POD_UID,
        "10.0.0.5",
    ));
    env.must_reconcile(&key).await;
    assert!(
        reg.names().is_empty(),
        "nothing is written before the pool exists"
    );

    let wp = worker_pool(ns, pool, &[("foo", "bar")]);
    env.pools.cache_put(wp.clone());
    env.s.enqueue_worker_pool(&wp);
    assert_eq!(env.s.queue.len(), 1);
    env.must_reconcile(&key).await;
    assert_eq!(labels(&reg.get(TEST_POD_UID)), map(&[("foo", "bar")]));
}

fn controller_ref(kind: &str, name: &str, uid: &str) -> Vec<OwnerReference> {
    vec![OwnerReference {
        api_version: "v1".into(),
        kind: kind.into(),
        name: name.into(),
        uid: uid.into(),
        controller: Some(true),
        block_owner_deletion: None,
    }]
}

/// The Deployment and ReplicaSet owning `pod`, linked up to a WorkerPool.
fn pool_chain(pod: &mut Pod, pool: &str) -> (Deployment, ReplicaSet) {
    let ns = pod.metadata.namespace.clone();
    let mut dep = Deployment::default();
    dep.metadata.name = Some(pool.into());
    dep.metadata.namespace = ns.clone();
    dep.metadata.uid = Some("dep-uid".into());
    dep.metadata.owner_references = Some(controller_ref("WorkerPool", pool, "pool-uid"));
    let mut rs = ReplicaSet::default();
    rs.metadata.name = Some(format!("{pool}-abc"));
    rs.metadata.namespace = ns;
    rs.metadata.uid = Some("rs-uid".into());
    rs.metadata.owner_references = Some(controller_ref("Deployment", pool, "dep-uid"));
    pod.metadata.owner_references = Some(controller_ref(
        "ReplicaSet",
        &format!("{pool}-abc"),
        "rs-uid",
    ));
    (dep, rs)
}

fn now() -> Option<Time> {
    Some(Time(k8s_openapi::jiff::Timestamp::now()))
}

#[tokio::test]
async fn deleted_worker_pool() {
    let (ns, pool, pod_name, ip) = ("ns-deleted-pool", "pool1", "worker-1", "10.0.0.5");
    type Shape = fn(&mut Pod, &mut ReplicaSet, &mut Deployment) -> (bool, bool);
    // Each shape edits the intact chain and says which of (rs, dep) to store.
    let cases: Vec<(&str, Shape, bool)> = vec![
        ("background cascade", |_, _, _| (true, true), false),
        (
            "background cascade, deployment collected",
            |_, _, _| (true, false),
            false,
        ),
        (
            "background cascade, replicaset collected",
            |_, _, _| (false, false),
            false,
        ),
        (
            "replicaset replaced under its name",
            |_, rs, _| {
                rs.metadata.uid = Some("new-uid".into());
                (true, true)
            },
            false,
        ),
        (
            "deployment deleting, no longer owned by the pool",
            |_, _, dep| {
                dep.metadata.owner_references = None;
                dep.metadata.deletion_timestamp = now();
                dep.metadata.finalizers = Some(vec!["test/hold".into()]);
                (true, true)
            },
            false,
        ),
        (
            "replicaset deleting, deployment orphaned",
            |_, rs, dep| {
                dep.metadata.owner_references = None;
                rs.metadata.deletion_timestamp = now();
                rs.metadata.finalizers = Some(vec!["test/hold".into()]);
                (true, true)
            },
            false,
        ),
        (
            "orphaned deployment",
            |_, _, dep| {
                dep.metadata.owner_references = None;
                (true, true)
            },
            true,
        ),
        (
            "orphaned replicaset",
            |_, rs, _| {
                rs.metadata.owner_references = None;
                (true, true)
            },
            true,
        ),
        (
            "orphaned pod",
            |pod, _, _| {
                pod.metadata.owner_references = None;
                (true, true)
            },
            true,
        ),
    ];
    for (name, shape, deleted) in cases {
        let reg = FakeRegistry::new();
        reg.put(registered_worker(ns, pool, pod_name, TEST_POD_UID));
        let fake = FakeApiServer::new();
        let mut pod = worker_pod(ns, pod_name, pool, TEST_POD_UID, ip);
        let (mut dep, mut rs) = pool_chain(&mut pod, pool);
        let (store_rs, store_dep) = shape(&mut pod, &mut rs, &mut dep);
        if store_rs {
            fake.insert(&rs);
        }
        if store_dep {
            fake.insert(&dep);
        }
        fake.insert(&pod);
        let env = setup_reconcile_with(&reg, vec![], fake);
        let key = env.seed(pod);
        env.must_reconcile(&key).await;

        let got = reg.get(TEST_POD_UID);
        assert!(got.is_some(), "{name}: worker deleted outright");
        let gone = env.fake.get::<Pod>(ns, pod_name).is_none();
        assert_eq!(gone, deleted, "{name}: pod deleted");
        let want = if deleted {
            pb::WorkerState::Draining
        } else {
            pb::WorkerState::Active
        };
        assert_eq!(state(&got), want, "{name}");
    }
}

#[tokio::test]
async fn deleted_worker_pool_not_ready_pod() {
    let (ns, pool, pod_name) = ("ns-deleted-pool-notready", "pool1", "worker-1");
    let reg = FakeRegistry::new();
    reg.put(registered_worker(ns, pool, pod_name, TEST_POD_UID));
    let fake = FakeApiServer::new();
    let mut pod = worker_pod(ns, pod_name, pool, TEST_POD_UID, "10.0.0.5");
    set_ready(&mut pod, None);
    fake.insert(&pod);
    let env = setup_reconcile_with(&reg, vec![], fake);
    let key = env.seed(pod);
    env.must_reconcile(&key).await;
    assert!(
        env.fake.get::<Pod>(ns, pod_name).is_none(),
        "orphaned not-Ready pod left"
    );
    assert_eq!(state(&reg.get(TEST_POD_UID)), pb::WorkerState::Draining);
}

#[tokio::test]
async fn deleted_worker_pool_errors() {
    let (ns, pool, pod_name) = ("ns-deleted-pool-err", "pool1", "worker-1");
    let reg = FakeRegistry::new();
    reg.put(registered_worker(ns, pool, pod_name, TEST_POD_UID));
    let fake = FakeApiServer::new();
    let mut pod = worker_pod(ns, pod_name, pool, TEST_POD_UID, "10.0.0.5");
    pool_chain(&mut pod, pool);
    fake.insert(&pod);
    fake.prepend_reactor("get", "replicasets", |_| {
        Reaction::fail(503, "ServiceUnavailable", "apiserver unavailable")
    });
    let env = setup_reconcile_with(&reg, vec![], fake);
    let key = env.seed(pod);
    assert!(
        env.s.reconcile(&key).await.is_err(),
        "failing owner reads must requeue"
    );
    assert!(
        env.fake.get::<Pod>(ns, pod_name).is_some(),
        "pod must be left alone"
    );
    assert_eq!(state(&reg.get(TEST_POD_UID)), pb::WorkerState::Active);
}

#[tokio::test]
async fn deleted_worker_pool_replacement_pod() {
    let (ns, pool, pod_name) = ("ns-deleted-pool-repl", "pool1", "worker-2");
    let reg = FakeRegistry::new();
    let fake = FakeApiServer::new();
    let pod = worker_pod(ns, pod_name, pool, OTHER_POD_UID, "10.0.0.6");
    fake.insert(&pod);
    let env = setup_reconcile_with(&reg, vec![], fake);
    let key = env.seed(pod);
    env.must_reconcile(&key).await;
    assert!(
        env.fake.get::<Pod>(ns, pod_name).is_some(),
        "replacement pod must be left alone"
    );
    assert!(reg.names().is_empty());
}

#[tokio::test]
async fn orphaned_pods_of_deleted_pool_via_informer() {
    let (ns, pool, pod_name) = ("ns-syncer-orphaned", "pool1", "worker-orphaned-1");
    let reg = FakeRegistry::new();
    let env = setup_syncer(&reg, vec![worker_pool(ns, pool, &[])]);
    env.fake
        .insert(&worker_pod(ns, pod_name, pool, TEST_POD_UID, "10.0.0.7"));
    wait_for_worker(&reg, TEST_POD_UID, active).await;

    Api::<WorkerPool>::namespaced(env.fake.client(), ns)
        .delete(pool, &DeleteParams::default())
        .await
        .unwrap();
    wait_for_worker(&reg, TEST_POD_UID, Option::is_none).await;
    assert!(
        env.fake.get::<Pod>(ns, pod_name).is_none(),
        "orphaned pod not deleted"
    );
}

#[tokio::test]
async fn start_waits_for_worker_pool_cache() {
    let (ns, pool, pod_name) = ("ns-syncer-poolsync", "pool1", "worker-poolsync-1");
    let reg = FakeRegistry::new();
    reg.put(registered_worker(ns, pool, pod_name, TEST_POD_UID));
    let fake = FakeApiServer::new();
    fake.insert(&worker_pod(ns, pod_name, pool, TEST_POD_UID, "10.0.0.8"));
    fake.insert(&worker_pool(ns, pool, &[("foo", "bar")]));
    let pods = worker_pod_informer(fake.client());
    let pools = worker_pool_informer(fake.client());
    let stop = CancellationToken::new();
    WorkerPoolSyncer::new(
        Arc::new(reg.clone()),
        fake.client(),
        pods.clone(),
        pools.clone(),
        2,
    )
    .start(stop.clone());
    let pod_task = pods.spawn();
    pods.wait_synced().await;

    // Pod cache warm, pool cache cold: nothing may shut the worker down.
    holds_for(Duration::from_millis(300), || {
        if active(&reg.get(TEST_POD_UID)) {
            Ok(())
        } else {
            Err("worker shut down before the WorkerPool cache synced".into())
        }
    })
    .await;

    let pool_task = pools.spawn();
    let want = map(&[("foo", "bar")]);
    wait_for_worker(&reg, TEST_POD_UID, |w| w.is_some() && labels(w) == want).await;
    stop.cancel();
    pod_task.abort();
    pool_task.abort();
}

#[tokio::test]
async fn enqueue_worker_pool() {
    let env = setup_reconcile(&FakeRegistry::new(), vec![]);
    let pool = worker_pool("ns-pool-update", "pool-a", &[]);
    let first = env.seed(worker_pod(
        "ns-pool-update",
        "worker-1",
        "pool-a",
        TEST_POD_UID,
        "10.0.0.1",
    ));
    let second = env.seed(worker_pod(
        "ns-pool-update",
        "worker-2",
        "pool-a",
        OTHER_POD_UID,
        "10.0.0.2",
    ));
    env.seed(worker_pod(
        "ns-pool-update",
        "other-pool-worker",
        "pool-b",
        "33333333-3333-3333-3333-333333333333",
        "10.0.0.3",
    ));

    env.s.enqueue_worker_pool(&pool);
    assert_eq!(env.s.queue.len(), 2);
    let mut got = Vec::new();
    for _ in 0..2 {
        let k = env.s.queue.get().await.unwrap();
        env.s.queue.done(&k);
        got.push(k);
    }
    assert!(got.contains(&first) && got.contains(&second), "{got:?}");
}

#[tokio::test]
async fn enqueue_worker_pool_on_missed_delete() {
    // A pool delete the informer missed arrives as the last known pool
    // (there are no untyped tombstones here); its pods are still enqueued.
    let reg = FakeRegistry::new();
    let fake = FakeApiServer::new();
    let pools = worker_pool_informer(fake.client());
    let env = setup_reconcile_with(&reg, vec![], fake.clone());
    env.seed(worker_pod(
        "ns-pool-tombstone",
        "worker-1",
        "pool-a",
        TEST_POD_UID,
        "10.0.0.1",
    ));
    let s = WorkerPoolSyncer::new(
        Arc::new(reg),
        fake.client(),
        env.pods.clone(),
        pools.clone(),
        1,
    );
    let stop = CancellationToken::new();
    s.start(stop.clone());
    fake.insert(&worker_pool("ns-pool-tombstone", "pool-a", &[]));
    let task = pools.spawn();
    pools.wait_synced().await;
    let added = s.queue.len();
    assert!(added <= 1);
    fake.remove(&hivekeep_k8s::fake::ObjectKey {
        group: "hivekeep.io".into(),
        resource: "workerpools".into(),
        namespace: "ns-pool-tombstone".into(),
        name: "pool-a".into(),
    });
    for _ in 0..100 {
        if s.queue.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(s.queue.len(), 1, "the deleted pool's pod must be queued");
    stop.cancel();
    task.abort();
}

#[tokio::test]
async fn soft_delete_via_informer() {
    let (ns, pool, pod_name) = ("ns-syncer-softdelete", "pool1", "worker-soft-1");
    let reg = FakeRegistry::new();
    let env = setup_syncer(&reg, vec![worker_pool(ns, pool, &[])]);
    let mut pod = worker_pod(ns, pod_name, pool, TEST_POD_UID, "10.0.0.6");
    // A finalizer keeps the pod Terminating, as graceful termination does.
    pod.metadata.finalizers = Some(vec!["test/graceful".into()]);
    env.fake.insert(&pod);
    wait_for_worker(&reg, TEST_POD_UID, active).await;
    pods_api(&env.fake, ns)
        .delete(pod_name, &DeleteParams::default())
        .await
        .unwrap();
    wait_for_worker(&reg, TEST_POD_UID, |w| {
        state(w) == pb::WorkerState::Draining
    })
    .await;
}

#[tokio::test]
async fn terminal_pod_deletes_pod() {
    let (ns, pool, pod_name) = ("ns-terminal", "pool1", "worker-terminal");
    for phase in ["Failed", "Succeeded"] {
        let reg = FakeRegistry::new();
        reg.put(registered_worker(ns, pool, pod_name, TEST_POD_UID));
        let fake = FakeApiServer::new();
        let mut pod = worker_pod(ns, pod_name, pool, TEST_POD_UID, "10.0.0.20");
        pod.status.as_mut().unwrap().phase = Some(phase.into());
        fake.insert(&pod);
        let env = setup_reconcile_with(&reg, vec![worker_pool(ns, pool, &[])], fake);
        let key = env.seed(pod);
        env.must_reconcile(&key).await;
        assert!(
            env.fake.get::<Pod>(ns, pod_name).is_none(),
            "{phase}: pod not deleted"
        );
        let deletes: Vec<_> = env
            .fake
            .actions()
            .into_iter()
            .filter(|a| a.is("delete", "pods"))
            .collect();
        assert_eq!(deletes.len(), 1, "{phase}");
        assert_eq!(
            deletes[0]
                .body
                .as_ref()
                .and_then(|b| b.pointer("/preconditions/uid"))
                .and_then(|v| v.as_str()),
            Some(TEST_POD_UID),
            "{phase}: delete preconditions"
        );
        assert_eq!(
            state(&reg.get(TEST_POD_UID)),
            pb::WorkerState::Draining,
            "{phase}"
        );
    }
}

#[tokio::test]
async fn terminal_pod_already_gone_or_replaced() {
    for (name, code, reason) in [
        ("not found", 404, "NotFound"),
        ("conflict", 409, "Conflict"),
    ] {
        let fake = FakeApiServer::new();
        fake.prepend_reactor("delete", "pods", move |_| {
            Reaction::fail(code, reason, "gone")
        });
        let env = setup_reconcile_with(&FakeRegistry::new(), vec![], fake);
        let mut pod = worker_pod(
            "ns-terminal-gone",
            "worker-terminal-gone",
            "pool1",
            TEST_POD_UID,
            "",
        );
        pod.status = Some(serde_json::from_value(json!({"phase": "Failed"})).unwrap());
        let key = env.seed(pod);
        env.s
            .reconcile(&key)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}

#[tokio::test]
async fn terminal_pod_via_informer() {
    let (ns, pool, pod_name) = ("ns-syncer-terminal", "pool1", "worker-terminal-1");
    let reg = FakeRegistry::new();
    let env = setup_syncer(&reg, vec![worker_pool(ns, pool, &[])]);
    let pod = env
        .fake
        .insert(&worker_pod(ns, pod_name, pool, TEST_POD_UID, "10.0.0.21"));
    wait_for_worker(&reg, TEST_POD_UID, active).await;
    let mut failed = pod.clone();
    failed.status.as_mut().unwrap().phase = Some("Failed".into());
    set_ready(&mut failed, Some("False"));
    failed.metadata.resource_version = None;
    pods_api(&env.fake, ns)
        .replace_status(pod_name, &PostParams::default(), &failed)
        .await
        .unwrap();
    wait_for_worker(&reg, TEST_POD_UID, Option::is_none).await;
    assert!(env.fake.get::<Pod>(ns, pod_name).is_none());
}

#[tokio::test]
async fn pod_recreated_with_new_uid() {
    let (ns, pool, pod_name) = ("ns-syncer-recreate", "pool1", "worker-recreate-1");
    let reg = FakeRegistry::new();
    let env = setup_reconcile(&reg, vec![worker_pool(ns, pool, &[])]);
    let old_key = env.seed(worker_pod(ns, pod_name, pool, TEST_POD_UID, "10.0.0.7"));
    env.must_reconcile(&old_key).await;
    assert!(reg.get(TEST_POD_UID).is_some());

    let new_key = env.seed(worker_pod(ns, pod_name, pool, OTHER_POD_UID, "10.0.0.8"));
    env.must_reconcile(&old_key).await;
    env.must_reconcile(&new_key).await;
    assert_eq!(
        reg.get(OTHER_POD_UID).unwrap().worker_pod_uid,
        OTHER_POD_UID
    );
    assert_eq!(reg.names(), vec![OTHER_POD_UID.to_string()]);
}

#[tokio::test]
async fn delete_unscheduled_pod() {
    let (ns, pool, pod_name) = ("ns-syncer-unscheduled", "pool1", "worker-unscheduled-1");
    let reg = FakeRegistry::new();
    let env = setup_reconcile(&reg, vec![worker_pool(ns, pool, &[])]);
    let mut pod = worker_pod(ns, pod_name, pool, TEST_POD_UID, "");
    pod.spec.as_mut().unwrap().node_name = None;
    let key = env.seed(pod);
    env.must_reconcile(&key).await;
    assert!(reg.names().is_empty());
    env.pods.cache_remove(ns, pod_name);
    env.must_reconcile(&key).await;
    assert!(reg.names().is_empty());
}

#[tokio::test]
async fn pod_with_ip_but_not_ready_is_unavailable() {
    for (name, ready) in [("no Ready condition", None), ("Ready=False", Some("False"))] {
        let reg = FakeRegistry::new();
        let env = setup_reconcile(&reg, vec![worker_pool("ns-syncer-notready", "pool1", &[])]);
        let mut pod = worker_pod(
            "ns-syncer-notready",
            "worker-notready-1",
            "pool1",
            TEST_POD_UID,
            "10.0.0.9",
        );
        set_ready(&mut pod, ready);
        let key = env.seed(pod);
        env.must_reconcile(&key).await;
        let got = reg.get(TEST_POD_UID);
        assert!(got.is_some(), "{name}: not registered");
        assert_eq!(state(&got), pb::WorkerState::Unavailable, "{name}");
    }
}

#[tokio::test]
async fn delete_never_ready_pod() {
    let (ns, pool, pod_name) = ("ns-syncer-neverready", "pool1", "worker-neverready-1");
    let reg = FakeRegistry::new();
    let env = setup_reconcile(&reg, vec![worker_pool(ns, pool, &[])]);
    let mut pod = worker_pod(ns, pod_name, pool, TEST_POD_UID, "10.0.0.9");
    set_ready(&mut pod, None);
    let key = env.seed(pod);
    env.must_reconcile(&key).await;
    assert_eq!(state(&reg.get(TEST_POD_UID)), pb::WorkerState::Unavailable);
    env.pods.cache_remove(ns, pod_name);
    env.must_reconcile(&key).await;
    assert!(reg.names().is_empty());
}

#[tokio::test]
async fn registered_pod_without_ip_is_unavailable() {
    let (ns, pool, pod_name) = ("ns-syncer-lostip", "pool1", "worker-lostip-1");
    let reg = FakeRegistry::new();
    let env = setup_reconcile(&reg, vec![worker_pool(ns, pool, &[])]);
    let key = env.seed(worker_pod(ns, pod_name, pool, TEST_POD_UID, "10.0.0.9"));
    env.must_reconcile(&key).await;
    env.seed(worker_pod(ns, pod_name, pool, TEST_POD_UID, ""));
    env.must_reconcile(&key).await;
    assert_eq!(state(&reg.get(TEST_POD_UID)), pb::WorkerState::Unavailable);
}

#[tokio::test]
async fn readiness_flap_does_not_deregister() {
    let (ns, pool, pod_name) = ("ns-syncer-flap", "pool1", "worker-flap-1");
    let reg = FakeRegistry::new();
    let env = setup_reconcile(&reg, vec![worker_pool(ns, pool, &[])]);
    let pod = worker_pod(ns, pod_name, pool, TEST_POD_UID, "10.0.0.9");
    let key = env.seed(pod.clone());
    env.must_reconcile(&key).await;
    assert!(reg.get(TEST_POD_UID).is_some());

    let mut not_ready = pod.clone();
    set_ready(&mut not_ready, Some("False"));
    env.seed(not_ready);
    env.must_reconcile(&key).await;
    let got = reg.get(TEST_POD_UID);
    assert!(got.is_some(), "deregistered on a readiness flap");
    assert_eq!(state(&got), pb::WorkerState::Unavailable);

    env.seed(pod);
    env.must_reconcile(&key).await;
    assert_eq!(state(&reg.get(TEST_POD_UID)), pb::WorkerState::Active);
}

#[tokio::test]
async fn reconcile_errors_are_classified() {
    for (name, code, want_requeues) in [
        ("invalid argument is dropped", Code::InvalidArgument, 0),
        ("unavailable requeues", Code::Unavailable, 1),
    ] {
        let reg = FakeRegistry::new();
        reg.set_create_hook(move |_| Err(Status::new(code, "rejected")));
        let env = setup_reconcile(&reg, vec![worker_pool("ns-syncer-classify", "pool1", &[])]);
        let key = env.seed(worker_pod(
            "ns-syncer-classify",
            "worker-classify-1",
            "pool1",
            TEST_POD_UID,
            "10.0.0.9",
        ));
        env.s.queue.add(key.clone());
        assert!(env.s.process_next_work_item().await, "queue shut down");
        assert_eq!(env.s.queue.failures(&key), want_requeues, "{name}");
        assert!(
            reg.names().is_empty(),
            "{name}: nothing written for a rejected create"
        );
    }
}

#[test]
fn availability_and_terminal_phase() {
    let mut pod = worker_pod("ns", "p", "pool", TEST_POD_UID, "10.0.0.1");
    assert!(super::syncer::is_worker_available(&pod));
    set_ready(&mut pod, Some("False"));
    assert!(!super::syncer::is_worker_available(&pod));
    assert!(!super::syncer::is_worker_available(&worker_pod(
        "ns",
        "p",
        "pool",
        TEST_POD_UID,
        ""
    )));
    for (phase, terminal) in [
        ("Running", false),
        ("Pending", false),
        ("Failed", true),
        ("Succeeded", true),
    ] {
        pod.status.as_mut().unwrap().phase = Some(phase.into());
        assert_eq!(super::syncer::is_pod_terminal(&pod), terminal, "{phase}");
    }
}

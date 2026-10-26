use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use hivekeep_k8s::crd::{
    SandboxClass, WorkerPool, WorkerPoolPodTemplate, WorkerPoolSandboxClass, WorkerPoolSpec,
    WorkerPoolStatus,
};
use hivekeep_proto::api::v1 as api;
use k8s_openapi::api::core::v1::ResourceRequirements;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::ResourceExt;
use parking_lot::Mutex;
use tonic::{Code, Status};

use super::*;
use crate::isolate::fake_workersync::ports::{
    CapacityRelay, Cluster, Node, WorkerClient, WorkerReport,
};
use crate::isolate::fake_workersync::template::template_of;
use crate::isolate::fakeworker::{self, MAX_ACTORS_ANNOTATION};
use crate::isolate::quantity;

const TEST_RUN: &str = "r1";
const TEST_DRAIN_GRACE: Duration = Duration::from_secs(60);
const NS: &str = "benchmark-workloads";

// ---------------------------------------------------------------- fakes

#[derive(Default)]
struct ControlState {
    workers: HashMap<String, api::Worker>,
    /// Actors assigned, by Worker name.
    assignments: HashMap<String, usize>,
    creates: usize,
    updates: usize,
    /// Returned by CreateWorker after the Worker is stored, as when the
    /// server commits a create whose reply never arrives.
    lost_reply: Option<Code>,
    /// Fails every DrainWorker.
    drain_err: Option<Code>,
    /// Fails every MarkWorkerAvailable.
    mark_err: Option<Code>,
    /// Fails that many ListWorkers calls with `list_code` (Unavailable when
    /// unset) before any succeeds.
    list_failures: usize,
    list_code: Option<Code>,
}

/// An in-memory Worker registry.
#[derive(Default)]
struct FakeControl {
    s: Mutex<ControlState>,
}

fn state_of(w: &api::Worker) -> api::WorkerState {
    w.status.as_ref().map(|s| s.state()).unwrap_or_default()
}

fn wname(w: &api::Worker) -> String {
    w.metadata
        .as_ref()
        .map(|m| m.name.clone())
        .unwrap_or_default()
}

fn ref_name(r: &Option<api::ObjectRef>) -> String {
    r.as_ref().map(|r| r.name.clone()).unwrap_or_default()
}

impl FakeControl {
    /// The pod names of the registered Workers, sorted, once per Worker.
    fn names(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .s
            .lock()
            .workers
            .values()
            .map(|w| w.worker_pod.clone())
            .collect();
        out.sort();
        out
    }

    /// The Worker registered for `pod`: the one not draining, if any.
    fn worker(&self, pod: &str) -> Option<api::Worker> {
        let s = self.s.lock();
        let mut found: Option<&api::Worker> = None;
        for w in s.workers.values() {
            if w.worker_pod != pod {
                continue;
            }
            if found.is_none() || state_of(w) != api::WorkerState::Draining {
                found = Some(w);
            }
        }
        found.cloned()
    }

    fn uid(&self, pod: &str) -> String {
        self.worker(pod).map(|w| wname(&w)).unwrap_or_default()
    }

    fn state(&self, pod: &str) -> api::WorkerState {
        self.worker(pod).map(|w| state_of(&w)).unwrap_or_default()
    }

    fn draining(&self, pod: &str) -> Option<api::Worker> {
        self.s
            .lock()
            .workers
            .values()
            .find(|w| w.worker_pod == pod && state_of(w) == api::WorkerState::Draining)
            .cloned()
    }

    fn all(&self) -> Vec<api::Worker> {
        self.s.lock().workers.values().cloned().collect()
    }

    fn set_assigned(&self, name: &str, n: usize) {
        self.s.lock().assignments.insert(name.to_string(), n);
    }
}

#[async_trait]
impl WorkerClient for FakeControl {
    async fn create_worker(&self, req: api::CreateWorkerRequest) -> Result<api::Worker, Status> {
        let mut s = self.s.lock();
        s.creates += 1;
        let mut w = req.worker.unwrap_or_default();
        let name = wname(&w);
        if s.workers.contains_key(&name) {
            return Err(Status::already_exists(format!(
                "Worker {name} already exists"
            )));
        }
        // Like hivekeep-api: a Worker takes no Actors until it is marked
        // available.
        w.status = Some(api::WorkerStatus {
            state: api::WorkerState::Unavailable as i32,
            ..Default::default()
        });
        s.workers.insert(name, w.clone());
        if let Some(code) = s.lost_reply {
            return Err(Status::new(code, "reply lost"));
        }
        Ok(w)
    }

    async fn delete_worker(&self, req: api::DeleteWorkerRequest) -> Result<api::Worker, Status> {
        let mut s = self.s.lock();
        let name = ref_name(&req.worker);
        let w = s
            .workers
            .remove(&name)
            .ok_or_else(|| Status::not_found(format!("Worker {name} not found")))?;
        // Like hivekeep-api: deleting a Worker releases its Actors.
        s.assignments.remove(&name);
        Ok(w)
    }

    async fn drain_worker(
        &self,
        req: api::DrainWorkerRequest,
    ) -> Result<api::DrainWorkerResponse, Status> {
        let mut s = self.s.lock();
        if let Some(code) = s.drain_err {
            return Err(Status::new(code, "connection reset"));
        }
        let w = s
            .workers
            .get_mut(&ref_name(&req.worker))
            .ok_or_else(|| Status::not_found("no such Worker"))?;
        w.status.get_or_insert_with(Default::default).state = api::WorkerState::Draining as i32;
        Ok(api::DrainWorkerResponse {
            worker: Some(w.clone()),
        })
    }

    async fn mark_worker_available(
        &self,
        req: api::MarkWorkerAvailableRequest,
    ) -> Result<api::MarkWorkerAvailableResponse, Status> {
        let mut s = self.s.lock();
        if let Some(code) = s.mark_err {
            return Err(Status::new(code, "connection reset"));
        }
        let w = s
            .workers
            .get_mut(&ref_name(&req.worker))
            .ok_or_else(|| Status::not_found("no such Worker"))?;
        if state_of(w) == api::WorkerState::Draining {
            return Err(Status::failed_precondition("Worker is draining"));
        }
        w.status.get_or_insert_with(Default::default).state = api::WorkerState::Active as i32;
        Ok(api::MarkWorkerAvailableResponse {
            worker: Some(w.clone()),
        })
    }

    async fn get_worker(&self, req: api::GetWorkerRequest) -> Result<api::Worker, Status> {
        self.s
            .lock()
            .workers
            .get(&ref_name(&req.worker))
            .cloned()
            .ok_or_else(|| Status::not_found("no such Worker"))
    }

    /// Only the labels may change, as hivekeep-api's immutable-field
    /// validation enforces for the fields the controller sets.
    async fn update_worker(&self, req: api::UpdateWorkerRequest) -> Result<api::Worker, Status> {
        let mut s = self.s.lock();
        s.updates += 1;
        let w = req.worker.unwrap_or_default();
        let name = wname(&w);
        let old = s
            .workers
            .get(&name)
            .ok_or_else(|| Status::not_found("no such Worker"))?;
        let (mut a, mut b) = (old.clone(), w.clone());
        a.labels.clear();
        b.labels.clear();
        if a != b {
            return Err(Status::invalid_argument(format!(
                "Worker {name}: only labels may change"
            )));
        }
        s.workers.insert(name, w.clone());
        Ok(w)
    }

    async fn list_workers(
        &self,
        _req: api::ListWorkersRequest,
    ) -> Result<api::ListWorkersResponse, Status> {
        let mut s = self.s.lock();
        if s.list_failures > 0 {
            s.list_failures -= 1;
            return Err(match s.list_code {
                Some(code) => Status::new(code, "refused"),
                None => Status::unavailable("no ready endpoints"),
            });
        }
        Ok(api::ListWorkersResponse {
            workers: s.workers.values().cloned().collect(),
            next_page_token: String::new(),
        })
    }

    async fn list_worker_actor_assignments(
        &self,
        req: api::ListWorkerActorAssignmentsRequest,
    ) -> Result<api::ListWorkerActorAssignmentsResponse, Status> {
        let s = self.s.lock();
        let name = ref_name(&req.worker);
        if !s.workers.contains_key(&name) {
            return Err(Status::not_found("no such Worker"));
        }
        let n = s.assignments.get(&name).copied().unwrap_or(0);
        Ok(api::ListWorkerActorAssignmentsResponse {
            actor_assignments: vec![Default::default(); n],
            next_page_token: String::new(),
        })
    }
}

#[derive(Default)]
struct RelayState {
    reported: HashMap<String, Option<api::WorkerResources>>,
    classes: HashMap<String, Vec<api::WorkerSandboxClass>>,
    via: HashMap<String, String>,
    ips: HashMap<String, Vec<String>>,
    epochs: HashMap<String, i64>,
    acquired: HashMap<String, usize>,
    reject: HashMap<String, Code>,
    calls: usize,
    live: HashMap<String, String>,
}

/// Records accepted reports, as hivekeep-api's view of capacity, and
/// refuses Workers listed in `reject`. A report with no epoch acquires the
/// Worker's next one. An accepted report is also written to the control's
/// Worker, as RegisterWorker and SetWorkerSandboxRuntimes record it.
struct FakeRelay {
    control: Arc<FakeControl>,
    s: Mutex<RelayState>,
}

impl FakeRelay {
    fn reported(&self, name: &str) -> Option<api::WorkerResources> {
        self.s.lock().reported.get(name).cloned().flatten()
    }
    fn calls(&self) -> usize {
        self.s.lock().calls
    }
}

#[async_trait]
impl CapacityRelay for FakeRelay {
    async fn report(&self, addr: &str, report: WorkerReport) -> Result<(), Status> {
        let mut s = self.s.lock();
        s.calls += 1;
        let name = report.name.clone();
        if let Some(code) = s.reject.get(&name) {
            return Err(Status::new(*code, "rejected"));
        }
        let mut epoch = report.epoch;
        if epoch == 0 {
            *s.acquired.entry(name.clone()).or_default() += 1;
            epoch = s.epochs.get(&name).copied().unwrap_or(0) + 1;
        }
        s.epochs.insert(name.clone(), epoch);
        s.reported.insert(name.clone(), report.capacity);
        s.classes
            .insert(name.clone(), report.sandbox_classes.clone());
        s.ips.insert(name.clone(), report.ips.clone());
        s.via.insert(name.clone(), addr.to_string());
        if let Some(w) = self.control.s.lock().workers.get_mut(&name) {
            let st = w.status.get_or_insert_with(Default::default);
            st.registered_epoch = epoch;
            st.ips = report.ips;
            st.capacity = report.capacity;
            st.sandbox_classes = report.sandbox_classes;
        }
        Ok(())
    }

    fn prune(&self, live: &HashMap<String, String>) {
        self.s.lock().live = live.clone();
    }
}

#[derive(Default)]
struct ClusterState {
    pools: Vec<WorkerPool>,
    nodes: Vec<Node>,
    relays: HashMap<String, String>,
    statuses: HashMap<String, WorkerPoolStatus>,
    updates: usize,
}

/// The controller's Kubernetes, in memory.
#[derive(Default)]
struct FakeCluster {
    s: Mutex<ClusterState>,
}

impl FakeCluster {
    fn edit_pool(&self, name: &str, edit: impl Fn(&mut WorkerPool)) {
        for p in self.s.lock().pools.iter_mut() {
            if p.name_any() == name {
                edit(p);
            }
        }
    }
    fn set_replicas(&self, pool: &str, n: i32) {
        self.edit_pool(pool, |p| p.spec.replicas = n);
    }
    fn status(&self, pool: &str) -> WorkerPoolStatus {
        self.s
            .lock()
            .statuses
            .get(pool)
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait]
impl Cluster for FakeCluster {
    async fn pools(&self) -> Result<Vec<WorkerPool>, String> {
        Ok(self.s.lock().pools.clone())
    }
    async fn nodes(&self) -> Result<Vec<Node>, String> {
        Ok(self.s.lock().nodes.clone())
    }
    async fn relays(&self) -> Result<HashMap<String, String>, String> {
        Ok(self.s.lock().relays.clone())
    }
    async fn update_status(&self, wp: &WorkerPool) -> Result<(), String> {
        let mut s = self.s.lock();
        s.updates += 1;
        s.statuses
            .insert(wp.name_any(), wp.status.clone().unwrap_or_default());
        // Like the API server: the next List returns the written status.
        for p in s.pools.iter_mut() {
            if p.name_any() == wp.name_any() && p.namespace() == wp.namespace() {
                *p = wp.clone();
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------- helpers

fn res(pairs: &[(&str, &str)]) -> BTreeMap<String, Quantity> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Quantity(v.to_string())))
        .collect()
}

fn pool(name: &str, replicas: i32, limits: Option<BTreeMap<String, Quantity>>) -> WorkerPool {
    let mut wp = WorkerPool::new(
        name,
        WorkerPoolSpec {
            replicas,
            sandbox_classes: vec![WorkerPoolSandboxClass {
                name: SandboxClass::Gvisor,
                config_ref: None,
            }],
            template: limits.map(|l| WorkerPoolPodTemplate {
                resources: Some(ResourceRequirements {
                    limits: Some(l),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    wp.metadata.namespace = Some(NS.into());
    wp.metadata.labels = Some([("workload".to_string(), name.to_string())].into());
    wp
}

fn test_nodes() -> Vec<Node> {
    vec![
        Node {
            name: "node-a".into(),
            allocatable: res(&[("cpu", "86"), ("memory", "160Gi")]),
        },
        Node {
            name: "node-b".into(),
            allocatable: res(&[("cpu", "44"), ("memory", "80Gi")]),
        },
    ]
}

struct Env {
    c: Controller,
    ctl: Arc<FakeControl>,
    rel: Arc<FakeRelay>,
    cl: Arc<FakeCluster>,
}

impl Env {
    /// A controller restarted over the same registry, relay and cluster.
    fn restarted(&self) -> Controller {
        Controller::new(
            self.ctl.clone(),
            self.rel.clone(),
            self.cl.clone(),
            TEST_RUN,
            4,
            TEST_DRAIN_GRACE,
        )
    }

    /// Moves the controller's clock forward by `d`.
    fn advance(&mut self, d: Duration) {
        let t = (self.c.now)() + d;
        self.c.now = Box::new(move || t);
    }

    async fn reconcile(&self) -> Result<(), String> {
        self.c.reconcile().await
    }
}

fn new_env(pools: Vec<WorkerPool>) -> Env {
    let ctl = Arc::new(FakeControl::default());
    let rel = Arc::new(FakeRelay {
        control: ctl.clone(),
        s: Mutex::new(RelayState::default()),
    });
    let cl = Arc::new(FakeCluster::default());
    {
        let mut s = cl.s.lock();
        s.pools = pools;
        s.nodes = test_nodes();
        s.relays = [
            ("node-a".to_string(), "10.0.0.1:8086".to_string()),
            ("node-b".to_string(), "10.0.0.2:8086".to_string()),
        ]
        .into();
    }
    let mut c = Controller::new(
        ctl.clone(),
        rel.clone(),
        cl.clone(),
        TEST_RUN,
        4,
        TEST_DRAIN_GRACE,
    );
    let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1_767_225_600);
    c.now = Box::new(move || start);
    let uids = Arc::new(AtomicI64::new(0));
    c.new_uid = Box::new(move || format!("worker-{}", uids.fetch_add(1, Ordering::SeqCst) + 1));
    Env { c, ctl, rel, cl }
}

fn pod_name(pool: &str, i: usize) -> String {
    fakeworker::name(TEST_RUN, NS, pool, i)
}

fn pool_names(pool: &str, n: usize) -> Vec<String> {
    let mut out: Vec<String> = (0..n).map(|i| pod_name(pool, i)).collect();
    out.sort();
    out
}

fn want_capacity(actors: i32, cpu: &str, mem: &str) -> Option<api::WorkerResources> {
    Some(api::WorkerResources {
        actors,
        cpu_millicores: quantity::milli_value(cpu).unwrap(),
        memory_bytes: quantity::value(mem).unwrap(),
    })
}

fn sandbox_class_of(w: &api::Worker) -> String {
    w.status
        .as_ref()
        .and_then(|s| s.sandbox_classes.first())
        .map(|c| c.sandbox_class.clone())
        .unwrap_or_default()
}

fn status(replicas: i32, ready: i32, pool: &str) -> WorkerPoolStatus {
    WorkerPoolStatus {
        replicas,
        ready_replicas: ready,
        selector: format!("hivekeep.io/worker-pool={pool}"),
        ..Default::default()
    }
}

// ---------------------------------------------------------------- tests

#[tokio::test]
async fn reconcile_honors_replicas_and_limits() {
    let e = new_env(vec![pool(
        "bench",
        3,
        Some(res(&[("cpu", "1500m"), ("memory", "4Gi")])),
    )]);
    e.reconcile().await.unwrap();
    assert_eq!(e.ctl.names(), pool_names("bench", 3));

    let name = pod_name("bench", 1);
    let w = e.ctl.worker(&name).unwrap();
    assert_eq!(w.node_name, "node-b", "round robin");
    assert_eq!(
        (w.worker_namespace.as_str(), w.worker_pool.as_str()),
        (NS, "bench")
    );
    assert_eq!(sandbox_class_of(&w), "gvisor");
    assert_eq!(w.labels.get("workload").map(String::as_str), Some("bench"));
    assert_eq!(w.worker_pod_uid, wname(&w));

    let uid = e.ctl.uid(&name);
    let s = e.rel.s.lock();
    assert_eq!(s.via[&uid], "10.0.0.2:8086");
    // A new Worker has no epoch, so its fake worker process acquires one
    // and registers with its fake pod IP.
    assert_eq!(s.acquired[&uid], 1);
    assert_eq!(s.ips[&uid], vec![fakeworker::ip(1)]);
    assert_eq!(s.reported[&uid], want_capacity(1000, "1500m", "4Gi"));
    let want_class = api::WorkerSandboxClass {
        sandbox_class: "gvisor".into(),
        default_runtime: Some(api::WorkerSandboxRuntime {
            version: Some(host_compat()),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(s.classes[&uid], vec![want_class]);
    drop(s);
    assert_eq!(e.cl.status("bench"), status(3, 3, "bench"));
}

// With no limit set, a real worker reports its node's allocatable.
#[tokio::test]
async fn reconcile_reports_node_allocatable_without_limits() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.reconcile().await.unwrap();
    assert_eq!(
        e.rel.reported(&e.ctl.uid(&pod_name("bench", 0))),
        want_capacity(1000, "86", "160Gi")
    );
    assert_eq!(
        e.rel.reported(&e.ctl.uid(&pod_name("bench", 1))),
        want_capacity(1000, "44", "80Gi")
    );
}

#[tokio::test]
async fn reconcile_honors_max_actors_annotation() {
    let mut wp = pool("bench", 1, None);
    wp.metadata.annotations = Some([(MAX_ACTORS_ANNOTATION.to_string(), "5".to_string())].into());
    let mut bad = pool("broken", 1, None);
    bad.metadata.annotations =
        Some([(MAX_ACTORS_ANNOTATION.to_string(), "lots".to_string())].into());
    let e = new_env(vec![wp, bad]);
    assert!(
        e.reconcile().await.is_err(),
        "reconcile succeeded with an unparseable annotation"
    );
    assert_eq!(
        e.rel
            .reported(&e.ctl.uid(&pod_name("bench", 0)))
            .unwrap()
            .actors,
        5
    );
    assert!(!e.ctl.names().contains(&pod_name("broken", 0)));
}

// An Actor count past 32 bits would wrap in the capacity report.
#[tokio::test]
async fn reconcile_rejects_a_max_actors_past_32_bits() {
    let mut wp = pool("bench", 1, None);
    wp.metadata.annotations =
        Some([(MAX_ACTORS_ANNOTATION.to_string(), "3000000000".to_string())].into());
    let e = new_env(vec![wp]);
    assert!(e.reconcile().await.is_err());
    assert!(e.ctl.names().is_empty() && e.rel.s.lock().reported.is_empty());
}

// Each pass lets go of relays the pod list no longer has.
#[tokio::test]
async fn reconcile_prunes_gone_relays() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.cl.s.lock().relays.remove("node-b");
    let _ = e.reconcile().await;
    let want: HashMap<String, String> =
        [("node-a".to_string(), "10.0.0.1:8086".to_string())].into();
    assert_eq!(e.rel.s.lock().live, want);
}

// An annotation typo on a running pool reports an error and leaves the
// pool's Workers as they are.
#[tokio::test]
async fn bad_annotation_leaves_the_pools_workers() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.reconcile().await.unwrap();
    e.cl.edit_pool("bench", |p| {
        p.metadata.annotations =
            Some([(MAX_ACTORS_ANNOTATION.to_string(), "abc".to_string())].into())
    });
    assert!(e.reconcile().await.is_err());
    assert_eq!(e.ctl.names(), pool_names("bench", 2));
}

// A Worker whose node has left the set is replaced on a remaining node; the
// old one drains with the capacity it last reported.
#[tokio::test]
async fn worker_on_a_departed_node_is_replaced() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.reconcile().await.unwrap();
    let pod = pod_name("bench", 1);
    let old = e.ctl.uid(&pod);
    let before = e.rel.reported(&old);
    e.ctl.set_assigned(&old, 1);
    e.cl.s.lock().nodes.truncate(1);
    e.reconcile().await.unwrap();
    let w = e.ctl.worker(&pod).unwrap();
    assert!(
        wname(&w) != old && w.node_name == "node-a",
        "{} on {}",
        wname(&w),
        w.node_name
    );
    assert_eq!(
        e.rel.reported(&wname(&w)),
        want_capacity(1000, "86", "160Gi")
    );
    let d = e.ctl.draining(&pod).unwrap();
    assert_eq!(wname(&d), old);
    assert_eq!(e.rel.reported(&old), before);
    let st = e.cl.status("bench");
    assert_eq!((st.replicas, st.ready_replicas), (2, 2));
}

// An empty node list replaces nothing.
#[tokio::test]
async fn no_fake_keeper_node_leaves_workers_in_place() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.reconcile().await.unwrap();
    e.cl.s.lock().nodes.clear();
    let _ = e.reconcile().await;
    let want = pool_names("bench", 2);
    assert_eq!(e.ctl.names(), want);
    assert!(e.ctl.draining(&want[0]).is_none() && e.ctl.draining(&want[1]).is_none());
}

// A Worker on a node whose fake keeper is not up yet is registered but not
// ready; the next pass reports it once the relay is there.
#[tokio::test]
async fn reconcile_waits_for_the_nodes_fake_keeper() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.cl.s.lock().relays.remove("node-b");
    assert!(e.reconcile().await.is_err());
    assert_eq!(e.ctl.names().len(), 2);
    let st = e.cl.status("bench");
    assert_eq!((st.replicas, st.ready_replicas), (2, 1));
    e.cl.s
        .lock()
        .relays
        .insert("node-b".into(), "10.0.0.2:8086".into());
    e.reconcile().await.unwrap();
    assert_eq!(e.rel.s.lock().reported.len(), 2);
    assert_eq!(e.cl.status("bench").ready_replicas, 2);
}

#[tokio::test]
async fn reconcile_retries_a_rejected_report() {
    let e = new_env(vec![pool("bench", 1, None)]);
    const FIRST: &str = "worker-1";
    e.rel.s.lock().reject.insert(FIRST.into(), Code::NotFound);
    assert!(e.reconcile().await.is_err());
    e.rel.s.lock().reject.remove(FIRST);
    e.reconcile().await.unwrap();
    assert!(e.rel.reported(&e.ctl.uid(&pod_name("bench", 0))).is_some());
    assert_eq!(e.cl.status("bench").ready_replicas, 1);
}

// A steady fleet costs hivekeep-api nothing.
#[tokio::test]
async fn reconcile_is_idempotent() {
    let e = new_env(vec![pool("bench", 3, None)]);
    e.reconcile().await.unwrap();
    let (creates, reports, updates) =
        (e.ctl.s.lock().creates, e.rel.calls(), e.cl.s.lock().updates);
    e.reconcile().await.unwrap();
    assert_eq!(e.ctl.s.lock().creates, creates);
    assert_eq!(e.rel.calls(), reports);
    assert_eq!(e.cl.s.lock().updates, updates);
}

// A limits edit replaces the pool's Workers; the old one is not reported
// again.
#[tokio::test]
async fn pool_limits_change_replaces_workers() {
    let e = new_env(vec![pool(
        "bench",
        1,
        Some(res(&[("cpu", "2"), ("memory", "4Gi")])),
    )]);
    e.reconcile().await.unwrap();
    let pod = pod_name("bench", 0);
    let old = e.ctl.uid(&pod);
    e.ctl.set_assigned(&old, 1);
    let reports = e.rel.calls();
    e.cl.edit_pool("bench", |p| {
        p.spec
            .template
            .as_mut()
            .unwrap()
            .resources
            .as_mut()
            .unwrap()
            .limits = Some(res(&[("cpu", "4"), ("memory", "8Gi")]))
    });
    e.reconcile().await.unwrap();
    let w = e.ctl.worker(&pod).unwrap();
    assert_ne!(wname(&w), old);
    assert_eq!(e.rel.reported(&wname(&w)), want_capacity(1000, "4", "8Gi"));
    assert_eq!(e.rel.reported(&old), want_capacity(1000, "2", "4Gi"));
    assert_eq!(e.rel.calls() - reports, 1);
    assert_eq!(wname(&e.ctl.draining(&pod).unwrap()), old);
    let st = e.cl.status("bench");
    assert_eq!((st.replicas, st.ready_replicas), (1, 1));
}

// The worker's Actor count is a flag, so changing it rolls the pods too.
#[tokio::test]
async fn max_actors_annotation_change_replaces_workers() {
    let e = new_env(vec![pool("bench", 1, None)]);
    e.reconcile().await.unwrap();
    let pod = pod_name("bench", 0);
    let old = e.ctl.uid(&pod);
    e.cl.edit_pool("bench", |p| {
        p.metadata.annotations = Some([(MAX_ACTORS_ANNOTATION.to_string(), "5".to_string())].into())
    });
    e.reconcile().await.unwrap();
    let w = e.ctl.worker(&pod).unwrap();
    assert_ne!(wname(&w), old);
    assert_eq!(e.rel.reported(&wname(&w)).unwrap().actors, 5);
    assert_eq!(e.ctl.names(), pool_names("bench", 1));
}

// A node's allocatable reaches the worker once, at startup.
#[tokio::test]
async fn allocatable_change_keeps_worker_capacity() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.reconcile().await.unwrap();
    let (creates, reports) = (e.ctl.s.lock().creates, e.rel.calls());
    e.cl.s.lock().nodes[0].allocatable = res(&[("cpu", "64"), ("memory", "128Gi")]);
    e.reconcile().await.unwrap();
    assert_eq!((e.ctl.s.lock().creates, e.rel.calls()), (creates, reports));
    let st = e.cl.status("bench");
    assert_eq!((st.replicas, st.ready_replicas), (2, 2));
}

// Scaling down drains first, deletes once no Actor is assigned.
#[tokio::test]
async fn scale_down_drains_then_deletes() {
    let e = new_env(vec![pool("bench", 4, None)]);
    e.reconcile().await.unwrap();
    let busy = pod_name("bench", 3);
    let idle = pod_name("bench", 2);
    e.ctl.set_assigned(&e.ctl.uid(&busy), 1);
    e.cl.set_replicas("bench", 2);
    e.reconcile().await.unwrap();
    assert!(!e.ctl.names().contains(&idle));
    assert_eq!(e.ctl.state(&busy), api::WorkerState::Draining);
    let st = e.cl.status("bench");
    assert_eq!((st.replicas, st.ready_replicas), (2, 2));
    e.ctl.set_assigned(&e.ctl.uid(&busy), 0);
    e.reconcile().await.unwrap();
    assert_eq!(e.ctl.names(), pool_names("bench", 2));
}

// A Worker still held after the drain grace is deleted anyway, which
// releases its Actors.
#[tokio::test]
async fn drain_grace_deletes_a_worker_still_held() {
    let mut e = new_env(vec![pool("bench", 2, None)]);
    e.reconcile().await.unwrap();
    let busy = pod_name("bench", 1);
    let held = e.ctl.uid(&busy);
    e.ctl.set_assigned(&held, 1);
    e.cl.set_replicas("bench", 1);
    e.reconcile().await.unwrap();
    e.advance(TEST_DRAIN_GRACE - Duration::from_secs(1));
    e.reconcile().await.unwrap();
    assert!(
        e.ctl.worker(&busy).is_some(),
        "deleted within the drain grace"
    );
    e.advance(Duration::from_secs(1));
    e.reconcile().await.unwrap();
    assert_eq!(e.ctl.names(), pool_names("bench", 1));
    assert_eq!(
        e.ctl.s.lock().assignments.get(&held).copied().unwrap_or(0),
        0
    );
}

// A pod scaled down while held, then wanted again, gets a new Worker.
#[tokio::test]
async fn scale_down_then_up_replaces_the_draining_worker() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.reconcile().await.unwrap();
    let busy = pod_name("bench", 1);
    let old = e.ctl.uid(&busy);
    e.ctl.set_assigned(&old, 1);
    e.cl.set_replicas("bench", 1);
    e.reconcile().await.unwrap();
    e.cl.set_replicas("bench", 2);
    e.reconcile().await.unwrap();
    let w = e.ctl.worker(&busy).unwrap();
    assert!(wname(&w) != old && state_of(&w) == api::WorkerState::Active);
    let st = e.cl.status("bench");
    assert_eq!((st.replicas, st.ready_replicas), (2, 2));
    e.ctl.set_assigned(&old, 0);
    e.reconcile().await.unwrap();
    assert_eq!(e.ctl.names(), pool_names("bench", 2));
}

// A create the server commits but whose reply is lost must not strand the
// Worker: cleanup still deletes it.
#[tokio::test]
async fn delete_all_after_a_lost_create_reply() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.ctl.s.lock().lost_reply = Some(Code::Unavailable);
    assert!(e.reconcile().await.is_err());
    assert_eq!(e.cl.status("bench").replicas, 0);
    e.c.delete_all().await.unwrap();
    assert!(e.ctl.names().is_empty());
}

// The same Worker, unwanted next pass, is retired; wanted, its create is
// retried and confirmed.
#[tokio::test]
async fn lost_create_reply_is_retired_or_confirmed() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.ctl.s.lock().lost_reply = Some(Code::DeadlineExceeded);
    assert!(e.reconcile().await.is_err());
    e.ctl.s.lock().lost_reply = None;
    e.cl.set_replicas("bench", 1);
    e.reconcile().await.unwrap();
    assert_eq!(e.ctl.names(), pool_names("bench", 1));
    let st = e.cl.status("bench");
    assert_eq!((st.replicas, st.ready_replicas), (1, 1));
}

// Every registered Worker is marked available.
#[tokio::test]
async fn reconcile_marks_workers_available() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.reconcile().await.unwrap();
    for name in pool_names("bench", 2) {
        assert_eq!(e.ctl.state(&name), api::WorkerState::Active, "{name}");
    }
}

// A Worker registered but not marked available is marked by the next pass,
// also after a restart.
#[tokio::test]
async fn unmarked_worker_is_marked_by_the_next_pass() {
    let e = new_env(vec![pool("bench", 1, None)]);
    e.ctl.s.lock().mark_err = Some(Code::Unavailable);
    assert!(e.reconcile().await.is_err());
    let name = pool_names("bench", 1).remove(0);
    assert_eq!(e.ctl.state(&name), api::WorkerState::Unavailable);
    e.ctl.s.lock().mark_err = None;
    let restarted = e.restarted();
    restarted.adopt().await.unwrap();
    restarted.reconcile().await.unwrap();
    assert_eq!(e.ctl.state(&name), api::WorkerState::Active);
    assert_eq!(e.ctl.names(), pool_names("bench", 1));
}

// A label edit on the pool reaches its Workers in place, also after a
// restart.
#[tokio::test]
async fn pool_label_change_updates_workers() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.reconcile().await.unwrap();
    let relabel = |v: &str| {
        let v = v.to_string();
        e.cl.edit_pool("bench", move |p| {
            p.metadata.labels = Some([("workload".to_string(), v.clone())].into())
        })
    };
    relabel("v2");
    let creates = e.ctl.s.lock().creates;
    e.reconcile().await.unwrap();
    for name in pool_names("bench", 2) {
        assert_eq!(e.ctl.worker(&name).unwrap().labels["workload"], "v2");
    }
    {
        let s = e.ctl.s.lock();
        assert_eq!((s.creates, s.updates), (creates, 2));
    }

    let restarted = e.restarted();
    restarted.adopt().await.unwrap();
    relabel("v3");
    restarted.reconcile().await.unwrap();
    for name in pool_names("bench", 2) {
        assert_eq!(e.ctl.worker(&name).unwrap().labels["workload"], "v3");
    }
}

// A sandbox class edit replaces the pool's Workers the way it replaces real
// worker pods.
#[tokio::test]
async fn pool_sandbox_class_change_replaces_workers() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.reconcile().await.unwrap();
    let idle = pod_name("bench", 0);
    let busy = pod_name("bench", 1);
    let old_busy = e.ctl.uid(&busy);
    e.ctl.set_assigned(&old_busy, 1);
    e.cl.edit_pool("bench", |p| {
        p.spec.sandbox_classes = vec![WorkerPoolSandboxClass {
            name: SandboxClass::Microvm,
            config_ref: None,
        }]
    });
    e.reconcile().await.unwrap();
    for pod in [&idle, &busy] {
        let w = e.ctl.worker(pod).unwrap();
        assert_eq!(sandbox_class_of(&w), "microvm");
        assert_eq!(state_of(&w), api::WorkerState::Active);
        assert_eq!(w.worker_pod_uid, wname(&w));
    }
    let d = e.ctl.draining(&busy).unwrap();
    assert_eq!(
        (wname(&d), sandbox_class_of(&d)),
        (old_busy.clone(), "gvisor".to_string())
    );
    let st = e.cl.status("bench");
    assert_eq!((st.replicas, st.ready_replicas), (2, 2));
    e.ctl.set_assigned(&old_busy, 0);
    e.reconcile().await.unwrap();
    assert_eq!(e.ctl.names(), pool_names("bench", 2));
}

// A replaced Worker whose drain fails is retried, and until then is no
// replica and is left alone.
#[tokio::test]
async fn failed_drain_of_a_replaced_worker_is_no_replica() {
    let e = new_env(vec![pool("bench", 1, None)]);
    e.reconcile().await.unwrap();
    let pod = pod_name("bench", 0);
    let old = wname(&e.ctl.worker(&pod).unwrap());
    e.ctl.s.lock().drain_err = Some(Code::Unavailable);
    e.cl.edit_pool("bench", |p| {
        p.spec.sandbox_classes = vec![WorkerPoolSandboxClass {
            name: SandboxClass::Microvm,
            config_ref: None,
        }];
        p.metadata.labels = Some([("workload".to_string(), "v2".to_string())].into());
    });
    assert!(e.reconcile().await.is_err());
    let replacement = e
        .ctl
        .all()
        .into_iter()
        .find(|w| sandbox_class_of(w) == "microvm");
    let replacement = replacement.expect("no microvm replacement");
    assert!(e.rel.reported(&wname(&replacement)).is_some());
    assert_eq!(e.ctl.s.lock().workers[&old].labels["workload"], "bench");
    let st = e.cl.status("bench");
    assert_eq!((st.replicas, st.ready_replicas), (1, 1));
    e.ctl.s.lock().drain_err = None;
    e.reconcile().await.unwrap();
    assert!(!e.ctl.s.lock().workers.contains_key(&old));
}

#[tokio::test]
async fn deleted_pool_retires_its_workers() {
    let e = new_env(vec![pool("bench", 2, None), pool("other", 1, None)]);
    e.reconcile().await.unwrap();
    e.cl.s.lock().pools.remove(0);
    e.reconcile().await.unwrap();
    assert_eq!(e.ctl.names(), pool_names("other", 1));
}

// After a restart the controller adopts its run's Workers and leaves every
// other Worker alone.
#[tokio::test]
async fn adopt_after_restart() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.reconcile().await.unwrap();
    let real = api::Worker {
        metadata: Some(api::ResourceMetadata {
            name: "0c4f6c2e-real-pod-uid".into(),
            ..Default::default()
        }),
        worker_namespace: NS.into(),
        worker_pool: "bench".into(),
        worker_pod: "bench-7d9f8c-x2x4k".into(),
        status: Some(Default::default()),
        ..Default::default()
    };
    let other_run = api::Worker {
        metadata: Some(api::ResourceMetadata {
            name: "9a1e0d3b-other-run".into(),
            ..Default::default()
        }),
        worker_namespace: NS.into(),
        worker_pool: "bench".into(),
        worker_pod: fakeworker::name("r2", NS, "bench", 0),
        status: Some(Default::default()),
        ..Default::default()
    };
    {
        let mut s = e.ctl.s.lock();
        s.workers.insert(wname(&real), real.clone());
        s.workers.insert(wname(&other_run), other_run.clone());
    }
    let restarted = e.restarted();
    restarted.adopt().await.unwrap();
    assert_eq!(restarted.workers.lock().len(), 2);
    let (creates, reports) = (e.ctl.s.lock().creates, e.rel.calls());
    restarted.reconcile().await.unwrap();
    assert_eq!((e.ctl.s.lock().creates, e.rel.calls()), (creates, reports));
    let s = e.ctl.s.lock();
    assert!(s.workers.contains_key(&wname(&real)) && s.workers.contains_key(&wname(&other_run)));
}

// Installed with hivekeep-api, fake-workersync starts before the server
// serves, and keeps trying to adopt rather than exiting.
#[tokio::test]
async fn adopt_until_served_retries() {
    let e = new_env(vec![pool("bench", 2, None)]);
    e.reconcile().await.unwrap();
    e.ctl.s.lock().list_failures = 3;
    let restarted = e.restarted();
    restarted
        .adopt_until_served(Duration::from_millis(1), Duration::from_millis(2))
        .await
        .unwrap();
    assert_eq!(restarted.workers.lock().len(), 2);
    assert_eq!(e.ctl.s.lock().list_failures, 0);
    // Each adopted Worker's node, so a restart keeps them in the node set.
    assert!(!restarted.nodes().is_empty());
}

// An error retrying cannot cure ends adopt at once.
#[tokio::test]
async fn adopt_until_served_fails_on_a_permanent_error() {
    let e = new_env(vec![]);
    {
        let mut s = e.ctl.s.lock();
        s.list_failures = 1 << 30;
        s.list_code = Some(Code::PermissionDenied);
    }
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        e.c.adopt_until_served(Duration::from_millis(1), Duration::from_millis(5)),
    )
    .await
    .expect("retried a permanent error until the deadline");
    assert_eq!(r.unwrap_err().code(), Code::PermissionDenied);
}

// A shutdown during the retries ends them.
#[tokio::test]
async fn adopt_until_served_stops_with_its_context() {
    let e = new_env(vec![]);
    e.ctl.s.lock().list_failures = 1 << 30;
    let r = tokio::time::timeout(
        Duration::from_millis(20),
        e.c.adopt_until_served(Duration::from_millis(1), Duration::from_millis(5)),
    )
    .await;
    assert!(
        r.is_err(),
        "adopt_until_served finished while the server never served"
    );
}

// A limits edit made while fake-workersync was down still replaces the
// pool's Workers.
#[tokio::test]
async fn adopt_replaces_workers_of_a_pool_edited_while_down() {
    let e = new_env(vec![pool(
        "bench",
        1,
        Some(res(&[("cpu", "2"), ("memory", "4Gi")])),
    )]);
    e.reconcile().await.unwrap();
    let pod = pod_name("bench", 0);
    let old = e.ctl.uid(&pod);
    e.cl.edit_pool("bench", |p| {
        p.spec
            .template
            .as_mut()
            .unwrap()
            .resources
            .as_mut()
            .unwrap()
            .limits = Some(res(&[("cpu", "4"), ("memory", "8Gi")]))
    });
    let restarted = e.restarted();
    restarted.adopt().await.unwrap();
    restarted.reconcile().await.unwrap();
    let w = e.ctl.worker(&pod).unwrap();
    assert_ne!(wname(&w), old);
    assert_eq!(e.rel.reported(&wname(&w)), want_capacity(1000, "4", "8Gi"));
    assert_eq!(e.ctl.names(), pool_names("bench", 1));
}

// An adopted Worker whose capacity hivekeep-api never accepted is reported
// after the restart.
#[tokio::test]
async fn adopt_reports_a_worker_never_accepted() {
    let e = new_env(vec![pool("bench", 1, None)]);
    const FIRST: &str = "worker-1";
    e.rel
        .s
        .lock()
        .reject
        .insert(FIRST.into(), Code::Unavailable);
    assert!(e.reconcile().await.is_err());
    e.rel.s.lock().reject.remove(FIRST);
    let restarted = e.restarted();
    restarted.adopt().await.unwrap();
    restarted.reconcile().await.unwrap();
    assert_eq!(e.rel.reported(FIRST), want_capacity(1000, "86", "160Gi"));
    let st = e.cl.status("bench");
    assert_eq!((st.replicas, st.ready_replicas), (1, 1));
}

// A Worker already registered with the same IPs and capacity registers its
// epoch again instead of acquiring a new one; one registered with other
// values acquires a new epoch.
#[tokio::test]
async fn report_reuses_the_registered_epoch() {
    let cases = [
        (
            "same registration",
            fakeworker::ip(0),
            want_capacity(1000, "86", "160Gi"),
            0,
            5,
        ),
        (
            "other capacity",
            fakeworker::ip(0),
            want_capacity(10, "86", "160Gi"),
            1,
            6,
        ),
        (
            "other IPs",
            fakeworker::ip(1),
            want_capacity(1000, "86", "160Gi"),
            1,
            6,
        ),
    ];
    for (name, ip, capacity, want_acquired, want_epoch) in cases {
        let e = new_env(vec![pool("bench", 1, None)]);
        const FIRST: &str = "worker-1";
        e.rel
            .s
            .lock()
            .reject
            .insert(FIRST.into(), Code::Unavailable);
        assert!(e.reconcile().await.is_err(), "{name}");
        {
            let mut r = e.rel.s.lock();
            r.reject.remove(FIRST);
            r.epochs.insert(FIRST.into(), 5);
        }
        {
            let mut s = e.ctl.s.lock();
            let st = s
                .workers
                .get_mut(FIRST)
                .unwrap()
                .status
                .get_or_insert_with(Default::default);
            st.registered_epoch = 5;
            st.ips = vec![ip];
            st.capacity = capacity;
        }
        e.reconcile().await.unwrap();
        let r = e.rel.s.lock();
        assert_eq!(
            r.acquired.get(FIRST).copied().unwrap_or(0),
            want_acquired,
            "{name}"
        );
        assert_eq!(r.epochs[FIRST], want_epoch, "{name}");
    }
}

#[tokio::test]
async fn delete_all() {
    let e = new_env(vec![pool("bench", 3, None)]);
    e.reconcile().await.unwrap();
    // One already gone, as after a cut-short shutdown.
    let gone = e.ctl.uid(&pod_name("bench", 0));
    e.ctl.s.lock().workers.remove(&gone);
    e.c.delete_all().await.unwrap();
    assert!(e.ctl.names().is_empty());
}

#[test]
fn capacity_formats_like_worker() {
    let capacity = |wp: &WorkerPool| template_of(wp).unwrap().capacity(None);
    let got = capacity(&pool(
        "p",
        1,
        Some(res(&[("cpu", "0.5"), ("memory", "1G")])),
    ));
    // The worker spells CPU as decimal millicores and memory as a byte count;
    // 1G is not a power of two, so 1e9.
    assert_eq!(Some(got), want_capacity(1000, "500m", "1e9"));
    let none = capacity(&pool("p", 1, None));
    assert_eq!(
        (none.cpu_millicores, none.memory_bytes, none.actors),
        (0, 0, 1000)
    );
}

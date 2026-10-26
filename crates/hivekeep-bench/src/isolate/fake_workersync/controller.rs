//! The controller: for every WorkerPool it keeps `spec.replicas` fake
//! Workers, none backed by a pod, has each one's capacity reported through
//! the fake keeper on its node, and writes the pool's status from them.
//!
//! It keeps the registered Workers in memory and lists them from
//! hivekeep-api only at startup ([`Controller::adopt`]). A pass with nothing
//! changed and nothing draining makes no hivekeep-api calls, so a steady
//! fleet adds no load to the system under test.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::StreamExt;
use hivekeep_k8s::crd::{WorkerPool, WorkerPoolStatus};
use hivekeep_proto::api::v1 as api;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::ResourceExt;
use parking_lot::Mutex;
use tonic::{Code, Status};

use super::ports::{CapacityRelay, Cluster, Node, WorkerClient, WorkerReport};
use super::template::{max_actors, sandbox_class, template_of, Template};
use crate::isolate::fakeworker;

type Allocatable = HashMap<String, BTreeMap<String, Quantity>>;

/// A fake Worker the controller has registered.
#[derive(Debug, Clone, Default)]
pub struct FakeWorker {
    /// The Worker's name and pod UID, fresh for each Worker, as a real
    /// Worker is named after its pod's UID. A replaced Worker's successor is
    /// a new Worker, so nothing hivekeep-api keyed by the old one carries
    /// over.
    pub name: String,
    /// The stand-in pod name ([`fakeworker::name`]), shared by every Worker
    /// that fills the same index of a pool. It is how adopt finds this run's
    /// Workers.
    pub pod: String,
    /// Pool namespace.
    pub namespace: String,
    /// Pool name.
    pub pool: String,
    /// Index in the pool.
    pub index: usize,
    /// The node it is placed on.
    pub node: String,
    /// Set once hivekeep-api has the Worker. A Worker is tracked from before
    /// its create, so a create the server commits but the caller sees fail
    /// is still retired, or deleted by cleanup.
    pub created: bool,
    /// Synced onto the Worker record.
    pub labels: BTreeMap<String, String>,
    /// The pool template the Worker was made from; `None` for a Worker
    /// adopted after a restart until reconcile matches it to its pool.
    pub tmpl: Option<Template>,
    /// Fixed when the Worker is made, as a worker reads its limits once at
    /// startup.
    pub capacity: Option<api::WorkerResources>,
    /// Whether hivekeep-api has accepted the capacity.
    pub reported: bool,
    /// Whether it is draining.
    pub draining: bool,
    /// When the controller drained it, or adopted it draining.
    pub drained_at: Option<SystemTime>,
}

/// A fake Worker some WorkerPool wants.
#[derive(Clone)]
struct Desired {
    pool: WorkerPool,
    index: usize,
}

/// Stands in for the WorkerPool controller and the worker registry sync.
pub struct Controller {
    client: Arc<dyn WorkerClient>,
    relay: Arc<dyn CapacityRelay>,
    cluster: Arc<dyn Cluster>,
    run: String,
    concurrency: usize,
    /// How long a draining Worker waits for its Actors to leave before it
    /// is deleted with them still assigned.
    drain_grace: Duration,
    pub(crate) now: Box<dyn Fn() -> SystemTime + Send + Sync>,
    pub(crate) new_uid: Box<dyn Fn() -> String + Send + Sync>,
    pub(crate) workers: Mutex<HashMap<String, FakeWorker>>,
}

fn ref_to(name: &str) -> Option<api::ObjectRef> {
    Some(api::ObjectRef {
        hive: String::new(),
        name: name.to_string(),
    })
}

fn wrap(e: Status, what: String) -> Status {
    Status::new(e.code(), format!("{what}: {}", e.message()))
}

/// The host's sandbox runtime compatibility, as a synthetic worker reports
/// it: schema v1 with the CPU architecture.
pub fn host_compat() -> api::VersionedSandboxCompat {
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    };
    api::VersionedSandboxCompat {
        schema_version: "v1".into(),
        attributes: vec![api::AttributeEntry {
            key: "architecture".into(),
            value: arch.into(),
        }],
    }
}

/// Whether `w` was made from `wp`'s current template, so a real pool would
/// keep its pod. A Worker adopted after a restart has only its reported
/// capacity to go by, so it fits when that capacity is what `wp` would give
/// it on its node now. A held pool's Workers fit, since they are left as
/// they are.
fn fits(w: &FakeWorker, wp: &WorkerPool, allocatable: &Allocatable) -> bool {
    let Ok(t) = template_of(wp) else { return true };
    if let Some(tmpl) = &w.tmpl {
        return *tmpl == t;
    }
    match (&w.capacity, allocatable.get(&w.node)) {
        (Some(cap), Some(alloc)) => *cap == t.capacity(Some(alloc)),
        _ => true,
    }
}

fn pool_labels(wp: &WorkerPool) -> BTreeMap<String, String> {
    wp.labels().clone()
}

fn pool_key(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}")
}

impl Controller {
    /// A controller over `client`, `relay` and `cluster` for run prefix
    /// `run`.
    pub fn new(
        client: Arc<dyn WorkerClient>,
        relay: Arc<dyn CapacityRelay>,
        cluster: Arc<dyn Cluster>,
        run: &str,
        concurrency: usize,
        drain_grace: Duration,
    ) -> Controller {
        Controller {
            client,
            relay,
            cluster,
            run: run.to_string(),
            concurrency: concurrency.max(1),
            drain_grace,
            now: Box::new(SystemTime::now),
            new_uid: Box::new(|| uuid::Uuid::new_v4().to_string()),
            workers: Mutex::new(HashMap::new()),
        }
    }

    /// Runs [`adopt`](Self::adopt) until it succeeds, waiting between tries
    /// from `initial` up to `max_backoff`. fake-workersync is installed in
    /// the same apply as hivekeep-api, so it retries the errors of a server
    /// that does not serve yet; any other error ends it.
    pub async fn adopt_until_served(
        &self,
        initial: Duration,
        max_backoff: Duration,
    ) -> Result<(), Status> {
        let mut backoff = initial;
        loop {
            let err = match self.adopt().await {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };
            if !matches!(err.code(), Code::Unavailable | Code::DeadlineExceeded) {
                return Err(err);
            }
            tracing::warn!(%err, ?backoff, "Failed to load registered fake Workers; retrying");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(max_backoff);
        }
    }

    /// The nodes the tracked Workers are on.
    pub fn nodes(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for w in self.workers.lock().values() {
            if !w.node.is_empty() && !out.contains(&w.node) {
                out.push(w.node.clone());
            }
        }
        out
    }

    /// Loads the fake Workers of this run that are already registered, as
    /// after a restart, so they are reconciled rather than created again.
    pub async fn adopt(&self) -> Result<(), Status> {
        let prefix = fakeworker::prefix(&self.run);
        let mut token = String::new();
        loop {
            let page = self
                .client
                .list_workers(api::ListWorkersRequest {
                    options: Some(api::ListOptions {
                        page_size: 1000,
                        page_token: token.clone(),
                    }),
                })
                .await
                .map_err(|e| wrap(e, "while listing Workers".into()))?;
            for w in page.workers {
                if !w.worker_pod.starts_with(&prefix) {
                    continue;
                }
                let Some(index) = fakeworker::index(
                    &self.run,
                    &w.worker_namespace,
                    &w.worker_pool,
                    &w.worker_pod,
                ) else {
                    continue;
                };
                let name = w
                    .metadata
                    .as_ref()
                    .map(|m| m.name.clone())
                    .unwrap_or_default();
                let status = w.status.clone().unwrap_or_default();
                let state = status.state();
                let draining = state == api::WorkerState::Draining;
                let fw = FakeWorker {
                    name: name.clone(),
                    pod: w.worker_pod.clone(),
                    namespace: w.worker_namespace.clone(),
                    pool: w.worker_pool.clone(),
                    index,
                    node: w.node_name.clone(),
                    labels: w.labels.into_iter().collect(),
                    capacity: status.capacity,
                    // Its runtimes are reported last, so a Worker without
                    // them is registered again.
                    reported: !status.sandbox_classes.is_empty(),
                    // One never marked available goes back through create,
                    // which marks it.
                    created: state != api::WorkerState::Unavailable,
                    draining,
                    // When it was drained is not recorded, so its grace
                    // restarts.
                    drained_at: draining.then(|| (self.now)()),
                    tmpl: None,
                };
                self.workers.lock().insert(name, fw);
            }
            token = page.next_page_token;
            if token.is_empty() {
                break;
            }
        }
        tracing::info!(
            workers = self.workers.lock().len(),
            "Adopted registered fake Workers"
        );
        Ok(())
    }

    /// Brings the fake Workers and the pools' status in line with the
    /// WorkerPools. Every step is idempotent, so a failed pass is retried by
    /// the next one. The error lists every failure of the pass.
    pub async fn reconcile(&self) -> Result<(), String> {
        let pools = self
            .cluster
            .pools()
            .await
            .map_err(|e| format!("while listing WorkerPools: {e}"))?;
        let nodes: Vec<Node> = self
            .cluster
            .nodes()
            .await
            .map_err(|e| format!("while listing nodes: {e}"))?;
        let relays = self
            .cluster
            .relays()
            .await
            .map_err(|e| format!("while listing fake-keeper pods: {e}"))?;
        self.relay.prune(&relays);
        let node_names: Vec<String> = nodes.iter().map(|n| n.name.clone()).collect();
        let allocatable: Allocatable = nodes
            .iter()
            .map(|n| (n.name.clone(), n.allocatable.clone()))
            .collect();
        let no_nodes = nodes.is_empty();

        let mut desired: BTreeMap<String, Desired> = BTreeMap::new();
        // Pools whose spec cannot be read. Their Workers are left as they
        // are rather than retired, so an annotation typo reports an error
        // instead of tearing the pool down.
        let mut held: Vec<String> = Vec::new();
        let errs: Mutex<Vec<String>> = Mutex::new(Vec::new());
        for wp in &pools {
            let (ns, name) = (wp.namespace().unwrap_or_default(), wp.name_any());
            if let Err(e) = max_actors(wp) {
                errs.lock().push(e);
                held.push(pool_key(&ns, &name));
                continue;
            }
            for i in 0..wp.spec.replicas.max(0) as usize {
                desired.insert(
                    fakeworker::name(&self.run, &ns, &name, i),
                    Desired {
                        pool: wp.clone(),
                        index: i,
                    },
                );
            }
        }

        let on_fake_keeper_node = |w: &FakeWorker| no_nodes || allocatable.contains_key(&w.node);

        // Each desired pod is served by one current Worker; every other
        // Worker is retired. A draining Worker is retired until it is gone,
        // even if its pool wants its pod again: it takes no new Actors and
        // cannot be undrained. A real pool replaces every worker pod when
        // its sandbox class, limits or Actor count change, and each new pod
        // reports its capacity once, so its fake Workers are replaced (see
        // `fits`). So is a Worker whose node has left the fake-keeper set,
        // as a pod is when its node goes away; with no fake-keeper node
        // listed, Workers are left where they are, since nothing could
        // replace them. A replaced pod gets a new Worker in the same pass
        // while the old one drains, as a Deployment creates a replacement
        // pod without waiting for the old one to terminate.
        let mut current: HashMap<String, String> = HashMap::new();
        let mut unwanted: Vec<String> = Vec::new();
        {
            let workers = self.workers.lock();
            let mut names: Vec<&String> = workers.keys().collect();
            names.sort();
            for name in names {
                let w = &workers[name];
                if held.contains(&pool_key(&w.namespace, &w.pool)) && !w.draining {
                    continue;
                }
                if let Some(d) = desired.get(&w.pod) {
                    if !w.draining
                        && on_fake_keeper_node(w)
                        && fits(w, &d.pool, &allocatable)
                        && !current.contains_key(&w.pod)
                    {
                        current.insert(w.pod.clone(), name.clone());
                        continue;
                    }
                }
                unwanted.push(name.clone());
            }
        }

        futures::stream::iter(unwanted)
            .for_each_concurrent(self.concurrency, |name| {
                let errs = &errs;
                async move {
                    if let Err(e) = self.retire(&name).await {
                        errs.lock().push(e.to_string());
                    }
                }
            })
            .await;

        futures::stream::iter(desired.iter())
            .for_each_concurrent(self.concurrency, |(pod, d)| {
                let (errs, node_names, allocatable, relays, current) =
                    (&errs, &node_names, &allocatable, &relays, &current);
                async move {
                    let cur = current.get(pod).cloned();
                    if let Err(e) = self
                        .ensure(pod, d, cur, node_names, allocatable, relays)
                        .await
                    {
                        errs.lock().push(e);
                    }
                }
            })
            .await;

        for wp in &pools {
            if let Err(e) = self
                .sync_status(wp, &on_fake_keeper_node, &allocatable)
                .await
            {
                errs.lock().push(e);
            }
        }
        let errs = errs.into_inner();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(errs.join("\n"))
        }
    }

    /// Registers a Worker for the desired pod if it has no current one,
    /// brings its labels in line with the pool's, then has its capacity
    /// reported until hivekeep-api accepts it. The capacity is fixed when
    /// the Worker is made and reported once. The Worker is tracked before
    /// CreateWorker and marked created after, so a failed create is retried
    /// by the next pass under the same name, and one the server committed
    /// anyway is never lost: retire and cleanup take a NotFound as done.
    async fn ensure(
        &self,
        pod: &str,
        d: &Desired,
        current: Option<String>,
        nodes: &[String],
        allocatable: &Allocatable,
        relays: &HashMap<String, String>,
    ) -> Result<(), String> {
        let (ns, pool_name) = (d.pool.namespace().unwrap_or_default(), d.pool.name_any());
        let name = match current {
            Some(name) => name,
            None => {
                if nodes.is_empty() {
                    return Err("no fake-keeper node to place fake Workers on".into());
                }
                let w = FakeWorker {
                    name: (self.new_uid)(),
                    pod: pod.to_string(),
                    namespace: ns.clone(),
                    pool: pool_name.clone(),
                    index: d.index,
                    node: fakeworker::node(d.index, nodes).to_string(),
                    labels: pool_labels(&d.pool),
                    ..Default::default()
                };
                let name = w.name.clone();
                self.workers.lock().insert(name.clone(), w);
                name
            }
        };
        let w = self
            .workers
            .lock()
            .get(&name)
            .cloned()
            .ok_or_else(|| format!("Worker {name} vanished"))?;
        // A new Worker takes its template and capacity from the pool now. An
        // adopted Worker fits its pool, or it would not be current, so it
        // takes the pool's template and keeps the capacity it reported.
        let mut fixed = w.capacity;
        if w.tmpl.is_none() || fixed.is_none() {
            let t = template_of(&d.pool)?;
            if fixed.is_none() {
                let alloc = allocatable.get(&w.node).ok_or_else(|| {
                    format!("node {} of Worker {name} is not a fake-keeper node", w.node)
                })?;
                fixed = Some(t.capacity(Some(alloc)));
            }
            if let Some(entry) = self.workers.lock().get_mut(&name) {
                entry.tmpl = Some(t);
                entry.capacity = fixed;
            }
        }
        if !w.created {
            self.create(&w, &d.pool).await.map_err(|e| e.to_string())?;
            if let Some(entry) = self.workers.lock().get_mut(&name) {
                entry.created = true;
            }
        }
        let want_labels = pool_labels(&d.pool);
        if w.labels != want_labels {
            self.update_labels(&name, &want_labels)
                .await
                .map_err(|e| e.to_string())?;
        }
        if w.reported {
            return Ok(());
        }
        let addr = relays
            .get(&w.node)
            .filter(|a| !a.is_empty())
            .ok_or_else(|| format!("no fake keeper on node {} yet for Worker {name}", w.node))?;
        let current = self
            .client
            .get_worker(api::GetWorkerRequest {
                worker: ref_to(&name),
            })
            .await
            .map_err(|e| format!("while reading Worker {name}: {e}"))?;
        let ips = vec![fakeworker::ip(w.index)];
        // An epoch's registration never changes, so a Worker registered with
        // other IPs or capacity takes a new epoch, as a restarted worker
        // process does.
        let status = current.status.unwrap_or_default();
        let epoch = if status.ips != ips || status.capacity != fixed {
            0
        } else {
            status.registered_epoch
        };
        self.relay
            .report(
                addr,
                WorkerReport {
                    name: name.clone(),
                    epoch,
                    ips,
                    capacity: fixed,
                    sandbox_classes: vec![api::WorkerSandboxClass {
                        sandbox_class: sandbox_class(&d.pool),
                        default_runtime: Some(api::WorkerSandboxRuntime {
                            version: Some(host_compat()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                },
            )
            .await
            .map_err(|e| format!("while reporting capacity for Worker {name}: {e}"))?;
        if let Some(entry) = self.workers.lock().get_mut(&name) {
            entry.reported = true;
        }
        Ok(())
    }

    /// Registers the Worker with the fields the registry sync would copy
    /// from the pool and its pod, then marks it available as the sync does
    /// once the pod is Ready.
    async fn create(&self, w: &FakeWorker, wp: &WorkerPool) -> Result<(), Status> {
        let name = &w.name;
        let res = self
            .client
            .create_worker(api::CreateWorkerRequest {
                worker: Some(api::Worker {
                    metadata: Some(api::ResourceMetadata {
                        name: name.clone(),
                        ..Default::default()
                    }),
                    worker_namespace: wp.namespace().unwrap_or_default(),
                    worker_pool: wp.name_any(),
                    worker_pod: w.pod.clone(),
                    worker_pod_uid: name.clone(),
                    node_name: w.node.clone(),
                    labels: pool_labels(wp).into_iter().collect(),
                    ..Default::default()
                }),
            })
            .await;
        if let Err(e) = res {
            if e.code() != Code::AlreadyExists {
                return Err(wrap(e, format!("while creating Worker {name}")));
            }
        }
        self.client
            .mark_worker_available(api::MarkWorkerAvailableRequest {
                worker: ref_to(name),
            })
            .await
            .map_err(|e| wrap(e, format!("while marking Worker {name} available")))?;
        Ok(())
    }

    /// Writes the pool's labels onto a registered Worker, as the registry
    /// sync does when a pool's labels change. UpdateWorker replaces the
    /// whole resource with the version it was read at as its precondition,
    /// so the Worker is read first; a conflicting write fails the call and
    /// the next pass retries it.
    async fn update_labels(
        &self,
        name: &str,
        want: &BTreeMap<String, String>,
    ) -> Result<(), Status> {
        let mut got = self
            .client
            .get_worker(api::GetWorkerRequest {
                worker: ref_to(name),
            })
            .await
            .map_err(|e| wrap(e, format!("while reading Worker {name}")))?;
        let have: BTreeMap<String, String> = got.labels.clone().into_iter().collect();
        if have != *want {
            got.labels = want.clone().into_iter().collect();
            self.client
                .update_worker(api::UpdateWorkerRequest { worker: Some(got) })
                .await
                .map_err(|e| wrap(e, format!("while updating Worker {name} labels")))?;
        }
        if let Some(entry) = self.workers.lock().get_mut(name) {
            entry.labels = want.clone();
        }
        Ok(())
    }

    /// Removes a Worker no pool wants, the way a real one goes when its pod
    /// does: drained first so nothing new lands on it, deleted once no Actor
    /// is assigned to it. No worker stops its Actors here, so after the
    /// drain grace the Worker is deleted with its Actors still assigned,
    /// and DeleteWorker releases them, as when a real pod is gone.
    async fn retire(&self, name: &str) -> Result<(), Status> {
        let Some(w) = self.workers.lock().get(name).cloned() else {
            return Ok(());
        };
        let mut drained_at = w.drained_at;
        if !w.draining {
            match self
                .client
                .drain_worker(api::DrainWorkerRequest {
                    worker: ref_to(name),
                })
                .await
            {
                Ok(_) => {}
                Err(e) if e.code() == Code::NotFound => {
                    self.forget(name);
                    return Ok(());
                }
                Err(e) => return Err(wrap(e, format!("while draining Worker {name}"))),
            }
            let now = (self.now)();
            drained_at = Some(now);
            if let Some(entry) = self.workers.lock().get_mut(name) {
                entry.draining = true;
                entry.drained_at = Some(now);
            }
        }
        let assigned = match self
            .client
            .list_worker_actor_assignments(api::ListWorkerActorAssignmentsRequest {
                worker: ref_to(name),
                options: Some(api::ListOptions {
                    page_size: 1,
                    page_token: String::new(),
                }),
            })
            .await
        {
            Ok(a) => a,
            Err(e) if e.code() == Code::NotFound => {
                self.forget(name);
                return Ok(());
            }
            Err(e) => return Err(wrap(e, format!("while listing Actors on Worker {name}"))),
        };
        let held_for = drained_at
            .and_then(|t| (self.now)().duration_since(t).ok())
            .unwrap_or_default();
        if !assigned.actor_assignments.is_empty() && held_for < self.drain_grace {
            return Ok(());
        }
        if let Err(e) = self
            .client
            .delete_worker(api::DeleteWorkerRequest {
                worker: ref_to(name),
                options: None,
            })
            .await
        {
            if e.code() != Code::NotFound {
                return Err(wrap(e, format!("while deleting Worker {name}")));
            }
        }
        self.forget(name);
        Ok(())
    }

    fn forget(&self, name: &str) {
        self.workers.lock().remove(name);
    }

    /// Writes the pool's status as the WorkerPool controller does from its
    /// Deployment: a replica is a registered Worker, ready once its capacity
    /// is reported, and the selector is the one worker pods would carry.
    async fn sync_status(
        &self,
        wp: &WorkerPool,
        on_fake_keeper_node: &dyn Fn(&FakeWorker) -> bool,
        allocatable: &Allocatable,
    ) -> Result<(), String> {
        let (ns, name) = (wp.namespace().unwrap_or_default(), wp.name_any());
        let mut want = WorkerPoolStatus {
            selector: format!("{}={name}", fakeworker::WORKER_POOL_LABEL),
            ..Default::default()
        };
        for w in self.workers.lock().values() {
            // A Worker its pool no longer wants, made from an older
            // template, or off the fake-keeper nodes is no replica even
            // before its drain lands.
            if w.namespace != ns
                || w.pool != name
                || !w.created
                || w.draining
                || w.index >= wp.spec.replicas.max(0) as usize
                || !on_fake_keeper_node(w)
                || !fits(w, wp, allocatable)
            {
                continue;
            }
            want.replicas += 1;
            if w.reported {
                want.ready_replicas += 1;
            }
        }
        if wp.status.clone().unwrap_or_default() == want {
            return Ok(());
        }
        let mut updated = wp.clone();
        updated.status = Some(want);
        self.cluster
            .update_status(&updated)
            .await
            .map_err(|e| format!("while updating WorkerPool {ns}/{name} status: {e}"))
    }

    /// Deletes every fake Worker this run registered, for cleanup. It keeps
    /// going past a failure and reports every one.
    pub async fn delete_all(&self) -> Result<(), String> {
        let mut names: Vec<String> = self.workers.lock().keys().cloned().collect();
        names.sort();
        let total = names.len();
        let errs: Mutex<Vec<String>> = Mutex::new(Vec::new());
        futures::stream::iter(names)
            .for_each_concurrent(self.concurrency, |name| {
                let errs = &errs;
                async move {
                    let res = self
                        .client
                        .delete_worker(api::DeleteWorkerRequest {
                            worker: ref_to(&name),
                            options: None,
                        })
                        .await;
                    match res {
                        Err(e) if e.code() != Code::NotFound => {
                            errs.lock()
                                .push(format!("while deleting Worker {name}: {e}"));
                        }
                        _ => self.forget(&name),
                    }
                }
            })
            .await;
        let errs = errs.into_inner();
        if !errs.is_empty() {
            return Err(errs.join("\n"));
        }
        tracing::info!(workers = total, "Deleted fake Workers");
        Ok(())
    }
}

#[cfg(test)]
#[path = "controller_tests.rs"]
mod tests;

//! Reconciles worker pods into the Worker registry.
//!
//! Informer handlers only enqueue keys; workers reconcile each key against
//! the current cache, requeueing with backoff on transient failures such as
//! a lost version precondition. The queue never hands one key to two
//! workers at once, so per-key ordering holds whatever the worker count.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_k8s::api_errors;
use hivekeep_k8s::crd::WorkerPool;
use hivekeep_k8s::informer::{Informer, InformerEvent};
use hivekeep_k8s::install_defaults;
use hivekeep_proto::api::v1 as pb;
use k8s_openapi::api::apps::v1::{Deployment, ReplicaSet};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::api::{Api, DeleteParams, Preconditions};
use kube::{Client, ResourceExt};
use tokio_util::sync::CancellationToken;
use tonic::Code;

use super::informers::pods_of_pool;
use super::key::WorkerKey;
use super::registry::WorkerRegistry;
use super::WORKER_POD_LABEL;
use crate::controllers::queue::WorkQueue;

/// Default first retry delay of a failed page of the startup scan.
pub const DEFAULT_LIST_BACKOFF: Duration = Duration::from_millis(500);
/// Default longest retry delay of a failed page of the startup scan.
pub const DEFAULT_LIST_CAP: Duration = Duration::from_secs(30);
/// Doublings before the scan's retry delay stops growing. With a 500ms base
/// the ramp reaches the 30s cap at step 6.
const LIST_BACKOFF_STEPS: u32 = 6;
/// Page size of the startup scan.
const LIST_PAGE_SIZE: i32 = 1000;

/// A reconcile failure.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// The registry rejected or failed a call.
    #[error("{context}{status}")]
    Registry {
        /// What was being done, with a trailing ": ", or empty.
        context: String,
        /// The gRPC status.
        status: tonic::Status,
    },
    /// A Kubernetes call failed.
    #[error("{context}: {source}")]
    Kube {
        /// What was being done.
        context: String,
        /// The API error.
        #[source]
        source: Box<kube::Error>,
    },
    /// Anything else.
    #[error("{0}")]
    Other(String),
}

impl SyncError {
    /// The gRPC code of a registry failure.
    pub fn code(&self) -> Option<Code> {
        match self {
            SyncError::Registry { status, .. } => Some(status.code()),
            SyncError::Kube { .. } | SyncError::Other(_) => None,
        }
    }
}

impl From<tonic::Status> for SyncError {
    fn from(status: tonic::Status) -> Self {
        SyncError::Registry {
            context: String::new(),
            status,
        }
    }
}

fn kube_err(context: impl Into<String>) -> impl FnOnce(kube::Error) -> SyncError {
    let context = context.into();
    move |e| SyncError::Kube {
        context,
        source: Box::new(e),
    }
}

/// Whether the pod is Ready with an IP, and so can take Actors.
pub fn is_worker_available(pod: &Pod) -> bool {
    let Some(status) = &pod.status else {
        return false;
    };
    if status.pod_ips.as_ref().is_none_or(Vec::is_empty) {
        return false;
    }
    status
        .conditions
        .iter()
        .flatten()
        .find(|c| c.type_ == "Ready")
        .is_some_and(|c| c.status == "True")
}

/// Whether the pod reached a terminal phase, which it never leaves.
pub fn is_pod_terminal(pod: &Pod) -> bool {
    matches!(
        pod.status.as_ref().and_then(|s| s.phase.as_deref()),
        Some("Succeeded") | Some("Failed")
    )
}

/// The SPIFFE ID the pod-identity signer issues to the pod.
pub fn worker_spiffe_id(pod: &Pod) -> String {
    let sa = pod
        .spec
        .as_ref()
        .and_then(|s| s.service_account_name.as_deref())
        .unwrap_or_default();
    install_defaults::spiffe_id(&pod.namespace().unwrap_or_default(), sa)
}

fn controller_of(meta: &ObjectMeta) -> Option<&OwnerReference> {
    meta.owner_references
        .iter()
        .flatten()
        .find(|r| r.controller == Some(true))
}

fn state_of(w: &pb::Worker) -> pb::WorkerState {
    w.status
        .as_ref()
        .and_then(|s| pb::WorkerState::try_from(s.state).ok())
        .unwrap_or(pb::WorkerState::Unspecified)
}

fn pool_labels(pool: &WorkerPool) -> HashMap<String, String> {
    pool.labels()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// The syncer.
pub struct WorkerPoolSyncer {
    registry: Arc<dyn WorkerRegistry>,
    client: Client,
    pods: Arc<Informer<Pod>>,
    pools: Arc<Informer<WorkerPool>>,
    pub(crate) queue: Arc<WorkQueue<WorkerKey>>,
    worker_count: usize,
    pub(crate) list_backoff: Duration,
    pub(crate) list_cap: Duration,
}

impl WorkerPoolSyncer {
    /// A syncer reading `pods` and `pools`. `client` deletes worker pods
    /// that reached a terminal phase or outlived their pool, and reads
    /// their owners to tell the latter from pods the garbage collector is
    /// still removing.
    pub fn new(
        registry: Arc<dyn WorkerRegistry>,
        client: Client,
        pods: Arc<Informer<Pod>>,
        pools: Arc<Informer<WorkerPool>>,
        worker_count: usize,
    ) -> Arc<Self> {
        Arc::new(WorkerPoolSyncer {
            registry,
            client,
            pods,
            pools,
            queue: WorkQueue::new(),
            worker_count: worker_count.max(1),
            list_backoff: DEFAULT_LIST_BACKOFF,
            list_cap: DEFAULT_LIST_CAP,
        })
    }

    /// Registers the informer handlers and starts the workers. Call before
    /// the informers run, so their initial list enqueues every existing pod.
    /// Nothing is reconciled until both caches synced: against an empty pool
    /// cache every pool reads as missing.
    pub fn start(self: &Arc<Self>, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        let q = self.queue.clone();
        self.pods.add_handler(move |ev| match ev {
            InformerEvent::Added(p) | InformerEvent::Deleted(p) => q.add(key_of(p)),
            InformerEvent::Updated { old, new } => {
                // A coalesced delete+create of one pod name surfaces as an
                // update; the old incarnation needs its record cleaned up.
                if old.metadata.uid != new.metadata.uid {
                    q.add(key_of(old));
                }
                q.add(key_of(new));
            }
        });
        let this = self.clone();
        self.pools
            .add_handler(move |ev| this.enqueue_worker_pool(ev.object()));

        let this = self.clone();
        tokio::spawn(async move {
            let synced = async {
                this.pods.wait_synced().await;
                this.pools.wait_synced().await;
            };
            tokio::select! {
                _ = shutdown.cancelled() => {
                    this.queue.shut_down();
                    return;
                }
                _ = synced => {}
            }
            let mut workers = Vec::new();
            for _ in 0..this.worker_count {
                let w = this.clone();
                workers.push(tokio::spawn(async move {
                    while w.process_next_work_item().await {}
                }));
            }
            // The other direction: every registered Worker is enqueued, so
            // records whose pods vanished while the controller was down are
            // cleaned up. Only now is the cache an authoritative snapshot.
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = this.enqueue_registered_workers(&shutdown) => {}
            }
            shutdown.cancelled().await;
            this.queue.shut_down();
            for w in workers {
                let _ = w.await;
            }
        })
    }

    /// Enqueues every cached pod of `pool`.
    pub fn enqueue_worker_pool(&self, pool: &WorkerPool) {
        let ns = pool.namespace().unwrap_or_default();
        for pod in pods_of_pool(&self.pods, &ns, &pool.name_any()) {
            self.queue.add(key_of(&pod));
        }
    }

    /// Reconciles the next key. `false` once the queue shut down.
    pub async fn process_next_work_item(&self) -> bool {
        let Some(key) = self.queue.get().await else {
            return false;
        };
        match self.reconcile(&key).await {
            Ok(()) => self.queue.forget(&key),
            // Requests are built from pod and pool state, so one rejected as
            // invalid would be resent verbatim: INVALID_ARGUMENT is terminal
            // and the key waits for the next pod event. Everything else,
            // including the UNAVAILABLE of a transport failure, requeues.
            Err(err) if err.code() == Some(Code::InvalidArgument) => {
                tracing::error!(worker = key.worker_name(), pod = %key.pod(), %err, "Reconcile rejected as invalid, dropping");
                self.queue.forget(&key);
            }
            Err(err) => {
                tracing::error!(worker = key.worker_name(), pod = %key.pod(), %err, "Reconcile failed, requeueing");
                self.queue.add_rate_limited(key.clone());
            }
        }
        self.queue.done(&key);
        true
    }

    /// Converges the registry record of `key` with the cached pod.
    pub async fn reconcile(&self, key: &WorkerKey) -> Result<(), SyncError> {
        let Some(pod) = self.pods.get(&key.namespace, &key.name) else {
            tracing::info!(worker = key.worker_name(), pod = %key.pod(), "Deregistering worker (pod deleted)");
            return self.reconcile_dead_worker(key).await;
        };
        if pod.metadata.uid.as_deref() != Some(key.uid.as_str()) {
            // Deleted and replaced under its name; the live pod has its own
            // key.
            tracing::info!(worker = key.worker_name(), pod = %key.pod(), "Deregistering worker (pod replaced)");
            return self.reconcile_dead_worker(key).await;
        }
        if pod.metadata.deletion_timestamp.is_some() {
            // Terminating: stop routing new Actors here, but leave the bound
            // Actor alone; the worker is shutting it down gracefully and the
            // record goes with the pod.
            return self.mark_worker_draining(key).await;
        }
        // A terminal pod is never Ready again; left alone its Worker would
        // stay UNAVAILABLE, and its Actors registered, while the object
        // lingers.
        if is_pod_terminal(&pod) {
            let phase = pod
                .status
                .as_ref()
                .and_then(|s| s.phase.clone())
                .unwrap_or_default();
            return self
                .delete_worker_pod(key, &pod, &format!("phase {phase}"))
                .await;
        }
        self.create_or_update_worker(key, &pod).await
    }

    /// Registers the pod's Worker or brings it in line with the pod. A Worker
    /// is registered UNAVAILABLE and marked available only while its pod is
    /// Ready; its epochs, IPs, capacity and runtimes are its own to report.
    async fn create_or_update_worker(&self, key: &WorkerKey, pod: &Pod) -> Result<(), SyncError> {
        let pool_name = pod
            .labels()
            .get(WORKER_POD_LABEL)
            .cloned()
            .unwrap_or_default();
        let Some(pool) = self.pools.get(&key.namespace, &pool_name) else {
            return self.reconcile_missing_pool(key, pod, &pool_name).await;
        };
        let available = is_worker_available(pod);

        let mut w = match self.registry.get_worker(key.worker_name()).await {
            Ok(w) => w,
            Err(s) if s.code() == Code::NotFound => {
                // node_name is required and immutable: the pod registers once
                // scheduled, and the binding update re-enqueues it.
                let node = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.node_name.clone())
                    .unwrap_or_default();
                if node.is_empty() {
                    return Ok(());
                }
                tracing::info!(worker = key.worker_name(), pod = %key.pod(), "Registering worker");
                let worker = pb::Worker {
                    metadata: Some(pb::ResourceMetadata {
                        name: key.worker_name().into(),
                        ..Default::default()
                    }),
                    worker_namespace: key.namespace.clone(),
                    worker_pool: pool_name.clone(),
                    worker_pod: key.name.clone(),
                    worker_pod_uid: key.uid.clone(),
                    node_name: node,
                    worker_spiffe_id: worker_spiffe_id(pod),
                    labels: pool_labels(&pool),
                    status: None,
                };
                // ALREADY_EXISTS is a lost create race: requeue and converge
                // through the update path.
                self.registry.create_worker(worker).await?
            }
            Err(status) => {
                return Err(SyncError::Registry {
                    context: "getting worker: ".into(),
                    status,
                })
            }
        };

        // Each write returns the Worker at its new version, which the next
        // carries as its precondition; ABORTED requeues and the retry
        // re-reads.
        if !available && state_of(&w) == pb::WorkerState::Active {
            tracing::info!(worker = key.worker_name(), pod = %key.pod(), "Marking worker unavailable (pod not Ready)");
            if let Some(m) = self
                .registry
                .mark_worker_unavailable(key.worker_name())
                .await?
            {
                w = m;
            }
        }

        // An update replaces the whole Worker, so the mutable fields are
        // edited onto the Worker as read and everything else goes back
        // unchanged. Drift in immutable fields takes a new pod.
        let labels = pool_labels(&pool);
        if w.labels != labels {
            tracing::info!(worker = key.worker_name(), pod = %key.pod(), "Updating worker (labels changed)");
            w.labels = labels;
            w = self.registry.update_worker(w).await?;
        }

        // A DRAINING Worker is left alone: its pod is on its way out.
        if available && state_of(&w) == pb::WorkerState::Unavailable {
            tracing::info!(worker = key.worker_name(), pod = %key.pod(), "Marking worker available (pod Ready)");
            self.registry
                .mark_worker_available(key.worker_name())
                .await?;
        }
        Ok(())
    }

    /// Deletes a worker pod gracefully after marking its Worker DRAINING;
    /// the pod's delete event then deregisters the Worker. The delete is
    /// preconditioned on the key's UID so it never removes a same-named
    /// replacement; a pod already gone or replaced is success.
    async fn delete_worker_pod(
        &self,
        key: &WorkerKey,
        pod: &Pod,
        reason: &str,
    ) -> Result<(), SyncError> {
        self.mark_worker_draining(key).await?;
        tracing::info!(worker = key.worker_name(), pod = %key.pod(), reason, "Deleting worker pod");
        let dp = DeleteParams {
            preconditions: Some(Preconditions {
                uid: pod.metadata.uid.clone(),
                resource_version: None,
            }),
            ..Default::default()
        };
        match Api::<Pod>::namespaced(self.client.clone(), &key.namespace)
            .delete(&key.name, &dp)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if api_errors::is_not_found(&e) || api_errors::is_conflict(&e) => Ok(()),
            Err(e) => Err(kube_err(format!("deleting pod ({reason})"))(e)),
        }
    }

    /// A live pod whose WorkerPool is not cached. The Worker is never
    /// deleted outright, which would crash its Actors: a pod the garbage
    /// collector is still removing is left to it, an orphaned pod is
    /// deleted gracefully, and a ReplicaSet's replacement (no Worker) is
    /// left alone.
    async fn reconcile_missing_pool(
        &self,
        key: &WorkerKey,
        pod: &Pod,
        pool: &str,
    ) -> Result<(), SyncError> {
        match self.registry.get_worker(key.worker_name()).await {
            Ok(_) => {}
            Err(s) if s.code() == Code::NotFound => return Ok(()),
            Err(status) => {
                return Err(SyncError::Registry {
                    context: "getting worker: ".into(),
                    status,
                })
            }
        }
        if self.pool_cleanup_pending(pod).await? {
            tracing::info!(worker = key.worker_name(), pod = %key.pod(), worker_pool = %format!("{}/{pool}", key.namespace), "Leaving worker to cascading delete (pool not found)");
            return Ok(());
        }
        self.delete_worker_pod(key, pod, &format!("orphaned from deleted pool {pool}"))
            .await
    }

    /// Whether the garbage collector will still remove `pod`. Walks the
    /// controller chain pod -> ReplicaSet -> Deployment -> WorkerPool: a gone
    /// or deleting owner means collection is pending, a cut link means the
    /// pod was orphaned.
    async fn pool_cleanup_pending(&self, pod: &Pod) -> Result<bool, SyncError> {
        let ns = pod.namespace().unwrap_or_default();
        let mut meta = pod.metadata.clone();
        for kind in ["ReplicaSet", "Deployment"] {
            let Some(r) = controller_of(&meta).filter(|r| r.kind == kind).cloned() else {
                return Ok(false);
            };
            match self.get_owner(&ns, &r).await? {
                None => return Ok(true),
                Some(owner) if owner.deletion_timestamp.is_some() => return Ok(true),
                Some(owner) => meta = owner,
            }
        }
        Ok(controller_of(&meta).is_some_and(|r| r.kind == "WorkerPool"))
    }

    /// The metadata of the owner `r` names, or `None` if it is gone or was
    /// replaced under its name.
    async fn get_owner(
        &self,
        ns: &str,
        r: &OwnerReference,
    ) -> Result<Option<ObjectMeta>, SyncError> {
        let ctx = format!("getting {} {ns}/{}", r.kind, r.name);
        let got = match r.kind.as_str() {
            "ReplicaSet" => api_errors::ignore_not_found(
                Api::<ReplicaSet>::namespaced(self.client.clone(), ns)
                    .get(&r.name)
                    .await,
            )
            .map(|o| o.map(|o| o.metadata)),
            "Deployment" => api_errors::ignore_not_found(
                Api::<Deployment>::namespaced(self.client.clone(), ns)
                    .get(&r.name)
                    .await,
            )
            .map(|o| o.map(|o| o.metadata)),
            other => {
                return Err(SyncError::Other(format!(
                    "{ctx}: unsupported owner kind {other:?}"
                )))
            }
        }
        .map_err(kube_err(ctx))?;
        Ok(got.filter(|m| m.uid.as_deref() == Some(r.uid.as_str())))
    }

    /// Moves the Worker to DRAINING so the scheduler stops routing new
    /// Actors to it. Draining is idempotent, and a Worker already gone is
    /// fine: the pod's delete event cleans up.
    pub async fn mark_worker_draining(&self, key: &WorkerKey) -> Result<(), SyncError> {
        tracing::info!(worker = key.worker_name(), pod = %key.pod(), "Marking worker draining (pod deleting)");
        match self.registry.drain_worker(key.worker_name()).await {
            Ok(_) => Ok(()),
            Err(s) if s.code() == Code::NotFound => Ok(()),
            Err(s) => Err(s.into()),
        }
    }

    /// Deregisters a Worker whose pod is gone. The delete releases the bound
    /// Actor and fails if that fails, leaving the record for a retry. A
    /// Worker already gone is success, which makes re-driving safe.
    pub async fn reconcile_dead_worker(&self, key: &WorkerKey) -> Result<(), SyncError> {
        match self.registry.delete_worker(key.worker_name()).await {
            Ok(_) => Ok(()),
            Err(s) if s.code() == Code::NotFound => Ok(()),
            Err(s) => Err(s.into()),
        }
    }

    /// Enqueues a key for every registered Worker, page by page. Live,
    /// unchanged Workers reconcile to a no-op; orphaned records get cleaned
    /// up. Each page is retried until it succeeds or `shutdown` fires, so a
    /// transient failure does not leave ghost Workers until the next start.
    pub async fn enqueue_registered_workers(&self, shutdown: &CancellationToken) {
        let mut token = String::new();
        loop {
            let page = match self.list_workers_page_with_retry(shutdown, &token).await {
                Ok(p) => p,
                Err(err) => {
                    tracing::error!(%err, "Stopped the scan of registered workers before it completed; the rest are retried at the next start");
                    return;
                }
            };
            for w in &page.workers {
                // The key is a pod identity, rebuilt from the recorded pod
                // fields rather than the Worker name.
                self.queue.add(WorkerKey::new(
                    &w.worker_namespace,
                    &w.worker_pod,
                    &w.worker_pod_uid,
                ));
            }
            if page.next_page_token.is_empty() {
                return;
            }
            token = page.next_page_token;
        }
    }

    fn list_delay(&self, attempt: u32) -> Duration {
        let factor = 1u32 << attempt.min(LIST_BACKOFF_STEPS);
        let base = self.list_backoff.saturating_mul(factor).min(self.list_cap);
        let spread = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() % 1000)
            .unwrap_or(0);
        let jitter = base.mul_f64(0.1 * f64::from(spread) / 1000.0);
        (base + jitter).min(self.list_cap.max(self.list_backoff))
    }

    /// One page of Workers, retried with capped exponential backoff until it
    /// succeeds or `shutdown` fires. The token is a stateless cursor, so a
    /// retry resumes at the same place; the backoff resets per page.
    pub async fn list_workers_page_with_retry(
        &self,
        shutdown: &CancellationToken,
        token: &str,
    ) -> Result<pb::ListWorkersResponse, String> {
        let mut attempt = 0;
        loop {
            match self.registry.list_workers(LIST_PAGE_SIZE, token).await {
                Ok(p) => return Ok(p),
                Err(err) => {
                    tracing::warn!(%err, "Listing a page of registered workers failed; retrying")
                }
            }
            let delay = self.list_delay(attempt);
            attempt += 1;
            tokio::select! {
                _ = shutdown.cancelled() => return Err("listing registered workers aborted: shutting down".into()),
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }
}

fn key_of(pod: &Pod) -> WorkerKey {
    WorkerKey::new(
        pod.namespace().unwrap_or_default(),
        pod.name_any(),
        pod.metadata.uid.clone().unwrap_or_default(),
    )
}

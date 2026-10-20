//! The informers the syncer reads.

use std::sync::Arc;
use std::time::Duration;

use hivekeep_k8s::crd::WorkerPool;
use hivekeep_k8s::informer::Informer;
use k8s_openapi::api::core::v1::Pod;
use kube::api::Api;
use kube::runtime::watcher;
use kube::{Client, ResourceExt};

use super::WORKER_POD_LABEL;

/// How often every cached worker pod is re-delivered, so a registry record
/// that drifted without a pod event is repaired on the next sweep.
pub const WORKER_POD_RESYNC: Duration = Duration::from_secs(5 * 60);

/// An informer over every pod carrying the worker pool label.
pub fn worker_pod_informer(client: Client) -> Arc<Informer<Pod>> {
    Informer::with_resync(
        Api::all(client),
        watcher::Config::default().labels(WORKER_POD_LABEL),
        Some(WORKER_POD_RESYNC),
    )
}

/// An informer over every WorkerPool.
pub fn worker_pool_informer(client: Client) -> Arc<Informer<WorkerPool>> {
    Informer::new(Api::all(client), watcher::Config::default())
}

/// The cached pods labeled as members of `namespace/pool`.
pub fn pods_of_pool(pods: &Informer<Pod>, namespace: &str, pool: &str) -> Vec<Arc<Pod>> {
    pods.list_filtered(|p| {
        p.namespace().as_deref() == Some(namespace)
            && p.labels().get(WORKER_POD_LABEL).map(String::as_str) == Some(pool)
    })
}

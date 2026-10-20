//! Drives one signer: queues every PodCertificateRequest change, answers
//! the requests this replica owns, and keeps the signer's trust bundles in
//! place.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_k8s::api_errors::is_not_found;
use hivekeep_k8s::cluster_trust_bundle::{ClusterTrustBundle, ClusterTrustBundleClient};
use hivekeep_k8s::informer::InformerEvent;
use hivekeep_node::workqueue::{RateLimiter, WorkQueue};
use kube::api::PostParams;
use kube::ResourceExt;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::status::is_answered;
use super::SignerImpl;
use crate::error::SignerError;
use crate::pod_certificate::{PcrClient, PodCertificateRequest};
use crate::rendezvous::hasher::LEASE_DURATION;
use crate::rendezvous::Hasher;

/// Base period of trust bundle maintenance; each wait adds up to as much
/// again in jitter.
pub const BUNDLE_PERIOD: Duration = Duration::from_secs(5);

/// The rendezvous item whose owner maintains trust bundles.
pub const BUNDLE_ITEM: &str = "maintain-trust-bundles";

/// First retry delay of a failing request.
pub const RETRY_BASE: Duration = Duration::from_millis(5);

/// Longest retry delay of a failing request.
pub const RETRY_MAX: Duration = Duration::from_secs(1000);

/// The controller for one signer.
pub struct Controller {
    handler: Arc<dyn SignerImpl>,
    hasher: Arc<dyn Hasher>,
    pcrs: Arc<PcrClient>,
    trust_bundles: Arc<ClusterTrustBundleClient>,
    queue: WorkQueue,
    limiter: RateLimiter,
}

impl Controller {
    /// A controller for `handler`, fed by the shared request cache of
    /// `pcrs`. Must be created on a tokio runtime, before the cache starts
    /// if the initial list should be queued.
    pub fn new(
        handler: Arc<dyn SignerImpl>,
        hasher: Arc<dyn Hasher>,
        pcrs: Arc<PcrClient>,
        trust_bundles: Arc<ClusterTrustBundleClient>,
    ) -> Arc<Self> {
        let queue = WorkQueue::new();
        let feed = queue.clone();
        pcrs.informer().add_handler(move |ev| {
            let obj = match ev {
                InformerEvent::Added(o) | InformerEvent::Deleted(o) => o,
                InformerEvent::Updated { new, .. } => new,
            };
            feed.add(&key_of(obj.namespace().as_deref(), &obj.name_any()));
        });
        Arc::new(Controller {
            handler,
            hasher,
            pcrs,
            trust_bundles,
            queue,
            limiter: RateLimiter::new(RETRY_BASE, RETRY_MAX),
        })
    }

    /// The signer this controller drives.
    pub fn signer_name(&self) -> &str {
        self.handler.signer_name()
    }

    /// Waits for the request cache, then runs `workers` (at least one)
    /// request workers and the trust bundle loop until `shutdown`.
    pub async fn run(self: Arc<Self>, workers: usize, shutdown: CancellationToken) {
        tokio::select! {
            _ = shutdown.cancelled() => {
                self.queue.shut_down();
                return;
            }
            _ = self.pcrs.informer().wait_synced() => {}
        }
        let mut tasks = JoinSet::new();
        for _ in 0..workers.max(1) {
            let me = self.clone();
            tasks.spawn(async move { while me.process_next_work_item().await {} });
        }
        let me = self.clone();
        let stop = shutdown.clone();
        tasks.spawn(async move {
            loop {
                me.ensure_bundles().await;
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tokio::time::sleep(jittered(BUNDLE_PERIOD)) => {}
                }
            }
        });
        shutdown.cancelled().await;
        self.queue.shut_down();
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    /// Handles the next queued request. Returns `false` once the queue
    /// has shut down.
    pub async fn process_next_work_item(&self) -> bool {
        let Some(key) = self.queue.get().await else {
            return false;
        };
        self.process(&key).await;
        self.queue.done(&key);
        true
    }

    async fn process(&self, key: &str) {
        let (namespace, name) = split_key(key);
        let Some(pcr) = self.pcrs.get_cached(namespace, name) else {
            // Deleted: nothing left to answer.
            self.limiter.forget(key);
            return;
        };
        match self.handle_pcr(&pcr).await {
            Ok(()) => self.limiter.forget(key),
            // Not a failure, so no backoff: ownership only moves when a
            // replica joins or its lease expires, so look again after a
            // lease duration.
            Err(SignerError::NotAssigned) => self.queue.add_after(key, LEASE_DURATION),
            Err(err) => {
                tracing::error!(%err, key, signer = self.signer_name(), "Error while handling PodCertificateRequest");
                self.queue.add_after(key, self.limiter.when(key));
            }
        }
    }

    /// Answers `pcr` if it is addressed to this signer, unanswered, and
    /// owned by this replica.
    pub async fn handle_pcr(&self, pcr: &PodCertificateRequest) -> Result<(), SignerError> {
        // A request for another signer never becomes ours: drop it.
        if pcr.spec.signer_name != self.handler.signer_name() {
            return Ok(());
        }
        // There is no approval stage, and kube-apiserver has already
        // checked that the Pod, service account and Node belong together.
        if is_answered(pcr) {
            return Ok(());
        }
        let key = key_of(pcr.metadata.namespace.as_deref(), &pcr.name_any());
        if !self.hasher.assigned_to_this_replica(&key) {
            return Err(SignerError::NotAssigned);
        }
        tracing::info!(key, signer = self.signer_name(), "Processing PCR");
        self.handler.make_cert(pcr).await
    }

    /// Creates or corrects this signer's trust bundles, if this replica
    /// owns their maintenance.
    pub async fn ensure_bundles(&self) {
        if !self.hasher.assigned_to_this_replica(BUNDLE_ITEM) {
            return;
        }
        let wanted = match self.handler.desired_cluster_trust_bundles() {
            Ok(w) => w,
            Err(err) => {
                tracing::error!(%err, signer = self.signer_name(), "Error while retrieving CA trust anchors");
                return;
            }
        };
        for want in wanted {
            if let Err(err) = self.ensure_bundle(&want).await {
                tracing::error!(%err, key = want.name_any(), "Error while reconciling ClusterTrustBundle");
                return;
            }
        }
    }

    async fn ensure_bundle(&self, want: &ClusterTrustBundle) -> Result<(), kube::Error> {
        let name = want.name_any();
        let mut current = match self.trust_bundles.get(&name).await {
            Ok(b) => b,
            Err(err) if is_not_found(&err) => {
                self.trust_bundles
                    .create(&PostParams::default(), want)
                    .await?;
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        if want.labels() == current.labels() && want.spec == current.spec {
            tracing::debug!(key = name, "ClusterTrustBundle already in correct state");
            return Ok(());
        }
        current.metadata.labels = want.metadata.labels.clone();
        current.spec.trust_bundle = want.spec.trust_bundle.clone();
        self.trust_bundles
            .update(&PostParams::default(), &current)
            .await?;
        Ok(())
    }
}

/// The queue key of an object: `namespace/name`, or `name` when
/// cluster-scoped.
pub fn key_of(namespace: Option<&str>, name: &str) -> String {
    match namespace {
        Some(ns) if !ns.is_empty() => format!("{ns}/{name}"),
        _ => name.to_string(),
    }
}

fn split_key(key: &str) -> (&str, &str) {
    key.split_once('/').unwrap_or(("", key))
}

/// `period` plus a random fraction of it.
fn jittered(period: Duration) -> Duration {
    let r = RandomState::new().hash_one(std::time::Instant::now());
    period + period.mul_f64((r >> 11) as f64 / (1u64 << 53) as f64)
}

#[cfg(test)]
#[path = "controller_tests.rs"]
mod tests;

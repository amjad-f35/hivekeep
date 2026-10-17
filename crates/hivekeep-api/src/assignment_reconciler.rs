//! Keeps each Worker's assignments in line with its epoch: when a new
//! worker process registers (registered epoch above observed), the Actors
//! the previous process took with it are crashed and released.

use std::sync::Arc;

use hivekeep_store::{Store, StoreError};
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::leases;
use crate::worker_cache::WorkerCache;
use crate::worker_workflow::{needs_assignment_reconcile, WorkerWorkflow};
use crate::workqueue::WorkQueue;

const WORKERS: usize = 2;

/// The reconciler.
pub struct AssignmentReconciler {
    store: Arc<dyn Store>,
    workflow: Arc<WorkerWorkflow>,
}

impl AssignmentReconciler {
    /// A reconciler over `store`.
    pub fn new(store: Arc<dyn Store>, workflow: Arc<WorkerWorkflow>) -> Arc<Self> {
        Arc::new(AssignmentReconciler { store, workflow })
    }

    /// Feeds Workers from `cache` into the queue and drains it until `stop`.
    pub fn start(self: &Arc<Self>, cache: &WorkerCache, stop: CancellationToken) {
        let q: WorkQueue<String> = WorkQueue::new(stop);
        for _ in 0..WORKERS {
            let (me, q) = (self.clone(), q.clone());
            tokio::spawn(async move {
                while let Some(name) = q.get().await {
                    match me.reconcile_one(&name).await {
                        Ok(()) => q.forget(&name),
                        Err(e) => {
                            if !matches!(e, Error::ActorBusy) {
                                tracing::error!(worker = %name, error = %e, "failed to reconcile a worker's assignments, requeueing");
                            }
                            q.add_rate_limited(name);
                        }
                    }
                }
            });
        }
        let q2 = q.clone();
        cache.add_handler(Arc::new(move |w| {
            if needs_assignment_reconcile(w) {
                q2.add(crate::fields::worker_name(w).to_string());
            }
        }));
    }

    /// Reconciles one Worker under its sweep lease; a held lease is retried.
    pub async fn reconcile_one(&self, name: &str) -> Result<()> {
        let lease = match self
            .store
            .acquire_lease(&leases::worker_assignment_key(name))
            .await
        {
            Ok(l) => l,
            Err(StoreError::LeaseConflict) => return Err(Error::ActorBusy),
            Err(e) => return Err(Error::from(e).context("while acquiring lease")),
        };
        let res = self.workflow.reconcile_assignments(name).await;
        lease.close().await;
        res
    }
}

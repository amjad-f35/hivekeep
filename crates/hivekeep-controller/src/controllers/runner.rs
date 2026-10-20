//! Running a reconciler: workers draining a [`WorkQueue`], and watches
//! that turn informer events into keys.

use std::future::Future;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use hivekeep_k8s::informer::InformerEvent;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::error::Result;
use super::queue::{ObjectKey, WorkQueue};

/// What to do with a key after a successful reconcile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Nothing until the next event.
    Done,
    /// Reconcile again after the delay.
    RequeueAfter(Duration),
}

/// One controller's reconcile.
#[async_trait]
pub trait Reconcile: Send + Sync + 'static {
    /// Moves the object named by `key` towards its desired state. An error
    /// requeues the key with backoff.
    async fn reconcile(&self, key: &ObjectKey) -> Result<Action>;
}

/// Reconciles per controller and result (`success`, `requeue_after`,
/// `error`). The family names follow the common controller convention so
/// dashboards built for other Kubernetes controllers read them unchanged.
pub const RECONCILE_TOTAL: &str = "controller_runtime_reconcile_total";
/// Failed reconciles per controller.
pub const RECONCILE_ERRORS_TOTAL: &str = "controller_runtime_reconcile_errors_total";
/// Reconcile durations per controller.
pub const RECONCILE_TIME: &str = "controller_runtime_reconcile_time_seconds";

struct ReconcileMetrics {
    total: prometheus::IntCounterVec,
    errors: prometheus::IntCounterVec,
    time: prometheus::HistogramVec,
}

static METRICS: LazyLock<ReconcileMetrics> = LazyLock::new(|| ReconcileMetrics {
    total: prometheus::IntCounterVec::new(
        prometheus::Opts::new(
            RECONCILE_TOTAL,
            "Total number of reconciliations per controller.",
        ),
        &["controller", "result"],
    )
    .expect("valid reconcile counter"),
    errors: prometheus::IntCounterVec::new(
        prometheus::Opts::new(
            RECONCILE_ERRORS_TOTAL,
            "Total number of reconciliation errors per controller.",
        ),
        &["controller"],
    )
    .expect("valid reconcile error counter"),
    time: prometheus::HistogramVec::new(
        prometheus::HistogramOpts::new(
            RECONCILE_TIME,
            "Length of time per reconciliation per controller.",
        )
        .buckets(vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
        ]),
        &["controller"],
    )
    .expect("valid reconcile histogram"),
});

/// Registers the reconcile metrics every runner records with `registry`.
/// Registering them twice with one registry is not an error.
pub fn register_metrics(registry: &prometheus::Registry) -> prometheus::Result<()> {
    let m = &*METRICS;
    for c in [
        Box::new(m.total.clone()) as Box<dyn prometheus::core::Collector>,
        Box::new(m.errors.clone()),
        Box::new(m.time.clone()),
    ] {
        match registry.register(c) {
            Ok(()) | Err(prometheus::Error::AlreadyReg) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Reconciles one key and settles it in `queue`.
pub async fn process<R: Reconcile + ?Sized>(
    name: &str,
    reconciler: &R,
    queue: &Arc<WorkQueue<ObjectKey>>,
    key: ObjectKey,
) {
    let start = Instant::now();
    let result = reconciler.reconcile(&key).await;
    METRICS
        .time
        .with_label_values(&[name])
        .observe(start.elapsed().as_secs_f64());
    let outcome = match result {
        Ok(Action::Done) => {
            queue.forget(&key);
            "success"
        }
        Ok(Action::RequeueAfter(d)) => {
            queue.forget(&key);
            queue.add_after(key.clone(), d);
            "requeue_after"
        }
        Err(err) => {
            tracing::warn!(controller = name, %key, %err, "Reconcile failed; retrying");
            queue.add_rate_limited(key.clone());
            METRICS.errors.with_label_values(&[name]).inc();
            "error"
        }
    };
    METRICS.total.with_label_values(&[name, outcome]).inc();
    queue.done(&key);
}

/// Spawns `workers` tasks reconciling keys from `queue` until it is shut
/// down or `shutdown` fires.
pub fn spawn_workers<R: Reconcile>(
    name: &'static str,
    reconciler: Arc<R>,
    queue: Arc<WorkQueue<ObjectKey>>,
    workers: usize,
    shutdown: CancellationToken,
) -> Vec<JoinHandle<()>> {
    (0..workers.max(1))
        .map(|_| {
            let (r, q, stop) = (reconciler.clone(), queue.clone(), shutdown.clone());
            tokio::spawn(async move {
                loop {
                    let key = tokio::select! {
                        k = q.get() => k,
                        _ = stop.cancelled() => None,
                    };
                    let Some(key) = key else { return };
                    process(name, r.as_ref(), &q, key).await;
                }
            })
        })
        .collect()
}

/// Spawns a task queueing the keys `map` returns for each object in
/// `events`: the object of an add or delete, and both the old and the new
/// object of an update.
pub fn spawn_watch<K, S, F, Fut>(
    events: S,
    queue: Arc<WorkQueue<ObjectKey>>,
    shutdown: CancellationToken,
    map: F,
) -> JoinHandle<()>
where
    K: Send + Sync + 'static,
    S: Stream<Item = InformerEvent<K>> + Send + 'static,
    F: Fn(Arc<K>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Vec<ObjectKey>> + Send,
{
    tokio::spawn(async move {
        let mut events = Box::pin(events);
        loop {
            let ev = tokio::select! {
                ev = events.next() => ev,
                _ = shutdown.cancelled() => None,
            };
            let Some(ev) = ev else { return };
            let objects = match ev {
                InformerEvent::Added(o) | InformerEvent::Deleted(o) => vec![o],
                InformerEvent::Updated { old, new } => vec![old, new],
            };
            for o in objects {
                for key in map(o).await {
                    queue.add(key);
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::error::Error;
    use parking_lot::Mutex;

    struct Flaky {
        calls: Mutex<Vec<ObjectKey>>,
        fail_first: Mutex<u32>,
    }

    #[async_trait]
    impl Reconcile for Flaky {
        async fn reconcile(&self, key: &ObjectKey) -> Result<Action> {
            self.calls.lock().push(key.clone());
            let mut f = self.fail_first.lock();
            if *f > 0 {
                *f -= 1;
                return Err(Error::Invalid("flaky".into()));
            }
            Ok(Action::Done)
        }
    }

    #[tokio::test]
    async fn failures_are_retried_until_success() {
        let r = Arc::new(Flaky {
            calls: Mutex::new(Vec::new()),
            fail_first: Mutex::new(2),
        });
        let q = WorkQueue::new();
        let stop = CancellationToken::new();
        let workers = spawn_workers("test", r.clone(), q.clone(), 1, stop.clone());
        q.add(ObjectKey::new("ns", "a"));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while r.calls.lock().len() < 3 {
            assert!(tokio::time::Instant::now() < deadline, "not retried");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            q.failures(&ObjectKey::new("ns", "a")),
            0,
            "success resets the backoff"
        );
        stop.cancel();
        for w in workers {
            w.await.unwrap();
        }
    }

    #[tokio::test]
    async fn watch_maps_old_and_new_objects() {
        let q = WorkQueue::new();
        let stop = CancellationToken::new();
        let events = futures::stream::iter(vec![InformerEvent::Updated {
            old: Arc::new("old".to_string()),
            new: Arc::new("new".to_string()),
        }]);
        let task = spawn_watch(events, q.clone(), stop, |o: Arc<String>| async move {
            vec![ObjectKey::cluster(o.as_str())]
        });
        task.await.unwrap();
        let mut got = vec![q.get().await.unwrap(), q.get().await.unwrap()];
        got.sort();
        assert_eq!(
            got,
            vec![ObjectKey::cluster("new"), ObjectKey::cluster("old")]
        );
    }
}

//! Per-pool worker gauges, computed from the WorkerPool cache at scrape
//! time.

use std::sync::Arc;

use hivekeep_k8s::crd::WorkerPool;
use kube::ResourceExt;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntGaugeVec, Opts, Registry};

/// Worker pods a pool asks for (`spec.replicas`).
pub const DESIRED_WORKERS: &str = "hivekeep_workerpool_desired_workers";
/// Worker pods of a pool that are ready (`status.readyReplicas`).
pub const READY_WORKERS: &str = "hivekeep_workerpool_ready_workers";
/// Label naming the pool's namespace.
pub const LABEL_NAMESPACE: &str = "hk_workerpool_namespace";
/// Label naming the pool.
pub const LABEL_NAME: &str = "hk_workerpool_name";

type PoolSource = dyn Fn() -> Vec<Arc<WorkerPool>> + Send + Sync;

/// Collects [`DESIRED_WORKERS`] and [`READY_WORKERS`] for every pool the
/// source returns.
pub struct WorkerPoolMetrics {
    source: Box<PoolSource>,
    desired: IntGaugeVec,
    ready: IntGaugeVec,
    descs: Vec<Desc>,
}

fn gauge(name: &str, help: &str) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), &[LABEL_NAMESPACE, LABEL_NAME])
        .expect("valid metric definition")
}

impl WorkerPoolMetrics {
    /// A collector over the pools `source` lists.
    pub fn new(source: impl Fn() -> Vec<Arc<WorkerPool>> + Send + Sync + 'static) -> Self {
        let desired = gauge(
            DESIRED_WORKERS,
            "Number of worker pods requested for a WorkerPool (spec.replicas).",
        );
        let ready = gauge(
            READY_WORKERS,
            "Number of worker pods currently ready for a WorkerPool (status.readyReplicas).",
        );
        let descs = desired
            .desc()
            .into_iter()
            .chain(ready.desc())
            .cloned()
            .collect();
        WorkerPoolMetrics {
            source: Box::new(source),
            desired,
            ready,
            descs,
        }
    }

    /// Registers a collector over `source` with `registry`.
    pub fn register(
        registry: &Registry,
        source: impl Fn() -> Vec<Arc<WorkerPool>> + Send + Sync + 'static,
    ) -> prometheus::Result<()> {
        registry.register(Box::new(Self::new(source)))
    }
}

impl Collector for WorkerPoolMetrics {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        // Pools that are gone drop out of the next scrape.
        self.desired.reset();
        self.ready.reset();
        for wp in (self.source)() {
            let ns = wp.namespace().unwrap_or_default();
            let name = wp.name_any();
            let labels = [ns.as_str(), name.as_str()];
            self.desired
                .with_label_values(&labels)
                .set(i64::from(wp.spec.replicas));
            let ready = wp.status.as_ref().map(|s| s.ready_replicas).unwrap_or(0);
            self.ready.with_label_values(&labels).set(i64::from(ready));
        }
        let mut out = self.desired.collect();
        out.extend(self.ready.collect());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::test_support::make_worker_pool;
    use hivekeep_k8s::crd::WorkerPoolStatus;
    use std::collections::HashMap;

    #[test]
    fn worker_pool_metrics() {
        let mut wp = make_worker_pool("test-metrics", "default", 4, "worker:v1");
        wp.status = Some(WorkerPoolStatus {
            replicas: 4,
            ready_replicas: 2,
            ..Default::default()
        });
        let pool = Arc::new(wp);
        let registry = Registry::new();
        WorkerPoolMetrics::register(&registry, move || vec![pool.clone()]).unwrap();
        let mut got = HashMap::new();
        for mf in registry.gather() {
            for m in mf.get_metric() {
                let labels: HashMap<&str, &str> = m
                    .get_label()
                    .iter()
                    .map(|l| (l.name(), l.value()))
                    .collect();
                if labels.get(LABEL_NAMESPACE) == Some(&"default")
                    && labels.get(LABEL_NAME) == Some(&"test-metrics")
                {
                    got.insert(mf.name().to_string(), m.get_gauge().value() as i64);
                }
            }
        }
        let want = HashMap::from([
            (DESIRED_WORKERS.to_string(), 4),
            (READY_WORKERS.to_string(), 2),
        ]);
        assert_eq!(got, want);
    }
}

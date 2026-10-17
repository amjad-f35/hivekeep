//! Prometheus metrics of the control plane.
//!
//! Every label set is fixed; dimensions unknown at record time are left
//! empty. Actor identity never appears in a label (it would explode
//! cardinality); the actor event log carries it instead.

use std::time::Instant;

use hivekeep_proto::api::v1 as pb;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};
use tonic::Code;

use crate::runtime_compat;

/// Prefix of every metric name.
pub const METRIC_PREFIX: &str = "hivekeep";

/// Label names. They are the shared attribute keys of
/// [`hivekeep_core::attr`] spelled for Prometheus (dots become
/// underscores), so a series reads the same whether it was scraped or
/// pushed over OTLP.
pub mod label {
    /// The lifecycle operation ([`hivekeep_core::attr::ACTOR_OPERATION_NAME_KEY`]).
    pub const OPERATION: &str = "hk_actor_operation_name";
    /// The template name ([`hivekeep_core::attr::TEMPLATE_NAME_KEY`]).
    pub const TEMPLATE_NAME: &str = "hk_template_name";
    /// The template hive ([`hivekeep_core::attr::TEMPLATE_HIVE_KEY`]).
    pub const TEMPLATE_HIVE: &str = "hk_template_hive";
    /// The worker pool namespace ([`hivekeep_core::attr::WORKER_POOL_NAMESPACE_KEY`]).
    pub const POOL_NAMESPACE: &str = "hk_workerpool_namespace";
    /// The worker pool ([`hivekeep_core::attr::WORKER_POOL_NAME_KEY`]).
    pub const POOL: &str = "hk_workerpool_name";
    /// The sandbox class ([`hivekeep_core::attr::SANDBOX_CLASS_KEY`]).
    pub const SANDBOX_CLASS: &str = "hk_sandbox_class";
    /// What a resume restored from ([`hivekeep_core::attr::SNAPSHOT_KIND_KEY`]).
    pub const SNAPSHOT_KIND: &str = "hk_snapshot_kind";
    /// The requested fidelity ([`hivekeep_core::attr::SNAPSHOT_FIDELITY_KEY`]).
    pub const SNAPSHOT_FIDELITY: &str = "hk_snapshot_fidelity";
    /// The worker scheduling state ([`hivekeep_core::attr::WORKER_STATE_KEY`]).
    pub const WORKER_STATE: &str = "hk_worker_state";
    /// The Actor state reached ([`hivekeep_core::attr::ACTOR_STATE_KEY`]).
    pub const ACTOR_STATE: &str = "hk_actor_state";
    /// The gRPC code of a failure ([`hivekeep_core::attr::ERROR_TYPE_KEY`]).
    pub const ERROR_TYPE: &str = "error_type";
    /// The scheduler outcome.
    pub const OUTCOME: &str = "outcome";
}

/// Committed Actor state changes by the state reached, counted where the
/// lifecycle event is logged (see [`crate::events`]).
pub const STATE_CHANGES: &str = "hivekeep_actor_state_changes_total";

static STATE_CHANGE_COUNTER: std::sync::LazyLock<IntCounterVec> = std::sync::LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            STATE_CHANGES,
            "Actor state change events, by the state reached.",
        ),
        &[label::ACTOR_STATE],
    )
    .expect("valid state change counter")
});

/// Counts one committed state change to `state`. Process-wide: the event
/// log is written from free functions, and every [`Metrics`] registry
/// serves the same counter.
pub fn count_state_change(state: &str) {
    STATE_CHANGE_COUNTER.with_label_values(&[state]).inc();
}

/// Operation names used as labels and in the event log.
pub mod op {
    /// CreateActor.
    pub const CREATE: &str = "create";
    /// ResumeActor.
    pub const RESUME: &str = "resume";
    /// SuspendActor.
    pub const SUSPEND: &str = "suspend";
    /// DeleteActor.
    pub const DELETE: &str = "delete";
    /// RevertActor.
    pub const REVERT: &str = "revert";
    /// Unknown or none.
    pub const UNKNOWN: &str = "unknown";
}

/// Scheduler outcomes.
pub mod outcome {
    /// A worker was assigned.
    pub const ASSIGNED: &str = "assigned";
    /// No worker had room.
    pub const NO_CAPACITY: &str = "no_capacity";
    /// The attempt failed.
    pub const ERROR: &str = "error";
}

/// Dimensions of one lifecycle operation.
#[derive(Debug, Clone, Default)]
pub struct OpLabels {
    /// Template name.
    pub template_name: String,
    /// Template hive.
    pub template_hive: String,
    /// Worker pool namespace.
    pub pool_namespace: String,
    /// Worker pool.
    pub pool: String,
    /// Sandbox class.
    pub sandbox_class: String,
    /// What a resume restored from (boot, local, latest).
    pub snapshot_kind: String,
    /// The fidelity requested on the wire.
    pub snapshot_fidelity: String,
}

impl OpLabels {
    /// The dimensions known from an Actor record.
    pub fn of(actor: Option<&pb::Actor>, snapshot_kind: &str, fidelity: &str) -> Self {
        let mut l = OpLabels {
            snapshot_kind: snapshot_kind.into(),
            snapshot_fidelity: fidelity.into(),
            ..Default::default()
        };
        let Some(actor) = actor else { return l };
        if let Some(t) = &actor.actor_template {
            l.template_name = t.name.clone();
            l.template_hive = t.hive.clone();
        }
        let status = actor.status.as_ref();
        if let Some(a) = status.and_then(|s| s.worker_assignment.as_ref()) {
            l.pool_namespace = a.worker_namespace.clone();
            l.pool = a.worker_pool.clone();
            l.sandbox_class = a
                .sandbox_runtime
                .as_ref()
                .map(|r| r.sandbox_class.clone())
                .unwrap_or_default();
        }
        if l.sandbox_class.is_empty() {
            if let Some(s) = status {
                l.sandbox_class = s
                    .snapshots
                    .iter()
                    .find(|x| x.generation == s.last_assigned_generation)
                    .and_then(|x| x.sandbox_runtime.as_ref())
                    .map(|r| r.sandbox_class.clone())
                    .unwrap_or_default();
            }
        }
        l
    }
}

/// The instruments.
pub struct Metrics {
    registry: Registry,
    lifecycle: HistogramVec,
    assignment: HistogramVec,
    crashes: IntCounterVec,
    workers: IntGaugeVec,
    known_pools: parking_lot::Mutex<Vec<KnownPool>>,
}

/// A worker pool reported with zeroes even when it has no Workers, so an
/// empty or saturated pool reads 0 rather than an absent series.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownPool {
    /// Pool namespace.
    pub namespace: String,
    /// Pool name.
    pub pool: String,
    /// The pool's default sandbox class.
    pub sandbox_class: String,
}

/// Every scheduling state the worker gauge reports.
pub const WORKER_STATES: [&str; 4] = ["idle", "partial", "at_capacity", "unschedulable"];

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

fn code_label(code: Option<Code>) -> String {
    code.map(|c| format!("{c:?}")).unwrap_or_default()
}

impl Metrics {
    /// Instruments registered on a fresh registry.
    pub fn new() -> Self {
        Self::with_registry(Registry::new())
    }

    /// Instruments registered on `registry`, which the caller serves (and
    /// may push over OTLP).
    pub fn with_registry(registry: Registry) -> Self {
        let lifecycle = HistogramVec::new(
            HistogramOpts::new(
                format!("{METRIC_PREFIX}_actor_lifecycle_operation_duration_seconds"),
                "Duration of an Actor lifecycle operation (create, resume, suspend, delete, revert).",
            )
            .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]),
            &[label::OPERATION, label::TEMPLATE_NAME, label::TEMPLATE_HIVE, label::POOL_NAMESPACE, label::POOL, label::SANDBOX_CLASS, label::SNAPSHOT_KIND, label::SNAPSHOT_FIDELITY, label::ERROR_TYPE],
        )
        .expect("valid lifecycle histogram");
        let assignment = HistogramVec::new(
            HistogramOpts::new(
                format!("{METRIC_PREFIX}_scheduler_assignment_duration_seconds"),
                "Duration of one worker-assignment attempt during Actor resume.",
            )
            .buckets(vec![
                0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ]),
            &[
                label::OUTCOME,
                label::POOL_NAMESPACE,
                label::POOL,
                label::SANDBOX_CLASS,
                label::ERROR_TYPE,
            ],
        )
        .expect("valid assignment histogram");
        let crashes = IntCounterVec::new(
            Opts::new(
                format!("{METRIC_PREFIX}_actor_crashes_total"),
                "Actors moved to CRASHED.",
            ),
            &[
                label::OPERATION,
                label::TEMPLATE_HIVE,
                label::TEMPLATE_NAME,
                label::POOL_NAMESPACE,
                label::POOL,
                label::SANDBOX_CLASS,
            ],
        )
        .expect("valid crash counter");
        let workers = IntGaugeVec::new(
            Opts::new(
                format!("{METRIC_PREFIX}_workerpool_workers"),
                "Workers by pool, scheduling state and sandbox class.",
            ),
            &[
                label::POOL_NAMESPACE,
                label::POOL,
                label::WORKER_STATE,
                label::SANDBOX_CLASS,
            ],
        )
        .expect("valid worker gauge");
        for c in [
            Box::new(lifecycle.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(assignment.clone()),
            Box::new(crashes.clone()),
            Box::new(workers.clone()),
            Box::new(STATE_CHANGE_COUNTER.clone()),
        ] {
            registry.register(c).expect("unique metric names");
        }
        Metrics {
            registry,
            lifecycle,
            assignment,
            crashes,
            workers,
            known_pools: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// The registry to expose.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Records one lifecycle operation that started at `start`.
    pub fn record_lifecycle_op(&self, op: &str, start: Instant, err: Option<Code>, l: &OpLabels) {
        self.lifecycle
            .with_label_values(&[
                op,
                &l.template_name,
                &l.template_hive,
                &l.pool_namespace,
                &l.pool,
                &l.sandbox_class,
                &l.snapshot_kind,
                &l.snapshot_fidelity,
                &code_label(err),
            ])
            .observe(start.elapsed().as_secs_f64());
    }

    /// Records one assignment attempt.
    pub fn record_assignment(
        &self,
        elapsed_secs: f64,
        outcome: &str,
        pool_namespace: &str,
        pool: &str,
        class: &str,
        err: Option<Code>,
    ) {
        let err = if outcome == outcome::ERROR {
            code_label(err)
        } else {
            String::new()
        };
        self.assignment
            .with_label_values(&[outcome, pool_namespace, pool, class, &err])
            .observe(elapsed_secs);
    }

    /// Counts a crash of `actor`, as it was before the crash cleared its
    /// assignment.
    pub fn record_crash(&self, actor: &pb::Actor, op: &str) {
        let l = OpLabels::of(Some(actor), "", "");
        self.crashes
            .with_label_values(&[
                op,
                &l.template_hive,
                &l.template_name,
                &l.pool_namespace,
                &l.pool,
                &l.sandbox_class,
            ])
            .inc();
    }

    /// Sets the pools seeded with zeroes on every observation.
    pub fn set_known_pools(&self, pools: Vec<KnownPool>) {
        *self.known_pools.lock() = pools;
    }

    /// Refreshes the worker gauge from `cache`, leaving it untouched while
    /// the cache is resyncing.
    pub fn observe_cache(&self, cache: &crate::worker_cache::WorkerCache) {
        if let Ok(ws) = cache.workers() {
            self.observe_workers(ws.iter().map(|w| w.as_ref()));
        }
    }

    /// Replaces the worker gauge with a tally of `workers`.
    pub fn observe_workers<'a>(&self, workers: impl IntoIterator<Item = &'a pb::Worker>) {
        self.workers.reset();
        for p in self.known_pools.lock().iter() {
            let class = if p.sandbox_class.is_empty() {
                "gvisor"
            } else {
                p.sandbox_class.as_str()
            };
            for st in WORKER_STATES {
                self.workers
                    .with_label_values(&[&p.namespace, &p.pool, st, class])
                    .set(0);
            }
        }
        for w in workers {
            let class = runtime_compat::sandbox_class(w.status.as_ref());
            let class = if class.is_empty() { "unknown" } else { class };
            self.workers
                .with_label_values(&[&w.worker_namespace, &w.worker_pool, worker_state(w), class])
                .inc();
        }
    }

    /// The text exposition of every metric.
    pub fn encode(&self) -> String {
        let mut buf = Vec::new();
        let _ = prometheus::Encoder::encode(
            &prometheus::TextEncoder::new(),
            &self.registry.gather(),
            &mut buf,
        );
        String::from_utf8(buf).unwrap_or_default()
    }
}

/// The scheduling state of a worker by actor slots: idle, partial,
/// at_capacity, or unschedulable when not ACTIVE or with no capacity.
pub fn worker_state(w: &pb::Worker) -> &'static str {
    let st = w.status.as_ref();
    let cap = st.and_then(|s| s.capacity).map(|c| c.actors).unwrap_or(0);
    if st.map(|s| s.state) != Some(pb::WorkerState::Active as i32) || cap <= 0 {
        return "unschedulable";
    }
    match st.and_then(|s| s.allocated).map(|a| a.actors).unwrap_or(0) {
        a if a <= 0 => "idle",
        a if a < cap => "partial",
        _ => "at_capacity",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_encodes() {
        let m = Metrics::new();
        m.record_lifecycle_op(
            op::RESUME,
            Instant::now(),
            Some(Code::Unavailable),
            &OpLabels::default(),
        );
        m.record_assignment(0.01, outcome::ASSIGNED, "ns", "p", "gvisor", None);
        m.record_crash(&pb::Actor::default(), op::UNKNOWN);
        let w = pb::Worker {
            worker_pool: "p".into(),
            status: Some(pb::WorkerStatus {
                state: pb::WorkerState::Active as i32,
                capacity: Some(pb::WorkerResources {
                    actors: 2,
                    ..Default::default()
                }),
                allocated: Some(pb::WorkerResources {
                    actors: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(worker_state(&w), "partial");
        m.observe_workers([&w]);
        let text = m.encode();
        assert!(text.contains("hivekeep_actor_lifecycle_operation_duration_seconds"));
        assert!(text.contains("error_type=\"Unavailable\""));
        assert!(text.contains("hivekeep_workerpool_workers{hk_sandbox_class=\"unknown\",hk_worker_state=\"partial\",hk_workerpool_name=\"p\""), "{text}");
        assert!(text.contains("hivekeep_actor_crashes_total"));
        assert_eq!(worker_state(&pb::Worker::default()), "unschedulable");
    }

    #[test]
    fn state_changes_are_served_by_every_registry() {
        let (a, b) = (Metrics::new(), Metrics::new());
        count_state_change("running");
        for m in [&a, &b] {
            assert!(
                m.encode()
                    .contains("hivekeep_actor_state_changes_total{hk_actor_state=\"running\"}"),
                "{}",
                m.encode()
            );
        }
    }
}

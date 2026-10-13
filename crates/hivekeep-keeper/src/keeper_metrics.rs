//! Per-phase timing of checkpoints and restores, as metrics and as a log
//! record per operation.
//!
//! Actor identity never becomes a metric label (it would be unbounded);
//! the log record carries it instead, with the same phase values the
//! histograms got, so the two never disagree.

use std::time::Duration;

use hivekeep_core::resource::ActorAttribution;
use prometheus::{HistogramOpts, HistogramVec, Registry};
use tonic::Status;

use crate::labels::sandbox_class_label;
use hivekeep_core::attr;

const PHASE_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0,
];
const SIZE_BUCKETS: &[f64] = &[
    1e6, 5e6, 1e7, 2.5e7, 5e7, 1e8, 2.5e8, 5e8, 1e9, 2e9, 5e9, 1e10,
];

/// The dimensions of one checkpoint or restore.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotOp {
    /// The template's hive.
    pub template_hive: String,
    /// The template's name.
    pub template_name: String,
    /// Where the snapshot came from or goes; empty when unknown.
    pub kind: String,
    /// The fidelity label.
    pub fidelity: String,
    /// The sandbox class; empty when unknown.
    pub sandbox_class: String,
}

/// One timed phase; a zero duration means it never ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Phase(pub &'static str, pub Duration);

/// The keeper's checkpoint and restore instruments.
#[derive(Clone)]
pub struct KeeperInstruments {
    restore: HistogramVec,
    checkpoint: HistogramVec,
    snapshot_size: HistogramVec,
}

const OP_LABELS: &[&str] = &[
    "hk_template_hive",
    "hk_template_name",
    "hk_snapshot_fidelity",
    "hk_snapshot_kind",
    "hk_sandbox_class",
    "hk_snapshot_phase",
];

impl KeeperInstruments {
    /// Registers the instruments on `registry`.
    pub fn new(registry: &Registry) -> prometheus::Result<Self> {
        let restore = HistogramVec::new(
            HistogramOpts::new(
                "hivekeep_actor_restore_duration_seconds",
                "Duration of one phase of an actor restore. Phases overlap, so they are independent observations.",
            )
            .buckets(PHASE_BUCKETS.to_vec()),
            OP_LABELS,
        )?;
        let checkpoint = HistogramVec::new(
            HistogramOpts::new(
                "hivekeep_actor_checkpoint_duration_seconds",
                "Duration of one phase of an actor checkpoint. Phases overlap, so they are independent observations.",
            )
            .buckets(PHASE_BUCKETS.to_vec()),
            OP_LABELS,
        )?;
        let snapshot_size = HistogramVec::new(
            HistogramOpts::new(
                "hivekeep_keeper_snapshot_size_bytes",
                "Allocated size of each snapshot file a checkpoint wrote.",
            )
            .buckets(SIZE_BUCKETS.to_vec()),
            &["file_name", "hk_template_hive", "hk_template_name"],
        )?;
        registry.register(Box::new(restore.clone()))?;
        registry.register(Box::new(checkpoint.clone()))?;
        registry.register(Box::new(snapshot_size.clone()))?;
        Ok(KeeperInstruments {
            restore,
            checkpoint,
            snapshot_size,
        })
    }

    /// Records a restore's phases.
    pub fn record_restore(&self, op: &SnapshotOp, phases: &[Phase]) {
        record(&self.restore, op, phases);
    }

    /// Records a checkpoint's phases.
    pub fn record_checkpoint(&self, op: &SnapshotOp, phases: &[Phase]) {
        record(&self.checkpoint, op, phases);
    }

    /// Records one snapshot file's allocated size.
    pub fn record_snapshot_size(
        &self,
        file: &str,
        size: i64,
        template_hive: &str,
        template_name: &str,
    ) {
        self.snapshot_size
            .with_label_values(&[file, template_hive, template_name])
            .observe(size as f64);
    }
}

fn record(h: &HistogramVec, op: &SnapshotOp, phases: &[Phase]) {
    for p in phases.iter().filter(|p| !p.1.is_zero()) {
        h.with_label_values(&[
            op.template_hive.as_str(),
            op.template_name.as_str(),
            op.fidelity.as_str(),
            op.kind.as_str(),
            sandbox_class_label(&op.sandbox_class),
            p.0,
        ])
        .observe(p.1.as_secs_f64());
    }
}

/// Renders the phases that ran as `name=seconds` pairs.
pub fn phases_field(phases: &[Phase]) -> String {
    phases
        .iter()
        .filter(|p| !p.1.is_zero())
        .map(|p| format!("{}={:.6}", p.0, p.1.as_secs_f64()))
        .collect::<Vec<_>>()
        .join(" ")
}

/// The key prefix of a restore's phase durations in its timing record.
pub const RESTORE_DURATION_KEY: &str = "hk.actor.restore.duration";
/// The key prefix of a checkpoint's phase durations in its timing record.
pub const CHECKPOINT_DURATION_KEY: &str = "hk.actor.checkpoint.duration";

/// The fields of one operation's timing record: the actor's identity, the
/// dimensions the histograms got (a dimension not known yet is absent, not
/// empty), the gRPC code name as `error.type` when it failed, and each
/// phase that ran as `<duration_key>.<phase>` in seconds.
pub fn timing_record(
    duration_key: &str,
    who: &ActorAttribution,
    op: &SnapshotOp,
    err: Option<&Status>,
    phases: &[Phase],
) -> serde_json::Map<String, serde_json::Value> {
    use serde_json::Value;
    let mut m = serde_json::Map::new();
    let mut put = |k: &str, v: &str| {
        m.insert(k.to_string(), Value::String(v.to_string()));
    };
    put(attr::HIVE_KEY, &who.actor.hive);
    put(attr::ACTOR_NAME_KEY, &who.actor.name);
    put(attr::ACTOR_UID_KEY, &who.uid);
    put(attr::TEMPLATE_HIVE_KEY, &op.template_hive);
    put(attr::TEMPLATE_NAME_KEY, &op.template_name);
    if !op.kind.is_empty() {
        put(attr::SNAPSHOT_KIND_KEY, &op.kind);
    }
    put(attr::SNAPSHOT_FIDELITY_KEY, &op.fidelity);
    let class = sandbox_class_label(&op.sandbox_class);
    if !class.is_empty() {
        put(attr::SANDBOX_CLASS_KEY, class);
    }
    if let Some(e) = err {
        put(attr::ERROR_TYPE_KEY, crate::status::code_name(e.code()));
    }
    for p in phases.iter().filter(|p| !p.1.is_zero()) {
        if let Some(n) = serde_json::Number::from_f64(p.1.as_secs_f64()) {
            m.insert(format!("{duration_key}.{}", p.0), Value::Number(n));
        }
    }
    m
}

/// Logs one operation's timing breakdown, as [`timing_record`] lays it
/// out, under `message`.
pub fn log_timing(
    message: &str,
    duration_key: &str,
    who: &ActorAttribution,
    op: &SnapshotOp,
    err: Option<&Status>,
    phases: &[Phase],
) {
    let record = serde_json::Value::Object(timing_record(duration_key, who, op, err, phases));
    tracing::info!(timing = %record, "{message}");
}

/// Whether a leg's error is collateral: another leg's error cancelled the
/// group, so its own timing is not a real measurement.
pub fn is_collateral(group: &Status, leg: Option<&Status>) -> bool {
    leg.is_some_and(|l| l.message() != group.message() || l.code() != group.code())
}

/// The sandbox-asset duration of a prep leg cancelled by the download's
/// failure: the fetch it was cut short in measured nothing real, but one
/// that finished before the cancel (the leg failed later, in the unpack,
/// or somewhere unattributed) is kept, since absence means it never ran.
pub fn assets_after_collateral(prep_failed_phase: &str, assets: Duration) -> Duration {
    if prep_failed_phase == crate::labels::phase::SANDBOX_ASSETS {
        Duration::ZERO
    } else {
        assets
    }
}

/// The kind of snapshot a restore used: local when it ran from the node's
/// copy, else the actor's latest durable one once the manifest is read, and
/// unknown (empty) before.
pub fn restore_snapshot_kind<T>(direct_local: bool, manifest: Option<&T>) -> &'static str {
    if direct_local {
        crate::labels::kind::LOCAL
    } else if manifest.is_none() {
        ""
    } else {
        crate::labels::kind::LATEST
    }
}

/// The kind of snapshot a checkpoint writes: a deferred upload keeps the
/// node-local one, an immediate upload writes the actor's latest durable
/// snapshot.
pub fn checkpoint_snapshot_kind(
    req: &hivekeep_proto::keeper::v1::CheckpointRequest,
) -> &'static str {
    let mode = req.upload_config.as_ref().map(|c| c.mode).unwrap_or(0);
    if mode == hivekeep_proto::keeper::v1::UploadMode::Deferred as i32 {
        crate::labels::kind::LOCAL
    } else {
        crate::labels::kind::LATEST
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_only_phases_that_ran() {
        let reg = Registry::new();
        let i = KeeperInstruments::new(&reg).unwrap();
        let op = SnapshotOp {
            template_hive: "h".into(),
            template_name: "t".into(),
            kind: "local".into(),
            fidelity: "memory".into(),
            sandbox_class: "weird".into(),
        };
        i.record_restore(
            &op,
            &[
                Phase("download", Duration::ZERO),
                Phase("total", Duration::from_millis(30)),
            ],
        );
        let fams = reg.gather();
        let restore = fams
            .iter()
            .find(|f| f.name() == "hivekeep_actor_restore_duration_seconds")
            .unwrap();
        assert_eq!(restore.get_metric().len(), 1);
        let labels: Vec<_> = restore.get_metric()[0]
            .get_label()
            .iter()
            .map(|l| (l.name().to_string(), l.value().to_string()))
            .collect();
        assert!(labels.contains(&("hk_sandbox_class".into(), "unknown".into())));
        assert!(labels.contains(&("hk_snapshot_phase".into(), "total".into())));
        assert_eq!(
            phases_field(&[
                Phase("a", Duration::ZERO),
                Phase("b", Duration::from_secs(1))
            ]),
            "b=1.000000"
        );
    }

    #[test]
    fn collateral() {
        let g = Status::internal("boom");
        assert!(!is_collateral(&g, None));
        assert!(!is_collateral(&g, Some(&Status::internal("boom"))));
        assert!(is_collateral(&g, Some(&Status::cancelled("ctx"))));
    }
}

#[cfg(test)]
#[path = "keeper_metrics_tests.rs"]
pub(crate) mod behavior_tests;

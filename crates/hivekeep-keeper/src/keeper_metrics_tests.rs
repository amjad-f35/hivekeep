use std::collections::HashMap;

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use serde_json::Value;

use super::*;
use crate::assets::SandboxAssetsRecord;
use crate::context::CtxError;
use crate::labels::{fidelity, kind, phase};

const TEMPLATE_HIVE: &str = "hivekeep-agents";
const TEMPLATE_NAME: &str = "support-agent";
const RESTORE_METRIC: &str = "hivekeep_actor_restore_duration_seconds";
pub(crate) const CHECKPOINT_METRIC: &str = "hivekeep_actor_checkpoint_duration_seconds";

fn instruments() -> (KeeperInstruments, Registry) {
    let reg = Registry::new();
    (KeeperInstruments::new(&reg).unwrap(), reg)
}

pub(crate) fn family(reg: &Registry, name: &str) -> prometheus::proto::MetricFamily {
    reg.gather()
        .into_iter()
        .find(|f| f.name() == name)
        .unwrap_or_else(|| panic!("metric {name} not collected"))
}

/// Each recorded phase's labels.
fn phase_labels(f: &prometheus::proto::MetricFamily) -> HashMap<String, HashMap<String, String>> {
    f.get_metric()
        .iter()
        .map(|m| {
            let labels: HashMap<String, String> = m
                .get_label()
                .iter()
                .map(|l| (l.name().to_string(), l.value().to_string()))
                .collect();
            (labels["hk_snapshot_phase"].clone(), labels)
        })
        .collect()
}

/// Each recorded phase's sum of observations.
pub(crate) fn phase_sums(f: &prometheus::proto::MetricFamily) -> HashMap<String, f64> {
    let mut out = HashMap::new();
    for m in f.get_metric() {
        let p = m
            .get_label()
            .iter()
            .find(|l| l.name() == "hk_snapshot_phase")
            .map(|l| l.value().to_string())
            .unwrap();
        *out.entry(p).or_default() += m.get_histogram().get_sample_sum();
    }
    out
}

fn full_op(k: &str, f: &str, class: &str) -> SnapshotOp {
    SnapshotOp {
        template_hive: TEMPLATE_HIVE.into(),
        template_name: TEMPLATE_NAME.into(),
        kind: k.into(),
        fidelity: f.into(),
        sandbox_class: class.into(),
    }
}

#[test]
fn restore_duration_shape() {
    let (i, reg) = instruments();
    i.record_restore(
        &full_op(kind::LATEST, fidelity::VOLUMES, "gvisor"),
        &[
            Phase(phase::DOWNLOAD, Duration::from_secs(2)),
            Phase(phase::TOTAL, Duration::from_secs(3)),
        ],
    );
    let f = family(&reg, RESTORE_METRIC);
    assert!(f.name().ends_with("_seconds"), "unit is not seconds");
    assert!(!f.help().is_empty(), "description is empty");
    let by_phase = phase_labels(&f);
    assert_eq!(by_phase.len(), 2, "want download and total");
    let got = &by_phase[phase::DOWNLOAD];
    for (k, want) in [
        ("hk_template_hive", TEMPLATE_HIVE),
        ("hk_template_name", TEMPLATE_NAME),
        ("hk_snapshot_kind", kind::LATEST),
        ("hk_snapshot_fidelity", fidelity::VOLUMES),
        ("hk_sandbox_class", "gvisor"),
    ] {
        assert_eq!(got[k], want, "{k}");
    }
    assert!(
        !got.contains_key("error_type"),
        "error type on a successful restore"
    );
    assert!(
        !got.keys().any(|k| k.starts_with("actor")),
        "actor identity must never reach a metric label: {got:?}"
    );
}

#[test]
fn checkpoint_duration_shape() {
    let (i, reg) = instruments();
    i.record_checkpoint(
        &full_op(kind::LOCAL, fidelity::MEMORY, "microvm"),
        &[
            Phase(phase::PERSIST, Duration::from_secs(1)),
            Phase(phase::CLEANUP, Duration::from_millis(20)),
        ],
    );
    let f = family(&reg, CHECKPOINT_METRIC);
    assert!(f.name().ends_with("_seconds"));
    let by_phase = phase_labels(&f);
    for p in [phase::PERSIST, phase::CLEANUP] {
        let set = by_phase
            .get(p)
            .unwrap_or_else(|| panic!("missing phase {p}"));
        assert_eq!(set["hk_snapshot_kind"], kind::LOCAL, "{p}");
        assert_eq!(set["hk_sandbox_class"], "microvm", "{p}");
    }
}

/// A phase that never started stays absent instead of landing in the
/// percentiles as instantaneous.
#[test]
fn record_phases_skips_zero_phases() {
    let (i, reg) = instruments();
    i.record_restore(
        &SnapshotOp {
            fidelity: fidelity::MEMORY.into(),
            ..Default::default()
        },
        &[
            Phase(phase::MANIFEST_FETCH, Duration::from_millis(50)),
            Phase(phase::DOWNLOAD, Duration::from_secs(2)),
            Phase(phase::WORKER_RESTORE, Duration::ZERO),
            Phase(phase::TOTAL, Duration::from_secs(2)),
        ],
    );
    let by_phase = phase_labels(&family(&reg, RESTORE_METRIC));
    assert!(!by_phase.contains_key(phase::WORKER_RESTORE));
    assert_eq!(by_phase.len(), 3);
}

/// A restore failing before the manifest resolves knows neither kind nor
/// sandbox class; those labels stay empty, which Prometheus treats as
/// absent, rather than taking a made-up value.
#[test]
fn snapshot_op_omits_unknown_dimensions() {
    let (i, reg) = instruments();
    i.record_restore(
        &SnapshotOp {
            template_hive: TEMPLATE_HIVE.into(),
            template_name: TEMPLATE_NAME.into(),
            fidelity: fidelity::MEMORY.into(),
            ..Default::default()
        },
        &[Phase(phase::TOTAL, Duration::from_secs(1))],
    );
    let labels = &phase_labels(&family(&reg, RESTORE_METRIC))[phase::TOTAL];
    assert_eq!(labels["hk_snapshot_kind"], "");
    assert_eq!(labels["hk_sandbox_class"], "");
}

/// An unvalidated manifest value cannot become an unbounded label.
#[test]
fn snapshot_op_normalizes_sandbox_class() {
    let (i, reg) = instruments();
    i.record_restore(
        &SnapshotOp {
            sandbox_class: "definitely-not-a-runtime".into(),
            ..Default::default()
        },
        &[Phase(phase::TOTAL, Duration::from_secs(1))],
    );
    let labels = &phase_labels(&family(&reg, RESTORE_METRIC))[phase::TOTAL];
    assert_eq!(labels["hk_sandbox_class"], "unknown");
}

/// A leg cancelled by the other leg's failure measured a partial duration,
/// which would drag the healthy-path percentiles down.
#[test]
fn is_collateral_cases() {
    let owner = Status::internal("prepare bundles: no entrypoint");
    let cancelled = Status::cancelled("context canceled");
    for (name, leg, want) in [
        (
            "leg that owns the error keeps its duration",
            Some(&owner),
            false,
        ),
        (
            "leg cancelled as collateral drops its duration",
            Some(&cancelled),
            true,
        ),
        ("leg that succeeded keeps its duration", None, false),
    ] {
        assert_eq!(is_collateral(&owner, leg), want, "{name}");
    }
}

/// A prep leg cancelled during the unpack had already fetched its assets;
/// zeroing that would report a step that completed as one that never ran.
#[test]
fn assets_after_collateral_cases() {
    let assets = Duration::from_millis(1200);
    for (name, failed_phase, want) in [
        (
            "cancelled during the asset fetch truncates it",
            phase::SANDBOX_ASSETS,
            Duration::ZERO,
        ),
        (
            "cancelled during the unpack keeps the completed asset fetch",
            phase::OCI_UNPACK,
            assets,
        ),
        (
            "an unattributed prep failure keeps it rather than guessing",
            "",
            assets,
        ),
    ] {
        assert_eq!(
            assets_after_collateral(failed_phase, assets),
            want,
            "{name}"
        );
    }
}

#[test]
fn restore_snapshot_kind_cases() {
    let rec = |hive: &str| SandboxAssetsRecord {
        hive: hive.into(),
        ..Default::default()
    };
    let (a, b, none) = (rec("team-a"), rec("team-b"), rec(""));
    for (name, direct_local, r, want) in [
        ("restored from the local copy", true, Some(&a), kind::LOCAL),
        (
            "a snapshot from another hive served locally is still local",
            true,
            Some(&b),
            kind::LOCAL,
        ),
        (
            "external snapshot written in another hive",
            false,
            Some(&b),
            kind::LATEST,
        ),
        (
            "external snapshot written by a tenant actor",
            false,
            Some(&a),
            kind::LATEST,
        ),
        (
            "manifest predating the identity fields",
            false,
            Some(&none),
            kind::LATEST,
        ),
        (
            "external kind is unknowable until the manifest is read",
            false,
            None,
            "",
        ),
    ] {
        assert_eq!(restore_snapshot_kind(direct_local, r), want, "{name}");
    }
}

#[test]
fn checkpoint_snapshot_kind_cases() {
    let req = |mode: pb::UploadMode, hive: &str| pb::CheckpointRequest {
        upload_config: Some(pb::UploadConfig { mode: mode as i32 }),
        hive: hive.into(),
        ..Default::default()
    };
    for (name, r, want) in [
        (
            "pause writes the node-local snapshot",
            req(pb::UploadMode::Deferred, "team-a"),
            kind::LOCAL,
        ),
        (
            "suspend writes the actor's durable snapshot",
            req(pb::UploadMode::Immediate, "team-a"),
            kind::LATEST,
        ),
        (
            "a durable checkpoint in another hive is latest",
            req(pb::UploadMode::Immediate, "team-b"),
            kind::LATEST,
        ),
        (
            "a local checkpoint in another hive is still local",
            req(pb::UploadMode::Deferred, "team-b"),
            kind::LOCAL,
        ),
    ] {
        assert_eq!(checkpoint_snapshot_kind(&r), want, "{name}");
    }
}

fn attribution() -> ActorAttribution {
    ActorAttribution {
        actor: ActorRef::new("team-a", "support-agent-42"),
        uid: "uid-abc".into(),
        template_hive: TEMPLATE_HIVE.into(),
        template_name: TEMPLATE_NAME.into(),
    }
}

/// The record as a collector parses it: rendered to JSON and read back.
fn render(
    op: &SnapshotOp,
    err: Option<&Status>,
    phases: &[Phase],
) -> serde_json::Map<String, Value> {
    let rec = timing_record(RESTORE_DURATION_KEY, &attribution(), op, err, phases);
    let text = serde_json::to_string(&rec).unwrap();
    serde_json::from_str(&text).unwrap()
}

/// The duration keys are named after an instrument in seconds, so the
/// values are seconds too, not nanoseconds.
#[test]
fn timing_record_seconds() {
    let rec = render(
        &full_op(kind::LATEST, fidelity::MEMORY, "gvisor"),
        None,
        &[
            Phase(phase::DOWNLOAD, Duration::from_millis(2500)),
            Phase(phase::TOTAL, Duration::from_secs(3)),
        ],
    );
    assert_eq!(
        rec["hk.actor.restore.duration.download"].as_f64(),
        Some(2.5)
    );
    assert_eq!(rec["hk.actor.restore.duration.total"].as_f64(), Some(3.0));
}

#[test]
fn timing_record_cases() {
    struct Case {
        name: &'static str,
        op: SnapshotOp,
        err: Option<Status>,
        phases: Vec<Phase>,
        strings: Vec<(&'static str, &'static str)>,
        numbers: Vec<(&'static str, f64)>,
        absent: Vec<&'static str>,
    }
    let full = full_op(kind::LATEST, fidelity::MEMORY, "gvisor");
    let ms = Duration::from_millis;
    let cases = vec![
        Case {
            name: "a successful restore carries identity, the metric dimensions and every phase that ran",
            op: full.clone(),
            err: None,
            phases: vec![
                Phase(phase::VOLUME_MOUNT, ms(4)),
                Phase(phase::MANIFEST_FETCH, ms(21)),
                Phase(phase::SANDBOX_ASSETS, ms(10)),
                Phase(phase::DOWNLOAD, ms(310)),
                Phase(phase::OCI_UNPACK, ms(50)),
                Phase(phase::WORKER_RESTORE, ms(60)),
                Phase(phase::TOTAL, ms(420)),
            ],
            strings: vec![
                ("hk.hive", "team-a"),
                ("hk.actor.name", "support-agent-42"),
                ("hk.actor.uid", "uid-abc"),
                ("hk.template.hive", TEMPLATE_HIVE),
                ("hk.template.name", TEMPLATE_NAME),
                ("hk.snapshot.kind", kind::LATEST),
                ("hk.snapshot.fidelity", fidelity::MEMORY),
                ("hk.sandbox.class", "gvisor"),
            ],
            numbers: vec![
                ("hk.actor.restore.duration.volume_mount", 0.004),
                ("hk.actor.restore.duration.manifest_fetch", 0.021),
                ("hk.actor.restore.duration.download", 0.310),
                ("hk.actor.restore.duration.total", 0.420),
            ],
            // The phase key names the one step a datapoint timed; this
            // record has them all. Absent error.type is success.
            absent: vec!["hk.snapshot.phase", "actor", "total", "download", "error.type"],
        },
        Case {
            name: "a failed restore keeps the phases it completed and is marked with the gRPC code",
            op: full.clone(),
            err: Some(Status::deadline_exceeded("download timed out")),
            phases: vec![
                Phase(phase::DOWNLOAD, Duration::from_secs(30)),
                Phase(phase::TOTAL, Duration::from_secs(30)),
            ],
            strings: vec![("error.type", "DeadlineExceeded")],
            numbers: vec![("hk.actor.restore.duration.download", 30.0)],
            absent: vec![],
        },
        Case {
            name: "a plain error is a bounded Unknown, not its message",
            op: full.clone(),
            err: Some(Status::unknown("disk on fire at /var/lib/hivekeep")),
            phases: vec![Phase(phase::TOTAL, Duration::from_secs(1))],
            strings: vec![("error.type", "Unknown")],
            numbers: vec![],
            absent: vec![],
        },
        Case {
            // The keeper's own transfers fail with context errors; a
            // timeout must not read as Unknown.
            name: "a wrapped context deadline reports DeadlineExceeded",
            op: full.clone(),
            err: Some(crate::status::wrap(
                "while downloading memory-ranges",
                CtxError::DeadlineExceeded.into(),
            )),
            phases: vec![Phase(phase::TOTAL, Duration::from_secs(30))],
            strings: vec![("error.type", "DeadlineExceeded")],
            numbers: vec![],
            absent: vec![],
        },
        Case {
            name: "a wrapped context cancellation reports Canceled",
            op: full.clone(),
            err: Some(crate::status::wrap("while restoring", CtxError::Canceled.into())),
            phases: vec![Phase(phase::TOTAL, Duration::from_secs(1))],
            strings: vec![("error.type", "Canceled")],
            numbers: vec![],
            absent: vec![],
        },
        Case {
            name: "a phase that never ran is absent, not zero",
            op: full.clone(),
            err: None,
            phases: vec![
                Phase(phase::DOWNLOAD, Duration::ZERO),
                Phase(phase::TOTAL, Duration::from_secs(1)),
            ],
            strings: vec![],
            numbers: vec![("hk.actor.restore.duration.total", 1.0)],
            absent: vec!["hk.actor.restore.duration.download"],
        },
        Case {
            name: "a restore that died before the manifest omits the dimensions it never learned",
            op: SnapshotOp {
                template_hive: TEMPLATE_HIVE.into(),
                template_name: TEMPLATE_NAME.into(),
                fidelity: fidelity::MEMORY.into(),
                ..Default::default()
            },
            err: None,
            phases: vec![Phase(phase::TOTAL, ms(700))],
            strings: vec![("hk.actor.uid", "uid-abc")],
            numbers: vec![("hk.actor.restore.duration.total", 0.7)],
            absent: vec!["hk.snapshot.kind", "hk.sandbox.class"],
        },
        Case {
            name: "a sandbox class the manifest invented is bounded, not passed through",
            op: SnapshotOp {
                template_hive: TEMPLATE_HIVE.into(),
                template_name: TEMPLATE_NAME.into(),
                fidelity: fidelity::MEMORY.into(),
                sandbox_class: "../../etc/passwd".into(),
                ..Default::default()
            },
            err: None,
            phases: vec![Phase(phase::TOTAL, Duration::from_secs(1))],
            strings: vec![("hk.sandbox.class", "unknown")],
            numbers: vec![],
            absent: vec![],
        },
    ];
    for c in cases {
        let rec = render(&c.op, c.err.as_ref(), &c.phases);
        for (k, want) in &c.strings {
            assert_eq!(
                rec.get(*k).and_then(Value::as_str),
                Some(*want),
                "{}: {k}",
                c.name
            );
        }
        for (k, want) in &c.numbers {
            let got = rec.get(*k).and_then(Value::as_f64);
            assert!(
                got.is_some_and(|g| (g - want).abs() < 1e-9),
                "{}: {k} = {got:?}, want {want}",
                c.name
            );
        }
        for k in &c.absent {
            assert!(!rec.contains_key(*k), "{}: {k} present", c.name);
        }
    }
}

//! The tracker's histograms and the undurable actors gauges, read back from
//! a Prometheus registry.

use std::collections::BTreeMap;
use std::fs;

use hivekeep_proto::keeper::v1 as pb;
use prometheus::proto::{MetricFamily, MetricType};
use prometheus::Registry;
use tonic::Status;

use super::actor::{ActorRecord, ActorSnapshots};
use super::metrics::error_type;
use super::op::OpKind;
use super::test_support_core::*;
use super::*;
use crate::context::Ctx;

const UPLOAD_DURATION: &str = "hivekeep_keeper_snapshot_upload_duration_seconds";
const DETACH_DURATION: &str = "hivekeep_keeper_snapshot_detach_duration_seconds";
const UPLOAD_OVERDUE: &str = "hivekeep_keeper_snapshot_upload_overdue_duration_seconds";
const DETACH_OVERDUE: &str = "hivekeep_keeper_snapshot_detach_overdue_duration_seconds";
const OVERDUE_ACTORS: &str = "hivekeep_keeper_snapshot_upload_overdue_actors";
const UNDURABLE_AGE: &str = "hivekeep_keeper_snapshot_undurable_age_max_seconds";

fn instruments() -> (Instruments, Registry) {
    let reg = Registry::new();
    (Instruments::new(&reg).unwrap(), reg)
}

fn family(reg: &Registry, name: &str) -> Option<MetricFamily> {
    reg.gather().into_iter().find(|f| f.name() == name)
}

/// One histogram series: its labels, count and sum.
#[derive(Debug)]
struct Series {
    labels: BTreeMap<String, String>,
    count: u64,
    sum: f64,
}

/// The series of the histogram `name` that recorded anything; `None` when
/// none did.
fn histogram(reg: &Registry, name: &str) -> Option<Vec<Series>> {
    let f = family(reg, name)?;
    assert_eq!(f.get_field_type(), MetricType::HISTOGRAM, "{name}");
    assert!(name.ends_with("_seconds"), "{name} is in seconds");
    assert!(!f.help().is_empty(), "{name} has a description");
    let out: Vec<Series> = f
        .get_metric()
        .iter()
        .map(|m| {
            let labels: BTreeMap<String, String> = m
                .get_label()
                .iter()
                .map(|l| (l.name().to_string(), l.value().to_string()))
                .collect();
            assert!(
                !labels
                    .keys()
                    .any(|k| k.contains("actor") || k.contains("uid")),
                "{name} carries actor identity: {labels:?}"
            );
            let h = m.get_histogram();
            Series {
                labels,
                count: h.get_sample_count(),
                sum: h.get_sample_sum(),
            }
        })
        .filter(|s| s.count > 0)
        .collect();
    (!out.is_empty()).then_some(out)
}

/// The error type of each recorded operation, "" for a success.
fn error_types(reg: &Registry, name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for s in histogram(reg, name).unwrap_or_default() {
        for _ in 0..s.count {
            out.push(s.labels.get("error_type").cloned().unwrap_or_default());
        }
    }
    out
}

fn one_series(reg: &Registry, name: &str) -> Series {
    let mut h = histogram(reg, name).unwrap_or_else(|| panic!("{name} not collected"));
    assert_eq!(h.len(), 1, "{name}: {h:?}");
    h.pop().unwrap()
}

/// What the undurable actors gauges report.
#[derive(Debug, Default, PartialEq)]
struct Undurable {
    under1m: i64,
    m1to5: i64,
    m5to15: i64,
    over15: i64,
    age: f64,
}

fn read_undurable(reg: &Registry) -> Undurable {
    let mut got = Undurable::default();
    let overdue = family(reg, OVERDUE_ACTORS).expect("overdue actors collected");
    assert_eq!(overdue.get_field_type(), MetricType::GAUGE);
    assert_eq!(overdue.get_metric().len(), 4, "one series per band");
    for m in overdue.get_metric() {
        assert_eq!(m.get_label().len(), 1);
        let l = &m.get_label()[0];
        assert_eq!(l.name(), "hk_snapshot_upload_overdue_band");
        let v = m.get_gauge().value() as i64;
        match l.value() {
            "under_1m" => got.under1m = v,
            "1m_to_5m" => got.m1to5 = v,
            "5m_to_15m" => got.m5to15 = v,
            "over_15m" => got.over15 = v,
            b => panic!("unknown band {b}"),
        }
    }
    let age = family(reg, UNDURABLE_AGE).expect("undurable age collected");
    assert_eq!(age.get_field_type(), MetricType::GAUGE);
    assert_eq!(age.get_metric().len(), 1);
    assert!(age.get_metric()[0].get_label().is_empty());
    got.age = age.get_metric()[0].get_gauge().value();
    got
}

fn age(secs: u64) -> Undurable {
    Undurable {
        age: secs as f64,
        ..Default::default()
    }
}

fn with_instruments(inst: &Instruments) -> impl FnOnce(&mut Options) + '_ {
    move |o| o.instruments = Some(inst.clone())
}

/// Waits until a background detach of the tracked actor has ended and
/// recorded how: no operation, and the local snapshot gone or a retry set.
async fn wait_detach_ended(t: &Tracker) {
    eventually("the detach ends", async || {
        let mut g = t.inner.lock(UID).await;
        t.inner.find(&mut g)
            && g.a_ref().op.is_none()
            && (g.a_ref().rec.latest_local.is_none() || g.a_ref().retry_at.is_some())
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn upload_duration_shape() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let t = n.store_tracker(&store, with_instruments(&inst));
    t.start().await;
    let mut r = test_ref("s1", 1, Some(now(&t)));
    r.snapshot.fidelity = pb::SnapshotFidelity::Volumes as i32;
    t.commit_local(UID, r).await;
    let op = start_upload(&t, &store).await;
    step_to(&t, at(now(&t), 2 * SECOND)).await;
    store.release(None).await;
    wait_op(&op, "the upload").await;

    let s = one_series(&reg, UPLOAD_DURATION);
    assert_eq!((s.count, s.sum), (1, 2.0));
    assert_eq!(s.labels["hk_snapshot_fidelity"], "volumes");
    assert_eq!(s.labels["error_type"], "", "no error type on a success");
    assert_eq!(s.labels.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn upload_duration_failure_and_unknown_fidelity() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let t = n.store_tracker(&store, with_instruments(&inst));
    t.start().await;
    let mut r = test_ref("s1", 1, Some(now(&t)));
    r.snapshot.fidelity = pb::SnapshotFidelity::Unspecified as i32;
    t.commit_local(UID, r).await;
    upload_now(&t, &store, Some(Status::unavailable("object store down"))).await;

    let s = one_series(&reg, UPLOAD_DURATION);
    assert_eq!(s.count, 1);
    assert_eq!(s.labels["hk_snapshot_fidelity"], "unknown");
    assert_eq!(s.labels["error_type"], "Unavailable");
}

/// Background detaches that upload record an upload; foreground detaches,
/// already-durable detaches, and probes finding the manifest committed do
/// not.
#[tokio::test(start_paused = true)]
async fn upload_duration_background_detach_that_uploads_records() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let t = n.store_tracker(&RecStore::new(), |o| {
        o.upload_workers = 0;
        o.instruments = Some(inst.clone());
    });
    t.start().await;
    t.commit_local(UID, test_ref("s1", 1, Some(now(&t)))).await;
    detach_now(&t).await;
    let total: u64 = histogram(&reg, UPLOAD_DURATION)
        .expect("recorded")
        .iter()
        .map(|s| s.count)
        .sum();
    assert_eq!(total, 1);
}

#[tokio::test(start_paused = true)]
async fn upload_duration_foreground_detach_does_not_record() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let t = n.store_tracker(&RecStore::new(), with_instruments(&inst));
    t.start().await;
    t.commit_local(UID, test_ref("s1", 1, Some(now(&t)))).await;
    t.detach(&Ctx::background(), UID).await.unwrap();
    assert!(histogram(&reg, UPLOAD_DURATION).is_none());
}

#[tokio::test(start_paused = true)]
async fn upload_duration_already_durable_detach_does_not_record() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let t = n.store_tracker(&RecStore::new(), with_instruments(&inst));
    t.start().await;
    let r = test_ref("s1", 1, Some(now(&t)));
    t.commit_durable(UID, r.clone()).await;
    t.commit_local(UID, r).await;
    detach_now(&t).await;
    assert!(histogram(&reg, UPLOAD_DURATION).is_none());
}

#[tokio::test(start_paused = true)]
async fn upload_duration_probe_finding_manifest_does_not_record() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let t = n.store_tracker(&store, with_instruments(&inst));
    t.start().await;
    let r = test_ref("s1", 1, Some(now(&t)));
    store.mark_uploaded(r.uri());
    t.commit_local(UID, r).await;
    upload_now(&t, &store, None).await;
    assert!(histogram(&reg, UPLOAD_DURATION).is_none());
}

/// A failure's error type is the context's reason once it has ended, and the
/// status code otherwise, spelled as gRPC names codes.
#[tokio::test(start_paused = true)]
async fn error_type_names() {
    let canceled = Ctx::background();
    canceled.cancel();
    let expired = Ctx::background().with_timeout(std::time::Duration::ZERO);
    let live = Ctx::background();
    for (name, ctx, err, want) in [
        (
            "canceled",
            &canceled,
            Status::cancelled("while uploading snapshot files: context canceled"),
            "Canceled",
        ),
        (
            "deadline exceeded",
            &expired,
            Status::deadline_exceeded("deadline exceeded"),
            "DeadlineExceeded",
        ),
        (
            "status error",
            &live,
            Status::unavailable("plugin: unreachable"),
            "Unavailable",
        ),
        (
            "plugin status",
            &live,
            Status::permission_denied("while uploading snapshot files: denied"),
            "PermissionDenied",
        ),
        ("plain error", &live, Status::internal("boom"), "Internal"),
        // A canceled context names its own reason whatever the plugin said.
        (
            "canceled with another code",
            &canceled,
            Status::unavailable("gone"),
            "Canceled",
        ),
    ] {
        assert_eq!(error_type(ctx, Some(&err)), want, "{name}");
    }
    assert_eq!(error_type(&live, None), "");
}

/// A background detach records its local cleanup labelled by fidelity,
/// without the time its upload took.
#[tokio::test(start_paused = true)]
async fn detach_duration_shape() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let t = n.store_tracker(&store, |o| {
        o.upload_workers = 0;
        o.instruments = Some(inst.clone());
    });
    t.start().await;
    let mut r = test_ref("s1", 1, Some(now(&t)));
    r.snapshot.fidelity = pb::SnapshotFidelity::Volumes as i32;
    t.commit_local(UID, r).await;
    step_to(&t, detach_due_of(&t, UID).await.unwrap()).await;
    store.wait_started().await;
    let op = op_of(&t, UID).await;
    step_to(&t, at(now(&t), 3 * SECOND)).await;
    store.release(None).await;
    wait_op(&op, "the detach").await;

    let up = one_series(&reg, UPLOAD_DURATION);
    assert_eq!((up.count, up.sum), (1, 3.0));
    let d = one_series(&reg, DETACH_DURATION);
    assert_eq!((d.count, d.sum), (1, 0.0), "upload time excluded");
    assert_eq!(d.labels["hk_snapshot_fidelity"], "volumes");
    assert_eq!(d.labels["error_type"], "");
}

fn detach_tracker(n: &Node, store: &std::sync::Arc<RecStore>, inst: &Instruments) -> Tracker {
    n.store_tracker(store, |o| {
        o.upload_workers = 0;
        o.instruments = Some(inst.clone());
    })
}

#[tokio::test(start_paused = true)]
async fn detach_duration_failed() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let t = detach_tracker(&n, &RecStore::new(), &inst);
    t.start().await;
    let r = test_ref("s1", 1, Some(now(&t)));
    t.commit_durable(UID, r.clone()).await;
    t.commit_local(UID, r).await;
    // A populated volume mount point fails the cleanup.
    let still_mounted = n
        .paths
        .external_volume_host_path(UID, "data")
        .join("payload");
    fs::create_dir_all(still_mounted.parent().unwrap()).unwrap();
    fs::write(&still_mounted, b"").unwrap();
    step_to(&t, detach_due_of(&t, UID).await.unwrap()).await;
    wait_detach_ended(&t).await;
    assert_eq!(error_types(&reg, DETACH_DURATION), vec!["Internal"]);
}

/// A detach whose context ends as its upload finishes records Canceled.
#[tokio::test(start_paused = true)]
async fn detach_duration_canceled() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let t = n.store_tracker(&store, with_instruments(&inst));
    t.commit_local(UID, test_ref("s1", 1, Some(now(&t)))).await;
    step_to(&t, detach_due_of(&t, UID).await.unwrap()).await;
    let run = t
        .inner
        .begin_due_work(UID, OpKind::Detach)
        .await
        .expect("the due detach begins");
    let runner = tokio::spawn(run);
    store.wait_started().await;
    // Forget cancels the detach just as its upload succeeds.
    let op = op_of(&t, UID).await;
    store.on_success(move || op.cancel());
    store.release(None).await;
    returns_promptly("Forget", t.forget(UID)).await.unwrap();
    runner.await.unwrap();
    assert_eq!(error_types(&reg, DETACH_DURATION), vec!["Canceled"]);
}

#[tokio::test(start_paused = true)]
async fn detach_duration_upload_failure_does_not_record() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let t = detach_tracker(&n, &store, &inst);
    t.start().await;
    t.commit_local(UID, test_ref("s1", 1, Some(now(&t)))).await;
    step_to(&t, detach_due_of(&t, UID).await.unwrap()).await;
    store.wait_started().await;
    let op = op_of(&t, UID).await;
    store
        .release(Some(Status::unavailable("object store down")))
        .await;
    wait_op(&op, "the detach").await;
    assert_eq!(error_types(&reg, UPLOAD_DURATION), vec!["Unavailable"]);
    assert!(histogram(&reg, DETACH_DURATION).is_none());
}

#[tokio::test(start_paused = true)]
async fn detach_duration_already_durable() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let t = detach_tracker(&n, &RecStore::new(), &inst);
    t.start().await;
    let r = test_ref("s1", 1, Some(now(&t)));
    t.commit_durable(UID, r.clone()).await;
    t.commit_local(UID, r).await;
    detach_now(&t).await;
    assert_eq!(error_types(&reg, DETACH_DURATION), vec![""]);
    assert!(histogram(&reg, UPLOAD_DURATION).is_none());
}

#[tokio::test(start_paused = true)]
async fn detach_duration_foreground_does_not_record() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let t = detach_tracker(&n, &RecStore::new(), &inst);
    t.start().await;
    t.commit_local(UID, test_ref("s1", 1, Some(now(&t)))).await;
    t.detach(&Ctx::background(), UID).await.unwrap();
    assert!(histogram(&reg, DETACH_DURATION).is_none());
}

/// Each background upload and detach records how long after it came due a
/// worker started it.
#[tokio::test(start_paused = true)]
async fn overdue_duration() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let t = n.store_tracker(&store, with_instruments(&inst));
    t.start().await;
    t.commit_local(UID, test_ref("s1", 1, Some(now(&t)))).await;
    let due = time_of(&t.state(UID).await.upload_due_time).unwrap();
    step_to(&t, at(due, 5 * SECOND)).await;
    store.wait_started().await;
    let op = op_of(&t, UID).await;
    store.release(None).await;
    wait_op(&op, "the upload").await;

    // Durable now, so the detach only deletes.
    let due = detach_due_of(&t, UID).await.unwrap();
    step_to(&t, at(due, 7 * SECOND)).await;
    wait_detach_ended(&t).await;

    for (name, want) in [(UPLOAD_OVERDUE, 5.0), (DETACH_OVERDUE, 7.0)] {
        let s = one_series(&reg, name);
        assert!(s.labels.is_empty(), "{name}: {:?}", s.labels);
        assert_eq!((s.count, s.sum), (1, want), "{name}");
    }
}

/// The overdue bands and the oldest undurable age follow checkpoints and an
/// upload.
#[tokio::test(start_paused = true)]
async fn undurable_actors_instruments() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let t = n.store_tracker(&store, with_instruments(&inst));
    t.start().await;
    let start = now(&t);
    assert_eq!(read_undurable(&reg), Undurable::default());

    // A newer checkpoint before the upload keeps the age running from the
    // first.
    t.commit_local(UID, test_ref("s1", 1, Some(start))).await;
    step_to(&t, at(start, 10 * MINUTE)).await;
    t.commit_local(UID, test_ref("s2", 2, Some(now(&t)))).await;
    assert_eq!(read_undurable(&reg), age(600));

    step_to(&t, at(start, UPLOAD_DELAY + 30 * SECOND)).await;
    store.wait_started().await;
    let h = UPLOAD_DELAY.as_secs();
    assert_eq!(
        read_undurable(&reg),
        Undurable {
            under1m: 1,
            ..age(h + 30)
        }
    );
    step_to(&t, at(start, UPLOAD_DELAY + 5 * MINUTE)).await;
    assert_eq!(
        read_undurable(&reg),
        Undurable {
            m1to5: 1,
            ..age(h + 300)
        }
    );
    step_to(&t, at(start, UPLOAD_DELAY + 10 * MINUTE)).await;
    assert_eq!(
        read_undurable(&reg),
        Undurable {
            m5to15: 1,
            ..age(h + 600)
        }
    );
    step_to(&t, at(start, UPLOAD_DELAY + 20 * MINUTE)).await;
    assert_eq!(
        read_undurable(&reg),
        Undurable {
            over15: 1,
            ..age(h + 1200)
        }
    );

    let op = op_of(&t, UID).await;
    store.release(None).await;
    wait_op(&op, "the upload").await;
    assert_eq!(read_undurable(&reg), Undurable::default());
}

/// A snapshot with no object storage location still ages, but is never
/// counted overdue: no upload can run for it.
#[tokio::test(start_paused = true)]
async fn undurable_actors_instruments_unuploadable() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let t = n.store_tracker(&RecStore::new(), with_instruments(&inst));
    t.start().await;
    let start = now(&t);
    let mut r = test_ref("s1", 1, Some(start));
    r.snapshot.object = None;
    t.commit_local(UID, r).await;
    step_to(&t, at(start, UPLOAD_DELAY + 10 * MINUTE)).await;
    assert_eq!(read_undurable(&reg), age(UPLOAD_DELAY.as_secs() + 600));
}

/// Forget drops the actor from the gauges, as does a save finding its
/// directory gone.
#[tokio::test(start_paused = true)]
async fn undurable_actors_instruments_drop_actor_on_forget() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let t = n.store_tracker(&RecStore::new(), with_instruments(&inst));
    t.commit_local(UID, test_ref("s1", 1, Some(now(&t)))).await;
    step_to(&t, at(now(&t), MINUTE)).await;
    assert_eq!(read_undurable(&reg), age(60));
    t.forget(UID).await.unwrap();
    assert_eq!(read_undurable(&reg), Undurable::default());
}

#[tokio::test(start_paused = true)]
async fn undurable_actors_instruments_drop_actor_without_directory() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let t = n.store_tracker(&RecStore::new(), with_instruments(&inst));
    t.commit_local(UID, test_ref("s1", 1, Some(now(&t)))).await;
    step_to(&t, at(now(&t), MINUTE)).await;
    assert_eq!(read_undurable(&reg), age(60));
    fs::remove_dir_all(n.paths.actor_dir(UID)).unwrap();
    t.commit_local(UID, test_ref("s2", 2, Some(now(&t)))).await;
    assert_eq!(read_undurable(&reg), Undurable::default());
}

/// After a restart, Start counts the undurable snapshots the state files
/// record before any worker reads them.
#[tokio::test(start_paused = true)]
async fn undurable_actors_instruments_after_restart() {
    let n = Node::new();
    let store = RecStore::new();
    let b = n.store_tracker(&store, |_| {});
    b.commit_local(UID, test_ref("s1", 1, Some(now(&b)))).await;

    let (inst, reg) = instruments();
    let a = n.store_tracker(&store, with_instruments(&inst));
    step_to(&a, at(now(&a), MINUTE)).await;
    assert_eq!(read_undurable(&reg), Undurable::default(), "before Start");
    a.start().await;
    assert_eq!(read_undurable(&reg), age(60));
}

/// Without UndurableSince the age counts from Uploading's commit before
/// LatestLocal's, as the upload due time does.
#[tokio::test(start_paused = true)]
async fn undurable_actors_instruments_zero_undurable_since() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let t = n.store_tracker(&RecStore::new(), with_instruments(&inst));
    let start = now(&t);
    step_to(&t, at(start, 2 * MINUTE)).await;
    let a = ActorSnapshots::new(
        1,
        UID,
        ActorRecord {
            uploading: Some(test_ref("s1", 1, Some(start))),
            latest_local: Some(test_ref("s2", 2, Some(at(start, 30 * SECOND)))),
            ..Default::default()
        },
    );
    t.inner.tally.as_ref().unwrap().observe(&a);
    assert_eq!(read_undurable(&reg), age(120));
}

/// Several undurable actors sum by band, and the age is the oldest's, then
/// the next oldest's once it leaves.
#[tokio::test(start_paused = true)]
async fn undurable_actors_instruments_multiple_actors() {
    const SECOND_UID: &str = "actor-uid-2";
    let (inst, reg) = instruments();
    let n = Node::new();
    n.actor_dir(SECOND_UID);
    let t = n.store_tracker(&RecStore::new(), with_instruments(&inst));
    let start = now(&t);
    let h = UPLOAD_DELAY.as_secs();

    t.commit_local(UID, test_ref("s1", 1, Some(start))).await;
    step_to(&t, at(start, 30 * SECOND)).await;
    t.commit_local(SECOND_UID, test_ref("s2", 1, Some(now(&t))))
        .await;

    step_to(&t, at(start, UPLOAD_DELAY + 45 * SECOND)).await;
    assert_eq!(
        read_undurable(&reg),
        Undurable {
            under1m: 2,
            ..age(h + 45)
        }
    );
    step_to(&t, at(start, UPLOAD_DELAY + MINUTE + 15 * SECOND)).await;
    assert_eq!(
        read_undurable(&reg),
        Undurable {
            under1m: 1,
            m1to5: 1,
            ..age(h + 75)
        }
    );
    t.forget(UID).await.unwrap();
    assert_eq!(
        read_undurable(&reg),
        Undurable {
            under1m: 1,
            ..age(h + 45)
        }
    );
}

/// A timed-out background upload records DeadlineExceeded and the timeout.
#[tokio::test(start_paused = true)]
async fn upload_duration_timeout_records_deadline_exceeded() {
    let (inst, reg) = instruments();
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let t = n.store_tracker(&store, with_instruments(&inst));
    t.start().await;
    t.commit_local(UID, test_ref("s1", 1, Some(now(&t)))).await;
    let op = start_upload(&t, &store).await;
    step_to(&t, at(now(&t), UPLOAD_TIMEOUT)).await;
    wait_op(&op, "the timed-out upload").await;
    assert_eq!(error_types(&reg, UPLOAD_DURATION), vec!["DeadlineExceeded"]);
    let s = one_series(&reg, UPLOAD_DURATION);
    assert_eq!(s.sum, UPLOAD_TIMEOUT.as_secs_f64());
}

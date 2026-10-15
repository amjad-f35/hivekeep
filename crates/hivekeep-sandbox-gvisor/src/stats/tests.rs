//! The stats handlers and usage records against fixture cgroup trees and a
//! hosted set built directly; the lifecycle RPCs that host Actors need
//! runsc and are covered elsewhere.

use super::*;
use crate::activation::Activation;
use crate::hosted::WorkloadSession;
use crate::ports::UsageSink;
use crate::runsc::{ContainerRuntime, ProcessRunner, Runsc, RunscCommand};
use crate::service::ServiceConfig;
use hivekeep_core::resource::ActorRef;
use parking_lot::Mutex;
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

const UID: &str = "aaaaaaaa-0000-4000-8000-000000000001";
const OTHER_UID: &str = "bbbbbbbb-0000-4000-8000-000000000002";
const HEALTHY_CPU_USEC: u64 = 1234567;
const DEAD_SANDBOX_MSG: &str = "Sandbox has no processes left while the actor is hosted";

fn test_actor() -> ActorAttribution {
    ActorAttribution {
        actor: ActorRef::new("space-a", "actor-a"),
        uid: UID.into(),
        template_hive: "ns-a".into(),
        template_name: "template-a".into(),
    }
}

fn actor_with_uid(uid: &str) -> ActorAttribution {
    ActorAttribution {
        uid: uid.into(),
        ..test_actor()
    }
}

type Files = &'static [(&'static str, &'static str)];

const HEALTHY: Files = &[
    ("memory.current", "157286400\n"),
    ("memory.peak", "209715200\n"),
    (
        "memory.stat",
        "anon 104857600\ninactive_file 20971520\nactive_file 31457280\n",
    ),
    ("cpu.stat", "usage_usec 1234567\nuser_usec 1000000\n"),
];
/// A sandbox leaf after the kernel OOM-killed the sentry: the leaf and its
/// counters remain, the processes do not.
const DEAD: Files = &[
    ("memory.current", "16384\n"),
    ("memory.events", "oom 5334\noom_kill 3\n"),
    ("cgroup.events", "populated 0\nfrozen 0\n"),
];
/// The sandbox leaf after a drain killed the app containers: the sentry
/// and gofers stay, so it is still populated.
const DRAINED: Files = &[
    ("memory.current", "31457280\n"),
    ("memory.events", "oom 0\noom_kill 0\n"),
    ("cgroup.events", "populated 1\nfrozen 0\n"),
];
const MALFORMED: Files = &[("memory.current", "max\n")];

#[derive(Default)]
struct Records(Mutex<Vec<(UsageKind, pb::WorkloadStatsSample)>>);

impl UsageSink for Records {
    fn emit(&self, kind: UsageKind, s: &pb::WorkloadStatsSample) {
        self.0.lock().push((kind, s.clone()));
    }
}

impl Records {
    fn kinds(&self) -> Vec<UsageKind> {
        self.0.lock().iter().map(|(k, _)| *k).collect()
    }

    fn sources(&self) -> Vec<i32> {
        self.0.lock().iter().map(|(_, s)| s.source).collect()
    }

    fn final_sources(&self) -> Vec<i32> {
        self.0
            .lock()
            .iter()
            .filter(|(k, _)| *k == UsageKind::Final)
            .map(|(_, s)| s.source)
            .collect()
    }
}

const CGROUP: i32 = pb::StatsSource::Cgroup as i32;
const UNSPECIFIED: i32 = pb::StatsSource::Unspecified as i32;

struct Fixture {
    root: tempfile::TempDir,
    svc: Arc<WorkerService>,
    records: Arc<Records>,
}

fn write_leaf(root: &Path, uid: &str, files: &[(&str, &str)]) {
    let dir = root.join(gvisor_cgroup_leaf(uid, SANDBOX_ID));
    std::fs::create_dir_all(&dir).unwrap();
    for (n, c) in files {
        std::fs::write(dir.join(n), c).unwrap();
    }
}

/// A service whose cgroup root is a fixture tree; `None` leaves the sandbox
/// leaf absent, which is what a torn-down sandbox looks like.
fn fixture(files: Option<Files>) -> Fixture {
    fixture_reading(files, None)
}

/// Like [`fixture`], with `during_read` run inside every cgroup read: the
/// window in which a checkpoint or a fresh run can change the hosted set.
type DuringRead = Box<dyn Fn(&WorkerService) + Send + Sync>;

fn fixture_reading(files: Option<Files>, during_read: Option<DuringRead>) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    if let Some(files) = files {
        write_leaf(root.path(), UID, files);
    }
    let records = Arc::new(Records::default());
    let me: Arc<OnceLock<std::sync::Weak<WorkerService>>> = Arc::default();
    let m = me.clone();
    let svc = WorkerService::new(ServiceConfig {
        cgroup_root: root.path().to_path_buf(),
        usage: Some(records.clone()),
        read_cgroup: Arc::new(move |dir| {
            if let (Some(f), Some(s)) = (&during_read, m.get().and_then(|w| w.upgrade())) {
                f(&s);
            }
            crate::cgroup_stats::read(dir)
        }),
        ..Default::default()
    });
    me.set(Arc::downgrade(&svc)).unwrap();
    Fixture { root, svc, records }
}

/// A running activation: its initial reading is done, so the sweep samples
/// it.
fn running(epoch: SystemTime) -> Activation {
    let a = Activation::new(epoch, false);
    a.initial(None, |_| {});
    a
}

/// Makes the worker host exactly `a` (a fresh activation, as admission
/// makes), or nothing.
fn set_hosted(svc: &WorkerService, a: Option<ActorAttribution>) -> Option<Arc<HostedActor>> {
    let mut t = svc.table.write();
    t.actors.clear();
    a.map(|a| {
        let h = Arc::new(HostedActor::with_usage(a, running(SystemTime::UNIX_EPOCH)));
        t.actors.insert(h.attribution.uid.clone(), h.clone());
        h
    })
}

/// Adds `a` to the hosted set as a running activation.
fn add_hosted(svc: &WorkerService, a: ActorAttribution) -> Arc<HostedActor> {
    let h = Arc::new(HostedActor::with_usage(a, running(SystemTime::UNIX_EPOCH)));
    svc.table
        .write()
        .actors
        .insert(h.attribution.uid.clone(), h.clone());
    h
}

/// Hosts the test Actor with the given epoch; `running` means its initial
/// reading is done.
fn host_with_epoch(svc: &WorkerService, epoch: SystemTime, is_running: bool) -> Arc<HostedActor> {
    let usage = if is_running {
        running(epoch)
    } else {
        Activation::new(epoch, false)
    };
    let h = Arc::new(HostedActor::with_usage(test_actor(), usage));
    let mut t = svc.table.write();
    t.actors.clear();
    t.actors.insert(UID.into(), h.clone());
    h
}

fn req(uid: &str) -> pb::GetWorkloadStatsRequest {
    pb::GetWorkloadStatsRequest {
        actor_uid: uid.into(),
    }
}

/// One sampler sweep, then the discovery read that serves it.
async fn sweep_and_list(svc: &WorkerService) -> pb::GetActiveWorkloadStatsResponse {
    svc.sweep_usage().await;
    svc.get_active_workload_stats()
}

fn remove_leaf(f: &Fixture) {
    std::fs::remove_dir_all(f.root.path().join(gvisor_cgroup_leaf(UID, SANDBOX_ID))).unwrap();
}

// ------------------------------------------------------------ keyed read

#[tokio::test]
async fn keyed_read_samples_the_sandbox_leaf() {
    let f = fixture(Some(HEALTHY));
    set_hosted(&f.svc, Some(test_actor()));
    let before = now_unix_nanos();
    let mut s = f
        .svc
        .get_workload_stats(req(UID))
        .await
        .unwrap()
        .sample
        .unwrap();
    let after = now_unix_nanos();
    assert!((before..=after).contains(&s.observed_at_unix_nano));
    s.observed_at_unix_nano = 0;
    // The activation starts at the unix epoch, so its epoch is zero.
    assert_eq!(
        s,
        pb::WorkloadStatsSample {
            hive: "space-a".into(),
            actor_name: "actor-a".into(),
            actor_uid: UID.into(),
            actor_template_hive: "ns-a".into(),
            actor_template_name: "template-a".into(),
            sandbox_class: "gvisor".into(),
            source: CGROUP,
            memory_current_bytes: 157286400,
            memory_peak_bytes: 209715200,
            memory_working_set_bytes: 136314880,
            cpu_usage_usec: HEALTHY_CPU_USEC,
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn keyed_read_errors() {
    let cases: Vec<(&str, Option<Files>, bool, &str, tonic::Code)> = vec![
        // A required field left off: a client bug, distinct from the races.
        (
            "empty actor_uid",
            Some(HEALTHY),
            true,
            "",
            tonic::Code::InvalidArgument,
        ),
        (
            "malformed actor_uid",
            Some(HEALTHY),
            true,
            "not-a-uuid",
            tonic::Code::InvalidArgument,
        ),
        // Not here at all: re-resolve rather than retry.
        (
            "worker is available",
            Some(HEALTHY),
            false,
            UID,
            tonic::Code::NotFound,
        ),
        // The worker was recycled under the caller's view of the world.
        (
            "actor_uid does not match the executing workload",
            Some(HEALTHY),
            true,
            OTHER_UID,
            tonic::Code::NotFound,
        ),
        // The one transient case: nothing to read yet, or any more.
        (
            "no sandbox cgroup to measure",
            None,
            true,
            UID,
            tonic::Code::FailedPrecondition,
        ),
        // There but unparseable: not a routine race.
        (
            "sandbox cgroup is malformed",
            Some(MALFORMED),
            true,
            UID,
            tonic::Code::Internal,
        ),
    ];
    for (name, files, hosted, uid, want) in cases {
        let f = fixture(files);
        if hosted {
            set_hosted(&f.svc, Some(test_actor()));
        }
        let err = f.svc.get_workload_stats(req(uid)).await.unwrap_err();
        assert_eq!(err.code(), want, "{name}: {err:?}");
    }
}

/// A stats poll must not queue behind a lifecycle RPC, which holds the
/// Actor's lock across its whole body.
#[tokio::test]
async fn keyed_read_does_not_take_the_lock() {
    let f = fixture(Some(HEALTHY));
    set_hosted(&f.svc, Some(test_actor()));
    let _held = f.svc.locks.lock(UID).await;
    tokio::time::timeout(Duration::from_secs(1), f.svc.get_workload_stats(req(UID)))
        .await
        .expect("stats read waited on the lifecycle lock")
        .unwrap();
}

/// A fresh service hosts nothing; the keyed read's NOT_FOUND on an idle
/// worker rests on this.
#[test]
fn service_starts_available() {
    assert!(WorkerService::new(ServiceConfig::default())
        .hosted_actors()
        .is_empty());
}

/// The caller asserted an Actor that is gone by the time the sample exists:
/// NOT_FOUND, its mapping wants re-resolving.
#[tokio::test]
async fn keyed_read_spots_a_transition_underneath() {
    let f = fixture_reading(
        Some(HEALTHY),
        Some(Box::new(|s| {
            set_hosted(s, None);
        })),
    );
    set_hosted(&f.svc, Some(test_actor()));
    let err = f.svc.get_workload_stats(req(UID)).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn keyed_read_stamps_the_epoch() {
    let f = fixture(Some(HEALTHY));
    let epoch = SystemTime::UNIX_EPOCH + Duration::from_secs(1700);
    let h = host_with_epoch(&f.svc, epoch, true);
    let s = f.svc.get_workload_stats(req(UID)).await.unwrap();
    assert_eq!(s.sample.unwrap().epoch_unix_nano, h.usage.epoch());
    assert_eq!(h.usage.epoch(), 1700 * 1_000_000_000);
}

/// The keyed read advances the activation's CPU, so it also updates the
/// sample the discovery cache serves.
#[tokio::test]
async fn keyed_read_stores_its_sample() {
    let f = fixture(Some(HEALTHY));
    let h = host_with_epoch(&f.svc, SystemTime::now(), true);
    let got = f.svc.get_workload_stats(req(UID)).await.unwrap();
    assert_eq!(h.usage.latest(), got.sample);
}

// -------------------------------------------------------- discovery read

/// The discovery read serves the same measurement as the keyed read.
#[tokio::test]
async fn discovery_matches_the_keyed_read() {
    let f = fixture(Some(HEALTHY));
    set_hosted(&f.svc, Some(test_actor()));
    let got = sweep_and_list(&f.svc).await;
    assert_eq!(got.samples.len(), 1);
    let mut keyed = f
        .svc
        .get_workload_stats(req(UID))
        .await
        .unwrap()
        .sample
        .unwrap();
    let mut sample = got.samples[0].clone();
    sample.observed_at_unix_nano = 0;
    keyed.observed_at_unix_nano = 0;
    // The keyed read is the second reading of the same counter, so it has
    // folded nothing new in.
    assert_eq!(keyed, sample);
}

/// An idle worker is an empty list, never an error.
#[tokio::test]
async fn discovery_on_an_available_worker_is_empty() {
    let f = fixture(Some(HEALTHY));
    assert!(sweep_and_list(&f.svc).await.samples.is_empty());
}

/// A hosted Actor with no cgroup is a pending entry, not an error.
#[tokio::test]
async fn discovery_without_a_cgroup_is_pending() {
    let f = fixture(None);
    set_hosted(&f.svc, Some(test_actor()));
    let got = sweep_and_list(&f.svc).await;
    assert_eq!(got.samples.len(), 1);
    let mut entry = got.samples[0].clone();
    assert_ne!(entry.observed_at_unix_nano, 0);
    entry.observed_at_unix_nano = 0;
    assert_eq!(
        entry,
        pb::WorkloadStatsSample {
            hive: "space-a".into(),
            actor_name: "actor-a".into(),
            actor_uid: UID.into(),
            actor_template_hive: "ns-a".into(),
            actor_template_name: "template-a".into(),
            sandbox_class: "gvisor".into(),
            ..Default::default()
        }
    );
}

/// Numbers read with no lock held are published only under the activation
/// they were read for.
#[tokio::test]
async fn discovery_transition_publishes_nothing() {
    let mut other_template = test_actor();
    other_template.template_name = "template-b".into();
    let cases: Vec<(&str, Option<ActorAttribution>)> = vec![
        ("re-hosted on another template", Some(other_template)),
        ("to another actor", Some(actor_with_uid(OTHER_UID))),
        ("to available", None),
    ];
    for (name, to) in cases {
        let target = to.clone();
        let f = fixture_reading(
            Some(HEALTHY),
            Some(Box::new(move |s| {
                set_hosted(s, target.clone());
            })),
        );
        set_hosted(&f.svc, Some(test_actor()));
        f.svc.sweep_usage().await;
        assert!(f.records.kinds().is_empty(), "{name}");
        if let Some(to) = to {
            let h = f.svc.lookup_actor(&to.uid).unwrap();
            assert_eq!(h.usage.latest(), None, "{name}: old reading served");
        }
    }
}

#[tokio::test]
async fn discovery_serves_several_actors() {
    let f = fixture(Some(HEALTHY));
    write_leaf(f.root.path(), OTHER_UID, HEALTHY);
    add_hosted(&f.svc, test_actor());
    add_hosted(&f.svc, actor_with_uid(OTHER_UID));
    let mut uids: Vec<_> = sweep_and_list(&f.svc)
        .await
        .samples
        .into_iter()
        .map(|s| {
            assert_eq!(s.source, CGROUP);
            s.actor_uid
        })
        .collect();
    uids.sort();
    assert_eq!(uids, [UID, OTHER_UID]);
}

/// One Actor with no cgroup, or one that does not parse, is pending
/// alongside the others' samples.
#[tokio::test]
async fn discovery_one_without_a_readable_cgroup() {
    for (name, broken) in [("no cgroup", None), ("unreadable", Some(MALFORMED))] {
        let f = fixture(Some(HEALTHY));
        if let Some(files) = broken {
            write_leaf(f.root.path(), "uid-broken", files);
        }
        add_hosted(&f.svc, test_actor());
        add_hosted(&f.svc, actor_with_uid("uid-broken"));
        let got = sweep_and_list(&f.svc).await;
        assert_eq!(got.samples.len(), 2, "{name}");
        for s in got.samples {
            let measured = s.source != UNSPECIFIED;
            assert_eq!(measured, s.actor_uid == UID, "{name}: {}", s.actor_uid);
        }
    }
}

// ----------------------------------------------------------- usage records

#[tokio::test]
async fn sweep_stamps_the_epoch_and_discovery_serves_it() {
    let f = fixture(Some(HEALTHY));
    let epoch = SystemTime::UNIX_EPOCH + Duration::from_secs(1700);
    let h = host_with_epoch(&f.svc, epoch, true);
    let before = &f.svc.get_active_workload_stats().samples[0];
    assert_eq!(before.source, UNSPECIFIED);
    assert_eq!(before.epoch_unix_nano, 1700 * 1_000_000_000);

    let after = sweep_and_list(&f.svc).await.samples[0].clone();
    assert_eq!(Some(&after), h.usage.latest().as_ref());
    assert_eq!(after.epoch_unix_nano, 1700 * 1_000_000_000);
    assert_eq!(after.cpu_usage_usec, HEALTHY_CPU_USEC);
}

#[tokio::test]
async fn records_initial_and_final() {
    let f = fixture(Some(HEALTHY));
    let h = host_with_epoch(&f.svc, SystemTime::now(), false);
    f.svc.record_initial(&h).await;
    assert!(h.usage.latest().is_some(), "initial sample not cached");
    f.svc.record_final(&h);
    assert_eq!(f.records.kinds(), [UsageKind::Initial, UsageKind::Final]);
    assert_eq!(f.records.sources(), [CGROUP, CGROUP]);
}

/// The final record waits for the Actor to be unhosted, as a teardown does
/// even when a later step fails.
#[tokio::test]
async fn final_record_once_the_activation_ends() {
    let f = fixture(Some(HEALTHY));
    let h = host_with_epoch(&f.svc, SystemTime::now(), true);
    f.svc.record_final_if_ended(Some(&h));
    assert!(f.records.kinds().is_empty());
    set_hosted(&f.svc, None);
    f.svc.record_final_if_ended(Some(&h));
    f.svc.record_final_if_ended(Some(&h));
    assert_eq!(f.records.kinds(), [UsageKind::Final]);
}

/// The drain ends each hosted activation with a final record, read before
/// its containers are killed.
#[tokio::test]
async fn graceful_shutdown_writes_final() {
    let f = fixture(Some(HEALTHY));
    let h = host_with_epoch(&f.svc, SystemTime::now(), true);
    let rt: Arc<dyn ContainerRuntime> = Arc::new(Runsc::new(
        RunscCommand::bare(
            "/nonexistent/runsc",
            UID,
            &pb::ActorDirs::default(),
            crate::flags::FlagStore::empty(),
        ),
        Arc::new(ProcessRunner),
    ));
    h.set_session(WorkloadSession {
        runtime: rt,
        containers: Vec::new(),
    });
    f.svc.graceful_shutdown().await;
    assert_eq!(f.records.kinds(), [UsageKind::Final]);
    assert_eq!(f.records.sources(), [CGROUP]);
}

/// A final read that finds the cgroup gone falls back to the latest
/// measured sample, and to a pending record when there is none.
#[tokio::test]
async fn final_record_after_the_sandbox_is_gone() {
    let f = fixture(Some(HEALTHY));
    let h = host_with_epoch(&f.svc, SystemTime::now(), true);
    f.svc.sweep_usage().await;
    remove_leaf(&f);
    f.svc.read_final(&h).await;
    f.svc.record_final(&h);

    let fresh = host_with_epoch(&f.svc, SystemTime::now(), true);
    f.svc.read_final(&fresh).await;
    f.svc.record_final(&fresh);
    assert_eq!(f.records.final_sources(), [CGROUP, UNSPECIFIED]);
}

/// The reading taken before a snapshot writes nothing by itself; the final
/// record written after the checkpoint carries it.
#[tokio::test]
async fn read_final_writes_nothing_until_recorded() {
    let f = fixture(Some(HEALTHY));
    let h = host_with_epoch(&f.svc, SystemTime::now(), true);
    f.svc.read_final(&h).await;
    assert!(f.records.kinds().is_empty());
    assert_eq!(h.usage.latest().map(|s| s.source), Some(CGROUP));
    remove_leaf(&f);
    f.svc.record_final(&h);
    assert_eq!(f.records.kinds(), [UsageKind::Final]);
    assert_eq!(f.records.sources(), [CGROUP]);
}

/// The sweep reads no Actor before its initial reading or after its final
/// record.
#[tokio::test]
async fn sweep_waits_for_initial_and_stops_at_final() {
    let f = fixture(Some(HEALTHY));
    let h = host_with_epoch(&f.svc, SystemTime::now(), false);
    f.svc.sweep_usage().await;
    f.svc.record_initial(&h).await;
    f.svc.sweep_usage().await;
    f.svc.record_final(&h);
    f.svc.sweep_usage().await;
    assert_eq!(
        f.records.kinds(),
        [UsageKind::Initial, UsageKind::Periodic, UsageKind::Final]
    );
}

#[tokio::test]
async fn final_record_without_a_measurement_is_pending() {
    let f = fixture(None);
    let h = host_with_epoch(&f.svc, SystemTime::now(), false);
    f.svc.record_initial(&h).await;
    set_hosted(&f.svc, None);
    f.svc.record_final_if_ended(Some(&h));
    assert_eq!(f.records.kinds(), [UsageKind::Final]);
    assert_eq!(f.records.sources(), [UNSPECIFIED]);
}

// ------------------------------------------------------------ dead sandbox

/// A hosted Actor whose sandbox cgroup is empty is warned about once per
/// activation, and not at all while a lifecycle RPC is tearing it down.
#[tokio::test(flavor = "current_thread")]
async fn sweep_warns_about_a_dead_sandbox() {
    let cases: [(&str, Option<Files>, bool, usize); 4] = [
        ("dead sandbox warns once", Some(DEAD), false, 1),
        ("teardown in progress is skipped", Some(DEAD), true, 0),
        ("live sandbox does not warn", Some(HEALTHY), false, 0),
        (
            "booting sandbox with no cgroup yet does not warn",
            None,
            false,
            0,
        ),
    ];
    for (name, files, busy, want) in cases {
        let (logs, _g) = crate::test_log::capture();
        let f = fixture(files);
        set_hosted(&f.svc, Some(test_actor()));
        let _held = if busy {
            Some(f.svc.locks.lock(UID).await)
        } else {
            None
        };
        // The second sweep must not repeat the warning.
        for _ in 0..2 {
            assert_eq!(sweep_and_list(&f.svc).await.samples.len(), 1, "{name}");
        }
        let warns = logs.with_message(DEAD_SANDBOX_MSG);
        assert_eq!(warns.len(), want, "{name}");
        if let Some(w) = warns.first() {
            assert_eq!(w.level, tracing::Level::WARN);
            for (k, v) in [
                ("actor_uid", UID),
                ("actor", "actor-a"),
                ("hive", "space-a"),
                ("template_name", "template-a"),
                ("oom_kills", "3"),
            ] {
                assert_eq!(w.get(k), v, "{name}: {k}");
            }
        }
    }
}

/// A record that is no longer the hosted one is never reported.
#[tokio::test]
async fn stale_records_are_not_reported() {
    let f = fixture(Some(DEAD));
    set_hosted(&f.svc, Some(test_actor()));
    let stale = Arc::new(HostedActor::new(test_actor()));
    let raw = crate::cgroup_stats::Sample {
        empty: true,
        ..Default::default()
    };
    assert!(!f.svc.warn_dead_sandbox(&stale, &raw));
    assert!(!stale.dead_reported.load(Ordering::SeqCst));
}

/// Once per activation: a new activation that dies again is reported again.
#[tokio::test(flavor = "current_thread")]
async fn a_new_activation_warns_again() {
    let (logs, _g) = crate::test_log::capture();
    let f = fixture(Some(DEAD));
    for _ in 0..2 {
        set_hosted(&f.svc, Some(test_actor()));
        f.svc.sweep_usage().await;
    }
    assert_eq!(logs.count(DEAD_SANDBOX_MSG), 2);
}

/// Concurrent sweeps of one dead sandbox still warn once.
#[tokio::test(flavor = "current_thread")]
async fn concurrent_sweeps_warn_once() {
    let (logs, _g) = crate::test_log::capture();
    let f = fixture(Some(DEAD));
    set_hosted(&f.svc, Some(test_actor()));
    futures::future::join_all((0..16).map(|_| f.svc.sweep_usage())).await;
    assert_eq!(logs.count(DEAD_SANDBOX_MSG), 1);
}

/// The drain kills app containers without locks or unhosting but never the
/// sandbox, so its leaf stays populated; a sandbox that dies during the
/// drain is still reported.
#[tokio::test(flavor = "current_thread")]
async fn dead_sandbox_check_during_the_drain() {
    for (name, files, want) in [
        ("app containers killed, sentry alive", DRAINED, 0),
        ("sandbox died during the drain", DEAD, 1),
    ] {
        let (logs, _g) = crate::test_log::capture();
        let f = fixture(Some(files));
        set_hosted(&f.svc, Some(test_actor()));
        f.svc.shutting_down.store(true, Ordering::SeqCst);
        f.svc.sweep_usage().await;
        assert_eq!(logs.count(DEAD_SANDBOX_MSG), want, "{name}");
    }
}

/// The drain kills the containers the spec names. Container names are DNS
/// labels and the sandbox id is not one, so no spec can put the sandbox on
/// the drain's kill list.
#[test]
fn the_drain_never_kills_the_sandbox() {
    assert!(
        !hivekeep_core::names::is_valid_resource_name(SANDBOX_ID),
        "{SANDBOX_ID} is a valid container name"
    );
    let spec = ["app", "sidecar"].map(|n| pb::Container {
        name: n.into(),
        ..Default::default()
    });
    assert!(!crate::lifecycle::container_names(&spec)
        .iter()
        .any(|n| n == SANDBOX_ID));
}

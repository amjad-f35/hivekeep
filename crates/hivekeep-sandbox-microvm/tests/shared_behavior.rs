//! Behaviour this worker relies on from the shared worker library
//! (`hivekeep-sandbox-gvisor`): per-Actor locks and the in-flight registry,
//! checkpoint completion records, request attribution and phase records.

use std::time::Duration;

use hivekeep_core::resource::ActorAttribution;
use hivekeep_proto::worker::v1 as pb;
use hivekeep_sandbox_gvisor::actor_lock::{InFlight, Locks, Rpc};
use hivekeep_sandbox_gvisor::attribution::{uid_only, ActorRequest};
use hivekeep_sandbox_gvisor::checkpoint_record::{record_checkpoint, reuse_checkpoint};
use hivekeep_sandbox_gvisor::phaselog::{
    fidelity_label, log_record, snapshot_phase_record, Phase, CHECKPOINT_DURATION_KEY,
    RESTORE_DURATION_KEY,
};
use tokio_util::sync::CancellationToken;

// ---- per-Actor locks

#[tokio::test]
async fn locks_do_not_block_other_actors() {
    let locks = Locks::new();
    let _a = locks.lock("actor-a").await;
    tokio::time::timeout(Duration::from_secs(5), locks.lock("actor-b"))
        .await
        .expect("actor-b blocked on actor-a's lock");
}

#[tokio::test]
async fn locks_serialize_the_same_actor() {
    let locks = Locks::new();
    let held = locks.lock("actor-a").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), locks.lock("actor-a"))
            .await
            .is_err(),
        "a second holder took actor-a's lock while it was held"
    );
    drop(held);
    tokio::time::timeout(Duration::from_secs(5), locks.lock("actor-a"))
        .await
        .expect("actor-a's lock was not released");
}

#[tokio::test]
async fn busy_while_held_and_only_for_its_actor() {
    let locks = Locks::new();
    assert!(!locks.busy("actor-a"));
    let g = locks.lock("actor-a").await;
    assert!(locks.busy("actor-a"));
    assert!(!locks.busy("actor-b"));
    drop(g);
    assert!(!locks.busy("actor-a"));
}

#[tokio::test]
async fn busy_counts_waiters() {
    // A waiter that gives up leaves the holder's busy state alone.
    let locks = std::sync::Arc::new(Locks::new());
    let held = locks.lock("actor-a").await;
    let cancel = CancellationToken::new();
    let (l, c) = (locks.clone(), cancel.clone());
    let waiter = tokio::spawn(async move { l.lock_cancellable("actor-a", &c).await.is_some() });
    tokio::time::sleep(Duration::from_millis(20)).await;
    cancel.cancel();
    assert!(!waiter.await.unwrap(), "the cancelled waiter took the lock");
    assert!(
        locks.busy("actor-a"),
        "idle after the waiter gave up, while still held"
    );
    drop(held);
    assert!(!locks.busy("actor-a"));

    // A waiter that takes over keeps the Actor busy.
    let held = locks.lock("actor-a").await;
    let l = locks.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let g = l.lock("actor-a").await;
        let _ = tx.send(());
        let _ = release_rx.await;
        drop(g);
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(held);
    rx.await.expect("the waiter did not take the lock");
    assert!(
        locks.busy("actor-a"),
        "idle while the former waiter holds it"
    );
    release_tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!locks.busy("actor-a"));
}

#[tokio::test]
async fn idle_actors_are_forgotten() {
    let locks = Locks::new();
    for uid in ["a", "b", "c"] {
        drop(locks.lock(uid).await);
    }
    for uid in ["a", "b", "c"] {
        assert!(!locks.busy(uid), "a lock was retained for idle actor {uid}");
    }
}

#[tokio::test]
async fn a_cancelled_context_never_takes_a_free_lock() {
    let locks = Locks::default();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(locks.lock_cancellable("actor-a", &cancel).await.is_none());
    assert!(!locks.busy("actor-a"));
    assert!(locks
        .lock_cancellable("actor-a", &CancellationToken::new())
        .await
        .is_some());
}

// ---- in-flight RPCs

#[tokio::test]
async fn wait_idle_waits_for_every_actor() {
    let f = InFlight::new();
    let a = f.add("actor-a", Rpc::Run, Some(CancellationToken::new()));
    let b = f.add("actor-b", Rpc::Checkpoint, None);
    let short = || {
        let c = CancellationToken::new();
        let c2 = c.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            c2.cancel();
        });
        c
    };
    assert!(
        !f.wait_idle(&short()).await,
        "returned while two RPCs were in flight"
    );
    drop(a);
    assert!(
        !f.wait_idle(&short()).await,
        "returned while actor-b was in flight"
    );
    drop(b);
    assert!(f.wait_idle(&CancellationToken::new()).await);
}

#[test]
fn cancel_startups_leaves_checkpoints_running() {
    let f = InFlight::new();
    let (ta, tb) = (CancellationToken::new(), CancellationToken::new());
    let _a = f.add("actor-a", Rpc::Run, Some(ta.clone()));
    let _b = f.add("actor-b", Rpc::Restore, Some(tb.clone()));
    let _c = f.add("actor-c", Rpc::Checkpoint, None);
    assert_eq!(f.cancel_startups().len(), 2);
    assert!(ta.is_cancelled() && tb.is_cancelled());
}

// ---- checkpoint completion records

#[test]
fn records_and_reuses_a_checkpoint() {
    let d = tempfile::tempdir().unwrap();
    std::fs::write(d.path().join("checkpoint.img"), "snapshot-bytes").unwrap();
    let mem = pb::SnapshotFidelity::Memory as i32;
    assert!(reuse_checkpoint(d.path(), mem).is_none());
    let resp = pb::CheckpointWorkloadResponse {
        snapshot_files: vec!["checkpoint.img".into()],
        data_snapshot_files: vec![],
    };
    record_checkpoint(d.path(), mem, &resp).unwrap();
    assert_eq!(
        reuse_checkpoint(d.path(), mem).unwrap().snapshot_files,
        resp.snapshot_files
    );
    assert!(
        reuse_checkpoint(d.path(), pb::SnapshotFidelity::Volumes as i32).is_none(),
        "another fidelity"
    );
    std::fs::remove_file(d.path().join("checkpoint.img")).unwrap();
    assert!(
        reuse_checkpoint(d.path(), mem).is_none(),
        "a listed file is gone"
    );
}

#[test]
fn data_files_must_fit_the_fidelity() {
    let d = tempfile::tempdir().unwrap();
    std::fs::create_dir(d.path().join("fs")).unwrap();
    for f in ["checkpoint.img", "fs/fscheckpoint.pb", "fs/multitar.img"] {
        std::fs::write(d.path().join(f), "data").unwrap();
    }
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let (mem, vol) = (
        pb::SnapshotFidelity::Memory as i32,
        pb::SnapshotFidelity::Volumes as i32,
    );
    let mem_resp = pb::CheckpointWorkloadResponse {
        snapshot_files: s(&["checkpoint.img", "fs/fscheckpoint.pb", "fs/multitar.img"]),
        data_snapshot_files: s(&["fs/fscheckpoint.pb", "fs/multitar.img"]),
    };
    record_checkpoint(d.path(), mem, &mem_resp).unwrap();
    assert_eq!(reuse_checkpoint(d.path(), mem), Some(mem_resp.clone()));
    let bad = pb::CheckpointWorkloadResponse {
        snapshot_files: s(&["checkpoint.img"]),
        data_snapshot_files: s(&["fs/fscheckpoint.pb"]),
    };
    record_checkpoint(d.path(), mem, &bad).unwrap();
    assert!(
        reuse_checkpoint(d.path(), mem).is_none(),
        "a data file not among the snapshot files"
    );
    let vol_resp = pb::CheckpointWorkloadResponse {
        snapshot_files: s(&["fs/multitar.img", "fs/fscheckpoint.pb"]),
        data_snapshot_files: s(&["fs/fscheckpoint.pb", "fs/multitar.img"]),
    };
    record_checkpoint(d.path(), vol, &vol_resp).unwrap();
    assert_eq!(reuse_checkpoint(d.path(), vol), Some(vol_resp));
    record_checkpoint(d.path(), vol, &mem_resp).unwrap();
    assert!(
        reuse_checkpoint(d.path(), vol).is_none(),
        "VOLUMES with non-data files"
    );
}

// ---- attribution

#[test]
fn attribution_from_every_request() {
    macro_rules! req {
        ($t:ty) => {
            <$t>::default()
        };
    }
    let full = ActorAttribution {
        actor: hivekeep_core::resource::ActorRef::new("hive-a", "actor-b"),
        uid: "uid-c".into(),
        template_hive: "template-ns-d".into(),
        template_name: "template-name-e".into(),
    };
    macro_rules! filled {
        ($t:ty) => {{
            let mut r = req!($t);
            r.hive = "hive-a".into();
            r.actor_name = "actor-b".into();
            r.actor_uid = "uid-c".into();
            r.actor_template_hive = "template-ns-d".into();
            r.actor_template_name = "template-name-e".into();
            r.attribution()
        }};
    }
    assert_eq!(filled!(pb::RunWorkloadRequest), full, "run");
    assert_eq!(filled!(pb::RestoreWorkloadRequest), full, "restore");
    assert_eq!(filled!(pb::CheckpointWorkloadRequest), full, "checkpoint");
    assert_eq!(filled!(pb::TerminateWorkloadRequest), full, "terminate");
    let empty = ActorAttribution {
        actor: hivekeep_core::resource::ActorRef::new("", ""),
        uid: String::new(),
        template_hive: String::new(),
        template_name: String::new(),
    };
    assert_eq!(
        req!(pb::RunWorkloadRequest).attribution(),
        empty,
        "empty run"
    );
    assert_eq!(
        req!(pb::TerminateWorkloadRequest).attribution(),
        empty,
        "empty terminate"
    );
}

// ---- phase records

#[test]
fn phase_records() {
    let a = uid_only("uid-1");
    let phases = [
        Phase {
            name: "prep",
            d: Duration::from_millis(1500),
        },
        Phase {
            name: "pause",
            d: Duration::ZERO,
        },
        Phase {
            name: "total",
            d: Duration::from_secs(2),
        },
    ];
    let r = snapshot_phase_record(
        &a,
        pb::SnapshotFidelity::Memory as i32,
        RESTORE_DURATION_KEY,
        None,
        &phases,
    );
    assert_eq!(r["hk.actor.uid"], "uid-1");
    assert_eq!(r["hk.snapshot.fidelity"], "memory");
    assert_eq!(
        r[&format!("{RESTORE_DURATION_KEY}.prep")].as_f64(),
        Some(1.5)
    );
    assert!(
        !r.contains_key(&format!("{RESTORE_DURATION_KEY}.pause")),
        "a phase that never ran is recorded"
    );
    assert!(!r.contains_key("error.type"));
    let failed = snapshot_phase_record(
        &a,
        pb::SnapshotFidelity::Volumes as i32,
        CHECKPOINT_DURATION_KEY,
        Some(tonic::Code::Unavailable),
        &phases,
    );
    assert!(failed.contains_key("error.type"));
    assert!(failed.contains_key(&format!("{CHECKPOINT_DURATION_KEY}.total")));
    for (f, want) in [
        (1, "volumes"),
        (2, "rootfs"),
        (3, "memory"),
        (0, "unknown"),
        (99, "unknown"),
    ] {
        assert_eq!(fidelity_label(f), want);
    }
    log_record("Restore timing breakdown", &r);
}

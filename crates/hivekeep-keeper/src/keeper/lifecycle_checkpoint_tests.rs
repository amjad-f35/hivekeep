//! Checkpoint and PromoteSnapshot against a fake worker: upload modes,
//! retries, generation fencing, timeouts.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::worker::v1 as wpb;
use tonic::Code;

use super::lifecycle_harness::*;
use crate::assets::{read_sandbox_record, SandboxAssetsRecord, MANIFEST_NAME};
use crate::boot_id::local_snapshot_boot_check;
use crate::context::Ctx;
use crate::keeper::spec::to_worker_fidelity;
use crate::labels::fidelity_label;
use crate::snapshot_tracker::{Options, SnapshotRef, Store};
use crate::worker_dialer::WorkerApi;

fn bg() -> Ctx {
    Ctx::background()
}

fn counting(n: &Arc<AtomicI32>) -> Hook {
    let n = n.clone();
    hook(move || {
        n.fetch_add(1, Ordering::SeqCst);
        async {}
    })
}

fn transfer_store() -> NodeOpts {
    NodeOpts {
        store: StoreKind::Transfer,
        ..Default::default()
    }
}

/// An immediate checkpoint uploads before it returns and keeps a local copy.
#[tokio::test]
async fn immediate_checkpoint_keeps_local_copy() {
    let a = run_plugin_actor(NodeOpts::default()).await;
    let snap = a.snapshot("suspend-snap-1", 1);
    let st = a
        .checkpoint(&snap, pb::UploadMode::Immediate)
        .await
        .expect("Checkpoint")
        .state
        .unwrap();
    assert!(
        a.uploaded("suspend-snap-1"),
        "objects = {:?}",
        a.node.objects.keys()
    );
    assert!(exists(&a.paths().local_snapshot_dir(UID, "suspend-snap-1")));
    let local = st.latest_local.as_ref().unwrap();
    assert_eq!(local.snapshot.as_ref(), Some(&snap));
    assert!(local.local_commit_time.is_some() && local.durable_complete_time.is_none());
    let durable = st.latest_durable.as_ref().unwrap();
    assert_eq!(durable.snapshot.as_ref(), Some(&snap));
    assert!(durable.local_commit_time.is_some() && durable.durable_complete_time.is_some());
}

/// An immediate checkpoint whose snapshot cannot be kept locally still
/// succeeds, reporting nothing local; a retry answers from the durable
/// snapshot without checkpointing or uploading again and finishes the
/// first call's cleanup.
#[tokio::test]
async fn immediate_checkpoint_without_local_copy() {
    let a = run_plugin_actor(NodeOpts::default()).await;
    let checkpoints = Arc::new(AtomicI32::new(0));
    a.worker().set_on_checkpoint(counting(&checkpoints));
    let snap = a.snapshot("suspend-snap-1", 1);
    // A file where the local checkpoints dir belongs fails the move.
    std::fs::create_dir_all(a.paths().actor_snapshot_dir(UID)).unwrap();
    std::fs::write(a.paths().local_checkpoints_dir(UID), b"").unwrap();
    let st = a
        .checkpoint(&snap, pb::UploadMode::Immediate)
        .await
        .expect("Checkpoint")
        .state
        .unwrap();
    assert!(a.uploaded("suspend-snap-1"));
    assert!(st.latest_local.is_none(), "{st:?}");
    let durable = st.latest_durable.as_ref().unwrap();
    assert_eq!(durable.snapshot.as_ref(), Some(&snap));
    assert!(durable.durable_complete_time.is_some());
    assert_eq!(a.node.tracker().state(UID).await, st);

    a.node
        .objects
        .fail_puts("retried Checkpoint uploaded a snapshot that was already durable");
    // What the first call's cleanup leaves behind when it fails.
    let leftover = a.paths().oci_bundle_dir(UID).join("leftover");
    std::fs::create_dir_all(leftover.parent().unwrap()).unwrap();
    std::fs::write(&leftover, b"").unwrap();
    let again = a
        .checkpoint(&snap, pb::UploadMode::Immediate)
        .await
        .expect("retried Checkpoint")
        .state
        .unwrap();
    assert_eq!(checkpoints.load(Ordering::SeqCst), 1);
    assert_eq!(again, st);
    assert!(!exists(&leftover), "the retry did not clean up");
}

/// A Checkpoint retried after the actor was restored from the snapshot it
/// committed (local copy, or upload after a detach) answers from it without
/// checkpointing or cleaning up the restored activation.
#[tokio::test]
async fn stale_checkpoint_retry_leaves_restored_actor_alone() {
    for detach in [false, true] {
        let a = run_plugin_actor(transfer_store()).await;
        let checkpoints = Arc::new(AtomicI32::new(0));
        a.worker().set_on_checkpoint(counting(&checkpoints));
        let snap = a.checkpointed("pause-snap-1", 1).await;
        if detach {
            a.node.tracker().detach(&bg(), UID).await.expect("Detach");
        }
        a.k()
            .restore(&bg(), a.restore_request(&snap))
            .await
            .expect("Restore");
        let in_use = a.paths().oci_bundle_dir(UID).join("in-use");
        std::fs::write(&in_use, b"").unwrap();
        let st = a
            .checkpoint(&snap, pb::UploadMode::Deferred)
            .await
            .expect("stale retry")
            .state
            .unwrap();
        assert_eq!(checkpoints.load(Ordering::SeqCst), 1, "detach={detach}");
        let committed = if detach {
            snap_of(st.latest_durable.as_ref())
        } else {
            snap_of(st.latest_local.as_ref())
        };
        assert_eq!(committed.as_ref(), Some(&snap), "detach={detach}: {st:?}");
        assert!(
            in_use.exists(),
            "detach={detach}: the retry touched the restored bundle dir"
        );
    }
}

/// A Checkpoint retried after it committed answers without the worker and
/// finishes the cleanup; under immediate upload it uploads first if the
/// first attempt deferred; after a detach a deferred retry returns nothing
/// local. A retry records no checkpoint duration.
#[tokio::test]
async fn retried_checkpoint_answers_from_committed_snapshot() {
    use pb::UploadMode::{Deferred as D, Immediate as I};
    for (name, first, retry, detach_first, want_local, want_durable) in [
        ("deferred", D, D, false, true, false),
        ("immediate", I, I, false, true, true),
        ("deferred then immediate", D, I, false, true, true),
        ("immediate then deferred", I, D, false, true, true),
        ("deferred after detach", D, D, true, false, true),
        ("immediate after detach", D, I, true, false, true),
    ] {
        let a = run_plugin_actor(transfer_store()).await;
        let checkpoints = Arc::new(AtomicI32::new(0));
        a.worker().set_on_checkpoint(counting(&checkpoints));
        let snap = a.snapshot("snap-1", 1);
        a.checkpoint(&snap, first).await.expect("Checkpoint");
        if detach_first {
            a.node.tracker().detach(&bg(), UID).await.expect("Detach");
        }
        let leftover = a.paths().oci_bundle_dir(UID).join("leftover");
        std::fs::create_dir_all(leftover.parent().unwrap()).unwrap();
        std::fs::write(&leftover, b"").unwrap();
        let samples = a.node.checkpoint_samples("total");

        let st = a
            .checkpoint(&snap, retry)
            .await
            .unwrap_or_else(|e| panic!("{name}: retried Checkpoint: {e:?}"))
            .state
            .unwrap();
        assert_eq!(checkpoints.load(Ordering::SeqCst), 1, "{name}");
        assert!(!exists(&leftover), "{name}: leftover not cleaned up");
        assert_eq!(
            exists(&a.paths().oci_bundle_dir(UID)),
            want_local,
            "{name}: bundle dir"
        );
        assert_eq!(
            snap_of(st.latest_local.as_ref()).as_ref() == Some(&snap),
            want_local,
            "{name}: {st:?}"
        );
        assert_eq!(
            snap_of(st.latest_durable.as_ref()).as_ref() == Some(&snap),
            want_durable,
            "{name}: {st:?}"
        );
        assert_eq!(a.uploaded("snap-1"), want_durable, "{name}: uploaded");
        assert_eq!(a.node.tracker().state(UID).await, st, "{name}");
        assert_eq!(
            a.node.checkpoint_samples("total"),
            samples,
            "{name}: the retry recorded a checkpoint duration"
        );
    }
}

/// A deferred Checkpoint retried after its first attempt stopped the
/// workload and began staging, but before the commit, commits from the
/// files the worker left in the checkpoint dir.
#[tokio::test]
async fn retried_checkpoint_after_uncommitted_staging() {
    for (name, not_written) in [
        ("staging interrupted", vec!["pages.img".to_string()]),
        ("local snapshot published", vec![]),
    ] {
        let a = run_plugin_actor(NodeOpts::default()).await;
        a.worker().stops_workload.store(true, Ordering::SeqCst);
        let captures = Arc::new(AtomicI32::new(0));
        a.worker().set_on_checkpoint(counting(&captures));
        let snap = a.snapshot("snap-1", 1);
        let req = a.checkpoint_request(&snap, pb::UploadMode::Deferred);

        // The first attempt gets as far as staging the local snapshot.
        let wresp = a
            .worker()
            .checkpoint_workload(
                &bg(),
                wpb::CheckpointWorkloadRequest {
                    actor_dirs: Some(a.paths().actor_dirs(UID)),
                    fidelity: to_worker_fidelity(snap.fidelity),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut rec = read_sandbox_record(a.paths(), UID).unwrap();
        rec.snapshot_files = wresp
            .snapshot_files
            .iter()
            .cloned()
            .chain(not_written.iter().cloned())
            .collect();
        let moved = a.k().move_local_checkpoint(&req, &rec);
        assert_eq!(moved.is_err(), !not_written.is_empty(), "{name}: {moved:?}");

        let st = a
            .k()
            .checkpoint(&bg(), req)
            .await
            .unwrap_or_else(|e| panic!("{name}: retried Checkpoint: {e:?}"))
            .state
            .unwrap();
        assert_eq!(
            snap_of(st.latest_local.as_ref()).as_ref(),
            Some(&snap),
            "{name}"
        );
        assert_eq!(captures.load(Ordering::SeqCst), 1, "{name}: captures");
        a.restore_local("snap-1", 1).await;
    }
}

async fn assert_cleaned_up(a: &PluginActor) {
    assert_eq!(
        a.node.tracker().state(UID).await,
        pb::ActorSnapshotState::default()
    );
    assert!(!exists(&a.paths().actor_snapshot_state_file(UID)));
    assert!(!exists(&a.paths().local_snapshot_dir(UID, "snap-1")));
    assert!(!exists(&a.paths().actor_dir(UID)));
}

/// Waits for `started`, then checks a Terminate waits behind the RPC that
/// holds the actor until `release`, and succeeds after.
async fn terminate_behind(a: &Arc<PluginActor>, started: &Latch, release: &Latch) {
    within(Duration::from_secs(10), "the RPC to block", started.wait()).await;
    let a2 = a.clone();
    let mut term = tokio::spawn(async move {
        let mut req = a2.terminate_request();
        req.actor_template_hive.clear();
        req.actor_template_name.clear();
        a2.k().terminate(&bg(), req).await
    });
    assert_pending(&mut term, Duration::from_millis(50), "Terminate").await;
    release.open();
    term.await.unwrap().expect("Terminate");
}

/// Terminate waits on the actor's lock while a Checkpoint or a
/// PromoteSnapshot runs, then cleans up after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_waits_for_in_flight_rpc() {
    for mode in [pb::UploadMode::Deferred, pb::UploadMode::Immediate] {
        let a = Arc::new(run_plugin_actor(NodeOpts::default()).await);
        let (started, release) = (Latch::new(), Latch::new());
        a.worker().set_on_checkpoint(block_once(&started, &release));
        let a2 = a.clone();
        let cp = tokio::spawn(async move { a2.checkpoint(&a2.snapshot("snap-1", 1), mode).await });
        terminate_behind(&a, &started, &release).await;
        cp.await.unwrap().expect("Checkpoint");
        assert_cleaned_up(&a).await;
    }

    let a = Arc::new(run_plugin_actor(NodeOpts::default()).await);
    let snap = a.checkpointed("snap-1", 1).await;
    let (started, release) = (Latch::new(), Latch::new());
    *a.node.objects.on_put.lock() = Some(block_once(&started, &release));
    let a2 = a.clone();
    let promote = tokio::spawn(async move {
        a2.k()
            .promote_snapshot(&bg(), a2.promote_request(&snap))
            .await
    });
    terminate_behind(&a, &started, &release).await;
    promote.await.unwrap().expect("PromoteSnapshot");
    assert_cleaned_up(&a).await;
}

/// A checkpoint of a resumed actor failing before its snapshot commits
/// keeps the local snapshot, recorded and restorable.
#[tokio::test]
async fn failed_checkpoint_keeps_local_snapshot() {
    for (name, snap_name, mode, no_new_snap) in [
        (
            "suspend upload fails",
            "suspend-snap-2",
            pb::UploadMode::Immediate,
            false,
        ),
        (
            "missing snapshot file",
            "pause-snap-2",
            pb::UploadMode::Deferred,
            true,
        ),
    ] {
        let a = run_plugin_actor(NodeOpts::default()).await;
        a.checkpointed("pause-snap-1", 1).await;
        a.restore_local("pause-snap-1", 1).await;
        if no_new_snap {
            let file = a.paths().checkpoint_state_dir(UID).join("checkpoint.img");
            let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
            a.worker().set_on_checkpoint(hook(move || {
                if !fired.swap(true, Ordering::SeqCst) {
                    std::fs::remove_file(&file).unwrap();
                }
                async {}
            }));
        } else {
            a.node.objects.fail_puts("object store unavailable");
        }
        assert!(
            a.checkpoint(&a.snapshot(snap_name, 2), mode).await.is_err(),
            "{name}: Checkpoint succeeded"
        );
        if no_new_snap {
            assert!(
                !exists(&a.paths().local_snapshot_dir(UID, snap_name)),
                "{name}"
            );
        }
        assert!(
            exists(&a.paths().local_snapshot_dir(UID, "pause-snap-1")),
            "{name}"
        );
        assert_eq!(
            uuid(a.node.tracker().state(UID).await.latest_local.as_ref()),
            "pause-snap-1",
            "{name}"
        );
        a.restore_local("pause-snap-1", 1).await;
        assert_eq!(
            std::fs::read_dir(a.paths().checkpoint_state_dir(UID))
                .unwrap()
                .count(),
            0,
            "{name}: checkpoint-state after the restore"
        );
    }
}

/// PromoteSnapshot records the upload as the latest durable snapshot with
/// the checkpoint's commit time and keeps the local copy; a retry reports
/// the same; a mismatched generation is refused.
#[tokio::test]
async fn promote_snapshot_records_upload() {
    let a = run_plugin_actor(NodeOpts::default()).await;
    let snap = a.snapshot("pause-snap-1", 1);
    let commit = a
        .checkpoint(&snap, pb::UploadMode::Deferred)
        .await
        .unwrap()
        .state
        .unwrap()
        .latest_local
        .unwrap()
        .local_commit_time;
    let st = a.promote(&snap).await;
    assert!(exists(&a.paths().local_snapshot_dir(UID, "pause-snap-1")));
    let local = st.latest_local.as_ref().unwrap();
    assert_eq!(local.snapshot.as_ref(), Some(&snap));
    assert_eq!(local.local_commit_time, commit);
    assert!(local.durable_complete_time.is_none());
    let durable = st.latest_durable.as_ref().unwrap();
    assert_eq!(durable.snapshot.as_ref(), Some(&snap));
    assert_eq!(durable.local_commit_time, commit);
    assert!(durable.durable_complete_time.is_some());
    assert_eq!(a.node.tracker().state(UID).await, st);

    a.node
        .objects
        .fail_puts("retried promote should not upload");
    assert_eq!(a.promote(&snap).await, st);

    let mut mismatch = snap.clone();
    mismatch.generation += 1;
    let err = a
        .k()
        .promote_snapshot(&bg(), a.promote_request(&mismatch))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
}

/// The transfer as the tracker's store: a fidelity mismatch is refused
/// before uploading; the first upload uploads; once the local copy is gone
/// the upload finds the remote manifest and reports it did not upload.
#[tokio::test]
async fn tracker_store_upload_local_snapshot() {
    let a = run_plugin_actor(NodeOpts::default()).await;
    let snap = a.checkpointed("pause-snap-1", 1).await;
    let st = &a.node.transfer;
    let actor = ActorRef::new(HIVE, ACTOR);
    let mut other = snap.clone();
    other.fidelity = pb::SnapshotFidelity::Unspecified as i32;
    let err = st
        .upload_local_snapshot(&bg(), UID, &SnapshotRef::new(other, actor.clone()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    let r = SnapshotRef::new(snap, actor);
    assert!(st.upload_local_snapshot(&bg(), UID, &r).await.unwrap());
    prune_local_checkpoints(a.paths(), UID, &[]).unwrap();
    assert!(!st.upload_local_snapshot(&bg(), UID, &r).await.unwrap());
}

/// Run and Restore report a failed directory reset as Canceled, which the
/// control plane retries, and the retry succeeds once the reset can.
#[tokio::test]
async fn reset_actor_dirs_failure_is_canceled() {
    for restore in [false, true] {
        let a = run_plugin_actor(NodeOpts::default()).await;
        let restore_req = if restore {
            Some(a.restore_request(&a.checkpointed("pause-snap-1", 1).await))
        } else {
            None
        };
        let call = || async {
            match &restore_req {
                Some(r) => a.k().restore(&bg(), r.clone()).await.map(|_| ()),
                None => a.k().run(&bg(), a.run_request()).await.map(|_| ()),
            }
        };
        // The reset keeps the volumes dir instead of recreating it, so a
        // file there fails it.
        let volumes = a.paths().external_volume_mounts_dir(UID);
        std::fs::remove_dir(&volumes).unwrap();
        std::fs::write(&volumes, b"").unwrap();
        let err = call().await.unwrap_err();
        assert_eq!(err.code(), Code::Cancelled, "restore={restore}: {err:?}");
        std::fs::remove_file(&volumes).unwrap();
        call()
            .await
            .unwrap_or_else(|e| panic!("restore={restore}: retry: {e:?}"));
    }
}

/// Checkpoint refuses, as Aborted, a generation below the newest snapshot
/// or equal to it under another name, before the worker, the prune or any
/// state change.
#[tokio::test]
async fn checkpoint_rejects_stale_generation() {
    let a = run_plugin_actor(NodeOpts::default()).await;
    let want = a
        .checkpoint(&a.snapshot("pause-snap-2", 2), pb::UploadMode::Deferred)
        .await
        .unwrap()
        .state
        .unwrap();
    a.restore_local("pause-snap-2", 2).await;
    a.worker().actor_dirs.lock().remove("CheckpointWorkload");
    for stale in [a.snapshot("stale-snap-1", 1), a.snapshot("other-snap-2", 2)] {
        let err = a
            .checkpoint(&stale, pb::UploadMode::Deferred)
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::Aborted,
            "{}: {err:?}",
            stale.snapshot_uuid
        );
        assert!(
            a.worker().dirs("CheckpointWorkload").is_none(),
            "the worker was called"
        );
        assert!(exists(&a.paths().local_snapshot_dir(UID, "pause-snap-2")));
        assert_eq!(a.node.tracker().state(UID).await, want);
    }
}

/// PromoteSnapshot refuses, as Aborted, a stale generation before uploading,
/// pruning or changing state.
#[tokio::test]
async fn promote_snapshot_rejects_stale_generation() {
    let a = run_plugin_actor(NodeOpts::default()).await;
    let want = a
        .checkpoint(&a.snapshot("pause-snap-2", 2), pb::UploadMode::Deferred)
        .await
        .unwrap()
        .state
        .unwrap();
    for stale in [a.snapshot("stale-snap-1", 1), a.snapshot("other-snap-2", 2)] {
        write_local_snapshot(
            &a.paths().local_snapshot_dir(UID, &stale.snapshot_uuid),
            &SandboxAssetsRecord {
                sandbox_class: "gvisor".into(),
                snapshot_files: vec!["checkpoint.img".into()],
                fidelity: fidelity_label(pb::SnapshotFidelity::Memory as i32).into(),
                ..Default::default()
            },
            &[("checkpoint.img", "stale-memory")],
        );
        let err = a
            .k()
            .promote_snapshot(&bg(), a.promote_request(&stale))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::Aborted,
            "{}: {err:?}",
            stale.snapshot_uuid
        );
        assert!(
            a.node.objects.keys().is_empty(),
            "uploaded {:?}",
            a.node.objects.keys()
        );
        assert!(exists(&a.paths().local_snapshot_dir(UID, "pause-snap-2")));
        assert_eq!(a.node.tracker().state(UID).await, want);
    }
}

/// While generation 3 checkpoints, a Checkpoint and a PromoteSnapshot of
/// generation 2 wait on the actor's lock and then fail Aborted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_in_flight_rejects_older_generation() {
    let a = Arc::new(run_plugin_actor(NodeOpts::default()).await);
    let (started, release) = (Latch::new(), Latch::new());
    a.worker().set_on_checkpoint(block_once(&started, &release));
    let a2 = a.clone();
    let cp = tokio::spawn(async move {
        a2.checkpoint(&a2.snapshot("pause-snap-3", 3), pb::UploadMode::Deferred)
            .await
    });
    within(
        Duration::from_secs(10),
        "Checkpoint(pause-snap-3) to reach the worker",
        started.wait(),
    )
    .await;
    let stale = a.snapshot("pause-snap-2", 2);
    let (a2, s2) = (a.clone(), stale.clone());
    let mut stale_cp =
        tokio::spawn(async move { a2.checkpoint(&s2, pb::UploadMode::Deferred).await });
    let (a2, s2) = (a.clone(), stale.clone());
    let mut stale_promote = tokio::spawn(async move {
        a2.k()
            .promote_snapshot(&bg(), a2.promote_request(&s2))
            .await
    });
    assert_pending(&mut stale_cp, Duration::from_millis(50), "stale Checkpoint").await;
    assert_pending(
        &mut stale_promote,
        Duration::from_millis(1),
        "stale PromoteSnapshot",
    )
    .await;
    release.open();
    cp.await.unwrap().expect("Checkpoint(pause-snap-3)");
    assert_eq!(stale_cp.await.unwrap().unwrap_err().code(), Code::Aborted);
    assert_eq!(
        stale_promote.await.unwrap().unwrap_err().code(),
        Code::Aborted
    );
    assert_eq!(
        uuid(a.node.tracker().state(UID).await.latest_local.as_ref()),
        "pause-snap-3"
    );
}

/// A stalled Checkpoint of generation 2 reaching its commit after
/// generation 3 committed locally keeps generation 3, tracked and on disk,
/// and prunes its own copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_skips_stale_commit_and_keeps_newer_local() {
    let a = Arc::new(run_plugin_actor(NodeOpts::default()).await);
    let (started, release) = (Latch::new(), Latch::new());
    a.worker().set_on_checkpoint(block_once(&started, &release));
    let a2 = a.clone();
    let cp = tokio::spawn(async move {
        a2.checkpoint(&a2.snapshot("pause-snap-2", 2), pb::UploadMode::Deferred)
            .await
    });
    within(
        Duration::from_secs(10),
        "Checkpoint(pause-snap-2) to reach the worker",
        started.wait(),
    )
    .await;
    let snap3 = a.snapshot("pause-snap-3", 3);
    let dir3 = a.paths().local_snapshot_dir(UID, "pause-snap-3");
    write_local_snapshot(
        &dir3,
        &SandboxAssetsRecord {
            sandbox_class: "gvisor".into(),
            snapshot_files: vec!["checkpoint.img".into()],
            fidelity: fidelity_label(pb::SnapshotFidelity::Memory as i32).into(),
            ..Default::default()
        },
        &[("checkpoint.img", "newer-memory")],
    );
    let mut r = SnapshotRef::new(snap3, ActorRef::new(HIVE, ACTOR));
    r.commit_time = Some(chrono::Utc::now());
    let want = a.node.tracker().commit_local(UID, r).await;
    release.open();
    let st = cp
        .await
        .unwrap()
        .expect("Checkpoint(pause-snap-2)")
        .state
        .unwrap();
    assert_eq!(st, want, "the stale commit replaced pause-snap-3");
    assert!(exists(&dir3));
    assert!(!exists(&a.paths().local_snapshot_dir(UID, "pause-snap-2")));
}

/// The node's boot ID goes into the local manifest only, and a restart
/// keeps a local snapshot from the same boot and drops one from an earlier
/// boot.
#[tokio::test]
async fn checkpoint_manifest_boot_id() {
    let a = run_plugin_actor(NodeOpts {
        boot_id: "boot-1".into(),
        ..Default::default()
    })
    .await;
    let snap = a.snapshot("suspend-snap-1", 1);
    a.checkpoint(&snap, pb::UploadMode::Immediate)
        .await
        .expect("Checkpoint");
    let local_dir = a.paths().local_snapshot_dir(UID, "suspend-snap-1");
    let local =
        SandboxAssetsRecord::parse(&std::fs::read(local_dir.join(MANIFEST_NAME)).unwrap()).unwrap();
    assert_eq!(local.boot_id, "boot-1");
    let remote = std::fs::read_to_string(
        a.node
            .objects
            .manifest_path(&snap.object.as_ref().unwrap().snapshot_uri),
    )
    .expect("remote manifest");
    assert!(
        !remote.contains("bootId") && !remote.contains("boot-1"),
        "remote manifest {remote} carries the boot ID"
    );

    let restarted = |boot: &str| {
        a.node.new_tracker(Options {
            upload_workers: 0,
            detach_workers: 0,
            report_workers: 0,
            boot_check: Some(local_snapshot_boot_check(a.node.paths.clone(), boot.into())),
            ..Default::default()
        })
    };
    let same = restarted("boot-1");
    same.start().await;
    assert!(
        exists(&local_dir),
        "a restart on the same boot dropped the local snapshot"
    );
    assert_eq!(
        uuid(same.state(UID).await.latest_local.as_ref()),
        "suspend-snap-1"
    );

    let newer = restarted("boot-2");
    newer.start().await;
    assert!(
        !exists(&local_dir),
        "a restart on a new boot kept the local snapshot"
    );
    let st = newer.state(UID).await;
    assert!(st.latest_local.is_none(), "{st:?}");
    assert_eq!(uuid(st.latest_durable.as_ref()), "suspend-snap-1");
}

/// Once underway a Checkpoint no longer follows its caller: canceling the
/// caller neither aborts the worker checkpoint nor the commit, a longer
/// caller deadline outlives the checkpoint timeout, and a retry waits on
/// the lock and answers from the committed snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_finishes_when_caller_cancels() {
    for mode in [pb::UploadMode::Deferred, pb::UploadMode::Immediate] {
        let a = Arc::new(
            run_plugin_actor(NodeOpts {
                checkpoint_timeout: Duration::from_millis(10),
                ..Default::default()
            })
            .await,
        );
        let snap = a.snapshot("snap-1", 1);
        let calls = Arc::new(AtomicI32::new(0));
        let (started, release) = (Latch::new(), Latch::new());
        {
            let (calls, started, release) = (calls.clone(), started.clone(), release.clone());
            a.worker().set_on_checkpoint(hook(move || {
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                let (started, release) = (started.clone(), release.clone());
                async move {
                    if first {
                        started.open();
                        release.wait().await;
                    }
                }
            }));
        }
        // A caller deadline longer than the checkpoint timeout.
        let caller = Ctx::background().with_timeout(Duration::from_secs(60));
        let (a2, c2) = (a.clone(), caller.clone());
        let req = a.checkpoint_request(&snap, mode);
        let first = tokio::spawn(async move { a2.k().checkpoint(&c2, req).await });
        within(
            Duration::from_secs(10),
            "the Checkpoint to reach the worker",
            started.wait(),
        )
        .await;
        caller.cancel();
        let (a2, s2) = (a.clone(), snap.clone());
        let mut retry = tokio::spawn(async move { a2.checkpoint(&s2, mode).await });
        // Past the checkpoint timeout the retry still waits behind the
        // first call, which kept the caller's longer deadline.
        assert_pending(&mut retry, Duration::from_millis(25), "retried Checkpoint").await;
        release.open();
        for (name, h) in [("first", first), ("retry", retry)] {
            let st = h
                .await
                .unwrap()
                .unwrap_or_else(|e| panic!("{mode:?} {name} Checkpoint: {e:?}"))
                .state
                .unwrap();
            assert_eq!(
                snap_of(st.latest_local.as_ref()).as_ref(),
                Some(&snap),
                "{name}"
            );
            if mode == pb::UploadMode::Immediate {
                assert_eq!(
                    snap_of(st.latest_durable.as_ref()).as_ref(),
                    Some(&snap),
                    "{name}"
                );
            }
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "{mode:?}: worker checkpoints"
        );
    }

    // Bounded by the checkpoint timeout.
    let a = Arc::new(
        run_plugin_actor(NodeOpts {
            checkpoint_timeout: Duration::from_millis(100),
            ..Default::default()
        })
        .await,
    );
    let (started, release) = (Latch::new(), Latch::new());
    {
        let (started, release) = (started.clone(), release.clone());
        a.worker().set_on_checkpoint(hook(move || {
            let (started, release) = (started.clone(), release.clone());
            async move {
                started.open();
                release.wait().await;
            }
        }));
    }
    let a2 = a.clone();
    let cp = tokio::spawn(async move {
        a2.checkpoint(&a2.snapshot("snap-timeout", 1), pb::UploadMode::Deferred)
            .await
    });
    within(
        Duration::from_secs(10),
        "the Checkpoint to reach the worker",
        started.wait(),
    )
    .await;
    let err = within(Duration::from_secs(10), "the Checkpoint to time out", cp)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(err.code(), Code::DeadlineExceeded, "{err:?}");
    release.open();
    let _held = a
        .k()
        .lock_actor(&bg(), UID)
        .await
        .expect("the actor's lock after the timeout");
}

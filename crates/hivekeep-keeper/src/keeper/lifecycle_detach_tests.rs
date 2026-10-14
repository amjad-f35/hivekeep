//! DetachActor, detaches and background uploads as the lifecycle RPCs see
//! them.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use tokio::sync::mpsc;
use tonic::{Code, Status};

use super::lifecycle_harness::*;
use crate::context::Ctx;
use crate::snapshot_tracker::{Options, SnapshotRef, Store};

fn bg() -> Ctx {
    Ctx::background()
}

fn with_store(store: StoreKind, tracker: Options, start: bool) -> NodeOpts {
    NodeOpts {
        store,
        tracker,
        start_tracker: start,
        ..Default::default()
    }
}

fn transfer() -> NodeOpts {
    with_store(StoreKind::Transfer, Options::default(), false)
}

/// Once an actor restored from its local copy runs, it is not detached.
#[tokio::test]
async fn restored_actor_is_not_detached() {
    let a = run_plugin_actor(transfer()).await;
    a.checkpointed("pause-snap-1", 1).await;
    a.restore_local("pause-snap-1", 1).await;
    let err = a.node.tracker().detach(&bg(), UID).await.unwrap_err();
    assert_eq!(err.code(), Code::Aborted, "{err:?}");
}

/// A restore that fails after it began keeps the local snapshot and, as a
/// successful one, holds the detach off until the next checkpoint.
#[tokio::test]
async fn failed_restore_unschedules_detach() {
    let a = run_plugin_actor(transfer()).await;
    a.checkpointed("pause-snap-1", 1).await;
    let err = a
        .k()
        .restore(&bg(), a.restore_request(&a.snapshot("missing-snap", 2)))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err:?}");
    assert_eq!(
        uuid(a.node.tracker().state(UID).await.latest_local.as_ref()),
        "pause-snap-1"
    );
    let err = a.node.tracker().detach(&bg(), UID).await.unwrap_err();
    assert_eq!(err.code(), Code::Aborted, "{err:?}");
}

/// A suspended actor detached before the control plane recorded the upload
/// is restored from that upload.
#[tokio::test]
async fn restore_after_detach_reads_upload() {
    let a = run_plugin_actor(transfer()).await;
    let snap = a.checkpointed("pause-snap-1", 1).await;
    let st = a.node.tracker().detach(&bg(), UID).await.expect("Detach");
    assert!(st.latest_local.is_none(), "{st:?}");
    assert_eq!(uuid(st.latest_durable.as_ref()), "pause-snap-1");
    a.k()
        .restore(&bg(), a.restore_request(&snap))
        .await
        .expect("Restore of the detached snapshot");
    assert!(!a.worker().preserved(), "restored a local copy in place");
    assert_eq!(
        a.worker().restored("checkpoint.img").as_deref(),
        Some("guest-memory")
    );
}

/// DetachActor uploads a suspended actor's local snapshot, deletes the
/// local copy, returns the new state, and the actor's directory goes once
/// the report is acknowledged; a second call returns nothing.
#[tokio::test]
async fn detach_actor_detaches_paused_actor() {
    let a = run_plugin_actor(with_store(StoreKind::Transfer, reporting(), true)).await;
    a.checkpointed("pause-snap-1", 1).await;
    let local = a.paths().local_snapshot_dir(UID, "pause-snap-1");
    assert!(exists(&local));
    let st = a
        .k()
        .detach_actor(&bg(), a.detach_request())
        .await
        .expect("DetachActor")
        .state
        .unwrap();
    assert!(st.latest_local.is_none(), "{st:?}");
    assert_eq!(uuid(st.latest_durable.as_ref()), "pause-snap-1");
    assert!(!exists(&local));
    let actor_dir = a.paths().actor_dir(UID);
    wait_for("the actor dir to go after the detach report", || {
        !exists(&actor_dir)
    })
    .await;
    let st = a
        .k()
        .detach_actor(&bg(), a.detach_request())
        .await
        .expect("second DetachActor")
        .state
        .unwrap();
    assert_eq!(st, pb::ActorSnapshotState::default());
    a.node.stop_tracker().await;
}

/// DetachActor of an actor with no local snapshot does nothing.
#[tokio::test]
async fn detach_actor_without_local_snapshot() {
    let a = run_plugin_actor(transfer()).await;
    let st = a
        .k()
        .detach_actor(&bg(), a.detach_request())
        .await
        .expect("DetachActor")
        .state
        .unwrap();
    assert_eq!(st, pb::ActorSnapshotState::default());
}

/// DetachActor of an actor restored from its local snapshot is Aborted.
#[tokio::test]
async fn detach_actor_rejects_restored_actor() {
    let a = run_plugin_actor(transfer()).await;
    a.checkpointed("pause-snap-1", 1).await;
    a.restore_local("pause-snap-1", 1).await;
    let err = a
        .k()
        .detach_actor(&bg(), a.detach_request())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Aborted, "{err:?}");
}

/// DetachActor without an actor UID is invalid.
#[tokio::test]
async fn detach_actor_validates_request() {
    let a = run_plugin_actor(transfer()).await;
    let mut req = a.detach_request();
    req.actor_uid.clear();
    let err = a.k().detach_actor(&bg(), req).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
}

/// DetachActor waits on the actor's lock behind a Checkpoint, then detaches
/// what it committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detach_actor_waits_for_in_flight_checkpoint() {
    let a = Arc::new(run_plugin_actor(transfer()).await);
    let (started, release) = (Latch::new(), Latch::new());
    a.worker().set_on_checkpoint(block_once(&started, &release));
    let a2 = a.clone();
    let cp = tokio::spawn(async move {
        a2.checkpoint(&a2.snapshot("pause-snap-1", 1), pb::UploadMode::Deferred)
            .await
    });
    within(
        Duration::from_secs(10),
        "the Checkpoint to reach the worker",
        started.wait(),
    )
    .await;
    let a2 = a.clone();
    let mut detach =
        tokio::spawn(async move { a2.k().detach_actor(&bg(), a2.detach_request()).await });
    assert_pending(&mut detach, Duration::from_millis(50), "DetachActor").await;
    release.open();
    cp.await.unwrap().expect("Checkpoint");
    let st = detach.await.unwrap().expect("DetachActor").state.unwrap();
    assert!(st.latest_local.is_none(), "{st:?}");
    assert_eq!(uuid(st.latest_durable.as_ref()), "pause-snap-1");
}

/// A Run that names no runtime fails before it begins the start, so the
/// actor's detach stays scheduled.
#[tokio::test]
async fn invalid_run_keeps_detach_scheduled() {
    let a = run_plugin_actor(transfer()).await;
    a.checkpointed("pause-snap-1", 1).await;
    let mut req = a.run_request();
    req.sandbox_runtime = None;
    let err = a.k().run(&bg(), req).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
    a.node
        .tracker()
        .detach(&bg(), UID)
        .await
        .expect("Detach after the invalid Run");
}

/// An actor checkpointed locally as pause-snap-1 with a detach whose
/// upload blocks until the tracker stops.
struct BlockedDetach {
    a: PluginActor,
    detach_ctx: Ctx,
    detach: tokio::task::JoinHandle<()>,
}

impl BlockedDetach {
    async fn start() -> Self {
        let (tx, mut rx) = mpsc::channel(1);
        let store = StoreKind::Custom(Box::new(move |_, _| {
            Arc::new(BlockingStore { started: tx }) as Arc<dyn Store>
        }));
        let a = run_plugin_actor(with_store(store, Options::default(), false)).await;
        a.checkpointed("pause-snap-1", 1).await;
        let detach_ctx = bg();
        let (t, c) = (a.node.tracker().clone(), detach_ctx.clone());
        let detach = tokio::spawn(async move {
            let _ = t.detach(&c, UID).await;
        });
        within(
            Duration::from_secs(10),
            "the detach's upload to start",
            rx.recv(),
        )
        .await;
        BlockedDetach {
            a,
            detach_ctx,
            detach,
        }
    }

    /// The detach outlives its call until the tracker's context ends.
    async fn finish(self) {
        self.detach_ctx.cancel();
        self.detach.await.unwrap();
        self.a.node.stop_tracker().await;
    }
}

/// A Checkpoint given up while it waits for the actor's detach still
/// records its duration.
#[tokio::test]
async fn checkpoint_records_detach_wait() {
    let b = BlockedDetach::start().await;
    let a = &b.a;
    let before = a.node.checkpoint_samples("total");
    let gave_up = bg();
    gave_up.cancel();
    let err = a
        .k()
        .checkpoint(
            &gave_up,
            a.checkpoint_request(&a.snapshot("pause-snap-2", 2), pb::UploadMode::Deferred),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Cancelled, "{err:?}");
    assert!(
        a.node.checkpoint_samples("total") > before,
        "no total duration recorded"
    );
    b.finish().await;
}

/// A PromoteSnapshot given up while it waits for the actor's detach still
/// records its duration.
#[tokio::test]
async fn promote_snapshot_records_detach_wait() {
    let b = BlockedDetach::start().await;
    let a = &b.a;
    let before = a.node.checkpoint_samples("total");
    let gave_up = bg();
    gave_up.cancel();
    let err = a
        .k()
        .promote_snapshot(&gave_up, a.promote_request(&a.snapshot("pause-snap-1", 1)))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Cancelled, "{err:?}");
    assert!(
        a.node.checkpoint_samples("total") > before,
        "no total duration recorded"
    );
    b.finish().await;
}

/// A promote of a snapshot whose upload committed unrecorded, its local copy
/// gone since, records it durable without an upload time.
#[tokio::test]
async fn promote_snapshot_found_already_uploaded() {
    let a = run_plugin_actor(NodeOpts::default()).await;
    let snap = a.checkpointed("pause-snap-1", 1).await;
    a.node
        .transfer
        .upload_local_snapshot(
            &bg(),
            UID,
            &SnapshotRef::new(snap.clone(), ActorRef::new(HIVE, ACTOR)),
        )
        .await
        .expect("upload");
    prune_local_checkpoints(a.paths(), UID, &[]).unwrap();
    let st = a.promote(&snap).await;
    let d = st.latest_durable.as_ref().unwrap();
    assert_eq!(d.snapshot.as_ref(), Some(&snap));
    assert!(d.durable_complete_time.is_none(), "{d:?}");
}

/// After a detach removed the actor's directory, an upload of the snapshot
/// finds it in object storage, and a promote succeeds without uploading.
#[tokio::test]
async fn promote_snapshot_after_detach() {
    let a = run_plugin_actor(with_store(StoreKind::Transfer, reporting(), true)).await;
    let snap = a.checkpointed("pause-snap-1", 1).await;
    let st = a.node.tracker().detach(&bg(), UID).await.expect("Detach");
    assert!(
        st.latest_local.is_none() && st.latest_durable.is_some(),
        "{st:?}"
    );
    let actor_dir = a.paths().actor_dir(UID);
    wait_for("the actor dir to go after the detach report", || {
        !exists(&actor_dir)
    })
    .await;
    let uploaded = a
        .node
        .transfer
        .upload_local_snapshot(
            &bg(),
            UID,
            &SnapshotRef::new(snap.clone(), ActorRef::new(HIVE, ACTOR)),
        )
        .await
        .expect("upload after detach");
    assert!(!uploaded);
    let st = a.promote(&snap).await;
    let d = st.latest_durable.as_ref().unwrap();
    assert_eq!(d.snapshot.as_ref(), Some(&snap));
    assert!(d.durable_complete_time.is_none(), "{d:?}");
    a.node.stop_tracker().await;
}

/// How long background uploads wait in these tests.
const UPLOAD_DELAY: Duration = Duration::from_millis(300);

/// A promote while the snapshot's background upload runs waits for it and
/// does not upload a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_snapshot_waits_for_background_upload() {
    let (store, mut started, release) = gating(true, None);
    let a = Arc::new(run_plugin_actor(with_store(store, upload_after(UPLOAD_DELAY), true)).await);
    let snap = a.checkpointed("pause-snap-1", 1).await;
    within(
        Duration::from_secs(10),
        "the background upload to start",
        started.recv(),
    )
    .await;
    let (a2, s2) = (a.clone(), snap.clone());
    let mut promote = tokio::spawn(async move { a2.promote(&s2).await });
    assert_pending(&mut promote, Duration::from_millis(50), "PromoteSnapshot").await;
    release.open();
    let st = promote.await.unwrap();
    let d = st.latest_durable.as_ref().unwrap();
    assert_eq!(d.snapshot.as_ref(), Some(&snap));
    assert!(d.durable_complete_time.is_some(), "{d:?}");
    a.node.stop_tracker().await;
}

/// A local checkpoint of a resumed actor while its previous snapshot's
/// upload runs keeps that snapshot for the upload, which deletes it after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_keeps_snapshot_being_uploaded() {
    let (store, mut started, release) = gating(true, None);
    let a = run_plugin_actor(with_store(store, upload_after(UPLOAD_DELAY), true)).await;
    a.checkpointed("pause-snap-1", 1).await;
    within(
        Duration::from_secs(10),
        "the background upload to start",
        started.recv(),
    )
    .await;
    a.restore_local("pause-snap-1", 1).await;
    a.checkpointed("pause-snap-2", 2).await;
    let dir1 = a.paths().local_snapshot_dir(UID, "pause-snap-1");
    assert!(exists(&dir1), "pause-snap-1 deleted during its upload");
    release.open();
    wait_for("pause-snap-1 to go once its upload ends", || !exists(&dir1)).await;
    assert!(exists(&a.paths().local_snapshot_dir(UID, "pause-snap-2")));
    a.node.stop_tracker().await;
}

/// An immediate checkpoint of a resumed actor while its previous snapshot's
/// upload runs checkpoints the worker without waiting for that upload, then
/// cancels it and returns only once it stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immediate_checkpoint_cancels_background_upload() {
    let (ctx_tx, mut canceled) = mpsc::channel(1);
    let (store, mut started, release) = gating(false, Some(ctx_tx));
    let a = Arc::new(run_plugin_actor(with_store(store, upload_after(UPLOAD_DELAY), true)).await);
    a.checkpointed("pause-snap-1", 1).await;
    within(
        Duration::from_secs(10),
        "the background upload to start",
        started.recv(),
    )
    .await;
    a.restore_local("pause-snap-1", 1).await;
    a.worker().actor_dirs.lock().remove("CheckpointWorkload");
    let a2 = a.clone();
    let mut cp = tokio::spawn(async move {
        a2.checkpoint(&a2.snapshot("suspend-snap-2", 2), pb::UploadMode::Immediate)
            .await
    });
    tokio::select! {
        _ = canceled.recv() => {}
        r = &mut cp => panic!("immediate Checkpoint returned {r:?} without canceling the upload"),
        _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("timed out waiting for the cancel"),
    }
    assert!(
        a.worker().dirs("CheckpointWorkload").is_some(),
        "the upload was canceled before the worker checkpointed"
    );
    assert_pending(
        &mut cp,
        Duration::from_millis(50),
        "immediate Checkpoint before the upload stopped",
    )
    .await;
    release.open();
    let st = within(Duration::from_secs(10), "the immediate Checkpoint", cp)
        .await
        .unwrap()
        .expect("Checkpoint(suspend-snap-2)")
        .state
        .unwrap();
    assert_eq!(uuid(st.latest_durable.as_ref()), "suspend-snap-2");
    assert!(st.uploading.is_none(), "{st:?}");
    a.node.stop_tracker().await;
}

/// The remaining time until `t`.
fn until(t: &prost_types::Timestamp) -> Duration {
    let at = SystemTime::UNIX_EPOCH + Duration::new(t.seconds as u64, t.nanos as u32);
    at.duration_since(SystemTime::now()).unwrap_or_default()
}

/// A plugin actor checkpointed into pause-snap-1 with a deferred upload and
/// resumed from its local copy before the upload is due, with a function
/// that waits past the due time and reports whether the upload starts.
async fn resumed_with_pending_upload() -> (
    Arc<PluginActor>,
    Arc<tokio::sync::Mutex<mpsc::Receiver<()>>>,
    Latch,
    Arc<dyn Fn() -> futures::future::BoxFuture<'static, bool> + Send + Sync>,
) {
    let (store, started, release) = gating(false, None);
    let a = Arc::new(
        run_plugin_actor(with_store(
            store,
            upload_after(Duration::from_millis(1500)),
            true,
        ))
        .await,
    );
    let st = a
        .checkpoint(&a.snapshot("pause-snap-1", 1), pb::UploadMode::Deferred)
        .await
        .expect("Checkpoint(pause-snap-1)")
        .state
        .unwrap();
    let due = st.upload_due_time.expect("an upload is due");
    a.restore_local("pause-snap-1", 1).await;
    assert!(
        until(&due) > Duration::ZERO,
        "the restore took past the upload's due time"
    );
    let started = Arc::new(tokio::sync::Mutex::new(started));
    let s2 = started.clone();
    let step_past_due = Arc::new(move || {
        let (s, due) = (s2.clone(), due);
        futures::FutureExt::boxed(async move {
            tokio::time::sleep(until(&due)).await;
            let mut rx = s.lock().await;
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .is_ok()
        })
    });
    (a, started, release, step_past_due)
}

/// Under immediate upload a checkpoint holds the previous snapshot's due
/// upload off, since it would only cancel it; under deferred it starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immediate_checkpoint_holds_off_background_upload() {
    for (mode, name, want_started) in [
        (pb::UploadMode::Immediate, "suspend-snap-2", false),
        (pb::UploadMode::Deferred, "pause-snap-2", true),
    ] {
        let (a, _started, release, step) = resumed_with_pending_upload().await;
        let began = Arc::new(AtomicBool::new(false));
        let b2 = began.clone();
        a.worker().set_on_checkpoint(hook(move || {
            let (step, b2) = (step.clone(), b2.clone());
            async move { b2.store(step().await, Ordering::SeqCst) }
        }));
        a.checkpoint(&a.snapshot(name, 2), mode)
            .await
            .unwrap_or_else(|e| panic!("Checkpoint({name}): {e:?}"));
        assert_eq!(began.load(Ordering::SeqCst), want_started, "{mode:?}");
        release.open();
        a.node.stop_tracker().await;
    }
}

/// A Terminate holds a due background upload off while the worker tears
/// the actor down; when the teardown fails, Terminate fails before it
/// forgets the actor and the upload starts once it returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_holds_off_background_upload() {
    for teardown_err in [None, Some(Status::internal("teardown failed"))] {
        let (a, started, release, step) = resumed_with_pending_upload().await;
        let began = Arc::new(AtomicBool::new(false));
        let (b2, e2) = (began.clone(), teardown_err.clone());
        a.worker().set_on_terminate(fallible_hook(move || {
            let (step, b2, e2) = (step.clone(), b2.clone(), e2.clone());
            async move {
                b2.store(step().await, Ordering::SeqCst);
                match e2 {
                    Some(e) => Err(e),
                    None => Ok(()),
                }
            }
        }));
        let res = a.k().terminate(&bg(), a.terminate_request()).await;
        assert_eq!(res.is_err(), teardown_err.is_some(), "{res:?}");
        assert!(
            !began.load(Ordering::SeqCst),
            "the upload started during Terminate"
        );
        let mut rx = started.lock().await;
        if teardown_err.is_none() {
            assert!(
                rx.try_recv().is_err(),
                "the upload started during Terminate"
            );
        } else {
            within(
                Duration::from_secs(10),
                "the upload to start once Terminate failed",
                rx.recv(),
            )
            .await;
        }
        drop(rx);
        release.open();
        a.node.stop_tracker().await;
    }
}

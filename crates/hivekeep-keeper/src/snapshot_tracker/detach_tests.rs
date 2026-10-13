//! Detach tests: background and Detach calls.

use std::fs;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_proto::keeper::v1 as pb;
use tonic::{Code, Status};

use super::actor::Hold;
use super::op::OpKind;
use super::test_support_ops::*;
use super::*;
use crate::context::Ctx;

const DEFERRED: pb::UploadMode = pb::UploadMode::Deferred;

fn no_upload_workers() -> Options {
    Options {
        upload_workers: 0,
        ..Env::opts()
    }
}

// A Detach call makes the latest local snapshot durable and drops it.
#[tokio::test(start_paused = true)]
async fn detach_requested() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = env.started(&store).await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let st = t.detach(&Ctx::background(), UID).await.unwrap();
    assert_eq!(store.uploads(), vec!["s1"]);
    assert!(st.latest_local.is_none());
    let durable = st.latest_durable.unwrap();
    assert_eq!(
        durable.snapshot.as_ref().unwrap().fidelity,
        pb::SnapshotFidelity::Memory as i32
    );
    assert!(durable.durable_complete_time.is_some() && durable.local_commit_time.is_some());
    assert_eq!(
        detach_due_of(&t, UID).await,
        None,
        "no detach after a detach"
    );
}

// A detach of an actor with no local snapshot is a no-op.
#[tokio::test(start_paused = true)]
async fn detach_is_a_no_op_without_local_snapshot() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = env.started(&store).await;
    let st = t.detach(&Ctx::background(), UID).await.unwrap();
    assert_eq!(st, pb::ActorSnapshotState::default());
    assert!(store.uploads().is_empty());
}

// The timer detaches an actor, making its snapshot durable itself when no
// upload worker did.
#[tokio::test(start_paused = true)]
async fn detach_by_the_timer() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = env.tracker(dyn_store(&store), no_upload_workers());
    t.start().await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    detach_now(&t).await;
    assert_eq!(store.uploads(), vec!["s1"]);
    let st = t.state(UID).await;
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s1");
}

// A snapshot an immediate checkpoint made durable is detached without an
// upload, keeping the checkpoint's upload time.
#[tokio::test(start_paused = true)]
async fn detach_already_durable_after_immediate_checkpoint() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = env.started(&store).await;
    let local = test_ref("s1", 1, Some(now(&t)));
    let mut durable = local.clone();
    durable.upload_time = Some(add(local.commit_time.unwrap(), SECOND));
    t.commit_durable(UID, durable.clone()).await;
    commit_local(&t, local).await;
    detach_now(&t).await;
    assert!(store.uploads().is_empty());
    let st = t.state(UID).await;
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert_eq!(
        time_of(&st.latest_durable.unwrap().durable_complete_time),
        durable.upload_time
    );
}

// A promoted snapshot is detached without an upload, keeping the promote's
// upload time.
#[tokio::test(start_paused = true)]
async fn detach_already_durable_after_promote() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = env.started(&store).await;
    let local = test_ref("s1", 1, Some(now(&t)));
    commit_local(&t, local.clone()).await;
    let mut promoted = test_ref("s1", 1, None);
    promoted.upload_time = Some(add(local.commit_time.unwrap(), MINUTE));
    t.commit_promoted(UID, promoted.clone()).await;
    detach_now(&t).await;
    assert!(store.uploads().is_empty());
    let st = t.state(UID).await;
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert_eq!(
        time_of(&st.latest_durable.unwrap().durable_complete_time),
        promoted.upload_time
    );
}

// The timer detaches a snapshot a background upload made durable without
// uploading it again.
#[tokio::test(start_paused = true)]
async fn detach_by_the_timer_after_the_upload() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    upload_now(&t, &store, None).await;
    let upload_time = t
        .state(UID)
        .await
        .latest_durable
        .unwrap()
        .durable_complete_time;
    detach_now(&t).await;
    assert_eq!(store.uploads().len(), 1);
    let st = t.state(UID).await;
    assert!(st.latest_local.is_none());
    assert_eq!(
        st.latest_durable.unwrap().durable_complete_time,
        upload_time
    );
}

// A Detach call waits for the upload of its snapshot and uploads nothing
// more.
#[tokio::test(start_paused = true)]
async fn detach_waits_for_the_upload_of_its_snapshot() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    start_upload(&t, &store).await;
    let detached = start_detach(&t);
    store.release(None).await;
    join("the detach", detached).await.unwrap();
    assert_eq!(store.uploads().len(), 1);
    assert!(t
        .state(UID)
        .await
        .latest_durable
        .unwrap()
        .durable_complete_time
        .is_some());
}

// A Detach call waits for an older snapshot's upload, then uploads its own.
#[tokio::test(start_paused = true)]
async fn detach_waits_for_an_older_snapshots_upload() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    start_upload(&t, &store).await;
    commit_local(&t, test_ref("s2", 2, Some(now(&t)))).await;
    store.set_blocking(false);
    let detached = start_detach(&t);
    still_running("the detach during the upload of s1", &detached).await;
    store.release(None).await;
    join("the detach", detached).await.unwrap();
    assert_eq!(store.uploads(), vec!["s1", "s2"]);
    let st = t.state(UID).await;
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s2");
}

// A refused detach keeps the local snapshot and uploads nothing.
#[tokio::test(start_paused = true)]
async fn detach_refusals() {
    for case in [
        "refused after a start",
        "refused while a start is in progress",
        "refused when durable generation mismatches local",
        "refused when durable URI mismatches local",
        "refused without an object storage location",
    ] {
        let env = Env::new();
        let store = FakeStore::new();
        let t = env.started(&store).await;
        let mut local = test_ref("s1", 1, Some(now(&t)));
        match case {
            "refused when durable URI mismatches local" => local
                .snapshot
                .object
                .as_mut()
                .unwrap()
                .snapshot_uri
                .push_str("-other"),
            "refused without an object storage location" => local.snapshot.object = None,
            _ => {}
        }
        commit_local(&t, local).await;
        let mut held = None;
        let want = match case {
            "refused after a start" => {
                t.begin_start(&Ctx::background(), UID)
                    .await
                    .unwrap()
                    .end()
                    .await;
                Code::Aborted
            }
            "refused while a start is in progress" => {
                held = Some(t.begin_start(&Ctx::background(), UID).await.unwrap());
                Code::Aborted
            }
            "refused when durable generation mismatches local" => {
                t.commit_durable(UID, test_ref("s1", 1, Some(now(&t))))
                    .await;
                commit_local(&t, test_ref("s1", 2, Some(now(&t)))).await;
                Code::FailedPrecondition
            }
            "refused when durable URI mismatches local" => {
                t.commit_durable(UID, test_ref("s1", 1, Some(now(&t))))
                    .await;
                Code::FailedPrecondition
            }
            _ => Code::FailedPrecondition,
        };
        let err = t.detach(&Ctx::background(), UID).await.unwrap_err();
        assert_eq!(err.code(), want, "{case}: {err:?}");
        assert!(store.uploads().is_empty(), "{case}");
        assert_eq!(uuid_of(&t.state(UID).await.latest_local), "s1", "{case}");
        if let Some(end) = held {
            end.end().await;
        }
    }
}

// A local snapshot no detach can make durable never comes due; one that
// replaces it schedules a detach again.
#[tokio::test(start_paused = true)]
async fn skips_undetachable_snapshots() {
    for case in ["no object storage location", "durable elsewhere"] {
        let env = Env::new();
        let store = FakeStore::new();
        let t = env.started(&store).await;
        if case == "durable elsewhere" {
            t.commit_durable(UID, test_ref("s1", 1, Some(now(&t))))
                .await;
            let mut local = test_ref("s1", 1, Some(now(&t)));
            local
                .snapshot
                .object
                .as_mut()
                .unwrap()
                .snapshot_uri
                .push_str("-other");
            commit_local(&t, local).await;
        } else {
            let mut local = test_ref("s1", 1, Some(now(&t)));
            local.snapshot.object = None;
            commit_local(&t, local).await;
        }
        assert_eq!(detach_due_of(&t, UID).await, None, "{case}");
        commit_local(&t, test_ref("s2", 2, Some(now(&t)))).await;
        detach_now(&t).await;
        assert_eq!(store.uploads(), vec!["s2"], "{case}");
    }
}

// BeginStart unschedules the detach and saves that first; only the next
// checkpoint schedules one. A worker finding the detach due while a
// Checkpoint holds the actor skips it without moving it or queuing the
// actor again; the checkpoint's end queues it, and the detach runs then.
#[tokio::test(start_paused = true)]
async fn detach_timer() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = env.idle(&store);
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;

    let end_start = t.begin_start(&Ctx::background(), UID).await.unwrap();
    let fresh = env.tracker(None, Env::opts());
    assert_eq!(
        detach_due_of(&fresh, UID).await,
        None,
        "saved before the start"
    );
    end_start.end().await;
    assert_eq!(detach_due_of(&t, UID).await, None);

    commit_local(&t, test_ref("s2", 2, Some(now(&t)))).await;
    let due = detach_due_of(&t, UID).await.unwrap();
    let (_, end_ck) = t
        .begin_checkpoint(
            &Ctx::background(),
            UID,
            &test_ref("s3", 3, None).snapshot,
            DEFERRED,
        )
        .await
        .unwrap();
    step_to(&t, due).await;
    process_next(&t, OpKind::Detach).await;
    assert_eq!(detach_due_of(&t, UID).await, Some(due), "kept while held");
    assert_eq!(
        queue_len(&t, OpKind::Detach),
        0,
        "not queued until the hold ends"
    );
    assert_eq!(uuid_of(&t.state(UID).await.latest_local), "s2");

    end_ck.end().await;
    process_next(&t, OpKind::Detach).await;
    assert!(t.state(UID).await.latest_local.is_none());
    assert_eq!(store.uploads(), vec!["s2"]);
}

// A start that cannot save that it began fails before anything starts,
// leaving the detach scheduled and the actor not started.
#[tokio::test(start_paused = true)]
async fn start_fails_if_it_cannot_save() {
    let env = Env::new();
    let t = env.idle(&FakeStore::new());
    let state_file = env.paths.actor_snapshot_state_file(UID);
    // A directory where the state file goes fails every save, so the entry
    // stays in memory.
    fs::create_dir_all(&state_file).unwrap();
    let s1 = test_ref("s1", 1, Some(now(&t)));
    commit_local(&t, s1.clone()).await;
    let due = detach_due_of(&t, UID).await.expect("detach scheduled");

    let err = t.begin_start(&Ctx::background(), UID).await.err().unwrap();
    assert_eq!(err.code(), Code::Unavailable);
    assert_eq!(detach_due_of(&t, UID).await, Some(due));
    assert!(t.not_started_since(UID, &s1.snapshot).await);
    assert_eq!(entry(&t, UID).await.unwrap().hold, Hold::None);

    fs::remove_dir(&state_file).unwrap();
    t.begin_start(&Ctx::background(), UID)
        .await
        .unwrap()
        .end()
        .await;
    let fresh = env.tracker(None, Env::opts());
    assert_eq!(detach_due_of(&fresh, UID).await, None);
}

// A start or a checkpoint arriving during a detach waits for it.
#[tokio::test(start_paused = true)]
async fn waits_for_detach() {
    for checkpoint in [false, true] {
        let env = Env::new();
        let store = FakeStore::new();
        store.set_blocking(true);
        let t = env.started(&store).await;
        commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
        let detached = start_detach(&t);
        store.wait_started().await;

        let h = {
            let t = t.clone();
            tokio::spawn(async move {
                let ctx = Ctx::background();
                let end = if checkpoint {
                    let s2 = test_ref("s2", 2, None).snapshot;
                    t.begin_checkpoint(&ctx, UID, &s2, DEFERRED).await?.1
                } else {
                    t.begin_start(&ctx, UID).await?
                };
                end.end().await;
                Ok::<_, Status>(())
            })
        };
        still_running("the begin during a detach", &h).await;
        store.release(None).await;
        join("the detach", detached).await.unwrap();
        join("the begin", h).await.unwrap();
        assert!(t.state(UID).await.latest_local.is_none());
    }
}

// BeginCheckpoint and BeginPromote of a stale snapshot fail at once during
// a detach.
#[tokio::test(start_paused = true)]
async fn detach_wait_checks_generation() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    commit_local(&t, test_ref("s1", 2, Some(now(&t)))).await;
    let detached = start_detach(&t);
    store.wait_started().await;
    let stale = test_ref("s0", 1, None);
    let ctx = Ctx::background();
    let err = within(
        "BeginCheckpoint",
        t.begin_checkpoint(&ctx, UID, &stale.snapshot, DEFERRED),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(err.code(), Code::Aborted);
    let err = within("BeginPromote", t.begin_promote(&ctx, UID, &stale))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), Code::Aborted);
    store.release(None).await;
    join("the detach", detached).await.unwrap();
}

// A failed Detach call keeps the local snapshot; its upload is held back
// the retry delay, and the detach stays as scheduled.
#[tokio::test(start_paused = true)]
async fn failed_detach_requested() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_err(Some(boom()));
    let t = env.idle(&store);
    commit_local(&t, test_ref("s1", 1, Some(sub(now(&t), UPLOAD_DELAY)))).await;
    let due = detach_due_of(&t, UID).await;
    t.detach(&Ctx::background(), UID).await.unwrap_err();
    let retry_at = add(now(&t), RETRY_DELAY);
    let st = t.state(UID).await;
    assert_eq!(uuid_of(&st.latest_local), "s1");
    assert_eq!(time_of(&st.upload_due_time), Some(retry_at));
    assert_eq!(
        detach_due_of(&t, UID).await,
        due,
        "the retry delay is shorter"
    );
}

// A failed background detach is retried no sooner than the retry delay
// after it.
#[tokio::test(start_paused = true)]
async fn failed_detach_background() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_err(Some(boom()));
    let t = env.tracker(dyn_store(&store), no_upload_workers());
    t.start().await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let due = detach_due_of(&t, UID).await.unwrap();
    step_to(&t, due).await;
    let retry_at = add(due, RETRY_DELAY);
    eventually("the failed detach is retried later", || async {
        detach_due_of(&t, UID).await == Some(retry_at)
    })
    .await;
    assert_eq!(uuid_of(&t.state(UID).await.latest_local), "s1");
    store.set_err(None);
    detach_now(&t).await;
    assert!(t.state(UID).await.latest_local.is_none());
}

// A detach whose cleanup fails keeps LatestLocal and its files, records the
// snapshot durable, and is retried: the retry only cleans up.
#[tokio::test(start_paused = true)]
async fn detach_retries_failed_prune() {
    let env = Env::new();
    let store = FakeStore::new();
    let reporter = FakeReporter::new();
    let t = env.reporting(&store, &reporter).await;
    env.local_copy("s1");
    commit_local(&t, committed_ref("s1", 1, Some(now(&t)))).await;
    let still_mounted = env.still_mounted();
    t.detach(&Ctx::background(), UID).await.unwrap_err();
    let st = t.state(UID).await;
    assert_eq!(uuid_of(&st.latest_local), "s1");
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert!(env.local_exists("s1"));
    assert!(still_mounted.exists());
    assert!(
        !env.paths.oci_bundle_dir(UID).exists(),
        "no bundle dir made"
    );

    fs::remove_file(&still_mounted).unwrap();
    detach_now(&t).await;
    assert_eq!(store.uploads(), vec!["s1"], "the retry only cleans up");
    assert!(t.state(UID).await.latest_local.is_none());
    process_next_report(&t).await;
    assert!(!in_memory(&t).await);
    assert!(!env.paths.actor_dir(UID).exists());
    assert!(!env.paths.actor_snapshot_dir(UID).exists());
}

// A Detach call without a store fails rather than uploading.
#[tokio::test(start_paused = true)]
async fn detach_without_store() {
    let env = Env::new();
    let t = env.tracker(None, Env::opts());
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let err = t.detach(&Ctx::background(), UID).await.unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);
    assert_eq!(uuid_of(&t.state(UID).await.latest_local), "s1");
}

// A Checkpoint holds off detaches even when nothing was tracked of the
// actor as it began.
#[tokio::test(start_paused = true)]
async fn holds_first_checkpoint() {
    let env = Env::new();
    let t = env.idle(&FakeStore::new());
    let s1 = test_ref("s1", 1, Some(now(&t)));
    let (_, end) = t
        .begin_checkpoint(&Ctx::background(), UID, &s1.snapshot, DEFERRED)
        .await
        .unwrap();
    commit_local(&t, s1).await;
    step_to(&t, detach_due_of(&t, UID).await.unwrap()).await;
    if let Some(run) = t.inner.begin_due_work(UID, OpKind::Detach).await {
        run.await;
        panic!("a due detach began during the checkpoint");
    }
    end.end().await;
    let run = t
        .inner
        .begin_due_work(UID, OpKind::Detach)
        .await
        .expect("the due detach begins after the checkpoint");
    run.await;
}

// A start with no upload due, or a checkpoint replacing the local snapshot,
// lifts the backoff of failed detaches, so the entry leaves memory.
#[tokio::test(start_paused = true)]
async fn lifts_backoff_with_detach() {
    for case in ["start", "checkpoint"] {
        let env = Env::new();
        let store = FakeStore::new();
        let t = env.started(&store).await;
        let s1 = test_ref("s1", 1, Some(now(&t)));
        t.commit_durable(UID, s1.clone()).await;
        commit_local(&t, s1).await;
        env.still_mounted();
        t.detach(&Ctx::background(), UID).await.unwrap_err();
        assert!(in_memory(&t).await, "{case}: kept for its backoff");
        if case == "start" {
            t.begin_start(&Ctx::background(), UID)
                .await
                .unwrap()
                .end()
                .await;
        } else {
            commit_local(&t, test_ref("s2", 2, Some(now(&t)))).await;
        }
        assert!(!in_memory(&t).await, "{case}: dropped with no backoff left");
    }
}

// Forget cancels a detach in progress and waits for it, leaving the local
// snapshots for Terminate to prune.
#[tokio::test(start_paused = true)]
async fn forget_cancels_detach() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    store.set_ignore_cancel(true);
    let t = env.started(&store).await;
    env.local_copy("s1");
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let detached = start_detach(&t);
    store.wait_started().await;
    let h = {
        let t = t.clone();
        tokio::spawn(async move { t.forget(UID).await })
    };
    assert_eq!(store.wait_canceled().await, "s1");
    still_running("Forget before the canceled detach ended", &h).await;
    store
        .release(Some(Status::cancelled("context canceled")))
        .await;
    let err = join("the detach", detached).await.unwrap_err();
    assert_eq!(err.code(), Code::Cancelled, "{err:?}");
    join("Forget", h).await.unwrap();
    assert_eq!(t.state(UID).await, pb::ActorSnapshotState::default());
    assert!(env.local_exists("s1"), "left for Terminate to prune");
}

// A Detach call does not wait behind other actors' background work, and a
// worker taking the actor afterwards finds nothing to do.
#[tokio::test(start_paused = true)]
async fn foreground_detach_bypasses_queue() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.tracker(
        dyn_store(&store),
        Options {
            upload_workers: 1,
            detach_workers: 0,
            ..Env::opts()
        },
    );
    t.start().await;
    for id in ["actor-1", "actor-2", "actor-3"] {
        fs::create_dir_all(env.paths.actor_dir(id)).unwrap();
    }
    let taken = sub(now(&t), UPLOAD_DELAY);
    t.commit_local("actor-1", test_ref("a1-s1", 1, Some(taken)))
        .await;
    assert_eq!(store.wait_started().await, "a1-s1");
    t.commit_local("actor-2", test_ref("a2-s1", 1, Some(taken)))
        .await;

    store.set_blocking(false);
    let st = t.detach(&Ctx::background(), "actor-2").await.unwrap();
    assert_eq!(store.wait_started().await, "a2-s1");
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "a2-s1");

    let op1 = op_of(&t, "actor-1").await;
    store.release(None).await;
    wait_closed(&op1, "actor-1's upload").await;
    t.commit_local("actor-3", test_ref("a3-s1", 1, Some(taken)))
        .await;
    assert_eq!(store.wait_started().await, "a3-s1");
    assert_eq!(store.uploads(), vec!["a1-s1", "a2-s1", "a3-s1"]);
}

// A worker taking the actor before its detach is due leaves it and queues
// it again for when it is.
#[tokio::test(start_paused = true)]
async fn requeues_until_detach_is_due() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = env.idle(&store);
    let t0 = now(&t);
    async fn commit_immediate(t: &Tracker, r: SnapshotRef) {
        t.commit_durable(UID, r.clone()).await;
        t.commit_local(UID, r).await;
    }
    commit_immediate(&t, test_ref("s1", 1, Some(t0))).await;
    step_to(&t, add(t0, Duration::from_secs(3600))).await;
    commit_immediate(&t, test_ref("s2", 2, Some(now(&t)))).await;

    step_to(&t, add(t0, DETACH_DELAY)).await;
    process_next(&t, OpKind::Detach).await;
    assert_eq!(uuid_of(&t.state(UID).await.latest_local), "s2");
    assert!(store.uploads().is_empty(), "both snapshots are durable");

    step_to(&t, add(t0, Duration::from_secs(3600) + DETACH_DELAY)).await;
    process_next(&t, OpKind::Detach).await;
    assert!(t.state(UID).await.latest_local.is_none());
}

/// Starts a Detach under a context of its own and waits for its upload.
async fn detach_then_leave(
    t: &Tracker,
    store: &FakeStore,
) -> (
    Ctx,
    tokio::task::JoinHandle<Result<pb::ActorSnapshotState, Status>>,
) {
    let ctx = Ctx::background();
    let h = {
        let (t, ctx) = (t.clone(), ctx.clone());
        tokio::spawn(async move { t.detach(&ctx, UID).await })
    };
    store.wait_started().await;
    (ctx, h)
}

// A Detach whose caller leaves mid-upload returns the caller's error; the
// detach runs on to finish and report, and Wait covers it.
#[tokio::test(start_paused = true)]
async fn detach_outlives_caller() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.idle(&store);
    env.local_copy("s1");
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let (ctx, first) = detach_then_leave(&t, &store).await;
    let op = op_of(&t, UID).await;
    ctx.cancel();
    let err = join("Detach after its caller left", first)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Cancelled);

    let w = {
        let t = t.clone();
        tokio::spawn(async move {
            t.wait(&Ctx::background().with_timeout(Duration::from_millis(50)))
                .await
        })
    };
    settle().await;
    tokio::time::advance(Duration::from_millis(50)).await;
    assert_eq!(
        join("Wait", w).await,
        Err(crate::context::CtxError::DeadlineExceeded),
        "Wait covers the detach"
    );

    store.release(None).await;
    wait_closed(&op, "the detach its caller left").await;
    let st = t.state(UID).await;
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert!(!env.local_exists("s1"));
    assert!(report_due(&t).await);
    assert_eq!(store.uploads(), vec!["s1"]);
}

// A detach failing after its caller left logs the failure as a warning.
#[tokio::test(start_paused = true)]
async fn logs_detach_failing_after_caller_left() {
    let (_guard, failures) = record_log_levels("Snapshot operation failed");
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.idle(&store);
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let (ctx, first) = detach_then_leave(&t, &store).await;
    let op = op_of(&t, UID).await;
    ctx.cancel();
    assert_eq!(
        join("Detach", first).await.unwrap_err().code(),
        Code::Cancelled
    );
    store
        .release(Some(Status::unavailable("object store unavailable")))
        .await;
    wait_closed(&op, "the detach its caller left").await;
    assert_eq!(failures.get(), vec![tracing::Level::WARN]);
}

// Once Wait begins, Detach starts nothing.
#[tokio::test(start_paused = true)]
async fn detach_refused_once_wait_begins() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = env.idle(&store);
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    within("Wait", t.wait(&Ctx::background())).await.unwrap();
    let err = t.detach(&Ctx::background(), UID).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    assert!(store.uploads().is_empty());
}

fn delays_only() -> Options {
    Options {
        upload_delay: UPLOAD_DELAY,
        detach_delay: DETACH_DELAY,
        ..Options::default()
    }
}

// A detach the tracker's shutdown ends fails a live caller with Unavailable.
#[tokio::test(start_paused = true)]
async fn detach_stopped_by_shutdown_is_unavailable() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let ctx = Ctx::background();
    let t = env.tracker_ctx(ctx.clone(), dyn_store(&store), delays_only());
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let detached = start_detach(&t);
    store.wait_started().await;
    ctx.cancel();
    let err = join("the detach", detached).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
}

// A detach whose context ends as its upload succeeds records the upload but
// deletes nothing; the shutdown that ended it makes it Unavailable.
#[tokio::test(start_paused = true)]
async fn detach_does_not_prune_once_cancelled() {
    let env = Env::new();
    let ctx = Ctx::background();
    let store: Arc<dyn Store> = Arc::new(CancelingStore(ctx.clone()));
    let t = env.tracker_ctx(ctx, Some(store), delays_only());
    env.local_copy("s1");
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let err = t.detach(&Ctx::background(), UID).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    assert!(env.local_exists("s1"));
    let st = t.state(UID).await;
    assert_eq!(uuid_of(&st.latest_local), "s1");
    assert_eq!(uuid_of(&st.latest_durable), "s1");
}

// A detach's upload acts for the actor the snapshot was committed for,
// across a restart that reloads the state file.
#[tokio::test(start_paused = true)]
async fn detach_acts_for_the_actor_after_a_restart() {
    let env = Env::new();
    let store = FakeStore::new();
    let first = env.idle(&store);
    commit_local(&first, committed_ref("s1", 1, Some(now(&first)))).await;
    let t = env.started(&store).await;
    t.detach(&Ctx::background(), UID).await.unwrap();
    assert!(store.actor_of("s1") == Some(test_actor()));
}

// A promote keeps the actor on the local copy.
#[tokio::test(start_paused = true)]
async fn detach_actor_kept_by_a_promote() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = env.started(&store).await;
    commit_local(&t, committed_ref("s1", 1, Some(now(&t)))).await;
    t.commit_promoted(UID, committed_ref("s1", 1, None)).await;
    let a = entry(&t, UID).await.unwrap();
    assert!(a.rec.latest_local.unwrap().actor == test_actor());
}

fn populate_actor_dirs(env: &Env) {
    let assets = env.paths.sandbox_assets_dir(UID);
    fs::create_dir_all(&assets).unwrap();
    fs::write(assets.join("dummy-binary"), b"bin").unwrap();
    fs::write(
        env.paths.actor_sandbox_assets_file(UID),
        br#"{"version":1}"#,
    )
    .unwrap();
    crate::actordirs::reset_actor_dirs(&env.paths, UID).unwrap();
}

fn assert_cleaned_except_state(env: &Env) {
    assert!(!env.paths.actor_snapshot_dir(UID).exists());
    assert!(!env.paths.sandbox_assets_dir(UID).exists());
    assert!(!env.paths.actor_sandbox_assets_file(UID).exists());
    assert!(
        env.report_due_on_disk(),
        "state file kept with the report due"
    );
}

fn assert_actor_dir_cleaned_up(env: &Env) {
    assert!(!env.paths.actor_dir(UID).exists());
    assert!(!env.paths.actor_snapshot_dir(UID).exists());
    assert!(!env.paths.sandbox_assets_dir(UID).exists());
    assert!(!env.paths.actor_sandbox_assets_file(UID).exists());
    assert!(!env.paths.actor_snapshot_state_file(UID).exists());
}

// A Detach call removes the actor's files at once, keeps the state file
// while the report is due, and removes it and the directory once the report
// is acknowledged.
#[tokio::test(start_paused = true)]
async fn detach_cleans_up_actor_directory_on_detach_call() {
    let env = Env::new();
    let store = FakeStore::new();
    let reporter = FakeReporter::new();
    let t = env.reporting(&store, &reporter).await;
    populate_actor_dirs(&env);
    commit_local(&t, committed_ref("s1", 1, Some(now(&t)))).await;
    let st = t.detach(&Ctx::background(), UID).await.unwrap();
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert_cleaned_except_state(&env);
    assert!(!in_memory(&t).await);

    // A retried call before the acknowledgement still sees LatestDurable.
    let st = t.detach(&Ctx::background(), UID).await.unwrap();
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s1");

    process_next_report(&t).await;
    assert!(!in_memory(&t).await);
    assert_actor_dir_cleaned_up(&env);
}

// The timer's detach does the same.
#[tokio::test(start_paused = true)]
async fn detach_cleans_up_actor_directory_on_timer() {
    let env = Env::new();
    let store = FakeStore::new();
    let reporter = FakeReporter::new();
    let t = env.reporting(&store, &reporter).await;
    populate_actor_dirs(&env);
    commit_local(&t, committed_ref("s1", 1, Some(now(&t)))).await;
    detach_now(&t).await;
    assert_cleaned_except_state(&env);
    assert!(!in_memory(&t).await);
    process_next_report(&t).await;
    assert!(!in_memory(&t).await);
    assert_actor_dir_cleaned_up(&env);
}

// A background detach whose upload runs past the timeout fails and keeps
// the local snapshot to retry.
#[tokio::test(start_paused = true)]
async fn detach_upload_timeout_background() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.tracker(dyn_store(&store), no_upload_workers());
    t.start().await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let due = detach_due_of(&t, UID).await.unwrap();
    step_to(&t, due).await;
    store.wait_started().await;
    let op = op_of(&t, UID).await;
    step_to(&t, add(now(&t), UPLOAD_TIMEOUT)).await;
    wait_closed(&op, "the timed-out background detach").await;
    let retry_at = add(now(&t), RETRY_DELAY);
    assert_eq!(detach_due_of(&t, UID).await, Some(retry_at));
    assert_eq!(uuid_of(&t.state(UID).await.latest_local), "s1");
    store.set_blocking(false);
    detach_now(&t).await;
    assert!(t.state(UID).await.latest_local.is_none());
}

// A Detach call's upload is bounded by the upload timeout too.
#[tokio::test(start_paused = true)]
async fn detach_upload_timeout_foreground() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.idle(&store);
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let detached = start_detach(&t);
    store.wait_started().await;
    assert!(
        store.last_deadline_from_now().is_some(),
        "the upload has a deadline"
    );
    step_to(&t, add(now(&t), UPLOAD_TIMEOUT)).await;
    let err = join("the detach", detached).await.unwrap_err();
    assert_eq!(err.code(), Code::DeadlineExceeded, "{err:?}");
    assert_eq!(uuid_of(&t.state(UID).await.latest_local), "s1");
}

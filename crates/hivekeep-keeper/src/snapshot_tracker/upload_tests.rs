//! Background upload tests.

use std::fs;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_proto::keeper::v1 as pb;
use tonic::{Code, Status};

use super::actor::Hold;
use super::op::OpKind;
use super::test_support_ops::*;
use super::upload::upload_allowance;
use super::*;
use crate::context::Ctx;

const DEFERRED: pb::UploadMode = pb::UploadMode::Deferred;
const IMMEDIATE: pb::UploadMode = pb::UploadMode::Immediate;

// Deferred checkpoints keep their snapshots local, and the latest one is
// uploaded once the oldest undurable one has waited the upload delay. The
// local copy stays after the upload.
#[tokio::test(start_paused = true)]
async fn uploads_latest_local_snapshot() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    env.local_copy("s2");
    let t0 = now(&t);

    let st = commit_local(&t, test_ref("s1", 1, Some(t0))).await;
    assert_eq!(uuid_of(&st.latest_local), "s1");
    assert_eq!(
        time_of(&st.latest_local.as_ref().unwrap().local_commit_time),
        Some(t0)
    );
    assert_eq!(time_of(&st.upload_due_time), Some(add(t0, UPLOAD_DELAY)));

    let st = commit_local(&t, test_ref("s2", 2, Some(add(t0, 10 * MINUTE)))).await;
    assert_eq!(
        time_of(&st.upload_due_time),
        Some(add(t0, UPLOAD_DELAY)),
        "due when the first snapshot was"
    );

    upload_now(&t, &store, None).await;
    assert_eq!(store.uploads(), vec!["s2"]);
    assert!(env.local_exists("s2"), "the local copy is kept");
    let st = t.state(UID).await;
    assert_eq!(uuid_of(&st.latest_durable), "s2");
    assert!(st
        .latest_durable
        .as_ref()
        .unwrap()
        .durable_complete_time
        .is_some());
    assert_eq!(uuid_of(&st.latest_local), "s2");
    assert!(st.uploading.is_none() && st.upload_due_time.is_none());
}

// A deferred checkpoint during an older snapshot's upload keeps that
// snapshot until the upload ends; the upload's end deletes it and schedules
// the newer one an upload delay after the success.
#[tokio::test(start_paused = true)]
async fn checkpoint_during_upload() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    env.local_copy("s1");
    env.local_copy("s2");
    let t0 = now(&t);
    commit_local(&t, test_ref("s1", 1, Some(t0))).await;
    let op = start_upload(&t, &store).await;

    let st = commit_local(&t, test_ref("s2", 2, Some(now(&t)))).await;
    assert_eq!(t.local_snapshots_to_keep(UID).await, vec!["s2", "s1"]);
    assert_eq!(uuid_of(&st.uploading), "s1");
    assert_eq!(time_of(&st.upload_due_time), Some(add(t0, UPLOAD_DELAY)));

    step_to(&t, add(now(&t), 10 * MINUTE)).await;
    store.release(None).await;
    wait_closed(&op, "the upload of s1").await;
    assert!(!env.local_exists("s1"), "s1 is deleted after its upload");
    assert!(env.local_exists("s2"));
    let st = t.state(UID).await;
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert_eq!(
        time_of(&st.upload_due_time),
        Some(add(now(&t), UPLOAD_DELAY)),
        "an upload delay after the upload of s1 succeeded"
    );

    upload_now(&t, &store, None).await;
    assert_eq!(uuid_of(&t.state(UID).await.latest_durable), "s2");
}

// An immediate checkpoint makes the newest snapshot durable, drops the older
// local snapshot, and cancels an older upload in progress, returning only
// once it has stopped; the upload deletes the older local copy, though the
// checkpoint holds the actor.
#[tokio::test(start_paused = true)]
async fn immediate_checkpoint_during_upload() {
    for case in [
        "older upload canceled",
        "older upload finished before cancel",
        "older upload failed",
    ] {
        let env = Env::new();
        let store = FakeStore::new();
        store.set_blocking(true);
        store.set_ignore_cancel(true);
        let t = env.started(&store).await;
        env.local_copy("s1");
        let t0 = now(&t);
        commit_local(&t, test_ref("s1", 1, Some(t0))).await;
        let op = start_upload(&t, &store).await;

        let mut s2 = test_ref("s2", 2, Some(add(t0, MINUTE)));
        let (_, end) = t
            .begin_checkpoint(&Ctx::background(), UID, &s2.snapshot, IMMEDIATE)
            .await
            .unwrap();
        s2.upload_time = Some(add(s2.commit_time.unwrap(), SECOND));
        let h = {
            let (t, s2) = (t.clone(), s2.clone());
            tokio::spawn(async move { t.commit_durable(UID, s2).await })
        };
        assert_eq!(store.wait_canceled().await, "s1", "{case}");
        still_running("CommitDurable before the older upload stopped", &h).await;
        store
            .release(match case {
                "older upload canceled" => Some(Status::cancelled("context canceled")),
                "older upload failed" => Some(boom()),
                _ => None,
            })
            .await;
        let st = join("CommitDurable", h).await;
        wait_closed(&op, "the older upload").await;
        assert!(
            st.latest_local.is_none() && st.upload_due_time.is_none() && st.uploading.is_none(),
            "{case}: {st:?}"
        );
        assert!(t.local_snapshots_to_keep(UID).await.is_empty(), "{case}");
        assert_eq!(uuid_of(&st.latest_durable), "s2", "{case}");
        assert_eq!(
            time_of(&st.latest_durable.as_ref().unwrap().durable_complete_time),
            s2.upload_time,
            "{case}"
        );
        assert!(
            !env.local_exists("s1"),
            "{case}: s1 deleted by the time CommitDurable returned"
        );
        commit_local(&t, test_ref("s2", 2, s2.commit_time)).await;
        end.end().await;
        assert!(
            !in_memory(&t).await,
            "{case}: entry dropped once the checkpoint ended"
        );

        // The next deferred checkpoint is due a full delay after it was taken.
        let s3_time = now(&t);
        let st = commit_local(&t, test_ref("s3", 3, Some(s3_time))).await;
        assert_eq!(
            time_of(&st.upload_due_time),
            Some(add(s3_time, UPLOAD_DELAY)),
            "{case}"
        );
    }
}

// CommitPromoted cancels an older upload in progress and returns once it
// has stopped and deleted the older local copy.
#[tokio::test(start_paused = true)]
async fn commit_promoted_cancels_older_upload() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    store.set_ignore_cancel(true);
    let t = env.started(&store).await;
    env.local_copy("s1");
    env.local_copy("s2");
    let t0 = now(&t);
    commit_local(&t, test_ref("s1", 1, Some(t0))).await;
    let op = start_upload(&t, &store).await;
    let mut s2 = test_ref("s2", 2, Some(add(t0, MINUTE)));
    commit_local(&t, s2.clone()).await;
    let (durable, end) = t.begin_promote(&Ctx::background(), UID, &s2).await.unwrap();
    assert!(!durable);
    s2.upload_time = Some(add(s2.commit_time.unwrap(), SECOND));

    let h = {
        let t = t.clone();
        tokio::spawn(async move { t.commit_promoted(UID, s2).await })
    };
    assert_eq!(store.wait_canceled().await, "s1");
    still_running("CommitPromoted before the older upload stopped", &h).await;
    store
        .release(Some(Status::cancelled("context canceled")))
        .await;
    let st = join("CommitPromoted", h).await;
    wait_closed(&op, "the older upload").await;
    assert_eq!(uuid_of(&st.latest_durable), "s2");
    assert_eq!(uuid_of(&st.latest_local), "s2");
    assert!(st.uploading.is_none() && st.upload_due_time.is_none());
    assert!(!env.local_exists("s1"));
    end.end().await;
}

// A failed upload is retried once the retry delay has passed, doubling with
// each further failure.
#[tokio::test(start_paused = true)]
async fn retries_failed_upload() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    for delay in [RETRY_DELAY, 2 * RETRY_DELAY] {
        upload_now(&t, &store, Some(boom())).await;
        let st = t.state(UID).await;
        assert!(st.latest_durable.is_none());
        assert_eq!(time_of(&st.upload_due_time), Some(add(now(&t), delay)));
    }
    upload_now(&t, &store, None).await;
    assert_eq!(uuid_of(&t.state(UID).await.latest_durable), "s1");
}

// A failed upload leaves the actor undurable since its oldest undurable
// snapshot, which a checkpoint during the upload does not move.
#[tokio::test(start_paused = true)]
async fn failed_upload_keeps_undurable_since() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    let t0 = now(&t);
    commit_local(&t, test_ref("s1", 1, Some(t0))).await;
    commit_local(&t, test_ref("s2", 2, Some(add(t0, 50 * MINUTE)))).await;
    let op = start_upload(&t, &store).await;
    commit_local(&t, test_ref("s3", 3, Some(now(&t)))).await;
    store.release(Some(boom())).await;
    wait_closed(&op, "the upload of s2").await;
    assert_eq!(
        time_of(&t.state(UID).await.upload_due_time),
        Some(add(now(&t), RETRY_DELAY))
    );
}

// Forget cancels an upload in progress and waits for it to end before
// dropping the actor's state. The canceled upload deletes the superseded
// local copy, and its metric records a Canceled error type.
#[tokio::test(start_paused = true)]
async fn forget_cancels_upload() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    store.set_ignore_cancel(true);
    let (inst, reg) = test_instruments();
    let t = env.tracker(
        dyn_store(&store),
        Options {
            instruments: Some(inst),
            ..Env::opts()
        },
    );
    t.start().await;
    env.local_copy("s1");
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let op = start_upload(&t, &store).await;
    commit_local(&t, test_ref("s2", 2, Some(now(&t)))).await;

    let h = {
        let t = t.clone();
        tokio::spawn(async move { t.forget(UID).await })
    };
    assert_eq!(store.wait_canceled().await, "s1");
    still_running("Forget before the canceled upload ended", &h).await;
    store
        .release(Some(Status::cancelled("context canceled")))
        .await;
    join("Forget", h).await.unwrap();
    wait_closed(&op, "the canceled upload").await;
    assert_eq!(t.state(UID).await, pb::ActorSnapshotState::default());
    assert!(!env.paths.actor_snapshot_state_file(UID).exists());
    assert!(!env.local_exists("s1"), "the canceled upload deletes s1");
    assert_eq!(recorded_upload_error_types(&reg), vec!["Canceled"]);
}

// The upload workers bound how many uploads run at once, and an actor that
// checkpoints again while waiting uploads only its newest snapshot.
#[tokio::test(start_paused = true)]
async fn workers_bound_background_work() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.tracker(
        dyn_store(&store),
        Options {
            upload_workers: 2,
            detach_workers: 1,
            ..Env::opts()
        },
    );
    t.start().await;
    let taken = sub(now(&t), UPLOAD_DELAY);
    for id in ["actor-1", "actor-2", "actor-3"] {
        fs::create_dir_all(env.paths.actor_dir(id)).unwrap();
        t.commit_local(id, test_ref(&format!("{id}-s1"), 1, Some(taken)))
            .await;
    }
    let mut started = vec![store.wait_started().await, store.wait_started().await];
    started.sort();
    assert_eq!(started, vec!["actor-1-s1", "actor-2-s1"]);
    settle().await;
    assert_eq!(store.try_started(), None, "both upload workers are busy");
    let st = t.state("actor-3").await;
    assert!(st.uploading.is_none() && st.upload_due_time.is_some());

    t.commit_local(
        "actor-3",
        test_ref("actor-3-s2", 2, Some(add(taken, MINUTE))),
    )
    .await;
    let op1 = op_of(&t, "actor-1").await;
    let op2 = op_of(&t, "actor-2").await;
    store.release(None).await;
    assert_eq!(store.wait_started().await, "actor-3-s2");
    let op3 = op_of(&t, "actor-3").await;
    store.release(None).await;
    store.release(None).await;
    wait_closed(&op1, "actor-1's upload").await;
    wait_closed(&op2, "actor-2's upload").await;
    wait_closed(&op3, "actor-3's upload").await;
    assert_eq!(
        uuid_of(&t.state("actor-3").await.latest_durable),
        "actor-3-s2"
    );
    assert_eq!(store.uploads().len(), 3, "{:?}", store.uploads());
}

// A worker that takes an actor before its upload is due does nothing and
// queues it again for when it is.
#[tokio::test(start_paused = true)]
async fn requeues_until_work_is_due() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = env.idle(&store);
    let t0 = now(&t);
    commit_local(&t, test_ref("s1", 1, Some(t0))).await;
    t.commit_durable(UID, test_ref("s2", 2, Some(add(t0, 10 * MINUTE))))
        .await;
    commit_local(&t, test_ref("s3", 3, Some(add(t0, 20 * MINUTE)))).await;

    step_to(&t, add(t0, UPLOAD_DELAY)).await;
    process_next(&t, OpKind::Upload).await;
    assert!(store.uploads().is_empty());

    step_to(&t, add(t0, 20 * MINUTE + UPLOAD_DELAY)).await;
    process_next(&t, OpKind::Upload).await;
    assert_eq!(store.uploads(), vec!["s3"]);
}

// A tracker started after a restart uploads the newest snapshot its
// predecessor left due, timed from the oldest undurable commit.
#[tokio::test(start_paused = true)]
async fn resumes_upload_after_restart() {
    let env = Env::new();
    let store = FakeStore::new();
    let before = env.idle(&store);
    let t0 = sub(now(&before), UPLOAD_DELAY);
    commit_local(&before, test_ref("s1", 1, Some(t0))).await;
    commit_local(&before, test_ref("s2", 2, Some(add(t0, 50 * MINUTE)))).await;

    let after = env.idle(&store);
    assert_eq!(
        time_of(&after.state(UID).await.upload_due_time),
        Some(add(t0, UPLOAD_DELAY))
    );
    after.start().await;
    assert_eq!(store.wait_started().await, "s2");
}

// Without a store nothing is scheduled.
#[tokio::test(start_paused = true)]
async fn without_store_schedules_no_uploads() {
    let env = Env::new();
    let t = env.tracker(None, Env::opts());
    let taken = sub(now(&t), Duration::from_secs(3600));
    let st = commit_local(&t, test_ref("s1", 1, Some(taken))).await;
    assert!(st.upload_due_time.is_none());
    assert_eq!(queued_now(&t), 0);
    let mut g = t.inner.lock(UID).await;
    assert!(t.inner.find(&mut g));
    assert_eq!(t.inner.next_due(&g), None);
}

// A local snapshot no upload can make durable never comes due; one that
// replaces it schedules an upload again.
#[tokio::test(start_paused = true)]
async fn skips_unuploadable_snapshots() {
    for case in [
        "no object storage location",
        "durable as another generation",
    ] {
        let env = Env::new();
        let t = env.idle(&FakeStore::new());
        let taken = sub(now(&t), UPLOAD_DELAY);
        if case == "no object storage location" {
            let mut local = test_ref("s1", 1, Some(taken));
            local.snapshot.object = None;
            commit_local(&t, local).await;
        } else {
            t.commit_durable(UID, test_ref("s1", 1, Some(taken))).await;
            commit_local(&t, test_ref("s1", 2, Some(taken))).await;
        }
        assert!(t.state(UID).await.upload_due_time.is_none(), "{case}");
        assert_eq!(queued_now(&t), 0, "{case}");
        commit_local(&t, test_ref("s2", 3, Some(taken))).await;
        assert!(t.state(UID).await.upload_due_time.is_some(), "{case}");
    }
}

// A start unschedules the detach but keeps a failed upload's backoff.
#[tokio::test(start_paused = true)]
async fn start_keeps_upload_backoff() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    upload_now(&t, &store, Some(boom())).await;
    let retry_at = add(now(&t), RETRY_DELAY);

    t.begin_start(&Ctx::background(), UID)
        .await
        .unwrap()
        .end()
        .await;
    assert_eq!(time_of(&t.state(UID).await.upload_due_time), Some(retry_at));
    upload_now(&t, &store, Some(boom())).await;
    assert_eq!(
        time_of(&t.state(UID).await.upload_due_time),
        Some(add(retry_at, 2 * RETRY_DELAY)),
        "doubled"
    );
}

// BeginStart and BeginCheckpoint in either mode do not wait for a
// background upload.
#[tokio::test(start_paused = true)]
async fn start_and_checkpoint_do_not_wait_for_upload() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let op = start_upload(&t, &store).await;
    let ctx = Ctx::background();
    let s2 = test_ref("s2", 2, None).snapshot;

    let end = within("BeginStart during an upload", t.begin_start(&ctx, UID))
        .await
        .unwrap();
    end.end().await;
    for mode in [DEFERRED, IMMEDIATE] {
        let (_, end) = within(
            "BeginCheckpoint during an upload",
            t.begin_checkpoint(&ctx, UID, &s2, mode),
        )
        .await
        .unwrap();
        end.end().await;
    }
    store.release(None).await;
    wait_closed(&op, "the upload").await;
}

// An upload runs while a Run, Restore or deferred Checkpoint holds the actor.
#[tokio::test(start_paused = true)]
async fn upload_runs_while_actor_held() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    let t0 = now(&t);
    commit_local(&t, test_ref("s1", 1, Some(t0))).await;
    let (_, end) = t
        .begin_checkpoint(
            &Ctx::background(),
            UID,
            &test_ref("s2", 2, Some(t0)).snapshot,
            DEFERRED,
        )
        .await
        .unwrap();
    upload_now(&t, &store, None).await;
    assert_eq!(uuid_of(&t.state(UID).await.latest_durable), "s1");
    end.end().await;
}

fn spawn_promote(t: &Tracker, s: SnapshotRef) -> tokio::task::JoinHandle<Result<bool, Status>> {
    let t = t.clone();
    tokio::spawn(async move {
        let (durable, end) = t.begin_promote(&Ctx::background(), UID, &s).await?;
        end.end().await;
        Ok(durable)
    })
}

// BeginPromote waits for a detach in progress and an upload of the same
// snapshot, reports whether it is durable already, and refuses a durable
// snapshot of the same UUID that differs.
#[tokio::test(start_paused = true)]
async fn begin_promote_waits_for_detach() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    let s1 = test_ref("s1", 1, Some(now(&t)));
    commit_local(&t, s1.clone()).await;
    let detached = start_detach(&t);
    store.wait_started().await;
    let h = spawn_promote(&t, s1);
    still_running("BeginPromote during a detach", &h).await;
    store.release(None).await;
    join("the detach", detached).await.unwrap();
    assert!(join("BeginPromote", h).await.unwrap(), "durable");
}

#[tokio::test(start_paused = true)]
async fn begin_promote_waits_for_upload_of_same_snapshot() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    let s1 = test_ref("s1", 1, Some(now(&t)));
    commit_local(&t, s1.clone()).await;
    let op = start_upload(&t, &store).await;
    let h = spawn_promote(&t, s1);
    still_running("BeginPromote during an upload of its snapshot", &h).await;
    store.release(None).await;
    wait_closed(&op, "the upload").await;
    assert!(join("BeginPromote", h).await.unwrap(), "durable");
}

#[tokio::test(start_paused = true)]
async fn begin_promote_rejects_snapshot_superseded_during_upload() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    let s1 = test_ref("s1", 1, Some(now(&t)));
    commit_local(&t, s1.clone()).await;
    let op = start_upload(&t, &store).await;
    let h = spawn_promote(&t, s1);
    still_running("BeginPromote during an upload of its snapshot", &h).await;
    commit_local(&t, test_ref("s2", 2, Some(now(&t)))).await;
    store.release(None).await;
    wait_closed(&op, "the upload").await;
    assert_eq!(
        join("BeginPromote", h).await.unwrap_err().code(),
        Code::Aborted
    );
}

#[tokio::test(start_paused = true)]
async fn begin_promote_does_not_wait_for_upload_of_older_snapshot() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let op = start_upload(&t, &store).await;
    let s2 = test_ref("s2", 2, Some(now(&t)));
    commit_local(&t, s2.clone()).await;
    let (durable, end) = within(
        "BeginPromote of a newer snapshot during an older upload",
        t.begin_promote(&Ctx::background(), UID, &s2),
    )
    .await
    .unwrap();
    end.end().await;
    assert!(!durable);
    store.release(None).await;
    wait_closed(&op, "the upload").await;
}

#[tokio::test(start_paused = true)]
async fn begin_promote_refuses_mismatches() {
    type Mutate = fn(&mut pb::Snapshot);
    let cases: [(&str, Mutate); 3] = [
        ("generation", |s| s.generation = 2),
        ("fidelity", |s| {
            s.fidelity = pb::SnapshotFidelity::Volumes as i32
        }),
        ("URI", |s| {
            s.object.as_mut().unwrap().snapshot_uri.push_str("-other")
        }),
    ];
    for (name, mutate) in cases {
        let env = Env::new();
        let t = env.idle(&FakeStore::new());
        t.commit_durable(UID, test_ref("s1", 1, Some(now(&t))))
            .await;
        let mut req = test_ref("s1", 1, None);
        mutate(&mut req.snapshot);
        let err = t
            .begin_promote(&Ctx::background(), UID, &req)
            .await
            .err()
            .unwrap_or_else(|| panic!("{name} mismatch accepted"));
        assert_eq!(err.code(), Code::FailedPrecondition, "{name}");
    }
}

// When s1's upload fails after a checkpoint replaced LatestLocal with s2,
// which has no object storage location, s1 stays in Uploading and is
// retried; later local checkpoints keep it and its backoff, and its retry's
// success deletes its copy.
#[tokio::test(start_paused = true)]
async fn retries_uploading_when_latest_local_has_no_object() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    for s in ["s1", "s2", "s3"] {
        env.local_copy(s);
    }
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let op = start_upload(&t, &store).await;

    let mut s2 = test_ref("s2", 2, Some(now(&t)));
    s2.snapshot.object = None;
    commit_local(&t, s2).await;
    assert_eq!(t.local_snapshots_to_keep(UID).await, vec!["s2", "s1"]);

    store.release(Some(boom())).await;
    wait_closed(&op, "the failed upload of s1").await;
    let retry_at = add(now(&t), RETRY_DELAY);
    assert!(env.local_exists("s1"), "kept to retry");
    let st = t.state(UID).await;
    assert_eq!(uuid_of(&st.latest_local), "s2");
    assert_eq!(uuid_of(&st.uploading), "s1");
    assert_eq!(time_of(&st.upload_due_time), Some(retry_at));

    let mut s3 = test_ref("s3", 3, Some(now(&t)));
    s3.snapshot.object = None;
    let st = commit_local(&t, s3).await;
    assert_eq!(t.local_snapshots_to_keep(UID).await, vec!["s3", "s1"]);
    assert_eq!(uuid_of(&st.uploading), "s1");
    assert_eq!(time_of(&st.upload_due_time), Some(retry_at));

    upload_now(&t, &store, None).await;
    assert_eq!(store.uploads(), vec!["s1", "s1"]);
    assert!(!env.local_exists("s1"));
    let st = t.state(UID).await;
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert_eq!(uuid_of(&st.latest_local), "s3");
    assert!(st.uploading.is_none() && st.upload_due_time.is_none());
}

/// Commits s1 an upload delay ago and fails its upload once, leaving it in
/// Uploading to retry.
async fn fail_s1_upload(env: &Env, store: &Arc<FakeStore>) -> Tracker {
    store.set_err(Some(boom()));
    let t = env.idle(store);
    env.local_copy("s1");
    commit_local(&t, test_ref("s1", 1, Some(sub(now(&t), UPLOAD_DELAY)))).await;
    process_next(&t, OpKind::Upload).await;
    t
}

// A newer durable snapshot supersedes a failed Uploading s1: a worker clears
// it and deletes its copy without waiting for the retry. A promote of s1
// itself clears it too but keeps the copy, still LatestLocal.
#[tokio::test(start_paused = true)]
async fn clears_idle_uploading_when_superseded() {
    for (case, keep_s1) in [("CommitDurable", false), ("CommitPromoted of s1", true)] {
        let env = Env::new();
        let store = FakeStore::new();
        let t = fail_s1_upload(&env, &store).await;
        store.set_err(None);
        if keep_s1 {
            let mut s1 = test_ref("s1", 1, None);
            s1.upload_time = Some(now(&t));
            t.commit_promoted(UID, s1).await;
        } else {
            t.commit_durable(UID, test_ref("s2", 2, Some(now(&t))))
                .await;
        }
        assert_eq!(queue_len(&t, OpKind::Detach), 0, "{case}");
        process_next(&t, OpKind::Upload).await;
        let rec = env.read_state().expect("state file");
        assert!(rec.uploading.is_none(), "{case}: Uploading cleared");
        assert_eq!(env.local_exists("s1"), keep_s1, "{case}");
    }
}

// CommitLocal of a newer uploadable snapshot drops a failed Uploading in the
// same save, so a restart uploads the newer one.
#[tokio::test(start_paused = true)]
async fn commit_local_drops_failed_uploading() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = fail_s1_upload(&env, &store).await;
    commit_local(&t, test_ref("s2", 2, Some(now(&t)))).await;
    assert!(env.read_state().unwrap().uploading.is_none());
    assert_eq!(t.local_snapshots_to_keep(UID).await, vec!["s2"]);

    let store2 = FakeStore::new();
    let after = env.started(&store2).await;
    let _ = &after;
    assert_eq!(store2.wait_started().await, "s2");
}

// A superseded local copy stays while a Run or Restore is in progress.
#[tokio::test(start_paused = true)]
async fn keeps_superseded_copy_while_held_when_upload_ends() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    env.local_copy("s1");
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let op = start_upload(&t, &store).await;
    commit_local(&t, test_ref("s2", 2, Some(now(&t)))).await;
    let end = t.begin_start(&Ctx::background(), UID).await.unwrap();
    store.release(None).await;
    wait_closed(&op, "the upload of s1").await;
    end.end().await;
    assert!(env.local_exists("s1"), "kept during a Restore");
    assert_eq!(t.local_snapshots_to_keep(UID).await, vec!["s2"]);
}

#[tokio::test(start_paused = true)]
async fn keeps_superseded_copy_while_held_when_worker_clears_uploading() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = fail_s1_upload(&env, &store).await;
    t.commit_durable(UID, test_ref("s2", 2, Some(now(&t))))
        .await;
    let end = t.begin_start(&Ctx::background(), UID).await.unwrap();
    process_next(&t, OpKind::Upload).await;
    end.end().await;
    assert!(env.read_state().unwrap().uploading.is_none());
    assert!(env.local_exists("s1"), "kept during a Restore");
}

// While Terminate holds the actor no worker clears a superseded Uploading.
// A terminate that fails before Forget lets a worker clear it, deleting the
// copy under the actor's lock.
#[tokio::test(start_paused = true)]
async fn terminate_holds_off_superseded_uploading_cleanup() {
    for forget in [false, true] {
        let env = Env::new();
        let store = FakeStore::new();
        let t = fail_s1_upload(&env, &store).await;
        t.commit_durable(UID, test_ref("s2", 2, Some(now(&t))))
            .await;
        let end = t.begin_terminate(UID).await;
        process_next(&t, OpKind::Upload).await;
        let rec = env.read_state().unwrap();
        assert_eq!(rec.uploading.as_ref().map(|u| u.uuid()), Some("s1"));
        assert!(env.local_exists("s1"), "left for Terminate to prune");
        if forget {
            t.forget(UID).await.unwrap();
            end.end().await;
            assert_eq!(queued_now(&t), 0, "nothing queued for the forgotten actor");
            continue;
        }
        end.end().await;
        if let Some(run) = t.inner.begin_due_work(UID, OpKind::Upload).await {
            run.await;
            panic!("work left to run after releasing the lock, want s1 deleted under it");
        }
        assert!(env.read_state().unwrap().uploading.is_none());
        assert!(!env.local_exists("s1"));
    }
}

// Only a Run or Restore may read an older local copy in place.
#[test]
fn local_in_use() {
    for (hold, want) in [
        (Hold::None, false),
        (Hold::Start, true),
        (Hold::Checkpoint, false),
        (Hold::Durable, false),
        (Hold::Terminate, false),
    ] {
        let mut a = ActorSnapshots::new(1, UID, ActorRecord::default());
        a.hold = hold;
        assert_eq!(a.local_in_use(), want, "{hold:?}");
    }
}

/// A tracker on its own context with only the test delays, as a keeper
/// that later shuts down runs.
fn stoppable(env: &Env, store: &Arc<FakeStore>) -> (Tracker, Ctx) {
    let ctx = Ctx::background();
    let t = env.tracker_ctx(
        ctx.clone(),
        dyn_store(store),
        Options {
            upload_delay: UPLOAD_DELAY,
            detach_delay: DETACH_DELAY,
            ..Options::default()
        },
    );
    (t, ctx)
}

async fn stop(t: &Tracker, ctx: &Ctx) {
    ctx.cancel();
    within("the tracker to stop", t.wait(&Ctx::background()))
        .await
        .unwrap();
}

// Uploading is saved, so a restart mid-upload resumes it.
#[tokio::test(start_paused = true)]
async fn resumes_uploading_across_pause_checkpoint() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let (before, ctx) = stoppable(&env, &store);
    before.start().await;
    let t0 = sub(now(&before), UPLOAD_DELAY);
    commit_local(&before, test_ref("s1", 1, Some(t0))).await;
    assert_eq!(store.wait_started().await, "s1");
    let mut s2 = test_ref("s2", 2, Some(add(t0, MINUTE)));
    s2.snapshot.object = None;
    commit_local(&before, s2).await;
    stop(&before, &ctx).await;

    let store2 = FakeStore::new();
    let after = env.idle(&store2);
    assert_eq!(uuid_of(&after.state(UID).await.uploading), "s1");
    after.start().await;
    assert_eq!(store2.wait_started().await, "s1");
}

#[tokio::test(start_paused = true)]
async fn resumes_uploading_across_uploadable_local_checkpoint() {
    let env = Env::new();
    env.local_copy("s1");
    env.local_copy("s2");
    let store = FakeStore::new();
    store.set_blocking(true);
    let (before, ctx) = stoppable(&env, &store);
    before.start().await;
    let t0 = sub(now(&before), UPLOAD_DELAY);
    commit_local(&before, test_ref("s1", 1, Some(t0))).await;
    assert_eq!(store.wait_started().await, "s1");
    commit_local(&before, test_ref("s2", 2, Some(add(t0, MINUTE)))).await;
    stop(&before, &ctx).await;

    let store2 = FakeStore::new();
    store2.set_blocking(true);
    let after = env.idle(&store2);
    assert_eq!(uuid_of(&after.state(UID).await.uploading), "s1");
    after.start().await;
    assert_eq!(store2.wait_started().await, "s1", "s1 continues before s2");
    let op = op_of(&after, UID).await;
    store2.release(None).await;
    wait_closed(&op, "the resumed upload of s1").await;
    assert!(!env.local_exists("s1"));
    let st = after.state(UID).await;
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert!(st.uploading.is_none());
    assert_eq!(
        time_of(&st.upload_due_time),
        Some(add(now(&after), UPLOAD_DELAY))
    );
}

#[tokio::test(start_paused = true)]
async fn drops_uploading_after_restart_when_newer_snapshot_was_durable() {
    let env = Env::new();
    env.local_copy("s1");
    let store = FakeStore::new();
    store.set_blocking(true);
    store.set_ignore_cancel(true);
    let (before, ctx) = stoppable(&env, &store);
    before.start().await;
    let t0 = sub(now(&before), UPLOAD_DELAY);
    commit_local(&before, test_ref("s1", 1, Some(t0))).await;
    assert_eq!(store.wait_started().await, "s1");
    let uploading = env.read_state().unwrap().uploading;
    assert!(uploading.is_some());
    let h = {
        let t = before.clone();
        tokio::spawn(async move {
            t.commit_durable(UID, test_ref("s2", 2, Some(add(t0, MINUTE))))
                .await
        })
    };
    store.wait_canceled().await;
    // The state file as a hard crash mid-upload leaves it: CommitDurable
    // saved LatestDurable s2, and the upload of s1 has not yet cleared
    // Uploading.
    let state_file = env.paths.actor_snapshot_state_file(UID);
    let crashed = fs::read(&state_file).unwrap();
    let rec: ActorRecord = serde_json::from_slice(&crashed).unwrap();
    assert_eq!(rec.latest_durable.as_ref().map(|d| d.uuid()), Some("s2"));
    assert!(rec.uploading == uploading, "Uploading s1 still saved");
    store
        .release(Some(Status::cancelled("context canceled")))
        .await;
    join("CommitDurable", h).await;
    stop(&before, &ctx).await;
    fs::write(&state_file, crashed).unwrap();
    env.local_copy("s1");

    let store2 = FakeStore::new();
    let after = env.tracker(
        dyn_store(&store2),
        Options {
            upload_workers: 0,
            detach_workers: 0,
            ..Env::opts()
        },
    );
    assert!(after.state(UID).await.uploading.is_none(), "s2 is durable");
    after.start().await;
    assert_eq!(queue_len(&after, OpKind::Detach), 0);
    process_next(&after, OpKind::Upload).await;
    assert!(store2.uploads().is_empty());
    assert!(!env.local_exists("s1"), "startup cleanup deletes s1");
}

// An upload acts for the actor its snapshot was committed for, also when a
// restart resumes it from the state file.
#[tokio::test(start_paused = true)]
async fn upload_acts_for_the_actor() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    commit_local(&t, committed_ref("s1", 1, Some(now(&t)))).await;
    upload_now(&t, &store, None).await;
    assert!(store.actor_of("s1") == Some(test_actor()));
}

#[tokio::test(start_paused = true)]
async fn resumed_upload_acts_for_the_actor() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let (before, ctx) = stoppable(&env, &store);
    before.start().await;
    let t0 = sub(now(&before), UPLOAD_DELAY);
    commit_local(&before, committed_ref("s1", 1, Some(t0))).await;
    assert_eq!(store.wait_started().await, "s1");
    let mut s2 = test_ref("s2", 2, Some(add(t0, MINUTE)));
    s2.snapshot.object = None;
    commit_local(&before, s2).await;
    stop(&before, &ctx).await;

    let store2 = FakeStore::new();
    let after = env.started(&store2).await;
    let _ = &after;
    assert_eq!(store2.wait_started().await, "s1");
    assert!(store2.actor_of("s1") == Some(test_actor()));
}

// An upload due while PromoteSnapshot holds the actor waits for the promote,
// then skips if it made the snapshot durable or runs if it failed.
#[tokio::test(start_paused = true)]
async fn upload_waits_for_promote() {
    for succeeds in [true, false] {
        let env = Env::new();
        let store = FakeStore::new();
        let t = env.idle(&store);
        let mut s1 = test_ref("s1", 1, Some(now(&t)));
        commit_local(&t, s1.clone()).await;
        let (durable, end) = t.begin_promote(&Ctx::background(), UID, &s1).await.unwrap();
        assert!(!durable);
        step_to(&t, add(now(&t), UPLOAD_DELAY)).await;
        process_next(&t, OpKind::Upload).await;
        assert!(store.uploads().is_empty(), "no upload during the promote");
        if succeeds {
            s1.upload_time = Some(now(&t));
            t.commit_promoted(UID, s1).await;
            end.end().await;
            if queue_len(&t, OpKind::Upload) != 0 {
                process_next(&t, OpKind::Upload).await;
            }
            assert!(store.uploads().is_empty());
        } else {
            assert_eq!(
                queue_len(&t, OpKind::Upload),
                0,
                "the promote's end queues it"
            );
            end.end().await;
            process_next(&t, OpKind::Upload).await;
            assert_eq!(store.uploads(), vec!["s1"]);
        }
    }
}

// An upload due while an immediate Checkpoint holds the actor waits for it,
// then skips if it made a newer snapshot durable or runs if it failed.
#[tokio::test(start_paused = true)]
async fn upload_waits_for_immediate_checkpoint() {
    for commit in [true, false] {
        let env = Env::new();
        let store = FakeStore::new();
        let t = env.idle(&store);
        commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
        let mut s2 = test_ref("s2", 2, Some(now(&t)));
        let (_, end) = t
            .begin_checkpoint(&Ctx::background(), UID, &s2.snapshot, IMMEDIATE)
            .await
            .unwrap();
        step_to(&t, add(now(&t), UPLOAD_DELAY)).await;
        process_next(&t, OpKind::Upload).await;
        assert!(store.uploads().is_empty());
        assert_eq!(queue_len(&t, OpKind::Upload), 0);
        if commit {
            s2.upload_time = Some(now(&t));
            t.commit_durable(UID, s2).await;
        }
        end.end().await;
        if queue_len(&t, OpKind::Upload) != 0 {
            process_next(&t, OpKind::Upload).await;
        }
        let want: Vec<String> = if commit { vec![] } else { vec!["s1".into()] };
        assert_eq!(store.uploads(), want, "commit = {commit}");
    }
}

// An out-of-order CommitLocal of an older generation does not replace a
// failed Uploading, reset its backoff, or delete its copy afterwards.
#[tokio::test(start_paused = true)]
async fn older_local_does_not_supersede_uploading() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    env.local_copy("s2");
    let t0 = now(&t);
    commit_local(&t, test_ref("s2", 2, Some(t0))).await;
    upload_now(&t, &store, Some(boom())).await;
    let retry_at = add(now(&t), RETRY_DELAY);
    let st = commit_local(&t, test_ref("s1", 1, Some(add(t0, MINUTE)))).await;
    assert_eq!(uuid_of(&st.uploading), "s2");
    assert_eq!(time_of(&st.upload_due_time), Some(retry_at));

    upload_now(&t, &store, None).await;
    assert_eq!(store.uploads(), vec!["s2", "s2"]);
    assert!(env.local_exists("s2"));
}

// An upload past its timeout is canceled, kept in Uploading, and retried.
#[tokio::test(start_paused = true)]
async fn background_upload_timeout() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.started(&store).await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let op = start_upload(&t, &store).await;
    step_to(&t, add(now(&t), UPLOAD_TIMEOUT)).await;
    wait_closed(&op, "the timed-out upload").await;
    let st = t.state(UID).await;
    assert!(st.latest_durable.is_none());
    assert_eq!(uuid_of(&st.uploading), "s1");
    assert_eq!(
        time_of(&st.upload_due_time),
        Some(add(now(&t), RETRY_DELAY))
    );
    upload_now(&t, &store, None).await;
    assert_eq!(uuid_of(&t.state(UID).await.latest_durable), "s1");
}

/// Writes a local copy of s1 one throughput-second large and returns how
/// long its upload may run.
fn write_local_copy(env: &Env) -> Duration {
    write_dense_file(
        &env.paths.local_snapshot_dir(UID, "s1").join("pages.img"),
        UPLOAD_EST_THROUGHPUT as usize,
    );
    let (size, err) = local_snapshot_size(&env.paths, UID, "s1");
    assert!(err.is_none(), "{err:?}");
    let scaled = UPLOAD_TIMEOUT + upload_allowance(size, UPLOAD_EST_THROUGHPUT);
    assert!(
        scaled >= UPLOAD_TIMEOUT + SECOND,
        "{size} bytes may upload for {scaled:?}"
    );
    scaled
}

// An upload, and the one a background detach runs first, may run the upload
// timeout plus the local copy's time at the expected throughput.
#[tokio::test(start_paused = true)]
async fn upload_timeout_scales_with_snapshot_size() {
    // A background upload.
    {
        let env = Env::new();
        let store = FakeStore::new();
        store.set_blocking(true);
        let t = env.started(&store).await;
        let scaled = write_local_copy(&env);
        commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
        let op = start_upload(&t, &store).await;
        assert_eq!(store.last_deadline_from_now(), Some(scaled));
        step_to(&t, add(now(&t), scaled)).await;
        wait_closed(&op, "the timed-out upload").await;
    }
    // A background detach.
    {
        let env = Env::new();
        let store = FakeStore::new();
        store.set_blocking(true);
        let t = env.tracker(
            dyn_store(&store),
            Options {
                upload_workers: 0,
                ..Env::opts()
            },
        );
        t.start().await;
        let scaled = write_local_copy(&env);
        commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
        step_to(&t, detach_due_of(&t, UID).await.unwrap()).await;
        store.wait_started().await;
        assert_eq!(store.last_deadline_from_now(), Some(scaled));
    }
    // No local copy.
    {
        let env = Env::new();
        let store = FakeStore::new();
        store.set_blocking(true);
        let t = env.started(&store).await;
        commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
        start_upload(&t, &store).await;
        assert_eq!(store.last_deadline_from_now(), Some(UPLOAD_TIMEOUT));
    }
    // A throughput of zero adds nothing.
    {
        let env = Env::new();
        let store = FakeStore::new();
        store.set_blocking(true);
        let t = env.tracker(
            dyn_store(&store),
            Options {
                upload_est_throughput: 0,
                ..Env::opts()
            },
        );
        t.start().await;
        write_local_copy(&env);
        commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
        start_upload(&t, &store).await;
        assert_eq!(store.last_deadline_from_now(), Some(UPLOAD_TIMEOUT));
    }
}

// Each consecutive failure doubles the next attempt's timeout, to at most
// eight times the first's; a success starts over.
#[tokio::test(start_paused = true)]
async fn upload_timeout_grows_with_failures() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.tracker(
        dyn_store(&store),
        Options {
            detach_workers: 0,
            ..Env::opts()
        },
    );
    t.start().await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    for (attempt, factor) in [1u32, 2, 4, 8, 8].into_iter().enumerate() {
        let op = start_upload(&t, &store).await;
        let want = factor * UPLOAD_TIMEOUT;
        assert_eq!(
            store.last_deadline_from_now(),
            Some(want),
            "attempt {}",
            attempt + 1
        );
        step_to(&t, add(now(&t), want)).await;
        wait_closed(&op, "the timed-out upload").await;
    }
    upload_now(&t, &store, None).await;
    commit_local(&t, test_ref("s2", 2, Some(now(&t)))).await;
    start_upload(&t, &store).await;
    assert_eq!(store.last_deadline_from_now(), Some(UPLOAD_TIMEOUT));
}

// A timeout too long to double saturates, and an upload under it still runs.
#[tokio::test(start_paused = true)]
async fn upload_timeout_for_saturates() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let t = env.tracker(
        dyn_store(&store),
        Options {
            upload_timeout: Duration::MAX,
            ..Env::opts()
        },
    );
    for _ in 0..MAX_UPLOAD_TIMEOUT_DOUBLINGS {
        t.inner.retries.when(UID);
    }
    let got = t
        .inner
        .upload_timeout_for(UID, &test_ref("s1", 1, Some(now(&t))));
    assert!(got >= Duration::MAX / 2, "{got:?}");
    t.inner.retries.forget(UID);

    // An upload bounded by it neither panics nor times out at once.
    t.start().await;
    commit_local(&t, test_ref("s1", 1, Some(now(&t)))).await;
    let op = start_upload(&t, &store).await;
    settle().await;
    assert!(!op.ctx.is_done(), "the upload timed out at once");
    store.release(None).await;
    wait_closed(&op, "the upload").await;
    assert_eq!(uuid_of(&t.state(UID).await.latest_durable), "s1");
}

// The allowance rounds up to a whole second, and is zero for an empty
// snapshot or a throughput of zero or less.
#[test]
fn upload_allowance_table() {
    const MIB: i64 = 1 << 20;
    for (size, bps, want) in [
        (0, 100 * MIB, 0),
        (100 * MIB, 0, 0),
        (100 * MIB, -1, 0),
        (1, 100 * MIB, 1),
        (100 * MIB, 100 * MIB, 1),
        (100 * MIB + 1, 100 * MIB, 2),
        // 102.4 seconds.
        (10 << 30, 100 * MIB, 103),
        // Too large to scale to nanoseconds before dividing.
        (1 << 40, MIB, MIB as u64),
    ] {
        assert_eq!(
            upload_allowance(size, bps),
            Duration::from_secs(want),
            "upload_allowance({size}, {bps})"
        );
    }
}

//! Report tests: what background changes report, retries, backoff, give-up,
//! restarts, and holds.
//!
//! A report attempt is bounded by a timeout on the same paused clock the
//! tests step, so tests that step time while a report is in flight keep
//! each step shorter than that timeout.

use std::fs;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_proto::keeper::v1 as pb;
use tonic::{Code, Status};
use tracing::Level;

use super::lifecycle::HoldEnd;
use super::op::OpKind;
use super::report::{
    MAX_REPORT_QUEUE_RETRY_DELAY, MAX_REPORT_RETRY_DELAY, REPORT_RETRY_DELAY,
    REPORT_STARTUP_JITTER, REPORT_TIMEOUT,
};
use super::test_support_ops::*;
use super::*;
use crate::context::Ctx;

const DEFERRED: pb::UploadMode = pb::UploadMode::Deferred;
const IMMEDIATE: pb::UploadMode = pb::UploadMode::Immediate;
const FAILED: &str = "Failed to report the actor's snapshot state";
const REFUSED: &str = "The control plane refused the actor's snapshot state";
const GAVE_UP: &str = "Giving up on reporting the actor's snapshot state";

/// Options reporting through `reporter` with one report worker.
fn worker_opts(reporter: &Arc<FakeReporter>) -> Options {
    Options {
        report_workers: 1,
        ..reporting_opts(reporter)
    }
}

/// Commits s1 and uploads it in the background, making a report due.
async fn upload_s1(t: &Tracker, store: &FakeStore) {
    commit_local(t, committed_ref("s1", 1, Some(now(t)))).await;
    upload_now(t, store, None).await;
}

// A background upload reports the actor's whole state for the actor its
// commit named; once acknowledged, nothing is due in memory or on disk.
#[tokio::test(start_paused = true)]
async fn reports_background_upload() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let reporter = FakeReporter::new();
    let t = env.reporting(&store, &reporter).await;
    upload_s1(&t, &store).await;
    assert!(env.report_due_on_disk());

    process_next_report(&t).await;
    let sent = reporter.reports();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].actor_uid, UID);
    assert!(sent[0].actor == test_actor());
    let st = &sent[0].state;
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert_eq!(uuid_of(&st.latest_local), "s1");
    assert_eq!(*st, t.state(UID).await);
    assert!(!report_due(&t).await && !env.report_due_on_disk());
    assert_eq!(report_queue_len(&t), 0);
}

fn assert_detached(s: &Sent) {
    assert!(s.state.latest_local.is_none() && s.state.uploading.is_none());
    assert_eq!(uuid_of(&s.state.latest_durable), "s1");
    assert!(s.actor == test_actor());
}

// A background detach reports once its local snapshots are gone.
#[tokio::test(start_paused = true)]
async fn reports_background_detach_that_uploads() {
    let env = Env::new();
    let store = FakeStore::new();
    let reporter = FakeReporter::new();
    let t = env.reporting(&store, &reporter).await;
    commit_local(&t, committed_ref("s1", 1, Some(now(&t)))).await;
    detach_now(&t).await;
    process_next_report(&t).await;
    let sent = reporter.reports();
    assert_eq!(sent.len(), 1);
    assert_detached(&sent[0]);
    assert!(!report_due(&t).await);
    assert_eq!(report_queue_len(&t), 0);
}

#[tokio::test(start_paused = true)]
async fn reports_background_detach_after_upload() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let reporter = FakeReporter::new();
    let t = env.reporting(&store, &reporter).await;
    upload_s1(&t, &store).await;
    process_next_report(&t).await;
    detach_now(&t).await;
    process_next_report(&t).await;
    let sent = reporter.reports();
    assert_eq!(sent.len(), 2, "one for the upload, one for the detach");
    assert_detached(&sent[1]);
}

// A Detach call makes a report due and keeps the state file until it is
// acknowledged or refused with NotFound; otherwise the state file stays so
// a later Detach still returns LatestDurable.
#[tokio::test(start_paused = true)]
async fn reports_detach_call() {
    let cases: Vec<(&str, Option<Status>, bool, bool)> = vec![
        ("acknowledged", None, false, true),
        (
            "refused with NotFound",
            Some(Status::not_found("actor gone")),
            false,
            true,
        ),
        (
            "refused with Unimplemented",
            Some(Status::unimplemented("older control plane")),
            false,
            false,
        ),
        (
            "refused with FailedPrecondition",
            Some(Status::failed_precondition("conflict")),
            false,
            false,
        ),
        (
            "gives up after timeout",
            Some(Status::unavailable("control plane down")),
            true,
            false,
        ),
    ];
    for (name, err, give_up, want_forget) in cases {
        let env = Env::new();
        let store = FakeStore::new();
        let reporter = FakeReporter::new();
        if let Some(e) = &err {
            let n = if give_up { 2 } else { 1 };
            reporter.fail_next(std::iter::repeat_n(e.clone(), n));
        }
        let t = env.reporting(&store, &reporter).await;
        commit_local(&t, committed_ref("s1", 1, Some(now(&t)))).await;
        let due_at = now(&t);
        t.detach(&Ctx::background(), UID).await.unwrap();
        assert!(report_due(&t).await && env.report_due_on_disk(), "{name}");
        assert_eq!(report_queue_len(&t), 1, "{name}");

        // A repeat while the report is pending returns LatestDurable and
        // leaves it due.
        let st = t.detach(&Ctx::background(), UID).await.unwrap();
        assert!(st.latest_local.is_none(), "{name}");
        assert_eq!(uuid_of(&st.latest_durable), "s1", "{name}");
        assert!(report_due(&t).await && env.report_due_on_disk(), "{name}");

        process_next_report(&t).await;
        if give_up {
            step_to(&t, add(due_at, DEFAULT_REPORT_GIVE_UP_AFTER)).await;
            process_next_report(&t).await;
        }
        assert!(!report_due(&t).await && !env.report_due_on_disk(), "{name}");
        if !want_forget {
            assert!(env.paths.actor_snapshot_state_file(UID).exists(), "{name}");
        }
        let st = t.detach(&Ctx::background(), UID).await.unwrap();
        if want_forget {
            assert!(!env.paths.actor_dir(UID).exists(), "{name}");
            assert_eq!(st, pb::ActorSnapshotState::default(), "{name}");
        } else {
            assert!(st.latest_local.is_none(), "{name}");
            assert_eq!(uuid_of(&st.latest_durable), "s1", "{name}");
            assert!(
                report_due(&t).await && env.report_due_on_disk(),
                "{name}: requeued"
            );
            process_next_report(&t).await;
            assert!(!env.paths.actor_dir(UID).exists(), "{name}");
        }
    }
}

// An actor started while its detach report is due keeps its state file.
#[tokio::test(start_paused = true)]
async fn reports_detach_call_started_while_due() {
    let env = Env::new();
    let store = FakeStore::new();
    let reporter = FakeReporter::new();
    let t = env.reporting(&store, &reporter).await;
    let s1 = committed_ref("s1", 1, Some(now(&t)));
    commit_local(&t, s1.clone()).await;
    t.detach(&Ctx::background(), UID).await.unwrap();
    t.begin_start(&Ctx::background(), UID)
        .await
        .unwrap()
        .end()
        .await;
    process_next_report(&t).await;
    assert!(!report_due(&t).await && !env.report_due_on_disk());
    assert!(env.paths.actor_snapshot_state_file(UID).exists());
    assert_eq!(uuid_of(&t.state(UID).await.latest_durable), "s1");
    let fresh = env.tracker(None, Env::opts());
    assert!(!fresh.not_started_since(UID, &s1.snapshot).await);
}

// A Detach call whose cleanup fails still reports its upload.
#[tokio::test(start_paused = true)]
async fn reports_detach_call_upload_when_prune_fails() {
    let env = Env::new();
    let store = FakeStore::new();
    let reporter = FakeReporter::new();
    let t = env.reporting(&store, &reporter).await;
    commit_local(&t, committed_ref("s1", 1, Some(now(&t)))).await;
    env.still_mounted();
    t.detach(&Ctx::background(), UID).await.unwrap_err();
    assert_eq!(uuid_of(&t.state(UID).await.latest_durable), "s1");
    assert!(report_due(&t).await && env.report_due_on_disk());
}

// A Detach call with nothing left to detach keeps a report already due.
#[tokio::test(start_paused = true)]
async fn detach_call_keeps_report_due() {
    let env = Env::new();
    let store = FakeStore::new();
    let reporter = FakeReporter::new();
    let t = env.reporting(&store, &reporter).await;
    commit_local(&t, committed_ref("s1", 1, Some(now(&t)))).await;
    detach_now(&t).await;
    assert!(report_due(&t).await);
    let st = t.detach(&Ctx::background(), UID).await.unwrap();
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert!(report_due(&t).await && env.report_due_on_disk());
}

// Background work that changes neither LatestDurable nor LatestLocal makes
// no report due.
#[tokio::test(start_paused = true)]
async fn does_not_report_upload_overtaken() {
    for (name, uuid, generation, cancels) in [
        ("newer durable snapshot", "s2", 2, true),
        ("same generation already durable", "s1", 1, false),
    ] {
        let env = Env::new();
        let store = FakeStore::new();
        store.set_blocking(true);
        let reporter = FakeReporter::new();
        let t = env.reporting(&store, &reporter).await;
        let t0 = now(&t);
        commit_local(&t, committed_ref("s1", 1, Some(t0))).await;
        let op = start_upload(&t, &store).await;
        let mut durable = committed_ref(uuid, generation, Some(add(t0, MINUTE)));
        durable.upload_time = Some(add(durable.commit_time.unwrap(), SECOND));
        t.commit_durable(UID, durable.clone()).await;
        if !cancels {
            store.release(None).await;
        }
        wait_closed(&op, "the upload of s1").await;
        assert!(!report_due(&t).await, "{name}");
        assert_eq!(report_queue_len(&t), 0, "{name}");
        assert_eq!(
            time_of(
                &t.state(UID)
                    .await
                    .latest_durable
                    .unwrap()
                    .durable_complete_time
            ),
            durable.upload_time,
            "{name}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn does_not_report_failed_detach_of_durable_snapshot() {
    let env = Env::new();
    let store = FakeStore::new();
    let reporter = FakeReporter::new();
    let t = env.tracker(dyn_store(&store), reporting_opts(&reporter));
    let t0 = now(&t);
    let local = committed_ref("s1", 1, Some(t0));
    let mut durable = local.clone();
    durable.upload_time = Some(add(t0, SECOND));
    t.commit_durable(UID, durable).await;
    commit_local(&t, local).await;
    env.still_mounted();
    step_to(&t, add(t0, DETACH_DELAY)).await;
    process_next(&t, OpKind::Detach).await;
    assert!(!report_due(&t).await);
    assert_eq!(report_queue_len(&t), 0);
}

// A report failing for a reason a later attempt can pass is retried once
// the retry delay has passed, not before. Aborted logs at info, other
// retryable errors warn.
#[tokio::test(start_paused = true)]
async fn retries_failed_report() {
    for (err, level) in [
        (
            Status::aborted("another operation is changing the actor"),
            Level::INFO,
        ),
        (
            Status::unavailable("the control plane is restarting"),
            Level::WARN,
        ),
        (
            Status::deadline_exceeded("the control plane is slow"),
            Level::WARN,
        ),
        (
            Status::cancelled("call canceled while the keeper still runs"),
            Level::WARN,
        ),
        (Status::unknown("not a status"), Level::WARN),
    ] {
        let code = err.code();
        let (_guard, levels) = record_log_levels(FAILED);
        let env = Env::new();
        let store = FakeStore::new();
        store.set_blocking(true);
        let reporter = FakeReporter::new();
        reporter.fail_next([err]);
        let t = env.reporting(&store, &reporter).await;
        upload_s1(&t, &store).await;

        let failed_at = now(&t);
        process_next_report(&t).await;
        assert!(report_due(&t).await && env.report_due_on_disk(), "{code:?}");
        assert_eq!(levels.get(), vec![level], "{code:?}");
        step_to(
            &t,
            sub(add(failed_at, REPORT_RETRY_DELAY), Duration::from_millis(1)),
        )
        .await;
        settle().await;
        assert_eq!(report_queue_len(&t), 0, "{code:?}: before the retry delay");

        step_to(&t, add(failed_at, REPORT_RETRY_DELAY)).await;
        process_next_report(&t).await;
        assert_eq!(reporter.reports().len(), 2, "{code:?}");
        assert!(
            !report_due(&t).await && !env.report_due_on_disk(),
            "{code:?}"
        );
    }
}

// A report still failing once the give-up window since it became due has
// passed is dropped; a later change starts a fresh window.
#[tokio::test(start_paused = true)]
async fn gives_up_on_report_after_the_window() {
    let (_guard, gave_up) = record_log_levels(GAVE_UP);
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let reporter = FakeReporter::new();
    reporter.fail_next(std::iter::repeat_n(Status::unavailable("down"), 3));
    let t = env.reporting(&store, &reporter).await;
    upload_s1(&t, &store).await;
    let due_at = now(&t);

    // The first attempt fails; its retry comes after the retry delay.
    process_next_report(&t).await;
    // Just short of the window the second fails, and its retry, twice the
    // delay out, lands past the window.
    step_to(&t, sub(add(due_at, DEFAULT_REPORT_GIVE_UP_AFTER), SECOND)).await;
    process_next_report(&t).await;
    assert!(report_due(&t).await && env.report_due_on_disk());
    assert!(gave_up.get().is_empty());

    step_to(&t, add(now(&t), 2 * REPORT_RETRY_DELAY)).await;
    process_next_report(&t).await;
    assert!(!report_due(&t).await && !env.report_due_on_disk());
    assert_eq!(gave_up.get(), vec![Level::WARN]);
    step_to(&t, add(now(&t), MAX_REPORT_RETRY_DELAY)).await;
    settle().await;
    assert_eq!(report_queue_len(&t), 0);

    detach_now(&t).await;
    process_next_report(&t).await;
    let sent = reporter.reports();
    assert_eq!(sent.len(), 4);
    assert!(
        sent[3].state.latest_local.is_none(),
        "the detach is reported"
    );
}

#[tokio::test(start_paused = true)]
async fn gives_up_after_the_configured_window() {
    let give_up = 2 * MINUTE;
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let reporter = FakeReporter::new();
    reporter.fail_next(std::iter::repeat_n(Status::unavailable("down"), 2));
    let t = env.tracker(
        dyn_store(&store),
        Options {
            report_give_up_after: give_up,
            ..reporting_opts(&reporter)
        },
    );
    t.start().await;
    upload_s1(&t, &store).await;
    let due_at = now(&t);
    process_next_report(&t).await;
    step_to(&t, add(due_at, give_up)).await;
    process_next_report(&t).await;
    assert!(!report_due(&t).await && !env.report_due_on_disk());
}

// Giving up on a report keeps a newer one made due while it was in flight.
#[tokio::test(start_paused = true)]
async fn give_up_keeps_a_newer_report_made_due_during_the_last_attempt() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let reporter = FakeReporter::new();
    reporter.set_blocking(true);
    // A short give-up window, and a detach due soon after the upload, so
    // the steps below stay within one report attempt's timeout.
    let window = REPORT_TIMEOUT / 4;
    let t = env.tracker(
        dyn_store(&store),
        Options {
            report_give_up_after: window,
            detach_delay: UPLOAD_DELAY + window,
            ..worker_opts(&reporter)
        },
    );
    t.start().await;
    upload_s1(&t, &store).await;
    let first = reporter.wait_reported().await;

    // Past the upload report's window, a background detach runs while that
    // report is still in flight.
    step_to(&t, add(now(&t), window)).await;
    detach_now(&t).await;
    reporter.set_blocking(false);
    reporter
        .release(Some(Status::unavailable("was down")))
        .await;
    settle().await;
    step_to(&t, add(now(&t), REPORT_RETRY_DELAY)).await;
    let second = reporter.wait_reported().await;
    assert!(
        first.state.latest_local.is_some(),
        "the upload report first"
    );
    assert!(
        second.state.latest_local.is_none(),
        "then the detach report"
    );
    eventually("the detach report is acknowledged", || async {
        !report_due(&t).await
    })
    .await;
}

/// Makes a report of `uid` due through a background upload of `uuid`.
async fn make_actor_report_due(t: &Tracker, env: &Env, store: &FakeStore, uid: &str, uuid: &str) {
    fs::create_dir_all(env.paths.actor_dir(uid)).unwrap();
    t.commit_local(uid, committed_ref(uuid, 1, Some(now(t))))
        .await;
    let due = time_of(&t.state(uid).await.upload_due_time).unwrap();
    step_to(t, due).await;
    store.wait_started().await;
    let op = op_of(t, uid).await;
    store.release(None).await;
    wait_closed(&op, "the upload").await;
}

/// A tracker with one report worker and an upload delay short enough that
/// a few uploads fit in one report attempt's timeout.
async fn queue_backoff_tracker(
    env: &Env,
    store: &Arc<FakeStore>,
    reporter: &Arc<FakeReporter>,
) -> Tracker {
    let t = env.tracker(
        dyn_store(store),
        Options {
            upload_delay: REPORT_TIMEOUT / 5,
            ..worker_opts(reporter)
        },
    );
    t.start().await;
    t
}

// A control-plane failure pauses the whole report queue; a response resets
// it, and Aborted, a conflict of one actor, does not hold back others.
#[tokio::test(start_paused = true)]
async fn unavailable_holds_back_the_next_actor() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let reporter = FakeReporter::new();
    reporter.set_blocking(true);
    let t = queue_backoff_tracker(&env, &store, &reporter).await;
    make_actor_report_due(&t, &env, &store, "actor-uid-1", "s1").await;
    assert_eq!(reporter.wait_reported().await.actor_uid, "actor-uid-1");
    make_actor_report_due(&t, &env, &store, "actor-uid-2", "s2").await;
    make_actor_report_due(&t, &env, &store, "actor-uid-3", "s3").await;

    let failed_at = now(&t);
    reporter.set_blocking(false);
    reporter
        .release(Some(Status::unavailable("overloaded")))
        .await;
    settle().await;
    // The jittered queue pause is in [delay/2, delay).
    step_to(
        &t,
        sub(
            add(failed_at, REPORT_RETRY_DELAY / 2),
            Duration::from_millis(1),
        ),
    )
    .await;
    settle().await;
    assert_eq!(reporter.reports().len(), 1, "only the failed attempt yet");

    // Past the pause, just short of actor-uid-1's own retry.
    step_to(
        &t,
        sub(add(failed_at, REPORT_RETRY_DELAY), Duration::from_nanos(1)),
    )
    .await;
    assert_eq!(reporter.wait_reported().await.actor_uid, "actor-uid-2");
    // Its success resets the pause: actor-uid-3 reports right away.
    assert_eq!(reporter.wait_reported().await.actor_uid, "actor-uid-3");
}

#[tokio::test(start_paused = true)]
async fn aborted_does_not_hold_back_other_actors() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let reporter = FakeReporter::new();
    reporter.set_blocking(true);
    let t = queue_backoff_tracker(&env, &store, &reporter).await;
    make_actor_report_due(&t, &env, &store, "actor-uid-1", "s1").await;
    assert_eq!(reporter.wait_reported().await.actor_uid, "actor-uid-1");
    make_actor_report_due(&t, &env, &store, "actor-uid-2", "s2").await;
    reporter.set_blocking(false);
    reporter
        .release(Some(Status::aborted(
            "another operation is changing the actor",
        )))
        .await;
    assert_eq!(reporter.wait_reported().await.actor_uid, "actor-uid-2");
}

#[tokio::test(start_paused = true)]
async fn queue_backoff_caps() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let reporter = FakeReporter::new();
    reporter.fail_next(std::iter::repeat_n(Status::unavailable("down"), 5));
    let t = env.tracker(dyn_store(&store), reporting_opts(&reporter));
    t.start().await;
    let mut delay = Duration::ZERO;
    for i in 1..=5 {
        let uid = format!("actor-uid-cap-{i}");
        make_actor_report_due(&t, &env, &store, &uid, "s1").await;
        delay = t.inner.report(&uid).await;
    }
    assert!(
        delay >= MAX_REPORT_QUEUE_RETRY_DELAY / 2 && delay < MAX_REPORT_QUEUE_RETRY_DELAY,
        "5th queue pause = {delay:?}"
    );
}

// A refused report is not retried and no longer due; NotFound and
// Unimplemented log at info, other refusals warn.
#[tokio::test(start_paused = true)]
async fn drops_refused_report() {
    for (code, level) in [
        (Code::NotFound, Level::INFO),
        (Code::FailedPrecondition, Level::WARN),
        (Code::InvalidArgument, Level::WARN),
        (Code::Unimplemented, Level::INFO),
    ] {
        let (_guard, levels) = record_log_levels(REFUSED);
        let env = Env::new();
        let store = FakeStore::new();
        store.set_blocking(true);
        let reporter = FakeReporter::new();
        reporter.fail_next([Status::new(code, "refused")]);
        let t = env.reporting(&store, &reporter).await;
        upload_s1(&t, &store).await;
        process_next_report(&t).await;
        assert!(
            !report_due(&t).await && !env.report_due_on_disk(),
            "{code:?}"
        );
        assert_eq!(levels.get(), vec![level], "{code:?}");
        step_to(&t, add(now(&t), MAX_REPORT_RETRY_DELAY)).await;
        settle().await;
        assert_eq!(report_queue_len(&t), 0, "{code:?}");
    }
}

// A due report survives a restart; once acknowledged, a further restart
// sends nothing.
#[tokio::test(start_paused = true)]
async fn reports_saved_report_after_restart() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    // Without a reporter the report is saved but not queued.
    let before = env.started(&store).await;
    upload_s1(&before, &store).await;
    assert_eq!(report_queue_len(&before), 0);

    let reporter = FakeReporter::new();
    let after = env.reporting(&store, &reporter).await;
    process_next_report(&after).await;
    let sent = reporter.reports();
    assert_eq!(sent.len(), 1);
    assert_eq!(uuid_of(&sent[0].state.latest_durable), "s1");
    assert!(sent[0].actor == test_actor());

    let again = env.reporting(&store, &reporter).await;
    assert_eq!(report_queue_len(&again), 0);
    assert_eq!(reporter.reports().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn reports_saved_detach_report_after_restart() {
    let env = Env::new();
    let store = FakeStore::new();
    let before = env.started(&store).await;
    commit_local(&before, committed_ref("s1", 1, Some(now(&before)))).await;
    detach_now(&before).await;
    assert!(env.report_due_on_disk());

    let reporter = FakeReporter::new();
    let after = env.reporting(&store, &reporter).await;
    process_next_report(&after).await;
    let sent = reporter.reports();
    assert_eq!(sent.len(), 1);
    assert_detached(&sent[0]);
    assert!(!env.paths.actor_dir(UID).exists());
}

#[tokio::test(start_paused = true)]
async fn jitters_saved_reports_at_startup() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let before = env.started(&store).await;
    upload_s1(&before, &store).await;

    let reporter = FakeReporter::new();
    let after = env.tracker(
        dyn_store(&store),
        Options {
            report_startup_jitter: REPORT_STARTUP_JITTER,
            ..reporting_opts(&reporter)
        },
    );
    after.start().await;
    assert_eq!(report_queue_len(&after), 0, "delayed by the startup jitter");
    step_to(&after, add(now(&after), REPORT_STARTUP_JITTER)).await;
    process_next_report(&after).await;
    assert_eq!(reporter.reports().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn fresh_change_does_not_wait_for_startup_jitter() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let before = env.started(&store).await;
    upload_s1(&before, &store).await;

    let reporter = FakeReporter::new();
    let after = env.tracker(
        dyn_store(&store),
        Options {
            report_startup_jitter: REPORT_STARTUP_JITTER,
            ..reporting_opts(&reporter)
        },
    );
    after.start().await;
    detach_now(&after).await;
    process_next_report(&after).await;
    let sent = reporter.reports();
    assert_eq!(sent.len(), 1);
    assert!(sent[0].state.latest_local.is_none());
}

// A report whose LatestLocal changed while it ran is sent again with the
// new state.
#[tokio::test(start_paused = true)]
async fn reports_again_when_latest_local_changes() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let reporter = FakeReporter::new();
    reporter.set_blocking(true);
    let t = env.tracker(
        dyn_store(&store),
        Options {
            detach_delay: UPLOAD_DELAY + REPORT_TIMEOUT / 2,
            ..worker_opts(&reporter)
        },
    );
    t.start().await;
    upload_s1(&t, &store).await;
    let first = reporter.wait_reported().await;
    detach_now(&t).await;
    reporter.set_blocking(false);
    reporter.release(None).await;
    let second = reporter.wait_reported().await;
    assert_eq!(uuid_of(&first.state.latest_local), "s1");
    assert_eq!(uuid_of(&second.state.latest_local), "");
    eventually("the report is no longer due", || async {
        !report_due(&t).await
    })
    .await;
}

// An upload starting while a report is in flight queues no other report.
#[tokio::test(start_paused = true)]
async fn ignores_uploading_starting_during_report() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let reporter = FakeReporter::new();
    reporter.set_blocking(true);
    let t = env.tracker(
        dyn_store(&store),
        Options {
            upload_delay: REPORT_TIMEOUT / 2,
            ..worker_opts(&reporter)
        },
    );
    t.start().await;
    commit_local(&t, committed_ref("s1", 1, Some(now(&t)))).await;
    let s1_op = start_upload(&t, &store).await;
    commit_local(&t, committed_ref("s2", 2, Some(now(&t)))).await;
    store.release(None).await;
    wait_closed(&s1_op, "the upload of s1").await;
    let first = reporter.wait_reported().await;
    assert!(first.state.uploading.is_none());

    let s2_op = start_upload(&t, &store).await;
    reporter.set_blocking(false);
    reporter.release(None).await;
    eventually("the s1 report finishes", || async { !report_due(&t).await }).await;
    assert_eq!(reporter.reports().len(), 1);

    store.release(None).await;
    wait_closed(&s2_op, "the upload of s2").await;
    let second = reporter.wait_reported().await;
    assert_eq!(uuid_of(&second.state.latest_durable), "s2");
}

const FOREGROUND_CALLS: [&str; 5] = [
    "CommitLocal",
    "CommitDurable",
    "CommitPromoted",
    "CommitPromoted of already durable snapshot",
    "CommittedCheckpointState with local snapshot",
];

async fn foreground_call(t: &Tracker, name: &str) {
    match name {
        "CommitLocal" => {
            commit_local(t, committed_ref("s2", 2, Some(now(t)))).await;
        }
        "CommitDurable" => {
            t.commit_durable(UID, committed_ref("s2", 2, Some(now(t))))
                .await;
        }
        "CommitPromoted" => {
            t.commit_promoted(UID, committed_ref("s2", 2, Some(now(t))))
                .await;
        }
        "CommitPromoted of already durable snapshot" => {
            t.commit_promoted(UID, committed_ref("s1", 1, Some(now(t))))
                .await;
        }
        _ => {
            t.committed_checkpoint_state(UID).await;
        }
    }
}

async fn make_report_due(t: &Tracker, store: &FakeStore) {
    commit_local(t, committed_ref("s1", 1, Some(now(t)))).await;
    store.set_blocking(true);
    upload_now(t, store, None).await;
    store.set_blocking(false);
}

// A Checkpoint or PromoteSnapshot while a local snapshot is kept cancels a
// pending report, whether in retry backoff or in flight.
#[tokio::test(start_paused = true)]
async fn cancels_queued_report_on_foreground_call() {
    for name in FOREGROUND_CALLS {
        let env = Env::new();
        let store = FakeStore::new();
        let reporter = FakeReporter::new();
        reporter.fail_next([Status::unavailable("restarting")]);
        let t = env.reporting(&store, &reporter).await;
        make_report_due(&t, &store).await;
        process_next_report(&t).await;
        assert!(report_due(&t).await && env.report_due_on_disk(), "{name}");
        foreground_call(&t, name).await;
        assert!(!report_due(&t).await && !env.report_due_on_disk(), "{name}");
        step_to(&t, add(now(&t), MAX_REPORT_RETRY_DELAY)).await;
        process_next_report(&t).await;
        assert_eq!(reporter.reports().len(), 1, "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn cancels_in_flight_report_on_foreground_call() {
    for name in FOREGROUND_CALLS {
        for (flight, err) in [
            ("in flight succeeds", None),
            (
                "in flight fails with Aborted",
                Some(Status::aborted("actor is suspending")),
            ),
        ] {
            let (_guard, failed) = record_log_levels(FAILED);
            let env = Env::new();
            let store = FakeStore::new();
            let reporter = FakeReporter::new();
            reporter.set_blocking(true);
            let t = env.reporting(&store, &reporter).await;
            make_report_due(&t, &store).await;
            let h = {
                let t = t.clone();
                tokio::spawn(async move { process_next_report(&t).await })
            };
            reporter.wait_reported().await;
            foreground_call(&t, name).await;
            assert!(
                !report_due(&t).await && !env.report_due_on_disk(),
                "{name}/{flight}"
            );
            reporter.set_blocking(false);
            reporter.release(err).await;
            join("the in-flight report", h).await;

            step_to(&t, add(now(&t), MAX_REPORT_RETRY_DELAY)).await;
            settle().await;
            assert!(
                !report_due(&t).await && !env.report_due_on_disk(),
                "{name}/{flight}"
            );
            assert_eq!(report_queue_len(&t), 0, "{name}/{flight}");
            assert!(
                failed.get().is_empty(),
                "{name}/{flight}: {:?}",
                failed.get()
            );
        }
    }
}

// After a detach cleared LatestLocal, CommitPromoted or
// CommittedCheckpointState keeps the detach report due.
#[tokio::test(start_paused = true)]
async fn keeps_detach_report_on_commit_promoted_when_detached() {
    for promote in [true, false] {
        let env = Env::new();
        let store = FakeStore::new();
        let reporter = FakeReporter::new();
        let t = env.reporting(&store, &reporter).await;
        commit_local(&t, committed_ref("s1", 1, Some(now(&t)))).await;
        detach_now(&t).await;
        assert!(report_due(&t).await && env.report_due_on_disk());
        if promote {
            t.commit_promoted(UID, committed_ref("s1", 1, Some(now(&t))))
                .await;
        } else {
            t.committed_checkpoint_state(UID).await;
        }
        assert!(
            report_due(&t).await && env.report_due_on_disk(),
            "promote = {promote}"
        );
        process_next_report(&t).await;
        let sent = reporter.reports();
        assert_eq!(sent.len(), 1);
        assert_detached(&sent[0]);
        assert!(!report_due(&t).await && !env.report_due_on_disk());
        assert!(!env.paths.actor_dir(UID).exists());
    }
}

// Forget does not wait for a report in flight, which records nothing once
// it ends.
#[tokio::test(start_paused = true)]
async fn forget_during_report() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let reporter = FakeReporter::new();
    reporter.set_blocking(true);
    let t = env.tracker(dyn_store(&store), worker_opts(&reporter));
    t.start().await;
    upload_s1(&t, &store).await;
    reporter.wait_reported().await;
    within("Forget during a report", t.forget(UID))
        .await
        .unwrap();
    reporter.release(None).await;
    settle().await;
    assert!(
        reporter.try_reported().is_none(),
        "nothing reported after Forget"
    );
    assert!(!env.paths.actor_snapshot_state_file(UID).exists());
}

// A report in flight at shutdown stays due for the next keeper, logged at
// info whether it fails with Canceled or another retryable error.
#[tokio::test(start_paused = true)]
async fn report_stays_due_at_shutdown() {
    for err in [None, Some(Status::unavailable("connection closing"))] {
        let (_guard, levels) = record_log_levels(FAILED);
        let env = Env::new();
        let ctx = Ctx::background();
        let store = FakeStore::new();
        store.set_blocking(true);
        let reporter = FakeReporter::new();
        reporter.set_blocking(true);
        if let Some(e) = &err {
            reporter.fail_next([e.clone()]);
        }
        // A give-up window shorter than one attempt's timeout, so the
        // report can be past it while still in flight.
        let window = REPORT_TIMEOUT / 2;
        let t = env.tracker_ctx(
            ctx.clone(),
            dyn_store(&store),
            Options {
                upload_delay: UPLOAD_DELAY,
                detach_delay: DETACH_DELAY,
                report_give_up_after: window,
                reporter: Some(reporter.clone() as Arc<dyn Reporter>),
                ..Options::default()
            },
        );
        t.start().await;
        upload_s1(&t, &store).await;
        reporter.wait_reported().await;
        // Past the window, so a retry would drop the report if shutdown fell
        // through to it.
        step_to(&t, add(now(&t), window)).await;
        ctx.cancel();
        within("the tracker to stop", t.wait(&Ctx::background()))
            .await
            .unwrap();
        assert!(
            env.report_due_on_disk(),
            "{err:?}: the interrupted report stays due"
        );
        assert_eq!(levels.get(), vec![Level::INFO], "{err:?}");
    }
}

/// Begins the RPC named `kind` on the tracked actor.
async fn begin_hold(t: &Tracker, kind: &str) -> HoldEnd {
    let ctx = Ctx::background();
    let s2 = committed_ref("s2", 2, Some(now(t)));
    match kind {
        "BeginStart" => t.begin_start(&ctx, UID).await.unwrap(),
        "BeginCheckpoint" => {
            t.begin_checkpoint(&ctx, UID, &s2.snapshot, DEFERRED)
                .await
                .unwrap()
                .1
        }
        "BeginCheckpoint immediate" => {
            t.begin_checkpoint(&ctx, UID, &s2.snapshot, IMMEDIATE)
                .await
                .unwrap()
                .1
        }
        // A snapshot other than s1, so it does not wait for s1's upload.
        "BeginPromote" => t.begin_promote(&ctx, UID, &s2).await.unwrap().1,
        // A Terminate that fails before Forget.
        _ => t.begin_terminate(UID).await,
    }
}

const HOLDS: [&str; 5] = [
    "BeginStart",
    "BeginCheckpoint",
    "BeginCheckpoint immediate",
    "BeginPromote",
    "BeginTerminate",
];

// A report waits while an RPC holds the actor, and is queued once the hold
// ends without a commit cancelling it.
#[tokio::test(start_paused = true)]
async fn defers_report_when_upload_finishes_during_hold() {
    for kind in HOLDS {
        let env = Env::new();
        let store = FakeStore::new();
        store.set_blocking(true);
        let reporter = FakeReporter::new();
        let t = env.reporting(&store, &reporter).await;
        commit_local(&t, committed_ref("s1", 1, Some(now(&t)))).await;
        let op = start_upload(&t, &store).await;
        let end = begin_hold(&t, kind).await;
        store.release(None).await;
        wait_closed(&op, "the upload").await;
        assert_eq!(report_queue_len(&t), 0, "{kind}");
        assert!(report_due(&t).await && env.report_due_on_disk(), "{kind}");
        end.end().await;
        process_next_report(&t).await;
        assert_eq!(reporter.reports().len(), 1, "{kind}");
        assert!(!report_due(&t).await, "{kind}");
    }
}

#[tokio::test(start_paused = true)]
async fn defers_report_popped_during_hold() {
    for kind in HOLDS {
        let env = Env::new();
        let store = FakeStore::new();
        store.set_blocking(true);
        let reporter = FakeReporter::new();
        let t = env.reporting(&store, &reporter).await;
        upload_s1(&t, &store).await;
        let end = begin_hold(&t, kind).await;
        process_next_report(&t).await;
        assert!(reporter.reports().is_empty(), "{kind}");
        assert_eq!(report_queue_len(&t), 0, "{kind}");
        assert!(report_due(&t).await, "{kind}");
        end.end().await;
        process_next_report(&t).await;
        assert_eq!(reporter.reports().len(), 1, "{kind}");
        assert!(!report_due(&t).await, "{kind}");
    }
}

#[tokio::test(start_paused = true)]
async fn commit_during_hold_cancels_deferred_report() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let reporter = FakeReporter::new();
    let t = env.reporting(&store, &reporter).await;
    upload_s1(&t, &store).await;
    let s2 = committed_ref("s2", 2, Some(now(&t)));
    let (_, end) = t
        .begin_checkpoint(&Ctx::background(), UID, &s2.snapshot, DEFERRED)
        .await
        .unwrap();
    process_next_report(&t).await;
    commit_local(&t, s2).await;
    end.end().await;
    assert_eq!(report_queue_len(&t), 0);
    assert!(!report_due(&t).await);
}

#[tokio::test(start_paused = true)]
async fn hold_during_retry_backoff_keeps_retry_delay() {
    let env = Env::new();
    let store = FakeStore::new();
    store.set_blocking(true);
    let reporter = FakeReporter::new();
    reporter.fail_next([Status::unavailable("down")]);
    let t = env.reporting(&store, &reporter).await;
    upload_s1(&t, &store).await;
    let failed_at = now(&t);
    process_next_report(&t).await;
    t.begin_start(&Ctx::background(), UID)
        .await
        .unwrap()
        .end()
        .await;
    step_to(
        &t,
        sub(add(failed_at, REPORT_RETRY_DELAY), Duration::from_millis(1)),
    )
    .await;
    settle().await;
    assert_eq!(report_queue_len(&t), 0, "before the retry delay");
    step_to(&t, add(failed_at, REPORT_RETRY_DELAY)).await;
    process_next_report(&t).await;
    assert_eq!(reporter.reports().len(), 2);
}

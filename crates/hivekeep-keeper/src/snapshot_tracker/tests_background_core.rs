//! Workers, startup recovery, an actor's operation in progress, Forget,
//! Terminate holds, backoff, and how operations are logged.

use std::fs;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;

use hivekeep_proto::keeper::v1 as pb;
use tonic::Status;

use super::actor::ActorRecord;
use super::background::log_op;
use super::op::OpKind;
use super::test_support_core::*;
use super::*;
use crate::context::{Ctx, CtxError};

/// A boot check passing only the local snapshots named in `uuids`.
fn current_boot(uuids: &[&str]) -> BootCheck {
    let uuids: Vec<String> = uuids.iter().map(|s| s.to_string()).collect();
    Arc::new(move |_, uuid| uuids.iter().any(|u| u == uuid))
}

fn no_workers(o: &mut Options) {
    o.upload_workers = 0;
    o.detach_workers = 0;
}

/// Wait returns once the workers stop as the tracker's context ends, even
/// with a worker waiting on an actor whose lock an RPC holds.
#[tokio::test(start_paused = true)]
async fn workers_stop_when_context_ends() {
    let n = Node::bare();
    let ctx = Ctx::background();
    let t = Tracker::new(
        ctx.clone(),
        n.paths.clone(),
        None,
        Arc::new(crate::clock::MockClock::new(t0())),
        Options::default(),
    );
    t.start().await;
    let _held = t.inner.lock(UID).await;
    for q in &t.inner.queues {
        q.add(UID);
    }
    settle().await;
    ctx.cancel();
    returns_promptly("Wait", t.wait(&Ctx::background()))
        .await
        .unwrap();
}

/// Wait gives up once its own context ends while the workers still run.
#[tokio::test(start_paused = true)]
async fn wait_gives_up_when_its_context_ends() {
    let n = Node::bare();
    let t = n.plain();
    t.start().await;
    let res = t
        .wait(&Ctx::background().with_timeout(Duration::from_millis(10)))
        .await;
    assert_eq!(res, Err(CtxError::DeadlineExceeded));
}

/// The entry stays in memory while an operation is in progress, even as
/// commits save it, and is dropped once the operation ends.
#[tokio::test(start_paused = true)]
async fn entry_stays_in_memory_during_op() {
    let n = Node::new();
    let t = n.plain();
    let s1 = test_ref("s1", 1, Some(t0()));
    t.commit_local(UID, s1.clone()).await;
    assert!(!in_memory(&t, UID).await);

    let (op, done) = begin_op(&t, s1).await;
    t.commit_local(UID, test_ref("s2", 2, Some(t0()))).await;
    assert!(in_memory(&t, UID).await, "kept during an operation");

    end_op(&t, &op, done).await;
    assert!(!in_memory(&t, UID).await);
    assert_eq!(uuid_of(&n.plain().state(UID).await.latest_local), "s2");
}

/// Start reads each state file, queues the actor only where work is due, and
/// drops its entry; directories without a state file are skipped.
#[tokio::test(start_paused = true)]
async fn start_queues_actors_with_snapshot_state() {
    let n = Node::bare();
    let store = RecStore::new();
    async fn queued(n: &Node, store: &Arc<RecStore>) -> [Vec<String>; 2] {
        let t = n.store_tracker(store, no_workers);
        t.start().await;
        assert_eq!(actors_in_memory(&t), 0, "in memory after Start");
        drain_queues(&t).await
    }
    let got = queued(&n, &store).await;
    assert!(got[0].is_empty() && got[1].is_empty(), "{got:?}");

    for uid in [
        "actor-both",
        "actor-upload",
        "actor-detach",
        "actor-durable",
        "actor-without-state",
    ] {
        n.actor_dir(uid);
    }
    fs::write(n.paths.actors_dir().join("not-an-actor"), b"").unwrap();
    // Committed a detach delay before the restarted tracker's clock, so any
    // scheduled work is due at Start.
    let start = before(t0(), DETACH_DELAY);
    let mut o = Node::opts();
    no_workers(&mut o);
    let b = n.tracker_at(Some(store.clone()), start, o);
    b.commit_local("actor-both", test_ref("s1", 1, Some(start)))
        .await;
    b.commit_local("actor-upload", test_ref("s2", 1, Some(start)))
        .await;
    b.begin_start(&Ctx::background(), "actor-upload")
        .await
        .unwrap()
        .end()
        .await;
    let s3 = test_ref("s3", 1, Some(start));
    b.commit_durable("actor-detach", s3.clone()).await;
    b.commit_local("actor-detach", s3).await;
    b.commit_durable("actor-durable", test_ref("s4", 1, Some(start)))
        .await;

    let got = queued(&n, &store).await;
    assert_eq!(
        got[OpKind::Upload as usize],
        vec!["actor-both", "actor-upload"]
    );
    assert_eq!(
        got[OpKind::Detach as usize],
        vec!["actor-both", "actor-detach"]
    );
}

/// With a boot check, Start deletes local snapshots written before the node
/// rebooted: a durable one is reported as detached, an undurable one is
/// lost; ones from this boot stay.
#[tokio::test(start_paused = true)]
async fn start_prunes_local_snapshots_from_earlier_boot() {
    let n = Node::bare();
    for uid in [
        "actor-suspended-old",
        "actor-running-old",
        "actor-same-boot",
        "actor-undurable-old",
    ] {
        n.actor_dir(uid);
    }
    let start = before(t0(), DETACH_DELAY);
    let b = n.tracker_at(Some(RecStore::new()), start, Node::opts());

    let s1 = test_ref("s1", 1, Some(start));
    b.commit_durable("actor-suspended-old", s1.clone()).await;
    b.commit_local("actor-suspended-old", s1).await;
    let suspended_old = write_local_snapshot(&n.paths, "actor-suspended-old", "s1");

    let s2 = test_ref("s2", 1, Some(start));
    b.commit_durable("actor-running-old", s2.clone()).await;
    b.commit_local("actor-running-old", s2).await;
    b.begin_start(&Ctx::background(), "actor-running-old")
        .await
        .unwrap()
        .end()
        .await;
    let running_old = write_local_snapshot(&n.paths, "actor-running-old", "s2");

    let s3 = test_ref("s3", 1, Some(start));
    b.commit_durable("actor-same-boot", s3.clone()).await;
    b.commit_local("actor-same-boot", s3).await;
    let same_boot = write_local_snapshot(&n.paths, "actor-same-boot", "s3");

    b.commit_local("actor-undurable-old", test_ref("s4", 1, Some(start)))
        .await;
    let undurable_old = write_local_snapshot(&n.paths, "actor-undurable-old", "s4");

    let rep = Arc::new(RecReporter::default());
    let mut o = Node::opts();
    no_workers(&mut o);
    o.report_workers = 0;
    o.boot_check = Some(current_boot(&["s3"]));
    o.reporter = Some(rep.clone());
    let a = n.tracker_at(Some(RecStore::new()), t0(), o);
    a.start().await;

    for (uid, snap, dir) in [
        ("actor-suspended-old", "s1", &suspended_old),
        ("actor-running-old", "s2", &running_old),
    ] {
        assert!(!dir.exists(), "{uid}");
        let st = a.state(uid).await;
        assert_eq!(uuid_of(&st.latest_local), "", "{uid}");
        assert_eq!(uuid_of(&st.latest_durable), snap, "{uid}");
    }
    assert!(same_boot.exists());
    assert_eq!(
        uuid_of(&a.state("actor-same-boot").await.latest_local),
        "s3"
    );
    assert!(!undurable_old.exists());
    assert!(a.state("actor-undurable-old").await.latest_local.is_none());

    let got = drain_queues(&a).await;
    assert!(got[OpKind::Upload as usize].is_empty(), "{got:?}");
    assert_eq!(got[OpKind::Detach as usize], vec!["actor-same-boot"]);

    // The two detached actors' reports are queued; send them as a report
    // worker would.
    let rq = a.inner.reporting.queue.clone();
    for _ in 0..2 {
        let uid = rq.get().await.unwrap();
        a.inner.report(&uid).await;
        rq.done(&uid);
    }
    let mut reported: Vec<String> = rep
        .sent
        .lock()
        .iter()
        .map(|r| {
            assert!(r.state.latest_local.is_none(), "{r:?}");
            r.actor_uid.clone()
        })
        .collect();
    reported.sort();
    assert_eq!(reported, vec!["actor-running-old", "actor-suspended-old"]);
    assert_eq!(rq.len(), 0);
}

/// A detach found at Start stays in memory when its save fails.
#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn start_prune_keeps_unsaved_detach_in_memory() {
    if unsafe { libc::geteuid() } == 0 {
        return; // root ignores directory modes
    }
    let n = Node::new();
    let b = n.store_tracker(&RecStore::new(), |_| {});
    let s1 = test_ref("s1", 1, Some(now(&b)));
    b.commit_durable(UID, s1.clone()).await;
    b.commit_local(UID, s1).await;
    // Pruning a missing local-checkpoint dir succeeds on a read-only actor
    // dir, while saving the state file fails.
    let actor_dir = n.paths.actor_dir(UID);
    crate::atomicfile::chmod(&actor_dir, 0o500).unwrap();
    let a = n.store_tracker(&RecStore::new(), |o| {
        no_workers(o);
        o.boot_check = Some(current_boot(&[]));
    });
    a.start().await;
    let st = a.state(UID).await;
    crate::atomicfile::chmod(&actor_dir, 0o700).unwrap();
    assert!(
        st.latest_local.is_none(),
        "kept in memory after a failed save"
    );
}

/// A Start prune that fails keeps LatestLocal and retries the detach after
/// the retry delay.
#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn start_prune_failure_schedules_detach_retry() {
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    let n = Node::new();
    let b = n.store_tracker(&RecStore::new(), |_| {});
    let s1 = test_ref("s1", 1, Some(now(&b)));
    b.commit_durable(UID, s1.clone()).await;
    b.commit_local(UID, s1).await;
    b.begin_start(&Ctx::background(), UID)
        .await
        .unwrap()
        .end()
        .await;
    let dir = write_local_snapshot(&n.paths, UID, "s1");
    let nested = dir.join("sub");
    fs::create_dir_all(&nested).unwrap();
    fs::write(nested.join("keep"), b"x").unwrap();
    crate::atomicfile::chmod(&nested, 0o500).unwrap();

    let a = n.store_tracker(&RecStore::new(), |o| {
        no_workers(o);
        o.boot_check = Some(current_boot(&[]));
    });
    a.start().await;
    let local = uuid_of(&a.state(UID).await.latest_local);
    let due = detach_due_of(&a, UID).await;
    crate::atomicfile::chmod(&nested, 0o700).unwrap();
    assert_eq!(local, "s1", "still recorded when the prune fails");
    assert_eq!(due, Some(at(now(&a), SNAPSHOT_RETRY_DELAY)));
}

/// Only an Uploading from an earlier boot is dropped; the current boot's
/// LatestLocal stays.
#[tokio::test(start_paused = true)]
async fn start_prune_keeps_current_boot_local_when_only_uploading_is_old() {
    let n = Node::new();
    let b = n.store_tracker(&RecStore::new(), |_| {});
    let start = now(&b);
    {
        let mut g = b.inner.lock(UID).await;
        g.g.entry = Some(b.inner.new_entry(
            UID,
            ActorRecord {
                uploading: Some(test_ref("s1", 1, Some(start))),
                latest_local: Some(test_ref("s2", 2, Some(at(start, SECOND)))),
                ..Default::default()
            },
        ));
        b.inner.save(&mut g);
    }
    let up_dir = write_local_snapshot(&n.paths, UID, "s1");
    let local_dir = write_local_snapshot(&n.paths, UID, "s2");

    let a = n.store_tracker(&RecStore::new(), |o| {
        no_workers(o);
        o.boot_check = Some(current_boot(&["s2"]));
    });
    a.start().await;
    assert!(!up_dir.exists());
    assert!(local_dir.exists());
    let st = a.state(UID).await;
    assert!(st.uploading.is_none());
    assert_eq!(uuid_of(&st.latest_local), "s2");
}

/// An undurable snapshot from an earlier boot stops being tracked even when
/// deleting it fails.
#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn start_prune_drops_undurable_snapshot_when_delete_fails() {
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    let n = Node::new();
    let b = n.store_tracker(&RecStore::new(), |_| {});
    // Older than the upload delay, so one still tracked would be overdue.
    b.commit_local(
        UID,
        test_ref("s1", 1, Some(before(now(&b), 2 * UPLOAD_DELAY))),
    )
    .await;
    let dir = write_local_snapshot(&n.paths, UID, "s1");
    let nested = dir.join("sub");
    fs::create_dir_all(&nested).unwrap();
    fs::write(nested.join("keep"), b"x").unwrap();
    crate::atomicfile::chmod(&nested, 0o500).unwrap();

    let reg = prometheus::Registry::new();
    let inst = Instruments::new(&reg).unwrap();
    let a = n.store_tracker(&RecStore::new(), |o| {
        no_workers(o);
        o.boot_check = Some(current_boot(&[]));
        o.instruments = Some(inst);
    });
    a.start().await;
    let st = a.state(UID).await;
    let counts = a.inner.tally.as_ref().unwrap().counts();
    crate::atomicfile::chmod(&nested, 0o700).unwrap();
    assert!(
        st.latest_local.is_none() && st.upload_due_time.is_none(),
        "{st:?}"
    );
    assert_eq!(counts, metrics::TallyCounts::default());
}

/// Forget cancels the operation in progress and waits for it to end,
/// releasing the actor's lock meanwhile, before dropping the actor.
#[tokio::test(start_paused = true)]
async fn forget_waits_for_in_flight_op() {
    let n = Node::new();
    let t = n.plain();
    let s1 = test_ref("s1", 1, Some(t0()));
    t.commit_local(UID, s1.clone()).await;
    let (op, done) = begin_op(&t, s1).await;

    let t2 = t.clone();
    let forget = tokio::spawn(async move { t2.forget(UID).await });
    returns_promptly("Forget cancelling the operation", op.ctx.done()).await;
    returns_promptly("lock while Forget waits", t.inner.lock(UID)).await;
    assert!(in_memory(&t, UID).await, "kept while Forget waits");
    assert_eq!(
        uuid_of(&n.plain().state(UID).await.latest_local),
        "s1",
        "a restarted keeper still reads the state while Forget waits"
    );
    settle().await;
    assert!(
        !forget.is_finished(),
        "Forget returned before the operation ended"
    );

    end_op(&t, &op, done).await;
    returns_promptly("Forget", forget).await.unwrap().unwrap();
    assert!(!in_memory(&t, UID).await);
    assert_eq!(
        n.plain().state(UID).await,
        pb::ActorSnapshotState::default()
    );
}

/// From BeginTerminate on no background work begins; a terminate that fails
/// before Forget lets it begin again, and after Forget nothing is queued.
#[tokio::test(start_paused = true)]
async fn terminate_holds_off_background_work() {
    for forget in [false, true] {
        let n = Node::new();
        let t = n.store_tracker(&RecStore::new(), |_| {});
        let start = now(&t);
        t.commit_local(UID, test_ref("s1", 1, Some(start))).await;
        step_to(&t, at(start, DETACH_DELAY)).await;
        eventually("upload and detach are queued", async || queued_now(&t) == 2).await;
        drain_queues(&t).await;

        let end = t.begin_terminate(UID).await;
        for kind in OpKind::ALL {
            if let Some(run) = t.inner.begin_due_work(UID, kind).await {
                run.await;
                panic!("{} began during Terminate", kind.name());
            }
        }
        if forget {
            let (op, done) = begin_op(&t, test_ref("s1", 1, Some(start))).await;
            let t2 = t.clone();
            let op2 = op.clone();
            let ender = tokio::spawn(async move {
                op2.ctx.done().await;
                end_op(&t2, &op2, done).await;
            });
            t.forget(UID).await.unwrap();
            ender.await.unwrap();
        }
        end.end().await;

        if forget {
            assert!(!in_memory(&t, UID).await);
            assert_eq!(queued_now(&t), 0, "nothing queued for a forgotten actor");
            continue;
        }
        let run = t
            .inner
            .begin_due_work(UID, OpKind::Upload)
            .await
            .expect("the due upload begins once the terminate failed");
        run.await;
    }
}

/// Commits, LocalSnapshotsToKeep and State neither wait for nor cancel the
/// operation in progress.
#[tokio::test(start_paused = true)]
async fn commits_do_not_wait_for_in_flight_op() {
    let n = Node::new();
    let t = n.plain();
    let s1 = test_ref("s1", 1, Some(t0()));
    t.commit_local(UID, s1.clone()).await;
    let (op, done) = begin_op(&t, s1).await;
    returns_promptly(
        "CommitLocal",
        t.commit_local(UID, test_ref("s2", 2, Some(t0()))),
    )
    .await;
    returns_promptly("LocalSnapshotsToKeep", t.local_snapshots_to_keep(UID)).await;
    returns_promptly("State", t.state(UID)).await;
    assert!(op.ctx.err().is_none(), "the operation still runs");
    end_op(&t, &op, done).await;
}

/// One operation per actor; another actor's does not stand in the way; the
/// operation keeps its own copy of its target.
#[tokio::test(start_paused = true)]
async fn begin_op_allows_one_op_per_actor() {
    let n = Node::bare();
    let t = n.plain();
    let mut s1 = test_ref("s1", 1, Some(t0()));
    let (op, done) = begin_op(&t, s1.clone()).await;
    {
        let mut g = t.inner.lock(UID).await;
        t.inner.find_or_add(&mut g);
        assert!(t
            .inner
            .begin_op(&mut g, t.inner.ctx.clone(), OpKind::Upload, s1.clone())
            .is_none());
    }
    {
        let mut g = t.inner.lock("other-actor-uid").await;
        t.inner.find_or_add(&mut g);
        assert!(t
            .inner
            .begin_op(&mut g, t.inner.ctx.clone(), OpKind::Upload, s1.clone())
            .is_some());
    }
    end_op(&t, &op, done).await;
    let (op, _done) = begin_op(&t, s1.clone()).await;
    s1.snapshot.snapshot_uuid = "changed".into();
    assert_eq!(op.target.uuid(), "s1");
}

/// A failure holds the actor back, doubling from the retry delay to its cap;
/// a commit of any kind or Forget starts the doubling over.
#[tokio::test(start_paused = true)]
async fn backoff() {
    let n = Node::new();
    let t = n.plain();
    async fn fail(t: &Tracker) -> Duration {
        let mut g = t.inner.lock(UID).await;
        t.inner.find_or_add(&mut g);
        crate::clock::until(now(t), t.inner.back_off(&mut g))
    }
    for (i, want) in [1u32, 2, 4, 8, 10, 10].into_iter().enumerate() {
        assert_eq!(fail(&t).await, MINUTE * want, "failure {}", i + 1);
    }
    for lift in ["CommitLocal", "CommitDurable", "CommitPromoted", "Forget"] {
        fail(&t).await;
        fail(&t).await;
        match lift {
            "CommitLocal" => {
                t.commit_local(UID, test_ref("s1", 1, Some(now(&t)))).await;
            }
            "CommitDurable" => {
                t.commit_durable(UID, test_ref("s2", 2, Some(now(&t))))
                    .await;
            }
            "CommitPromoted" => {
                t.commit_promoted(UID, test_ref("s3", 3, Some(now(&t))))
                    .await;
            }
            _ => t.forget(UID).await.unwrap(),
        }
        assert_eq!(fail(&t).await, SNAPSHOT_RETRY_DELAY, "after {lift}");
    }
}

/// A finished operation logs at info, a failed one as a warning, and a
/// canceled one at info, in the background or not.
#[tokio::test(start_paused = true)]
async fn log_op_levels() {
    let canceled = Ctx::background();
    canceled.cancel();
    let live = Ctx::background();
    for (name, ctx, background, err, want) in [
        (
            "finished",
            &live,
            true,
            None,
            "INFO Snapshot operation finished",
        ),
        (
            "background failure",
            &live,
            true,
            Some(Status::unknown("boom")),
            "WARN Snapshot operation failed",
        ),
        (
            "background canceled",
            &canceled,
            true,
            Some(Status::cancelled("canceled")),
            "INFO Snapshot operation canceled",
        ),
        (
            "foreground failure",
            &live,
            false,
            Some(Status::unknown("boom")),
            "WARN Snapshot operation failed",
        ),
        (
            "foreground canceled",
            &canceled,
            false,
            Some(Status::cancelled("canceled")),
            "INFO Snapshot operation canceled",
        ),
    ] {
        let logs = LogCapture::start();
        log_op(
            ctx,
            UID,
            OpKind::Upload,
            &test_ref("s1", 1, None),
            false,
            background,
            SECOND,
            err.as_ref(),
        );
        assert_eq!(logs.with_prefix("Snapshot operation"), vec![want], "{name}");
    }
}

/// A failed background detach logs a warning rather than a cancellation.
#[tokio::test(start_paused = true)]
async fn background_detach_failure_logs_warning() {
    let n = Node::new();
    let store = RecStore::new();
    store.set_err(Some(Status::unknown("boom")));
    let t = n.store_tracker(&store, |_| {});
    let start = now(&t);
    t.commit_local(UID, test_ref("s1", 1, Some(start))).await;
    let logs = LogCapture::start();
    step_to(&t, at(start, DETACH_DELAY)).await;
    process_next(&t, OpKind::Detach).await;
    assert_eq!(
        logs.with_prefix("Snapshot operation"),
        vec!["WARN Snapshot operation failed"]
    );
}

/// A context with a timeout ends with DeadlineExceeded once tokio time
/// reaches its deadline, and with Canceled when it or its parent is
/// cancelled first.
#[tokio::test(start_paused = true)]
async fn timeout_contexts_follow_tokio_time() {
    // The deadline is reached.
    let ctx = Ctx::background().with_timeout(MINUTE);
    assert_eq!(ctx.deadline(), Some(tokio::time::Instant::now() + MINUTE));
    tokio::time::advance(MINUTE).await;
    assert_eq!(
        returns_promptly("timeout", ctx.done()).await,
        CtxError::DeadlineExceeded
    );
    assert_eq!(ctx.err(), Some(CtxError::DeadlineExceeded));

    // The parent is canceled first.
    let parent = Ctx::background();
    let ctx = parent.with_timeout(MINUTE);
    parent.cancel();
    assert_eq!(
        returns_promptly("parent cancel", ctx.done()).await,
        CtxError::Canceled
    );
    assert_eq!(ctx.err(), Some(CtxError::Canceled));

    // It is canceled first.
    let ctx = Ctx::background().with_timeout(MINUTE);
    ctx.cancel();
    assert_eq!(
        returns_promptly("cancel", ctx.done()).await,
        CtxError::Canceled
    );
    assert_eq!(ctx.err(), Some(CtxError::Canceled));
}

/// An upload attempt times out on tokio time, whatever the tracker's clock
/// reads.
#[tokio::test(start_paused = true)]
async fn upload_timeout_runs_on_tokio_time() {
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let long_ago = DateTime::from_timestamp(946_684_800, 0).unwrap();
    let t = n.tracker_at(Some(store.clone()), long_ago, Node::opts());
    t.start().await;
    t.commit_local(UID, test_ref("s1", 1, Some(now(&t)))).await;
    let op = start_upload(&t, &store).await;
    let deadline = store.last_deadline().expect("the upload has a deadline");
    assert_eq!(deadline - tokio::time::Instant::now(), UPLOAD_TIMEOUT);
    store.release(None).await;
    wait_op(&op, "the upload").await;
}

/// A timed-out background upload logs a warning: only the attempt timed out,
/// not the operation.
#[tokio::test(start_paused = true)]
async fn background_upload_timeout_logs_warning() {
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let t = n.store_tracker(&store, |_| {});
    t.start().await;
    let logs = LogCapture::start();
    t.commit_local(UID, test_ref("s1", 1, Some(now(&t)))).await;
    let op = start_upload(&t, &store).await;
    step_to(&t, at(now(&t), UPLOAD_TIMEOUT)).await;
    wait_op(&op, "the timed-out upload").await;
    assert_eq!(
        logs.with_prefix("Snapshot operation"),
        vec!["WARN Snapshot operation failed"]
    );
}

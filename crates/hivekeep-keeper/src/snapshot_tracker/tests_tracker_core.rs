//! Commits, holds, generation fencing, state-file persistence, and the
//! separate upload and detach queues.

use std::fs;

use hivekeep_proto::keeper::v1 as pb;
use tonic::{Code, Status};

use super::actor::{ActorRecord, ActorSnapshots};
use super::op::{ActorOp, OpKind};
use super::test_support_core::*;
use super::*;
use crate::context::Ctx;

fn code_of<T>(r: &Result<T, Status>) -> Code {
    match r {
        Ok(_) => Code::Ok,
        Err(e) => e.code(),
    }
}

#[test]
fn snapshot_ref_json_keeps_the_actor() {
    let mut r = test_ref("s1", 1, Some(t0()));
    r.actor = ActorRef::new("demo", "counter");
    let data = serde_json::to_vec(&r).unwrap();
    let got: SnapshotRef = serde_json::from_slice(&data).unwrap();
    assert_eq!(got.actor, r.actor);
    assert_eq!(got.snapshot, r.snapshot);
    assert_eq!(got.commit_time, r.commit_time);
}

/// The state reports what CommitLocal and CommitDurable record; each update
/// is saved and evicted from memory; Forget deletes the file.
#[tokio::test(start_paused = true)]
async fn tracker_state() {
    let n = Node::new();
    let t = n.plain();
    let start = t0();
    let st = t.commit_local(UID, test_ref("s1", 1, Some(start))).await;
    assert_eq!(uuid_of(&st.latest_local), "s1");
    assert_eq!(
        time_of(&st.latest_local.as_ref().unwrap().local_commit_time),
        Some(start)
    );
    assert!(st.latest_durable.is_none());
    assert_eq!(actors_in_memory(&t), 0, "evicted after writing the file");

    let s2 = test_ref("s2", 2, Some(at(start, MINUTE)));
    let mut durable = s2.clone();
    durable.upload_time = Some(at(at(start, MINUTE), SECOND));
    let st = t.commit_durable(UID, durable.clone()).await;
    assert!(st.latest_local.is_none(), "s2 supersedes s1");
    assert_eq!(uuid_of(&st.latest_durable), "s2");
    assert_eq!(
        time_of(&st.latest_durable.as_ref().unwrap().durable_complete_time),
        durable.upload_time
    );
    assert_eq!(actors_in_memory(&t), 0);

    let st = t.commit_local(UID, s2.clone()).await;
    let local = st.latest_local.as_ref().unwrap();
    assert_eq!(uuid_of(&st.latest_local), "s2");
    assert_eq!(time_of(&local.local_commit_time), s2.commit_time);
    assert!(local.durable_complete_time.is_none());

    let st = t
        .commit_local(UID, test_ref("s3", 3, Some(at(start, 2 * MINUTE))))
        .await;
    assert_eq!(uuid_of(&st.latest_local), "s3");
    assert_eq!(uuid_of(&st.latest_durable), "s2");
    assert_eq!(t.state(UID).await, st);
    assert_eq!(n.plain().state(UID).await, st, "read from the state file");

    t.forget(UID).await.unwrap();
    assert!(!n.paths.actor_snapshot_state_file(UID).exists());
    assert_eq!(t.state(UID).await, pb::ActorSnapshotState::default());
}

/// CommitDurable drops an older local snapshot but keeps a local copy of the
/// durable snapshot itself.
#[tokio::test(start_paused = true)]
async fn commit_durable_drops_older_local() {
    let n = Node::new();
    let t = n.plain();
    assert!(t.local_snapshots_to_keep(UID).await.is_empty());
    t.commit_local(UID, test_ref("s1", 1, Some(t0()))).await;
    assert_eq!(t.local_snapshots_to_keep(UID).await, vec!["s1"]);
    let s2 = test_ref("s2", 2, Some(at(t0(), MINUTE)));
    t.commit_durable(UID, s2.clone()).await;
    let st = n.plain().state(UID).await;
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s2");
    assert!(t.local_snapshots_to_keep(UID).await.is_empty());

    t.commit_local(UID, s2.clone()).await;
    let st = t.commit_durable(UID, s2).await;
    assert_eq!(
        uuid_of(&st.latest_local),
        "s2",
        "the same snapshot's copy stays"
    );
}

/// CommitPromoted makes the local snapshot durable keeping its commit time;
/// a retry keeps what was recorded; another or an older snapshot leaves the
/// newer ones.
#[tokio::test(start_paused = true)]
async fn commit_promoted() {
    let n = Node::new();
    let t = n.plain();
    let start = t0();
    t.commit_local(UID, test_ref("s1", 1, Some(start))).await;

    let mut promoted = test_ref("s1", 1, None);
    promoted.upload_time = Some(at(start, MINUTE));
    let st = t.commit_promoted(UID, promoted.clone()).await;
    let local = st.latest_local.as_ref().unwrap();
    assert_eq!(uuid_of(&st.latest_local), "s1");
    assert_eq!(time_of(&local.local_commit_time), Some(start));
    assert!(local.durable_complete_time.is_none());
    let d = st.latest_durable.as_ref().unwrap();
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert_eq!(time_of(&d.local_commit_time), Some(start));
    assert_eq!(time_of(&d.durable_complete_time), promoted.upload_time);
    assert_eq!(n.plain().state(UID).await, st);

    let mut retry = test_ref("s1", 1, None);
    retry.upload_time = Some(at(start, UPLOAD_DELAY));
    assert_eq!(t.commit_promoted(UID, retry).await, st);

    t.commit_local(UID, test_ref("s2", 2, Some(at(start, 2 * UPLOAD_DELAY))))
        .await;
    let mut other = test_ref("s3", 3, None);
    other.upload_time = Some(at(start, 3 * UPLOAD_DELAY));
    let st = t.commit_promoted(UID, other).await;
    assert_eq!(uuid_of(&st.latest_durable), "s3");
    assert!(st
        .latest_durable
        .as_ref()
        .unwrap()
        .local_commit_time
        .is_none());
    assert_eq!(uuid_of(&st.latest_local), "s2");

    t.commit_local(UID, test_ref("s4", 4, Some(at(start, 4 * UPLOAD_DELAY))))
        .await;
    let mut older = test_ref("s2", 2, None);
    older.upload_time = Some(at(start, 5 * UPLOAD_DELAY));
    let st = t.commit_promoted(UID, older).await;
    assert_eq!(uuid_of(&st.latest_durable), "s3");
    assert_eq!(uuid_of(&st.latest_local), "s4");
}

/// A promote of the local snapshot leaves its detach schedule as it is.
#[tokio::test(start_paused = true)]
async fn commit_promoted_keeps_detach_schedule() {
    for started in [false, true] {
        let n = Node::new();
        let store = RecStore::new();
        let t = n.store_tracker(&store, |_| {});
        t.commit_local(UID, test_ref("s1", 1, Some(now(&t)))).await;
        let mut want = Some(at(now(&t), DETACH_DELAY));
        if started {
            t.begin_start(&Ctx::background(), UID)
                .await
                .unwrap()
                .end()
                .await;
            want = None;
        }
        step_to(&t, at(now(&t), UPLOAD_DELAY)).await;
        t.commit_promoted(UID, test_ref("s1", 1, None)).await;
        assert_eq!(detach_due_of(&t, UID).await, want, "started={started}");
    }
}

/// Promoting the latest local snapshot clears its upload due time; promoting
/// an older one keeps the upload due when the oldest undurable snapshot was,
/// including any retry backoff.
#[tokio::test(start_paused = true)]
async fn commit_promoted_clears_upload_due() {
    let n = Node::new();
    let store = RecStore::new();
    let t = n.store_tracker(&store, |_| {});
    let start = now(&t);
    let st = t.commit_local(UID, test_ref("s1", 1, Some(start))).await;
    assert!(st.upload_due_time.is_some());

    let mut promoted = test_ref("s1", 1, None);
    promoted.upload_time = Some(at(start, MINUTE));
    let st = t.commit_promoted(UID, promoted).await;
    assert!(st.upload_due_time.is_none());

    let t2 = at(start, 2 * MINUTE);
    t.commit_local(UID, test_ref("s2", 2, Some(t2))).await;
    t.commit_local(UID, test_ref("s3", 3, Some(at(t2, MINUTE))))
        .await;
    t.commit_local(UID, test_ref("s4", 4, Some(at(t2, 2 * MINUTE))))
        .await;

    let mut older = test_ref("s0", 0, None);
    older.upload_time = Some(at(t2, 3 * MINUTE));
    let st = t.commit_promoted(UID, older.clone()).await;
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert_eq!(time_of(&st.upload_due_time), Some(at(t2, UPLOAD_DELAY)));

    let mut between = test_ref("s2", 2, None);
    between.upload_time = Some(at(t2, 3 * MINUTE));
    let st = t.commit_promoted(UID, between).await;
    assert_eq!(uuid_of(&st.latest_durable), "s2");
    assert_eq!(time_of(&st.upload_due_time), Some(at(t2, UPLOAD_DELAY)));

    step_to(&t, at(t2, UPLOAD_DELAY)).await;
    store.set_err(Some(Status::unknown("boom")));
    assert!(t.detach(&Ctx::background(), UID).await.is_err());
    let want_retry = at(now(&t), SNAPSHOT_RETRY_DELAY);
    let st = t.commit_promoted(UID, older).await;
    assert_eq!(time_of(&st.upload_due_time), Some(want_retry));
    let mut between_backoff = test_ref("s3", 3, None);
    between_backoff.upload_time = Some(now(&t));
    let st = t.commit_promoted(UID, between_backoff).await;
    assert_eq!(uuid_of(&st.latest_durable), "s3");
    assert_eq!(time_of(&st.upload_due_time), Some(want_retry));
}

/// A failed save keeps the entry in memory, and it is reported from there
/// until a later save succeeds.
#[tokio::test(start_paused = true)]
async fn keeps_unsaved_state_in_memory() {
    let n = Node::bare();
    let state_file = n.paths.actor_snapshot_state_file(UID);
    // A directory in the state file's place makes the rename fail.
    fs::create_dir_all(&state_file).unwrap();
    let t = n.plain();
    let st = t.commit_local(UID, test_ref("s1", 1, Some(t0()))).await;
    assert_eq!(actors_in_memory(&t), 1);
    assert_eq!(t.state(UID).await, st);

    fs::remove_dir(&state_file).unwrap();
    let st = t
        .commit_local(UID, test_ref("s2", 2, Some(at(t0(), MINUTE))))
        .await;
    assert_eq!(actors_in_memory(&t), 0);
    assert_eq!(n.plain().state(UID).await, st);
}

/// A save finding the actor's directory gone writes nothing and drops the
/// entry.
#[tokio::test(start_paused = true)]
async fn drops_actor_without_directory() {
    let n = Node::bare();
    let t = n.plain();
    t.commit_local(UID, test_ref("s1", 1, Some(t0()))).await;
    assert_eq!(actors_in_memory(&t), 0);
    assert!(!n.paths.actor_snapshot_state_file(UID).exists());
    assert_eq!(t.state(UID).await, pb::ActorSnapshotState::default());
}

/// Commits for one actor take turns, and an older local commit never replaces
/// a newer durable snapshot.
#[tokio::test(start_paused = true)]
async fn serializes_each_actor() {
    let n = Node::new();
    let t = n.plain();
    for i in 0..20 {
        let local = test_ref(&format!("local-{i}"), 2 * i + 1, Some(t0()));
        let durable = test_ref(&format!("durable-{i}"), 2 * i + 2, Some(t0()));
        let (a, b) = (t.clone(), t.clone());
        let h1 = tokio::spawn(async move { a.commit_local(UID, local).await });
        let d2 = durable.clone();
        let h2 = tokio::spawn(async move { b.commit_durable(UID, d2).await });
        h1.await.unwrap();
        h2.await.unwrap();
        let st = t.state(UID).await;
        assert_eq!(uuid_of(&st.latest_durable), durable.uuid(), "round {i}");
        assert!(st.latest_local.is_none(), "round {i}");
    }
    assert_eq!(actors_in_memory(&t), 0);
}

/// Holding one actor's lock does not hold up another actor's commit.
#[tokio::test(start_paused = true)]
async fn locks_each_actor_separately() {
    let n = Node::bare();
    let t = n.plain();
    let _other = t.inner.lock("other-actor-uid").await;
    returns_promptly(
        "CommitLocal while another actor is locked",
        t.commit_local(UID, test_ref("s1", 1, Some(t0()))),
    )
    .await;
}

async fn check_begin(t: &Tracker, r: &SnapshotRef, want: Code, what: &str) {
    for mode in [pb::UploadMode::Deferred, pb::UploadMode::Immediate] {
        let res = t
            .begin_checkpoint(&Ctx::background(), UID, &r.snapshot, mode)
            .await;
        let code = code_of(&res);
        if let Ok((_, end)) = res {
            end.end().await;
        }
        assert_eq!(
            code,
            want,
            "{what}: BeginCheckpoint({}, {mode:?})",
            r.uuid()
        );
    }
    let res = t.begin_promote(&Ctx::background(), UID, r).await;
    let code = code_of(&res);
    if let Ok((_, end)) = res {
        end.end().await;
    }
    assert_eq!(code, want, "{what}: BeginPromote({})", r.uuid());
}

/// With nothing tracked any generation is accepted; otherwise one below the
/// newest snapshot, or equal to it under another UUID, is Aborted.
#[tokio::test(start_paused = true)]
async fn check_generation() {
    let n = Node::new();
    let t = n.plain();
    let r = |u: &str, g: i32| test_ref(u, g, Some(t0()));

    check_begin(&t, &r("s1", 1), Code::Ok, "untracked").await;

    t.commit_local(UID, r("s1", 2)).await;
    for (name, rf, want) in [
        ("same snapshot at newest generation", r("s1", 2), Code::Ok),
        ("higher generation", r("s2", 3), Code::Ok),
        ("generation below newest", r("s0", 1), Code::Aborted),
        ("same uuid at lower generation", r("s1", 1), Code::Aborted),
        (
            "different uuid at newest generation",
            r("other", 2),
            Code::Aborted,
        ),
    ] {
        check_begin(&t, &rf, want, &format!("localNewest/{name}")).await;
        check_begin(
            &n.plain(),
            &rf,
            want,
            &format!("localNewest/{name} (fresh)"),
        )
        .await;
    }

    t.commit_durable(UID, r("s2", 4)).await;
    for (name, rf, want) in [
        ("same snapshot at newest generation", r("s2", 4), Code::Ok),
        ("higher generation", r("s3", 5), Code::Ok),
        ("generation below newest", r("s1", 3), Code::Aborted),
        (
            "different uuid at newest generation",
            r("other", 4),
            Code::Aborted,
        ),
    ] {
        check_begin(&t, &rf, want, &format!("durableNewest/{name}")).await;
    }

    t.commit_local(UID, r("s3", 6)).await;
    check_begin(&t, &r("s2", 4), Code::Aborted, "both, older durable").await;
    check_begin(&t, &r("s3", 6), Code::Ok, "both, newest local").await;

    // A durable snapshot newer than the local one is the newest.
    let mut a = ActorSnapshots::new(
        1,
        UID,
        ActorRecord {
            latest_local: Some(r("local", 3)),
            latest_durable: Some(r("durable", 5)),
            ..Default::default()
        },
    );
    let check =
        |a: &ActorSnapshots, u: &str, g: i32| code_of(&a.check_generation(&r(u, g).snapshot));
    assert_eq!(check(&a, "local", 3), Code::Aborted);
    assert_eq!(check(&a, "other", 5), Code::Aborted);
    assert_eq!(check(&a, "durable", 5), Code::Ok);

    // A newer Uploading is the newest even while no operation runs.
    a.rec.uploading = Some(r("saved-uploading", 6));
    assert_eq!(check(&a, "durable", 5), Code::Aborted);
    assert_eq!(check(&a, "other", 6), Code::Aborted);
    assert_eq!(check(&a, "saved-uploading", 6), Code::Ok);

    // A newer operation target is the newest.
    a.op = Some(ActorOp::new(OpKind::Upload, r("uploading", 7), Ctx::background()).0);
    assert_eq!(check(&a, "saved-uploading", 6), Code::Aborted);
    assert_eq!(check(&a, "other", 7), Code::Aborted);
    assert_eq!(check(&a, "uploading", 7), Code::Ok);
}

/// BeginCheckpoint reports whether its snapshot is committed already and
/// refuses one committed under the same UUID but named differently.
#[tokio::test(start_paused = true)]
async fn begin_checkpoint_reports_committed() {
    let s1 = test_ref("s1", 1, None);
    let renamed = |f: fn(&mut pb::Snapshot)| {
        let mut s = s1.snapshot.clone();
        f(&mut s);
        s
    };
    #[derive(Clone, Copy)]
    enum Commit {
        None,
        Local,
        Durable,
    }
    let cases: Vec<(&str, Commit, pb::Snapshot, bool, Code)> = vec![
        (
            "not committed",
            Commit::None,
            s1.snapshot.clone(),
            false,
            Code::Ok,
        ),
        (
            "committed locally",
            Commit::Local,
            s1.snapshot.clone(),
            true,
            Code::Ok,
        ),
        (
            "durable only",
            Commit::Durable,
            s1.snapshot.clone(),
            true,
            Code::Ok,
        ),
        (
            "committed with another generation",
            Commit::Durable,
            renamed(|s| s.generation = 2),
            false,
            Code::FailedPrecondition,
        ),
        (
            "committed with another fidelity",
            Commit::Durable,
            renamed(|s| s.fidelity = pb::SnapshotFidelity::Volumes as i32),
            false,
            Code::FailedPrecondition,
        ),
        (
            "committed with another uri",
            Commit::Durable,
            renamed(|s| s.object.as_mut().unwrap().snapshot_uri.push_str("-other")),
            false,
            Code::FailedPrecondition,
        ),
    ];
    for (name, commit, snap, want_committed, want_code) in cases {
        let n = Node::new();
        let t = n.plain();
        match commit {
            Commit::None => {}
            Commit::Local => {
                t.commit_local(UID, s1.clone()).await;
            }
            Commit::Durable => {
                t.commit_durable(UID, s1.clone()).await;
            }
        }
        let res = t
            .begin_checkpoint(&Ctx::background(), UID, &snap, pb::UploadMode::Deferred)
            .await;
        let code = code_of(&res);
        let committed = match res {
            Ok((c, end)) => {
                end.end().await;
                c
            }
            Err(_) => false,
        };
        assert_eq!((committed, code), (want_committed, want_code), "{name}");
    }
}

/// NotStartedSince holds for a committed snapshot until a Run or Restore
/// begins, and a fresh tracker reads the same from the state file.
#[tokio::test(start_paused = true)]
async fn not_started_since() {
    #[derive(Clone, Copy)]
    enum Step {
        Local(i32),
        Durable(i32),
        Start,
    }
    let snap = |g: i32| test_ref(&format!("s{g}"), g, None);
    let cases: Vec<(&str, Vec<Step>, pb::Snapshot, bool)> = vec![
        ("not committed", vec![], snap(1).snapshot, false),
        (
            "committed locally",
            vec![Step::Local(1)],
            snap(1).snapshot,
            true,
        ),
        (
            "another snapshot",
            vec![Step::Local(1)],
            test_ref("other", 1, None).snapshot,
            false,
        ),
        (
            "durable only",
            vec![Step::Durable(1)],
            snap(1).snapshot,
            true,
        ),
        (
            "started since a local commit",
            vec![Step::Local(1), Step::Start],
            snap(1).snapshot,
            false,
        ),
        (
            "started since a durable commit",
            vec![Step::Durable(1), Step::Start],
            snap(1).snapshot,
            false,
        ),
        (
            "committed locally since a start",
            vec![Step::Local(1), Step::Start, Step::Local(2)],
            snap(2).snapshot,
            true,
        ),
        (
            "committed durably since a start",
            vec![Step::Local(1), Step::Start, Step::Durable(2)],
            snap(2).snapshot,
            true,
        ),
    ];
    for (name, steps, s, want) in cases {
        let n = Node::new();
        let t = n.plain();
        for step in steps {
            match step {
                Step::Local(g) => {
                    t.commit_local(UID, snap(g)).await;
                }
                Step::Durable(g) => {
                    t.commit_durable(UID, snap(g)).await;
                }
                Step::Start => {
                    t.begin_start(&Ctx::background(), UID)
                        .await
                        .unwrap()
                        .end()
                        .await
                }
            }
        }
        assert_eq!(t.not_started_since(UID, &s).await, want, "{name}");
        assert_eq!(
            n.plain().not_started_since(UID, &s).await,
            want,
            "{name}: fresh tracker"
        );
    }
}

/// A promote inside an immediate Checkpoint's hold restores that hold when
/// it ends, so detaches stay blocked until the checkpoint ends.
#[tokio::test(start_paused = true)]
async fn promote_inside_checkpoint_keeps_hold() {
    let n = Node::new();
    let store = RecStore::new();
    let t = n.store_tracker(&store, |_| {});
    let s1 = test_ref("s1", 1, Some(now(&t)));
    t.commit_local(UID, s1.clone()).await;

    let (_, end_checkpoint) = t
        .begin_checkpoint(
            &Ctx::background(),
            UID,
            &s1.snapshot,
            pb::UploadMode::Immediate,
        )
        .await
        .unwrap();
    let (_, end_promote) = t.begin_promote(&Ctx::background(), UID, &s1).await.unwrap();
    t.commit_promoted(UID, s1.clone()).await;
    end_promote.end().await;

    step_to(&t, detach_due_of(&t, UID).await.unwrap()).await;
    if let Some(run) = t.inner.begin_due_work(UID, OpKind::Detach).await {
        run.await;
        panic!("a due detach began while the Checkpoint holds the actor");
    }
    end_checkpoint.end().await;
    let run = t
        .inner
        .begin_due_work(UID, OpKind::Detach)
        .await
        .expect("the due detach begins once the Checkpoint ends");
    run.await;
}

/// Commits skip older generations, and an older CommitDurable leaves a newer
/// LatestLocal's upload schedule and backoff intact.
#[tokio::test(start_paused = true)]
async fn commits_skip_older_generations() {
    let n = Node::new();
    let store = RecStore::new();
    let t = n.store_tracker(&store, |_| {});
    let start = now(&t);

    let st = t.commit_local(UID, test_ref("s3", 3, Some(start))).await;
    for stale in [
        test_ref("s2", 2, Some(at(start, MINUTE))),
        test_ref("other", 3, Some(at(start, MINUTE))),
        test_ref("s3", 3, Some(at(start, MINUTE))),
    ] {
        let what = stale.uuid().to_string();
        assert_eq!(t.commit_local(UID, stale).await, st, "CommitLocal({what})");
        assert_eq!(t.local_snapshots_to_keep(UID).await, vec!["s3"]);
    }

    let mut s4 = test_ref("s4", 4, Some(at(start, 2 * MINUTE)));
    s4.upload_time = Some(at(start, 2 * MINUTE));
    let st = t.commit_durable(UID, s4).await;
    for stale in [
        test_ref("s2", 2, Some(at(start, 3 * MINUTE))),
        test_ref("other", 4, Some(at(start, 3 * MINUTE))),
        test_ref("s4", 4, Some(at(start, 3 * MINUTE))),
    ] {
        let what = stale.uuid().to_string();
        assert_eq!(
            t.commit_durable(UID, stale).await,
            st,
            "CommitDurable({what})"
        );
    }
    for stale in [
        test_ref("s3", 3, Some(at(start, 3 * MINUTE))),
        test_ref("other", 4, Some(at(start, 3 * MINUTE))),
    ] {
        let what = stale.uuid().to_string();
        assert_eq!(
            t.commit_local(UID, stale).await,
            st,
            "CommitLocal({what}) below durable"
        );
    }

    let t6 = at(start, 4 * MINUTE);
    t.commit_local(UID, test_ref("s6", 6, Some(t6))).await;
    step_to(&t, at(t6, UPLOAD_DELAY)).await;
    store.set_err(Some(Status::unknown("boom")));
    assert!(t.detach(&Ctx::background(), UID).await.is_err());
    let want_retry = at(now(&t), SNAPSHOT_RETRY_DELAY);

    let mut s5 = test_ref("s5", 5, Some(at(t6, MINUTE)));
    s5.upload_time = Some(now(&t));
    let st = t.commit_durable(UID, s5).await;
    assert_eq!(uuid_of(&st.latest_durable), "s5");
    assert_eq!(uuid_of(&st.latest_local), "s6");
    assert_eq!(time_of(&st.upload_due_time), Some(want_retry));
}

/// Uploads and detaches have workers of their own: a due detach runs while a
/// slow upload holds the only upload worker, and an upload that comes due
/// meanwhile waits for that worker.
#[tokio::test(start_paused = true)]
async fn detach_does_not_wait_behind_uploads() {
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let t = n.store_tracker(&store, |o| {
        o.upload_workers = 1;
        o.detach_workers = 1;
    });
    t.start().await;
    for id in ["actor-1", "actor-2", "actor-3"] {
        n.actor_dir(id);
    }

    // actor-2's snapshot is durable already, so its detach only prunes.
    let start = now(&t);
    let a2 = test_ref("a2-s1", 1, Some(start));
    t.commit_durable("actor-2", a2.clone()).await;
    t.commit_local("actor-2", a2).await;
    step_to(&t, before(at(start, DETACH_DELAY), MINUTE)).await;

    let taken = before(now(&t), UPLOAD_DELAY);
    t.commit_local("actor-1", test_ref("a1-s1", 1, Some(taken)))
        .await;
    assert_eq!(store.wait_started().await, "a1-s1");
    t.commit_local("actor-3", test_ref("a3-s1", 1, Some(taken)))
        .await;

    step_to(&t, at(start, DETACH_DELAY)).await;
    eventually("actor-2 is detached while actor-1 uploads", async || {
        t.state("actor-2").await.latest_local.is_none()
    })
    .await;
    assert_eq!(
        store.try_started(),
        None,
        "actor-1's upload holds the only worker"
    );

    store.release(None).await;
    assert_eq!(store.wait_started().await, "a3-s1");
    store.release(None).await;
}

/// Whichever of an upload and a detach due for the same actor starts first
/// holds the actor's operation, and the other queue's worker skips it.
#[tokio::test(start_paused = true)]
async fn same_actor_upload_and_detach_due_upload_first() {
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let t = n.store_tracker(&store, |_| {});
    let start = now(&t);
    t.commit_local(UID, test_ref("s1", 1, Some(start))).await;
    step_to(&t, at(start, DETACH_DELAY)).await;

    let t2 = t.clone();
    let upload = tokio::spawn(async move { process_next(&t2, OpKind::Upload).await });
    assert_eq!(store.wait_started().await, "s1");

    process_next(&t, OpKind::Detach).await;
    assert_eq!(store.try_started(), None);
    assert_eq!(t.inner.queues[OpKind::Detach as usize].len(), 0);
    assert_eq!(uuid_of(&t.state(UID).await.latest_local), "s1");

    store.release(None).await;
    returns_promptly("the upload worker", upload).await.unwrap();
    let st = t.state(UID).await;
    assert_eq!(uuid_of(&st.latest_local), "s1");
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert_eq!(
        t.inner.queues[OpKind::Detach as usize].len(),
        1,
        "the upload's end queues the detach"
    );

    process_next(&t, OpKind::Detach).await;
    assert_eq!(store.uploads(), vec!["s1"]);
    let st = t.state(UID).await;
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert_eq!(queued_now(&t), 0);
}

#[tokio::test(start_paused = true)]
async fn same_actor_upload_and_detach_due_detach_first() {
    let n = Node::new();
    let store = RecStore::new();
    store.set_blocking(true);
    let t = n.store_tracker(&store, |_| {});
    let start = now(&t);
    t.commit_local(UID, test_ref("s1", 1, Some(start))).await;
    step_to(&t, at(start, DETACH_DELAY)).await;

    let t2 = t.clone();
    let detach = tokio::spawn(async move { process_next(&t2, OpKind::Detach).await });
    assert_eq!(store.wait_started().await, "s1");

    process_next(&t, OpKind::Upload).await;
    assert_eq!(store.try_started(), None);
    assert_eq!(t.inner.queues[OpKind::Upload as usize].len(), 0);

    store.release(None).await;
    returns_promptly("the detach worker", detach).await.unwrap();
    assert_eq!(store.uploads(), vec!["s1"]);
    let st = t.state(UID).await;
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert_eq!(queued_now(&t), 0);
}

/// A Checkpoint hold keeps the detach off the local snapshots while the
/// upload still runs; once the hold ends the detach prunes without
/// uploading again.
#[tokio::test(start_paused = true)]
async fn upload_runs_during_checkpoint_hold_while_detach_waits() {
    let n = Node::new();
    let store = RecStore::new();
    let t = n.store_tracker(&store, |_| {});
    let start = now(&t);
    t.commit_local(UID, test_ref("s1", 1, Some(start))).await;

    let (_, end_checkpoint) = t
        .begin_checkpoint(
            &Ctx::background(),
            UID,
            &test_ref("s2", 2, None).snapshot,
            pb::UploadMode::Deferred,
        )
        .await
        .unwrap();
    step_to(&t, at(start, DETACH_DELAY)).await;

    process_next(&t, OpKind::Detach).await;
    assert!(store.uploads().is_empty());
    assert_eq!(uuid_of(&t.state(UID).await.latest_local), "s1");
    assert_eq!(t.inner.queues[OpKind::Detach as usize].len(), 0);

    process_next(&t, OpKind::Upload).await;
    assert_eq!(store.uploads(), vec!["s1"]);
    let st = t.state(UID).await;
    assert_eq!(uuid_of(&st.latest_local), "s1");
    assert_eq!(uuid_of(&st.latest_durable), "s1");

    // The upload's end queues the detach, which still waits while held.
    process_next(&t, OpKind::Detach).await;
    assert_eq!(uuid_of(&t.state(UID).await.latest_local), "s1");

    end_checkpoint.end().await;
    process_next(&t, OpKind::Detach).await;
    assert_eq!(store.uploads(), vec!["s1"]);
    let st = t.state(UID).await;
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s1");
}

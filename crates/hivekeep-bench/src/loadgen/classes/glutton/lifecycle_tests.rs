use hivekeep_proto::api::v1 as api;
use tonic::Status;

use super::*;
use crate::glutton::fake;
use crate::loadgen::classes::glutton::test_support::{config, count};
use crate::loadgen::duration::Seconds;
use crate::loadgen::dynconfig::{Holder, Lifecycle, WaitTime, LIFECYCLE_MODE_ASYNC};
use crate::loadgen::fake_control::FakeControl;
use crate::loadgen::lifecycle::{CONCURRENT_UPDATE_MSG, CONFLICT_RETRY_ATTEMPTS};

fn mode(m: &str) -> GluttonKnobs {
    GluttonKnobs {
        lifecycle: Lifecycle {
            lifecycle_mode: m.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

async fn runtime(knobs: GluttonKnobs, api: Arc<FakeControl>) -> (TaskRuntime, fake::Server) {
    let srv = fake::Server::start(Default::default()).await;
    let mut cfg = config(api, &srv.url(), Holder::fixed(knobs));
    cfg.hive = "bench-test".into();
    (TaskRuntime::new(Arc::new(cfg)), srv)
}

fn ends_with(calls: &[String], tail: &[&str]) -> bool {
    calls.len() >= tail.len()
        && calls[calls.len() - tail.len()..]
            .iter()
            .zip(tail)
            .all(|(a, b)| a == b)
}

#[tokio::test]
async fn iterate_suspend_mode() {
    let api = FakeControl::new();
    let (rt, _srv) = runtime(mode(LIFECYCLE_MODE_SYNC), api.clone()).await;
    rt.iterate(0).await;
    let calls = api.calls();
    assert!(calls.contains(&"SuspendActor".to_string()), "{calls:?}");
    assert!(
        !calls.contains(&"SuspendActor(async)".to_string()),
        "{calls:?}"
    );
    for req in api.with(|s| s.suspend_reqs.clone()) {
        assert_eq!(
            req.options.unwrap().mode(),
            api::DurabilityMode::Synchronous
        );
    }
}

#[tokio::test]
async fn iterate_pause_mode() {
    let api = FakeControl::new();
    let (rt, _srv) = runtime(mode(LIFECYCLE_MODE_ASYNC), api.clone()).await;
    rt.iterate(0).await;
    let calls = api.calls();
    assert!(
        calls.contains(&"SuspendActor(async)".to_string()),
        "{calls:?}"
    );
    assert!(!calls.contains(&"SuspendActor".to_string()), "{calls:?}");
}

#[tokio::test]
async fn iterate_none_mode() {
    let api = FakeControl::new();
    let (rt, _srv) = runtime(mode(LIFECYCLE_MODE_NONE), api.clone()).await;
    rt.iterate(0).await;
    rt.iterate(0).await;
    let calls = api.calls();
    assert!(
        !calls.iter().any(|c| c.starts_with("SuspendActor")),
        "{calls:?}"
    );
    assert_eq!(
        count(&calls, "ResumeActor"),
        1,
        "first activation only: {calls:?}"
    );
    rt.shutdown().await;
    let after = api.calls();
    assert!(
        ends_with(&after, &["SuspendActor(async)", "DeleteActor"]),
        "{after:?}"
    );
}

fn user_with(rt: &TaskRuntime, name: &str, running: bool) {
    let mut a = GluttonActor::new(rt.cfg.clone(), name.into());
    a.actor_running = running;
    let u = GluttonUser {
        actors: vec![a],
        next_idx: 0,
    };
    rt.users
        .lock()
        .insert(0, Arc::new(tokio::sync::Mutex::new(u)));
}

#[tokio::test]
async fn shutdown_pause_mode_running_actor() {
    let api = FakeControl::new();
    let (rt, _srv) = runtime(mode(LIFECYCLE_MODE_ASYNC), api.clone()).await;
    user_with(&rt, "running-actor", true);
    rt.shutdown().await;
    assert!(
        ends_with(&api.calls(), &["SuspendActor(async)", "DeleteActor"]),
        "{:?}",
        api.calls()
    );
}

#[tokio::test]
async fn shutdown_stopped_actor_only_deleted() {
    let api = FakeControl::new();
    let (rt, _srv) = runtime(mode(LIFECYCLE_MODE_SYNC), api.clone()).await;
    user_with(&rt, "stopped-actor", false);
    rt.shutdown().await;
    assert_eq!(api.calls(), vec!["DeleteActor"]);
}

async fn replacement_runtime(
    resume_errs: Vec<Status>,
) -> (TaskRuntime, Arc<FakeControl>, fake::Server) {
    let api = FakeControl::new();
    api.with(|s| s.resume_errs.extend(resume_errs));
    let (rt, srv) = runtime(mode(LIFECYCLE_MODE_SYNC), api.clone()).await;
    (rt, api, srv)
}

/// FailedPrecondition is the left-in-SUSPENDING case: replaced at once.
#[tokio::test]
async fn iterate_replaces_actor_on_terminal_resume_failure() {
    let (rt, api, _srv) = replacement_runtime(vec![Status::failed_precondition(
        "AssignWorker prerequisite not met (got: ACTOR_STATE_SUSPENDING)",
    )])
    .await;
    rt.iterate(0).await;
    let calls = api.calls();
    assert_eq!(
        count(&calls, "CreateActor"),
        2,
        "initial + replacement: {calls:?}"
    );
    assert_eq!(count(&calls, "DeleteActor"), 1, "{calls:?}");
}

/// A saturated pool tells every user ResourceExhausted at once; the Actor
/// must survive the shortage however long it lasts.
#[tokio::test]
async fn iterate_keeps_actor_on_transient_resume_failure() {
    let iterations = MAX_CONSECUTIVE_FAILURES as usize + 2;
    let (rt, api, _srv) = replacement_runtime(vec![
        Status::resource_exhausted(
            "no worker has room for the actor"
        );
        iterations
    ])
    .await;
    for _ in 0..iterations {
        rt.iterate(0).await;
    }
    let calls = api.calls();
    assert_eq!(count(&calls, "DeleteActor"), 0, "{calls:?}");
    assert_eq!(count(&calls, "CreateActor"), 1, "{calls:?}");
}

use super::super::actor::MAX_CONSECUTIVE_FAILURES;

/// An exhausted conflict budget replaces the Actor only once it repeats
/// MAX_CONSECUTIVE_FAILURES times.
#[tokio::test]
async fn iterate_replaces_actor_after_repeated_conflicts() {
    let n = CONFLICT_RETRY_ATTEMPTS * MAX_CONSECUTIVE_FAILURES as usize;
    let (rt, api, _srv) =
        replacement_runtime(vec![Status::aborted(CONCURRENT_UPDATE_MSG); n]).await;
    for i in 0..MAX_CONSECUTIVE_FAILURES {
        rt.iterate(0).await;
        let want = usize::from(i == MAX_CONSECUTIVE_FAILURES - 1);
        assert_eq!(
            count(&api.calls(), "DeleteActor"),
            want,
            "after iteration {}",
            i + 1
        );
    }
}

/// A retryable suspend failure strands the Actor; the next iteration
/// re-drives the suspend rather than resume.
#[tokio::test]
async fn iterate_retries_stranded_hibernate() {
    let api = FakeControl::new();
    api.with(|s| {
        s.suspend_errs
            .push_back(Status::unavailable("hivekeep-api restarting"))
    });
    let (rt, _srv) = runtime(mode(LIFECYCLE_MODE_SYNC), api.clone()).await;
    rt.iterate(0).await;
    let before = api.calls();
    rt.iterate(0).await;
    let after = api.calls();
    assert_eq!(after.last().unwrap(), "SuspendActor", "{after:?}");
    assert_eq!(
        count(&after, "ResumeActor"),
        count(&before, "ResumeActor"),
        "{after:?}"
    );
    assert_eq!(count(&after, "DeleteActor"), 0, "{after:?}");
    rt.iterate(0).await;
    let fin = api.calls();
    assert_eq!(
        count(&fin, "ResumeActor"),
        count(&after, "ResumeActor") + 1,
        "{fin:?}"
    );
}

fn bare_actor() -> GluttonActor {
    let cfg = config(
        FakeControl::new(),
        "http://127.0.0.1:1",
        Holder::fixed(GluttonKnobs::default()),
    );
    GluttonActor::new(Arc::new(cfg), "sb-1".into())
}

#[test]
fn note_failure_resets_on_success() {
    let mut a = bare_actor();
    for _ in 0..MAX_CONSECUTIVE_FAILURES - 1 {
        assert!(
            !a.note_failure(&Status::aborted(CONCURRENT_UPDATE_MSG)),
            "below the threshold"
        );
    }
    a.note_success();
    assert!(
        !a.note_failure(&Status::aborted(CONCURRENT_UPDATE_MSG)),
        "the count must reset"
    );
}

/// Cluster-wide transient errors must not push an Actor toward the
/// threshold, or a long shortage would replace every Actor at once.
#[test]
fn note_failure_ignores_retry_later() {
    let mut a = bare_actor();
    for _ in 0..MAX_CONSECUTIVE_FAILURES * 2 {
        assert!(!a.note_failure(&Status::unavailable("down")));
    }
    assert_eq!(a.consecutive_failures, 0);
}

/// The wait and live windows read different keys; setting one must not
/// leak into the other.
#[tokio::test]
async fn wait_and_live_windows_are_independent() {
    let api = FakeControl::new();
    let k = GluttonKnobs {
        wait: WaitTime {
            min_wait: Seconds::from_millis(200),
            max_wait: Seconds::from_secs(1),
        },
        ..Default::default()
    };
    let (rt, _srv) = runtime(k, api.clone()).await;
    for _ in 0..100 {
        assert_eq!(rt.live_wait(), Duration::ZERO);
        let w = rt.dynamic_wait();
        assert!(
            w >= Duration::from_millis(200) && w <= Duration::from_secs(1),
            "{w:?}"
        );
    }
    let k = GluttonKnobs {
        min_live: Seconds::from_secs(9),
        max_live: Seconds::from_secs(14),
        ..Default::default()
    };
    let (rt, _srv) = runtime(k, api).await;
    for _ in 0..100 {
        assert_eq!(rt.dynamic_wait(), Duration::ZERO);
        let l = rt.live_wait();
        assert!(
            l >= Duration::from_secs(9) && l <= Duration::from_secs(14),
            "{l:?}"
        );
    }
}

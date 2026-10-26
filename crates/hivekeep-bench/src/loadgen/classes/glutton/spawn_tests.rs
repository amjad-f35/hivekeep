use std::sync::atomic::AtomicUsize;

use super::*;
use crate::loadgen::classes::glutton::test_support::{config, count, requests_total};
use crate::loadgen::dynconfig::Holder;
use crate::loadgen::fake_control::FakeControl;
use crate::loadgen::lifecycle::CONCURRENT_UPDATE_MSG;
use crate::testutil::{HttpServer, Resp};

async fn ping_server() -> HttpServer {
    HttpServer::start(|req| {
        if req.uri.path() == PING_ROUTE {
            Resp::ok(
                pb::PingResponse {
                    message: "pong".into(),
                }
                .encode_to_vec(),
            )
        } else {
            Resp::status(404, "")
        }
    })
    .await
}

/// A runtime whose stats are booked under a class name unique to the test,
/// so parallel tests do not see each other's counters.
fn runtime(
    api: Arc<FakeControl>,
    srv: &HttpServer,
    total: usize,
    concurrency: usize,
    knobs: SpawnKnobs,
    class: &str,
) -> SpawnRuntime {
    let mut cfg = config(api, &srv.url(), Holder::fixed(knobs));
    cfg.hive = "test-space".into();
    cfg.total_actors = total;
    cfg.spawn_concurrency = concurrency;
    cfg.actor_deadline = Duration::from_secs(5);
    let mut rt = SpawnRuntime::new(Arc::new(cfg));
    rt.user_class = class.into();
    rt
}

#[test]
fn decile_thresholds() {
    let all: Vec<usize> = DECILES.to_vec();
    type Fires = Vec<(usize, Vec<usize>)>;
    let cases: Vec<(usize, Fires)> = vec![
        (1, vec![(1, all)]),
        (
            5,
            vec![
                (1, vec![10, 20]),
                (2, vec![30, 40]),
                (3, vec![50, 60]),
                (4, vec![70, 80]),
                (5, vec![90, 100]),
            ],
        ),
        (10, (1..=10).map(|r| (r, vec![r * 10])).collect()),
        (100, vec![(10, vec![10]), (50, vec![50]), (100, vec![100])]),
    ];
    for (total, fires) in cases {
        for (rank, want) in fires {
            assert_eq!(
                fired_deciles(rank, total),
                want,
                "total={total} rank={rank}"
            );
        }
    }
}

#[test]
fn is_terminal_error() {
    for code in [
        Code::NotFound,
        Code::DataLoss,
        Code::AlreadyExists,
        Code::FailedPrecondition,
        Code::InvalidArgument,
        Code::PermissionDenied,
        Code::Unauthenticated,
        Code::Unimplemented,
    ] {
        assert!(
            is_spawn_terminal_error(&Status::new(code, "test terminal")),
            "{code:?}"
        );
    }
    assert!(is_spawn_terminal_error(&Status::aborted(
        "actor crashed while restoring"
    )));
    assert!(is_spawn_terminal_error(&Status::aborted(
        "actor entered ACTOR_STATE_CRASHED"
    )));
    for code in [
        Code::Unavailable,
        Code::ResourceExhausted,
        Code::Unknown,
        Code::Internal,
        Code::DeadlineExceeded,
    ] {
        assert!(
            !is_spawn_terminal_error(&Status::new(code, "test retryable")),
            "{code:?}"
        );
    }
    assert!(!is_spawn_terminal_error(&Status::aborted(
        CONCURRENT_UPDATE_MSG
    )));
    // A non-status error is never terminal: it cannot reach this function
    // as anything but a status here, which the default (Unknown) covers.
    assert!(!is_spawn_terminal_error(&Status::unknown(
        "plain non-status error"
    )));
}

#[tokio::test]
async fn run_batch_success() {
    let srv = ping_server().await;
    let api = FakeControl::new();
    let rt = runtime(api, &srv, 10, 3, SpawnKnobs::default(), "SpawnOk");
    let before = requests_total("TimeToAllReady", "success", "SpawnOk");
    rt.run_batch().await;
    assert_eq!(rt.ready_count.load(Ordering::SeqCst), 10);
    assert!(rt.last_ready_elapsed.load(Ordering::SeqCst) > 0);
    assert_eq!(
        requests_total("TimeToAllReady", "success", "SpawnOk") - before,
        1.0
    );
    assert_eq!(rt.actor_names().len(), 10);
}

#[tokio::test]
async fn run_batch_partial_failure() {
    let srv = ping_server().await;
    let api = FakeControl::new();
    let fails = Arc::new(AtomicUsize::new(0));
    let f = fails.clone();
    api.with(|s| {
        s.create_actor_fn = Some(Arc::new(move |name: &str| {
            // Two of five fail terminally.
            if name.ends_with("-1") || name.ends_with("-2") {
                f.fetch_add(1, Ordering::SeqCst);
                return Err(Status::invalid_argument("invalid actor spec"));
            }
            Ok(())
        }))
    });
    let class = "SpawnPartial";
    let rt = runtime(api, &srv, 5, 2, SpawnKnobs::default(), class);
    rt.run_batch().await;
    assert_eq!(rt.ready_count.load(Ordering::SeqCst), 3);
    assert_eq!(fails.load(Ordering::SeqCst), 2);
    assert_eq!(requests_total("TimeToAllReady", "success", class), 1.0);
    assert_eq!(requests_total("ActorTimeToReady", "success", class), 3.0);
    assert_eq!(requests_total("ActorTimeToReady", "failure", class), 2.0);
}

#[tokio::test]
async fn run_batch_all_failed() {
    let srv = ping_server().await;
    let api = FakeControl::new();
    api.with(|s| {
        s.create_actor_fn = Some(Arc::new(|_: &str| {
            Err(Status::invalid_argument("invalid actor spec"))
        }))
    });
    let rt = runtime(api, &srv, 4, 2, SpawnKnobs::default(), "SpawnAllFail");
    rt.run_batch().await;
    assert_eq!(rt.ready_count.load(Ordering::SeqCst), 0);
    assert_eq!(
        requests_total("TimeToAllReady", "failure", "SpawnAllFail"),
        1.0
    );
}

#[tokio::test]
async fn iterate_runs_the_batch_once_and_blocks() {
    let srv = ping_server().await;
    let api = FakeControl::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let r = runs.clone();
    api.with(|s| {
        s.create_actor_fn = Some(Arc::new(move |_: &str| {
            r.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }))
    });
    let rt = Arc::new(runtime(api, &srv, 1, 1, SpawnKnobs::default(), "SpawnOnce"));
    let a = tokio::spawn({
        let rt = rt.clone();
        async move { rt.iterate().await }
    });
    let b = tokio::spawn({
        let rt = rt.clone();
        async move { rt.iterate().await }
    });
    let r = runs.clone();
    crate::testutil::eventually(Duration::from_secs(2), || r.load(Ordering::SeqCst) > 0).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the batch runs exactly once"
    );
    assert!(
        !a.is_finished() && !b.is_finished(),
        "both users stay parked"
    );
    tokio::time::timeout(Duration::from_secs(2), rt.shutdown())
        .await
        .unwrap();
    a.abort();
    b.abort();
}

#[tokio::test]
async fn create_already_exists_on_retry_succeeds() {
    let srv = ping_server().await;
    let api = FakeControl::new();
    let attempts = Arc::new(AtomicUsize::new(0));
    let a = attempts.clone();
    api.with(|s| {
        s.create_actor_fn = Some(Arc::new(move |_: &str| {
            // A blip after the server committed, then AlreadyExists.
            if a.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(Status::unavailable("network timeout"))
            } else {
                Err(Status::already_exists("actor already exists"))
            }
        }))
    });
    let rt = runtime(api, &srv, 1, 1, SpawnKnobs::default(), "SpawnExists");
    rt.run_batch().await;
    assert_eq!(rt.ready_count.load(Ordering::SeqCst), 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn resume_concurrent_update_retry() {
    let srv = ping_server().await;
    let api = FakeControl::new();
    api.with(|s| {
        s.resume_errs.extend([
            Status::aborted(CONCURRENT_UPDATE_MSG),
            Status::aborted(CONCURRENT_UPDATE_MSG),
        ])
    });
    let rt = runtime(
        api.clone(),
        &srv,
        1,
        1,
        SpawnKnobs::default(),
        "SpawnConflict",
    );
    rt.run_batch().await;
    assert_eq!(rt.ready_count.load(Ordering::SeqCst), 1);
    assert_eq!(count(&api.calls(), "ResumeActor"), 3);
}

#[tokio::test]
async fn resume_crashed() {
    let srv = ping_server().await;
    let api = FakeControl::new();
    api.with(|s| {
        s.resume_errs
            .push_back(Status::aborted("actor crashed while restoring"))
    });
    let rt = runtime(
        api.clone(),
        &srv,
        1,
        1,
        SpawnKnobs::default(),
        "SpawnCrashed",
    );
    rt.run_batch().await;
    assert_eq!(rt.ready_count.load(Ordering::SeqCst), 0);
    assert_eq!(count(&api.calls(), "ResumeActor"), 1);
}

#[tokio::test]
async fn shutdown_deletes_all() {
    let srv = ping_server().await;
    let api = FakeControl::new();
    let rt = runtime(
        api.clone(),
        &srv,
        4,
        2,
        SpawnKnobs::default(),
        "SpawnDelete",
    );
    *rt.actor_names.lock() = ["spawn-a-1", "spawn-a-2", "spawn-a-3", "spawn-a-4"]
        .map(String::from)
        .to_vec();
    tokio::time::timeout(Duration::from_secs(5), rt.shutdown())
        .await
        .unwrap();
    assert_eq!(api.with(|s| s.delete_reqs.len()), 4);
}

/// A Control API whose CreateActor never answers.
struct HangingCreate {
    inner: Arc<FakeControl>,
    started: AtomicUsize,
}

#[async_trait]
impl crate::loadgen::control::ControlApi for HangingCreate {
    async fn create_hive(
        &self,
        r: tonic::Request<api::CreateHiveRequest>,
    ) -> Result<tonic::Response<api::Hive>, Status> {
        self.inner.create_hive(r).await
    }
    async fn create_actor(
        &self,
        r: tonic::Request<api::CreateActorRequest>,
    ) -> Result<tonic::Response<api::Actor>, Status> {
        let _ = self.inner.create_actor(r).await;
        self.started.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
    async fn resume_actor(
        &self,
        r: tonic::Request<api::ResumeActorRequest>,
    ) -> Result<tonic::Response<api::ResumeActorResponse>, Status> {
        self.inner.resume_actor(r).await
    }
    async fn suspend_actor(
        &self,
        r: tonic::Request<api::SuspendActorRequest>,
    ) -> Result<tonic::Response<api::SuspendActorResponse>, Status> {
        self.inner.suspend_actor(r).await
    }
    async fn delete_actor(
        &self,
        r: tonic::Request<api::DeleteActorRequest>,
    ) -> Result<tonic::Response<api::Actor>, Status> {
        self.inner.delete_actor(r).await
    }
    async fn get_actor_template(
        &self,
        r: tonic::Request<api::GetActorTemplateRequest>,
    ) -> Result<tonic::Response<api::ActorTemplate>, Status> {
        self.inner.get_actor_template(r).await
    }
}

/// Shutdown mid-batch, with creates that never answer: it waits a bounded
/// time for the batch, then deletes every Actor the batch started.
#[tokio::test]
async fn shutdown_mid_batch_bounded_wait() {
    let srv = ping_server().await;
    let fake = FakeControl::new();
    let api = Arc::new(HangingCreate {
        inner: fake.clone(),
        started: AtomicUsize::new(0),
    });
    let mut cfg = config(
        fake.clone(),
        &srv.url(),
        Holder::fixed(SpawnKnobs::default()),
    );
    cfg.api = api.clone();
    cfg.total_actors = 2;
    cfg.spawn_concurrency = 2;
    cfg.actor_deadline = Duration::from_secs(10);
    let rt = Arc::new(SpawnRuntime::new(Arc::new(cfg)));
    let task = tokio::spawn({
        let rt = rt.clone();
        async move { rt.iterate().await }
    });
    let a = api.clone();
    assert!(
        crate::testutil::eventually(Duration::from_secs(2), || a.started.load(Ordering::SeqCst)
            == 2)
        .await
    );
    let start = Instant::now();
    tokio::time::timeout(Duration::from_secs(10), rt.shutdown())
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(1800) && elapsed <= Duration::from_millis(4500),
        "shutdown should bound its wait around 2s, took {elapsed:?}"
    );
    let created = count(&fake.calls(), "CreateActor");
    let deleted = fake.with(|s| s.delete_reqs.len());
    assert!(
        deleted > 0 && deleted == created,
        "created {created}, deleted {deleted}"
    );
    task.abort();
}

#[tokio::test]
async fn run_batch_dynconfig_override() {
    let srv = ping_server().await;
    // Overrides when set.
    let knobs = SpawnKnobs {
        total_actors: 3,
        spawn_concurrency: 2,
        actor_deadline: crate::loadgen::duration::Seconds::from_secs(5),
    };
    let mut rt = runtime(FakeControl::new(), &srv, 100, 1, knobs, "SpawnDynSet");
    Arc::get_mut(&mut rt.cfg).unwrap().actor_deadline = Duration::from_secs(10);
    rt.run_batch().await;
    assert_eq!(rt.ready_count.load(Ordering::SeqCst), 3);
    assert_eq!(rt.actor_names().len(), 3);

    // Falls back to the flags when the config is zero.
    let rt = runtime(
        FakeControl::new(),
        &srv,
        4,
        2,
        SpawnKnobs::default(),
        "SpawnDynZero",
    );
    rt.run_batch().await;
    assert_eq!(rt.ready_count.load(Ordering::SeqCst), 4);
    assert_eq!(rt.actor_names().len(), 4);
}

use std::pin::Pin;

use futures::Stream;
use hivekeep_proto::api::v1 as api;
use tokio::net::TcpListener;
use tonic::{Request, Response, Status};

use super::*;
use crate::loadgen::classes::glutton::test_support::{config, count};
use crate::loadgen::duration::Seconds;
use crate::loadgen::dynconfig::{Holder, Lifecycle, WaitTime, LIFECYCLE_MODE_ASYNC};
use crate::loadgen::fake_control::FakeControl;
use crate::loadgen::trace::{Sampler, Tracer, TRACEPARENT};
use crate::pb::guest::v1 as guest;
use crate::wire::TARGET_ACTOR_HEADER;

/// What the fake guest does, and what it saw.
#[derive(Default)]
struct ProcState {
    start_reqs: Vec<guest::StartProcessRequest>,
    stream_reqs: Vec<guest::StreamProcessOutputRequest>,
    incoming_md: Vec<tonic::metadata::MetadataMap>,
    start_err: Option<Status>,
    stream_err: Option<Status>,
    empty_pid: bool,
    omit_exit: bool,
    block_stream: bool,
    custom_stdout: Option<String>,
    stdout_chunks: Vec<String>,
    exit_state: Option<guest::ProcessState>,
    exit_code: Option<i32>,
}

#[derive(Clone, Default)]
struct FakeProcess(Arc<Mutex<ProcState>>);

type OutputStream = Pin<Box<dyn Stream<Item = Result<guest::ProcessOutput, Status>> + Send>>;

#[async_trait]
impl guest::process_service_server::ProcessService for FakeProcess {
    async fn start_process(
        &self,
        req: Request<guest::StartProcessRequest>,
    ) -> Result<Response<guest::Process>, Status> {
        let mut s = self.0.lock();
        s.incoming_md.push(req.metadata().clone());
        let inner = req.into_inner();
        s.start_reqs.push(inner.clone());
        if let Some(e) = s.start_err.clone() {
            return Err(e);
        }
        Ok(Response::new(guest::Process {
            process_id: if s.empty_pid {
                String::new()
            } else {
                "proc-1".into()
            },
            command: inner.command,
            state: guest::ProcessState::Running as i32,
            ..Default::default()
        }))
    }

    async fn get_process(
        &self,
        _: Request<guest::GetProcessRequest>,
    ) -> Result<Response<guest::Process>, Status> {
        Err(Status::unimplemented("get_process"))
    }

    type StreamProcessOutputStream = OutputStream;

    async fn stream_process_output(
        &self,
        req: Request<guest::StreamProcessOutputRequest>,
    ) -> Result<Response<OutputStream>, Status> {
        let mut s = self.0.lock();
        s.incoming_md.push(req.metadata().clone());
        let inner = req.into_inner();
        s.stream_reqs.push(inner.clone());
        if s.block_stream {
            return Ok(Response::new(Box::pin(futures::stream::pending())));
        }
        if let Some(e) = s.stream_err.clone() {
            return Err(e);
        }
        let chunks: Vec<String> = if !s.stdout_chunks.is_empty() {
            s.stdout_chunks.clone()
        } else {
            let out = s
                .custom_stdout
                .clone()
                .unwrap_or_else(|| super::super::actor::EXPECTED_STDOUT.into());
            if out.is_empty() {
                vec![]
            } else {
                vec![out]
            }
        };
        let mut items: Vec<Result<guest::ProcessOutput, Status>> = chunks
            .into_iter()
            .map(|c| {
                Ok(guest::ProcessOutput {
                    output: Some(guest::process_output::Output::Stdout(c.into_bytes())),
                })
            })
            .collect();
        if !s.omit_exit {
            items.push(Ok(guest::ProcessOutput {
                output: Some(guest::process_output::Output::Exit(guest::Process {
                    process_id: inner.process_id,
                    state: s.exit_state.unwrap_or(guest::ProcessState::Exited) as i32,
                    exit_code: s.exit_code.unwrap_or(0),
                    ..Default::default()
                })),
            }));
        }
        Ok(Response::new(Box::pin(futures::stream::iter(items))))
    }

    async fn write_process_input(
        &self,
        _: Request<tonic::Streaming<guest::WriteProcessInputRequest>>,
    ) -> Result<Response<guest::WriteProcessInputResponse>, Status> {
        Err(Status::unimplemented("write_process_input"))
    }

    async fn signal_process(
        &self,
        _: Request<guest::SignalProcessRequest>,
    ) -> Result<Response<guest::Process>, Status> {
        Err(Status::unimplemented("signal_process"))
    }
}

struct Fixture {
    srv: FakeProcess,
    conn: RouterConn,
    _task: tokio::task::JoinHandle<()>,
}

async fn fixture(srv: FakeProcess) -> Fixture {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = guest::process_service_server::ProcessServiceServer::new(srv.clone());
    let task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_lazy();
    Fixture {
        srv,
        conn: RouterConn::new(ProcessClient::new(channel)),
        _task: task,
    }
}

fn mode(m: &str) -> HkEnvKnobs {
    HkEnvKnobs {
        lifecycle: Lifecycle {
            lifecycle_mode: m.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

async fn runtime(
    api: Arc<FakeControl>,
    srv: FakeProcess,
    knobs: HkEnvKnobs,
) -> (TaskRuntime, Fixture) {
    let fix = fixture(srv).await;
    let mut cfg = config(api, "", Holder::fixed(knobs));
    cfg.hive = "bench-test".into();
    let conn = fix.conn.clone();
    let rt = TaskRuntime::with_dial(Arc::new(cfg), Some(Arc::new(move || Ok(conn.clone()))));
    (rt, fix)
}

fn has(calls: &[String], name: &str) -> bool {
    calls.iter().any(|c| c == name)
}

#[tokio::test]
async fn iterate_suspend_mode() {
    let api = FakeControl::new();
    let (rt, fix) = runtime(
        api.clone(),
        FakeProcess::default(),
        mode(LIFECYCLE_MODE_SYNC),
    )
    .await;
    rt.iterate(0).await;
    let calls = api.calls();
    for want in ["CreateHive", "CreateActor", "ResumeActor", "SuspendActor"] {
        assert!(has(&calls, want), "missing {want}: {calls:?}");
    }
    assert!(!has(&calls, "SuspendActor(async)"), "{calls:?}");
    for req in api.with(|s| s.suspend_reqs.clone()) {
        assert_eq!(
            req.options.unwrap().mode(),
            api::DurabilityMode::Synchronous
        );
    }
    let s = fix.srv.0.lock();
    assert_eq!(s.start_reqs.len(), 1);
    assert_eq!(s.start_reqs[0].command, vec!["echo", "hello world"]);
    assert_eq!(s.stream_reqs.len(), 1);
    assert_eq!(s.stream_reqs[0].process_id, "proc-1");
    assert!(s.stream_reqs[0].follow);
}

#[tokio::test]
async fn iterate_pause_mode() {
    let api = FakeControl::new();
    let (rt, _fix) = runtime(
        api.clone(),
        FakeProcess::default(),
        mode(LIFECYCLE_MODE_ASYNC),
    )
    .await;
    rt.iterate(0).await;
    let calls = api.calls();
    assert!(has(&calls, "SuspendActor(async)"), "{calls:?}");
    assert!(!has(&calls, "SuspendActor"), "{calls:?}");
}

#[tokio::test]
async fn iterate_none_mode() {
    let api = FakeControl::new();
    let (rt, _fix) = runtime(
        api.clone(),
        FakeProcess::default(),
        mode(LIFECYCLE_MODE_NONE),
    )
    .await;
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
    rt.shutdown_within(Duration::from_secs(10)).await;
    let after = api.calls();
    assert_eq!(
        after[after.len() - 2..],
        ["SuspendActor(async)", "DeleteActor"],
        "{after:?}"
    );
}

#[tokio::test]
async fn exec_metadata_propagation() {
    let api = FakeControl::new();
    let srv = FakeProcess::default();
    srv.0.lock().stdout_chunks = vec!["hello ".into(), "world\n".into()];
    let fix = fixture(srv).await;
    let mut cfg = config(api, "", Holder::fixed(mode(LIFECYCLE_MODE_SYNC)));
    cfg.hive = "bench-test".into();
    cfg.tracer = Tracer::new("test-sampled", Arc::new(Sampler::new(1.0)));
    let conn = fix.conn.clone();
    let rt = TaskRuntime::with_dial(Arc::new(cfg), Some(Arc::new(move || Ok(conn.clone()))));
    rt.iterate(0).await;
    let s = fix.srv.0.lock();
    assert!(s.incoming_md.len() >= 2, "{}", s.incoming_md.len());
    for (i, md) in s.incoming_md.iter().enumerate() {
        let target: Vec<_> = md.get_all(TARGET_ACTOR_HEADER).iter().collect();
        assert_eq!(target.len(), 1, "call {i}");
        assert!(
            target[0].to_str().unwrap().starts_with("bench-test/sb-"),
            "call {i}: {target:?}"
        );
        let tp = md
            .get(TRACEPARENT)
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default();
        assert!(
            !tp.is_empty() && tp.ends_with("-01"),
            "call {i}: sampled traceparent, got {tp:?}"
        );
    }
}

fn bare_actor(rt: &TaskRuntime, fix: &Fixture, name: &str) -> HkEnvActor {
    HkEnvActor::new(
        rt.cfg.clone(),
        name.into(),
        fix.conn.client.clone(),
        fix.conn.closed.clone(),
    )
}

#[tokio::test]
async fn exec_validation_failures() {
    type Setup = Box<dyn Fn(&mut ProcState)>;
    let cases: Vec<(&str, Setup, &str)> = vec![
        (
            "start process error",
            Box::new(|s| s.start_err = Some(Status::internal("failed to fork"))),
            "StartProcess:",
        ),
        (
            "empty process_id",
            Box::new(|s| s.empty_pid = true),
            "StartProcess returned empty process_id",
        ),
        (
            "stream process output error",
            Box::new(|s| s.stream_err = Some(Status::unavailable("guest down"))),
            "StreamProcessOutput.Recv:",
        ),
        (
            "stream closed without exit event",
            Box::new(|s| s.omit_exit = true),
            "stream closed without exit event",
        ),
        (
            "non-zero exit code",
            Box::new(|s| s.exit_code = Some(1)),
            "unexpected exit code: 1, want 0",
        ),
        (
            "unexpected process state",
            Box::new(|s| s.exit_state = Some(guest::ProcessState::Running)),
            "unexpected process state: PROCESS_STATE_RUNNING, want PROCESS_STATE_EXITED",
        ),
        (
            "stdout mismatch",
            Box::new(|s| s.custom_stdout = Some("hello mars\n".into())),
            "stdout mismatch:",
        ),
    ];
    for (name, setup, want) in cases {
        let api = FakeControl::new();
        let srv = FakeProcess::default();
        setup(&mut srv.0.lock());
        let (rt, fix) = runtime(api.clone(), srv, mode(LIFECYCLE_MODE_SYNC)).await;
        let actor = bare_actor(&rt, &fix, "sb-test");
        let err = actor.exec_hello_world().await.expect_err(name);
        assert!(
            err.message().contains(want),
            "{name}: {err:?}, want {want:?}"
        );

        // The lifecycle still runs, and one exec failure is below the
        // replacement threshold.
        rt.iterate(0).await;
        let calls = api.calls();
        assert!(
            has(&calls, "ResumeActor") && has(&calls, "SuspendActor"),
            "{name}: {calls:?}"
        );
        assert!(!has(&calls, "DeleteActor"), "{name}: {calls:?}");
    }
}

#[tokio::test]
async fn exec_timeout() {
    let srv = FakeProcess::default();
    srv.0.lock().block_stream = true;
    let (rt, fix) = runtime(FakeControl::new(), srv, mode(LIFECYCLE_MODE_SYNC)).await;
    let mut actor = bare_actor(&rt, &fix, "sb-timeout");
    actor.exec_timeout = Some(Duration::from_millis(20));
    let err = actor
        .exec_hello_world()
        .await
        .expect_err("a hung stream must time out");
    assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
}

#[tokio::test]
async fn iterate_retries_stranded_hibernate() {
    let api = FakeControl::new();
    api.with(|s| {
        s.suspend_errs
            .push_back(Status::unavailable("hivekeep-api restarting"))
    });
    let (rt, _fix) = runtime(
        api.clone(),
        FakeProcess::default(),
        mode(LIFECYCLE_MODE_SYNC),
    )
    .await;
    rt.iterate(0).await;
    let before = api.calls();
    rt.iterate(0).await;
    let after = api.calls();
    assert_eq!(after.last().unwrap(), "SuspendActor", "{after:?}");
    assert_eq!(
        count(&before, "ResumeActor"),
        count(&after, "ResumeActor"),
        "{after:?}"
    );
}

#[tokio::test]
async fn iterate_replaces_actor_on_terminal_resume_failure() {
    let api = FakeControl::new();
    api.with(|s| {
        s.resume_errs.push_back(Status::failed_precondition(
            "AssignWorker prerequisite not met",
        ))
    });
    let (rt, _fix) = runtime(
        api.clone(),
        FakeProcess::default(),
        mode(LIFECYCLE_MODE_SYNC),
    )
    .await;
    rt.iterate(0).await;
    let calls = api.calls();
    assert_eq!(count(&calls, "CreateActor"), 2, "{calls:?}");
    assert_eq!(count(&calls, "DeleteActor"), 1, "{calls:?}");
}

use super::super::actor::MAX_CONSECUTIVE_FAILURES;

#[tokio::test]
async fn iterate_replaces_actor_on_repeated_exec_failures() {
    let api = FakeControl::new();
    let srv = FakeProcess::default();
    srv.0.lock().start_err = Some(Status::unavailable("guest wedged"));
    let (rt, _fix) = runtime(api.clone(), srv, mode(LIFECYCLE_MODE_SYNC)).await;
    for _ in 0..MAX_CONSECUTIVE_FAILURES {
        rt.iterate(0).await;
    }
    let calls = api.calls();
    assert_eq!(
        count(&calls, "CreateActor"),
        2,
        "initial + replacement: {calls:?}"
    );
    assert_eq!(
        count(&calls, "SuspendActor"),
        MAX_CONSECUTIVE_FAILURES as usize,
        "hibernated first: {calls:?}"
    );
    assert_eq!(count(&calls, "DeleteActor"), 1, "{calls:?}");
}

#[tokio::test]
async fn iterate_counts_at_most_one_exec_failure_per_wake() {
    let api = FakeControl::new();
    let srv = FakeProcess::default();
    srv.0.lock().start_err = Some(Status::unavailable("transient router blip"));
    let k = HkEnvKnobs {
        min_live: Seconds::from_secs(5),
        max_live: Seconds::from_secs(5),
        max_pings_per_wake: MAX_CONSECUTIVE_FAILURES as i64,
        ..mode(LIFECYCLE_MODE_SYNC)
    };
    let (mut rt, fix) = runtime(api.clone(), srv, k).await;
    rt.sleep_fn = Some(Arc::new(|_| true));
    rt.iterate(0).await;
    let calls = api.calls();
    assert_eq!(count(&calls, "CreateActor"), 1, "{calls:?}");
    assert_eq!(count(&calls, "DeleteActor"), 0, "{calls:?}");
    assert_eq!(
        fix.srv.0.lock().start_reqs.len(),
        MAX_CONSECUTIVE_FAILURES as usize,
        "every exec ran"
    );
}

#[tokio::test]
async fn iterate_does_not_replace_on_resource_exhausted_exec_failure() {
    let api = FakeControl::new();
    let srv = FakeProcess::default();
    srv.0.lock().start_err = Some(Status::resource_exhausted("rate limit exceeded"));
    let (rt, _fix) = runtime(api.clone(), srv, mode(LIFECYCLE_MODE_SYNC)).await;
    for _ in 0..=MAX_CONSECUTIVE_FAILURES {
        rt.iterate(0).await;
    }
    let calls = api.calls();
    assert_eq!(count(&calls, "CreateActor"), 1, "{calls:?}");
    assert_eq!(count(&calls, "DeleteActor"), 0, "{calls:?}");
}

fn install_user(rt: &TaskRuntime, fix: &Fixture, name: &str, running: bool) {
    let a = bare_actor(rt, fix, name);
    a.state.lock().running = running;
    let u = HkEnvUser {
        conn: fix.conn.clone(),
        actors: Mutex::new(vec![Arc::new(a)]),
        next_idx: Mutex::new(0),
    };
    rt.users.lock().insert(0, Arc::new(u));
}

#[tokio::test]
async fn shutdown_pause_mode_running_actor() {
    let api = FakeControl::new();
    let (rt, fix) = runtime(
        api.clone(),
        FakeProcess::default(),
        mode(LIFECYCLE_MODE_ASYNC),
    )
    .await;
    install_user(&rt, &fix, "running-actor", true);
    rt.shutdown_within(Duration::from_secs(10)).await;
    assert_eq!(api.calls(), vec!["SuspendActor(async)", "DeleteActor"]);
    assert!(
        fix.conn.closed.is_cancelled(),
        "the router connection must be closed"
    );
}

#[tokio::test]
async fn shutdown_stopped_actor() {
    let api = FakeControl::new();
    let (rt, fix) = runtime(
        api.clone(),
        FakeProcess::default(),
        mode(LIFECYCLE_MODE_SYNC),
    )
    .await;
    install_user(&rt, &fix, "stopped-actor", false);
    rt.shutdown_within(Duration::from_secs(10)).await;
    assert_eq!(api.calls(), vec!["DeleteActor"]);
    assert!(fix.conn.closed.is_cancelled());
}

async fn wait_for(fix: &Fixture, f: impl Fn(&ProcState) -> bool) {
    let srv = fix.srv.clone();
    assert!(
        crate::testutil::eventually(Duration::from_secs(2), move || f(&srv.0.lock())).await,
        "timed out waiting for the guest"
    );
}

#[tokio::test]
async fn shutdown_waits_for_in_flight_iterate() {
    let api = FakeControl::new();
    let k = HkEnvKnobs {
        wait: WaitTime {
            min_wait: Seconds::from_secs(5),
            max_wait: Seconds::from_secs(5),
        },
        min_live: Seconds::from_secs(5),
        max_live: Seconds::from_secs(5),
        max_pings_per_wake: 1,
        ..mode(LIFECYCLE_MODE_SYNC)
    };
    let (rt, fix) = runtime(api.clone(), FakeProcess::default(), k).await;
    let rt = Arc::new(rt);
    let iter = tokio::spawn({
        let rt = rt.clone();
        async move { rt.iterate(0).await }
    });
    wait_for(&fix, |s| !s.start_reqs.is_empty()).await;
    // Shutdown cuts the live and wait sleeps short, lets the iteration's
    // suspend finish, then deletes once without any replacement.
    let start = tokio::time::Instant::now();
    rt.shutdown_within(Duration::from_secs(60)).await;
    iter.await.unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "{:?}",
        start.elapsed()
    );
    let calls = api.calls();
    assert_eq!(count(&calls, "CreateActor"), 1, "{calls:?}");
    assert_eq!(count(&calls, "SuspendActor"), 1, "{calls:?}");
    assert_eq!(count(&calls, "DeleteActor"), 1, "{calls:?}");
}

#[tokio::test]
async fn shutdown_respects_its_budget() {
    let api = FakeControl::new();
    let srv = FakeProcess::default();
    srv.0.lock().block_stream = true;
    let (rt, fix) = runtime(api.clone(), srv, mode(LIFECYCLE_MODE_SYNC)).await;
    let rt = Arc::new(rt);
    let iter = tokio::spawn({
        let rt = rt.clone();
        async move { rt.iterate(0).await }
    });
    wait_for(&fix, |s| !s.stream_reqs.is_empty()).await;
    let start = tokio::time::Instant::now();
    rt.shutdown_within(Duration::from_millis(100)).await;
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "shutdown ignored its budget: {:?}",
        start.elapsed()
    );
    // Closing the router connection unblocks the stuck exec, so the
    // iteration ends too.
    tokio::time::timeout(Duration::from_secs(5), iter)
        .await
        .unwrap()
        .unwrap();
    let calls = api.calls();
    assert!(has(&calls, "SuspendActor"), "{calls:?}");
    assert!(has(&calls, "DeleteActor"), "{calls:?}");
}

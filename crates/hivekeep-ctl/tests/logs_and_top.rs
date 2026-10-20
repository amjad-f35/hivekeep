//! `logs actor` and `top worker` against a fake Control server, with fake
//! pod-log and pod-metrics sources standing in for the cluster.

mod common;

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use common::*;
use hivekeep_core::resource::ActorRef;
use hivekeep_ctl::cli::WorkerFilter;
use hivekeep_ctl::client::Api;
use hivekeep_ctl::commands::logs::{LogOptions, LogStream, LogsRunner, PodLogs};
use hivekeep_ctl::commands::worker_top::{run_with, ContainerMetrics, MetricsSource, PodMetrics};
use hivekeep_ctl::connection::{ConnectionConfig, RequestDecorator};
use hivekeep_ctl::io::{SharedBuf, Sink};
use hivekeep_ctl::log_filter::LogFilter;
use hivekeep_ctl::output::{OutputFormat, Printer};
use hivekeep_ctl::verbosity::{Logger, BASIC};
use hivekeep_proto::api::v1 as pb;
use tokio::sync::mpsc;

async fn api(addr: std::net::SocketAddr) -> Api {
    let cfg = ConnectionConfig {
        endpoint: Some(addr.to_string()),
        plaintext: true,
        ..Default::default()
    };
    let ch = cfg.connect(&Logger::silent()).await.unwrap();
    Api::new(ch, RequestDecorator::default(), Logger::silent())
}

fn line(ts: &str, msg: &str) -> String {
    format!(r#"{ts} {{"msg":"{msg}","labels":{{"hk.hive":"h","hk.actor.name":"a1"}}}}"#)
}

fn shown(msg: &str) -> String {
    format!(r#"{{"msg":"{msg}"}}"#)
}

/// One scripted connection: its lines, and whether it stays open after them;
/// or the error opening it fails with.
type Script = Result<(Vec<String>, bool), String>;

/// Serves scripted streams per pod, one script entry per connection.
#[derive(Default)]
struct FakeLogs {
    scripts: Mutex<HashMap<String, VecDeque<Script>>>,
    opened: Mutex<Vec<(String, String, LogOptions)>>,
    // Senders of streams scripted to stay open, kept alive here.
    open: Mutex<Vec<mpsc::Sender<Result<String, String>>>>,
}

impl FakeLogs {
    fn script(&self, pod: &str, lines: Vec<String>, keep_open: bool) {
        self.scripts
            .lock()
            .unwrap()
            .entry(pod.to_string())
            .or_default()
            .push_back(Ok((lines, keep_open)));
    }

    fn script_error(&self, pod: &str, err: &str) {
        self.scripts
            .lock()
            .unwrap()
            .entry(pod.to_string())
            .or_default()
            .push_back(Err(err.to_string()));
    }
}

impl PodLogs for FakeLogs {
    async fn stream(&self, ns: &str, pod: &str, opts: &LogOptions) -> Result<LogStream, String> {
        self.opened
            .lock()
            .unwrap()
            .push((ns.into(), pod.into(), opts.clone()));
        let (lines, keep_open) = self
            .scripts
            .lock()
            .unwrap()
            .get_mut(pod)
            .and_then(VecDeque::pop_front)
            .ok_or_else(|| format!("no logs for {pod}"))??;
        let (tx, rx) = mpsc::channel(64);
        for l in lines {
            tx.send(Ok(l)).await.unwrap();
        }
        if keep_open {
            self.open.lock().unwrap().push(tx);
        }
        Ok(LogStream::new(rx))
    }
}

struct Harness {
    fake: Fake,
    runner: LogsRunner<FakeLogs>,
    out: SharedBuf,
    err: SharedBuf,
}

async fn harness(source: &str, container: &str) -> Harness {
    let fake = Fake::default();
    let addr = serve(fake.clone()).await;
    let (out, out_buf) = Sink::buffer();
    let (err, err_buf) = Sink::buffer();
    let runner = LogsRunner {
        api: api(addr).await,
        logs: FakeLogs::default(),
        filter: LogFilter::new(ActorRef::new("h", "a1"), source, container).unwrap(),
        out,
        err: err.clone(),
        log: Logger::new(BASIC, err),
        poll_interval: Duration::from_millis(10),
        reconnect_interval: Duration::from_millis(5),
        monitor_interval: Duration::from_millis(10),
    };
    Harness {
        fake,
        runner,
        out: out_buf,
        err: err_buf,
    }
}

fn set_actor(fake: &Fake, a: pb::Actor) {
    fake.state().actors.insert(("h".into(), "a1".into()), a);
}

fn running(pod: &str) -> pb::Actor {
    running_on(actor("h", "a1", pb::ActorState::Running), "w", "ns", pod)
}

async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn one_shot_prints_the_actors_lines() {
    let h = harness("all", "").await;
    set_actor(&h.fake, running("pod-1"));
    h.runner.logs.script(
        "pod-1",
        vec![
            r#"{"msg":"one","labels":{"hk.hive":"h","hk.actor.name":"a1"}}"#.into(),
            r#"{"msg":"other","labels":{"hk.hive":"h","hk.actor.name":"a2"}}"#.into(),
            "plain text".into(),
            r#"{"msg":"two","labels":{"hk.hive":"h","hk.actor.name":"a1","hk.actor.container.name":"app"}}"#.into(),
        ],
        false,
    );
    h.runner.run_once().await.unwrap();
    assert_eq!(
        h.out.contents(),
        format!("{}\n{}\n", shown("one"), shown("two"))
    );
    let opened = h.runner.logs.opened.lock().unwrap().clone();
    assert_eq!(
        opened,
        [("ns".into(), "pod-1".into(), LogOptions::default())]
    );
}

#[tokio::test]
async fn one_shot_source_filter() {
    let h = harness("lifecycle", "").await;
    set_actor(&h.fake, running("pod-1"));
    h.runner.logs.script(
        "pod-1",
        vec![
            r#"{"msg":"started","labels":{"hk.hive":"h","hk.actor.name":"a1"}}"#.into(),
            r#"{"msg":"app","labels":{"hk.hive":"h","hk.actor.name":"a1","hk.actor.container.name":"app"}}"#.into(),
        ],
        false,
    );
    h.runner.run_once().await.unwrap();
    assert_eq!(h.out.contents(), format!("{}\n", shown("started")));
}

#[tokio::test]
async fn one_shot_requires_a_running_actor() {
    let h = harness("all", "").await;
    set_actor(&h.fake, actor("h", "a1", pb::ActorState::Suspended));
    let err = h.runner.run_once().await.unwrap_err();
    assert_eq!(
        err.message(),
        "actor h/a1 is not currently running on any worker pod"
    );
}

#[tokio::test]
async fn follow_fails_for_a_missing_actor() {
    let h = harness("all", "").await;
    let err = h.runner.follow().await.unwrap_err();
    assert!(err.message().starts_with("actor h/a1 not found"), "{err}");
}

#[tokio::test]
async fn follow_waits_for_the_actor_to_run() {
    let h = harness("all", "").await;
    set_actor(&h.fake, actor("h", "a1", pb::ActorState::Suspended));
    h.runner
        .logs
        .script("pod-1", vec![line("2026-05-16T01:00:01Z", "hello")], true);
    let fake = h.fake.clone();
    let follow = tokio::time::timeout(Duration::from_millis(400), h.runner.follow());
    let flip = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        set_actor(&fake, running("pod-1"));
    };
    let (result, ()) = tokio::join!(follow, flip);
    assert!(result.is_err(), "follow runs until stopped");
    assert_eq!(h.out.contents(), format!("{}\n", shown("hello")));
    let err = h.err.contents();
    assert_eq!(
        err.matches("Actor h/a1 is not running (state=ACTOR_STATE_SUSPENDED); waiting for worker assignment...").count(),
        1,
        "{err}"
    );
    assert!(err.contains("Actor is currently running on pod ns/pod-1"));
}

#[tokio::test]
async fn follow_resumes_after_the_last_line_read() {
    let h = harness("all", "").await;
    set_actor(&h.fake, running("pod-1"));
    // The first stream ends; the reconnect re-reads the cursor's second.
    h.runner.logs.script(
        "pod-1",
        vec![
            line("2026-05-16T01:00:01.100Z", "one"),
            line("2026-05-16T01:00:02.500Z", "two"),
        ],
        false,
    );
    h.runner.logs.script(
        "pod-1",
        vec![
            line("2026-05-16T01:00:02.500Z", "two"),
            line("2026-05-16T01:00:03Z", "three"),
        ],
        true,
    );
    let _ = tokio::time::timeout(Duration::from_millis(200), h.runner.follow()).await;
    assert_eq!(
        h.out.contents(),
        format!("{}\n{}\n{}\n", shown("one"), shown("two"), shown("three"))
    );
    let opened = h.runner.logs.opened.lock().unwrap().clone();
    assert_eq!(opened.len(), 2);
    assert!(opened[0].2.follow && opened[0].2.timestamps && opened[0].2.since.is_none());
    assert_eq!(
        opened[1].2.since.unwrap().to_rfc3339(),
        "2026-05-16T01:00:02.500+00:00"
    );
    assert_eq!(
        h.err.contents().matches("currently running on pod").count(),
        1
    );
}

#[tokio::test]
async fn follow_moves_with_the_actor() {
    let h = harness("all", "").await;
    set_actor(&h.fake, running("pod-1"));
    h.runner
        .logs
        .script("pod-1", vec![line("2026-05-16T01:00:05Z", "on-one")], true);
    // The new node's clock is behind; its lines are still all shown.
    h.runner
        .logs
        .script("pod-2", vec![line("2026-05-16T01:00:01Z", "on-two")], true);
    let fake = h.fake.clone();
    let out = h.out.clone();
    let follow = tokio::time::timeout(Duration::from_millis(500), h.runner.follow());
    let migrate = async {
        eventually("first line", || out.contents().contains("on-one")).await;
        set_actor(&fake, running("pod-2"));
        eventually("second line", || out.contents().contains("on-two")).await;
    };
    let _ = tokio::join!(follow, migrate);
    assert_eq!(
        h.out.contents(),
        format!("{}\n{}\n", shown("on-one"), shown("on-two"))
    );
    let err = h.err.contents();
    assert!(err.contains("Actor is currently running on pod ns/pod-1"));
    assert!(err.contains("Actor h/a1 left pod pod-1 (state=ACTOR_STATE_RUNNING, pod=pod-2); closing current log stream"), "{err}");
    assert!(err.contains("Actor is currently running on pod ns/pod-2"));
}

#[tokio::test]
async fn follow_closes_the_stream_when_the_actor_suspends() {
    let h = harness("all", "").await;
    set_actor(&h.fake, running("pod-1"));
    h.runner
        .logs
        .script("pod-1", vec![line("2026-05-16T01:00:05Z", "before")], true);
    let fake = h.fake.clone();
    let err = h.err.clone();
    let follow = tokio::time::timeout(Duration::from_millis(400), h.runner.follow());
    let suspend = async {
        eventually("stream", || err.contents().contains("currently running")).await;
        set_actor(&fake, actor("h", "a1", pb::ActorState::Suspended));
        eventually("wait notice", || err.contents().contains("is not running")).await;
    };
    let _ = tokio::join!(follow, suspend);
    let err = h.err.contents();
    assert!(err.contains("Actor h/a1 left pod pod-1 (state=ACTOR_STATE_SUSPENDED, pod=); closing current log stream"), "{err}");
}

struct FakeMetrics(Result<Vec<PodMetrics>, String>);

impl MetricsSource for FakeMetrics {
    async fn list(&self, _namespace: &str) -> Result<Vec<PodMetrics>, String> {
        self.0.clone()
    }
}

async fn top(fake: &Fake, metrics: FakeMetrics, filter: WorkerFilter) -> (String, String) {
    let addr = serve(fake.clone()).await;
    let api = api(addr).await;
    let (out, out_buf) = Sink::buffer();
    let (err, err_buf) = Sink::buffer();
    run_with(
        &api,
        &metrics,
        &filter,
        &Printer::new(OutputFormat::Table),
        &out,
        &Logger::new(BASIC, err),
    )
    .await
    .unwrap();
    (out_buf.contents(), err_buf.contents())
}

fn seed(fake: &Fake) {
    let mut s = fake.state();
    s.workers
        .insert("w1".into(), worker("w1", "ns-a", "pool-1", &[], "gvisor"));
    s.workers
        .insert("w2".into(), worker("w2", "ns-a", "pool-1", &[], ""));
}

#[tokio::test]
async fn top_workers_reports_usage() {
    let fake = Fake::default();
    seed(&fake);
    let metrics = FakeMetrics(Ok(vec![PodMetrics {
        namespace: "ns-a".into(),
        name: "w1-pod".into(),
        containers: vec![ContainerMetrics {
            name: "worker".into(),
            usage: [
                ("cpu".to_string(), "250m".to_string()),
                ("memory".to_string(), "512Mi".to_string()),
            ]
            .into(),
        }],
    }]));
    let (out, err) = top(&fake, metrics, WorkerFilter::default()).await;
    assert_eq!(
        out,
        "NAME     POOL     CLASS    STATUS   CPU(CORES)            MEMORY(bytes)\n\
         w1-pod   pool-1   gvisor   FREE     250m                  512Mi\n\
         w2-pod   pool-1            FREE     metrics unavailable   metrics unavailable\n"
    );
    assert!(
        err.contains("No pod metrics returned for worker pod ns-a/w2-pod"),
        "{err}"
    );
}

#[tokio::test]
async fn top_workers_without_metrics() {
    let fake = Fake::default();
    seed(&fake);
    let (out, err) = top(
        &fake,
        FakeMetrics(Err("metrics API not installed".into())),
        WorkerFilter::default(),
    )
    .await;
    assert_eq!(out.matches("metrics unavailable").count(), 4);
    assert!(err.contains(
        "Failed to list pod metrics (CPU/memory will be unavailable): metrics API not installed"
    ));
}

fn labelled(fields: &str, container: Option<&str>) -> String {
    let c = container
        .map(|c| format!(r#","hk.actor.container.name":"{c}""#))
        .unwrap_or_default();
    format!(
        r#"{{{fields},"logging.googleapis.com/labels":{{"hk.hive":"h","hk.actor.name":"a1"{c}}}}}"#
    )
}

#[tokio::test]
async fn one_shot_source_and_container_selection() {
    let started = labelled(
        r#""time":"2026-05-16T01:03:37Z","message":"Actor started""#,
        None,
    );
    let counter = labelled(
        r#""time":"2026-05-16T01:03:38Z","level":"info","msg":"from counter""#,
        Some("counter"),
    );
    let sidecar = labelled(
        r#""time":"2026-05-16T01:03:39Z","level":"info","msg":"from sidecar""#,
        Some("sidecar"),
    );
    let sandbox = labelled(
        r#""time":"2026-05-16T01:03:39Z","message":"Args: [runsc create _sandbox]""#,
        Some("_sandbox"),
    );
    let checkpointing = labelled(
        r#""time":"2026-05-16T01:03:40Z","message":"Actor checkpointing""#,
        None,
    );
    let stream = vec![started, counter, sidecar, sandbox, checkpointing];
    for (name, source, container, want) in [
        (
            "no filter shows application and lifecycle logs",
            "all",
            "",
            &[
                "Actor started",
                "from counter",
                "from sidecar",
                "Actor checkpointing",
            ][..],
        ),
        (
            "source=all excludes sandbox diagnostics",
            "all",
            "",
            &[
                "Actor started",
                "from counter",
                "from sidecar",
                "Actor checkpointing",
            ],
        ),
        ("container filter", "all", "counter", &["from counter"]),
        (
            "explicit sandbox filter",
            "all",
            "_sandbox",
            &["Args: [runsc create _sandbox]"],
        ),
        (
            "source=containers with explicit sandbox filter",
            "containers",
            "_sandbox",
            &["Args: [runsc create _sandbox]"],
        ),
        (
            "source=containers drops lifecycle events",
            "containers",
            "",
            &["from counter", "from sidecar"],
        ),
        (
            "source=containers with container filter",
            "containers",
            "sidecar",
            &["from sidecar"],
        ),
        (
            "source=lifecycle keeps only lifecycle events",
            "lifecycle",
            "",
            &["Actor started", "Actor checkpointing"],
        ),
    ] {
        let h = harness(source, container).await;
        set_actor(&h.fake, running("pod-1"));
        h.runner.logs.script("pod-1", stream.clone(), false);
        h.runner.run_once().await.unwrap();
        let out = h.out.contents();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), want.len(), "{name}: {out}");
        for (line, want) in lines.iter().zip(want) {
            assert!(line.contains(want), "{name}: {line} lacks {want}");
        }
        assert_eq!(h.runner.logs.opened.lock().unwrap().len(), 1, "{name}");
    }
}

#[tokio::test]
async fn follow_resumes_on_the_same_pod_after_a_suspend() {
    let h = harness("all", "").await;
    set_actor(&h.fake, running("pod-1"));
    h.runner.logs.script(
        "pod-1",
        vec![line("2026-05-16T01:03:38Z", "before suspend")],
        true,
    );
    h.runner.logs.script(
        "pod-1",
        vec![line("2026-05-16T01:03:40Z", "after resume")],
        true,
    );
    let fake = h.fake.clone();
    let (out, err) = (h.out.clone(), h.err.clone());
    let follow = tokio::time::timeout(Duration::from_millis(800), h.runner.follow());
    let cycle = async {
        eventually("first line", || out.contents().contains("before suspend")).await;
        set_actor(&fake, actor("h", "a1", pb::ActorState::Suspended));
        eventually("wait notice", || err.contents().contains("is not running")).await;
        set_actor(&fake, running("pod-1"));
        eventually("second line", || out.contents().contains("after resume")).await;
    };
    let _ = tokio::join!(follow, cycle);
    assert_eq!(
        h.out.contents(),
        format!("{}\n{}\n", shown("before suspend"), shown("after resume"))
    );
}

/// The cursor a reconnect resumes from is the kubelet's timestamp on the
/// last line read, whether or not it was shown, and not the time the line
/// itself carries (an Actor's clock may run ahead of the node's).
#[tokio::test]
async fn follow_cursor_is_the_kubelet_timestamp_of_the_last_line_read() {
    let h = harness("lifecycle", "").await;
    set_actor(&h.fake, running("pod-1"));
    let lifecycle = format!(
        "2026-05-16T01:03:38.100000000Z {}",
        labelled(
            r#""time":"2026-05-16T01:03:38Z","message":"Actor started""#,
            None
        )
    );
    let foreign = r#"2026-05-16T01:03:39.200000000Z {"time":"2026-05-16T01:03:39Z","message":"not mine","logging.googleapis.com/labels":{"hk.hive":"h","hk.actor.name":"other"}}"#.to_string();
    let counter = format!(
        "2026-05-16T01:03:40.300000000Z {}",
        labelled(
            r#""time":"2099-01-01T00:00:00Z","level":"info","msg":"from counter""#,
            Some("counter")
        )
    );
    // After the reconnect the kubelet re-sends the cursor's second.
    let later = format!(
        "2026-05-16T01:03:40.900000000Z {}",
        labelled(
            r#""time":"2026-05-16T01:03:40Z","message":"Actor checkpointing""#,
            None
        )
    );
    h.runner
        .logs
        .script("pod-1", vec![lifecycle, foreign, counter.clone()], false);
    h.runner.logs.script("pod-1", vec![counter, later], false);
    h.runner.logs.script("pod-1", vec![], true);
    let logs = &h.runner.logs;
    let follow = tokio::time::timeout(Duration::from_millis(800), h.runner.follow());
    let third = eventually("third stream", || logs.opened.lock().unwrap().len() >= 3);
    tokio::select! {
        _ = follow => {}
        _ = third => {}
    }
    let opened = h.runner.logs.opened.lock().unwrap().clone();
    assert!(opened.len() >= 3);
    assert!(opened[1].2.timestamps, "follow asks for kubelet timestamps");
    assert_eq!(
        opened[1].2.since.unwrap().to_rfc3339(),
        "2026-05-16T01:03:40.300+00:00"
    );
    assert_eq!(
        h.out.contents(),
        "{\"time\":\"2026-05-16T01:03:38Z\",\"message\":\"Actor started\"}\n\
         {\"time\":\"2026-05-16T01:03:40Z\",\"message\":\"Actor checkpointing\"}\n"
    );
}

#[tokio::test]
async fn follow_verbose_surfaces_delays_and_errors() {
    let h = harness("all", "").await;
    set_actor(&h.fake, actor("h", "a1", pb::ActorState::Suspended));
    h.fake.state().fail(
        "GetActor",
        tonic::Status::unavailable("transient apiserver error"),
    );
    h.runner
        .logs
        .script_error("pod-1", "kubelet stream unavailable");
    h.runner.logs.script("pod-1", vec![], true);
    let fake = h.fake.clone();
    let err = h.err.clone();
    let follow = tokio::time::timeout(Duration::from_millis(800), h.runner.follow());
    let drive = async {
        eventually("wait notice", || err.contents().contains("is not running")).await;
        set_actor(&fake, running("pod-1"));
        eventually("stream failure", || {
            err.contents().contains("Failed to stream logs")
        })
        .await;
    };
    let _ = tokio::join!(follow, drive);
    let err = h.err.contents();
    for want in [
        "Failed to get actor h/a1: rpc error: code = Unavailable desc = transient apiserver error",
        "Actor h/a1 is not running (state=ACTOR_STATE_SUSPENDED); waiting for worker assignment...",
        "Failed to stream logs from pod ns/pod-1: kubelet stream unavailable",
    ] {
        assert!(err.contains(want), "{err} lacks {want}");
    }
}

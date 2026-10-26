use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hivekeep_proto::api::v1 as api;
use parking_lot::Mutex;
use prost::Message;
use tonic::{Request, Response, Status};

use super::errors::{
    is_capacity_error, retry_while_full, HttpError, OpError, WAKE_RETRY_MAX_BACKOFF,
};
use super::knobs::{self, Knobs, DEFAULT_RESUME_RETRY_BUDGET};
use super::runtime::{
    make_ingest_buf, LoadedScript, Runtime, StartError, TEMPLATE_RECHECK_INTERVAL,
};
use super::session::{burn_rate_per_thread, SessionUser, MAX_CONSECUTIVE_STEP_FAILURES};
use super::*;
use crate::glutton::fake;
use crate::loadgen::control::ControlApi;
use crate::loadgen::duration::Seconds;
use crate::loadgen::dynconfig::{
    Holder, Lifecycle, LIFECYCLE_MODE_ASYNC, LIFECYCLE_MODE_NONE, LIFECYCLE_MODE_SYNC,
    RESUME_MODE_EXPLICIT,
};
use crate::loadgen::metrics;
use crate::loadgen::trace::Tracer;
use crate::loadgen::userclass::Config;
use crate::testutil::{HttpServer, Resp};

// ---------------------------------------------------------------- fakes

/// Records control-plane calls; fails ResumeActor / SuspendActor with
/// queued errors until each queue drains.
#[derive(Default)]
struct FakeControl {
    calls: Mutex<Vec<String>>,
    resume_errs: Mutex<VecDeque<Status>>,
    suspend_errs: Mutex<VecDeque<Status>>,
    suspend_reqs: Mutex<Vec<api::SuspendActorRequest>>,
    /// Set when a call arrived carrying a deadline.
    saw_deadline: AtomicBool,
    /// GetActorTemplate's memory limit; 0 is none.
    template_memory: AtomicI64,
    /// Stalls each DeleteActor; in_flight/max_in_flight prove fan-out.
    delete_delay: Duration,
    in_flight: AtomicI32,
    max_in_flight: AtomicI32,
    /// When set, notified once a CreateActor began; `create_gate` holds
    /// every CreateActor until opened.
    create_started: Option<tokio::sync::mpsc::UnboundedSender<()>>,
    create_gate: Option<tokio::sync::watch::Receiver<bool>>,
}

impl FakeControl {
    fn record<T>(&self, req: &Request<T>, name: &str) {
        self.calls.lock().push(name.to_string());
        if req.metadata().get("grpc-timeout").is_some() {
            self.saw_deadline.store(true, Ordering::SeqCst);
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }

    fn count(&self, name: &str) -> usize {
        self.calls.lock().iter().filter(|c| *c == name).count()
    }

    fn with_resume_errs(self, errs: Vec<Status>) -> Self {
        *self.resume_errs.lock() = errs.into();
        self
    }

    fn with_suspend_errs(self, errs: Vec<Status>) -> Self {
        *self.suspend_errs.lock() = errs.into();
        self
    }

    fn with_memory(self, m: i64) -> Self {
        self.template_memory.store(m, Ordering::SeqCst);
        self
    }
}

#[async_trait]
impl ControlApi for FakeControl {
    async fn create_hive(
        &self,
        req: Request<api::CreateHiveRequest>,
    ) -> Result<Response<api::Hive>, Status> {
        self.record(&req, "CreateHive");
        Ok(Response::new(api::Hive::default()))
    }

    async fn create_actor(
        &self,
        req: Request<api::CreateActorRequest>,
    ) -> Result<Response<api::Actor>, Status> {
        self.record(&req, "CreateActor");
        if let Some(tx) = &self.create_started {
            let _ = tx.send(());
        }
        if let Some(gate) = &self.create_gate {
            let mut g = gate.clone();
            let _ = g.wait_for(|open| *open).await;
        }
        Ok(Response::new(api::Actor::default()))
    }

    async fn resume_actor(
        &self,
        req: Request<api::ResumeActorRequest>,
    ) -> Result<Response<api::ResumeActorResponse>, Status> {
        self.record(&req, "ResumeActor");
        match self.resume_errs.lock().pop_front() {
            Some(e) => Err(e),
            None => Ok(Response::new(Default::default())),
        }
    }

    async fn suspend_actor(
        &self,
        req: Request<api::SuspendActorRequest>,
    ) -> Result<Response<api::SuspendActorResponse>, Status> {
        self.record(&req, "SuspendActor");
        self.suspend_reqs.lock().push(req.get_ref().clone());
        match self.suspend_errs.lock().pop_front() {
            Some(e) => Err(e),
            None => Ok(Response::new(Default::default())),
        }
    }

    async fn delete_actor(
        &self,
        req: Request<api::DeleteActorRequest>,
    ) -> Result<Response<api::Actor>, Status> {
        self.record(&req, "DeleteActor");
        let n = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(n, Ordering::SeqCst);
        if !self.delete_delay.is_zero() {
            tokio::time::sleep(self.delete_delay).await;
        }
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(Response::new(api::Actor::default()))
    }

    async fn get_actor_template(
        &self,
        req: Request<api::GetActorTemplateRequest>,
    ) -> Result<Response<api::ActorTemplate>, Status> {
        self.record(&req, "GetActorTemplate");
        let m = self.template_memory.load(Ordering::SeqCst);
        let mut t = api::ActorTemplate::default();
        if m != 0 {
            t.resources = Some(api::ResourceRequirements {
                limits: Some(api::Resources {
                    memory_bytes: m,
                    ..Default::default()
                }),
            });
        }
        Ok(Response::new(t))
    }
}

/// A fresh user-class label per test, so parallel tests keep their stats
/// rows apart.
fn class(test: &str) -> Arc<str> {
    format!("AgentSessionUser-{test}").into()
}

fn config(api: Arc<FakeControl>, router_url: String, k: Knobs) -> Arc<Config> {
    Arc::new(Config {
        api,
        http: reqwest::Client::new(),
        router_url,
        hive: "benchmark".into(),
        dyn_config: Arc::new(Holder::fixed(k)),
        tracer: Tracer::never("test"),
        actors_per_user: 1,
        total_actors: 1,
        spawn_concurrency: 1,
        actor_deadline: Duration::from_secs(1),
    })
}

/// A started fake glutton and a session over it.
async fn test_user(
    srv_cfg: fake::Config,
    ctl: Arc<FakeControl>,
    k: Knobs,
    test: &str,
) -> (fake::Server, SessionUser) {
    let srv = fake::Server::start(srv_cfg).await;
    let u = SessionUser::new(
        config(ctl, srv.url(), k),
        class(test),
        "agent-test".into(),
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
    );
    (srv, u)
}

fn ping_step() -> Step {
    Step {
        name: "01_test".into(),
        agent: "testing".into(),
        think: Seconds::from_secs(1),
        ops: vec![Op::ping()],
    }
}

fn explicit() -> Lifecycle {
    Lifecycle {
        resume_mode: RESUME_MODE_EXPLICIT.into(),
        ..Default::default()
    }
}

fn exhausted() -> Status {
    Status::resource_exhausted("no worker has room for the actor")
}

/// The cumulative count of stats row `name` for `class`, and its failures.
fn row_count(name: &str, class: &str) -> (f64, f64) {
    let mut total = 0.0;
    let mut failed = 0.0;
    for line in metrics::lines_of("locust_requests_total").lines() {
        if line.starts_with('#')
            || !line.contains(&format!("name=\"{name}\""))
            || !line.contains(&format!("user_class=\"{class}\""))
        {
            continue;
        }
        let v: f64 = line.rsplit(' ').next().unwrap().parse().unwrap();
        total += v;
        if line.contains("status=\"failure\"") {
            failed += v;
        }
    }
    (total, failed)
}

async fn expect_rows<F: std::future::Future<Output = ()>>(
    name: &str,
    class: &str,
    want_total: f64,
    want_failed: f64,
    f: F,
) {
    let (t0, f0) = row_count(name, class);
    f.await;
    let (t, fl) = row_count(name, class);
    assert_eq!(
        (t - t0, fl - f0),
        (want_total, want_failed),
        "{name} rows recorded (total, failed)"
    );
}

const VALID_SCRIPT: &str = r#"
name: tiny
min_actor_memory: 64Mi
steps:
- name: 01_fill
  agent: fills
  think: 1s
  ops:
  - fill_ram: {key: ctx, size: 4Mi}
  - ingest: {key: repo, size: 1Mi}
- name: 02_use
  agent: uses
  think: 500ms
  ops:
  - walk_ram: {key: ctx}
  - read_disk_data: {key: repo}
  - burn_cpu: {millis: 10}
  - dwell: {millis: 5}
  - ping: {}
"#;

// ---------------------------------------------------------------- knobs

/// The codec takes a payload naming every key the class reads and refuses
/// the values it cannot run on.
#[test]
fn knobs_codec() {
    let h = Holder::new(knobs::codec());
    let got = h.get::<Knobs>();
    assert_eq!(got, knobs::defaults());
    assert_eq!(
        got.resume_retry_budget.duration(),
        DEFAULT_RESUME_RETRY_BUDGET
    );
    h.apply(
        br#"{
        "resume_mode": "explicit", "lifecycle_mode": "async",
        "agentsession_script": "coding-session",
        "agentsession_script_file": "/etc/agentsession/script.yaml",
        "agentsession_think_scale": 0.5,
        "agentsession_snapshot_steps": "3,10,20,30,40,50,61",
        "agentsession_phase_in": 300,
        "agentsession_resume_retry_budget": 45.5
    }"#,
    )
    .expect("valid payload");
    let want = Knobs {
        lifecycle: Lifecycle {
            resume_mode: RESUME_MODE_EXPLICIT.into(),
            lifecycle_mode: LIFECYCLE_MODE_ASYNC.into(),
        },
        script: "coding-session".into(),
        script_file: "/etc/agentsession/script.yaml".into(),
        think_scale: 0.5,
        snapshot_steps: "3,10,20,30,40,50,61".into(),
        phase_in: Seconds::from_secs(300),
        resume_retry_budget: Seconds::from_millis(45_500),
    };
    assert_eq!(h.get::<Knobs>(), want);
    for (name, payload) in [
        (
            "negative think scale",
            r#"{"agentsession_think_scale": -1}"#,
        ),
        ("negative phase-in", r#"{"agentsession_phase_in": -1}"#),
        (
            "negative resume retry budget",
            r#"{"agentsession_resume_retry_budget": -1}"#,
        ),
        ("invalid resume mode", r#"{"resume_mode": "invalid_mode"}"#),
        (
            "invalid snapshot steps entry",
            r#"{"agentsession_snapshot_steps": "3,zero,10"}"#,
        ),
        (
            "non-positive snapshot steps entry",
            r#"{"agentsession_snapshot_steps": "0,10"}"#,
        ),
        (
            "snapshot steps with lifecycle mode none",
            r#"{"lifecycle_mode": "none", "agentsession_snapshot_steps": "3,10"}"#,
        ),
    ] {
        assert!(
            h.apply(payload.as_bytes()).is_err(),
            "{name}: took {payload}"
        );
        assert_eq!(
            h.get::<Knobs>(),
            want,
            "{name}: a refused payload changed the config"
        );
    }
}

// ---------------------------------------------------------------- scripts

/// Every built-in variant loads.
#[test]
fn embedded_scripts_are_valid() {
    let n = names();
    assert!(n.contains(&DEFAULT_SCRIPT), "{n:?}");
    for name in n {
        load(name).unwrap_or_else(|e| panic!("load({name}): {e}"));
    }
    assert!(load("no-such-script").is_err());
}

/// What the docs promise about the default variant: 20 steps, a 1Gi floor
/// and declared bytes well under it.
#[test]
fn default_script_shape() {
    let s = load(DEFAULT_SCRIPT).unwrap();
    assert_eq!(s.steps.len(), 20);
    assert_eq!(s.min_actor_memory, 1 << 30);
    let b = budgets(&s.steps);
    assert!(b.ram <= 128 << 20 && b.disk <= 256 << 20, "declared {b:?}");
}

/// Encode writes exactly what decode reads.
#[test]
fn encode_decode_round_trip() {
    for name in names() {
        let s = load(name).unwrap();
        let out = encode(&s);
        let back =
            decode(out.as_bytes()).unwrap_or_else(|e| panic!("decode(encode({name})): {e}\n{out}"));
        assert_eq!(s, back, "round trip changed {name}:\n{out}");
    }
}

/// Argument defaults: parallel is optional and reads as 1.
#[test]
fn decode_accepts_valid_script() {
    let s = decode(VALID_SCRIPT.as_bytes()).unwrap();
    assert_eq!(
        s.steps[1].ops[2],
        Op {
            kind: OpKind::BurnCpu,
            millis: 10,
            parallel: 1,
            ..Default::default()
        }
    );
    assert_eq!(
        s.steps[0].ops[0],
        Op {
            kind: OpKind::FillRam,
            key: "ctx".into(),
            bytes: 4 << 20,
            ..Default::default()
        }
    );
    assert_eq!(
        s.steps[1].ops[3],
        Op {
            kind: OpKind::Dwell,
            millis: 5,
            ..Default::default()
        }
    );
}

/// The mistakes a hand-written script can make, each refused at load with
/// a message naming the problem.
#[test]
fn decode_rejects() {
    for (name, from, to, want) in [
        (
            "unknown op kind",
            "- ping: {}",
            "- sleep: {}",
            "unknown op kind",
        ),
        (
            "unknown field",
            "think: 1s",
            "think: 1s\n  typo: x",
            "unknown field `typo`",
        ),
        (
            "two-key op",
            "- ping: {}",
            "- {ping: {}, walk_ram: {key: ctx}}",
            "single-key map",
        ),
        (
            "ping with key",
            "- ping: {}",
            "- ping: {key: ctx}",
            "takes no key",
        ),
        (
            "missing size",
            "- ingest: {key: repo, size: 1Mi}",
            "- ingest: {key: repo}",
            "size is required",
        ),
        (
            "bad key",
            "- ingest: {key: repo, size: 1Mi}",
            "- ingest: {key: ../repo, size: 1Mi}",
            "must match",
        ),
        (
            "zero millis",
            "- burn_cpu: {millis: 10}",
            "- burn_cpu: {millis: 0}",
            "millis must be positive",
        ),
        (
            "dwell with parallel",
            "- dwell: {millis: 5}",
            "- dwell: {millis: 5, parallel: 2}",
            "takes no parallel",
        ),
        (
            "ping with millis",
            "- ping: {}",
            "- ping: {millis: 5}",
            "takes no millis",
        ),
        (
            "walk before fill",
            "- fill_ram: {key: ctx, size: 4Mi}",
            "- ping: {}",
            "before any fill_ram",
        ),
        (
            "read before write",
            "- ingest: {key: repo, size: 1Mi}",
            "- ping: {}",
            "before any ingest",
        ),
        (
            "duplicate step",
            "name: 02_use",
            "name: 01_fill",
            "duplicate step name",
        ),
        (
            "zero think",
            "think: 500ms",
            "think: 0s",
            "think time must be positive",
        ),
        (
            "no min memory",
            "min_actor_memory: 64Mi",
            "",
            "min_actor_memory is required",
        ),
        (
            "min below declared",
            "min_actor_memory: 64Mi",
            "min_actor_memory: 4Mi",
            "below the declared",
        ),
        (
            "bad script name",
            "name: tiny",
            "name: Tiny Script",
            "script name",
        ),
    ] {
        let doc = VALID_SCRIPT.replacen(from, to, 1);
        assert_ne!(
            doc, VALID_SCRIPT,
            "{name}: the edit did not change the script"
        );
        let err = decode(doc.as_bytes()).expect_err(name).to_string();
        assert!(
            err.contains(want),
            "{name}: error {err:?} does not mention {want:?}"
        );
    }
}

/// A script file is read from disk; errors name the path.
#[test]
fn load_file_reads_and_names_the_path() {
    let dir = tempfile::tempdir().unwrap();
    let good = dir.path().join("tiny.yaml");
    std::fs::write(&good, VALID_SCRIPT).unwrap();
    let s = load_file(&good).unwrap();
    assert_eq!((s.name.as_str(), s.steps.len()), ("tiny", 2));

    let bad = dir.path().join("bad.yaml");
    std::fs::write(&bad, VALID_SCRIPT.replacen("- ping: {}", "- nap: {}", 1)).unwrap();
    let err = load_file(&bad).unwrap_err().to_string();
    assert!(
        err.contains(bad.to_str().unwrap()) && err.contains("unknown op kind"),
        "{err}"
    );
    assert!(load_file(&dir.path().join("missing.yaml")).is_err());
}

// ---------------------------------------------------------------- ops

/// Every op of the default script marshals a request the Actor's routes
/// accept.
#[tokio::test]
async fn exec_op_against_fake() {
    let (srv, u) = test_user(
        fake::Config {
            data: b"filecontents".to_vec(),
            ..Default::default()
        },
        Arc::new(FakeControl::default()),
        Knobs::default(),
        "exec_op",
    )
    .await;
    let script = load(DEFAULT_SCRIPT).unwrap();
    let mut sent = 0;
    for s in &script.steps {
        for (i, o) in s.ops.iter().enumerate() {
            let mut o = o.clone();
            // Transport shape is what matters here, not moving MiB.
            if o.kind == OpKind::Ingest && o.bytes > 1 << 10 {
                o.bytes = 1 << 10;
            }
            if matches!(o.kind, OpKind::BurnCpu | OpKind::Dwell) {
                o.millis = 1;
            }
            u.exec_op(&o)
                .await
                .unwrap_or_else(|e| panic!("step {} op {i}: {e}", s.name));
            if o.kind != OpKind::Dwell {
                sent += 1;
            }
        }
    }
    let rec = srv.recorded();
    assert_eq!(rec.paths.len(), sent);
    assert!(rec.ingest_sizes.iter().all(|n| *n <= 1 << 10));
    assert!(rec.burn_millis.iter().all(|ms| *ms == 1));
    // Requests name the target Actor.
    assert_eq!(
        rec.headers[0]
            .get(crate::wire::TARGET_ACTOR_HEADER)
            .unwrap(),
        "benchmark/agent-test"
    );
}

/// A dwell idles for its duration without any request; a caller that
/// gives up ends it early.
#[tokio::test]
async fn dwell() {
    let (srv, u) = test_user(
        fake::Config::default(),
        Arc::new(FakeControl::default()),
        Knobs::default(),
        "dwell",
    )
    .await;
    let start = Instant::now();
    let d = Op {
        kind: OpKind::Dwell,
        millis: 30,
        ..Default::default()
    };
    u.exec_op(&d).await.unwrap();
    assert!(start.elapsed() >= Duration::from_millis(30));
    assert!(srv.recorded_paths().is_empty(), "a dwell sends nothing");
    let long = Op {
        kind: OpKind::Dwell,
        millis: 10_000,
        ..Default::default()
    };
    let start = Instant::now();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), u.exec_op(&long))
            .await
            .is_err()
    );
    assert!(start.elapsed() < Duration::from_secs(5));
}

#[test]
fn burn_rate_per_thread_normalizes() {
    let o = Op {
        kind: OpKind::BurnCpu,
        millis: 500,
        parallel: 2,
        ..Default::default()
    };
    assert_eq!(burn_rate_per_thread(&o, 10_000), Some(10_000.0));
    let zero = Op {
        kind: OpKind::BurnCpu,
        parallel: 1,
        ..Default::default()
    };
    assert_eq!(burn_rate_per_thread(&zero, 5), None);
}

// ---------------------------------------------------------------- loading

fn runtime(ctl: Arc<FakeControl>, url: String, k: Knobs, test: &str) -> Runtime {
    Runtime::with_user_class(config(ctl, url, k), &class(test))
}

fn swap_knobs(rt: &mut Runtime, k: Knobs) {
    let cfg = Config {
        dyn_config: Arc::new(Holder::fixed(k)),
        ..(*rt.cfg).clone()
    };
    rt.cfg = Arc::new(cfg);
}

/// The script follows the knob: empty means the default, an unknown name
/// is an error that keeps the previous script, a changed knob or a
/// rewritten file loads the new one.
#[test]
fn load_script_follows_the_knob() {
    let ctl = Arc::new(FakeControl::default());
    let mut rt = runtime(
        ctl,
        String::new(),
        Knobs {
            script: "no-such-script".into(),
            ..Default::default()
        },
        "load_script",
    );
    assert!(rt.load_script().is_err());
    assert!(
        rt.loaded.lock().is_none(),
        "a failed load must not cache a script"
    );

    swap_knobs(&mut rt, Knobs::default());
    let s = rt.load_script().unwrap();
    assert_eq!(s.script.name, DEFAULT_SCRIPT);
    assert!(!s.ingest_buf.is_empty());
    assert!(
        Arc::ptr_eq(&rt.load_script().unwrap(), &s),
        "unchanged knob reuses the script"
    );

    swap_knobs(
        &mut rt,
        Knobs {
            script: "no-such-script".into(),
            ..Default::default()
        },
    );
    assert!(rt.load_script().is_err());
    assert!(
        Arc::ptr_eq(rt.loaded.lock().as_ref().unwrap(), &s),
        "a failed reload keeps the loaded script"
    );

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tiny.yaml");
    std::fs::write(&path, VALID_SCRIPT).unwrap();
    swap_knobs(
        &mut rt,
        Knobs {
            script_file: path.to_str().unwrap().into(),
            ..Default::default()
        },
    );
    let next = rt.load_script().unwrap();
    assert_eq!(next.script.name, "tiny");
    assert_eq!(next.ingest_buf.len(), 1 << 20);

    // A rewritten file at the same path is a new script.
    std::fs::write(
        &path,
        VALID_SCRIPT.replacen("name: tiny", "name: tiny-v2", 1),
    )
    .unwrap();
    let rewritten = rt.load_script().unwrap();
    assert_eq!(rewritten.script.name, "tiny-v2");
    assert!(Arc::ptr_eq(&rt.load_script().unwrap(), &rewritten));
}

/// A file path beats a built-in name.
#[test]
fn load_script_prefers_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tiny.yaml");
    std::fs::write(&path, VALID_SCRIPT).unwrap();
    let rt = runtime(
        Arc::new(FakeControl::default()),
        String::new(),
        Knobs {
            script: DEFAULT_SCRIPT.into(),
            script_file: path.to_str().unwrap().into(),
            ..Default::default()
        },
        "prefers_file",
    );
    assert_eq!(rt.load_script().unwrap().script.name, "tiny");
}

fn loaded_default() -> LoadedScript {
    LoadedScript::new(load(DEFAULT_SCRIPT).unwrap(), DEFAULT_SCRIPT, [0; 32])
}

/// A template smaller than the script's floor is refused before any Actor
/// is created; a big enough one, or one with no limit, proceeds.
#[tokio::test]
async fn start_user_checks_template_memory() {
    for (name, memory, want_start) in [
        ("too small", 512 << 20, false),
        ("exact", 1 << 30, true),
        ("no limit", 0, true),
    ] {
        let srv = fake::Server::start(fake::Config::default()).await;
        let ctl = Arc::new(FakeControl::default().with_memory(memory));
        let rt = runtime(ctl.clone(), srv.url(), Knobs::default(), "template_memory");
        let loaded = loaded_default();
        let res = rt.start_user(0, &loaded).await;
        let created = ctl.count("CreateActor") > 0;
        if want_start {
            let u = res.unwrap_or_else(|e| panic!("{name}: {e:?}"));
            assert!(created, "{name}");
            assert_eq!(u.steps.len(), loaded.script.steps.len());
            assert_eq!(
                u.ingest_buf.len(),
                make_ingest_buf(&loaded.script.steps).len()
            );
        } else {
            match res {
                Err(StartError::Failed(e)) => {
                    assert!(e.contains("min_actor_memory"), "{name}: {e}")
                }
                other => panic!("{name}: want a refusal, got {:?}", other.map(|_| ())),
            }
            assert!(!created, "{name}: CreateActor despite the refusal");
        }
    }
}

/// A refusal is re-checked after the recheck interval, so a redeploy with
/// a bigger limit is picked up; success is cached.
#[tokio::test]
async fn template_memory_refusal_expires() {
    let ctl = Arc::new(FakeControl::default().with_memory(512 << 20));
    let rt = runtime(
        ctl.clone(),
        String::new(),
        Knobs::default(),
        "refusal_expires",
    );
    let script = Arc::new(load(DEFAULT_SCRIPT).unwrap());
    assert!(rt.check_template_memory(&script).await.is_err());
    ctl.template_memory.store(1 << 30, Ordering::SeqCst);
    assert!(
        rt.check_template_memory(&script).await.is_err(),
        "the refusal stands for the interval"
    );
    assert_eq!(ctl.count("GetActorTemplate"), 1);
    rt.template.lock().await.err_at = Instant::now().checked_sub(2 * TEMPLATE_RECHECK_INTERVAL);
    rt.check_template_memory(&script)
        .await
        .expect("the redeployed template passes");
    rt.check_template_memory(&script).await.unwrap();
    assert_eq!(ctl.count("GetActorTemplate"), 2, "success is cached");
}

/// A knob change affects sessions started afterwards, not a running one.
#[tokio::test]
async fn running_session_keeps_its_script() {
    let srv = fake::Server::start(fake::Config::default()).await;
    let ctl = Arc::new(FakeControl::default().with_memory(1 << 30));
    let mut rt = runtime(ctl, srv.url(), Knobs::default(), "keeps_script");
    let first = rt.load_script().unwrap();
    let session = rt.start_user(0, &first).await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tiny.yaml");
    std::fs::write(&path, VALID_SCRIPT).unwrap();
    swap_knobs(
        &mut rt,
        Knobs {
            script_file: path.to_str().unwrap().into(),
            ..Default::default()
        },
    );
    let second = rt.load_script().unwrap();
    assert_eq!(second.script.name, "tiny");
    assert_eq!(session.steps.len(), first.script.steps.len());
    assert_eq!(session.ingest_buf.len(), first.ingest_buf.len());
}

/// The think multiplier and its jitter bounds.
#[test]
fn think_scaling() {
    let mut rt = runtime(
        Arc::new(FakeControl::default()),
        String::new(),
        Knobs {
            think_scale: 0.5,
            ..Default::default()
        },
        "think",
    );
    let s = Step {
        think: Seconds::from_secs(10),
        ..ping_step()
    };
    for _ in 0..100 {
        let got = rt.think(&s);
        assert!(
            got >= Duration::from_secs(4) && got <= Duration::from_secs(6),
            "{got:?}"
        );
    }
    swap_knobs(&mut rt, Knobs::default());
    for _ in 0..100 {
        let got = rt.think(&s);
        assert!(
            got >= Duration::from_secs(8) && got <= Duration::from_secs(12),
            "unset scale: {got:?}"
        );
    }
}

#[test]
fn phase_in_delay() {
    let off = runtime(
        Arc::new(FakeControl::default()),
        String::new(),
        Knobs::default(),
        "phase",
    );
    assert_eq!(off.phase_in_delay(), Duration::ZERO);
    let window = Duration::from_secs(10);
    let r = runtime(
        Arc::new(FakeControl::default()),
        String::new(),
        Knobs {
            phase_in: window.into(),
            ..Default::default()
        },
        "phase",
    );
    let (mut lo, mut hi) = (window, Duration::ZERO);
    for _ in 0..200 {
        let got = r.phase_in_delay();
        assert!(got < window, "{got:?}");
        lo = lo.min(got);
        hi = hi.max(got);
    }
    assert!(hi - lo >= window / 4, "draws spanned only {:?}", hi - lo);
}

/// A user waits once: a retried start does not wait again.
#[tokio::test]
async fn phase_in_waits_once() {
    let window = Duration::from_millis(40);
    let r = runtime(
        Arc::new(FakeControl::default()),
        String::new(),
        Knobs {
            phase_in: window.into(),
            ..Default::default()
        },
        "phase_once",
    );
    r.phase_in(7).await;
    let start = Instant::now();
    r.phase_in(7).await;
    assert!(start.elapsed() < window / 4);
    assert!(r.phased_in.lock().contains(&7));
}

// ---------------------------------------------------------------- shutdown

/// A user still in its phase-in wait when the run ends must not start a
/// session once it wakes.
#[tokio::test]
async fn iterate_after_shutdown_starts_no_session() {
    let srv = fake::Server::start(fake::Config::default()).await;
    let ctl = Arc::new(FakeControl::default());
    let rt = runtime(ctl.clone(), srv.url(), Knobs::default(), "after_shutdown");
    rt.shutdown_all().await;
    rt.iterate_vu(0).await;
    assert_eq!(ctl.count("CreateActor"), 0);
    assert!(rt.users.lock().is_empty());
}

fn gated_control() -> (
    Arc<FakeControl>,
    tokio::sync::mpsc::UnboundedReceiver<()>,
    tokio::sync::watch::Sender<bool>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
    let ctl = FakeControl {
        create_started: Some(tx),
        create_gate: Some(gate_rx),
        ..Default::default()
    };
    (Arc::new(ctl), rx, gate_tx)
}

/// A user inside its start when shutdown runs is not in `users` yet;
/// shutdown waits for it, and the user cleans up its own session exactly
/// once before shutdown returns.
#[tokio::test]
async fn shutdown_waits_for_session_started_during_it() {
    let srv = fake::Server::start(fake::Config::default()).await;
    let (ctl, mut started, gate) = gated_control();
    let rt = Arc::new(runtime(
        ctl.clone(),
        srv.url(),
        Knobs::default(),
        "shutdown_waits",
    ));
    let r = rt.clone();
    let iterated = tokio::spawn(async move { r.iterate_vu(0).await });
    started.recv().await.unwrap();
    let r = rt.clone();
    let shutdown = tokio::spawn(async move { r.shutdown_all().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !shutdown.is_finished(),
        "shutdown returned with a start in flight"
    );
    gate.send(true).unwrap();
    shutdown.await.unwrap();
    assert_eq!(
        ctl.count("DeleteActor"),
        1,
        "the session started during shutdown is cleaned up once"
    );
    assert!(rt.users.lock().is_empty());
    iterated.await.unwrap();
}

/// The wait for in-flight starts is bounded by the caller's budget.
#[tokio::test]
async fn shutdown_stops_waiting_for_starts_when_budget_expires() {
    let srv = fake::Server::start(fake::Config::default()).await;
    let (ctl, mut started, gate) = gated_control();
    let rt = Arc::new(runtime(
        ctl.clone(),
        srv.url(),
        Knobs::default(),
        "shutdown_budget",
    ));
    let r = rt.clone();
    let iterated = tokio::spawn(async move { r.iterate_vu(0).await });
    started.recv().await.unwrap();
    let start = Instant::now();
    let _ = tokio::time::timeout(Duration::from_millis(50), rt.shutdown_all()).await;
    assert!(start.elapsed() < Duration::from_secs(2));
    assert_eq!(ctl.count("DeleteActor"), 0, "the start is still gated");
    gate.send(true).unwrap();
    iterated.await.unwrap();
}

/// The sweep runs sessions concurrently and still cleans up every one.
#[tokio::test]
async fn shutdown_fans_out() {
    const SESSIONS: usize = 40;
    let ctl = Arc::new(FakeControl {
        delete_delay: Duration::from_millis(20),
        ..Default::default()
    });
    let rt = runtime(ctl.clone(), String::new(), Knobs::default(), "fans_out");
    let mut held = Vec::new();
    for i in 0..SESSIONS {
        let u = Arc::new(tokio::sync::Mutex::new(SessionUser::new(
            rt.cfg.clone(),
            rt.user_class.clone(),
            format!("agent-{i}"),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )));
        held.push(u.clone());
        rt.users.lock().insert(i as u64, u);
    }
    let start = Instant::now();
    rt.shutdown_all().await;
    let elapsed = start.elapsed();
    assert_eq!(ctl.count("DeleteActor"), SESSIONS);
    assert!(ctl.max_in_flight.load(Ordering::SeqCst) >= 2);
    assert!(
        elapsed < ctl.delete_delay * SESSIONS as u32,
        "no faster than a serial sweep: {elapsed:?}"
    );
    for u in held {
        assert!(u.lock().await.cleaned_up);
    }
}

// ---------------------------------------------------------------- steps

/// A failed suspend strands the Actor; the next step finishes the suspend
/// instead of waking an awake Actor.
#[tokio::test]
async fn run_step_retries_stranded_hibernate() {
    let ctl = Arc::new(
        FakeControl::default()
            .with_suspend_errs(vec![Status::unavailable("hivekeep-api restarting")]),
    );
    let (srv, mut u) = test_user(
        fake::Config::default(),
        ctl.clone(),
        Knobs::default(),
        "stranded",
    )
    .await;
    assert!(u.run_step(&ping_step()).await, "the step's ops succeeded");
    assert!(u.hibernate_pending && !u.broken);
    let served = srv.recorded_paths().len();

    assert!(
        !u.run_step(&ping_step()).await,
        "the step must not advance while re-driving the suspend"
    );
    assert_eq!(ctl.calls().last().map(String::as_str), Some("SuspendActor"));
    assert_eq!(
        srv.recorded_paths().len(),
        served,
        "no wake ping against a stranded Actor"
    );
    assert!(!u.hibernate_pending && !u.broken);
    assert!(
        u.run_step(&ping_step()).await,
        "the step runs once the suspend cleared"
    );
}

/// A step ends with an async suspend by default and in async mode, a sync
/// one in sync mode.
#[tokio::test]
async fn run_step_hibernate_mode() {
    for (name, mode, want) in [
        ("default", "", api::DurabilityMode::Asynchronous),
        (
            "async",
            LIFECYCLE_MODE_ASYNC,
            api::DurabilityMode::Asynchronous,
        ),
        (
            "sync",
            LIFECYCLE_MODE_SYNC,
            api::DurabilityMode::Synchronous,
        ),
    ] {
        let ctl = Arc::new(FakeControl::default());
        let k = Knobs {
            lifecycle: Lifecycle {
                lifecycle_mode: mode.into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let (_srv, mut u) =
            test_user(fake::Config::default(), ctl.clone(), k, "hibernate_mode").await;
        assert!(u.run_step(&ping_step()).await, "{name}");
        let want = api::SuspendActorRequest {
            actor: Some(api::ObjectRef {
                hive: "benchmark".into(),
                name: "agent-test".into(),
            }),
            options: Some(api::SuspendOptions { mode: want as i32 }),
        };
        assert_eq!(*ctl.suspend_reqs.lock(), vec![want], "{name}");
    }
}

/// A CRASHED Actor never recovers, so it is replaced on the first failure.
#[tokio::test]
async fn run_step_replaces_crashed_actor_immediately() {
    let ctl = Arc::new(
        FakeControl::default()
            .with_resume_errs(vec![Status::aborted("actor benchmark/agent-test crashed")]),
    );
    let k = Knobs {
        lifecycle: explicit(),
        ..Default::default()
    };
    let (_srv, mut u) = test_user(fake::Config::default(), ctl, k, "crashed").await;
    assert!(!u.run_step(&ping_step()).await);
    assert!(u.broken);
}

/// In implicit mode the gRPC verdict arrives from the suspend that follows
/// a failed wake: FAILED_PRECONDITION means a state nothing moves the Actor
/// out of.
#[tokio::test]
async fn run_step_replaces_stuck_actor_after_failed_wake() {
    let ctl =
        Arc::new(
            FakeControl::default().with_suspend_errs(vec![Status::failed_precondition(
                "MarkSuspending prerequisite not met (got: ACTOR_STATE_CRASHED)",
            )]),
        );
    let (_srv, mut u) = test_user(
        fake::Config {
            status: Some(503),
            ..Default::default()
        },
        ctl,
        Knobs::default(),
        "stuck",
    )
    .await;
    assert!(!u.run_step(&ping_step()).await);
    assert!(u.broken);
}

/// A saturated pool reports RESOURCE_EXHAUSTED to every user; the Actor is
/// kept.
#[tokio::test]
async fn run_step_keeps_actor_through_capacity_shortage() {
    let rounds = MAX_CONSECUTIVE_STEP_FAILURES as usize + 2;
    let ctl = Arc::new(FakeControl::default().with_resume_errs(vec![exhausted(); rounds]));
    let k = Knobs {
        lifecycle: explicit(),
        ..Default::default()
    };
    let (_srv, mut u) = test_user(fake::Config::default(), ctl.clone(), k, "capacity").await;
    for _ in 0..rounds {
        assert!(!u.run_step(&ping_step()).await);
    }
    assert!(!u.broken && u.consecutive_failures == 0);
    assert_eq!(
        ctl.count("ResumeActor"),
        rounds,
        "a zero budget fits one attempt per step"
    );
}

/// A full fleet is retried within the budget; the step goes through once a
/// slot opens, with one TimeToWake row for the whole wake.
#[tokio::test]
async fn run_step_retries_wake_while_fleet_is_full() {
    const FULL: usize = 3;
    let ctl = Arc::new(FakeControl::default().with_resume_errs(vec![exhausted(); FULL]));
    let k = Knobs {
        lifecycle: explicit(),
        resume_retry_budget: Seconds::from_secs(10),
        ..Default::default()
    };
    let (srv, mut u) = test_user(fake::Config::default(), ctl.clone(), k, "retries_full").await;
    let cls = u.user_class.clone();
    expect_rows("TimeToWake", &cls, 1.0, 0.0, async {
        assert!(
            u.run_step(&ping_step()).await,
            "the resume succeeded within the budget"
        );
    })
    .await;
    assert_eq!(ctl.count("ResumeActor"), FULL + 1);
    assert!(!u.broken && u.consecutive_failures == 0);
    assert_eq!(
        srv.recorded_paths().len(),
        2,
        "the wake ping and the step's op"
    );
}

/// When the budget runs out the step is given up: the Actor is kept and
/// nothing counts toward replacement.
#[tokio::test]
async fn run_step_gives_up_wake_after_retry_budget() {
    let ctl = Arc::new(FakeControl::default().with_resume_errs(vec![exhausted(); 100]));
    let budget = Duration::from_millis(200);
    let k = Knobs {
        lifecycle: explicit(),
        resume_retry_budget: budget.into(),
        ..Default::default()
    };
    let (srv, mut u) = test_user(fake::Config::default(), ctl.clone(), k, "gives_up").await;
    let cls = u.user_class.clone();
    let start = Instant::now();
    expect_rows("TimeToWake", &cls, 1.0, 1.0, async {
        assert!(!u.run_step(&ping_step()).await);
    })
    .await;
    assert!(start.elapsed() <= budget + WAKE_RETRY_MAX_BACKOFF);
    let attempts = ctl.count("ResumeActor");
    assert!((2..=5).contains(&attempts), "attempts = {attempts}");
    assert!(!u.broken && u.consecutive_failures == 0);
    assert!(srv.recorded_paths().is_empty());
    assert_ne!(ctl.calls().last().map(String::as_str), Some("SuspendActor"));
}

/// In implicit mode the capacity error is the router's 503 on the wake
/// ping; it is retried and the step counts once the ping lands.
#[tokio::test]
async fn run_step_retries_wake_ping_on_router_capacity_error() {
    let failed = Arc::new(AtomicUsize::new(0));
    let served = Arc::new(AtomicUsize::new(0));
    let (f, s) = (failed.clone(), served.clone());
    let router = HttpServer::start(move |_| {
        if f.load(Ordering::SeqCst) < 2 {
            f.fetch_add(1, Ordering::SeqCst);
            return Resp::status(503, "no worker has room for the actor");
        }
        s.fetch_add(1, Ordering::SeqCst);
        Resp::ok(
            crate::pb::glutton::v1::PingResponse {
                message: "think".into(),
            }
            .encode_to_vec(),
        )
    })
    .await;
    let cls = class("router_capacity");
    let mut u = SessionUser::new(
        config(
            Arc::new(FakeControl::default()),
            router.url(),
            Knobs {
                resume_retry_budget: Seconds::from_secs(10),
                ..Default::default()
            },
        ),
        cls.clone(),
        "agent-test".into(),
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
    );
    expect_rows("TimeToWake", &cls, 1.0, 0.0, async {
        assert!(u.run_step(&ping_step()).await);
    })
    .await;
    assert_eq!(failed.load(Ordering::SeqCst), 2);
    assert_eq!(
        served.load(Ordering::SeqCst),
        2,
        "the wake ping and the step's op"
    );
    assert!(!u.broken && u.consecutive_failures == 0);
}

#[test]
fn is_capacity_error_table() {
    let http = |status| {
        OpError::Http(HttpError {
            route: "/ping".into(),
            status,
            body: String::new(),
        })
    };
    for (name, err, want) in [
        ("no room", Some(OpError::Status(exhausted())), true),
        (
            "api server restarting",
            Some(OpError::Status(Status::unavailable("connection refused"))),
            true,
        ),
        ("router 503", Some(http(503)), true),
        ("router 504", Some(http(504)), true),
        ("router 429", Some(http(429)), true),
        ("router 404", Some(http(404)), false),
        ("router 500", Some(http(500)), false),
        (
            "crashed",
            Some(OpError::Status(Status::aborted(
                "actor benchmark/agent-test crashed",
            ))),
            false,
        ),
        (
            "deadline",
            Some(OpError::Status(Status::deadline_exceeded("deadline"))),
            false,
        ),
        (
            "plain error",
            Some(OpError::Other("dial tcp: connection refused".into())),
            false,
        ),
        ("nil", None, false),
    ] {
        assert_eq!(is_capacity_error(err.as_ref()), want, "{name}");
    }
}

/// A capacity error that becomes another error is returned at once.
#[tokio::test]
async fn retry_while_full_stops_on_other_errors() {
    let calls = AtomicUsize::new(0);
    let res = retry_while_full(
        tokio::time::Instant::now() + Duration::from_secs(60),
        || {
            let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if n == 1 {
                    Err(OpError::Status(exhausted()))
                } else {
                    Err(OpError::Other("sandbox exploded".into()))
                }
            }
        },
    )
    .await;
    assert!(matches!(res, Err(OpError::Other(m)) if m == "sandbox exploded"));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

/// Router 503/504/429 are a busy fleet; the Actor is kept.
#[tokio::test]
async fn run_step_keeps_actor_through_router_capacity_errors() {
    for status in [503u16, 504, 429] {
        let (_srv, mut u) = test_user(
            fake::Config {
                status: Some(status),
                ..Default::default()
            },
            Arc::new(FakeControl::default()),
            Knobs::default(),
            "router_busy",
        )
        .await;
        for _ in 0..MAX_CONSECUTIVE_STEP_FAILURES + 2 {
            assert!(!u.run_step(&ping_step()).await);
        }
        assert!(!u.broken && u.consecutive_failures == 0, "HTTP {status}");
    }
}

/// A router 404 means the Actor record is gone: replaced at once.
#[tokio::test]
async fn run_step_replaces_actor_on_router_not_found() {
    let (_srv, mut u) = test_user(
        fake::Config {
            status: Some(404),
            ..Default::default()
        },
        Arc::new(FakeControl::default()),
        Knobs::default(),
        "router_404",
    )
    .await;
    u.run_step(&ping_step()).await;
    assert!(u.broken);
}

/// Other HTTP failures count toward the threshold.
#[tokio::test]
async fn run_step_replaces_actor_after_repeated_step_failures() {
    let (_srv, mut u) = test_user(
        fake::Config {
            status: Some(502),
            ..Default::default()
        },
        Arc::new(FakeControl::default()),
        Knobs::default(),
        "router_502",
    )
    .await;
    for i in 1..=MAX_CONSECUTIVE_STEP_FAILURES {
        u.run_step(&ping_step()).await;
        assert_eq!(
            u.broken,
            i == MAX_CONSECUTIVE_STEP_FAILURES,
            "after {i} failed steps"
        );
    }
}

#[tokio::test]
async fn control_rpcs_carry_a_deadline() {
    let ctl = Arc::new(FakeControl::default());
    let k = Knobs {
        lifecycle: explicit(),
        ..Default::default()
    };
    let (_srv, mut u) = test_user(fake::Config::default(), ctl.clone(), k, "deadline").await;
    u.run_step(&ping_step()).await;
    u.suspend_and_delete().await;
    assert!(ctl.saw_deadline.load(Ordering::SeqCst));
    for name in ["ResumeActor", "SuspendActor", "DeleteActor"] {
        assert!(
            ctl.count(name) > 0,
            "{name} never called: {:?}",
            ctl.calls()
        );
    }
}

/// Resident mode never parks or explicitly resumes, records no wake rows,
/// and still suspends before deleting on teardown.
#[tokio::test]
async fn lifecycle_mode_none() {
    let srv = fake::Server::start(fake::Config::default()).await;
    let ctl = Arc::new(FakeControl::default().with_memory(1 << 30));
    let k = Knobs {
        lifecycle: Lifecycle {
            lifecycle_mode: LIFECYCLE_MODE_NONE.into(),
            resume_mode: RESUME_MODE_EXPLICIT.into(),
        },
        ..Default::default()
    };
    let rt = runtime(ctl.clone(), srv.url(), k, "mode_none");
    let loaded = rt.load_script().unwrap();
    let mut session = rt.start_user(0, &loaded).await.unwrap();
    assert!(session.awake);
    let cls = rt.user_class.clone();
    let (wakes, _) = row_count("WakeFirstTouch", &cls);
    expect_rows("TimeToWake", &cls, 0.0, 0.0, async {
        for _ in 0..3 {
            assert!(session.run_step(&ping_step()).await);
        }
    })
    .await;
    assert_eq!(row_count("WakeFirstTouch", &cls).0, wakes);
    assert_eq!(ctl.count("SuspendActor"), 0);
    assert_eq!(ctl.count("ResumeActor"), 0);
    session.suspend_and_delete().await;
    assert_eq!(ctl.count("SuspendActor"), 1);
    assert_eq!(ctl.count("DeleteActor"), 1);
}

/// With snapshot steps the Actor stays resident at start and between other
/// steps, parks only after the chosen steps, and books a wake only for the
/// steps that start parked. A step number past the script is refused
/// before CreateActor.
#[tokio::test]
async fn snapshot_steps() {
    let srv = fake::Server::start(fake::Config::default()).await;
    let ctl = Arc::new(FakeControl::default().with_memory(1 << 30));
    let k = Knobs {
        lifecycle: Lifecycle {
            lifecycle_mode: LIFECYCLE_MODE_ASYNC.into(),
            resume_mode: RESUME_MODE_EXPLICIT.into(),
        },
        snapshot_steps: "2,4".into(),
        ..Default::default()
    };
    let rt = runtime(ctl.clone(), srv.url(), k, "snapshot_steps");
    let loaded = rt.load_script().unwrap();
    let mut session = rt.start_user(0, &loaded).await.unwrap();
    assert!(ctl.suspend_reqs.lock().is_empty());
    let cls = rt.user_class.clone();
    let (wakes, _) = row_count("WakeFirstTouch", &cls);
    for i in 0..4 {
        session.step_index = i;
        assert!(session.run_step(&ping_step()).await, "step {}", i + 1);
    }
    let want = api::SuspendActorRequest {
        actor: Some(session.actor_ref()),
        options: Some(api::SuspendOptions {
            mode: api::DurabilityMode::Asynchronous as i32,
        }),
    };
    assert_eq!(
        *ctl.suspend_reqs.lock(),
        vec![want.clone(), want],
        "steps 2 and 4 park"
    );
    assert_eq!(ctl.count("ResumeActor"), 1, "only step 3 starts parked");
    assert_eq!(row_count("WakeFirstTouch", &cls).0 - wakes, 1.0);

    let bad_ctl = Arc::new(FakeControl::default().with_memory(1 << 30));
    let bad = runtime(
        bad_ctl.clone(),
        srv.url(),
        Knobs {
            lifecycle: Lifecycle {
                lifecycle_mode: LIFECYCLE_MODE_ASYNC.into(),
                ..Default::default()
            },
            snapshot_steps: "2,99".into(),
            ..Default::default()
        },
        "snapshot_steps_bad",
    );
    match bad.start_user(0, &loaded).await {
        Err(StartError::Failed(e)) => assert!(e.contains("exceeds script"), "{e}"),
        other => panic!("want a refusal, got {:?}", other.map(|_| ())),
    }
    assert_eq!(bad_ctl.count("CreateActor"), 0);
}

#[test]
fn entries_register_the_class() {
    let e = entries();
    assert_eq!(e.len(), 1);
    assert_eq!(
        (e[0].name, e[0].user_class),
        ("agentsession", AGENT_SESSION_USER_CLASS)
    );
    assert_eq!(e[0].holder().get::<Knobs>(), knobs::defaults());
}

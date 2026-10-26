use std::sync::atomic::AtomicI32;

use hivekeep_proto::api::v1::ActorState;
use tokio::sync::{mpsc, watch};

use super::super::knobs;
use super::*;
use crate::loadgen::control::ControlApi;
use crate::loadgen::dynconfig::Holder;
use crate::loadgen::trace::Tracer;

/// A gate that holds every ResumeActor: each signals `entered`, then waits
/// for the gate to open (or its caller to give up).
struct Gate {
    entered: mpsc::UnboundedSender<()>,
    open: watch::Receiver<bool>,
}

/// Keeps each Actor's state, so a resume of a running Actor reports
/// `resumed: false` the way hivekeep-api does.
#[derive(Default)]
struct Fake {
    s: Mutex<FakeState>,
}

#[derive(Default)]
struct FakeState {
    calls: Vec<String>,
    states: HashMap<String, ActorState>,
    resume_err: Option<Status>,
    resume_failures: usize,
    pause_err: Option<Status>,
    hive_err: Option<Status>,
    create_err: Option<Status>,
    delete_aborts: usize,
    server_elapsed_us: Option<String>,
    gate: Option<Gate>,
}

impl Fake {
    fn with<R>(&self, f: impl FnOnce(&mut FakeState) -> R) -> R {
        f(&mut self.s.lock())
    }

    fn calls_to(&self, rpc: &str) -> Vec<String> {
        self.s
            .lock()
            .calls
            .iter()
            .filter_map(|c| {
                c.split_once(' ')
                    .filter(|(r, _)| *r == rpc)
                    .map(|(_, a)| a.to_string())
            })
            .collect()
    }

    fn calls(&self) -> Vec<String> {
        self.s.lock().calls.clone()
    }

    fn state(&self, name: &str) -> Option<ActorState> {
        self.s.lock().states.get(name).copied()
    }

    fn answer<T>(&self, s: &FakeState, v: T) -> Response<T> {
        let mut r = Response::new(v);
        if let Some(us) = &s.server_elapsed_us {
            r.metadata_mut()
                .insert(SERVER_ELAPSED_HEADER, us.parse().unwrap());
        }
        r
    }

    /// Installs a resume gate; returns the entered signal and the opener.
    fn install_gate(&self) -> (mpsc::UnboundedReceiver<()>, watch::Sender<bool>) {
        let (etx, erx) = mpsc::unbounded_channel();
        let (otx, orx) = watch::channel(false);
        self.with(|s| {
            s.gate = Some(Gate {
                entered: etx,
                open: orx,
            })
        });
        (erx, otx)
    }
}

fn actor_name(r: &Option<api::ObjectRef>) -> String {
    r.as_ref().map(|r| r.name.clone()).unwrap_or_default()
}

#[async_trait]
impl ControlApi for Fake {
    async fn create_hive(
        &self,
        req: Request<api::CreateHiveRequest>,
    ) -> Result<Response<api::Hive>, Status> {
        let name = req
            .get_ref()
            .hive
            .as_ref()
            .and_then(|h| h.metadata.as_ref())
            .map(|m| m.name.clone())
            .unwrap_or_default();
        let mut s = self.s.lock();
        s.calls.push(format!("CreateHive {name}"));
        match s.hive_err.clone() {
            Some(e) => Err(e),
            None => Ok(Response::new(api::Hive::default())),
        }
    }

    async fn create_actor(
        &self,
        req: Request<api::CreateActorRequest>,
    ) -> Result<Response<api::Actor>, Status> {
        let name = req
            .get_ref()
            .actor
            .as_ref()
            .and_then(|a| a.metadata.as_ref())
            .map(|m| m.name.clone())
            .unwrap_or_default();
        let mut s = self.s.lock();
        s.calls.push(format!("CreateActor {name}"));
        if let Some(e) = s.create_err.clone() {
            return Err(e);
        }
        s.states.insert(name, ActorState::Suspended);
        Ok(Response::new(api::Actor::default()))
    }

    async fn resume_actor(
        &self,
        req: Request<api::ResumeActorRequest>,
    ) -> Result<Response<api::ResumeActorResponse>, Status> {
        let gate = self.with(|s| s.gate.as_ref().map(|g| (g.entered.clone(), g.open.clone())));
        if let Some((entered, mut open)) = gate {
            let _ = entered.send(());
            let _ = open.wait_for(|o| *o).await;
        }
        let name = actor_name(&req.get_ref().actor);
        let mut s = self.s.lock();
        s.calls.push(format!("ResumeActor {name}"));
        if let Some(e) = s.resume_err.clone() {
            return Err(e);
        }
        if s.resume_failures > 0 {
            s.resume_failures -= 1;
            return Err(Status::unavailable("down"));
        }
        match s.states.get(&name).copied() {
            Some(ActorState::Running) => Ok(self.answer(
                &s,
                api::ResumeActorResponse {
                    resumed: false,
                    ..Default::default()
                },
            )),
            Some(ActorState::Suspended) => {
                s.states.insert(name, ActorState::Running);
                Ok(self.answer(
                    &s,
                    api::ResumeActorResponse {
                        resumed: true,
                        ..Default::default()
                    },
                ))
            }
            _ => Err(Status::aborted("actor crashed")),
        }
    }

    /// Only an asynchronous suspend is faked, which is how the user pauses.
    async fn suspend_actor(
        &self,
        req: Request<api::SuspendActorRequest>,
    ) -> Result<Response<api::SuspendActorResponse>, Status> {
        let name = actor_name(&req.get_ref().actor);
        let mut s = self.s.lock();
        s.calls.push(format!("SuspendActor(async) {name}"));
        let mode = req
            .get_ref()
            .options
            .as_ref()
            .map(|o| o.mode())
            .unwrap_or_default();
        if mode != api::DurabilityMode::Asynchronous {
            return Err(Status::unimplemented("only async suspend is faked"));
        }
        if let Some(e) = s.pause_err.clone() {
            return Err(e);
        }
        if s.states.get(&name) != Some(&ActorState::Running) {
            return Err(Status::failed_precondition("not running"));
        }
        s.states.insert(name, ActorState::Suspended);
        Ok(self.answer(&s, api::SuspendActorResponse::default()))
    }

    async fn delete_actor(
        &self,
        req: Request<api::DeleteActorRequest>,
    ) -> Result<Response<api::Actor>, Status> {
        let name = actor_name(&req.get_ref().actor);
        let mut s = self.s.lock();
        s.calls.push(format!("DeleteActor {name}"));
        if s.delete_aborts > 0 {
            s.delete_aborts -= 1;
            return Err(Status::aborted("lease held"));
        }
        s.states.remove(&name);
        Ok(Response::new(api::Actor::default()))
    }

    async fn get_actor_template(
        &self,
        _req: Request<api::GetActorTemplateRequest>,
    ) -> Result<Response<api::ActorTemplate>, Status> {
        Err(Status::unimplemented("not faked"))
    }
}

/// A runtime over `fake` recording under its own user class, so parallel
/// tests read their own series.
fn runtime(fake: &Arc<Fake>, class: &str) -> Arc<Runtime> {
    Arc::new(build(fake, class))
}

fn build(fake: &Arc<Fake>, class: &str) -> Runtime {
    let cfg = Config {
        api: fake.clone(),
        http: reqwest::Client::new(),
        router_url: String::new(),
        hive: "bench".into(),
        dyn_config: Arc::new(Holder::new(Box::new(knobs::codec()))),
        tracer: Tracer::never("test"),
        actors_per_user: 1,
        total_actors: 1,
        spawn_concurrency: 1,
        actor_deadline: Duration::from_secs(1),
    };
    let mut r = Runtime::new(Arc::new(cfg), class, false);
    r.retry_backoff = Duration::from_millis(1);
    r.setup_retry_wait = Duration::ZERO;
    r
}

fn set_rate(r: &Runtime, rps: f64) {
    r.cfg
        .dyn_config
        .apply(format!(r#"{{"target_rps": {rps}}}"#).as_bytes())
        .unwrap();
}

/// Runs the first iteration of `n` users concurrently and returns their
/// Actors, sorted.
async fn set_up_users(r: &Arc<Runtime>, n: u64) -> Vec<String> {
    let tasks: Vec<_> = (0..n)
        .map(|vu| {
            let r = r.clone();
            tokio::spawn(async move { r.iterate_user(vu).await })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    let got = r.user_actors();
    assert_eq!(got.len(), n as usize, "set up {n} users, got {got:?}");
    got
}

fn scrape(prefix: &str) -> f64 {
    metrics::render()
        .lines()
        .find_map(|l| {
            l.strip_prefix(prefix)
                .map(|v| v.trim().parse::<f64>().unwrap())
        })
        .unwrap_or(0.0)
}

fn request_count(class: &str, name: &str, status: &str) -> f64 {
    scrape(&format!(
        r#"locust_requests_total{{method="grpc",name="{name}",status="{status}",user_class="{class}"}} "#
    ))
}

fn server_latency_sum(class: &str, name: &str) -> f64 {
    scrape(&format!(
        r#"locust_server_duration_milliseconds_sum{{method="grpc",name="{name}",user_class="{class}"}} "#
    ))
}

#[test]
fn registered() {
    let e = super::super::entries()
        .into_iter()
        .find(|e| e.name == "resumepaused")
        .expect("registered");
    assert_eq!(e.locust_file, "resumepaused.py");
    assert_eq!(e.user_class, super::super::USER_CLASS);
}

#[tokio::test]
async fn start_ensures_the_hive() {
    let fake = Arc::new(Fake::default());
    let r = runtime(&fake, "RPStart");
    r.start().await.unwrap();
    assert_eq!(fake.calls_to("CreateHive"), vec!["bench"]);
    assert!(
        fake.calls_to("CreateActor").is_empty(),
        "start created actors before a user ran"
    );
}

#[tokio::test]
async fn start_fails_when_the_hive_cannot_be_ensured() {
    let fake = Arc::new(Fake::default());
    fake.with(|s| s.hive_err = Some(Status::permission_denied("denied")));
    let r = runtime(&fake, "RPStartFail");
    assert!(r.start().await.is_err());
    // An existing Hive is fine.
    fake.with(|s| s.hive_err = Some(Status::already_exists("exists")));
    r.start().await.unwrap();
}

#[tokio::test]
async fn first_iteration_makes_the_users_actor_cold() {
    let fake = Arc::new(Fake::default());
    let class = "RPFirst";
    let r = runtime(&fake, class);
    let cold = request_count(class, "ResumeActorCold", "success");
    r.iterate_user(0).await;
    let actors = r.user_actors();
    assert_eq!(actors.len(), 1);
    let name = &actors[0];
    assert_eq!(
        fake.calls(),
        vec![
            format!("CreateActor {name}"),
            format!("ResumeActor {name}"),
            format!("SuspendActor(async) {name}")
        ]
    );
    assert_eq!(fake.state(name), Some(ActorState::Suspended));
    assert_eq!(r.tracked_actors(), actors);
    assert_eq!(
        request_count(class, "ResumeActorCold", "success") - cold,
        0.0,
        "setup is not a cold resume"
    );
}

/// Each cycle records hivekeep-api's own elapsed time beside the client
/// latency, so the gap between them is network and client time.
#[tokio::test]
async fn iterate_records_server_latency() {
    let fake = Arc::new(Fake::default());
    let class = "RPServerLatency";
    let r = runtime(&fake, class);
    r.iterate_user(0).await;
    fake.with(|s| s.server_elapsed_us = Some("1500".into()));
    let (resumes, pauses) = (
        server_latency_sum(class, "ResumeActorCold"),
        server_latency_sum(class, "SuspendActorAsync"),
    );
    r.iterate_user(0).await;
    assert_eq!(server_latency_sum(class, "ResumeActorCold") - resumes, 1.5);
    assert_eq!(server_latency_sum(class, "SuspendActorAsync") - pauses, 1.5);
}

/// A failed call stays out of the server histogram, which has no status.
#[tokio::test]
async fn iterate_leaves_failures_out_of_server_latency() {
    let fake = Arc::new(Fake::default());
    let class = "RPServerFail";
    let r = runtime(&fake, class);
    r.iterate_user(0).await;
    fake.with(|s| {
        s.server_elapsed_us = Some("1500".into());
        s.resume_err = Some(Status::aborted("lease held"));
    });
    let before = server_latency_sum(class, "ResumeActorCold");
    r.iterate_user(0).await;
    assert_eq!(server_latency_sum(class, "ResumeActorCold") - before, 0.0);
}

#[tokio::test]
async fn iterate_resumes_then_pauses_the_users_actor() {
    let fake = Arc::new(Fake::default());
    let class = "RPCycle";
    let r = runtime(&fake, class);
    r.iterate_user(0).await;
    let name = r.user_actors()[0].clone();
    fake.with(|s| s.calls.clear());
    let before = request_count(class, "ResumeActorCold", "success");
    for _ in 0..3 {
        r.iterate_user(0).await;
    }
    let one = vec![
        format!("ResumeActor {name}"),
        format!("SuspendActor(async) {name}"),
    ];
    assert_eq!(fake.calls(), [one.clone(), one.clone(), one].concat());
    assert_eq!(
        request_count(class, "ResumeActorCold", "success") - before,
        3.0
    );
}

#[tokio::test]
async fn each_user_cycles_its_own_actor() {
    let fake = Arc::new(Fake::default());
    let r = runtime(&fake, "RPEachUser");
    let tasks: Vec<_> = (0..3u64)
        .map(|vu| {
            let r = r.clone();
            tokio::spawn(async move {
                for _ in 0..4 {
                    r.iterate_user(vu).await;
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    let actors = r.user_actors();
    assert_eq!(actors.len(), 3);
    assert_eq!(
        fake.calls_to("CreateActor").len(),
        3,
        "one CreateActor per user"
    );
    // Each user warms its Actor once, then cycles it three times.
    let resumes = fake.calls_to("ResumeActor");
    for name in &actors {
        assert_eq!(resumes.iter().filter(|n| *n == name).count(), 4, "{name}");
    }
}

#[tokio::test]
async fn setup_is_not_paced() {
    let fake = Arc::new(Fake::default());
    let r = runtime(&fake, "RPSetupUnpaced");
    set_rate(&r, 1.0);
    let start = Instant::now();
    set_up_users(&r, 5).await;
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "setting up 5 users took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn setup_deletes_an_actor_that_does_not_warm_and_retries() {
    let fake = Arc::new(Fake::default());
    fake.with(|s| s.resume_failures = 1);
    let r = runtime(&fake, "RPSetupRetry");
    r.iterate_user(0).await;
    assert!(
        r.user_actors().is_empty(),
        "user has an actor after a failed warm-up"
    );
    let created = fake.calls_to("CreateActor");
    assert_eq!(created.len(), 1);
    assert_eq!(
        fake.calls_to("DeleteActor"),
        created,
        "the one actor is deleted"
    );
    assert!(r.tracked_actors().is_empty());

    r.iterate_user(0).await;
    let actors = r.user_actors();
    assert_eq!(actors.len(), 1);
    assert_ne!(actors[0], created[0], "want a new actor");
    assert_eq!(fake.state(&actors[0]), Some(ActorState::Suspended));
}

#[tokio::test]
async fn setup_keeps_track_of_an_actor_it_could_not_delete() {
    let fake = Arc::new(Fake::default());
    fake.with(|s| {
        s.resume_err = Some(Status::unavailable("down"));
        s.delete_aborts = 5;
    });
    let r = runtime(&fake, "RPSetupKeep");
    r.iterate_user(0).await;
    assert_eq!(
        r.tracked_actors(),
        fake.calls_to("CreateActor"),
        "tracked for shutdown to delete"
    );
}

#[tokio::test]
async fn setup_retries_a_failed_create() {
    let fake = Arc::new(Fake::default());
    fake.with(|s| s.create_err = Some(Status::unavailable("down")));
    let r = runtime(&fake, "RPCreateRetry");
    r.iterate_user(0).await;
    assert!(
        fake.calls_to("DeleteActor").is_empty(),
        "nothing to delete after a failed create"
    );
    assert!(r.tracked_actors().is_empty());
    fake.with(|s| s.create_err = None);
    r.iterate_user(0).await;
    assert_eq!(r.user_actors().len(), 1);
}

#[tokio::test]
async fn iterate_fails_a_resume_of_a_running_actor() {
    let fake = Arc::new(Fake::default());
    let class = "RPAlreadyRunning";
    let r = runtime(&fake, class);
    r.iterate_user(0).await;
    let name = r.user_actors()[0].clone();
    fake.with(|s| {
        s.states.insert(name.clone(), ActorState::Running);
        s.calls.clear();
    });
    let before = request_count(class, "ResumeActorCold", "failure");
    let recoveries = request_count(class, "SuspendActorAsyncRecovery", "success");
    r.iterate_user(0).await;
    assert_eq!(
        request_count(class, "ResumeActorCold", "failure") - before,
        1.0
    );
    assert_eq!(
        request_count(class, "SuspendActorAsyncRecovery", "success") - recoveries,
        1.0
    );
    // Recovery pauses the running Actor.
    assert_eq!(
        fake.calls(),
        vec![
            format!("ResumeActor {name}"),
            format!("SuspendActor(async) {name}")
        ]
    );
    assert_eq!(fake.state(&name), Some(ActorState::Suspended));
}

#[tokio::test]
async fn iterate_keeps_the_actor_after_a_failed_resume() {
    let fake = Arc::new(Fake::default());
    let r = runtime(&fake, "RPKeepActor");
    r.iterate_user(0).await;
    let name = r.user_actors()[0].clone();
    fake.with(|s| s.resume_err = Some(Status::resource_exhausted("no free workers available")));
    r.iterate_user(0).await;
    assert_eq!(r.user_actors(), vec![name.clone()]);
    assert_eq!(fake.state(&name), Some(ActorState::Suspended));
}

#[tokio::test]
async fn iterate_paces_resumes() {
    let fake = Arc::new(Fake::default());
    let r = runtime(&fake, "RPPaced");
    r.iterate_user(0).await;
    set_rate(&r, 200.0);
    let start = Instant::now();
    for _ in 0..21 {
        r.iterate_user(0).await;
    }
    // The first resume goes at once; the other 20 take 5 ms each.
    assert!(
        start.elapsed() >= Duration::from_millis(95),
        "21 resumes at 200/s took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn iterate_records_the_recovery_pause_of_a_paused_actor_as_a_success() {
    let fake = Arc::new(Fake::default());
    let class = "RPRecoverPaused";
    let r = runtime(&fake, class);
    r.iterate_user(0).await;
    let name = r.user_actors()[0].clone();
    fake.with(|s| {
        s.calls.clear();
        s.resume_err = Some(Status::aborted("lease held"));
    });
    let resume_failures = request_count(class, "ResumeActorCold", "failure");
    let pause_failures = request_count(class, "SuspendActorAsync", "failure");
    let rec_ok = request_count(class, "SuspendActorAsyncRecovery", "success");
    let rec_fail = request_count(class, "SuspendActorAsyncRecovery", "failure");
    // The resume fails with the Actor still paused, so the recovery pause
    // finds it paused: a success, as the Actor is cold.
    r.iterate_user(0).await;
    assert_eq!(fake.calls_to("SuspendActor(async)"), vec![name]);
    assert_eq!(
        request_count(class, "ResumeActorCold", "failure") - resume_failures,
        1.0
    );
    assert_eq!(
        request_count(class, "SuspendActorAsync", "failure") - pause_failures,
        0.0
    );
    assert_eq!(
        request_count(class, "SuspendActorAsyncRecovery", "success") - rec_ok,
        1.0
    );
    assert_eq!(
        request_count(class, "SuspendActorAsyncRecovery", "failure") - rec_fail,
        0.0
    );
}

#[tokio::test]
async fn iterate_records_a_failed_recovery_pause() {
    let fake = Arc::new(Fake::default());
    let class = "RPRecoverFail";
    let r = runtime(&fake, class);
    r.iterate_user(0).await;
    fake.with(|s| s.pause_err = Some(Status::unavailable("down")));
    let pause_failures = request_count(class, "SuspendActorAsync", "failure");
    let rec_fail = request_count(class, "SuspendActorAsyncRecovery", "failure");
    // The resume succeeds, the measured pause fails, and so does the
    // recovery pause of the running Actor.
    r.iterate_user(0).await;
    assert_eq!(
        request_count(class, "SuspendActorAsync", "failure") - pause_failures,
        1.0
    );
    assert_eq!(
        request_count(class, "SuspendActorAsyncRecovery", "failure") - rec_fail,
        1.0
    );
}

#[tokio::test]
async fn shutdown_deletes_every_actor() {
    let fake = Arc::new(Fake::default());
    let r = runtime(&fake, "RPShutdownAll");
    let want = set_up_users(&r, 3).await;
    r.shutdown_within(None).await;
    let mut got = fake.calls_to("DeleteActor");
    got.sort();
    assert_eq!(got, want);
}

#[tokio::test]
async fn shutdown_retries_an_aborted_delete() {
    let fake = Arc::new(Fake::default());
    let r = runtime(&fake, "RPShutdownRetry");
    let name = set_up_users(&r, 1).await.remove(0);
    fake.with(|s| s.delete_aborts = 2);
    r.shutdown_within(None).await;
    assert_eq!(fake.calls_to("DeleteActor").len(), 3);
    assert_eq!(fake.state(&name), None, "actor still exists after shutdown");
}

#[tokio::test]
async fn shutdown_waits_for_a_running_cycle() {
    let fake = Arc::new(Fake::default());
    let r = runtime(&fake, "RPShutdownWaitCycle");
    r.iterate_user(0).await;
    let name = r.user_actors()[0].clone();
    let (mut entered, open) = fake.install_gate();
    let cycle = {
        let (r, name) = (r.clone(), name.clone());
        tokio::spawn(async move { r.cycle(&name).await })
    };
    entered.recv().await.unwrap();
    let shutdown = {
        let r = r.clone();
        tokio::spawn(async move { r.shutdown_within(None).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !shutdown.is_finished(),
        "shutdown returned while a cycle was running"
    );
    assert!(
        fake.calls_to("DeleteActor").is_empty(),
        "deleted while a cycle was running"
    );

    open.send(true).unwrap();
    cycle.await.unwrap();
    shutdown.await.unwrap();
    assert_eq!(fake.calls_to("DeleteActor"), vec![name]);
    r.iterate_user(0).await;
    assert_eq!(
        fake.calls_to("ResumeActor").len(),
        2,
        "the warm-up and the one cycle only"
    );
}

#[tokio::test]
async fn shutdown_waits_for_a_running_setup_and_deletes_its_actor() {
    let fake = Arc::new(Fake::default());
    let r = runtime(&fake, "RPShutdownWaitSetup");
    let (mut entered, open) = fake.install_gate();
    let setup = {
        let r = r.clone();
        tokio::spawn(async move { r.iterate_user(0).await })
    };
    entered.recv().await.unwrap();
    let shutdown = {
        let r = r.clone();
        tokio::spawn(async move { r.shutdown_within(None).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !shutdown.is_finished(),
        "shutdown returned while a setup was running"
    );

    open.send(true).unwrap();
    setup.await.unwrap();
    shutdown.await.unwrap();
    let created = fake.calls_to("CreateActor");
    assert_eq!(created.len(), 1);
    assert_eq!(
        fake.calls_to("DeleteActor"),
        created,
        "the setup's actor is deleted"
    );

    // No setup starts once shutdown has begun.
    r.iterate_user(1).await;
    assert_eq!(fake.calls_to("CreateActor").len(), 1);
}

#[tokio::test]
async fn shutdown_records_only_to_prometheus() {
    let fake = Arc::new(Fake::default());
    let class = "RPShutdownProm";
    let r = runtime(&fake, class);
    let names = set_up_users(&r, 3).await;
    let to_master = Arc::new(AtomicI32::new(0));
    let (a, b) = (to_master.clone(), to_master.clone());
    r.set_recorders(
        Arc::new(move |_, _, _, _, _| {
            a.fetch_add(1, Ordering::SeqCst);
        }),
        Arc::new(move |_, _, _, _, _| {
            b.fetch_add(1, Ordering::SeqCst);
        }),
    );
    r.cycle(&names[0]).await;
    assert_eq!(
        to_master.load(Ordering::SeqCst),
        2,
        "a cycle sends the resume and the pause"
    );

    // A cycle finishing once shutdown has begun, and the deletes, record to
    // Prometheus only.
    let (mut entered, open) = fake.install_gate();
    let cycle = {
        let (r, name) = (r.clone(), names[0].clone());
        tokio::spawn(async move { r.cycle(&name).await })
    };
    entered.recv().await.unwrap();
    let resumes = request_count(class, "ResumeActorCold", "success");
    let deletes = request_count(class, "DeleteActor", "success");
    to_master.store(0, Ordering::SeqCst);
    let shutdown = {
        let r = r.clone();
        tokio::spawn(async move { r.shutdown_within(None).await })
    };
    while !r.closing() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    open.send(true).unwrap();
    cycle.await.unwrap();
    shutdown.await.unwrap();
    assert_eq!(
        to_master.load(Ordering::SeqCst),
        0,
        "shutdown sent samples to the master"
    );
    assert_eq!(
        request_count(class, "ResumeActorCold", "success") - resumes,
        1.0,
        "the in-flight resume"
    );
    assert_eq!(
        request_count(class, "DeleteActor", "success") - deletes,
        3.0
    );
}

#[tokio::test]
async fn shutdown_cancels_a_cycle_that_outlasts_its_share() {
    let fake = Arc::new(Fake::default());
    let r = runtime(&fake, "RPShutdownCancel");
    r.iterate_user(0).await;
    let name = r.user_actors()[0].clone();
    // The gate never opens: the resume hangs.
    let (mut entered, _open) = fake.install_gate();
    let cycle = {
        let (r, name) = (r.clone(), name.clone());
        tokio::spawn(async move { r.cycle(&name).await })
    };
    entered.recv().await.unwrap();
    let start = Instant::now();
    r.shutdown_within(Some(Duration::from_millis(200))).await;
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "shutdown spent its whole budget"
    );
    tokio::time::timeout(Duration::from_secs(1), cycle)
        .await
        .expect("the hung cycle was not canceled")
        .unwrap();
    assert_eq!(fake.calls_to("DeleteActor"), vec![name.clone()]);
    assert_eq!(fake.state(&name), None, "actor still exists after shutdown");
}

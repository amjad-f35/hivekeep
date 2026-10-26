//! The per-worker state of ResumePausedUser: one cold Actor per user, the
//! paced resume-then-pause cycle, and a shutdown that waits for running
//! cycles before deleting every Actor.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use hivekeep_proto::api::v1 as api;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataMap;
use tonic::{Code, Request, Response, Status};

use super::ctx::Ctx;
use super::knobs::Knobs;
use super::pacer::Pacer;
use crate::loadgen::metrics;
use crate::loadgen::telemetry::{
    elapsed_from_metadata, log_sampled_trace, SOURCE_CLIENT, SOURCE_SERVER,
};
use crate::loadgen::trace::inject_metadata;
use crate::loadgen::userclass::{Config, UserTask, VuId};
use crate::wire::SERVER_ELAPSED_HEADER;

/// The template every Actor is created from.
pub const TEMPLATE_NAME: &str = "glutton";
/// The Hive holding [`TEMPLATE_NAME`].
pub const TEMPLATE_HIVE: &str = "benchmark-workloads";

/// How many Actors shutdown deletes at once.
const SHUTDOWN_PARALLELISM: usize = 16;

/// How many attempts a delete makes while another operation holds the
/// Actor's lease.
const DELETE_ATTEMPTS: usize = 5;

/// The time shutdown takes for itself when run through [`UserTask`]; the
/// binary bounds the whole shutdown a little above it.
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(50);

/// Reports a successful sample to the master and to Prometheus.
pub type RecordSuccess = Arc<dyn Fn(&str, &str, &str, Duration, i64) + Send + Sync>;
/// Reports a failed sample to the master and to Prometheus.
pub type RecordFailure = Arc<dyn Fn(&str, &str, &str, Duration, &str) + Send + Sync>;

/// A ResumeActor that found its Actor running: no cold resume happened, so
/// the sample must not count as one.
fn already_running() -> Status {
    Status::unknown("actor was already running: not a cold resume")
}

#[derive(Default)]
struct Cycles {
    /// Set once shutdown begins; no cycle or setup starts afterwards.
    closing: bool,
    /// Cycles and setups in progress.
    running: usize,
}

/// Marks one running cycle or setup; dropping it lets shutdown proceed.
struct CycleGuard<'a>(&'a Runtime);

impl Drop for CycleGuard<'_> {
    fn drop(&mut self) {
        let mut c = self.0.cycles.lock();
        c.running -= 1;
        if c.running == 0 {
            self.0.drained.notify_waiters();
        }
    }
}

/// The ResumePausedUser runtime.
pub struct Runtime {
    cfg: Arc<Config>,
    user_class: String,
    /// Set when the Hive still has to be ensured before the first user runs.
    needs_start: bool,
    started: tokio::sync::OnceCell<()>,
    /// The Actor each user cycles. A user the master stopped keeps its
    /// entry, and its Actor stays paused until shutdown.
    users: Mutex<HashMap<VuId, String>>,
    /// Every Actor that may exist, for shutdown.
    actors: Mutex<BTreeSet<String>>,
    pacer: Pacer,
    /// The gap between retries of a delete.
    pub(crate) retry_backoff: Duration,
    /// How long a user waits after its Actor failed to warm.
    pub(crate) setup_retry_wait: Duration,
    cycles: Mutex<Cycles>,
    drained: Notify,
    /// Carries every cycle's and setup's calls; shutdown cancels it when
    /// they outlast their share of its budget.
    cycle_cancel: CancellationToken,
    /// Set when shutdown begins: from then on samples go to Prometheus
    /// only, as the run reporting them to the master is over.
    stopped: AtomicBool,
    recorders: RwLock<(RecordSuccess, RecordFailure)>,
}

impl Runtime {
    /// A runtime over `cfg` recording under `user_class`. With
    /// `needs_start`, the first iteration ensures the Hive and ends the
    /// process if it cannot.
    pub fn new(cfg: Arc<Config>, user_class: &str, needs_start: bool) -> Self {
        Runtime {
            cfg,
            user_class: user_class.to_string(),
            needs_start,
            started: tokio::sync::OnceCell::new(),
            users: Mutex::new(HashMap::new()),
            actors: Mutex::new(BTreeSet::new()),
            pacer: Pacer::default(),
            retry_backoff: Duration::from_secs(1),
            setup_retry_wait: Duration::from_secs(1),
            cycles: Mutex::new(Cycles::default()),
            drained: Notify::new(),
            cycle_cancel: CancellationToken::new(),
            stopped: AtomicBool::new(false),
            recorders: RwLock::new((
                Arc::new(metrics::record_success),
                Arc::new(|m: &str, n: &str, c: &str, l: Duration, e: &str| {
                    metrics::record_failure(m, n, c, l, e)
                }),
            )),
        }
    }

    /// Replaces the master-and-Prometheus recorders (tests count them).
    pub fn set_recorders(&self, success: RecordSuccess, failure: RecordFailure) {
        *self.recorders.write() = (success, failure);
    }

    fn cycle_ctx(&self) -> Ctx {
        Ctx::new(self.cycle_cancel.clone())
    }

    /// Ensures the Hive every Actor is created in.
    pub async fn start(&self) -> Result<(), String> {
        self.ensure_hive(&Ctx::background())
            .await
            .map_err(|e| format!("while ensuring hive {}: {}", self.cfg.hive, e.message()))
    }

    /// The Actor of every user, sorted.
    pub fn user_actors(&self) -> Vec<String> {
        let mut v: Vec<String> = self.users.lock().values().cloned().collect();
        v.sort();
        v
    }

    /// Every Actor shutdown would delete, sorted.
    pub fn tracked_actors(&self) -> Vec<String> {
        self.actors.lock().iter().cloned().collect()
    }

    /// Whether shutdown has begun.
    pub fn closing(&self) -> bool {
        self.cycles.lock().closing
    }

    /// One user iteration: the first creates the user's Actor and makes it
    /// cold; every later one runs a measured cycle on it.
    pub async fn iterate_user(&self, vu: VuId) {
        if self.needs_start {
            if let Err(e) = self.started.get_or_try_init(|| self.start()).await {
                tracing::error!(err = %e, "fatal: resumepaused");
                std::process::exit(1);
            }
        }
        let existing = self.users.lock().get(&vu).cloned();
        if let Some(name) = existing {
            self.cycle(&name).await;
            return;
        }
        match self.set_up().await {
            Some(name) => {
                self.users.lock().insert(vu, name);
            }
            None => tokio::time::sleep(self.setup_retry_wait).await,
        }
    }

    /// Creates one Actor and makes it cold. Not paced, so its calls land in
    /// the step that added the user, under their own names. An Actor that
    /// fails to warm is deleted and the user tries again next iteration.
    async fn set_up(&self) -> Option<String> {
        let _guard = self.start_cycle()?;
        let ctx = self.cycle_ctx();
        let name = format!("rp-{}", uuid::Uuid::new_v4());
        self.create(&ctx, &name).await.ok()?;
        self.actors.lock().insert(name.clone());
        if let Err(e) = self.warm(&ctx, &name).await {
            tracing::warn!(actor = %name, err = %e.message(), "resumepaused: actor did not warm; the user will retry");
            if self.delete(&ctx, &name).await.is_ok() {
                self.actors.lock().remove(&name);
            }
            return None;
        }
        Some(name)
    }

    /// Registers a cycle or setup with shutdown, or `None` once shutdown has
    /// begun.
    fn start_cycle(&self) -> Option<CycleGuard<'_>> {
        let mut c = self.cycles.lock();
        if c.closing {
            return None;
        }
        c.running += 1;
        Some(CycleGuard(self))
    }

    /// Makes a new Actor cold: a first resume, then a pause.
    async fn warm(&self, ctx: &Ctx, name: &str) -> Result<(), Status> {
        self.resume(ctx, "ResumeActorWarmup", name).await?;
        self.pause(ctx, "Warmup", name).await
    }

    /// One paced cold resume of the user's Actor, then the pause that makes
    /// it cold again. Only the resume is paced, so the target rate is cold
    /// resumes per second.
    pub async fn cycle(&self, name: &str) {
        let ctx = self.cycle_ctx();
        self.pace(&ctx).await;
        let Some(_guard) = self.start_cycle() else {
            return;
        };
        let mut res = self.resume(&ctx, "ResumeActorCold", name).await;
        if res.is_ok() {
            res = self.pause(&ctx, "", name).await;
        }
        if res.is_err() {
            // The Actor may have been left running; pausing it again makes
            // it cold for the next cycle.
            self.recover(&ctx, name).await;
        }
    }

    /// Waits for the next resume slot at the current target rate.
    async fn pace(&self, ctx: &Ctx) {
        let k = self.cfg.dyn_config.get::<Knobs>();
        self.pacer
            .set_rate((k.target_rps > 0.0).then_some(k.target_rps));
        let _ = ctx.run(self.pacer.wait()).await;
    }

    /// Suspends an Actor asynchronously after a failed cycle, so it is cold
    /// for the next one. Recorded as SuspendActorAsyncRecovery, apart from
    /// the measured suspends. An Actor already suspended answers
    /// FAILED_PRECONDITION; it is cold, which is all recovery wants.
    async fn recover(&self, ctx: &Ctx, name: &str) {
        let req = self.suspend_req(name);
        let api = self.cfg.api.clone();
        let _ = self
            .call(
                ctx,
                "SuspendActorAsyncRecovery",
                move |md| async move {
                    match api.suspend_actor(with_md(req, md)).await {
                        Err(e) if e.code() == Code::FailedPrecondition => {
                            Ok(Response::new(Default::default()))
                        }
                        other => other,
                    }
                },
                |_| Ok(()),
            )
            .await;
    }

    fn reference(&self, name: &str) -> Option<api::ObjectRef> {
        Some(api::ObjectRef {
            hive: self.cfg.hive.clone(),
            name: name.to_string(),
        })
    }

    fn suspend_req(&self, name: &str) -> api::SuspendActorRequest {
        api::SuspendActorRequest {
            actor: self.reference(name),
            options: Some(api::SuspendOptions {
                mode: api::DurabilityMode::Asynchronous as i32,
            }),
        }
    }

    /// Creates one Actor from the template.
    async fn create(&self, ctx: &Ctx, name: &str) -> Result<(), Status> {
        let req = api::CreateActorRequest {
            actor: Some(api::Actor {
                metadata: Some(api::ResourceMetadata {
                    hive: self.cfg.hive.clone(),
                    name: name.to_string(),
                    ..Default::default()
                }),
                actor_template: Some(api::ObjectRef {
                    hive: TEMPLATE_HIVE.into(),
                    name: TEMPLATE_NAME.into(),
                }),
                ..Default::default()
            }),
        };
        let api = self.cfg.api.clone();
        self.call(
            ctx,
            "CreateActor",
            move |md| async move { api.create_actor(with_md(req, md)).await },
            |_| Ok(()),
        )
        .await
    }

    /// Calls ResumeActor; an answer that reports no resume fails the sample.
    async fn resume(&self, ctx: &Ctx, metric: &str, name: &str) -> Result<(), Status> {
        let req = api::ResumeActorRequest {
            actor: self.reference(name),
        };
        let api = self.cfg.api.clone();
        self.call(
            ctx,
            metric,
            move |md| async move { api.resume_actor(with_md(req, md)).await },
            |resp: api::ResumeActorResponse| {
                if resp.resumed {
                    Ok(())
                } else {
                    Err(already_running())
                }
            },
        )
        .await
    }

    /// Suspends an Actor asynchronously, keeping its snapshot on the node.
    /// `suffix` keeps warm-up calls apart from the measured ones.
    async fn pause(&self, ctx: &Ctx, suffix: &str, name: &str) -> Result<(), Status> {
        let req = self.suspend_req(name);
        let api = self.cfg.api.clone();
        self.call(
            ctx,
            &format!("SuspendActorAsync{suffix}"),
            move |md| async move { api.suspend_actor(with_md(req, md)).await },
            |_| Ok(()),
        )
        .await
    }

    /// Deletes an Actor, retrying while another operation holds its lease.
    async fn delete(&self, ctx: &Ctx, name: &str) -> Result<(), Status> {
        let mut last = Ok(());
        for attempt in 0..DELETE_ATTEMPTS {
            if attempt > 0 {
                if let Err(cut) = ctx.sleep(self.retry_backoff).await {
                    return Err(cut.status());
                }
            }
            let req = api::DeleteActorRequest {
                actor: self.reference(name),
                options: None,
            };
            let api = self.cfg.api.clone();
            last = self
                .call(
                    ctx,
                    "DeleteActor",
                    move |md| async move { api.delete_actor(with_md(req, md)).await },
                    |_| Ok(()),
                )
                .await;
            match &last {
                Err(e) if matches!(e.code(), Code::Aborted | Code::Unavailable) => continue,
                _ => return last,
            }
        }
        last
    }

    async fn ensure_hive(&self, ctx: &Ctx) -> Result<(), Status> {
        let req = api::CreateHiveRequest {
            hive: Some(api::Hive {
                metadata: Some(api::ResourceMetadata {
                    name: self.cfg.hive.clone(),
                    ..Default::default()
                }),
            }),
        };
        let api = self.cfg.api.clone();
        self.call(
            ctx,
            "CreateHive",
            move |md| async move {
                match api.create_hive(with_md(req, md)).await {
                    Err(e) if e.code() == Code::AlreadyExists => {
                        Ok(Response::new(Default::default()))
                    }
                    other => other,
                }
            },
            |_| Ok(()),
        )
        .await
    }

    /// Times one unary call and reports it. The client latency goes to the
    /// master and Prometheus; for a successful call the server's own
    /// elapsed time from the response trailer goes to Prometheus as a
    /// second histogram, so the gap between them is network and client
    /// time. Failures stay out of it: that histogram has no status, and a
    /// call cut off by its deadline brings back no trailer. `check` turns
    /// a successful answer into a failure when it reports no real work.
    async fn call<T, Fut>(
        &self,
        ctx: &Ctx,
        name: &str,
        rpc: impl FnOnce(MetadataMap) -> Fut,
        check: impl FnOnce(T) -> Result<(), Status>,
    ) -> Result<(), Status>
    where
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        let span = self.cfg.tracer.start(name);
        let mut md = MetadataMap::new();
        inject_metadata(span.context(), &mut md);
        let start = Instant::now();
        let res = ctx
            .run(rpc(md))
            .await
            .unwrap_or_else(|cut| Err(cut.status()));
        let latency = start.elapsed();
        let (res, trailer) = match res {
            Ok(resp) => {
                let md = resp.metadata().clone();
                (check(resp.into_inner()), Some(md))
            }
            Err(e) => (Err(e), None),
        };
        let err = res.as_ref().err().map(|e| e.message().to_string());
        log_sampled_trace(span.context(), name, latency, SOURCE_CLIENT, err.as_deref());
        self.record(name, latency, err.as_deref());
        if let (Ok(()), Some(md)) = (&res, trailer) {
            let (server, source) =
                elapsed_from_metadata(&md, SERVER_ELAPSED_HEADER, Duration::ZERO);
            if source == SOURCE_SERVER {
                metrics::record_server_latency("grpc", name, &self.user_class, server);
            }
        }
        span.end();
        res
    }

    /// Reports one sample: to the master and Prometheus while the run is
    /// on, to Prometheus only once shutdown has begun.
    fn record(&self, name: &str, latency: Duration, err: Option<&str>) {
        let stopped = self.stopped.load(Ordering::SeqCst);
        let class = self.user_class.as_str();
        match (err, stopped) {
            (Some(_), true) => metrics::record_prom_failure("grpc", name, class, latency),
            (Some(e), false) => (self.recorders.read().1)("grpc", name, class, latency, e),
            (None, true) => metrics::record_prom_success("grpc", name, class, latency),
            (None, false) => (self.recorders.read().0)("grpc", name, class, latency, 0),
        }
    }

    async fn wait_drained(&self) {
        loop {
            let notified = self.drained.notified();
            if self.cycles.lock().running == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Waits for the running cycles and setups, then deletes every Actor.
    /// The master stops scheduling iterations before shutdown but does not
    /// wait for those in flight, and a delete racing one loses the Actor
    /// lease. The running ones get half of `budget`; any still running then
    /// are canceled, so a hung call cannot spend the time the deletes need.
    /// DeleteActor removes an Actor in any state, so a canceled cycle
    /// still leaves nothing behind.
    pub async fn shutdown_within(&self, budget: Option<Duration>) {
        self.stopped.store(true, Ordering::SeqCst);
        self.cycles.lock().closing = true;
        let ctx = match budget {
            Some(b) => Ctx::background().with_timeout(b),
            None => Ctx::background(),
        };
        let drain = match budget {
            Some(b) => ctx.with_timeout(b / 2),
            None => ctx.clone(),
        };
        if drain.run(self.wait_drained()).await.is_err() {
            self.cycle_cancel.cancel();
            let _ = ctx.run(self.wait_drained()).await;
        }
        self.cycle_cancel.cancel();

        let names: Vec<String> = std::mem::take(&mut *self.actors.lock())
            .into_iter()
            .collect();
        let left = futures::stream::iter(names)
            .map(|name| {
                let ctx = ctx.clone();
                async move { self.delete(&ctx, &name).await.is_err() }
            })
            .buffer_unordered(SHUTDOWN_PARALLELISM)
            .filter(|failed| futures::future::ready(*failed))
            .count()
            .await;
        if left > 0 {
            tracing::warn!(count = left, hive = %self.cfg.hive, "resumepaused: actors left behind after shutdown");
        }
    }
}

/// `msg` as a request carrying `md`.
fn with_md<T>(msg: T, md: MetadataMap) -> Request<T> {
    let mut req = Request::new(msg);
    *req.metadata_mut() = md;
    req
}

#[async_trait]
impl UserTask for Runtime {
    async fn iterate(&self, vu: VuId) {
        self.iterate_user(vu).await;
    }

    async fn shutdown(&self) {
        self.shutdown_within(Some(SHUTDOWN_BUDGET)).await;
    }
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;

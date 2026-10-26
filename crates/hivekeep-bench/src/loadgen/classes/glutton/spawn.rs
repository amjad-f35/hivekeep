//! SpawnUser: creates one batch of Actors per worker process and measures
//! how long each takes from CreateActor to its first answered ping, how
//! long each decile of the batch takes to become ready, and how long the
//! whole batch takes.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hivekeep_proto::api::v1 as api;
use parking_lot::Mutex;
use prost::Message;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Status};

use super::calls::{self, post, traced_call, traced_request, Recorder};
use super::knobs::SpawnKnobs;
use super::wait_group::{Done, WaitGroup};
use super::{GLUTTON_TEMPLATE, SPAWN_USER_CLASS, TEMPLATE_HIVE};
use crate::glutton::PING_ROUTE;
use crate::loadgen::lifecycle::retry_on_conflict;
use crate::loadgen::metrics;
use crate::loadgen::telemetry::{log_sampled_trace, SOURCE_CLIENT};
use crate::loadgen::userclass::{Config, UserTask, VuId};
use crate::pb::glutton::v1 as pb;

const INITIAL_RETRY_BACKOFF: Duration = Duration::from_millis(50);
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(1);
const RETRY_BACKOFF_JITTER: Duration = Duration::from_millis(5);
/// How long shutdown waits for the batch before deleting what it created.
const SHUTDOWN_BATCH_WAIT: Duration = Duration::from_secs(2);
/// DeleteActor calls in flight at once during shutdown.
const MAX_DELETE_CONCURRENCY: usize = 32;

const DECILES: [usize; 10] = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100];

/// The deciles (10..100) whose readiness threshold is the `rank`-th ready
/// Actor of a batch of `total`.
pub fn fired_deciles(rank: usize, total: usize) -> Vec<usize> {
    if total == 0 || rank == 0 {
        return Vec::new();
    }
    DECILES
        .iter()
        .copied()
        .filter(|k| (k * total).div_ceil(100) == rank)
        .collect()
}

/// Whether retrying `err` cannot help. Codes not listed, ResourceExhausted
/// included, are retried until the Actor's deadline; ABORTED is terminal
/// only for a crashed Actor.
pub fn is_spawn_terminal_error(err: &Status) -> bool {
    match err.code() {
        Code::NotFound
        | Code::DataLoss
        | Code::AlreadyExists
        | Code::FailedPrecondition
        | Code::InvalidArgument
        | Code::PermissionDenied
        | Code::Unauthenticated
        | Code::Unimplemented => true,
        Code::Aborted => is_spawn_crashed(err),
        _ => false,
    }
}

fn is_spawn_crashed(err: &Status) -> bool {
    err.code() == Code::Aborted && err.message().to_lowercase().contains("crashed")
}

fn jittered(delay: Duration) -> Duration {
    delay + RETRY_BACKOFF_JITTER.mul_f64(rand::random::<f64>())
}

/// Books outcomes unless the run was cancelled, so shutdown clean-up does
/// not show up as batch failures.
struct SpawnRecorder<'a> {
    class: &'a str,
    cancel: &'a CancellationToken,
}

impl Recorder for SpawnRecorder<'_> {
    fn success(&self, method: &str, name: &str, latency: Duration, bytes: i64) {
        if !self.cancel.is_cancelled() {
            metrics::record_success(method, name, self.class, latency, bytes);
        }
    }
    fn failure(&self, method: &str, name: &str, latency: Duration, err: &str) {
        if !self.cancel.is_cancelled() {
            metrics::record_failure(method, name, self.class, latency, err);
        }
    }
}

/// The SpawnUser task.
pub struct SpawnRuntime {
    pub(crate) cfg: Arc<Config>,
    /// The class the batch's stats are booked under.
    pub(crate) user_class: String,
    run_once: OnceCell<()>,
    pub(crate) cancel: CancellationToken,
    workers: WaitGroup,
    pub(crate) ready_count: AtomicUsize,
    /// Nanoseconds from the batch start to its last ready Actor.
    pub(crate) last_ready_elapsed: AtomicI64,
    pub(crate) actor_names: Mutex<Vec<String>>,
    counted_user: AtomicBool,
}

impl SpawnRuntime {
    /// A runtime over `cfg`.
    pub fn new(cfg: Arc<Config>) -> Self {
        SpawnRuntime {
            cfg,
            user_class: SPAWN_USER_CLASS.into(),
            run_once: OnceCell::new(),
            cancel: CancellationToken::new(),
            workers: WaitGroup::default(),
            ready_count: AtomicUsize::new(0),
            last_ready_elapsed: AtomicI64::new(0),
            actor_names: Mutex::new(Vec::new()),
            counted_user: AtomicBool::new(false),
        }
    }

    fn rec(&self) -> SpawnRecorder<'_> {
        SpawnRecorder {
            class: &self.user_class,
            cancel: &self.cancel,
        }
    }

    /// The names of every Actor the batch started.
    pub fn actor_names(&self) -> Vec<String> {
        self.actor_names.lock().clone()
    }

    /// Runs the batch once (the first user to arrive runs it; the others
    /// wait for it), then parks the user for good: a returning iteration
    /// would only be called again.
    pub async fn iterate(&self) {
        self.run_once.get_or_init(|| self.run_batch()).await;
        std::future::pending::<()>().await;
    }

    /// Creates the batch and books its readiness timings.
    pub async fn run_batch(&self) {
        metrics::update_users(&self.user_class, 1.0);
        self.counted_user.store(true, Ordering::SeqCst);
        if let Err(e) = calls::ensure_hive(&self.cfg, &self.rec()).await {
            tracing::error!(err = %e.message(), "failed to ensure the hive");
            self.rec().failure(
                "summary",
                "TimeToAllReady",
                Duration::ZERO,
                &calls::status_text(&e),
            );
            return;
        }
        let batch_start = Instant::now();
        // Values from the web form override the flags.
        let knobs = self.cfg.dyn_config.get::<SpawnKnobs>();
        let total = if knobs.total_actors > 0 {
            knobs.total_actors as usize
        } else {
            self.cfg.total_actors
        };
        let concurrency = if knobs.spawn_concurrency > 0 {
            knobs.spawn_concurrency as usize
        } else {
            self.cfg.spawn_concurrency
        };
        let deadline = if knobs.actor_deadline.as_nanos() > 0 {
            knobs.actor_deadline.duration()
        } else {
            self.cfg.actor_deadline
        };
        let concurrency = concurrency.min(total).max(1);
        let run_id = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
        let next = AtomicUsize::new(0);

        let worker = || async {
            let _done = Done(&self.workers);
            loop {
                let idx = next.fetch_add(1, Ordering::SeqCst) + 1;
                if idx > total || self.cancel.is_cancelled() {
                    return;
                }
                let name = format!("spawn-{run_id}-{idx}");
                self.run_one_actor(batch_start, &name, total, deadline)
                    .await;
            }
        };
        let mut all = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            self.workers.add();
            all.push(worker());
        }
        futures::future::join_all(all).await;

        if self.cancel.is_cancelled() {
            return;
        }
        if self.ready_count.load(Ordering::SeqCst) > 0 {
            let elapsed =
                Duration::from_nanos(self.last_ready_elapsed.load(Ordering::SeqCst) as u64);
            self.rec().success("summary", "TimeToAllReady", elapsed, 0);
        } else {
            self.rec().failure(
                "summary",
                "TimeToAllReady",
                batch_start.elapsed(),
                "no actors became ready",
            );
        }
    }

    async fn run_one_actor(
        &self,
        batch_start: Instant,
        name: &str,
        total: usize,
        deadline: Duration,
    ) {
        self.actor_names.lock().push(name.to_string());
        let actor_start = Instant::now();
        let work = async {
            self.create_with_retry(name).await?;
            self.resume_with_retry(name).await?;
            self.ping_until_ready(name).await
        };
        let res = match tokio::time::timeout(deadline, work).await {
            Ok(r) => r,
            Err(_) => Err("context deadline exceeded".to_string()),
        };
        if let Err(e) = res {
            self.rec()
                .failure("summary", "ActorTimeToReady", actor_start.elapsed(), &e);
            return;
        }
        let ready_elapsed = batch_start.elapsed();
        let actor_elapsed = actor_start.elapsed();
        // The maximum, not the last write: workers finish out of order.
        self.last_ready_elapsed
            .fetch_max(ready_elapsed.as_nanos() as i64, Ordering::SeqCst);
        let rank = self.ready_count.fetch_add(1, Ordering::SeqCst) + 1;
        for k in fired_deciles(rank, total) {
            self.rec()
                .success("summary", &format!("TimeToReady_{k}pct"), ready_elapsed, 0);
        }
        self.rec()
            .success("summary", "ActorTimeToReady", actor_elapsed, 0);
    }

    /// Sleeps a backoff, or fails at once if the run is cancelled.
    async fn backoff(&self, delay: Duration) -> Result<(), String> {
        tokio::select! {
            _ = self.cancel.cancelled() => Err("context canceled".into()),
            _ = tokio::time::sleep(jittered(delay)) => Ok(()),
        }
    }

    async fn create_with_retry(&self, name: &str) -> Result<(), String> {
        let cfg = &self.cfg;
        let mut delay = INITIAL_RETRY_BACKOFF;
        for attempt in 0.. {
            if self.cancel.is_cancelled() {
                return Err("context canceled".into());
            }
            let req = calls::create_request(cfg, name, TEMPLATE_HIVE, GLUTTON_TEMPLATE);
            let res = traced_call(cfg, &self.rec(), "CreateActor", |span| {
                cfg.api.create_actor(traced_request(req, &span))
            })
            .await;
            let err = match res {
                Ok(_) => return Ok(()),
                Err(e) => e,
            };
            // Names carry a per-run prefix, so AlreadyExists on a retry means
            // an earlier attempt landed before its answer was lost.
            if attempt > 0 && err.code() == Code::AlreadyExists {
                return Ok(());
            }
            if is_spawn_terminal_error(&err) {
                return Err(calls::status_text(&err));
            }
            self.backoff(delay).await?;
            delay = (delay * 2).min(MAX_RETRY_BACKOFF);
        }
        unreachable!("the retry loop only ends by returning")
    }

    async fn resume_with_retry(&self, name: &str) -> Result<(), String> {
        let cfg = &self.cfg;
        let actor = calls::actor_ref(cfg, name);
        let mut delay = INITIAL_RETRY_BACKOFF;
        loop {
            if self.cancel.is_cancelled() {
                return Err("context canceled".into());
            }
            let res = traced_call(cfg, &self.rec(), "ResumeActor", |span| {
                let actor = actor.clone();
                retry_on_conflict(move || {
                    cfg.api.resume_actor(traced_request(
                        api::ResumeActorRequest {
                            actor: Some(actor.clone()),
                        },
                        &span,
                    ))
                })
            })
            .await;
            let err = match res {
                Ok(_) => return Ok(()),
                Err(e) => e,
            };
            if is_spawn_crashed(&err) {
                self.rec().failure(
                    "actor",
                    "CrashCount",
                    Duration::ZERO,
                    "actor entered ACTOR_STATE_CRASHED",
                );
                return Err(calls::status_text(&err));
            }
            if is_spawn_terminal_error(&err) {
                return Err(calls::status_text(&err));
            }
            self.backoff(delay).await?;
            delay = (delay * 2).min(MAX_RETRY_BACKOFF);
        }
    }

    async fn ping_until_ready(&self, name: &str) -> Result<(), String> {
        let body = pb::PingRequest {
            message: uuid::Uuid::new_v4().to_string(),
        }
        .encode_to_vec();
        loop {
            if self.cancel.is_cancelled() {
                return Err("context canceled".into());
            }
            if self.one_ping(name, body.clone()).await.is_ok() {
                return Ok(());
            }
            tokio::select! {
                _ = self.cancel.cancelled() => return Err("context canceled".into()),
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    }

    async fn one_ping(&self, name: &str, body: Vec<u8>) -> Result<(), String> {
        let span = self.cfg.tracer.start("GluttonPing");
        let ctx = *span.context();
        let start = Instant::now();
        let reply = post(&self.cfg, name, PING_ROUTE, body, &ctx).await;
        let latency = start.elapsed();
        let res = reply.and_then(|r| {
            if r.status != reqwest::StatusCode::OK {
                return Err(format!(
                    "HTTP {}: {}",
                    r.status.as_u16(),
                    String::from_utf8_lossy(&r.body).trim()
                ));
            }
            pb::PingResponse::decode(r.body.as_ref()).map_err(|e| e.to_string())?;
            Ok(r.body.len() as i64)
        });
        log_sampled_trace(
            &ctx,
            "GluttonPing",
            latency,
            SOURCE_CLIENT,
            res.as_ref().err().map(String::as_str),
        );
        span.end();
        match res {
            Ok(n) => {
                self.rec().success("http", "GluttonPing", latency, n);
                Ok(())
            }
            Err(e) => {
                self.rec().failure("http", "GluttonPing", latency, &e);
                Err(e)
            }
        }
    }

    /// Stops the batch, waits briefly for it, and deletes every Actor it
    /// started, whether or not it got ready.
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        let _ = tokio::time::timeout(SHUTDOWN_BATCH_WAIT, self.workers.wait()).await;
        self.delete_all().await;
        if self.counted_user.swap(false, Ordering::SeqCst) {
            metrics::update_users(&self.user_class, -1.0);
        }
    }

    async fn delete_all(&self) {
        let names = self.actor_names();
        if names.is_empty() {
            return;
        }
        let queue = Mutex::new(names.into_iter());
        let concurrency = MAX_DELETE_CONCURRENCY.min(queue.lock().len());
        let worker = || async {
            loop {
                let Some(name) = queue.lock().next() else {
                    return;
                };
                let cfg = &self.cfg;
                let req = api::DeleteActorRequest {
                    actor: Some(calls::actor_ref(cfg, &name)),
                    ..Default::default()
                };
                let _ = traced_call(cfg, &self.rec(), "DeleteActor", |span| {
                    cfg.api.delete_actor(traced_request(req, &span))
                })
                .await;
            }
        };
        futures::future::join_all((0..concurrency).map(|_| worker())).await;
    }
}

#[async_trait]
impl UserTask for SpawnRuntime {
    async fn iterate(&self, _vu: VuId) {
        SpawnRuntime::iterate(self).await
    }
    async fn shutdown(&self) {
        SpawnRuntime::shutdown(self).await
    }
}

#[cfg(test)]
#[path = "spawn_tests.rs"]
mod tests;

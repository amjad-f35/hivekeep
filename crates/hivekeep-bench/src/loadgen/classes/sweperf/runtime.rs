//! The per-worker state of SweperfUser: one session per user, started on
//! first use, run one cycle per iteration, retired when its trajectory is
//! done; and a shutdown that cancels in-flight work and deletes every
//! Actor still bound to a user.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tonic::Code;

use super::chunks::generate_dynamic_chunks;
use super::ctx::Ctx;
use super::events::event;
use super::knobs::Knobs;
use super::session::Session;
use super::{DEFAULT_NUM_CYCLES, DEFAULT_TEMPLATE, DEFAULT_TOTAL_STEPS};
use crate::loadgen::metrics;
use crate::loadgen::userclass::{Config, UserTask, VuId};

/// How many Actors shutdown deletes at once: the pod's termination grace,
/// not the worker's shutdown budget, is the real limit, and a serial sweep
/// over thousands of sessions would overrun it.
const SHUTDOWN_CONCURRENCY: usize = 64;

/// The time shutdown takes for itself when run through [`UserTask`].
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(50);

#[derive(Default)]
struct Inflight {
    /// Set once by shutdown and never cleared.
    stopping: bool,
    /// Iterations in progress, each of which may create or delete an Actor.
    running: usize,
}

/// Marks one iteration in progress; dropping it lets shutdown proceed.
pub struct InflightGuard<'a>(&'a SweperfRuntime);

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        let mut i = self.0.inflight.lock();
        i.running -= 1;
        if i.running == 0 {
            self.0.drained.notify_waiters();
        }
    }
}

/// The SweperfUser runtime.
pub struct SweperfRuntime {
    cfg: Arc<Config>,
    user_class: String,
    users: Mutex<HashMap<VuId, Arc<Session>>>,
    /// Carries every startup and cycle; shutdown cancels it so a slow poll
    /// or cycle does not hold up teardown.
    run: CancellationToken,
    inflight: Mutex<Inflight>,
    drained: Notify,
    /// Set once CreateHive succeeds, so later sessions skip it; cleared when
    /// CreateActor reports the Hive missing.
    hive_ready: AtomicBool,
    /// Bounds deleting an Actor once the run may already be canceled.
    pub(crate) teardown_timeout: Duration,
    /// Caps how long shutdown waits for in-flight iterations, which are
    /// canceled and normally end at once.
    pub(crate) inflight_wait_timeout: Duration,
}

impl SweperfRuntime {
    /// A runtime over `cfg` recording under `user_class`.
    pub fn new(cfg: Arc<Config>, user_class: &str) -> Self {
        SweperfRuntime {
            cfg,
            user_class: user_class.to_string(),
            users: Mutex::new(HashMap::new()),
            run: CancellationToken::new(),
            inflight: Mutex::new(Inflight::default()),
            drained: Notify::new(),
            hive_ready: AtomicBool::new(false),
            teardown_timeout: Duration::from_secs(30),
            inflight_wait_timeout: Duration::from_secs(10),
        }
    }

    fn run_ctx(&self) -> Ctx {
        Ctx::new(self.run.clone())
    }

    /// A context for deleting an Actor that outlives the run, so shutdown
    /// does not strand the Actor.
    fn teardown_ctx(&self) -> Ctx {
        Ctx::background().with_timeout(self.teardown_timeout)
    }

    /// Registers an iteration, or `None` once shutdown has begun.
    pub fn enter(&self) -> Option<InflightGuard<'_>> {
        let mut i = self.inflight.lock();
        if i.stopping {
            return None;
        }
        i.running += 1;
        Some(InflightGuard(self))
    }

    /// Binds `s` to user `vu`.
    pub fn bind(&self, vu: VuId, s: Arc<Session>) {
        self.users.lock().insert(vu, s);
    }

    /// The session bound to `vu`.
    pub fn session(&self, vu: VuId) -> Option<Arc<Session>> {
        self.users.lock().get(&vu).cloned()
    }

    /// The template, step count and cycle count for a new session, from
    /// the config with built-in fallbacks. Read per session, so a mid-run
    /// change applies to later sessions.
    pub fn resolve_config(&self) -> (String, usize, usize) {
        let k = self.cfg.dyn_config.get::<Knobs>();
        let template = if k.template.is_empty() {
            DEFAULT_TEMPLATE.to_string()
        } else {
            k.template
        };
        let steps = if k.total_steps > 0 {
            k.total_steps as usize
        } else {
            DEFAULT_TOTAL_STEPS
        };
        let cycles = if k.num_cycles > 0 {
            k.num_cycles as usize
        } else {
            DEFAULT_NUM_CYCLES
        };
        (template, steps, cycles)
    }

    /// The think time between cycles.
    pub fn dynamic_wait(&self) -> Duration {
        self.cfg.dyn_config.get::<Knobs>().wait.draw()
    }

    /// One user iteration: binds a session on first use and runs one cycle
    /// per call; a finished trajectory deletes its Actor and unbinds it, so
    /// the next call starts a fresh one. A session that fails to start is
    /// retried next iteration. Nothing new starts once shutdown begins.
    pub async fn iterate_user(&self, vu: VuId) {
        let Some(guard) = self.enter() else { return };
        let before = match self.session(vu) {
            Some(s) => s.progress.lock().await.cycle_index,
            None => 0,
        };
        self.run_cycle(vu).await;
        drop(guard);

        let wait = self.dynamic_wait();
        // Think time counts toward a task only between its cycles: this
        // call completed a cycle and the session is still bound.
        if let Some(s) = self.session(vu) {
            let mut p = s.progress.lock().await;
            if p.cycle_index > before {
                p.loop_think += wait;
            }
        }
        tokio::time::sleep(wait).await;
    }

    /// The part of an iteration shutdown waits for: it may start, cycle or
    /// delete an Actor, but excludes the think time.
    async fn run_cycle(&self, vu: VuId) {
        let user = match self.session(vu) {
            Some(u) => u,
            None => match self.start_user(&self.run_ctx(), vu).await {
                Ok(u) => {
                    self.bind(vu, u.clone());
                    u
                }
                Err(_) => return, // start_user logged it
            },
        };
        if self.run.is_cancelled() {
            return; // shutdown's sweep tears the Actor down
        }
        let mut p = user.progress.lock().await;
        user.step(&self.run_ctx(), &mut p).await;
        if p.is_done() {
            // A task finished as shutdown began records nothing.
            if !self.run.is_cancelled() {
                user.record_task_metrics(&mut p);
            }
            drop(p);
            self.retire_user(vu, &user).await;
        }
    }

    /// Creates an Actor and waits until its sandbox serves, so the session
    /// is ready for cycles. On failure the Actor is torn down and the error
    /// logged with the user, Actor and template.
    pub async fn start_user(&self, ctx: &Ctx, vu: VuId) -> Result<Arc<Session>, String> {
        let (template, steps, cycles) = self.resolve_config();
        let chunks = generate_dynamic_chunks(steps, cycles);
        let u = Arc::new(Session::new(
            self.cfg.clone(),
            &format!("sb-{}", uuid::Uuid::new_v4()),
            &template,
            &self.user_class,
            vu,
            chunks,
        ));
        tracing::info!(actor = %u.actor_name, template = %u.template_name, "Creating new sweperf user session");
        let res = self.start_session(ctx, &u).await;
        if let Err(e) = &res {
            event!(
                warn,
                "sweperf on_start failed; the user will retry next iteration",
                user = vu,
                actor = u.actor_name.as_str(),
                template = u.template_name.as_str(),
                err = e.as_str(),
            );
        }
        res.map(|()| u)
    }

    async fn start_session(&self, ctx: &Ctx, u: &Arc<Session>) -> Result<(), String> {
        metrics::update_users(&self.user_class, 1.0);
        if !self.hive_ready.load(Ordering::SeqCst) {
            if let Err(e) = u.ensure_hive(ctx).await {
                metrics::update_users(&self.user_class, -1.0);
                return Err(format!("ensureHive: {}", e.message()));
            }
            self.hive_ready.store(true, Ordering::SeqCst);
        }
        // CreateActor runs detached from the run: canceled mid-call, the
        // Actor might exist without the session knowing to delete it.
        let started = Instant::now();
        if let Err(e) = u.create(&self.teardown_ctx()).await {
            // hivekeep-api answers FAILED_PRECONDITION for a missing Hive.
            if e.code() == Code::FailedPrecondition {
                self.hive_ready.store(false, Ordering::SeqCst);
            }
            metrics::update_users(&self.user_class, -1.0);
            return Err(format!("createActor: {}", e.message()));
        }
        if let Err(e) = u.poll_liveness(ctx).await {
            // Torn down even when shutdown canceled `ctx`; teardown also
            // drops the user gauge.
            u.teardown(&self.teardown_ctx()).await;
            return Err(format!("pollLiveness: {e}"));
        }
        let mut p = u.progress.lock().await;
        p.startup_dur = started.elapsed();
        // The liveness poll left the Actor running, so cycle 1's resume is
        // a no-op.
        p.awake = true;
        tracing::info!(actor = %u.actor_name, "Sweperf user session is ready");
        Ok(())
    }

    /// Deletes the Actor of a finished session and unbinds it. The last
    /// cycle leaves the Actor running and DeleteActor removes it in any
    /// state, so no suspend is sent first. A failed delete is retried once
    /// with a fresh timeout, then the Actor is left behind and logged.
    pub async fn retire_user(&self, vu: VuId, u: &Arc<Session>) {
        {
            let mut users = self.users.lock();
            if users.get(&vu).is_some_and(|cur| Arc::ptr_eq(cur, u)) {
                users.remove(&vu);
            }
        }
        if u.cleaned_up.swap(true, Ordering::SeqCst) {
            return; // shutdown already owns this Actor's teardown
        }
        metrics::update_users(&self.user_class, -1.0);
        if u.delete(&self.teardown_ctx()).await.is_err() {
            if let Err(e) = u.delete(&self.teardown_ctx()).await {
                tracing::error!(actor = %u.actor_name, err = %e.message(), "DeleteActor failed twice for finished actor; it is leaked");
                return;
            }
        }
        let cycles = u.progress.lock().await.chunks.len();
        tracing::info!(
            actor = %u.actor_name,
            cycles,
            "Actor finished all cycles and was deleted; a new actor will run the trajectory next"
        );
    }

    async fn wait_drained(&self) {
        loop {
            let notified = self.drained.notified();
            if self.inflight.lock().running == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Stops new iterations, cancels in-flight startups and cycles, waits
    /// up to the in-flight cap for them, then deletes every Actor still
    /// bound to a user, [`SHUTDOWN_CONCURRENCY`] at a time, within
    /// `budget`. A user the master stopped mid-run keeps its Actor until
    /// this runs.
    pub async fn shutdown_within(&self, budget: Option<Duration>) {
        event!(info, "sweperf shutdown started");
        self.inflight.lock().stopping = true;
        self.run.cancel();
        let ctx = match budget {
            Some(b) => Ctx::background().with_timeout(b),
            None => Ctx::background(),
        };
        if ctx
            .with_timeout(self.inflight_wait_timeout)
            .run(self.wait_drained())
            .await
            .is_err()
        {
            tracing::warn!(
                "shutdown timed out waiting for in-flight iterations; their actors may leak"
            );
        }
        let users: Vec<Arc<Session>> = self.users.lock().values().cloned().collect();
        let sweep = futures::stream::iter(users)
            .map(|u| {
                let ctx = ctx.clone();
                async move { u.teardown(&ctx).await }
            })
            .buffer_unordered(SHUTDOWN_CONCURRENCY)
            .collect::<Vec<()>>();
        let _ = ctx.run(sweep).await;
    }
}

#[async_trait]
impl UserTask for SweperfRuntime {
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

//! The HkEnvUser task: users, their router connections, the iteration
//! cycle, and a shutdown that stops new iterations, waits (bounded) for the
//! ones in flight, and cleans up.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use tokio_util::sync::CancellationToken;

use super::actor::{HkEnvActor, ProcessClient};
use super::knobs::HkEnvKnobs;
use super::target::router_grpc_target;
use super::USER_CLASS;
use crate::loadgen::classes::glutton::wait_group::{Done, WaitGroup};
use crate::loadgen::client::{
    router_server_name, router_tls_config, POD_IDENTITY_CRED_BUNDLE, SERVICE_DNS_CA_FILE,
};
use crate::loadgen::dynconfig::{uniform, LIFECYCLE_MODE_NONE, LIFECYCLE_MODE_SYNC};
use crate::loadgen::metrics;
use crate::loadgen::userclass::{Config, UserTask, VuId};

/// Execs after the first in a wake are spaced by a random gap in
/// `[MIN_PING_GAP, MAX_PING_GAP)`.
pub const MIN_PING_GAP: Duration = Duration::from_millis(200);
/// See [`MIN_PING_GAP`].
pub const MAX_PING_GAP: Duration = Duration::from_secs(1);
/// The cleanup budget a plain [`UserTask::shutdown`] gets.
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(60);

/// A user's connection to the guest API through the router. Closing it
/// fails any exec still waiting on it.
#[derive(Clone)]
pub struct RouterConn {
    /// The process API client.
    pub client: ProcessClient,
    /// Fired by [`RouterConn::close`].
    pub closed: CancellationToken,
}

impl RouterConn {
    /// A connection over `client`.
    pub fn new(client: ProcessClient) -> Self {
        RouterConn {
            client,
            closed: CancellationToken::new(),
        }
    }

    /// Closes the connection.
    pub fn close(&self) {
        self.closed.cancel();
    }
}

/// Opens a user's router connection.
pub type DialFn = Arc<dyn Fn() -> Result<RouterConn, String> + Send + Sync>;

/// Stands in for sleeping in tests; returns whether the sleep ran out
/// (rather than being cut short by shutdown).
pub type SleepFn = Arc<dyn Fn(Duration) -> bool + Send + Sync>;

/// One virtual user.
pub struct HkEnvUser {
    pub(crate) conn: RouterConn,
    pub(crate) actors: Mutex<Vec<Arc<HkEnvActor>>>,
    pub(crate) next_idx: Mutex<usize>,
}

impl HkEnvUser {
    fn next_actor(&self) -> Option<Arc<HkEnvActor>> {
        let actors = self.actors.lock();
        if actors.is_empty() {
            return None;
        }
        let mut i = self.next_idx.lock();
        let a = actors[*i].clone();
        *i = (*i + 1) % actors.len();
        Some(a)
    }

    fn snapshot(&self) -> Vec<Arc<HkEnvActor>> {
        self.actors.lock().clone()
    }

    async fn replace_actor(&self, broken: &Arc<HkEnvActor>) {
        if broken.mark_deleted() {
            broken.delete().await;
        }
        let replacement = Arc::new(HkEnvActor::new(
            broken.cfg.clone(),
            format!("sb-{}", uuid::Uuid::new_v4()),
            broken.client.clone(),
            broken.closed.clone(),
        ));
        if let Err(e) = replacement.create().await {
            tracing::warn!(old_actor = %broken.actor_name, err = %e.message(),
                "hkenv actor replacement create failed; will retry on next failure");
            return;
        }
        let mut actors = self.actors.lock();
        if let Some(slot) = actors.iter_mut().find(|a| Arc::ptr_eq(a, broken)) {
            *slot = replacement;
        }
    }
}

/// The HkEnvUser task.
pub struct TaskRuntime {
    pub(crate) cfg: Arc<Config>,
    pub(crate) users: Mutex<HashMap<VuId, Arc<HkEnvUser>>>,
    pub(crate) dial: Option<DialFn>,
    pub(crate) sleep_fn: Option<SleepFn>,
    stopping: RwLock<bool>,
    stop: CancellationToken,
    in_flight: WaitGroup,
}

/// The real dial: TLS to the router, gRPC to the guest API behind it.
fn router_dial(router_url: &str) -> Result<DialFn, String> {
    let target = router_grpc_target(router_url)?;
    let addr = target.trim_start_matches("dns:///").to_string();
    let mut tls = router_tls_config(
        SERVICE_DNS_CA_FILE,
        POD_IDENTITY_CRED_BUNDLE,
        &router_server_name(),
    )
    .map_err(|e| e.to_string())?;
    tls.alpn_protocols = vec![b"h2".to_vec()];
    let tls = Arc::new(tls);
    Ok(Arc::new(move || {
        let channel =
            crate::tls::client::lazy_channel(&addr, tls.clone()).map_err(|e| e.to_string())?;
        Ok(RouterConn::new(ProcessClient::new(channel)))
    }))
}

impl TaskRuntime {
    /// A runtime over `cfg`, dialing the router itself.
    pub fn new(cfg: Arc<Config>) -> Self {
        Self::with_dial(cfg, None)
    }

    /// A runtime over `cfg` using `dial` for router connections.
    pub fn with_dial(cfg: Arc<Config>, dial: Option<DialFn>) -> Self {
        TaskRuntime {
            cfg,
            users: Mutex::new(HashMap::new()),
            dial,
            sleep_fn: None,
            stopping: RwLock::new(false),
            stop: CancellationToken::new(),
            in_flight: WaitGroup::default(),
        }
    }

    fn knobs(&self) -> HkEnvKnobs {
        self.cfg.dyn_config.get::<HkEnvKnobs>()
    }

    fn dynamic_wait(&self) -> Duration {
        self.knobs().wait.draw()
    }

    fn live_wait(&self) -> Duration {
        let k = self.knobs();
        uniform(k.min_live.duration(), k.max_live.duration())
    }

    fn is_stopping(&self) -> bool {
        *self.stopping.read()
    }

    /// Counts an iteration in, unless shutdown has begun.
    fn try_start_iter(&self) -> bool {
        let stopping = self.stopping.read();
        if *stopping {
            return false;
        }
        self.in_flight.add();
        true
    }

    fn mark_stopping(&self) {
        let mut s = self.stopping.write();
        *s = true;
        self.stop.cancel();
    }

    /// Sleeps `d`; false when shutdown cut it short.
    async fn sleep(&self, d: Duration) -> bool {
        if let Some(f) = &self.sleep_fn {
            return f(d);
        }
        if d.is_zero() {
            return !self.is_stopping();
        }
        tokio::select! {
            _ = tokio::time::sleep(d) => true,
            _ = self.stop.cancelled() => false,
        }
    }

    async fn start_user(&self) -> Result<HkEnvUser, String> {
        let n = self.cfg.actors_per_user.max(1);
        let dial = match &self.dial {
            Some(d) => d.clone(),
            None => router_dial(&self.cfg.router_url)?,
        };
        let conn = dial().map_err(|e| format!("dial router gRPC: {e}"))?;
        let user = HkEnvUser {
            conn: conn.clone(),
            actors: Mutex::new(Vec::with_capacity(n)),
            next_idx: Mutex::new(0),
        };
        metrics::update_users(USER_CLASS, 1.0);
        let mut last_err = None;
        for i in 0..n {
            let a = HkEnvActor::new(
                self.cfg.clone(),
                format!("sb-{}", uuid::Uuid::new_v4()),
                conn.client.clone(),
                conn.closed.clone(),
            );
            if i == 0 {
                if let Err(e) = a.ensure_hive().await {
                    conn.close();
                    metrics::update_users(USER_CLASS, -1.0);
                    return Err(e.message().to_string());
                }
            }
            if let Err(e) = a.create().await {
                tracing::warn!(hive = %self.cfg.hive, wanted = n, got = user.actors.lock().len(), err = %e.message(),
                    "hkenv create failed partway; using actors created so far");
                last_err = Some(e);
                break;
            }
            user.actors.lock().push(Arc::new(a));
        }
        if user.actors.lock().is_empty() {
            conn.close();
            metrics::update_users(USER_CLASS, -1.0);
            return Err(format!(
                "no actors created: {}",
                last_err
                    .map(|e| e.message().to_string())
                    .unwrap_or_default()
            ));
        }
        Ok(user)
    }

    async fn iterate_once(&self, vu: VuId) {
        let existing = self.users.lock().get(&vu).cloned();
        let user = match existing {
            Some(u) => u,
            None => match self.start_user().await {
                Ok(u) => self
                    .users
                    .lock()
                    .entry(vu)
                    .or_insert_with(|| Arc::new(u))
                    .clone(),
                Err(e) => {
                    tracing::warn!(err = %e, "hkenv on_start failed; the user will retry next iteration");
                    return;
                }
            },
        };
        let Some(actor) = user.next_actor() else {
            return;
        };

        // Reaching a crashed Actor means its replacement's create failed.
        if actor.is_crashed() {
            user.replace_actor(&actor).await;
            return;
        }
        // A stranded Actor goes back to suspend, not resume.
        if actor.is_hibernate_pending() {
            match actor.hibernate().await {
                Err(e) => {
                    if actor.note_failure(&e) {
                        user.replace_actor(&actor).await;
                    }
                }
                Ok(()) => actor.note_success(),
            }
            return;
        }
        let knobs = self.knobs();
        if !actor.is_running() || knobs.lifecycle.lifecycle_mode != LIFECYCLE_MODE_NONE {
            if let Err(e) = actor.resume().await {
                if actor.note_failure(&e) {
                    user.replace_actor(&actor).await;
                }
                return;
            }
        }

        // Live window: exec, then hold the Actor running until it closes.
        // At most one exec failure per wake counts toward replacement, so a
        // multi-exec wake cannot replace on one transient blip.
        let deadline = tokio::time::Instant::now() + self.live_wait();
        let max_pings = knobs.max_pings_per_wake.max(1);
        let mut exec_err = None;
        for sent in 0..max_pings {
            if sent > 0 {
                let gap = MIN_PING_GAP
                    + (MAX_PING_GAP - MIN_PING_GAP).mul_f64(rand::rng().random::<f64>());
                if tokio::time::Instant::now() + gap > deadline || !self.sleep(gap).await {
                    break;
                }
            }
            match actor.exec_hello_world().await {
                Err(e) => exec_err = Some(e),
                Ok(()) => {
                    exec_err = None;
                    actor.note_success();
                }
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if !remaining.is_zero() {
            self.sleep(remaining).await;
        }
        if let Err(e) = actor.hibernate().await {
            if actor.note_failure(&e) {
                user.replace_actor(&actor).await;
            }
            return;
        }
        if let Some(e) = exec_err {
            if actor.note_exec_failure(&e) {
                user.replace_actor(&actor).await;
            }
            return;
        }
        actor.note_success();
    }

    /// One iteration, then the wait window; nothing once shutdown began.
    pub async fn iterate(&self, vu: VuId) {
        if !self.try_start_iter() {
            return;
        }
        {
            let _done = Done(&self.in_flight);
            self.iterate_once(vu).await;
        }
        self.sleep(self.dynamic_wait()).await;
    }

    /// Stops new iterations, waits up to half of `budget` for those in
    /// flight, then (within what is left) suspends still-running Actors and
    /// deletes them all, and closes every router connection.
    pub async fn shutdown_within(&self, budget: Duration) {
        let started = tokio::time::Instant::now();
        self.mark_stopping();
        let _ = tokio::time::timeout(budget / 2, self.in_flight.wait()).await;
        let users: Vec<_> = self.users.lock().values().cloned().collect();
        let sync = self.knobs().lifecycle.lifecycle_mode == LIFECYCLE_MODE_SYNC;
        let cleanup = async {
            for u in &users {
                for a in u.snapshot() {
                    if a.is_running() {
                        let _ = if sync {
                            a.suspend().await
                        } else {
                            a.pause().await
                        };
                    }
                    a.delete().await;
                }
                u.conn.close();
                metrics::update_users(USER_CLASS, -1.0);
            }
        };
        let left = budget.saturating_sub(started.elapsed());
        let _ = tokio::time::timeout(left, cleanup).await;
        // Whatever the budget allowed, no connection outlives shutdown.
        for u in &users {
            u.conn.close();
        }
    }
}

#[async_trait]
impl UserTask for TaskRuntime {
    async fn iterate(&self, vu: VuId) {
        TaskRuntime::iterate(self, vu).await
    }
    async fn shutdown(&self) {
        self.shutdown_within(SHUTDOWN_BUDGET).await
    }
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;

//! GluttonUser: each user owns `--actors-per-user` glutton Actors and, one
//! Actor per iteration in round robin, resumes it, fills and churns its
//! working set, pings it through the live window, and suspends it again.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use rand::Rng;

use super::actor::GluttonActor;
use super::knobs::GluttonKnobs;
use super::GLUTTON_USER_CLASS;
use crate::loadgen::dynconfig::{uniform, LIFECYCLE_MODE_NONE, LIFECYCLE_MODE_SYNC};
use crate::loadgen::metrics;
use crate::loadgen::userclass::{Config, UserTask, VuId};

/// Pings after the first in a wake are spaced by a random gap in
/// `[MIN_PING_GAP, MAX_PING_GAP)`.
pub const MIN_PING_GAP: Duration = Duration::from_millis(200);
/// See [`MIN_PING_GAP`].
pub const MAX_PING_GAP: Duration = Duration::from_secs(1);

/// One virtual user: its Actors and the round-robin cursor.
#[derive(Default)]
pub struct GluttonUser {
    pub(crate) actors: Vec<GluttonActor>,
    pub(crate) next_idx: usize,
}

impl GluttonUser {
    /// The index of the Actor to drive now, advancing the cursor.
    pub fn next_actor(&mut self) -> Option<usize> {
        if self.actors.is_empty() {
            return None;
        }
        let i = self.next_idx;
        self.next_idx = (self.next_idx + 1) % self.actors.len();
        Some(i)
    }

    /// Deletes a wedged Actor (once) and puts a fresh one in its slot, so
    /// the user keeps its full count without driving a stuck Actor.
    pub async fn replace_actor(&mut self, i: usize) {
        let broken = &mut self.actors[i];
        if !broken.deleted {
            broken.delete().await;
            broken.deleted = true;
        }
        let replacement =
            GluttonActor::new(broken.cfg.clone(), format!("sb-{}", uuid::Uuid::new_v4()));
        if let Err(e) = replacement.create().await {
            tracing::warn!(old_actor = %broken.actor_name, err = %e.message(),
                "glutton actor replacement create failed; will retry on next failure");
            return;
        }
        self.actors[i] = replacement;
    }
}

/// The GluttonUser task.
pub struct TaskRuntime {
    pub(crate) cfg: Arc<Config>,
    pub(crate) users: Mutex<HashMap<VuId, Arc<tokio::sync::Mutex<GluttonUser>>>>,
}

impl TaskRuntime {
    /// A runtime over `cfg`.
    pub fn new(cfg: Arc<Config>) -> Self {
        TaskRuntime {
            cfg,
            users: Mutex::new(HashMap::new()),
        }
    }

    fn knobs(&self) -> GluttonKnobs {
        self.cfg.dyn_config.get::<GluttonKnobs>()
    }

    /// The gap between suspending one Actor and resuming the next.
    pub fn dynamic_wait(&self) -> Duration {
        self.knobs().wait.draw()
    }

    /// How long an Actor stays resumed between its first ping and its
    /// suspend; the default zero window suspends right after the ping.
    pub fn live_wait(&self) -> Duration {
        let k = self.knobs();
        uniform(k.min_live.duration(), k.max_live.duration())
    }

    /// Creates the user's Actors (and the Hive, once per user). Uses the
    /// Actors created so far if a create fails partway.
    pub async fn start_user(&self) -> Result<GluttonUser, String> {
        let n = self.cfg.actors_per_user.max(1);
        let mut u = GluttonUser::default();
        metrics::update_users(GLUTTON_USER_CLASS, 1.0);
        let mut last_err = None;
        for i in 0..n {
            let a = GluttonActor::new(self.cfg.clone(), format!("sb-{}", uuid::Uuid::new_v4()));
            if i == 0 {
                if let Err(e) = a.ensure_hive().await {
                    metrics::update_users(GLUTTON_USER_CLASS, -1.0);
                    return Err(e.message().to_string());
                }
            }
            if let Err(e) = a.create().await {
                tracing::warn!(hive = %self.cfg.hive, wanted = n, got = u.actors.len(), err = %e.message(),
                    "glutton create failed partway; using actors created so far");
                last_err = Some(e);
                break;
            }
            u.actors.push(a);
        }
        if u.actors.is_empty() {
            metrics::update_users(GLUTTON_USER_CLASS, -1.0);
            return Err(format!(
                "no actors created: {}",
                last_err
                    .map(|e| e.message().to_string())
                    .unwrap_or_default()
            ));
        }
        Ok(u)
    }

    async fn user(&self, vu: VuId) -> Option<Arc<tokio::sync::Mutex<GluttonUser>>> {
        if let Some(u) = self.users.lock().get(&vu).cloned() {
            return Some(u);
        }
        match self.start_user().await {
            Ok(u) => Some(
                self.users
                    .lock()
                    .entry(vu)
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(u)))
                    .clone(),
            ),
            Err(e) => {
                tracing::warn!(err = %e, "glutton on_start failed; the user will retry next iteration");
                None
            }
        }
    }

    async fn iterate_once(&self, vu: VuId) {
        let Some(user) = self.user(vu).await else {
            return;
        };
        let mut user = user.lock().await;
        let Some(i) = user.next_actor() else { return };

        // A crashed Actor is replaced on the spot by resume's failure path,
        // so reaching here means the replacement's create failed last time.
        if user.actors[i].crashed {
            user.replace_actor(i).await;
            return;
        }

        // A stranded Actor goes back to suspend, not resume: SuspendActor is
        // re-entrant, and a resume would fail FailedPrecondition and cost
        // the Actor for a transient error.
        if user.actors[i].hibernate_pending {
            if let Err(e) = user.actors[i].hibernate().await {
                if user.actors[i].note_failure(&e) {
                    user.replace_actor(i).await;
                }
            }
            return;
        }

        let knobs = self.knobs();
        if !user.actors[i].actor_running || knobs.lifecycle.lifecycle_mode != LIFECYCLE_MODE_NONE {
            if let Err(e) = user.actors[i].resume().await {
                if user.actors[i].note_failure(&e) {
                    user.replace_actor(i).await;
                }
                return;
            }
        }
        let actor = &mut user.actors[i];
        // Fill and start the CPU load before the first suspend, so every
        // snapshot carries them; both persist in the glutton process.
        actor.ensure_ram_filled().await;
        actor.ensure_cpu_load().await;
        // Walk the working set before churn dirties it.
        actor.read_ram().await;
        actor.churn_ram().await;

        // Live window: ping at once, then up to max_pings-1 more a random
        // gap apart while the window lasts; sleep out the rest so the Actor
        // stays live for the whole window.
        let deadline = tokio::time::Instant::now() + self.live_wait();
        let max_pings = knobs.max_pings_per_wake.max(1);
        actor.ping().await;
        for _ in 1..max_pings {
            let gap =
                MIN_PING_GAP + (MAX_PING_GAP - MIN_PING_GAP).mul_f64(rand::rng().random::<f64>());
            if tokio::time::Instant::now() + gap > deadline {
                break;
            }
            tokio::time::sleep(gap).await;
            actor.ping().await;
        }
        tokio::time::sleep_until(deadline).await;
        if let Err(e) = actor.hibernate().await {
            if actor.note_failure(&e) {
                user.replace_actor(i).await;
            }
        }
    }

    /// One iteration, then the wait window. Every path waits, so a failing
    /// start, resume or crashed Actor does not loop hot against the API.
    pub async fn iterate(&self, vu: VuId) {
        self.iterate_once(vu).await;
        tokio::time::sleep(self.dynamic_wait()).await;
    }

    /// Suspends any still-running Actor and deletes every Actor this worker
    /// created. Users removed mid-run keep their Actors until here.
    pub async fn shutdown(&self) {
        let users: Vec<_> = self.users.lock().values().cloned().collect();
        let sync = self.knobs().lifecycle.lifecycle_mode == LIFECYCLE_MODE_SYNC;
        for u in users {
            let mut u = u.lock().await;
            for a in u.actors.iter_mut() {
                if a.actor_running {
                    let _ = if sync {
                        a.suspend().await
                    } else {
                        a.pause().await
                    };
                }
                a.delete().await;
            }
            metrics::update_users(GLUTTON_USER_CLASS, -1.0);
        }
    }
}

#[async_trait]
impl UserTask for TaskRuntime {
    async fn iterate(&self, vu: VuId) {
        TaskRuntime::iterate(self, vu).await
    }
    async fn shutdown(&self) {
        TaskRuntime::shutdown(self).await
    }
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;

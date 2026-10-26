//! The worker side of a locust run: answers the master's spawn, stop and
//! quit messages, keeps the requested number of virtual users iterating,
//! and sends heartbeats and request stats.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use rmpv::Value;
use tokio_util::sync::CancellationToken;

use super::message::{map, map_get, number, Message};
use super::stats;
use super::transport::{Transport, TransportError};
use super::usage::Usage;
use crate::loadgen::events::Events;
use crate::loadgen::userclass::{UserTask, VuId};

/// The version a worker announces: `-1` tells the master not to compare
/// versions.
const CLIENT_VERSION: i64 = -1;

/// Where the worker is in the run, as the master's heartbeats name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Connected, no users.
    Ready,
    /// Adjusting the user count.
    Spawning,
    /// Users running.
    Running,
    /// Stopped by the master.
    Stopped,
}

impl State {
    /// The wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            State::Ready => "ready",
            State::Spawning => "spawning",
            State::Running => "running",
            State::Stopped => "stopped",
        }
    }
}

/// Why the run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// The master sent `quit`.
    MasterQuit,
    /// The process was asked to stop.
    Signal,
}

/// Timing of the worker's own messages.
#[derive(Debug, Clone)]
pub struct Timing {
    /// Gap between heartbeats.
    pub heartbeat: Duration,
    /// Gap between stats reports.
    pub stats: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Timing {
            heartbeat: Duration::from_secs(1),
            stats: Duration::from_secs(3),
        }
    }
}

/// One worker process's run of one user class.
pub struct Runner {
    user_class: String,
    task: Arc<dyn UserTask>,
    events: Events,
    node_id: String,
    timing: Timing,
}

/// The running virtual users.
struct Users {
    next_id: VuId,
    running: Vec<CancellationToken>,
}

impl Users {
    fn count(&self) -> usize {
        self.running.len()
    }

    /// Starts or stops users until `n` run. Stopped users finish their
    /// current iteration first.
    fn scale(&mut self, n: usize, task: &Arc<dyn UserTask>) {
        while self.running.len() > n {
            if let Some(t) = self.running.pop() {
                t.cancel();
            }
        }
        while self.running.len() < n {
            let vu = self.next_id;
            self.next_id += 1;
            let token = CancellationToken::new();
            self.running.push(token.clone());
            let task = task.clone();
            tokio::spawn(async move {
                while !token.is_cancelled() {
                    if let Err(panic) = AssertUnwindSafe(task.iterate(vu)).catch_unwind().await {
                        let msg = panic
                            .downcast_ref::<&str>()
                            .map(|s| s.to_string())
                            .or_else(|| panic.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "panic".to_string());
                        tracing::error!(vu, err = %msg, "user iteration panicked");
                        stats::record_failure("unknown", "panic", 0, &msg);
                    }
                    tokio::task::yield_now().await;
                }
            });
        }
    }
}

/// A node ID the master tells workers apart by: host name and a random
/// suffix.
pub fn new_node_id() -> String {
    let host = hostname().unwrap_or_else(|| "worker".to_string());
    format!("{host}_{}", uuid::Uuid::new_v4().simple())
}

fn hostname() -> Option<String> {
    if let Ok(h) = std::env::var("HOSTNAME") {
        if !h.is_empty() {
            return Some(h);
        }
    }
    #[cfg(unix)]
    {
        let mut buf = [0u8; 256];
        // SAFETY: the buffer is valid for its length and gethostname
        // NUL-terminates within it on success.
        let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
        if rc == 0 {
            let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
            return String::from_utf8(buf[..end].to_vec()).ok();
        }
    }
    None
}

impl Runner {
    /// A runner of `task` as the locust class `user_class`.
    pub fn new(user_class: &str, task: Arc<dyn UserTask>, events: Events) -> Self {
        Runner {
            user_class: user_class.to_string(),
            task,
            events,
            node_id: new_node_id(),
            timing: Timing::default(),
        }
    }

    /// Overrides the worker's node ID.
    pub fn with_node_id(mut self, id: &str) -> Self {
        self.node_id = id.to_string();
        self
    }

    /// Overrides heartbeat and stats timing.
    pub fn with_timing(mut self, timing: Timing) -> Self {
        self.timing = timing;
        self
    }

    /// The worker's node ID.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    fn msg(&self, kind: &str, data: Value) -> Message {
        Message::new(kind, data, &self.node_id)
    }

    fn class_counts(&self, n: usize) -> Value {
        map([(self.user_class.as_str(), Value::from(n as u64))])
    }

    /// The user count a spawn message asks of this worker's class.
    fn wanted_users(&self, m: &Message) -> usize {
        if let Some(counts) = m.get("user_classes_count") {
            return map_get(counts, &self.user_class)
                .and_then(number)
                .unwrap_or(0.0)
                .max(0.0) as usize;
        }
        // Older masters send a plain count.
        m.get("num_users").and_then(number).unwrap_or(0.0).max(0.0) as usize
    }

    fn stats_message(&self, users: &Users) -> Message {
        let mut entries = stats::take_report();
        entries.push(("user_count".into(), Value::from(users.count() as u64)));
        entries.push((
            "user_classes_count".into(),
            self.class_counts(users.count()),
        ));
        self.msg("stats", map(entries))
    }

    /// Runs until the master says quit or `stop` resolves. The caller runs
    /// the class's shutdown afterwards.
    pub async fn run<T: Transport>(
        &self,
        transport: &mut T,
        stop: impl Future<Output = ()>,
    ) -> Result<Exit, TransportError> {
        stats::set_enabled(true);
        let res = self.run_inner(transport, stop).await;
        stats::set_enabled(false);
        res
    }

    async fn run_inner<T: Transport>(
        &self,
        transport: &mut T,
        stop: impl Future<Output = ()>,
    ) -> Result<Exit, TransportError> {
        tokio::pin!(stop);
        let mut users = Users {
            next_id: 0,
            running: Vec::new(),
        };
        let mut state = State::Ready;
        let mut usage = Usage::start();
        let mut heartbeat = tokio::time::interval(self.timing.heartbeat);
        let mut report = tokio::time::interval_at(
            tokio::time::Instant::now() + self.timing.stats,
            self.timing.stats,
        );
        transport
            .send(self.msg("client_ready", Value::from(CLIENT_VERSION)))
            .await?;
        tracing::info!(node_id = %self.node_id, user_class = %self.user_class, "connected to the locust master");
        loop {
            tokio::select! {
                _ = &mut stop => {
                    users.scale(0, &self.task);
                    let _ = transport.send(self.stats_message(&users)).await;
                    let _ = transport.send(self.msg("quit", Value::Nil)).await;
                    return Ok(Exit::Signal);
                }
                _ = heartbeat.tick() => {
                    let (cpu, mem) = usage.sample();
                    let data = map([
                        ("state", Value::from(state.as_str())),
                        ("current_cpu_usage", Value::from(cpu)),
                        ("current_memory_usage", Value::from(mem)),
                    ]);
                    transport.send(self.msg("heartbeat", data)).await?;
                }
                _ = report.tick() => {
                    if matches!(state, State::Spawning | State::Running) {
                        transport.send(self.stats_message(&users)).await?;
                    }
                }
                m = transport.recv() => {
                    let m = m?;
                    match m.kind.as_str() {
                        "spawn" => {
                            let n = self.wanted_users(&m);
                            let rate = m.get("spawn_rate").and_then(number).unwrap_or(0.0);
                            transport.send(self.msg("spawning", Value::Nil)).await?;
                            self.events.publish_spawn(n, rate).await;
                            users.scale(n, &self.task);
                            tracing::info!(users = n, "spawned");
                            let data = map([
                                ("user_classes_count", self.class_counts(n)),
                                ("user_count", Value::from(n as u64)),
                            ]);
                            transport.send(self.msg("spawning_complete", data)).await?;
                            state = State::Running;
                        }
                        "stop" => {
                            users.scale(0, &self.task);
                            transport.send(self.stats_message(&users)).await?;
                            transport.send(self.msg("client_stopped", Value::Nil)).await?;
                            transport.send(self.msg("client_ready", Value::from(CLIENT_VERSION))).await?;
                            state = State::Ready;
                        }
                        "reconnect" => {
                            users.scale(0, &self.task);
                            state = State::Ready;
                            transport.send(self.msg("client_ready", Value::from(CLIENT_VERSION))).await?;
                        }
                        "quit" => {
                            users.scale(0, &self.task);
                            tracing::info!("the master asked the worker to quit");
                            return Ok(Exit::MasterQuit);
                        }
                        // ack, heartbeat and the rest need no answer.
                        _ => {}
                    }
                }
            }
        }
    }
}

/// Runs one iteration of `task` without a master, for debugging a class.
pub async fn run_once(task: &Arc<dyn UserTask>) {
    task.iterate(0).await;
}

#[cfg(test)]
#[path = "runner_tests.rs"]
mod tests;

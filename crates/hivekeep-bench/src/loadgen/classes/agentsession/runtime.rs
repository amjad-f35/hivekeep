//! The per-worker state every virtual user shares: the loaded script, the
//! template memory check, each user's session, and the shutdown sweep.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use hivekeep_proto::api::v1 as api;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tonic::Request;

use super::knobs::{parse_snapshot_steps, Knobs};
use super::quantity::format_size;
use super::script::{OpKind, Script, Step};
use super::scripts::{decode_script, read_script, DEFAULT_SCRIPT};
use super::session::{SessionUser, CONTROL_RPC_TIMEOUT, TEMPLATE_HIVE, TEMPLATE_NAME};
use super::validate::budgets;
use super::AGENT_SESSION_USER_CLASS;
use crate::loadgen::dynconfig::uniform;
use crate::loadgen::metrics;
use crate::loadgen::userclass::{Config, UserTask, VuId};

/// How long a template memory refusal stands before the next start asks
/// hivekeep-api again, so a redeploy with a bigger limit is picked up
/// without restarting the workers.
pub const TEMPLATE_RECHECK_INTERVAL: Duration = Duration::from_secs(30);

/// How many sessions the shutdown sweep releases at once: a serial sweep
/// over thousands of sessions leaks most of them before the shutdown
/// budget runs out, and this many in flight does not flood hivekeep-api.
pub const SHUTDOWN_CONCURRENCY: usize = 64;

/// A resolved script with what was derived from it, so a session never
/// pairs one script's steps with another's buffer.
#[derive(Debug)]
pub struct LoadedScript {
    /// The script.
    pub script: Arc<Script>,
    /// Its steps, shared by the sessions started on it.
    pub steps: Arc<Vec<Step>>,
    /// The file path or built-in name the knobs resolved to.
    pub source: String,
    /// The content hash when loaded; with `source`, says whether the knobs
    /// still select this script.
    pub sum: [u8; 32],
    /// The shared ingest payload, sized to the largest ingest op.
    pub ingest_buf: Arc<Vec<u8>>,
}

impl LoadedScript {
    /// Wraps `script` as if loaded from `source`.
    pub fn new(script: Script, source: &str, sum: [u8; 32]) -> Self {
        let ingest_buf = Arc::new(make_ingest_buf(&script.steps));
        LoadedScript {
            steps: Arc::new(script.steps.clone()),
            script: Arc::new(script),
            source: source.to_string(),
            sum,
            ingest_buf,
        }
    }
}

/// One random payload sized to the script's largest ingest. Generating
/// bytes per op would put client-side randomness inside step timings and
/// allocate tens of MiB per user per op; the glutton only writes the
/// payload, so every session can share one.
pub fn make_ingest_buf(steps: &[Step]) -> Vec<u8> {
    let max = steps
        .iter()
        .flat_map(|s| &s.ops)
        .filter(|o| o.kind == OpKind::Ingest)
        .map(|o| o.bytes)
        .max()
        .unwrap_or(0)
        .max(0) as usize;
    let mut buf = vec![0u8; max];
    rand::Rng::fill(&mut rand::rng(), &mut buf[..]);
    buf
}

/// What the knobs select: a file wins, then a built-in name, then the
/// default.
fn script_source(k: &Knobs) -> (String, bool) {
    if !k.script_file.is_empty() {
        return (k.script_file.clone(), true);
    }
    if !k.script.is_empty() {
        return (k.script.clone(), false);
    }
    (DEFAULT_SCRIPT.to_string(), false)
}

/// The template check's cached verdict.
#[derive(Default)]
pub(super) struct TemplateCheck {
    pub(super) ok_for: Option<Arc<Script>>,
    pub(super) err: Option<String>,
    pub(super) err_at: Option<Instant>,
    pub(super) err_for: Option<Arc<Script>>,
}

/// Counts in-flight session starts, so shutdown can wait for them.
#[derive(Default)]
pub(super) struct Starting {
    count: Mutex<usize>,
    zero: Notify,
}

impl Starting {
    fn add(&self) {
        *self.count.lock() += 1;
    }

    fn done(&self) {
        let mut c = self.count.lock();
        *c = c.saturating_sub(1);
        if *c == 0 {
            self.zero.notify_waiters();
        }
    }

    async fn wait(&self) {
        loop {
            let notified = self.zero.notified();
            if *self.count.lock() == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// Why a session did not start.
#[derive(Debug)]
pub(super) enum StartError {
    /// Shutdown has begun; no session may start.
    Stopping,
    /// The start failed; the user retries next iteration.
    Failed(String),
}

/// A session shared between its user and the shutdown sweep.
pub(super) type Shared = Arc<AsyncMutex<SessionUser>>;

/// The class's task.
pub struct Runtime {
    pub(super) cfg: Arc<Config>,
    pub(super) user_class: Arc<str>,
    pub(super) users: Mutex<HashMap<VuId, Shared>>,
    /// Users that have served their phase-in wait, so a retried start does
    /// not wait twice.
    pub(super) phased_in: Mutex<HashSet<VuId>>,
    /// Set by shutdown: a user leaving its phase-in wait afterwards starts
    /// nothing, and one whose start finishes afterwards cleans up its own.
    pub(super) stopping: AtomicBool,
    /// Orders setting `stopping` against joining `starting`.
    start_mu: Mutex<()>,
    starting: Starting,
    /// The script the knobs select, reloaded when they change or the file's
    /// content changes; a running session keeps the one it started on.
    pub(super) loaded: Mutex<Option<Arc<LoadedScript>>>,
    pub(super) template: AsyncMutex<TemplateCheck>,
}

impl Runtime {
    /// A runtime over `cfg`.
    pub fn new(cfg: Arc<Config>) -> Self {
        Self::with_user_class(cfg, AGENT_SESSION_USER_CLASS)
    }

    /// A runtime whose stats rows carry `user_class` (tests keep their rows
    /// apart this way).
    pub fn with_user_class(cfg: Arc<Config>, user_class: &str) -> Self {
        Runtime {
            cfg,
            user_class: user_class.into(),
            users: Mutex::new(HashMap::new()),
            phased_in: Mutex::new(HashSet::new()),
            stopping: AtomicBool::new(false),
            start_mu: Mutex::new(()),
            starting: Starting::default(),
            loaded: Mutex::new(None),
            template: AsyncMutex::new(TemplateCheck::default()),
        }
    }

    fn knobs(&self) -> Knobs {
        self.cfg.dyn_config.get::<Knobs>()
    }

    /// Resolves the script the knobs select, loading it the first time and
    /// again whenever the knobs or the file's content change (a redeployed
    /// ConfigMap reaches a pod as a rewritten file at the same path). It
    /// cannot happen at start-up: the operator's values arrive with the
    /// first spawn message. A failed load is returned each time, so new
    /// sessions refuse loudly until the knob is fixed.
    pub fn load_script(&self) -> Result<Arc<LoadedScript>, String> {
        let mut loaded = self.loaded.lock();
        let (source, from_file) = script_source(&self.knobs());
        let data = read_script(&source, from_file).map_err(|e| e.to_string())?;
        let sum: [u8; 32] = Sha256::digest(&data).into();
        if let Some(l) = loaded.as_ref() {
            if l.source == source && l.sum == sum {
                return Ok(l.clone());
            }
        }
        let s = decode_script(&data, &source, from_file).map_err(|e| e.to_string())?;
        let b = budgets(&s.steps);
        tracing::info!(script = %s.name, %source, steps = s.steps.len(), declared_ram = %format_size(b.ram),
            declared_disk = %format_size(b.disk), min_actor_memory = %format_size(s.min_actor_memory),
            "agentsession: loaded script");
        if let Some(old) = loaded.as_ref() {
            tracing::warn!(old = %old.script.name, new = %s.name,
                "agentsession: script changed; sessions already running keep the old one");
        }
        let l = Arc::new(LoadedScript::new(s, &source, sum));
        *loaded = Some(l.clone());
        Ok(l)
    }

    /// Refuses to start sessions against a template whose memory limit is
    /// below the script's floor: a too-small Actor runs out of memory
    /// partway through and shows up as flaky steps. A template without a
    /// limit is let through with a warning. Success is cached until the
    /// script changes; a refusal for [`TEMPLATE_RECHECK_INTERVAL`].
    pub async fn check_template_memory(&self, script: &Arc<Script>) -> Result<(), String> {
        let mut t = self.template.lock().await;
        if t.ok_for.as_ref().is_some_and(|s| Arc::ptr_eq(s, script)) {
            return Ok(());
        }
        if let (Some(err), Some(at), Some(for_)) = (&t.err, t.err_at, &t.err_for) {
            if Arc::ptr_eq(for_, script) && at.elapsed() < TEMPLATE_RECHECK_INTERVAL {
                return Err(err.clone());
            }
        }
        let mut req = Request::new(api::GetActorTemplateRequest {
            actor_template: Some(api::ObjectRef {
                hive: TEMPLATE_HIVE.into(),
                name: TEMPLATE_NAME.into(),
            }),
        });
        req.set_timeout(CONTROL_RPC_TIMEOUT);
        // A failed read is no verdict: the next start asks again.
        let tmpl = self
            .cfg
            .api
            .get_actor_template(req)
            .await
            .map_err(|s| {
                format!(
                    "GetActorTemplate {TEMPLATE_HIVE}/{TEMPLATE_NAME}: {}",
                    s.message()
                )
            })?
            .into_inner();
        let limit = tmpl
            .resources
            .and_then(|r| r.limits)
            .map(|l| l.memory_bytes)
            .unwrap_or(0);
        if limit == 0 {
            tracing::warn!(template = TEMPLATE_NAME, min_actor_memory = %format_size(script.min_actor_memory),
                "agentsession: template sets no memory limit; cannot check it against the script");
        } else if limit < script.min_actor_memory {
            let m = format_size(script.min_actor_memory);
            let err = format!(
                "template {TEMPLATE_HIVE}/{TEMPLATE_NAME} memory limit {} is below script {:?} min_actor_memory {m}; redeploy the workloads with --actor-memory {m}",
                format_size(limit),
                script.name
            );
            t.err = Some(err.clone());
            t.err_at = Some(Instant::now());
            t.err_for = Some(script.clone());
            return Err(err);
        }
        t.ok_for = Some(script.clone());
        t.err = None;
        Ok(())
    }

    /// The suspended think gap before `s`: its think time scaled by the
    /// knob (0 reads as 1) with ±20% jitter, so sessions drift apart.
    pub fn think(&self, s: &Step) -> Duration {
        let scale = match self.knobs().think_scale {
            x if x <= 0.0 => 1.0,
            x => x,
        };
        let jitter = 0.8 + 0.4 * rand::random::<f64>();
        s.think.duration().mul_f64(scale * jitter)
    }

    /// Makes a user that has no session yet wait its share of the phase-in
    /// window, once: without it a fleet spawned in one ramp reaches every
    /// later step together and asks every worker for slots at once.
    pub async fn phase_in(&self, vu: VuId) {
        if !self.phased_in.lock().insert(vu) {
            return;
        }
        let d = self.phase_in_delay();
        if !d.is_zero() {
            tokio::time::sleep(d).await;
        }
    }

    /// One user's phase-in wait: uniform in `[0, window)`, or none.
    pub fn phase_in_delay(&self) -> Duration {
        let window = self.knobs().phase_in.duration();
        if window.is_zero() {
            return Duration::ZERO;
        }
        let d = uniform(Duration::ZERO, window);
        if d >= window {
            window - Duration::from_nanos(1)
        } else {
            d
        }
    }

    /// Creates the session's Actor, waits for it to serve, then parks it so
    /// step 1 starts, like every later step, from a wake. The phase-in wait
    /// comes after the preflight checks, so a refusal a redeploy fixes does
    /// not leave the users whose wait elapsed meanwhile starting together.
    /// A start past the wait counts in `starting` until the caller has
    /// stored or cleaned up the session; a failed start leaves it itself.
    pub(super) async fn start_user(
        &self,
        vu: VuId,
        loaded: &LoadedScript,
    ) -> Result<SessionUser, StartError> {
        self.check_template_memory(&loaded.script)
            .await
            .map_err(StartError::Failed)?;
        let k = self.knobs();
        check_snapshot_steps(&k, &loaded.script).map_err(StartError::Failed)?;
        self.phase_in(vu).await;
        {
            let _g = self.start_mu.lock();
            if self.stopping.load(Ordering::SeqCst) {
                return Err(StartError::Stopping);
            }
            self.starting.add();
        }
        match self.create_session(&k, loaded).await {
            Ok(u) => Ok(u),
            Err(e) => {
                self.starting.done();
                Err(StartError::Failed(e))
            }
        }
    }

    async fn create_session(
        &self,
        k: &Knobs,
        loaded: &LoadedScript,
    ) -> Result<SessionUser, String> {
        let mut u = SessionUser::new(
            self.cfg.clone(),
            self.user_class.clone(),
            format!("agent-{}", uuid::Uuid::new_v4()),
            loaded.steps.clone(),
            loaded.ingest_buf.clone(),
        );
        tracing::info!(actor = %u.actor_name, "Creating agent session");
        metrics::update_users(&self.user_class, 1.0);
        if let Err(e) = u.ensure_hive().await {
            metrics::update_users(&self.user_class, -1.0);
            return Err(format!("ensure hive: {}", e.message()));
        }
        if let Err(e) = u.create().await {
            metrics::update_users(&self.user_class, -1.0);
            return Err(format!("createActor: {}", e.message()));
        }
        if let Err(e) = u.wait_serving().await {
            // suspend_and_delete takes the user off the gauge.
            u.suspend_and_delete().await;
            return Err(format!("waitServing: {e}"));
        }
        u.awake = true;
        // Park the fresh Actor so step 1 wakes it like any other, unless the
        // session stays resident. A failed park is re-driven by step 1.
        if u.should_hibernate_at_start(k) {
            u.hibernate().await;
        }
        Ok(u)
    }

    /// One iteration for `vu`: start its session if it has none, else run
    /// its next step after the think gap. A wedged session's Actor is
    /// deleted and the next iteration starts afresh.
    pub async fn iterate_vu(&self, vu: VuId) {
        let existing = self.users.lock().get(&vu).cloned();
        let user = match existing {
            Some(u) => u,
            None => {
                let loaded = match self.load_script() {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!(err = %e, "agentsession: cannot load script; the user retries next iteration");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        return;
                    }
                };
                let u = match self.start_user(vu, &loaded).await {
                    Ok(u) => u,
                    Err(StartError::Stopping) => return,
                    Err(StartError::Failed(e)) => {
                        tracing::warn!(err = %e, "agentsession start failed; the user retries next iteration");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        return;
                    }
                };
                let shared = self
                    .users
                    .lock()
                    .entry(vu)
                    .or_insert_with(|| Arc::new(AsyncMutex::new(u)))
                    .clone();
                let stopping = self.stopping.load(Ordering::SeqCst);
                if stopping {
                    // Shutdown ran while the session started, and its sweep
                    // may or may not have seen it. Removing the entry claims
                    // the cleanup, so it happens exactly once.
                    let claimed = self.users.lock().remove(&vu);
                    if let Some(u) = claimed {
                        u.lock().await.suspend_and_delete().await;
                    }
                }
                self.starting.done();
                if stopping {
                    return;
                }
                shared
            }
        };

        let step = {
            let u = user.lock().await;
            u.steps[u.step_index].clone()
        };
        // The model is thinking: the Actor stays suspended for the gap.
        tokio::time::sleep(self.think(&step)).await;

        let mut u = user.lock().await;
        if u.cleaned_up {
            return;
        }
        if u.run_step(&step).await {
            u.step_index = (u.step_index + 1) % u.steps.len();
            if u.step_index == 0 {
                tracing::info!(actor = %u.actor_name, "agent session completed the full script; starting over");
            }
        }
        if u.broken {
            tracing::warn!(actor = %u.actor_name, consecutive_failures = u.consecutive_failures,
                "agent session wedged; deleting its actor and starting a fresh session");
            u.suspend_and_delete().await;
            drop(u);
            self.users.lock().remove(&vu);
        }
    }

    /// Suspends and deletes every session's Actor, [`SHUTDOWN_CONCURRENCY`]
    /// at a time, then waits for starts still in flight (they clean up
    /// their own sessions). Marks the runtime stopping first. The caller
    /// bounds it with a timeout.
    pub async fn shutdown_all(&self) {
        {
            let _g = self.start_mu.lock();
            self.stopping.store(true, Ordering::SeqCst);
        }
        let sessions: Vec<Shared> = self.users.lock().drain().map(|(_, u)| u).collect();
        futures::stream::iter(sessions)
            .for_each_concurrent(SHUTDOWN_CONCURRENCY, |u| async move {
                u.lock().await.suspend_and_delete().await;
            })
            .await;
        self.starting.wait().await;
    }
}

/// Refuses a snapshot step past the script's length, so a typo does not
/// silently run resident.
pub fn check_snapshot_steps(k: &Knobs, script: &Script) -> Result<(), String> {
    for step in parse_snapshot_steps(&k.snapshot_steps)? {
        if step > script.steps.len() {
            return Err(format!(
                "agentsession_snapshot_steps entry {step} exceeds script {:?} length ({} steps)",
                script.name,
                script.steps.len()
            ));
        }
    }
    Ok(())
}

#[async_trait]
impl UserTask for Runtime {
    async fn iterate(&self, vu: VuId) {
        self.iterate_vu(vu).await;
    }

    async fn shutdown(&self) {
        self.shutdown_all().await;
    }
}

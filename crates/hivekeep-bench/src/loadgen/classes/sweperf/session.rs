//! One sweperf session: a single Actor, the cycle plan it runs once, and
//! its progress through that plan, with the calls and records each cycle
//! makes.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use hivekeep_proto::api::v1 as api;
use parking_lot::Mutex;
use tonic::metadata::MetadataMap;
use tonic::{Code, Request, Response, Status};

use super::chunks::Chunk;
use super::ctx::{Ctx, Cut};
use super::events::event;
use super::knobs::Knobs;
use super::wire::{ExecuteRequest, ExecuteResponse, JobStatusResponse, StatusResponse};
use super::{
    CYCLE_CEL, DEFAULT_POLL_INTERVAL, DERIVED_METHOD, RESUME_TO_FIRST_EXEC, TASK_CEL,
    TASK_WALL_CLOCK, TEMPLATE_HIVE,
};
use crate::loadgen::dynconfig::LIFECYCLE_MODE_SYNC;
use crate::loadgen::metrics;
use crate::loadgen::telemetry::{
    elapsed_from_headers, elapsed_from_metadata, log_sampled_trace, ms_float, SOURCE_CLIENT,
    SOURCE_SERVER,
};
use crate::loadgen::trace::{inject_headers, inject_metadata};
use crate::loadgen::userclass::{Config, VuId};
use crate::wire::{SERVER_ELAPSED_HEADER, TARGET_ACTOR_HEADER};

/// How long a new Actor may take to answer `/status` with "up".
pub const LIVENESS_TIMEOUT: Duration = Duration::from_secs(60);

/// How long one cycle's background job may take.
pub const JOB_TIMEOUT: Duration = Duration::from_secs(120);

/// The `/status` poll interval from the config, or the default.
pub fn poll_interval(cfg: &Config) -> Duration {
    match cfg.dyn_config.get::<Knobs>().poll_interval_ms {
        ms if ms > 0 => Duration::from_millis(ms as u64),
        _ => DEFAULT_POLL_INTERVAL,
    }
}

/// How a cycle's sandbox work failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecError {
    /// A synchronous reply whose command exited non-zero: the resume and
    /// routing worked, so it is no ResumeToFirstExec failure.
    SyncExit(String),
    /// Anything else.
    Other(String),
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecError::SyncExit(m) => write!(f, "synchronous exec failed: {m}"),
            ExecError::Other(m) => f.write_str(m),
        }
    }
}

fn cut_message(c: Cut) -> String {
    match c {
        Cut::Canceled => "context canceled".into(),
        Cut::DeadlineExceeded => "context deadline exceeded".into(),
    }
}

/// What a session's owning user changes as it runs.
#[derive(Debug, Default)]
pub struct Progress {
    /// The cycle plan.
    pub chunks: Vec<Chunk>,
    /// The next cycle to run.
    pub cycle_index: usize,
    /// Set while the Actor still runs from the liveness poll: the next
    /// resume is a no-op and its success samples are not recorded.
    pub awake: bool,
    /// Container compute so far this task.
    pub loop_cel: Duration,
    /// Cycle wall clock so far this task.
    pub loop_wall: Duration,
    /// Whether any cycle failed this task.
    pub loop_failed: bool,
    /// From CreateActor until the sandbox answered `/status`.
    pub startup_dur: Duration,
    /// Think time between this task's cycles.
    pub loop_think: Duration,
    /// Failed resumes, each of which retries its cycle.
    pub loop_retries: u64,
    /// The slowest non-no-op ResumeActor, as recorded.
    pub resume_max: Duration,
    /// The slowest resume-to-ack; zero when no measured cycle got an ack.
    pub ttfe_max: Duration,
    /// The first failed step: resume, execute or suspend.
    pub fail_step: String,
    /// The cycle of `fail_step`.
    pub fail_cycle: usize,
}

impl Progress {
    /// A fresh plan of `chunks`.
    pub fn new(chunks: Vec<Chunk>) -> Self {
        Progress {
            chunks,
            ..Default::default()
        }
    }

    /// Whether every cycle has run.
    pub fn is_done(&self) -> bool {
        self.cycle_index >= self.chunks.len()
    }

    /// The steps the cycles cover: the last chunk's end.
    pub fn total_steps(&self) -> usize {
        self.chunks.last().map_or(0, |c| c.end)
    }

    /// Keeps the task's first failure.
    fn note_failure(&mut self, step: &str, cycle: usize) {
        if self.fail_step.is_empty() {
            self.fail_step = step.to_string();
            self.fail_cycle = cycle;
        }
    }
}

/// One benchmark session.
pub struct Session {
    cfg: Arc<Config>,
    /// The Actor's name.
    pub actor_name: String,
    /// The template it is created from.
    pub template_name: String,
    user_class: String,
    /// The user that owns the session.
    pub user_id: VuId,
    task_start: SystemTime,
    /// Claimed by whichever of the owning user or shutdown tears the Actor
    /// down first, so it is deleted once.
    pub cleaned_up: AtomicBool,
    /// The owning user's progress.
    pub progress: tokio::sync::Mutex<Progress>,
}

impl Session {
    /// A session for `actor_name` running `chunks`.
    pub fn new(
        cfg: Arc<Config>,
        actor_name: &str,
        template: &str,
        user_class: &str,
        user_id: VuId,
        chunks: Vec<Chunk>,
    ) -> Self {
        Session {
            cfg,
            actor_name: actor_name.to_string(),
            template_name: template.to_string(),
            user_class: user_class.to_string(),
            user_id,
            task_start: SystemTime::now(),
            cleaned_up: AtomicBool::new(false),
            progress: tokio::sync::Mutex::new(Progress::new(chunks)),
        }
    }

    fn reference(&self) -> Option<api::ObjectRef> {
        Some(api::ObjectRef {
            hive: self.cfg.hive.clone(),
            name: self.actor_name.clone(),
        })
    }

    /// The gate routes by this header alone, so the request's own Host
    /// stays the router's.
    fn routed(&self, b: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        b.header(
            TARGET_ACTOR_HEADER,
            format!("{}/{}", self.cfg.hive, self.actor_name),
        )
    }

    /// Creates the configured Hive; an existing one is fine, so users racing
    /// the first creation all proceed.
    pub async fn ensure_hive(&self, ctx: &Ctx) -> Result<(), Status> {
        let req = api::CreateHiveRequest {
            hive: Some(api::Hive {
                metadata: Some(api::ResourceMetadata {
                    name: self.cfg.hive.clone(),
                    ..Default::default()
                }),
            }),
        };
        let api = self.cfg.api.clone();
        self.traced_call(ctx, "CreateHive", move |md| async move {
            match api.create_hive(with_md(req, md)).await {
                Err(e) if e.code() == Code::AlreadyExists => Ok(Response::new(Default::default())),
                other => other,
            }
        })
        .await
    }

    /// Creates the Actor from its template. It is not running yet: the
    /// first request through the gate starts it.
    pub async fn create(&self, ctx: &Ctx) -> Result<(), Status> {
        let req = api::CreateActorRequest {
            actor: Some(api::Actor {
                metadata: Some(api::ResourceMetadata {
                    hive: self.cfg.hive.clone(),
                    name: self.actor_name.clone(),
                    ..Default::default()
                }),
                actor_template: Some(api::ObjectRef {
                    hive: TEMPLATE_HIVE.into(),
                    name: self.template_name.clone(),
                }),
                ..Default::default()
            }),
        };
        let api = self.cfg.api.clone();
        self.traced_call(ctx, "CreateActor", move |md| async move {
            api.create_actor(with_md(req, md)).await
        })
        .await
    }

    /// Wakes the Actor for the next cycle; returns the latency recorded on
    /// its row and whether it worked. While `awake`, a success is not
    /// recorded.
    pub async fn resume(&self, ctx: &Ctx, awake: bool) -> (Duration, bool) {
        let req = api::ResumeActorRequest {
            actor: self.reference(),
        };
        let api = self.cfg.api.clone();
        let (latency, res) = self
            .traced_call_record(ctx, "ResumeActor", !awake, move |md| async move {
                api.resume_actor(with_md(req, md)).await
            })
            .await;
        if let Err(e) = &res {
            tracing::error!(actor = %self.actor_name, err = %e.message(), "ResumeActor failed");
        }
        (latency, res.is_ok())
    }

    /// Snapshots the Actor and puts it to sleep, ending a cycle: an
    /// asynchronous suspend recorded as SuspendActorAsync, or in sync
    /// lifecycle mode a synchronous one recorded as SuspendActor.
    pub async fn suspend(&self, ctx: &Ctx) -> bool {
        let (name, mode) =
            if self.cfg.dyn_config.get::<Knobs>().lifecycle.lifecycle_mode == LIFECYCLE_MODE_SYNC {
                ("SuspendActor", api::DurabilityMode::Synchronous)
            } else {
                ("SuspendActorAsync", api::DurabilityMode::Asynchronous)
            };
        let req = api::SuspendActorRequest {
            actor: self.reference(),
            options: Some(api::SuspendOptions { mode: mode as i32 }),
        };
        let api = self.cfg.api.clone();
        let res = self
            .traced_call(ctx, name, move |md| async move {
                api.suspend_actor(with_md(req, md)).await
            })
            .await;
        if let Err(e) = &res {
            tracing::error!(actor = %self.actor_name, err = %e.message(), "{name} failed");
        }
        res.is_ok()
    }

    /// Deletes the Actor; a failure is recorded and returned for a retry.
    pub async fn delete(&self, ctx: &Ctx) -> Result<(), Status> {
        let req = api::DeleteActorRequest {
            actor: self.reference(),
            options: None,
        };
        let api = self.cfg.api.clone();
        self.traced_call(ctx, "DeleteActor", move |md| async move {
            api.delete_actor(with_md(req, md)).await
        })
        .await
    }

    /// Releases the Actor and its worker and drops the user gauge. No
    /// suspend first: DeleteActor removes an Actor in any state. Only the
    /// first call does anything.
    pub async fn teardown(&self, ctx: &Ctx) {
        if self.cleaned_up.swap(true, Ordering::SeqCst) {
            return;
        }
        let _ = self.delete(ctx).await;
        metrics::update_users(&self.user_class, -1.0);
    }

    /// Runs one control-plane call and records it on a row of its name.
    pub async fn traced_call<T, Fut>(
        &self,
        ctx: &Ctx,
        name: &str,
        rpc: impl FnOnce(MetadataMap) -> Fut,
    ) -> Result<(), Status>
    where
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        self.traced_call_record(ctx, name, true, rpc).await.1
    }

    /// [`Self::traced_call`] with optional success rows; failures are always
    /// recorded, unless shutdown cut the call short. The server's elapsed
    /// time from the trailer is preferred over the client's latency, so the
    /// figure leaves out client queueing; the client figure then goes out
    /// as a `<name>_rtt` row. Returns the latency it records.
    pub async fn traced_call_record<T, Fut>(
        &self,
        ctx: &Ctx,
        name: &str,
        record_success: bool,
        rpc: impl FnOnce(MetadataMap) -> Fut,
    ) -> (Duration, Result<(), Status>)
    where
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        let mut span = self.cfg.tracer.start(name);
        let mut md = MetadataMap::new();
        inject_metadata(span.context(), &mut md);
        let start = Instant::now();
        let res = ctx.run(rpc(md)).await.unwrap_or_else(|c| Err(c.status()));
        let client = start.elapsed();
        let (res, trailer) = match res {
            Ok(r) => (Ok(()), r.metadata().clone()),
            Err(e) => {
                let md = e.metadata().clone();
                (Err(e), md)
            }
        };
        if res.is_err() && ctx.is_canceled() {
            return (client, res);
        }
        let (latency, source) = elapsed_from_metadata(&trailer, SERVER_ELAPSED_HEADER, client);
        if source == SOURCE_SERVER {
            span.set_attribute("server.elapsed_ms", ms_float(latency));
            if res.is_err() || record_success {
                self.record_client_rtt(name, client, res.as_ref().err());
            }
        }
        let err = res.as_ref().err().map(|e| e.message().to_string());
        log_sampled_trace(span.context(), name, latency, source, err.as_deref());
        span.end();
        match &err {
            Some(e) => metrics::record_failure("grpc", name, &self.user_class, latency, e),
            None if record_success => {
                metrics::record_success("grpc", name, &self.user_class, latency, 0)
            }
            None => {}
        }
        (latency, res)
    }

    fn record_client_rtt(&self, name: &str, client: Duration, err: Option<&Status>) {
        let row = format!("{name}_rtt");
        match err {
            Some(e) => metrics::record_failure("grpc", &row, &self.user_class, client, e.message()),
            None => metrics::record_success("grpc", &row, &self.user_class, client, 0),
        }
    }

    /// Waits up to [`LIVENESS_TIMEOUT`] for the sandbox to answer `/status`
    /// with "up". The first request through the gate also wakes a new
    /// Actor, so this doubles as its implicit resume.
    pub async fn poll_liveness(&self, ctx: &Ctx) -> Result<(), String> {
        let url = format!("{}/status", self.cfg.router_url);
        let retry = poll_interval(&self.cfg);
        let deadline = Instant::now() + LIVENESS_TIMEOUT;
        let mut attempt = 0u64;
        while Instant::now() < deadline {
            let req = self.routed(self.cfg.http.get(&url));
            let up = ctx
                .run(async {
                    let resp = req.send().await.ok()?;
                    if resp.status() != reqwest::StatusCode::OK {
                        return None;
                    }
                    let body = resp.bytes().await.ok()?;
                    let st: StatusResponse = serde_json::from_slice(&body).ok()?;
                    Some(st.status.eq_ignore_ascii_case("up"))
                })
                .await
                .map_err(cut_message)?;
            if up == Some(true) {
                tracing::info!(actor = %self.actor_name, attempt, "Server is up and ready");
                return Ok(());
            }
            ctx.sleep(retry).await.map_err(cut_message)?;
            attempt += 1;
        }
        Err(format!(
            "failed to verify server liveness for actor {}",
            self.actor_name
        ))
    }

    /// Polls `/status?job_id=` every poll interval, starting one interval
    /// in, until the job ends, for at most [`JOB_TIMEOUT`]. Returns the
    /// chunk's reported in-container time; a failed job, a non-zero exit or
    /// the timeout is an error.
    pub async fn poll_job_completion(
        &self,
        ctx: &Ctx,
        job_id: &str,
        cycle: usize,
    ) -> Result<Duration, ExecError> {
        let retry = poll_interval(&self.cfg);
        let poll = ctx.with_timeout(JOB_TIMEOUT);
        let url = format!("{}/status?job_id={}", self.cfg.router_url, job_id);
        loop {
            if let Err(c) = poll.sleep(retry).await {
                return Err(match (c, ctx.is_canceled()) {
                    (_, true) | (Cut::Canceled, _) => ExecError::Other(cut_message(Cut::Canceled)),
                    _ => ExecError::Other(format!(
                        "timed out waiting for job {job_id} to complete in cycle {cycle}"
                    )),
                });
            }
            let req = self.routed(self.cfg.http.get(&url));
            let job = match poll
                .run(async {
                    let resp = req.send().await.ok()?;
                    if resp.status() != reqwest::StatusCode::OK {
                        return None;
                    }
                    let body = resp.bytes().await.ok()?;
                    serde_json::from_slice::<JobStatusResponse>(&body).ok()
                })
                .await
            {
                Ok(Some(j)) => j,
                Ok(None) => continue,
                // The next sleep reports how the poll ended.
                Err(_) => continue,
            };
            if job.status.eq_ignore_ascii_case("COMPLETED") {
                if let Some(code) = job.exit_code.filter(|c| *c != 0) {
                    return Err(ExecError::Other(format!(
                        "cycle {cycle} job {job_id} failed with exit code {code}"
                    )));
                }
                return Ok(Duration::from_secs_f64(
                    job.execution_duration_ms.max(0.0) / 1e3,
                ));
            }
            if job.status.eq_ignore_ascii_case("FAILED") {
                return Err(ExecError::Other(format!(
                    "cycle {cycle} job {job_id} failed with exit code {}: {}",
                    job.exit_code.unwrap_or(-1),
                    job.error
                )));
            }
        }
    }

    /// Runs one cycle's steps in the sandbox, timed as
    /// `Workload_Cycle_<cycle>`. A job ID in the reply means the run is
    /// asynchronous and is polled to its end; an exit code alone means it
    /// already finished. Returns when an asynchronous job was accepted (if
    /// one was), the chunk's in-container time (zero when unreported), and
    /// the outcome.
    pub async fn execute(
        &self,
        ctx: &Ctx,
        cycle: usize,
        start: usize,
        end: usize,
    ) -> (Option<Instant>, Duration, Result<(), ExecError>) {
        let metric = format!("Workload_Cycle_{cycle}");
        let body = serde_json::to_vec(&ExecuteRequest {
            start_step: start + 1,
            end_step: end,
        })
        .unwrap_or_default();
        let ack = Mutex::new(None);
        let dur = Mutex::new(Duration::ZERO);
        let (ack_ref, dur_ref) = (&ack, &dur);
        let res = self
            .http_json_call(ctx, &metric, "/execute", body, |bytes| async move {
                let resp: ExecuteResponse = serde_json::from_slice(&bytes)
                    .map_err(|e| ExecError::Other(format!("unmarshal executeResponse: {e}")))?;
                if !resp.job_id.is_empty() {
                    // A synchronous reply only lands after the chunk ran, so
                    // only this is an acknowledgement.
                    *ack_ref.lock() = Some(Instant::now());
                    *dur_ref.lock() = self.poll_job_completion(ctx, &resp.job_id, cycle).await?;
                    return Ok(());
                }
                if resp.exit_code != 0 {
                    return Err(ExecError::SyncExit(format!(
                        "cycle {cycle} failed: exit code {}, stderr: {}",
                        resp.exit_code, resp.stderr
                    )));
                }
                Ok(())
            })
            .await;
        let ack_at = *ack.lock();
        let exec = *dur.lock();
        (ack_at, exec, res)
    }

    /// Posts `body` to `route` on the sandbox and records the outcome under
    /// `metric`; `validate` judges the reply. A call shutdown cut short
    /// records no failure.
    async fn http_json_call<Fut>(
        &self,
        ctx: &Ctx,
        metric: &str,
        route: &str,
        body: Vec<u8>,
        validate: impl FnOnce(Vec<u8>) -> Fut,
    ) -> Result<(), ExecError>
    where
        Fut: Future<Output = Result<(), ExecError>>,
    {
        let mut span = self.cfg.tracer.start(metric);
        let record_failure = |latency: Duration, msg: &str| {
            if !ctx.is_canceled() {
                metrics::record_failure("http", metric, &self.user_class, latency, msg);
            }
        };
        let mut headers = http::HeaderMap::new();
        inject_headers(span.context(), &mut headers);
        let req = self
            .routed(
                self.cfg
                    .http
                    .post(format!("{}{route}", self.cfg.router_url)),
            )
            .headers(headers)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(body);
        let start = Instant::now();
        let sent = ctx.run(req.send()).await;
        let client = start.elapsed();
        let resp = match sent {
            Err(c) => {
                record_failure(client, &cut_message(c));
                return Err(ExecError::Other(cut_message(c)));
            }
            Ok(Err(e)) => {
                record_failure(client, &e.to_string());
                return Err(ExecError::Other(e.to_string()));
            }
            Ok(Ok(r)) => r,
        };
        let status = resp.status();
        let (server, source) = elapsed_from_headers(resp.headers(), SERVER_ELAPSED_HEADER, client);
        if source == SOURCE_SERVER {
            span.set_attribute("server.elapsed_ms", ms_float(server));
        }
        let bytes = match ctx.run(resp.bytes()).await {
            Ok(Ok(b)) => b.to_vec(),
            Ok(Err(e)) => {
                record_failure(client, &e.to_string());
                return Err(ExecError::Other(e.to_string()));
            }
            Err(c) => {
                record_failure(client, &cut_message(c));
                return Err(ExecError::Other(cut_message(c)));
            }
        };
        if status.as_u16() >= 400 {
            let msg = format!(
                "HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes).trim()
            );
            log_sampled_trace(span.context(), metric, client, SOURCE_CLIENT, Some(&msg));
            record_failure(client, &msg);
            return Err(ExecError::Other(msg));
        }
        let len = bytes.len() as i64;
        if let Err(e) = validate(bytes).await {
            let elapsed = start.elapsed();
            log_sampled_trace(
                span.context(),
                metric,
                elapsed,
                SOURCE_CLIENT,
                Some(&e.to_string()),
            );
            record_failure(elapsed, &e.to_string());
            span.record_error(&e);
            span.end();
            return Err(e);
        }
        let total = start.elapsed();
        log_sampled_trace(span.context(), metric, total, SOURCE_CLIENT, None);
        metrics::record_success("http", metric, &self.user_class, total, len);
        span.end();
        Ok(())
    }

    /// Runs one cycle: resume, replay the cycle's steps, suspend. The final
    /// cycle skips the suspend, as the Actor is deleted straight after. A
    /// cycle shutdown cuts short returns without recording or advancing.
    pub async fn step(&self, ctx: &Ctx, p: &mut Progress) {
        if p.is_done() {
            return;
        }
        let cycle = p.cycle_index + 1;
        let chunk = p.chunks[p.cycle_index];
        let n_cycles = p.chunks.len();
        tracing::info!(
            actor = %self.actor_name,
            cycle,
            start_step = chunk.start + 1,
            end_step = chunk.end,
            "Starting sweperf cycle"
        );

        // 1. Resume.
        let noop_resume = p.awake;
        let resume_start = Instant::now();
        let (resume_latency, resumed) = self.resume(ctx, noop_resume).await;
        if !resumed {
            if ctx.is_canceled() {
                p.loop_wall += resume_start.elapsed();
                return;
            }
            p.loop_failed = true;
            p.loop_retries += 1;
            p.note_failure("resume", cycle);
            metrics::record_failure(
                DERIVED_METHOD,
                RESUME_TO_FIRST_EXEC,
                &self.user_class,
                resume_start.elapsed(),
                "ResumeActor failed",
            );
            event!(
                error,
                "ResumeActor failed in step",
                actor = self.actor_name.as_str(),
                cycle = cycle as u64,
                wall_ms = ms_float(resume_start.elapsed()),
            );
            p.loop_wall += resume_start.elapsed();
            return;
        }
        p.awake = false;
        if !noop_resume {
            p.resume_max = p.resume_max.max(resume_latency);
        }

        // 2. Replay the cycle's steps in the sandbox.
        let (ack_at, exec_dur, res) = self.execute(ctx, cycle, chunk.start, chunk.end).await;
        if res.is_err() && ctx.is_canceled() {
            p.loop_wall += resume_start.elapsed();
            return;
        }
        let cycle_cel = match &res {
            Err(e) => {
                p.loop_failed = true;
                p.note_failure("execute", cycle);
                tracing::error!(actor = %self.actor_name, cycle, err = %e, "execute steps failed");
                Duration::ZERO
            }
            Ok(()) => {
                p.loop_cel += exec_dur;
                exec_dur
            }
        };
        let mut ttfe = Duration::ZERO;
        if let (false, Some(at)) = (noop_resume, ack_at) {
            ttfe = at.duration_since(resume_start);
            p.ttfe_max = p.ttfe_max.max(ttfe);
        }
        // A no-op resume would record a near-zero sample; failures count.
        if !noop_resume || ack_at.is_none() {
            self.record_resume_to_first_exec(resume_start, ack_at, res.as_ref().err());
        }
        self.record_cycle_cel(exec_dur, res.as_ref().err());

        // 3. Suspend, unless this was the last cycle.
        let mut suspended = true;
        if cycle < n_cycles {
            suspended = self.suspend(ctx).await;
            if !suspended {
                if ctx.is_canceled() {
                    p.loop_wall += resume_start.elapsed();
                    return;
                }
                p.loop_failed = true;
                p.note_failure("suspend", cycle);
            }
        }

        // A no-op resume and a reply without an acknowledgement have
        // nothing to report on those fields.
        event!(
            info,
            "Completed sweperf cycle",
            actor = self.actor_name.as_str(),
            cycle = cycle as u64,
            cel_ms = ms_float(cycle_cel),
            wall_ms = ms_float(resume_start.elapsed()),
            ok = res.is_ok() && suspended,
            resume_ms = (!noop_resume).then(|| ms_float(resume_latency)),
            ttfe_ms = (!ttfe.is_zero()).then(|| ms_float(ttfe)),
        );
        p.loop_wall += resume_start.elapsed();
        p.cycle_index += 1;
    }

    /// Times the resume through to the sandbox accepting the chunk. An
    /// acknowledgement is a success even when the chunk fails later, as the
    /// resume itself worked; a synchronous reply has none and is skipped.
    fn record_resume_to_first_exec(
        &self,
        resume_start: Instant,
        ack_at: Option<Instant>,
        err: Option<&ExecError>,
    ) {
        match (ack_at, err) {
            (Some(at), _) => metrics::record_success(
                DERIVED_METHOD,
                RESUME_TO_FIRST_EXEC,
                &self.user_class,
                at.duration_since(resume_start),
                0,
            ),
            (None, Some(ExecError::SyncExit(_))) | (None, None) => {}
            (None, Some(e)) => metrics::record_failure(
                DERIVED_METHOD,
                RESUME_TO_FIRST_EXEC,
                &self.user_class,
                resume_start.elapsed(),
                &e.to_string(),
            ),
        }
    }

    /// Reports one chunk's in-container command time; a synchronous reply
    /// carries none, so there is nothing to record.
    fn record_cycle_cel(&self, exec_dur: Duration, err: Option<&ExecError>) {
        match err {
            Some(e) => metrics::record_failure(
                DERIVED_METHOD,
                CYCLE_CEL,
                &self.user_class,
                exec_dur,
                &e.to_string(),
            ),
            None if !exec_dur.is_zero() => {
                metrics::record_success(DERIVED_METHOD, CYCLE_CEL, &self.user_class, exec_dur, 0)
            }
            None => {}
        }
    }

    /// Reports the finished trajectory's compute and wall clock, logs its
    /// task record, and rearms the loop counters.
    pub fn record_task_metrics(&self, p: &mut Progress) {
        self.log_task_record(p);
        let class = self.user_class.as_str();
        if p.loop_failed {
            metrics::record_failure(
                DERIVED_METHOD,
                TASK_CEL,
                class,
                p.loop_cel,
                "cycle failure in trajectory",
            );
            metrics::record_failure(
                DERIVED_METHOD,
                TASK_WALL_CLOCK,
                class,
                p.loop_wall,
                "cycle failure in trajectory",
            );
        } else {
            metrics::record_success(DERIVED_METHOD, TASK_CEL, class, p.loop_cel, 0);
            metrics::record_success(DERIVED_METHOD, TASK_WALL_CLOCK, class, p.loop_wall, 0);
        }
        p.loop_cel = Duration::ZERO;
        p.loop_wall = Duration::ZERO;
        p.loop_failed = false;
    }

    /// One line of facts about the finished task, for a separate script to
    /// judge. The counts are the ones resolved for this task, so records
    /// from before and after a config change stay distinguishable.
    fn log_task_record(&self, p: &Progress) {
        let (start, now) = (rfc3339(self.task_start), rfc3339(SystemTime::now()));
        event!(
            info,
            "sweperf task record",
            v = 1u64,
            user = self.user_id,
            actor = self.actor_name.as_str(),
            template = self.template_name.as_str(),
            cycles = p.chunks.len() as u64,
            steps = p.total_steps() as u64,
            task_start = start.as_str(),
            end = now.as_str(),
            startup_ms = ms_float(p.startup_dur),
            cel_ms = ms_float(p.loop_cel),
            wall_ms = ms_float(p.loop_wall),
            think_ms = ms_float(p.loop_think),
            resume_max_ms = ms_float(p.resume_max),
            retries = p.loop_retries,
            failed = p.loop_failed,
            fail_step = p.fail_step.as_str(),
            fail_cycle = p.fail_cycle as u64,
            ttfe_max_ms = (!p.ttfe_max.is_zero()).then(|| ms_float(p.ttfe_max)),
        );
    }
}

fn rfc3339(t: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

/// `msg` as a request carrying `md`.
fn with_md<T>(msg: T, md: MetadataMap) -> Request<T> {
    let mut req = Request::new(msg);
    *req.metadata_mut() = md;
    req
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;

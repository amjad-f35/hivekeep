//! One agent session: a single Actor and its progress through the script.

use std::future::Future;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use hivekeep_proto::api::v1 as api;
use prometheus::{exponential_buckets, Histogram, HistogramOpts};
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::{Code, Request, Status};

use super::errors::{classify, is_capacity_error, retry_while_full, HttpError, OpError};
use super::knobs::{parse_snapshot_steps, Knobs};
use super::script::{Op, OpKind, Step};
use crate::glutton::{
    BURN_CPU_ROUTE, INGEST_ROUTE, PING_ROUTE, READ_DISK_ROUTE, READ_RAM_ROUTE, WRITE_DISK_ROUTE,
    WRITE_RAM_ROUTE,
};
use crate::loadgen::dynconfig::{LIFECYCLE_MODE_NONE, LIFECYCLE_MODE_SYNC, RESUME_MODE_EXPLICIT};
use crate::loadgen::lifecycle::{is_crashed, retry_on_conflict, FailureAction};
use crate::loadgen::metrics;
use crate::loadgen::telemetry::{elapsed_from_metadata, log_sampled_trace};
use crate::loadgen::trace::{inject_metadata, SpanContext};
use crate::loadgen::userclass::Config;
use crate::pb::glutton::v1 as pb;
use crate::wire::{SERVER_ELAPSED_HEADER, TARGET_ACTOR_HEADER};

/// The Hive holding the benchmark ActorTemplates.
pub const TEMPLATE_HIVE: &str = "benchmark-workloads";
/// The template instantiated per session: the stock glutton Actor.
pub const TEMPLATE_NAME: &str = "glutton";
/// Bounds every control-plane call, so a server-side hang in a suspend or
/// delete cannot park a user for the rest of the run.
pub const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(60);
/// Failures in a row that count toward replacement before a session's
/// Actor is deleted and recreated.
pub const MAX_CONSECUTIVE_STEP_FAILURES: u32 = 3;

/// BurnCPU's per-thread hash rate. A burn runs for a fixed wall-clock time,
/// so a starved sandbox shows here while `Step_*` latency stays flat.
static BURN_RATE: LazyLock<Histogram> = LazyLock::new(|| {
    let h = Histogram::with_opts(
        HistogramOpts::new(
            "agentsession_burn_cpu_iterations_per_second",
            "BurnCPU sha256 iterations per second per thread; drops under CPU contention while Step_* latency stays flat.",
        )
        .buckets(exponential_buckets(256.0, 2.0, 16).unwrap()),
    )
    .unwrap();
    metrics::register(Box::new(h.clone()));
    h
});

/// A session, owned by one virtual user.
pub struct SessionUser {
    pub(super) cfg: Arc<Config>,
    /// The `user_class` label every stats row carries.
    pub(super) user_class: Arc<str>,
    pub(super) actor_name: String,
    pub(super) step_index: usize,
    pub(super) cleaned_up: bool,
    /// The script this session started on; a knob change does not move a
    /// running session to another script.
    pub(super) steps: Arc<Vec<Step>>,
    /// The shared read-only ingest payload.
    pub(super) ingest_buf: Arc<Vec<u8>>,
    /// Whether the Actor is resident rather than suspended.
    pub(super) awake: bool,
    /// Set by a failed suspend: the Actor is stranded RUNNING or
    /// SUSPENDING, and waking it would misreport the wake, so the next
    /// step finishes the suspend first.
    pub(super) hibernate_pending: bool,
    /// Failures counting toward replacement since the last good step.
    pub(super) consecutive_failures: u32,
    /// The Actor should be replaced: a CRASHED or stuck Actor never
    /// recovers and would wedge its user for the rest of the run.
    pub(super) broken: bool,
}

impl SessionUser {
    /// A session for `actor_name` running `steps`.
    pub fn new(
        cfg: Arc<Config>,
        user_class: Arc<str>,
        actor_name: String,
        steps: Arc<Vec<Step>>,
        ingest_buf: Arc<Vec<u8>>,
    ) -> Self {
        SessionUser {
            cfg,
            user_class,
            actor_name,
            step_index: 0,
            cleaned_up: false,
            steps,
            ingest_buf,
            awake: false,
            hibernate_pending: false,
            consecutive_failures: 0,
            broken: false,
        }
    }

    fn knobs(&self) -> Knobs {
        self.cfg.dyn_config.get::<Knobs>()
    }

    /// Classifies a failure and marks the Actor broken when replacing it is
    /// warranted. Cluster-wide errors (no capacity, a restarting API
    /// server) do not count: the replacement would need the very room the
    /// error reported missing.
    pub(super) fn note_failure(&mut self, err: &OpError) {
        match classify(err) {
            FailureAction::ReplaceNow => self.broken = true,
            FailureAction::RetryLater => {}
            FailureAction::ReplaceIfPersistent => {
                self.consecutive_failures += 1;
                if self.consecutive_failures >= MAX_CONSECUTIVE_STEP_FAILURES {
                    self.broken = true;
                }
            }
        }
    }

    pub(super) fn actor_ref(&self) -> api::ObjectRef {
        api::ObjectRef {
            hive: self.cfg.hive.clone(),
            name: self.actor_name.clone(),
        }
    }

    /// Whether a fresh session is parked before step 1: not when resident
    /// (lifecycle `none`) or when only chosen steps snapshot.
    pub(super) fn should_hibernate_at_start(&self, k: &Knobs) -> bool {
        if k.lifecycle.lifecycle_mode == LIFECYCLE_MODE_NONE {
            return false;
        }
        parse_snapshot_steps(&k.snapshot_steps)
            .map(|s| s.is_empty())
            .unwrap_or(true)
    }

    /// Whether the Actor is parked after the current step (1-based
    /// `step_index + 1`).
    pub(super) fn should_hibernate_after_step(&self, k: &Knobs) -> bool {
        if k.lifecycle.lifecycle_mode == LIFECYCLE_MODE_NONE {
            return false;
        }
        match parse_snapshot_steps(&k.snapshot_steps) {
            Ok(steps) if !steps.is_empty() => steps.contains(&(self.step_index + 1)),
            _ => true,
        }
    }

    /// Wakes the Actor, runs the step's ops in order and suspends it again,
    /// reporting whether everything succeeded. The wake is measured by a
    /// ping sent ahead of the ops and booked as `WakeFirstTouch`; in
    /// implicit resume mode that ping is also what wakes the parked Actor,
    /// so the row measures the wake alone.
    pub async fn run_step(&mut self, step: &Step) -> bool {
        // A failed suspend left the Actor awake. Finish the suspend and
        // pick the step up next iteration.
        if self.hibernate_pending {
            self.hibernate().await;
            return false;
        }
        let k = self.knobs();
        let was_parked = !self.awake && k.lifecycle.lifecycle_mode != LIFECYCLE_MODE_NONE;
        // A parked Actor's wake, from the first attempt to the ping that
        // finds it serving, is one TimeToWake sample; a fleet without room
        // is retried within the budget, so waiting for a slot shows up as
        // latency.
        let wake_start = Instant::now();
        let deadline = tokio::time::Instant::now() + k.resume_retry_budget.duration();
        if was_parked && k.lifecycle.resume_mode == RESUME_MODE_EXPLICIT {
            if let Err(e) = retry_while_full(deadline, || self.resume()).await {
                self.note_failure(&e);
                self.record_time_to_wake(wake_start, Some(&e));
                if is_capacity_error(Some(&e)) {
                    tracing::warn!(actor = %self.actor_name, step = %step.name, waited = ?wake_start.elapsed(), err = %e,
                        "agent session found no room within the retry budget");
                }
                return false;
            }
        }
        let res = if was_parked {
            retry_while_full(deadline, || self.wake_ping()).await
        } else {
            self.exec_op(&Op::ping()).await
        };
        if let Err(e) = res {
            self.note_failure(&e);
            if was_parked {
                self.record_time_to_wake(wake_start, Some(&e));
            }
            tracing::warn!(actor = %self.actor_name, step = %step.name, waited = ?wake_start.elapsed(), err = %e,
                "agent session wake failed");
            if was_parked || self.should_hibernate_after_step(&k) {
                self.hibernate().await;
            }
            return false;
        }
        self.awake = true;
        if was_parked {
            self.record_time_to_wake(wake_start, None);
        }

        let row = format!("Step_{}", step.name);
        let step_start = Instant::now();
        for (i, o) in step.ops.iter().enumerate() {
            if let Err(e) = self.exec_op(o).await {
                self.note_failure(&e);
                metrics::record_failure(
                    "http",
                    &row,
                    &self.user_class,
                    step_start.elapsed(),
                    &e.to_string(),
                );
                tracing::warn!(actor = %self.actor_name, step = %step.name, op = i, err = %e, "agent session step failed");
                if was_parked || self.should_hibernate_after_step(&k) {
                    self.hibernate().await;
                }
                return false;
            }
        }
        self.consecutive_failures = 0;
        metrics::record_success("http", &row, &self.user_class, step_start.elapsed(), 0);
        if self.should_hibernate_after_step(&k) {
            self.hibernate().await;
        }
        true
    }

    /// The ping measuring a parked Actor's wake, booked as `WakeFirstTouch`.
    async fn wake_ping(&self) -> Result<(), OpError> {
        let start = Instant::now();
        match self.exec_op(&Op::ping()).await {
            Err(e) => {
                metrics::record_failure(
                    "http",
                    "WakeFirstTouch",
                    &self.user_class,
                    start.elapsed(),
                    &e.to_string(),
                );
                Err(e)
            }
            Ok(()) => {
                metrics::record_success(
                    "http",
                    "WakeFirstTouch",
                    &self.user_class,
                    start.elapsed(),
                    0,
                );
                Ok(())
            }
        }
    }

    fn record_time_to_wake(&self, start: Instant, err: Option<&OpError>) {
        match err {
            Some(e) => metrics::record_failure(
                "summary",
                "TimeToWake",
                &self.user_class,
                start.elapsed(),
                &e.to_string(),
            ),
            None => metrics::record_success(
                "summary",
                "TimeToWake",
                &self.user_class,
                start.elapsed(),
                0,
            ),
        }
    }

    /// Performs one op as a glutton request through the router.
    pub async fn exec_op(&self, o: &Op) -> Result<(), OpError> {
        match o.kind {
            OpKind::Ingest => {
                let n = o.bytes.max(0) as usize;
                // Callers that built a session by hand may have no buffer.
                let payload = if self.ingest_buf.len() >= n {
                    self.ingest_buf[..n].to_vec()
                } else {
                    vec![0u8; n]
                };
                self.post::<pb::IngestResponse>(
                    INGEST_ROUTE,
                    &pb::IngestRequest {
                        key: o.key.clone(),
                        payload,
                        append: false,
                    },
                )
                .await
                .map(drop)
            }
            OpKind::BurnCpu => {
                let resp: pb::BurnCpuResponse = self
                    .post(
                        BURN_CPU_ROUTE,
                        &pb::BurnCpuRequest {
                            duration_ms: o.millis,
                            parallelism: o.parallel,
                        },
                    )
                    .await?;
                if let Some(rate) = burn_rate_per_thread(o, resp.iterations) {
                    BURN_RATE.observe(rate);
                }
                Ok(())
            }
            OpKind::WriteDisk => self
                .post::<pb::WriteDiskResponse>(
                    WRITE_DISK_ROUTE,
                    &pb::WriteDiskRequest {
                        key: o.key.clone(),
                        size: o.bytes as i32,
                        write_mode: pb::WriteMode::Truncate as i32,
                    },
                )
                .await
                .map(drop),
            OpKind::ReadDiskDigest | OpKind::ReadDiskData => {
                let mode = if o.kind == OpKind::ReadDiskData {
                    pb::ReadMode::Data
                } else {
                    pb::ReadMode::DigestOnly
                };
                self.post::<pb::ReadDiskResponse>(
                    READ_DISK_ROUTE,
                    &pb::ReadDiskRequest {
                        key: o.key.clone(),
                        read_mode: mode as i32,
                    },
                )
                .await
                .map(drop)
            }
            // OVERWRITE grows the array on first touch and re-randomizes it
            // in place afterwards; TRUNCATE would reallocate and briefly
            // double the guest heap, which can exhaust a tight sandbox.
            OpKind::FillRam | OpKind::ChurnRam => {
                let mode = if o.kind == OpKind::FillRam {
                    pb::WriteMode::Overwrite
                } else {
                    pb::WriteMode::OverwriteRotate
                };
                self.post::<pb::WriteRamResponse>(
                    WRITE_RAM_ROUTE,
                    &pb::WriteRamRequest {
                        key: o.key.clone(),
                        size: o.bytes.to_string(),
                        write_mode: mode as i32,
                    },
                )
                .await
                .map(drop)
            }
            OpKind::WalkRam => self
                .post::<pb::ReadRamResponse>(
                    READ_RAM_ROUTE,
                    &pb::ReadRamRequest {
                        key: o.key.clone(),
                        size: String::new(),
                    },
                )
                .await
                .map(drop),
            OpKind::Ping => self
                .post::<pb::PingResponse>(
                    PING_ROUTE,
                    &pb::PingRequest {
                        message: "think".into(),
                    },
                )
                .await
                .map(drop),
            // No request: the Actor stays resident while the driver waits,
            // the way an agent sits in a model round trip.
            OpKind::Dwell => {
                tokio::time::sleep(Duration::from_millis(o.millis.max(0) as u64)).await;
                Ok(())
            }
        }
    }

    /// POSTs `req` to the Actor's `route` through the router; an HTTP status
    /// of 400 or more is an error carrying the body.
    async fn post<R: Message + Default>(
        &self,
        route: &str,
        req: &impl Message,
    ) -> Result<R, OpError> {
        let resp = self
            .cfg
            .http
            .post(format!("{}{}", self.cfg.router_url, route))
            .header(
                TARGET_ACTOR_HEADER,
                format!("{}/{}", self.cfg.hive, self.actor_name),
            )
            .header(http::header::CONTENT_TYPE, "application/x-protobuf")
            .body(req.encode_to_vec())
            .send()
            .await
            .map_err(|e| OpError::Other(format!("POST {route}: {e}")))?;
        let status = resp.status().as_u16();
        let body = resp
            .bytes()
            .await
            .map_err(|e| OpError::Other(format!("read {route}: {e}")))?;
        if status >= 400 {
            return Err(OpError::Http(HttpError {
                route: route.to_string(),
                status,
                body: String::from_utf8_lossy(&body).trim().to_string(),
            }));
        }
        R::decode(body).map_err(|e| OpError::Other(format!("decode {route}: {e}")))
    }

    /// A control request carrying the span's trace context and the RPC
    /// deadline.
    fn request<T>(msg: T, span: &SpanContext) -> Request<T> {
        let mut req = Request::new(msg);
        req.set_timeout(CONTROL_RPC_TIMEOUT);
        inject_metadata(span, req.metadata_mut());
        req
    }

    /// Runs one control call under a span and the RPC deadline and books it
    /// as a stats row of the same name, preferring the server's elapsed
    /// time from the response metadata for the trace line.
    async fn traced<F, Fut>(&self, name: &str, call: F) -> Result<(), Status>
    where
        F: FnOnce(SpanContext) -> Fut,
        Fut: Future<Output = Result<MetadataMap, Status>>,
    {
        let span = self.cfg.tracer.start(name);
        let ctx = *span.context();
        let start = Instant::now();
        let res = match tokio::time::timeout(CONTROL_RPC_TIMEOUT, call(ctx)).await {
            Ok(r) => r,
            Err(_) => Err(Status::deadline_exceeded(format!(
                "{name}: no answer within {CONTROL_RPC_TIMEOUT:?}"
            ))),
        };
        let latency = start.elapsed();
        let empty = MetadataMap::new();
        let md = match &res {
            Ok(md) => md,
            Err(s) => s.metadata(),
        };
        let md = if md.is_empty() { &empty } else { md };
        let (server, source) = elapsed_from_metadata(md, SERVER_ELAPSED_HEADER, latency);
        let err_text = res.as_ref().err().map(|s| s.message().to_string());
        log_sampled_trace(&ctx, name, server, source, err_text.as_deref());
        span.end();
        match res {
            Err(s) => {
                metrics::record_failure(
                    "grpc",
                    name,
                    &self.user_class,
                    latency,
                    &OpError::Status(s.clone()).to_string(),
                );
                Err(s)
            }
            Ok(_) => {
                metrics::record_success("grpc", name, &self.user_class, latency, 0);
                Ok(())
            }
        }
    }

    /// Creates the configured Hive; `ALREADY_EXISTS` is success, so sessions
    /// racing the first creation all proceed.
    pub(super) async fn ensure_hive(&self) -> Result<(), Status> {
        let api = self.cfg.api.clone();
        let hive = self.cfg.hive.clone();
        self.traced("CreateHive", |span| async move {
            let req = Self::request(
                api::CreateHiveRequest {
                    hive: Some(api::Hive {
                        metadata: Some(api::ResourceMetadata {
                            name: hive,
                            ..Default::default()
                        }),
                    }),
                },
                &span,
            );
            match api.create_hive(req).await {
                Ok(r) => Ok(r.metadata().clone()),
                Err(s) if s.code() == Code::AlreadyExists => Ok(s.metadata().clone()),
                Err(s) => Err(s),
            }
        })
        .await
    }

    /// Creates the session's Actor from the glutton template.
    pub(super) async fn create(&self) -> Result<(), Status> {
        let api = self.cfg.api.clone();
        let actor = api::Actor {
            metadata: Some(api::ResourceMetadata {
                hive: self.cfg.hive.clone(),
                name: self.actor_name.clone(),
                ..Default::default()
            }),
            actor_template: Some(api::ObjectRef {
                hive: TEMPLATE_HIVE.into(),
                name: TEMPLATE_NAME.into(),
            }),
            ..Default::default()
        };
        self.traced("CreateActor", |span| async move {
            let req = Self::request(api::CreateActorRequest { actor: Some(actor) }, &span);
            api.create_actor(req).await.map(|r| r.metadata().clone())
        })
        .await
    }

    /// Pings the Actor until the glutton answers. The first request is also
    /// what activates a new Actor.
    pub(super) async fn wait_serving(&self) -> Result<(), OpError> {
        const MAX_RETRIES: usize = 30;
        const RETRY_INTERVAL: Duration = Duration::from_secs(2);
        let mut last = None;
        for _ in 0..MAX_RETRIES {
            match self
                .post::<pb::PingResponse>(
                    PING_ROUTE,
                    &pb::PingRequest {
                        message: "hello".into(),
                    },
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => last = Some(e),
            }
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
        Err(OpError::Other(format!(
            "actor {} never served: {}",
            self.actor_name,
            last.map(|e| e.to_string()).unwrap_or_default()
        )))
    }

    /// ResumeActor, retrying update conflicts inside the traced call so the
    /// latency covers every attempt.
    pub(super) async fn resume(&self) -> Result<(), OpError> {
        let api = self.cfg.api.clone();
        let actor = self.actor_ref();
        let res = self
            .traced("ResumeActor", |span| async move {
                retry_on_conflict(|| {
                    let req = Self::request(
                        api::ResumeActorRequest {
                            actor: Some(actor.clone()),
                        },
                        &span,
                    );
                    let api = api.clone();
                    async move { api.resume_actor(req).await.map(|r| r.metadata().clone()) }
                })
                .await
            })
            .await;
        if let Err(s) = &res {
            if is_crashed(s) {
                metrics::record_failure(
                    "actor",
                    "CrashCount",
                    &self.user_class,
                    Duration::ZERO,
                    "actor entered ACTOR_STATE_CRASHED",
                );
            }
            // A full fleet is retried by the caller and reported once.
            let e = OpError::Status(s.clone());
            if !is_capacity_error(Some(&e)) {
                tracing::error!(actor = %self.actor_name, err = %e, "ResumeActor failed");
            }
        }
        res.map_err(OpError::Status)
    }

    /// Suspends the Actor at the end of a step: asynchronously unless the
    /// lifecycle mode is `sync`, not at all when it is `none`. A failure
    /// leaves `hibernate_pending` set so the next step re-drives it.
    pub(super) async fn hibernate(&mut self) {
        let mode = self.knobs().lifecycle.lifecycle_mode;
        if mode == LIFECYCLE_MODE_NONE {
            self.hibernate_pending = false;
            return;
        }
        let (name, durability) = if mode == LIFECYCLE_MODE_SYNC {
            ("SuspendActor", api::DurabilityMode::Synchronous)
        } else {
            ("SuspendActorAsync", api::DurabilityMode::Asynchronous)
        };
        let api = self.cfg.api.clone();
        let actor = self.actor_ref();
        let res = self
            .traced(name, |span| async move {
                let req = Self::request(
                    api::SuspendActorRequest {
                        actor: Some(actor),
                        options: Some(api::SuspendOptions {
                            mode: durability as i32,
                        }),
                    },
                    &span,
                );
                api.suspend_actor(req).await.map(|r| r.metadata().clone())
            })
            .await;
        self.hibernate_pending = res.is_err();
        match res {
            Err(s) => self.note_failure(&OpError::Status(s)),
            Ok(()) => self.awake = false,
        }
    }

    /// Releases the Actor and its worker. Suspends first: a worker only
    /// frees an Actor it can account for, and an Actor still awake at
    /// delete risks being left CRASHED. Safe to call more than once.
    pub async fn suspend_and_delete(&mut self) {
        if self.cleaned_up {
            return;
        }
        let mut req = Request::new(api::SuspendActorRequest {
            actor: Some(self.actor_ref()),
            options: None,
        });
        req.set_timeout(CONTROL_RPC_TIMEOUT);
        let _ = tokio::time::timeout(CONTROL_RPC_TIMEOUT, self.cfg.api.suspend_actor(req)).await;
        let api = self.cfg.api.clone();
        let actor = self.actor_ref();
        let _ = self
            .traced("DeleteActor", |span| async move {
                let req = Self::request(
                    api::DeleteActorRequest {
                        actor: Some(actor),
                        ..Default::default()
                    },
                    &span,
                );
                api.delete_actor(req).await.map(|r| r.metadata().clone())
            })
            .await;
        metrics::update_users(&self.user_class, -1.0);
        self.cleaned_up = true;
    }
}

/// A burn's iterations per thread-second; a zero-length burn has no rate.
pub fn burn_rate_per_thread(o: &Op, iterations: i64) -> Option<f64> {
    if o.millis <= 0 {
        return None;
    }
    let threads = i64::from(o.parallel).max(1);
    Some(iterations as f64 / threads as f64 / (o.millis as f64 / 1000.0))
}

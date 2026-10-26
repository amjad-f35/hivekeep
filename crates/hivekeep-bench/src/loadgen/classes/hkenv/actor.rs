//! One HkEnvUser Actor: lifecycle calls through hivekeep-api, and a
//! `echo hello world` exec through the guest process API.

use std::sync::Arc;
use std::time::{Duration, Instant};

use hivekeep_proto::api::v1 as api;
use parking_lot::Mutex;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::{Code, Status};

use super::knobs::HkEnvKnobs;
use super::{TEMPLATE_HIVE, TEMPLATE_NAME, USER_CLASS};
use crate::loadgen::classes::glutton::calls::{
    self, traced_call, traced_request, ClassRecorder, Recorder,
};
use crate::loadgen::dynconfig::{LIFECYCLE_MODE_NONE, LIFECYCLE_MODE_SYNC};
use crate::loadgen::lifecycle::{
    classify_lifecycle_failure, is_crashed, retry_on_conflict, FailureAction,
};
use crate::loadgen::metrics;
use crate::loadgen::telemetry::{log_sampled_trace, SOURCE_CLIENT};
use crate::loadgen::trace::inject_metadata;
use crate::loadgen::userclass::Config;
use crate::pb::guest::v1 as guest;
use crate::wire::TARGET_ACTOR_HEADER;

/// The guest process API client.
pub type ProcessClient = guest::process_service_client::ProcessServiceClient<Channel>;

/// The output the exec must produce.
pub const EXPECTED_STDOUT: &str = "hello world\n";
/// How long one exec may take.
pub const EXEC_TIMEOUT: Duration = Duration::from_secs(30);
/// Consecutive counted failures that replace an Actor.
pub const MAX_CONSECUTIVE_FAILURES: u32 = 3;

/// The mutable part of an Actor.
#[derive(Debug, Default)]
pub struct State {
    pub(crate) first_resume: bool,
    pub(crate) running: bool,
    pub(crate) crashed: bool,
    pub(crate) hibernate_pending: bool,
    pub(crate) deleted: bool,
    pub(crate) consecutive_failures: u32,
}

/// One Actor's state within a user. Shared between the user's iterations
/// and shutdown, so its state sits behind a lock.
pub struct HkEnvActor {
    pub(crate) cfg: Arc<Config>,
    pub(crate) actor_name: String,
    pub(crate) client: ProcessClient,
    /// Fires when the user's router connection is closed.
    pub(crate) closed: CancellationToken,
    /// Overrides [`EXEC_TIMEOUT`].
    pub(crate) exec_timeout: Option<Duration>,
    pub(crate) state: Mutex<State>,
}

fn rec() -> ClassRecorder {
    ClassRecorder(USER_CLASS.into())
}

/// `err` with `what` in front of its message, keeping its code.
fn prefixed(what: &str, err: &Status) -> Status {
    Status::new(err.code(), format!("{what}: {}", err.message()))
}

impl HkEnvActor {
    /// A new, not yet created Actor.
    pub fn new(
        cfg: Arc<Config>,
        name: String,
        client: ProcessClient,
        closed: CancellationToken,
    ) -> Self {
        HkEnvActor {
            cfg,
            actor_name: name,
            client,
            closed,
            exec_timeout: None,
            state: Mutex::new(State {
                first_resume: true,
                ..Default::default()
            }),
        }
    }

    fn knobs(&self) -> HkEnvKnobs {
        self.cfg.dyn_config.get::<HkEnvKnobs>()
    }

    /// Whether the Actor is resumed.
    pub fn is_running(&self) -> bool {
        self.state.lock().running
    }

    /// Whether a suspend failed and must be re-driven.
    pub fn is_hibernate_pending(&self) -> bool {
        self.state.lock().hibernate_pending
    }

    /// Whether the Actor crashed.
    pub fn is_crashed(&self) -> bool {
        self.state.lock().crashed
    }

    /// Marks the Actor deleted; false when it already was.
    pub fn mark_deleted(&self) -> bool {
        let mut s = self.state.lock();
        !std::mem::replace(&mut s.deleted, true)
    }

    /// Records a failed lifecycle call; whether to replace the Actor.
    pub fn note_failure(&self, err: &Status) -> bool {
        match classify_lifecycle_failure(err) {
            FailureAction::ReplaceNow => true,
            FailureAction::RetryLater => false,
            FailureAction::ReplaceIfPersistent => self.count_failure(err),
        }
    }

    /// Records a failed exec; whether to replace the Actor. Load or
    /// misconfiguration errors never count against the Actor.
    pub fn note_exec_failure(&self, err: &Status) -> bool {
        match err.code() {
            Code::Cancelled
            | Code::ResourceExhausted
            | Code::InvalidArgument
            | Code::PermissionDenied
            | Code::Unauthenticated
            | Code::Unimplemented => false,
            _ => self.count_failure(err),
        }
    }

    fn count_failure(&self, err: &Status) -> bool {
        let n = {
            let mut s = self.state.lock();
            s.consecutive_failures += 1;
            s.consecutive_failures
        };
        if n < MAX_CONSECUTIVE_FAILURES {
            return false;
        }
        tracing::warn!(actor = %self.actor_name, consecutive_failures = n, err = %err.message(),
            "hkenv actor failed repeatedly; replacing it");
        true
    }

    /// Clears the failure count.
    pub fn note_success(&self) {
        self.state.lock().consecutive_failures = 0;
    }

    /// Creates the configured Hive.
    pub async fn ensure_hive(&self) -> Result<(), Status> {
        calls::ensure_hive(&self.cfg, &rec()).await
    }

    /// Creates the Actor.
    pub async fn create(&self) -> Result<(), Status> {
        let cfg = &self.cfg;
        let req = calls::create_request(cfg, &self.actor_name, TEMPLATE_HIVE, TEMPLATE_NAME);
        traced_call(cfg, &rec(), "CreateActor", |span| {
            cfg.api.create_actor(traced_request(req, &span))
        })
        .await
        .map(|_| ())
    }

    /// Resumes the Actor, retrying update conflicts.
    pub async fn resume(&self) -> Result<(), Status> {
        let name = if self.state.lock().first_resume {
            "ResumeActorFirstResume"
        } else {
            "ResumeActor"
        };
        let cfg = &self.cfg;
        let actor = calls::actor_ref(cfg, &self.actor_name);
        let res = traced_call(cfg, &rec(), name, |span| {
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
        match res {
            Err(e) => {
                if is_crashed(&e) {
                    self.state.lock().crashed = true;
                    metrics::record_failure(
                        "actor",
                        "CrashCount",
                        USER_CLASS,
                        Duration::ZERO,
                        "actor entered ACTOR_STATE_CRASHED",
                    );
                    tracing::warn!(actor = %self.actor_name, err = %e.message(), "hkenv actor crashed; will stop sending requests");
                }
                Err(e)
            }
            Ok(_) => {
                let mut s = self.state.lock();
                s.first_resume = false;
                s.running = true;
                Ok(())
            }
        }
    }

    /// Takes the Actor off its worker the way the lifecycle mode says.
    pub async fn hibernate(&self) -> Result<(), Status> {
        match self.knobs().lifecycle.lifecycle_mode.as_str() {
            LIFECYCLE_MODE_NONE => Ok(()),
            LIFECYCLE_MODE_SYNC => self.suspend().await,
            _ => self.pause().await,
        }
    }

    async fn suspend_with(&self, name: &str, mode: api::DurabilityMode) -> Result<(), Status> {
        let cfg = &self.cfg;
        let req = calls::suspend_request(cfg, &self.actor_name, mode);
        let res = traced_call(cfg, &rec(), name, |span| {
            cfg.api.suspend_actor(traced_request(req, &span))
        })
        .await
        .map(|_| ());
        let mut s = self.state.lock();
        s.running = false;
        s.hibernate_pending = res.is_err();
        res
    }

    /// Asynchronous suspend.
    pub async fn pause(&self) -> Result<(), Status> {
        self.suspend_with("SuspendActorAsync", api::DurabilityMode::Asynchronous)
            .await
    }

    /// Synchronous suspend.
    pub async fn suspend(&self) -> Result<(), Status> {
        self.suspend_with("SuspendActor", api::DurabilityMode::Synchronous)
            .await
    }

    /// Deletes the Actor; failures are booked, not returned.
    pub async fn delete(&self) {
        let cfg = &self.cfg;
        let req = api::DeleteActorRequest {
            actor: Some(calls::actor_ref(cfg, &self.actor_name)),
            ..Default::default()
        };
        let _ = traced_call(cfg, &rec(), "DeleteActor", |span| {
            cfg.api.delete_actor(traced_request(req, &span))
        })
        .await;
    }

    /// Runs `echo hello world` in the Actor and checks it exited 0 with the
    /// expected output. Booked as `HkEnvExec`.
    pub async fn exec_hello_world(&self) -> Result<(), Status> {
        let timeout = self.exec_timeout.unwrap_or(EXEC_TIMEOUT);
        let span = self.cfg.tracer.start("HkEnvExec");
        let ctx = *span.context();
        let start = Instant::now();
        let res = tokio::select! {
            _ = self.closed.cancelled() => Err(Status::cancelled("router connection closed")),
            r = tokio::time::timeout(timeout, self.exec(&ctx)) => {
                r.unwrap_or_else(|_| Err(Status::deadline_exceeded("context deadline exceeded")))
            }
        };
        let latency = start.elapsed();
        match &res {
            Ok(n) => {
                log_sampled_trace(&ctx, "HkEnvExec", latency, SOURCE_CLIENT, None);
                rec().success("grpc", "HkEnvExec", latency, *n);
            }
            Err(e) => {
                let text = calls::status_text(e);
                log_sampled_trace(&ctx, "HkEnvExec", latency, SOURCE_CLIENT, Some(&text));
                rec().failure("grpc", "HkEnvExec", latency, &text);
            }
        }
        span.end();
        res.map(|_| ())
    }

    fn request<T>(&self, msg: T, ctx: &crate::loadgen::trace::SpanContext) -> tonic::Request<T> {
        let mut req = tonic::Request::new(msg);
        inject_metadata(ctx, req.metadata_mut());
        if let Ok(v) = format!("{}/{}", self.cfg.hive, self.actor_name).parse() {
            req.metadata_mut().insert(TARGET_ACTOR_HEADER, v);
        }
        req
    }

    /// The exec itself; the stdout length on success.
    async fn exec(&self, ctx: &crate::loadgen::trace::SpanContext) -> Result<i64, Status> {
        let mut client = self.client.clone();
        let started = client
            .start_process(self.request(
                guest::StartProcessRequest {
                    command: vec!["echo".into(), "hello world".into()],
                    ..Default::default()
                },
                ctx,
            ))
            .await
            .map_err(|e| prefixed("StartProcess", &e))?
            .into_inner();
        if started.process_id.is_empty() {
            return Err(Status::unknown("StartProcess returned empty process_id"));
        }
        // An error answer before any output is the stream failing at once.
        let mut stream = client
            .stream_process_output(self.request(
                guest::StreamProcessOutputRequest {
                    process_id: started.process_id.clone(),
                    follow: true,
                    ..Default::default()
                },
                ctx,
            ))
            .await
            .map_err(|e| prefixed("StreamProcessOutput.Recv", &e))?
            .into_inner();
        let mut stdout = Vec::new();
        let mut exit = None;
        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|e| prefixed("StreamProcessOutput.Recv", &e))?;
            match msg.output {
                Some(guest::process_output::Output::Stdout(chunk)) => {
                    stdout.extend_from_slice(&chunk)
                }
                Some(guest::process_output::Output::Exit(p)) => exit = Some(p),
                _ => {}
            }
        }
        let exit = exit.ok_or_else(|| Status::unknown("stream closed without exit event"))?;
        if exit.state() != guest::ProcessState::Exited {
            return Err(Status::unknown(format!(
                "unexpected process state: {}, want PROCESS_STATE_EXITED",
                exit.state().as_str_name()
            )));
        }
        if exit.exit_code != 0 {
            return Err(Status::unknown(format!(
                "unexpected exit code: {}, want 0",
                exit.exit_code
            )));
        }
        let got = String::from_utf8_lossy(&stdout);
        if got != EXPECTED_STDOUT {
            return Err(Status::unknown(format!(
                "stdout mismatch: got {got:?}, want {EXPECTED_STDOUT:?}"
            )));
        }
        Ok(stdout.len() as i64)
    }
}

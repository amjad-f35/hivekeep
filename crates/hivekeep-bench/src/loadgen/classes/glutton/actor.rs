//! One GluttonUser Actor: its lifecycle calls through hivekeep-api and its
//! working-set, CPU and ping traffic through the gate router.

use std::sync::Arc;
use std::time::Instant;

use hivekeep_proto::api::v1 as api;
use tonic::Status;

use super::calls::{self, post, post_proto, traced_call, traced_request, ClassRecorder, Recorder};
use super::knobs::GluttonKnobs;
use super::{GLUTTON_TEMPLATE, GLUTTON_USER_CLASS, TEMPLATE_HIVE};
use crate::glutton::{PING_ROUTE, READ_RAM_ROUTE, USE_CPU_ROUTE, WRITE_RAM_ROUTE};
use crate::loadgen::dynconfig::{LIFECYCLE_MODE_NONE, LIFECYCLE_MODE_SYNC};
use crate::loadgen::lifecycle::{
    classify_lifecycle_failure, is_crashed, retry_on_conflict, FailureAction,
};
use crate::loadgen::metrics;
use crate::loadgen::telemetry::{log_sampled_trace, SOURCE_CLIENT};
use crate::loadgen::userclass::Config;
use crate::pb::glutton::v1 as pb;

/// The key the working set lives under in the glutton.
pub const MEM_LOAD_KEY: &str = "memload";
/// `mem_read` value that walks the whole working set.
pub const MEM_READ_ALL: &str = "all";
/// Consecutive replace-if-persistent failures that replace an Actor: one
/// blip is not evidence of a wedged Actor, three in a row (each a wait
/// window apart) is.
pub const MAX_CONSECUTIVE_FAILURES: u32 = 3;

/// One Actor's state within a user.
pub struct GluttonActor {
    pub(crate) cfg: Arc<Config>,
    pub(crate) actor_name: String,
    pub(crate) first_resume: bool,
    pub(crate) actor_running: bool,
    pub(crate) ram_filled: bool,
    pub(crate) cpu_loaded: bool,
    /// Set when ResumeActor reports the Actor CRASHED; such an Actor never
    /// recovers, so the user replaces it.
    pub(crate) crashed: bool,
    /// Set by a failed SuspendActor: the Actor is stranded RUNNING or
    /// SUSPENDING, which a resume has no edge out of.
    pub(crate) hibernate_pending: bool,
    /// Set once the Actor's DeleteActor was sent during a replacement.
    pub(crate) deleted: bool,
    pub(crate) consecutive_failures: u32,
}

fn rec() -> ClassRecorder {
    ClassRecorder(GLUTTON_USER_CLASS.into())
}

impl GluttonActor {
    /// A new, not yet created Actor named `name`.
    pub fn new(cfg: Arc<Config>, name: String) -> Self {
        GluttonActor {
            cfg,
            actor_name: name,
            first_resume: true,
            actor_running: false,
            ram_filled: false,
            cpu_loaded: false,
            crashed: false,
            hibernate_pending: false,
            deleted: false,
            consecutive_failures: 0,
        }
    }

    fn knobs(&self) -> GluttonKnobs {
        self.cfg.dyn_config.get::<GluttonKnobs>()
    }

    /// Records a failed lifecycle call and says whether to replace the
    /// Actor.
    pub fn note_failure(&mut self, err: &Status) -> bool {
        match classify_lifecycle_failure(err) {
            FailureAction::ReplaceNow => return true,
            FailureAction::RetryLater => return false,
            FailureAction::ReplaceIfPersistent => {}
        }
        self.consecutive_failures += 1;
        if self.consecutive_failures < MAX_CONSECUTIVE_FAILURES {
            return false;
        }
        tracing::warn!(actor = %self.actor_name, consecutive_failures = self.consecutive_failures,
            err = %calls::status_text(err), "glutton actor failed repeatedly; replacing it");
        true
    }

    /// Clears the failure count: the Actor just made progress.
    pub fn note_success(&mut self) {
        self.consecutive_failures = 0;
    }

    /// Creates the configured Hive.
    pub async fn ensure_hive(&self) -> Result<(), Status> {
        calls::ensure_hive(&self.cfg, &rec()).await
    }

    /// Creates the Actor from the glutton template.
    pub async fn create(&self) -> Result<(), Status> {
        let cfg = &self.cfg;
        let req = calls::create_request(cfg, &self.actor_name, TEMPLATE_HIVE, GLUTTON_TEMPLATE);
        traced_call(cfg, &rec(), "CreateActor", |span| {
            cfg.api.create_actor(traced_request(req, &span))
        })
        .await
        .map(|_| ())
    }

    /// Resumes the Actor, retrying update conflicts inside one booked call.
    pub async fn resume(&mut self) -> Result<(), Status> {
        let name = if self.first_resume {
            "ResumeActorFirstResume"
        } else {
            "ResumeActor"
        };
        let cfg = &self.cfg;
        let actor = calls::actor_ref(cfg, &self.actor_name);
        let res = traced_call(cfg, &rec(), name, |span| {
            retry_on_conflict(move || {
                let req = traced_request(
                    api::ResumeActorRequest {
                        actor: Some(actor.clone()),
                    },
                    &span,
                );
                cfg.api.resume_actor(req)
            })
        })
        .await;
        match res {
            Err(e) => {
                if is_crashed(&e) {
                    self.crashed = true;
                    metrics::record_failure(
                        "actor",
                        "CrashCount",
                        GLUTTON_USER_CLASS,
                        std::time::Duration::ZERO,
                        "actor entered ACTOR_STATE_CRASHED",
                    );
                    tracing::warn!(actor = %self.actor_name, err = %calls::status_text(&e),
                        "glutton actor crashed; will stop sending requests");
                }
                Err(e)
            }
            Ok(_) => {
                self.first_resume = false;
                self.actor_running = true;
                self.note_success();
                Ok(())
            }
        }
    }

    /// Takes the Actor off its worker the way the lifecycle mode says:
    /// async suspend by default, sync suspend, or nothing in `none` mode.
    pub async fn hibernate(&mut self) -> Result<(), Status> {
        match self.knobs().lifecycle.lifecycle_mode.as_str() {
            LIFECYCLE_MODE_NONE => Ok(()),
            LIFECYCLE_MODE_SYNC => self.suspend().await,
            _ => self.pause().await,
        }
    }

    async fn suspend_with(&mut self, name: &str, mode: api::DurabilityMode) -> Result<(), Status> {
        let cfg = &self.cfg;
        let req = calls::suspend_request(cfg, &self.actor_name, mode);
        let res = traced_call(cfg, &rec(), name, |span| {
            cfg.api.suspend_actor(traced_request(req, &span))
        })
        .await
        .map(|_| ());
        self.actor_running = false;
        self.hibernate_pending = res.is_err();
        if res.is_ok() {
            self.note_success();
        }
        res
    }

    /// Asynchronous suspend: the snapshot stays on the node.
    pub async fn pause(&mut self) -> Result<(), Status> {
        self.suspend_with("SuspendActorAsync", api::DurabilityMode::Asynchronous)
            .await
    }

    /// Synchronous suspend: the snapshot is written durably first.
    pub async fn suspend(&mut self) -> Result<(), Status> {
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

    /// One echo through the router.
    pub async fn ping(&self) {
        let r = rec();
        let span = self.cfg.tracer.start("GluttonPing");
        let ctx = *span.context();
        let message = uuid::Uuid::new_v4().to_string();
        let body = prost::Message::encode_to_vec(&pb::PingRequest {
            message: message.clone(),
        });
        let start = Instant::now();
        let reply = post(&self.cfg, &self.actor_name, PING_ROUTE, body, &ctx).await;
        let latency = start.elapsed();
        let fail = |e: String| {
            log_sampled_trace(&ctx, "GluttonPing", latency, SOURCE_CLIENT, Some(&e));
            r.failure("http", "GluttonPing", latency, &e);
        };
        let reply = match reply {
            Ok(r) => r,
            Err(e) => {
                r.failure("http", "GluttonPing", latency, &e);
                return span.end();
            }
        };
        if reply.status.as_u16() >= 400 {
            fail(format!(
                "HTTP {}: {}",
                reply.status.as_u16(),
                String::from_utf8_lossy(&reply.body).trim()
            ));
            return span.end();
        }
        let pong = match <pb::PingResponse as prost::Message>::decode(reply.body.as_ref()) {
            Ok(p) => p,
            Err(e) => {
                fail(e.to_string());
                return span.end();
            }
        };
        if pong.message != message {
            fail(format!(
                "ping echo mismatch: sent={message:?} recv={:?}",
                pong.message
            ));
            return span.end();
        }
        log_sampled_trace(&ctx, "GluttonPing", latency, SOURCE_CLIENT, None);
        r.success("http", "GluttonPing", latency, reply.body.len() as i64);
        span.end();
    }

    /// Runs one booked working-set or CPU request, as its own stats row.
    async fn booked<F, Fut>(&self, name: &str, call: F) -> Result<i64, String>
    where
        F: FnOnce(crate::loadgen::trace::SpanContext) -> Fut,
        Fut: std::future::Future<Output = Result<i64, String>>,
    {
        let r = rec();
        let span = self.cfg.tracer.start(name);
        let ctx = *span.context();
        let start = Instant::now();
        let res = call(ctx).await;
        let latency = start.elapsed();
        log_sampled_trace(
            &ctx,
            name,
            latency,
            SOURCE_CLIENT,
            res.as_ref().err().map(String::as_str),
        );
        match &res {
            Ok(bytes) => r.success("http", name, latency, *bytes),
            Err(e) => r.failure("http", name, latency, e),
        }
        span.end();
        res
    }

    async fn write_ram(
        &self,
        size: &str,
        mode: pb::WriteMode,
        span: &crate::loadgen::trace::SpanContext,
    ) -> Result<(), String> {
        let req = pb::WriteRamRequest {
            key: MEM_LOAD_KEY.into(),
            size: size.into(),
            write_mode: mode as i32,
        };
        post_proto::<_, pb::WriteRamResponse>(
            &self.cfg,
            &self.actor_name,
            WRITE_RAM_ROUTE,
            &req,
            span,
        )
        .await
        .map(|_| ())
        .map_err(|e| format!("WriteRAM {MEM_LOAD_KEY} ({size}): {e}"))
    }

    /// Fills the working set to `mem_target`, once per Actor: the glutton
    /// keeps the allocation across suspend and resume, so every snapshot
    /// from the first suspend on is at size. A failure leaves it unfilled
    /// so the next iteration retries.
    pub async fn ensure_ram_filled(&mut self) {
        if self.ram_filled {
            return;
        }
        let target = self.knobs().mem_target;
        if target.is_empty() {
            self.ram_filled = true;
            return;
        }
        let this: &Self = self;
        let res = this
            .booked("GluttonFillRAM", |span| async move {
                this.write_ram(&target, pb::WriteMode::Truncate, &span)
                    .await
                    .map(|_| 0)
            })
            .await;
        if res.is_ok() {
            self.ram_filled = true;
        }
    }

    /// Starts `cpu_cores` CPU threads in the Actor, once per Actor: they
    /// live in the glutton process and resume with it.
    pub async fn ensure_cpu_load(&mut self) {
        if self.cpu_loaded {
            return;
        }
        let knobs = self.knobs();
        if knobs.cpu_cores == 0 {
            self.cpu_loaded = true;
            return;
        }
        let req = pb::UseCpuRequest {
            num_cores: knobs.cpu_cores as i32,
            duty_cycle: knobs.cpu_duty_cycle,
            ..Default::default()
        };
        let this: &Self = self;
        let res = this
            .booked("GluttonUseCPU", |span| async move {
                post_proto::<_, pb::UseCpuResponse>(
                    &this.cfg,
                    &this.actor_name,
                    USE_CPU_ROUTE,
                    &req,
                    &span,
                )
                .await
                .map(|_| 0)
            })
            .await;
        if res.is_ok() {
            self.cpu_loaded = true;
        }
    }

    /// Re-randomizes `mem_churn` of the working set in place each cycle
    /// (rotate mode, so the dirty window moves), so every suspend sees
    /// changing memory. Only after a successful fill.
    pub async fn churn_ram(&self) {
        let churn = self.knobs().mem_churn;
        if churn.is_empty() || !self.ram_filled {
            return;
        }
        let _ = self
            .booked("GluttonChurnRAM", |span| async move {
                self.write_ram(&churn, pb::WriteMode::OverwriteRotate, &span)
                    .await
                    .map(|_| 0)
            })
            .await;
    }

    /// Walks `mem_read` of the working set right after resume; under a
    /// demand-paged restore its latency is the cost of reaching the last
    /// snapshot's memory.
    pub async fn read_ram(&self) {
        let read = self.knobs().mem_read;
        if read.is_empty() || !self.ram_filled {
            return;
        }
        // An empty size walks the whole array.
        let size = if read == MEM_READ_ALL {
            String::new()
        } else {
            read
        };
        let req = pb::ReadRamRequest {
            key: MEM_LOAD_KEY.into(),
            size,
        };
        let _ = self
            .booked("GluttonReadRAM", |span| async move {
                post_proto::<_, pb::ReadRamResponse>(
                    &self.cfg,
                    &self.actor_name,
                    READ_RAM_ROUTE,
                    &req,
                    &span,
                )
                .await
                .map(|r| r.size)
            })
            .await;
    }
}

#[cfg(test)]
#[path = "actor_tests.rs"]
mod tests;

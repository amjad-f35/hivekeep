//! Usage of hosted Actors, read from inside the guest.
//!
//! The host cgroup holds the VMM, whose memory is the guest RAM it took at
//! boot: near-constant whatever the workload does. The guest kernel accounts
//! the workload, and the agent reads it out. None of this takes an Actor's
//! lifecycle lock, which is held across whole boots and snapshots, the
//! phases whose usage matters most.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hivekeep_core::resource::ActorAttribution;
use hivekeep_proto::worker::v1 as pb;
use hivekeep_sandbox_gvisor::error::{status, Error, Result};
use hivekeep_sandbox_gvisor::ports::UsageKind;
use hivekeep_sandbox_gvisor::validation;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::activation::{unix_nanos, Reading};
use crate::agent_stats::Sample;
use crate::service::{GuestStatsTarget, HostedActor, WorkerService};
use crate::usage::emit;

/// Bounds one container's agent call: polled on a timer, so it must fail
/// fast rather than pile pollers up behind a guest that stopped answering.
pub const STATS_CALL_TIMEOUT: Duration = Duration::from_secs(2);
/// Bounds the final record's reading on the checkpoint path; a slower one
/// falls back to the newest cached sample.
pub const FINAL_READ_TIMEOUT: Duration = Duration::from_millis(10);
/// A sweep gives up on the rest after this, so it ends inside one interval.
pub const STATS_SWEEP_BUDGET: Duration = Duration::from_secs(45);

/// The message of the one bug-shaped failure: the published guest target and
/// the attribution disagree.
pub const STALE_GUEST_TARGET: &str = "guest agent connection belongs to a different actor";

fn is_stale(e: &Error) -> bool {
    matches!(e, Error::Status(s) if s.code() == tonic::Code::Internal)
}

/// A workload with no numbers yet: attribution and runtime class, source
/// unspecified ("not measured", never "measured as zero").
pub fn pending_sample(a: &ActorAttribution) -> pb::WorkloadStatsSample {
    pb::WorkloadStatsSample {
        hive: a.actor.hive.clone(),
        actor_name: a.actor.name.clone(),
        actor_uid: a.uid.clone(),
        actor_template_hive: a.template_hive.clone(),
        actor_template_name: a.template_name.clone(),
        sandbox_class: hivekeep_core::sandbox_class::MICROVM.into(),
        observed_at_unix_nano: unix_nanos(SystemTime::now()),
        ..Default::default()
    }
}

/// Reads every container of the Actor and adds them up, with each one's CPU
/// by id so the activation can tell an unreadable container from one whose
/// usage fell. A container the agent cannot report contributes nothing (an
/// exited one consumes nothing); no container answering at all is an error,
/// as is `deadline` passing before every container answered.
pub async fn sum_container_stats(
    target: &GuestStatsTarget,
    deadline: Option<Instant>,
) -> Result<(Sample, HashMap<String, u64>)> {
    let mut total = Sample::default();
    let mut cpu = HashMap::with_capacity(target.workload_ids.len());
    let mut last_err = None;
    for id in &target.workload_ids {
        let now = Instant::now();
        if deadline.is_some_and(|d| now >= d) {
            return Err(Error::DeadlineExceeded);
        }
        let call_deadline = deadline.map_or(now + STATS_CALL_TIMEOUT, |d| {
            d.min(now + STATS_CALL_TIMEOUT)
        });
        match tokio::time::timeout_at(call_deadline, target.agent.stats_container(id)).await {
            Ok(Ok(cs)) => {
                let one = Sample::from_cgroup_stats(cs.as_ref());
                cpu.insert(id.clone(), one.cpu_usage_usec);
                total = total.plus(one);
            }
            Ok(Err(e)) => last_err = Some(Error::from(e)),
            Err(_) if deadline.is_some_and(|d| Instant::now() >= d) => {
                return Err(Error::DeadlineExceeded)
            }
            Err(_) => {
                last_err = Some(Error::msg(format!(
                    "agent StatsContainer {id:?}: context deadline exceeded"
                )))
            }
        }
    }
    if cpu.is_empty() {
        return Err(last_err.unwrap_or_else(|| Error::msg("no containers to measure")));
    }
    Ok((total, cpu))
}

impl WorkerService {
    /// GetWorkloadStats: one Actor's sample, read from its guest now.
    pub async fn get_workload_stats(
        &self,
        req: pb::GetWorkloadStatsRequest,
    ) -> std::result::Result<pb::GetWorkloadStatsResponse, tonic::Status> {
        validation::into_status(validation::get_workload_stats_request(&req))?;
        // NOT_FOUND: retrying on the same timer will not bring the Actor here.
        let hosted = self.lookup_actor(&req.actor_uid).ok_or_else(|| {
            status::not_found(format!("worker is not executing actor {:?}", req.actor_uid))
        })?;
        let sample = match self.measure_guest(&hosted, None).await {
            Ok(s) => s,
            Err(e) if is_stale(&e) => return Err(status::internal(e.to_string())),
            // "No numbers right now": a boot or restore before the target is
            // published, a teardown in progress, or a guest not answering.
            Err(e) => return Err(status::failed_precondition(e.to_string())),
        };
        // The read held no lock, so a checkpoint and a fresh run can have
        // landed underneath it; a new record means another activation.
        if !self.is_current(&hosted) {
            return Err(status::not_found(format!(
                "worker stopped executing actor {:?} while the sample was being taken",
                req.actor_uid
            )));
        }
        hosted.usage.store(sample.clone());
        Ok(pb::GetWorkloadStatsResponse {
            sample: Some(sample),
        })
    }

    /// GetActiveWorkloadStats: each Actor's latest sample from the sampler,
    /// so a poll puts no load on the guests. An Actor not sampled yet is
    /// pending; an empty list is a normal answer.
    pub fn get_active_workload_stats(&self) -> pb::GetActiveWorkloadStatsResponse {
        let samples = self
            .hosted_actors()
            .iter()
            .map(|h| {
                h.usage
                    .latest()
                    .unwrap_or_else(|| h.usage.with_epoch(pending_sample(&h.attribution)))
            })
            .collect();
        pb::GetActiveWorkloadStatsResponse { samples }
    }

    /// Samples every Actor between its initial reading and its final record,
    /// storing each sample and writing its periodic record. Each Actor is
    /// read on a task of its own, so a panic in one read is logged and
    /// skips only that Actor.
    pub async fn sweep_usage(self: &Arc<Self>) {
        let deadline = Instant::now() + STATS_SWEEP_BUDGET;
        let tasks: Vec<_> = self
            .hosted_actors()
            .into_iter()
            .map(|h| {
                let s = self.clone();
                let uid = h.attribution.uid.clone();
                (
                    uid,
                    tokio::spawn(async move { s.sample_hosted_guest(&h, deadline).await }),
                )
            })
            .collect();
        for (uid, t) in tasks {
            if let Err(e) = t.await {
                if e.is_panic() {
                    tracing::error!(actor_uid = %uid, panic = ?e, "Usage sample panicked");
                }
            }
        }
    }

    /// Measures one Actor for the sweep. A guest not reached in time, or not
    /// answering, is pending, so it stays attributable.
    async fn sample_hosted_guest(self: &Arc<Self>, h: &Arc<HostedActor>, deadline: Instant) {
        if !h.usage.sampling() {
            return;
        }
        let mut sample = h.usage.with_epoch(pending_sample(&h.attribution));
        let read = tokio::time::timeout_at(
            deadline,
            h.usage.measure(&CancellationToken::new(), || {
                self.sample_guest_in_slot(h, Some(deadline))
            }),
        )
        .await;
        match read {
            Ok(Ok(m)) => sample = m,
            Ok(Err(e)) if is_stale(&e) => {
                tracing::error!(actor_uid = %h.attribution.uid, err = %e, "Guest stats target belongs to another actor");
            }
            _ => {}
        }
        // Found by uid alone: if the Actor was re-hosted meanwhile, the
        // numbers belong to the new activation, which starts with its own
        // initial reading.
        if !self.is_current(h) {
            return;
        }
        let sink = self.cfg.usage.clone();
        h.usage
            .periodic(sample, |s| emit(sink.as_ref(), UsageKind::Periodic, s));
    }

    /// Reads `h`'s guest as a reading of its activation.
    pub(crate) async fn measure_guest(
        &self,
        h: &HostedActor,
        deadline: Option<Instant>,
    ) -> Result<pb::WorkloadStatsSample> {
        h.usage
            .measure(&CancellationToken::new(), || self.sample_guest(h, deadline))
            .await
    }

    /// Reads `h`'s guest while holding one of the guest slots. The slot is
    /// taken inside the reading, so a reading waiting for another holds none.
    async fn sample_guest_in_slot(
        &self,
        h: &HostedActor,
        deadline: Option<Instant>,
    ) -> Result<Reading> {
        let _permit = match deadline {
            Some(d) => tokio::time::timeout_at(d, self.guest_slots.acquire())
                .await
                .map_err(|_| Error::DeadlineExceeded)?,
            None => self.guest_slots.acquire().await,
        }
        .map_err(|_| Error::Cancelled)?;
        self.sample_guest(h, deadline).await
    }

    /// Samples a new activation off the resume path and writes its initial
    /// record. Runs on a task of its own: a panic in the read is logged, and
    /// the activation is then sampled without an initial record.
    pub(crate) async fn record_initial(self: &Arc<Self>, h: &Arc<HostedActor>) {
        let (s, hh) = (self.clone(), h.clone());
        let deadline = Instant::now() + STATS_SWEEP_BUDGET;
        let read = tokio::spawn(async move {
            tokio::time::timeout_at(
                deadline,
                hh.usage.measure(&CancellationToken::new(), || {
                    s.sample_guest_in_slot(&hh, Some(deadline))
                }),
            )
            .await
            .unwrap_or(Err(Error::DeadlineExceeded))
        })
        .await;
        let sample = match read {
            Ok(Ok(s)) => Some(s),
            Ok(Err(e)) => {
                tracing::warn!(actor_uid = %h.attribution.uid, err = %e, "No initial usage sample");
                None
            }
            Err(e) => {
                tracing::error!(actor_uid = %h.attribution.uid, panic = ?e, "Initial usage sample panicked");
                None
            }
        };
        let sink = self.cfg.usage.clone();
        h.usage
            .initial(sample, |s| emit(sink.as_ref(), UsageKind::Initial, s));
    }

    /// Writes `h`'s final record if a checkpoint or terminate unhosted it.
    pub(crate) fn record_final_if_ended(&self, h: Option<&Arc<HostedActor>>) {
        if let Some(h) = h {
            if !self.is_current(h) {
                self.record_final(h);
            }
        }
    }

    /// Reads `h`'s guest for the final record, bounded by
    /// [`FINAL_READ_TIMEOUT`]; a failed read leaves the newest measured
    /// sample to stand in.
    pub(crate) async fn read_final(&self, h: &HostedActor) {
        let deadline = Instant::now() + FINAL_READ_TIMEOUT;
        match tokio::time::timeout_at(deadline, self.measure_guest(h, Some(deadline))).await {
            Ok(Ok(s)) => h.usage.store(s),
            Ok(Err(e)) => {
                tracing::debug!(actor_uid = %h.attribution.uid, err = %e, "Final usage reading failed; using the newest sample")
            }
            Err(_) => {
                tracing::debug!(actor_uid = %h.attribution.uid, "Final usage reading timed out; using the newest sample")
            }
        }
    }

    /// Writes the final record of an ended activation from its newest
    /// measured sample, or a pending one.
    pub(crate) fn record_final(&self, h: &HostedActor) {
        let pending = h.usage.with_epoch(pending_sample(&h.attribution));
        let sink = self.cfg.usage.clone();
        h.usage
            .finish(|m| emit(sink.as_ref(), UsageKind::Final, m.unwrap_or(&pending)));
    }

    /// Reads `h`'s guest containers through the agent and builds the sample.
    /// Its errors mean "no numbers right now", except a stale target. The
    /// target is read from `h`, so a re-host during the read cannot hand
    /// over the next activation's guest.
    pub(crate) async fn sample_guest(
        &self,
        h: &HostedActor,
        deadline: Option<Instant>,
    ) -> Result<Reading> {
        let a = &h.attribution;
        let target = self
            .guest_of(h)
            .ok_or_else(|| Error::msg("no guest agent connection to measure yet"))?;
        if target.actor_uid != a.uid {
            return Err(Error::Status(tonic::Status::internal(format!(
                "{STALE_GUEST_TARGET}: {:?}, not {:?}",
                target.actor_uid, a.uid
            ))));
        }
        let observed_at = unix_nanos(SystemTime::now());
        let (sample, cpu) = sum_container_stats(&target, deadline)
            .await
            .map_err(|e| match e {
                Error::DeadlineExceeded | Error::Cancelled => e,
                e => Error::msg(format!("no container stats from the guest agent: {e}")),
            })?;
        Ok((
            Some(pb::WorkloadStatsSample {
                hive: a.actor.hive.clone(),
                actor_name: a.actor.name.clone(),
                actor_uid: a.uid.clone(),
                actor_template_hive: a.template_hive.clone(),
                actor_template_name: a.template_name.clone(),
                sandbox_class: hivekeep_core::sandbox_class::MICROVM.into(),
                source: pb::StatsSource::GuestAgent as i32,
                memory_current_bytes: sample.memory_current_bytes,
                memory_peak_bytes: sample.memory_peak_bytes,
                memory_working_set_bytes: sample.memory_working_set_bytes,
                cpu_usage_usec: sample.cpu_usage_usec,
                observed_at_unix_nano: observed_at,
                epoch_unix_nano: 0,
            }),
            Some(cpu),
        ))
    }
}

#[cfg(test)]
pub(crate) mod tests;

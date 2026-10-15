//! Usage statistics: the keyed and discovery stats reads, the periodic
//! sweep, and an activation's initial and final records.
//!
//! gVisor runs every container of a sandbox inside one host process tree
//! (the sentry and its gofers), which runsc starts inside the sandbox's own
//! cgroup leaf. The leaves runsc makes for the application containers are
//! discovery markers and stay empty. So a sample measures the whole sandbox,
//! sentry overhead included, from that one leaf, and is attributed to the
//! Actor rather than a container.
//!
//! None of this takes the Actor's lifecycle lock, which is held across whole
//! boots and checkpoints: polls would stall behind runsc. Reads re-check the
//! hosted record afterwards instead.

use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use hivekeep_core::resource::ActorAttribution;
use hivekeep_core::sandbox_class;
use hivekeep_proto::worker::v1 as pb;

use crate::activation::now_unix_nanos;
use crate::cgroup_stats::Sample;
use crate::error::{status, Error, Result};
use crate::hosted::HostedActor;
use crate::oci::{gvisor_cgroup_leaf, SANDBOX_ID};
use crate::ports::UsageKind;
use crate::service::WorkerService;
use crate::validation;

/// Whether `e` is a cgroup that is not there (yet, or any more).
fn is_not_found(e: &Error) -> bool {
    match e {
        Error::Io { source, .. } => source.kind() == io::ErrorKind::NotFound,
        Error::Context { source, .. } => is_not_found(source),
        _ => false,
    }
}

/// A workload with no numbers yet: attribution and runtime family, source
/// unspecified ("not measured", not "measured as zero").
pub fn pending_sample(a: &ActorAttribution) -> pb::WorkloadStatsSample {
    pb::WorkloadStatsSample {
        hive: a.actor.hive.clone(),
        actor_name: a.actor.name.clone(),
        actor_uid: a.uid.clone(),
        actor_template_hive: a.template_hive.clone(),
        actor_template_name: a.template_name.clone(),
        sandbox_class: sandbox_class::GVISOR.into(),
        observed_at_unix_nano: now_unix_nanos(),
        ..Default::default()
    }
}

impl WorkerService {
    /// The sandbox leaf of an Actor.
    pub(crate) fn sandbox_leaf(&self, uid: &str) -> std::path::PathBuf {
        self.cfg
            .cgroup_root
            .join(gvisor_cgroup_leaf(uid, SANDBOX_ID))
    }

    /// Reads the sandbox cgroup into a sample attributed to `a`, with the raw
    /// reading. Errors are returned as is: a missing cgroup means different
    /// things to different callers.
    fn sample_sandbox(&self, a: &ActorAttribution) -> Result<(pb::WorkloadStatsSample, Sample)> {
        let observed = now_unix_nanos();
        let leaf = self.sandbox_leaf(&a.uid);
        let raw = (self.cfg.read_cgroup)(&leaf)
            .map_err(|e| Error::io(format!("reading {}", leaf.display()), e))?;
        Ok((
            pb::WorkloadStatsSample {
                hive: a.actor.hive.clone(),
                actor_name: a.actor.name.clone(),
                actor_uid: a.uid.clone(),
                actor_template_hive: a.template_hive.clone(),
                actor_template_name: a.template_name.clone(),
                sandbox_class: sandbox_class::GVISOR.into(),
                source: pb::StatsSource::Cgroup as i32,
                memory_current_bytes: raw.memory_current_bytes,
                memory_peak_bytes: raw.memory_peak_bytes,
                memory_working_set_bytes: raw.memory_working_set_bytes,
                cpu_usage_usec: raw.cpu_usage_usec,
                observed_at_unix_nano: observed,
                epoch_unix_nano: 0,
            },
            raw,
        ))
    }

    /// Reads `h`'s sandbox as a reading of its activation, with the raw
    /// cgroup reading for the dead-sandbox check.
    pub(crate) async fn measure(
        &self,
        h: &HostedActor,
    ) -> Result<(pb::WorkloadStatsSample, Sample)> {
        let mut raw = Sample::default();
        let s = h
            .usage
            .measure(|| {
                let (s, r) = self.sample_sandbox(&h.attribution)?;
                raw = r;
                Ok((Some(s), None))
            })
            .await?;
        Ok((s, raw))
    }

    fn emit(&self, kind: UsageKind, s: &pb::WorkloadStatsSample) {
        if let Some(u) = &self.cfg.usage {
            u.emit(kind, s);
        }
    }

    /// `Worker/GetWorkloadStats`: a fresh sample of the named Actor.
    pub async fn get_workload_stats(
        &self,
        req: pb::GetWorkloadStatsRequest,
    ) -> Result<pb::GetWorkloadStatsResponse, tonic::Status> {
        validation::into_status(validation::get_workload_stats_request(&req))?;
        // NOT_FOUND: retrying on this worker will not change it; the
        // caller's mapping of Actor to worker needs refreshing.
        let hosted = self.lookup_actor(&req.actor_uid).ok_or_else(|| {
            status::not_found(format!("worker is not executing actor {:?}", req.actor_uid))
        })?;
        let sample = match self.measure(&hosted).await {
            Ok((s, _)) => s,
            // A poll landing in the boot, before runsc made the leaf, or a
            // sandbox going away: no numbers right now.
            Err(e) if is_not_found(&e) => {
                return Err(status::failed_precondition(
                    "no sandbox cgroup to measure yet",
                ))
            }
            Err(e) => return Err(status::internal(format!("reading sandbox cgroup: {e}"))),
        };
        // A checkpoint and a fresh run can land under the lock-free read.
        if !self.is_current(&hosted) {
            return Err(status::not_found(format!(
                "worker stopped executing actor {:?} while the sample was being taken",
                req.actor_uid
            )));
        }
        // The reading advanced the activation's CPU; keep the cache with it.
        hosted.usage.store(sample.clone());
        Ok(pb::GetWorkloadStatsResponse {
            sample: Some(sample),
        })
    }

    /// `Worker/GetActiveWorkloadStats`: each hosted Actor's latest sample
    /// from the sweep, or a pending one. An empty list is a normal answer.
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

    /// Samples every hosted Actor between its initial and final records,
    /// caching each sample and writing its periodic record.
    pub async fn sweep_usage(&self) {
        for h in self.hosted_actors() {
            if !h.usage.sampling() {
                continue;
            }
            let (sample, raw) = match self.measure(&h).await {
                Ok(r) => r,
                Err(e) => {
                    if !is_not_found(&e) {
                        tracing::warn!(actor_uid = %h.attribution.uid, err = %e, "Failed to read sandbox cgroup");
                    }
                    (
                        h.usage.with_epoch(pending_sample(&h.attribution)),
                        Sample::default(),
                    )
                }
            };
            // Re-hosted meanwhile: the numbers are the new activation's.
            if !self.is_current(&h) {
                continue;
            }
            if raw.empty {
                let _ = self.warn_dead_sandbox(&h, &raw);
            }
            h.usage
                .periodic(sample, |s| self.emit(UsageKind::Periodic, s));
        }
    }

    /// Samples a new activation once its sandbox is up and writes its
    /// initial record.
    pub(crate) async fn record_initial(&self, h: &HostedActor) {
        let sample = match self.measure(h).await {
            Ok((s, _)) => Some(s),
            Err(e) => {
                tracing::warn!(actor_uid = %h.attribution.uid, err = %e, "No initial usage sample");
                None
            }
        };
        h.usage
            .initial(sample, |s| self.emit(UsageKind::Initial, s));
    }

    /// Samples an activation while its sandbox still exists, for the final
    /// record; a failed read leaves the newest measured sample to stand in.
    pub(crate) async fn read_final(&self, h: &HostedActor) {
        if let Ok((s, _)) = self.measure(h).await {
            h.usage.store(s);
        }
    }

    /// Writes `h`'s final record if a checkpoint or terminate ended the
    /// activation (unhosting it, even when a later step failed).
    pub(crate) fn record_final_if_ended(&self, h: Option<&Arc<HostedActor>>) {
        if let Some(h) = h {
            if !self.is_current(h) {
                self.record_final(h);
            }
        }
    }

    /// Writes the final record from the newest measured sample.
    pub(crate) fn record_final(&self, h: &HostedActor) {
        let pending = h.usage.with_epoch(pending_sample(&h.attribution));
        h.usage
            .finish(|measured| self.emit(UsageKind::Final, measured.unwrap_or(&pending)));
    }

    /// Logs, once per activation, that `h`'s sandbox cgroup has no process
    /// left while it is hosted. An Actor whose lock is held is skipped: a
    /// lifecycle RPC empties the cgroup on purpose.
    pub(crate) fn warn_dead_sandbox(&self, h: &Arc<HostedActor>, raw: &Sample) -> bool {
        if h.dead_reported.load(Ordering::SeqCst) {
            return false;
        }
        if self.locks.busy(&h.attribution.uid) || !self.is_current(h) {
            return false;
        }
        if h.dead_reported
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }
        let a = &h.attribution;
        tracing::warn!(
            actor_uid = %a.uid,
            actor = %a.actor.name,
            hive = %a.actor.hive,
            template_hive = %a.template_hive,
            template_name = %a.template_name,
            oom_kills = raw.oom_kills,
            "Sandbox has no processes left while the actor is hosted"
        );
        true
    }
}

#[cfg(test)]
mod tests;

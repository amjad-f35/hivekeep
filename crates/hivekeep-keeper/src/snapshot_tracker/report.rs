//! Reports of an actor's snapshot state to the control plane after
//! background changes, with per-actor and queue-wide backoff.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use tonic::{Code, Status};

use super::actor::Hold;
use super::{Guard, Inner, Options};
use crate::workqueue::{RateLimiter, WorkQueue};

/// Bounds one report attempt.
pub(super) const REPORT_TIMEOUT: Duration = Duration::from_secs(10);
/// The hold-back after a failed report, and the queue's base pause.
pub(super) const REPORT_RETRY_DELAY: Duration = Duration::from_secs(5);
/// The longest hold-back of one actor's report.
pub(super) const MAX_REPORT_RETRY_DELAY: Duration = Duration::from_secs(5 * 60);
/// The longest pause of the whole queue, so reports resume soon after the
/// control plane recovers.
pub(super) const MAX_REPORT_QUEUE_RETRY_DELAY: Duration = Duration::from_secs(30);
/// The window saved reports are spread over at startup, so a fleet-wide
/// restart does not hit the control plane at once.
pub const REPORT_STARTUP_JITTER: Duration = Duration::from_secs(30);

/// The report queue and its settings.
pub(super) struct ReportQueue {
    pub(super) queue: WorkQueue,
    pub(super) retries: RateLimiter,
    queue_retries: RateLimiter,
}

impl ReportQueue {
    pub(super) fn new(_opts: &Options) -> Self {
        ReportQueue {
            queue: WorkQueue::new(),
            retries: RateLimiter::new(REPORT_RETRY_DELAY, MAX_REPORT_RETRY_DELAY),
            queue_retries: RateLimiter::new(REPORT_RETRY_DELAY, MAX_REPORT_QUEUE_RETRY_DELAY),
        }
    }
}

/// Whether a report refused with `err` is worth sending again. NotFound
/// (the actor is gone or no longer here), FailedPrecondition and
/// InvalidArgument (it conflicts with the actor's record), and
/// Unimplemented only repeat on a resend.
pub fn report_retryable(err: &Status) -> bool {
    !matches!(
        err.code(),
        Code::NotFound | Code::FailedPrecondition | Code::InvalidArgument | Code::Unimplemented
    )
}

/// A random delay in `[d/2, d)`, so nodes hitting the same failure do not
/// retry in lockstep.
pub fn jittered_report_delay(d: Duration) -> Duration {
    let half = d / 2;
    if half.is_zero() {
        return d;
    }
    half + crate::rand::duration_below(d - half)
}

/// Whether two states name the same LatestLocal and LatestDurable; changes
/// to anything else make no report due.
pub fn same_report(a: &pb::ActorSnapshotState, b: &pb::ActorSnapshotState) -> bool {
    a.latest_local == b.latest_local && a.latest_durable == b.latest_durable
}

impl Inner {
    /// Records that a background change is owed a report, starting a fresh
    /// give-up window and backoff.
    pub(super) fn mark_report_due(&self, g: &mut Guard) {
        g.a().rec.report_due_since = Some(self.now());
        self.reporting.retries.forget(g.uid());
    }

    /// Clears a pending report and its backoff. The caller saves.
    pub(super) fn cancel_report(&self, g: &mut Guard) {
        let a = g.a();
        a.rec.report_due_since = None;
        a.report_deferred = false;
        self.reporting.retries.forget(g.uid());
    }

    /// Queues the actor's report if one is due, or defers it while an RPC
    /// holds the actor.
    pub(super) fn queue_report(&self, g: &mut Guard) {
        if self.opts.reporter.is_none() || !g.a_ref().report_due() {
            return;
        }
        let a = g.a();
        if a.hold != Hold::None {
            a.report_deferred = true;
            return;
        }
        a.report_deferred = false;
        self.reporting.queue.add(g.uid());
    }

    /// Queues a saved report at startup after a random delay.
    pub(super) fn enqueue_saved_report(&self, g: &mut Guard) {
        if self.opts.reporter.is_none() || !g.a_ref().report_due() {
            return;
        }
        let jitter = self.opts.report_startup_jitter;
        let delay = if jitter.is_zero() {
            Duration::ZERO
        } else {
            Duration::from_nanos(1) + crate::rand::duration_below(jitter)
        };
        self.reporting.queue.add_after(g.uid(), delay);
    }

    /// Sends the reports of the actors taken from the queue until it shuts
    /// down.
    pub(super) async fn run_reporter(self: Arc<Self>) {
        let q = self.reporting.queue.clone();
        while let Some(uid) = q.get().await {
            let delay = self.report(&uid).await;
            q.done(&uid);
            if !delay.is_zero() && self.ctx.sleep(delay).await.is_err() {
                return;
            }
        }
    }

    /// Sends the actor's state if a report is due, and returns how long the
    /// whole queue should pause.
    pub(super) async fn report(&self, actor_uid: &str) -> Duration {
        let Some((actor, sent, due_since)) = self.begin_report(actor_uid).await else {
            return Duration::ZERO;
        };
        let reporter = self
            .opts
            .reporter
            .clone()
            .expect("report workers run only with a reporter");
        let ctx = self.ctx.with_timeout(REPORT_TIMEOUT);
        let res = match ctx
            .run(reporter.report_actor_snapshot_state(&ctx, actor_uid, &actor, &sent))
            .await
        {
            Ok(r) => r,
            Err(c) => Err(c.into()),
        };
        let latest_local = sent
            .latest_local
            .as_ref()
            .and_then(|r| r.snapshot.as_ref())
            .map(|s| s.snapshot_uuid.clone())
            .unwrap_or_default();
        let latest_durable = sent
            .latest_durable
            .as_ref()
            .and_then(|r| r.snapshot.as_ref())
            .map(|s| s.snapshot_uuid.clone())
            .unwrap_or_default();
        let actor_s = actor.to_string();
        match &res {
            Ok(()) => {
                tracing::info!(actor_uid, actor = %actor_s, latest_local, latest_durable, "Reported the actor's snapshot state")
            }
            Err(err) if report_retryable(err) => {
                if self.ctx.is_done() {
                    // Shutting down: the next keeper sends it.
                    tracing::info!(actor_uid, actor = %actor_s, %err, "Failed to report the actor's snapshot state");
                    return Duration::ZERO;
                }
                let queue_delay = if err.code() == Code::Aborted {
                    // The control plane answered; only this actor conflicted.
                    self.reporting.queue_retries.forget("");
                    Duration::ZERO
                } else {
                    jittered_report_delay(self.reporting.queue_retries.when(""))
                };
                self.retry_report(actor_uid, due_since, &actor_s, err).await;
                return queue_delay;
            }
            Err(err) if matches!(err.code(), Code::NotFound | Code::Unimplemented) => {
                // Expected once the actor is deleted or released from this
                // node, or while the control plane is older than the keeper.
                tracing::info!(actor_uid, actor = %actor_s, %err, "The control plane refused the actor's snapshot state")
            }
            Err(err) => {
                tracing::warn!(actor_uid, actor = %actor_s, %err, "The control plane refused the actor's snapshot state")
            }
        }
        self.reporting.queue_retries.forget("");
        self.reporting.retries.forget(actor_uid);
        let forget = match &res {
            Ok(()) => true,
            Err(e) => e.code() == Code::NotFound,
        };
        self.end_report(actor_uid, &sent, forget).await;
        Duration::ZERO
    }

    /// The actor, the state to send, and when the report became due; `None`
    /// when none is due or an RPC holds the actor.
    async fn begin_report(
        &self,
        actor_uid: &str,
    ) -> Option<(ActorRef, pb::ActorSnapshotState, DateTime<Utc>)> {
        let mut g = self.lock_ctx(actor_uid).await?;
        if !self.find(&mut g) || !g.a_ref().report_due() {
            self.reporting.retries.forget(actor_uid);
            return None;
        }
        if g.a_ref().hold != Hold::None {
            g.a().report_deferred = true;
            return None;
        }
        g.a().report_deferred = false;
        let a = g.a_ref();
        Some((
            a.latest_durable_actor(),
            self.state_of(a),
            a.rec.report_due_since.expect("due"),
        ))
    }

    /// Records that `sent` was acknowledged or refused. With `forget`, a
    /// detached actor that has not started again loses its state file and
    /// directory. A background change made while it was in flight is
    /// reported again instead.
    async fn end_report(&self, actor_uid: &str, sent: &pb::ActorSnapshotState, forget: bool) {
        let mut g = self.lock(actor_uid).await;
        if !self.find(&mut g) || !g.a_ref().report_due() {
            return;
        }
        if !same_report(sent, &self.state_of(g.a_ref())) {
            self.queue_report(&mut g);
            return;
        }
        self.finish_report_due(&mut g, forget);
    }

    /// Schedules another attempt at the report due since `due_since`,
    /// unless a commit cancelled it, a newer change replaced it, or the
    /// give-up window has passed.
    async fn retry_report(
        &self,
        actor_uid: &str,
        due_since: DateTime<Utc>,
        actor: &str,
        err: &Status,
    ) {
        let mut g = self.lock(actor_uid).await;
        if !self.find(&mut g) || !g.a_ref().report_due() {
            self.reporting.retries.forget(actor_uid);
            return;
        }
        if g.a_ref().rec.report_due_since != Some(due_since) {
            self.queue_report(&mut g);
            return;
        }
        let now = self.now();
        if now >= crate::clock::add(due_since, self.opts.report_give_up_after) {
            self.clear_report_due(&mut g);
            drop(g);
            tracing::warn!(actor_uid, actor, %err, "Giving up on reporting the actor's snapshot state");
            return;
        }
        let delay = self.reporting.retries.when(actor_uid);
        drop(g);
        let retry_after_ms = delay.as_millis() as u64;
        if err.code() == Code::Aborted {
            tracing::info!(actor_uid, actor, retry_after_ms, %err, "Failed to report the actor's snapshot state");
        } else {
            tracing::warn!(actor_uid, actor, retry_after_ms, %err, "Failed to report the actor's snapshot state");
        }
        self.reporting.queue.add_after(actor_uid, delay);
    }

    /// Clears a due report and saves.
    pub(super) fn clear_report_due(&self, g: &mut Guard) {
        self.finish_report_due(g, false);
    }

    /// Clears a due report. With `forget`, an actor that is detached, has
    /// no upload, and has not started again loses its state file and empty
    /// directory; otherwise its state is saved, so a running actor keeps it
    /// and a later Detach still sees LatestDurable.
    fn finish_report_due(&self, g: &mut Guard, forget: bool) {
        self.add(g);
        self.cancel_report(g);
        let a = g.a_ref();
        if forget && !a.rec.started && a.rec.latest_local.is_none() && a.rec.uploading.is_none() {
            g.a().unsaved = false;
            if let Some(t) = &self.tally {
                t.forget(g.uid());
            }
            self.remove_detached_actor_state(g.uid());
        } else {
            self.save(g);
        }
        self.requeue(g);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_codes() {
        assert!(report_retryable(&Status::unavailable("x")));
        assert!(report_retryable(&Status::aborted("x")));
        assert!(!report_retryable(&Status::not_found("x")));
        assert!(!report_retryable(&Status::failed_precondition("x")));
        assert!(!report_retryable(&Status::unimplemented("x")));
    }

    #[test]
    fn jitter_stays_in_upper_half() {
        for _ in 0..50 {
            let d = jittered_report_delay(Duration::from_secs(10));
            assert!(d >= Duration::from_secs(5) && d < Duration::from_secs(10));
        }
        assert_eq!(
            jittered_report_delay(Duration::from_nanos(1)),
            Duration::from_nanos(1)
        );
    }
}

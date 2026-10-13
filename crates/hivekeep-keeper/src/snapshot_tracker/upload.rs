//! Background uploads.

use std::sync::Arc;
use std::time::Duration;

use tonic::Status;

use super::background::{log_op, Run};
use super::op::{ActorOp, OpDone, OpKind};
use super::snapshot_ref::{same_uuid, SnapshotRef};
use super::{
    local_snapshot_size, remove_local_checkpoint, Guard, Inner, MAX_UPLOAD_TIMEOUT_DOUBLINGS,
};
use crate::context::Ctx;

impl Inner {
    /// Begins an upload of the actor's next upload target and returns it to
    /// run once the lock is released. A superseded Uploading is replaced in
    /// the same save and its local copy removed.
    pub(super) fn begin_upload(self: &Arc<Self>, g: &mut Guard, ctx: Ctx) -> Run {
        let (superseded, _) = g.a().clear_superseded_uploading();
        let target = g
            .a_ref()
            .next_upload_target()
            .cloned()
            .expect("an upload is due only with a target");
        let was_uploading = same_uuid(g.a_ref().rec.uploading.as_ref(), Some(&target));
        let (op, done) = self
            .begin_op(g, ctx, OpKind::Upload, target)
            .expect("the caller checked no operation is in progress");
        // Retrying or resuming an upload finds it saved already.
        if !was_uploading {
            self.save(g);
        }
        if let Some(s) = superseded {
            self.remove_superseded_local_copy(g, &s);
        }
        let uid = g.uid().to_string();
        let this = self.clone();
        Box::pin(async move { this.background_upload(uid, op, done).await })
    }

    /// Clears an Uploading a newer commit superseded when no upload runs,
    /// saving the change and removing the copy kept for it without
    /// releasing the lock, so no Terminate begins in between.
    pub(super) fn cleanup_superseded_uploading(&self, g: &mut Guard) {
        let (superseded, cleared) = g.a().clear_superseded_uploading();
        if !cleared {
            return;
        }
        self.save(g);
        if let Some(s) = superseded {
            self.remove_superseded_local_copy(g, &s);
        }
    }

    /// Uploads `target` from its local copy, bounded by
    /// [`upload_timeout_for`](Inner::upload_timeout_for) unless the timeout
    /// is off. Records the upload metric when `background`.
    pub(super) async fn upload_snapshot(
        &self,
        ctx: &Ctx,
        actor_uid: &str,
        target: &SnapshotRef,
        background: bool,
    ) -> (bool, Duration, Result<(), Status>) {
        let upload_ctx = if self.opts.upload_timeout.is_zero() {
            ctx.child()
        } else {
            ctx.with_timeout(self.upload_timeout_for(actor_uid, target))
        };
        let store = self.store.clone().expect("uploads run only with a store");
        let start = tokio::time::Instant::now();
        // The store sees the context end and returns once it has stopped:
        // dropping its call instead could leave copies running after the
        // caller was told the upload ended.
        let res = store
            .upload_local_snapshot(&upload_ctx, actor_uid, target)
            .await;
        let elapsed = start.elapsed();
        let (uploaded, res) = match res {
            Ok(u) => (u, Ok(())),
            Err(e) => (false, Err(e)),
        };
        if background {
            if let Some(i) = &self.opts.instruments {
                i.record_upload(&upload_ctx, target, elapsed, uploaded, res.as_ref().err());
            }
        }
        (uploaded, elapsed, res)
    }

    /// How long one upload of `target` may run: the upload timeout plus
    /// the time its local copy takes at the expected throughput, doubled
    /// for each consecutive failure up to a cap.
    pub(super) fn upload_timeout_for(&self, actor_uid: &str, target: &SnapshotRef) -> Duration {
        let mut timeout = self.opts.upload_timeout;
        if self.opts.upload_est_throughput > 0 {
            let (size, err) = local_snapshot_size(&self.paths, actor_uid, target.uuid());
            if let Some(err) = err {
                tracing::warn!(actor_uid, snapshot = target.uuid(), %err, "Failed to measure a local snapshot to bound its upload");
            }
            timeout =
                timeout.saturating_add(upload_allowance(size, self.opts.upload_est_throughput));
        }
        let n = self
            .retries
            .num_requeues(actor_uid)
            .min(MAX_UPLOAD_TIMEOUT_DOUBLINGS);
        timeout.saturating_mul(1 << n)
    }

    /// Runs a background upload. Once it succeeds, or a newer durable
    /// snapshot makes it moot, it removes the target's local copy if
    /// LatestLocal moved on; when it fails and nothing replaces it, it keeps
    /// the target in Uploading to retry.
    async fn background_upload(self: Arc<Self>, actor_uid: String, op: ActorOp, done: OpDone) {
        let target = op.target.clone();
        let (uploaded, elapsed, res) = self
            .upload_snapshot(&op.ctx, &actor_uid, &target, true)
            .await;
        log_op(
            &op.ctx,
            &actor_uid,
            op.kind,
            &target,
            uploaded,
            true,
            elapsed,
            res.as_ref().err(),
        );

        let mut g = self.lock(&actor_uid).await;
        if !self.end_op_locked(&mut g, &op) {
            drop(g);
            done.finish(None);
            return;
        }
        let mut remove_old = false;
        match res {
            Ok(()) => {
                if self.record_upload(&mut g, &target, uploaded) {
                    self.mark_report_due(&mut g);
                }
                let a = g.a();
                a.rec.uploading = None;
                a.uploading_failed = false;
                self.succeeded(&mut g);
                self.queue_report(&mut g);
                remove_old = g.a_ref().local_superseded(&target);
            }
            Err(_) if !g.a_ref().upload_needed(&target) => {
                // A newer snapshot became durable meanwhile, so the failure
                // costs nothing.
                let a = g.a();
                a.rec.uploading = None;
                a.uploading_failed = false;
                self.save(&mut g);
                self.requeue(&mut g);
                remove_old = g.a_ref().local_superseded(&target);
            }
            Err(_) => {
                if !self.ctx.is_done() {
                    g.a().uploading_failed = true;
                    let (superseded, cleared) = g.a().clear_superseded_uploading();
                    if cleared {
                        self.save(&mut g);
                        remove_old = superseded.is_some();
                    }
                    if self.next_due(&g).is_some() {
                        self.back_off(&mut g);
                    }
                }
                self.requeue(&mut g);
            }
        }
        if remove_old {
            self.remove_superseded_local_copy(&g, &target);
        }
        drop(g);
        done.finish(None);
    }

    /// Records a successful upload of `target`, and whether it is now
    /// LatestDurable. `uploaded` says whether it wrote the manifest rather
    /// than finding it committed.
    pub(super) fn record_upload(
        &self,
        g: &mut Guard,
        target: &SnapshotRef,
        uploaded: bool,
    ) -> bool {
        let now = self.now();
        let mut d = target.clone();
        if uploaded {
            d.upload_time = Some(now);
        }
        let a = g.a();
        if !a.record_durable(d) {
            return false;
        }
        a.upload_succeeded(now);
        true
    }

    /// Deletes the local copy of `target`, kept only for an upload a newer
    /// snapshot superseded, unless a Run or Restore in progress may use it.
    /// Best-effort: the next checkpoint, detach, or terminate prunes it.
    pub(super) fn remove_superseded_local_copy(&self, g: &Guard, target: &SnapshotRef) {
        if self.local_in_use(g) {
            return;
        }
        if let Err(err) = remove_local_checkpoint(&self.paths, g.uid(), target.uuid()) {
            tracing::warn!(actor_uid = %g.uid(), %err, "Failed to delete a superseded local checkpoint");
        }
    }
}

/// How long `size` bytes take at `bytes_per_second`, rounded up to a whole
/// second; zero when either is zero or less.
pub fn upload_allowance(size: i64, bytes_per_second: i64) -> Duration {
    if size <= 0 || bytes_per_second <= 0 {
        return Duration::ZERO;
    }
    Duration::from_secs(((size - 1) / bytes_per_second + 1) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowance_rounds_up() {
        assert_eq!(upload_allowance(0, 100), Duration::ZERO);
        assert_eq!(upload_allowance(1, 100), Duration::from_secs(1));
        assert_eq!(upload_allowance(100, 100), Duration::from_secs(1));
        assert_eq!(upload_allowance(101, 100), Duration::from_secs(2));
        assert_eq!(upload_allowance(100, 0), Duration::ZERO);
    }
}

//! Detaches: make the latest local snapshot durable, then delete the
//! actor's local snapshots and files on this node.

use std::sync::Arc;

use hivekeep_proto::keeper::v1 as pb;
use tonic::Status;

use super::actor::later;
use super::background::{log_op, Run};
use super::op::{ActorOp, OpKind};
use super::{Guard, Inner, Tracker};
use crate::context::Ctx;

impl Inner {
    /// Begins a detach of the actor's latest local snapshot under a child
    /// of `ctx`, and returns it with the work to run once the lock is
    /// released. Fails when no detach can make the snapshot durable.
    pub(super) fn begin_detach(
        self: &Arc<Self>,
        g: &mut Guard,
        ctx: Ctx,
        background: bool,
    ) -> Result<(ActorOp, Run), Status> {
        let (target, already_durable) = g.a_ref().detach_target()?;
        let (op, done) = self
            .begin_op(g, ctx, OpKind::Detach, target)
            .ok_or_else(|| Status::aborted("another snapshot operation is in progress"))?;
        let uid = g.uid().to_string();
        let this = self.clone();
        let op2 = op.clone();
        let run: Run = Box::pin(async move {
            let r = this
                .finish_detach(&uid, &op2, already_durable, background)
                .await;
            done.finish(Some(r));
        });
        Ok((op, run))
    }

    /// Runs a detach: uploads the snapshot unless durable, then deletes the
    /// actor's local snapshots and other files except its state file. The
    /// upload is recorded before anything is deleted, and LatestLocal is
    /// cleared only once the files are gone; until then it may name a
    /// half-deleted copy, which is safe because the snapshot is durable.
    async fn finish_detach(
        &self,
        actor_uid: &str,
        op: &ActorOp,
        already_durable: bool,
        background: bool,
    ) -> Result<pb::ActorSnapshotState, Status> {
        let ctx = &op.ctx;
        let target = &op.target;
        let mut uploaded = false;
        let mut start = tokio::time::Instant::now();
        let mut err: Option<Status> = None;
        if !already_durable {
            err = ctx.err().map(Status::from);
            if err.is_none() {
                let (up, _, res) = self
                    .upload_snapshot(ctx, actor_uid, target, background)
                    .await;
                uploaded = up;
                match res {
                    Ok(()) => {
                        self.update_op(actor_uid, |t, g| {
                            if t.record_upload(g, target, uploaded) {
                                t.mark_report_due(g);
                            }
                        })
                        .await
                    }
                    Err(e) => err = Some(e),
                }
            }
        }
        let record_detach = err.is_none();
        if record_detach {
            start = tokio::time::Instant::now();
            // Forget or shutdown may have ended ctx as the upload finished.
            err = ctx.err().map(Status::from);
        }
        if err.is_none() {
            let (paths, uid) = (self.paths.clone(), actor_uid.to_string());
            err = match tokio::task::spawn_blocking(move || {
                crate::actordirs::remove_actor_files_except_state(&paths, &uid)
            })
            .await
            {
                Ok(Ok(())) => None,
                Ok(Err(e)) => Some(e.into()),
                Err(e) => Some(Status::internal(format!("cleanup task failed: {e}"))),
            };
        }
        let failed = err.is_some();
        let canceled = ctx.is_done();
        self.update_op(actor_uid, |t, g| {
            if !failed {
                t.clear_latest_local(g);
                t.mark_report_due(g);
            } else if !canceled {
                // Detach again once the retry delay has passed, and no
                // sooner than it was due.
                let at = t.back_off(g);
                let a = g.a();
                a.rec.detach_at = Some(later(at, a.rec.detach_at));
            }
        })
        .await;
        let elapsed = start.elapsed();
        log_op(
            ctx,
            actor_uid,
            op.kind,
            target,
            uploaded,
            background,
            elapsed,
            err.as_ref(),
        );
        if background && record_detach {
            if let Some(i) = &self.opts.instruments {
                i.record_detach(ctx, target, elapsed, err.as_ref());
            }
        }
        self.end_op(actor_uid, op, err).await
    }
}

impl Tracker {
    /// Makes the actor's latest local snapshot durable, then deletes its
    /// local snapshots and files on this node (keeping its state file until
    /// the detach is reported). A no-op when no local snapshot is held.
    /// Fails with Aborted once a Run or Restore has begun since that
    /// snapshot, and FailedPrecondition when no detach can make it durable.
    /// It starts once the actor's operation in progress ends and runs on
    /// the tracker's context: if `ctx` ends first, this returns its error
    /// and the detach runs on, recorded by its report.
    pub async fn detach(
        &self,
        ctx: &Ctx,
        actor_uid: &str,
    ) -> Result<pb::ActorSnapshotState, Status> {
        let t = &self.inner;
        if t.store.is_none() {
            return Err(Status::unimplemented(
                "the keeper has no snapshot store to detach actors with",
            ));
        }
        let g = t.lock(actor_uid).await;
        let (mut g, found) = t.wait_idle(g, ctx).await?;
        if !found || g.a_ref().rec.latest_local.is_none() {
            // Nothing is stored here. A report already due stays due, so a
            // retried call still sees LatestDurable until it is
            // acknowledged; with none due for an actor that has not started,
            // make one due so the acknowledgement cleans up.
            if !found {
                return Ok(pb::ActorSnapshotState::default());
            }
            let st = t.state_of(g.a_ref());
            let a = g.a_ref();
            if !a.rec.started && !a.report_due() && a.rec.latest_durable.is_some() {
                t.add(&mut g);
                t.mark_report_due(&mut g);
                t.save(&mut g);
                t.queue_report(&mut g);
                t.requeue(&mut g);
            }
            return Ok(st);
        }
        if !g.a_ref().detach_scheduled() {
            return Err(Status::aborted(format!(
                "actor {actor_uid} is in use on this node"
            )));
        }
        let op = {
            let waiting = t.waiting.lock();
            if *waiting || t.ctx.is_done() {
                return Err(Status::unavailable("the keeper is shutting down"));
            }
            let (op, run) = t.begin_detach(&mut g, t.ctx.clone(), false)?;
            t.workers.spawn(run);
            op
        };
        drop(g);
        match op.wait_ctx(ctx).await {
            Ok(()) => {
                let res = op.result().unwrap_or_else(|| {
                    Err(Status::unavailable("the detach ended without a result"))
                });
                self.detach_call_error(ctx, res)
            }
            Err(c) => Err(c.into()),
        }
    }

    /// Reports a detach the tracker's shutdown ended as Unavailable, so the
    /// caller does not mistake it for its own cancellation.
    fn detach_call_error(
        &self,
        ctx: &Ctx,
        res: Result<pb::ActorSnapshotState, Status>,
    ) -> Result<pb::ActorSnapshotState, Status> {
        match res {
            Err(e) if self.inner.ctx.is_done() && !ctx.is_done() => Err(Status::unavailable(
                format!("the keeper stopped during the detach: {}", e.message()),
            )),
            r => r,
        }
    }
}

//! The background workers: startup recovery, the upload and detach queues,
//! and the bookkeeping of an actor's one operation in progress.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use hivekeep_proto::keeper::v1 as pb;
use tonic::Status;

use super::actor::{later, Hold};
use super::op::{ActorOp, OpDone, OpKind};
use super::snapshot_ref::{same_uuid, SnapshotRef};
use super::{prune_local_checkpoints, Guard, Inner, Tracker};
use crate::clock::until;
use crate::context::{Ctx, CtxError};

/// Work to run once the actor's lock is released.
pub(super) type Run = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Logs how an upload or detach ended: a failure as a warning, or at info
/// once its context ended, as when Terminate or a newer durable snapshot
/// cancels it.
#[allow(clippy::too_many_arguments)]
pub(super) fn log_op(
    ctx: &Ctx,
    actor_uid: &str,
    kind: OpKind,
    target: &SnapshotRef,
    uploaded: bool,
    background: bool,
    elapsed: Duration,
    err: Option<&Status>,
) {
    let (op, snapshot, generation, elapsed_ms) = (
        kind.name(),
        target.uuid(),
        target.generation(),
        elapsed.as_millis() as u64,
    );
    match err {
        None => tracing::info!(
            op,
            actor_uid,
            snapshot,
            generation,
            background,
            elapsed_ms,
            uploaded,
            "Snapshot operation finished"
        ),
        Some(err) if ctx.is_done() => {
            tracing::info!(op, actor_uid, snapshot, generation, background, elapsed_ms, %err, "Snapshot operation canceled")
        }
        Some(err) => {
            tracing::warn!(op, actor_uid, snapshot, generation, background, elapsed_ms, %err, "Snapshot operation failed")
        }
    }
}

impl Tracker {
    /// Reads each actor's state file, deletes local snapshots from before a
    /// node reboot, queues every actor for its next background work and any
    /// report due, and starts the workers.
    pub async fn start(&self) {
        let t = &self.inner;
        if t.store.is_some()
            || t.opts.reporter.is_some()
            || t.tally.is_some()
            || t.opts.boot_check.is_some()
        {
            let uids = match t.saved_actor_uids() {
                Ok(u) => u,
                Err(err) => {
                    // Each actor's next commit queues it again.
                    tracing::warn!(%err, "Failed to find the actors with snapshot state to queue");
                    Vec::new()
                }
            };
            for uid in uids {
                let Some(mut g) = t.lock_ctx(&uid).await else {
                    break;
                };
                if t.find(&mut g) {
                    t.prune_pre_reboot_local(&mut g);
                    if let Some(tally) = &t.tally {
                        tally.observe(g.a_ref());
                    }
                    t.enqueue_saved_report(&mut g);
                    t.requeue(&mut g);
                }
            }
        }
        for kind in OpKind::ALL {
            let n = match kind {
                OpKind::Upload => t.opts.upload_workers,
                OpKind::Detach => t.opts.detach_workers,
            };
            for _ in 0..n {
                t.workers.spawn(t.clone().run_worker(kind));
            }
        }
        if t.opts.reporter.is_some() {
            for _ in 0..t.opts.report_workers {
                t.workers.spawn(t.clone().run_reporter());
            }
        }
    }

    /// Waits until the workers, and any detach a Detach call started, have
    /// stopped, which they do once the tracker's context ends. Detach starts
    /// nothing after this begins.
    pub async fn wait(&self, ctx: &Ctx) -> Result<(), CtxError> {
        *self.inner.waiting.lock() = true;
        self.inner.workers.close();
        ctx.run(self.inner.workers.wait()).await
    }
}

impl Inner {
    /// Deletes the actor's local snapshots when one it tracks was written
    /// before the node rebooted, since its files may be torn. A durable
    /// LatestLocal is reported as detached; one not yet uploaded is lost.
    fn prune_pre_reboot_local(&self, g: &mut Guard) {
        let Some(check) = self.opts.boot_check.clone() else {
            return;
        };
        let uid = g.uid().to_string();
        let a = g.a_ref();
        let local_pre = a
            .rec
            .latest_local
            .as_ref()
            .is_some_and(|l| !check(&uid, l.uuid()));
        let uploading_pre = a
            .rec
            .uploading
            .as_ref()
            .is_some_and(|u| !check(&uid, u.uuid()));
        if !local_pre && !uploading_pre {
            return;
        }
        self.add(g);
        let keep: Vec<String> = match &g.a_ref().rec.latest_local {
            Some(l) if !local_pre => vec![l.uuid().to_string()],
            _ => Vec::new(),
        };
        let prune_err = prune_local_checkpoints(&self.paths, &uid, &keep).err();
        if let Some(err) = &prune_err {
            tracing::warn!(actor_uid = %uid, %err, "Failed to delete local snapshots left by an earlier boot or an interrupted detach");
        }
        if uploading_pre {
            g.a().rec.uploading = None;
        }
        if local_pre {
            let a = g.a_ref();
            let local = a.rec.latest_local.clone().expect("checked above");
            if a.upload_needed(&local) {
                tracing::warn!(actor_uid = %uid, snapshot = local.uuid(), "Lost a local snapshot that was not uploaded before the node rebooted");
                self.clear_latest_local(g);
            } else if prune_err.is_some() {
                let at = self.back_off(g);
                let a = g.a();
                a.rec.detach_at = Some(later(at, a.rec.detach_at));
            } else {
                tracing::info!(actor_uid = %uid, snapshot = local.uuid(), "Deleted an uploaded local snapshot left by an earlier boot or an interrupted detach");
                self.clear_latest_local(g);
                self.mark_report_due(g);
            }
        }
        self.save(g);
    }

    /// Runs the operations of `kind` of the actors taken from its queue,
    /// until the queue shuts down.
    async fn run_worker(self: Arc<Self>, kind: OpKind) {
        let q = self.queues[kind as usize].clone();
        while let Some(uid) = q.get().await {
            if let Some(run) = self.begin_due_work(&uid, kind).await {
                run.await;
            }
            q.done(&uid);
        }
    }

    /// Begins the actor's operation of `kind` if one is due and returns it
    /// to run. Otherwise queues the actor for when its next work is due,
    /// unless an operation or RPC on it will queue it when it ends.
    pub(super) async fn begin_due_work(
        self: &Arc<Self>,
        actor_uid: &str,
        kind: OpKind,
    ) -> Option<Run> {
        let mut g = self.lock_ctx(actor_uid).await?;
        if self.ctx.is_done() {
            return None;
        }
        if !self.find(&mut g) || g.a_ref().op.is_some() {
            return None;
        }
        if g.a_ref().hold == Hold::Terminate {
            return None;
        }
        let now = self.now();
        match kind {
            OpKind::Detach => {
                if let Some(due) = self.detach_due(g.a_ref()) {
                    if due <= now {
                        if g.a_ref().hold != Hold::None {
                            return None;
                        }
                        if let Ok((_, run)) = self.begin_detach(&mut g, self.ctx.clone(), true) {
                            if let Some(i) = &self.opts.instruments {
                                i.record_detach_overdue(until(due, now));
                            }
                            return Some(run);
                        }
                    }
                }
            }
            OpKind::Upload => {
                if let Some(due) = self.upload_due(g.a_ref()) {
                    if due <= now {
                        if g.a_ref().hold.blocks_uploads() {
                            return None;
                        }
                        let run = self.begin_upload(&mut g, self.ctx.clone());
                        if let Some(i) = &self.opts.instruments {
                            i.record_upload_overdue(until(due, now));
                        }
                        return Some(run);
                    }
                }
                // No upload runs: clear an Uploading a newer commit made
                // obsolete, as an upload beginning would.
                self.cleanup_superseded_uploading(&mut g);
            }
        }
        self.requeue(&mut g);
        None
    }

    /// When the actor's next detach or upload is due, if either is.
    pub(super) fn next_due(&self, g: &Guard) -> Option<DateTime<Utc>> {
        let a = g.a_ref();
        match (self.detach_due(a), self.upload_due(a)) {
            (Some(d), Some(u)) => Some(d.min(u)),
            (d, u) => d.or(u),
        }
    }

    /// Queues the actor on each queue for when its next work of that kind is
    /// due, then lets its entry go from memory unless something keeps it:
    /// an operation or RPC in progress, a retry hold, a failed upload kept
    /// to retry, an unsaved change, or a superseded Uploading an upload
    /// worker must clean up. It runs after a change is saved, so it changes
    /// nothing the state file keeps.
    pub(super) fn requeue(&self, g: &mut Guard) {
        let uid = g.uid().to_string();
        let a = g.a_ref();
        if a.hold == Hold::Terminate {
            return;
        }
        let now = self.now();
        let superseded = self.store.is_some() && a.rec.uploading.is_some() && !a.keep_uploading();
        if superseded {
            self.queues[OpKind::Upload as usize].add(&uid);
        } else if let Some(due) = self.upload_due(a) {
            self.queues[OpKind::Upload as usize].add_after(&uid, until(now, due));
        }
        if let Some(due) = self.detach_due(a) {
            self.queues[OpKind::Detach as usize].add_after(&uid, until(now, due));
        }
        if !superseded
            && a.op.is_none()
            && a.hold == Hold::None
            && a.retry_at.is_none()
            && !a.uploading_failed
            && !a.unsaved
        {
            self.drop_entry(g);
        }
    }

    /// Records an operation of `kind` on `target` as the actor's one in
    /// progress, under a child of `ctx` that Forget cancels. `None` when an
    /// operation is already in progress.
    pub(super) fn begin_op(
        &self,
        g: &mut Guard,
        ctx: Ctx,
        kind: OpKind,
        target: SnapshotRef,
    ) -> Option<(ActorOp, OpDone)> {
        if g.a_ref().op.is_some() {
            return None;
        }
        let a = g.a();
        if kind == OpKind::Upload {
            a.rec.uploading = Some(target.clone());
            a.uploading_failed = false;
        }
        let (op, done) = ActorOp::new(kind, target, ctx.child());
        a.op = Some(op.clone());
        self.add(g);
        Some((op, done))
    }

    /// Runs `f` on the actor's entry under its lock while its operation
    /// runs, then saves.
    pub(super) async fn update_op(&self, actor_uid: &str, f: impl FnOnce(&Inner, &mut Guard)) {
        let mut g = self.lock(actor_uid).await;
        if !self.find(&mut g) {
            return;
        }
        f(self, &mut g);
        self.save(&mut g);
    }

    /// Ends `op`, the actor's operation in progress.
    pub(super) fn end_op_locked(&self, g: &mut Guard, op: &ActorOp) -> bool {
        op.cancel();
        if !self.find(g) {
            return false;
        }
        let a = g.a();
        if a.op.as_ref().is_some_and(|o| o.same(op)) {
            a.op = None;
        }
        true
    }

    /// Ends `op`, queues the actor for its next work and any report due,
    /// and returns its state, or `err`.
    pub(super) async fn end_op(
        &self,
        actor_uid: &str,
        op: &ActorOp,
        err: Option<Status>,
    ) -> Result<pb::ActorSnapshotState, Status> {
        let mut g = self.lock(actor_uid).await;
        if self.end_op_locked(&mut g, op) {
            self.requeue(&mut g);
            self.queue_report(&mut g);
        }
        if let Some(e) = err {
            return Err(e);
        }
        if !g.has() {
            return Ok(pb::ActorSnapshotState::default());
        }
        Ok(self.state_of(g.a_ref()))
    }

    /// Waits, releasing the lock meanwhile, until the actor has no
    /// operation in progress. Returns the guard and whether the actor is
    /// tracked.
    pub(super) async fn wait_idle(&self, mut g: Guard, ctx: &Ctx) -> Result<(Guard, bool), Status> {
        let uid = g.uid().to_string();
        loop {
            let found = self.find(&mut g);
            let Some(op) = found.then(|| g.a_ref().op.clone()).flatten() else {
                return Ok((g, found));
            };
            drop(g);
            op.wait_ctx(ctx).await?;
            g = self.lock(&uid).await;
        }
    }

    /// Waits until no detach of the actor is in progress, vetting the entry
    /// with `check` on every pass. Checkpoint, Run and Restore call it so
    /// they never race a detach pruning the actor's local directory.
    pub(super) async fn wait_for_detach(
        &self,
        mut g: Guard,
        ctx: &Ctx,
        check: impl Fn(&super::ActorSnapshots) -> Result<(), Status>,
    ) -> Result<(Guard, bool), Status> {
        let uid = g.uid().to_string();
        loop {
            let found = self.find(&mut g);
            if !found {
                return Ok((g, false));
            }
            check(g.a_ref())?;
            if g.a_ref().running_detach().is_none() {
                return Ok((g, true));
            }
            let op = g.a_ref().op.clone().expect("a detach is running");
            drop(g);
            op.wait_ctx(ctx).await?;
            g = self.lock(&uid).await;
        }
    }

    /// Waits until no detach of the actor and no upload of `snap` is in
    /// progress, checking `snap`'s generation on every pass.
    pub(super) async fn wait_for_detach_or_snapshot_upload(
        &self,
        mut g: Guard,
        ctx: &Ctx,
        snap: &SnapshotRef,
    ) -> Result<(Guard, bool), Status> {
        let uid = g.uid().to_string();
        loop {
            let found = self.find(&mut g);
            if !found {
                return Ok((g, false));
            }
            g.a_ref().check_generation(&snap.snapshot)?;
            let Some(op) = g.a_ref().op.clone() else {
                return Ok((g, true));
            };
            if op.kind == OpKind::Upload && !same_uuid(Some(&op.target), Some(snap)) {
                return Ok((g, true));
            }
            drop(g);
            op.wait_ctx(ctx).await?;
            g = self.lock(&uid).await;
        }
    }
}

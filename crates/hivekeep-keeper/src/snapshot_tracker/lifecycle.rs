//! What lifecycle RPCs tell the tracker: commits, and the holds that keep
//! background work off an actor while an RPC runs.

use std::sync::Arc;

use hivekeep_proto::keeper::v1 as pb;
use tonic::Status;

use super::actor::{ActorSnapshots, Hold};
use super::snapshot_ref::{same_uuid, SnapshotRef};
use super::{Guard, Inner, Tracker};
use crate::context::Ctx;

/// Ends an RPC's hold on an actor. Call [`end`](HoldEnd::end) when the RPC
/// ends, whether or not it succeeded; a hold dropped without it ends in
/// the background.
#[must_use = "a hold must be ended when its RPC ends"]
pub struct HoldEnd {
    inner: Arc<Inner>,
    actor_uid: String,
    id: u64,
    prev: Hold,
    ended: bool,
}

impl HoldEnd {
    /// Ends the hold and queues the actor's due background work again,
    /// unless Forget dropped the actor meanwhile.
    pub async fn end(mut self) {
        self.ended = true;
        end_hold(
            self.inner.clone(),
            self.actor_uid.clone(),
            self.id,
            self.prev,
        )
        .await;
    }
}

impl Drop for HoldEnd {
    fn drop(&mut self) {
        if !self.ended {
            let (inner, uid, id, prev) = (
                self.inner.clone(),
                self.actor_uid.clone(),
                self.id,
                self.prev,
            );
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                rt.spawn(end_hold(inner, uid, id, prev));
            }
        }
    }
}

async fn end_hold(inner: Arc<Inner>, actor_uid: String, id: u64, prev: Hold) {
    let mut g = inner.lock(&actor_uid).await;
    if !inner.find(&mut g) || g.a_ref().id != id {
        // Forget dropped the actor, as a Terminate does while it holds it.
        return;
    }
    g.a().hold = prev;
    if prev != Hold::None {
        // The outer Checkpoint's end queues the actor.
        return;
    }
    if g.a_ref().report_deferred {
        inner.queue_report(&mut g);
    }
    inner.requeue(&mut g);
}

impl Tracker {
    /// Records `snap`, just stored locally by a Checkpoint in either upload
    /// mode, as the actor's latest local snapshot; schedules its detach and,
    /// if it needs one, its upload; and returns the actor's state. A
    /// snapshot already recorded or superseded leaves the state as it is.
    /// The Checkpoint then prunes what [`local_snapshots_to_keep`] no longer
    /// names.
    ///
    /// [`local_snapshots_to_keep`]: Tracker::local_snapshots_to_keep
    pub async fn commit_local(&self, actor_uid: &str, snap: SnapshotRef) -> pb::ActorSnapshotState {
        let t = &self.inner;
        let mut g = t.lock(actor_uid).await;
        t.find_or_add(&mut g);
        if !g.a_ref().local_needed(&snap) {
            if g.a_ref().report_due() {
                t.clear_report_due(&mut g);
            } else {
                t.requeue(&mut g);
            }
            return t.state_of(g.a_ref());
        }
        let detach_at = crate::clock::add(t.now(), t.opts.detach_delay);
        let a = g.a();
        a.set_latest_local(&snap, Some(detach_at));
        a.rec.started = false;
        if a.rec.undurable_since.is_none() && a.upload_needed(&snap) {
            a.rec.undurable_since = snap.commit_time;
        }
        // Drop a failed upload that snap replaces in this same save: the
        // failure flag is not saved, so after a restart the upload would
        // resume from a local copy the Checkpoint's prune deletes.
        a.clear_superseded_uploading();
        // Start the backoff over unless a kept Uploading still waits it out.
        if !a.keep_uploading() || same_uuid(a.next_upload_target(), Some(&snap)) {
            t.reset_backoff(&mut g);
        }
        t.cancel_report(&mut g);
        t.save(&mut g);
        t.requeue(&mut g);
        t.state_of(g.a_ref())
    }

    /// Records `snap`, just uploaded by a Checkpoint under immediate upload,
    /// as the actor's latest durable snapshot. An older LatestLocal is
    /// dropped, so the Checkpoint's prune deletes it. Once it records
    /// `snap`, it returns only after any upload of an older snapshot has
    /// stopped.
    pub async fn commit_durable(
        &self,
        actor_uid: &str,
        snap: SnapshotRef,
    ) -> pb::ActorSnapshotState {
        let t = &self.inner;
        let mut g = t.lock(actor_uid).await;
        t.find_or_add(&mut g);
        if !g.a_ref().upload_needed(&snap) {
            if g.a_ref().report_due() {
                t.clear_report_due(&mut g);
            } else {
                t.requeue(&mut g);
            }
            return t.state_of(g.a_ref());
        }
        let a = g.a();
        let generation = snap.generation();
        a.rec.latest_durable = Some(snap);
        a.rec.started = false;
        if a.rec
            .latest_local
            .as_ref()
            .is_some_and(|l| l.generation() < generation)
        {
            a.clear_latest_local();
        }
        t.cancel_report(&mut g);
        t.durable_committed(&mut g);
        t.durable_state(g).await
    }

    /// The actor's state for a retried Checkpoint whose snapshot is
    /// committed already. A pending upload report is cancelled while a
    /// local snapshot is kept; once none is, a detach report stays due.
    pub async fn committed_checkpoint_state(&self, actor_uid: &str) -> pb::ActorSnapshotState {
        let t = &self.inner;
        let mut g = t.lock(actor_uid).await;
        if !t.find(&mut g) {
            return pb::ActorSnapshotState::default();
        }
        if g.a_ref().rec.latest_local.is_some() && g.a_ref().report_due() {
            t.clear_report_due(&mut g);
        }
        t.state_of(g.a_ref())
    }

    /// The local snapshots still needed: LatestLocal, and Uploading while an
    /// upload reads it or will retry it.
    pub async fn local_snapshots_to_keep(&self, actor_uid: &str) -> Vec<String> {
        let t = &self.inner;
        let mut g = t.lock(actor_uid).await;
        if !t.find(&mut g) {
            return Vec::new();
        }
        let a = g.a_ref();
        let mut keep = Vec::new();
        if let Some(l) = &a.rec.latest_local {
            keep.push(l.uuid().to_string());
        }
        if a.keep_uploading() && !same_uuid(a.rec.uploading.as_ref(), a.rec.latest_local.as_ref()) {
            if let Some(u) = &a.rec.uploading {
                keep.push(u.uuid().to_string());
            }
        }
        keep
    }

    /// Records `snap`, a local snapshot PromoteSnapshot made durable or
    /// found durable, as the latest durable snapshot. It keeps the local
    /// copy and its commit time, and leaves the detach schedule as it is.
    pub async fn commit_promoted(
        &self,
        actor_uid: &str,
        mut snap: SnapshotRef,
    ) -> pb::ActorSnapshotState {
        let t = &self.inner;
        let mut g = t.lock(actor_uid).await;
        t.find_or_add(&mut g);
        if !g.a_ref().upload_needed(&snap) {
            if g.a_ref().rec.latest_local.is_some() && g.a_ref().report_due() {
                t.clear_report_due(&mut g);
            } else {
                t.requeue(&mut g);
            }
            return t.state_of(g.a_ref());
        }
        let a = g.a();
        if let Some(l) = &a.rec.latest_local {
            if l.uuid() == snap.uuid() {
                snap.commit_time = l.commit_time;
            }
        }
        a.rec.latest_durable = Some(snap);
        // The control plane reads a promote's response as no detach, so a
        // pending detach report stays due once nothing is local.
        if a.rec.latest_local.is_some() {
            t.cancel_report(&mut g);
        }
        t.durable_committed(&mut g);
        t.durable_state(g).await
    }

    /// Records that a Run or Restore is starting the actor, once a detach in
    /// progress has finished, and saves it before returning, so the actor
    /// is not detached while a workload restored from its local snapshot
    /// may use it, even across a restart or a failed start. Fails with
    /// Unavailable if the start cannot be saved; the caller must not start
    /// the actor then.
    pub async fn begin_start(&self, ctx: &Ctx, actor_uid: &str) -> Result<HoldEnd, Status> {
        let t = &self.inner;
        let g = t.lock(actor_uid).await;
        let (mut g, found) = t.wait_for_detach(g, ctx, |_| Ok(())).await?;
        if found {
            let a = g.a();
            let (detach_at, started, unsaved) = (a.rec.detach_at, a.rec.started, a.unsaved);
            if a.mark_started() {
                if let Err(err) = t.write_state(&mut g) {
                    let a = g.a();
                    a.rec.detach_at = detach_at;
                    a.rec.started = started;
                    a.unsaved = unsaved;
                    return Err(Status::unavailable(format!(
                        "while recording the start of actor {actor_uid}: {err}"
                    )));
                }
            }
            // Nothing is left to retry for a detach; keep the backoff while
            // an upload is still due.
            if t.upload_due(g.a_ref()).is_none() {
                t.reset_backoff(&mut g);
            }
        }
        Ok(t.hold(g, found, Hold::Start))
    }

    /// Records that a Checkpoint of `snap` under `mode` is in progress, once
    /// a detach in progress has finished, so no detach deletes the snapshot
    /// it writes (and under immediate upload, no background upload starts).
    /// Reports whether `snap` is committed already: the Checkpoint is then a
    /// retry of one that committed it. Fails with Aborted on a stale
    /// generation and FailedPrecondition on a conflicting name.
    pub async fn begin_checkpoint(
        &self,
        ctx: &Ctx,
        actor_uid: &str,
        snap: &pb::Snapshot,
        mode: pb::UploadMode,
    ) -> Result<(bool, HoldEnd), Status> {
        let t = &self.inner;
        let g = t.lock(actor_uid).await;
        let (g, found) = t
            .wait_for_detach(g, ctx, |a| a.check_generation(snap))
            .await?;
        let committed = if found {
            g.a_ref().checkpoint_committed(snap)?
        } else {
            false
        };
        let kind = if mode == pb::UploadMode::Immediate {
            Hold::Durable
        } else {
            Hold::Checkpoint
        };
        Ok((committed, t.hold(g, found, kind)))
    }

    /// Whether `snap` is committed and no Run or Restore has begun since
    /// the latest Checkpoint commit.
    pub async fn not_started_since(&self, actor_uid: &str, snap: &pb::Snapshot) -> bool {
        let t = &self.inner;
        let mut g = t.lock(actor_uid).await;
        if !t.find(&mut g) || g.a_ref().rec.started {
            return false;
        }
        g.a_ref().checkpoint_committed(snap).unwrap_or(false)
    }

    /// Records that a PromoteSnapshot of `snap` is in progress, once a
    /// detach and any background upload of `snap` have finished. Reports
    /// whether `snap` is already the latest durable snapshot.
    pub async fn begin_promote(
        &self,
        ctx: &Ctx,
        actor_uid: &str,
        snap: &SnapshotRef,
    ) -> Result<(bool, HoldEnd), Status> {
        let t = &self.inner;
        let g = t.lock(actor_uid).await;
        let (g, found) = t.wait_for_detach_or_snapshot_upload(g, ctx, snap).await?;
        let durable = if found {
            g.a_ref().is_latest_durable(snap)?
        } else {
            false
        };
        Ok((durable, t.hold(g, found, Hold::Durable)))
    }

    /// Records that a Terminate is in progress: no background work starts
    /// on the actor until it ends, and [`forget`](Tracker::forget) cancels
    /// what is running.
    pub async fn begin_terminate(&self, actor_uid: &str) -> HoldEnd {
        let t = &self.inner;
        let mut g = t.lock(actor_uid).await;
        let found = t.find(&mut g);
        t.hold(g, found, Hold::Terminate)
    }

    /// Drops the actor's state in memory and on disk, after cancelling any
    /// upload or detach in progress and waiting for it to end.
    pub async fn forget(&self, actor_uid: &str) -> Result<(), Status> {
        let t = &self.inner;
        let mut g = t.lock(actor_uid).await;
        if t.find(&mut g) {
            t.add(&mut g);
            g.a().hold = Hold::Terminate;
            while let Some(op) = g.a_ref().op.clone() {
                op.cancel();
                drop(g);
                op.wait().await;
                g = t.lock(actor_uid).await;
                if !t.find(&mut g) {
                    break;
                }
            }
        }
        t.retries.forget(actor_uid);
        t.reporting.retries.forget(actor_uid);
        t.drop_entry(&mut g);
        if let Some(tally) = &t.tally {
            tally.forget(actor_uid);
        }
        t.remove_state(actor_uid)
    }
}

impl Inner {
    /// Records an RPC of `kind` on the actor, holding detaches and reports
    /// off (and uploads when `kind` blocks them), and returns its end. An
    /// actor nothing is tracked of yet is held too, so nothing begins on a
    /// snapshot its first Checkpoint commits.
    pub(super) fn hold(self: &Arc<Self>, mut g: Guard, found: bool, kind: Hold) -> HoldEnd {
        if !found {
            let e = self.new_entry(g.uid(), Default::default());
            g.g.entry = Some(e);
        }
        self.add(&mut g);
        let actor_uid = g.uid().to_string();
        let a: &mut ActorSnapshots = g.a();
        let prev = a.hold;
        a.hold = kind;
        HoldEnd {
            inner: self.clone(),
            actor_uid,
            id: a.id,
            prev,
            ended: false,
        }
    }

    /// Finishes a durable commit: when LatestLocal is durable too (or
    /// absent) clears UndurableSince and the backoff; saves and requeues.
    pub(super) fn durable_committed(&self, g: &mut Guard) {
        let a = g.a_ref();
        let local_durable = match &a.rec.latest_local {
            None => true,
            Some(l) => !a.upload_needed(l),
        };
        if local_durable {
            g.a().rec.undurable_since = None;
            self.reset_backoff(g);
        }
        self.save(g);
        self.requeue(g);
    }

    /// The state to return once a new LatestDurable is recorded. The control
    /// plane may then delete older snapshots' objects, so any upload of an
    /// older snapshot is cancelled and awaited first, releasing the lock.
    pub(super) async fn durable_state(&self, mut g: Guard) -> pb::ActorSnapshotState {
        let uid = g.uid().to_string();
        loop {
            let Some(op) = g.a_ref().superseded_upload() else {
                break;
            };
            op.cancel();
            drop(g);
            op.wait().await;
            g = self.lock(&uid).await;
            if !self.find(&mut g) {
                return pb::ActorSnapshotState::default();
            }
        }
        self.state_of(g.a_ref())
    }

    /// Records that no local snapshot is held, as after a detach, and lifts
    /// the backoff, which has nothing left to retry.
    pub(super) fn clear_latest_local(&self, g: &mut Guard) {
        let a = g.a();
        a.rec.uploading = None;
        a.uploading_failed = false;
        a.rec.undurable_since = None;
        a.clear_latest_local();
        self.reset_backoff(g);
    }

    /// Finishes a change that leaves the actor in a good end state.
    pub(super) fn succeeded(&self, g: &mut Guard) {
        self.reset_backoff(g);
        self.save(g);
        self.requeue(g);
    }
}

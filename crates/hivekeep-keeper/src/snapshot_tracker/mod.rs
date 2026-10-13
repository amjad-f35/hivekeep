//! Tracks each actor's snapshots on this node, uploads and detaches them in
//! the background, and reports what that changes to the control plane.
//!
//! For each actor the tracker knows the latest snapshot stored locally and
//! the latest one confirmed in object storage. Checkpoint and
//! PromoteSnapshot are fenced by generation before they do any work, and a
//! commit never replaces a snapshot with an older generation. Callers
//! serialize lifecycle RPCs per actor, so at most one runs for an actor at a
//! time; the tracker's own per-actor lock guards only short critical
//! sections, including the actor's state-file I/O.
//!
//! Background work runs off two delaying queues of actor UIDs, one for
//! uploads and one for detaches, each drained by its own workers so slow
//! uploads never hold up a due detach. Every change to an actor queues it
//! for when its next upload or detach is due:
//!
//! 1. A checkpoint records its snapshot as LatestLocal ([`Tracker::commit_local`])
//!    and schedules the actor's detach a detach delay later. An immediate
//!    checkpoint records its upload first ([`Tracker::commit_durable`]). If
//!    LatestLocal needs an upload, it is due an upload delay after
//!    UndurableSince.
//! 2. A worker that takes the actor runs its upload or detach if due, and
//!    otherwise queues it again for when it is.
//! 3. An upload takes the LatestLocal of when it runs, so snapshots
//!    superseded while waiting are never uploaded. One that has begun
//!    (Uploading) goes on across newer checkpoints and restarts until it
//!    succeeds, a newer snapshot is durable, or it fails while a newer
//!    local snapshot can replace it.
//! 4. An actor has at most one operation in progress; a worker that finds
//!    one leaves the actor to that operation's end, which queues it again.
//! 5. An upload leaves LatestLocal on disk for a same-node Restore. A
//!    detach makes LatestLocal durable, then deletes all the actor's local
//!    snapshots and files except its state file.
//! 6. A Run or Restore unschedules the detach as it begins
//!    ([`Tracker::begin_start`]) and saves that before the workload starts,
//!    so an actor whose start has begun is never detached; only its next
//!    checkpoint schedules a detach again. Background work that comes due
//!    while an RPC holds the actor waits for it to end.
//! 7. A background upload or detach that changes what the control plane
//!    knows makes a report due, saved with the change. Report workers send
//!    the actor's whole state and retry with backoff until it is
//!    acknowledged or refused, a commit cancels it, or the give-up window
//!    passes. Once a detach's report is acknowledged (or refused with
//!    NotFound), the actor's state file and directory are removed.
//!
//! How each RPC treats the actor's operation in progress and report due:
//!
//! ```text
//! RPC                   upload in progress  detach in progress  report due
//! Run, Restore          -                   waits               -
//! deferred Checkpoint   -                   waits               cancels
//! immediate Checkpoint  cancels             waits               cancels
//! PromoteSnapshot       waits if the same   waits               cancels if a local
//!                       snapshot, else                          snapshot is kept
//!                       cancels
//! DetachActor           waits               waits               keeps, and makes one due
//! Terminate             cancels             cancels             cancels
//! ```

mod actor;
mod background;
mod detach;
mod lifecycle;
pub mod local_checkpoints;
pub mod metrics;
mod op;
mod report;
mod snapshot_ref;
mod upload;

#[cfg(test)]
mod detach_tests;
#[cfg(test)]
mod report_tests;
#[cfg(test)]
mod test_support_core;
#[cfg(test)]
mod test_support_ops;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_background_core;
#[cfg(test)]
mod tests_local_checkpoints;
#[cfg(test)]
mod tests_metrics_core;
#[cfg(test)]
mod tests_tracker_core;
#[cfg(test)]
mod upload_tests;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use parking_lot::Mutex;
use tokio_util::task::TaskTracker;
use tonic::Status;

pub use actor::{ActorRecord, ActorSnapshots, Hold};
pub use lifecycle::HoldEnd;
pub use local_checkpoints::{
    local_snapshot_size, prune_local_checkpoints, remove_local_checkpoint,
};
pub use metrics::Instruments;
pub use op::OpKind;
pub use snapshot_ref::{same_uuid, timestamp, SnapshotRef};

use crate::clock::SharedClock;
use crate::context::Ctx;
use crate::keyed_lock::{KeyedGuard, KeyedLocks, Vacant};
use crate::paths::NodePaths;
use crate::workqueue::{RateLimiter, WorkQueue};
use metrics::UndurableTally;
use op::NUM_OP_KINDS;
use report::ReportQueue;

/// How long after an actor's oldest undurable snapshot its latest local
/// snapshot is uploaded, by default.
pub const DEFAULT_UPLOAD_DELAY: Duration = Duration::from_secs(5 * 60);
/// How long an actor stays suspended here, its latest snapshot kept
/// locally, before it is detached, by default.
pub const DEFAULT_DETACH_DELAY: Duration = Duration::from_secs(30 * 60);
/// How long one upload attempt may run beyond its estimated time.
pub const DEFAULT_UPLOAD_TIMEOUT: Duration = Duration::from_secs(2 * 60);
/// The expected upload rate, in bytes per second of allocated size.
pub const DEFAULT_UPLOAD_EST_THROUGHPUT: i64 = 100 << 20;
/// Background uploads at once, by default.
pub const DEFAULT_UPLOAD_WORKERS: usize = 2;
/// Background detaches at once, by default.
pub const DEFAULT_DETACH_WORKERS: usize = 2;
/// Reports at once, by default.
pub const DEFAULT_REPORT_WORKERS: usize = 1;
/// How long a failed report is retried, by default.
pub const DEFAULT_REPORT_GIVE_UP_AFTER: Duration = Duration::from_secs(10 * 60);

/// Caps how often consecutive failures double an upload's timeout.
const MAX_UPLOAD_TIMEOUT_DOUBLINGS: u32 = 3;
/// The first hold-back after a failure of background work.
const SNAPSHOT_RETRY_DELAY: Duration = Duration::from_secs(60);
/// The longest hold-back after failures of background work.
const MAX_SNAPSHOT_RETRY_DELAY: Duration = Duration::from_secs(10 * 60);

/// Moves an actor's snapshots between this node and object storage.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    /// Makes `r` durable at its URI from its local copy, acting for
    /// `r.actor`. With the local copy gone it succeeds without uploading
    /// when a manifest is already committed there. Reports whether it
    /// uploaded.
    async fn upload_local_snapshot(
        &self,
        ctx: &Ctx,
        actor_uid: &str,
        r: &SnapshotRef,
    ) -> Result<bool, Status>;
}

/// Tells the control plane what the keeper holds of an actor's snapshots.
#[async_trait]
pub trait Reporter: Send + Sync + 'static {
    /// Reports the actor's whole state; repeating one is safe.
    async fn report_actor_snapshot_state(
        &self,
        ctx: &Ctx,
        actor_uid: &str,
        actor: &ActorRef,
        state: &pb::ActorSnapshotState,
    ) -> Result<(), Status>;
}

/// Tells whether an actor's local snapshot was written on this boot. A
/// snapshot whose local copy is gone counts as written before.
pub type BootCheck = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

/// Tracker settings.
#[derive(Clone)]
pub struct Options {
    /// See [`DEFAULT_UPLOAD_DELAY`].
    pub upload_delay: Duration,
    /// See [`DEFAULT_DETACH_DELAY`].
    pub detach_delay: Duration,
    /// How long one upload attempt may run on top of its size allowance.
    /// Each consecutive failure doubles it, to at most 8 times. Zero turns
    /// the timeout off.
    pub upload_timeout: Duration,
    /// Bytes per second of allocated size an upload is expected to move.
    /// Zero adds no allowance for size.
    pub upload_est_throughput: i64,
    /// Background uploads at once.
    pub upload_workers: usize,
    /// Background detaches at once.
    pub detach_workers: usize,
    /// Reports at once.
    pub report_workers: usize,
    /// How long a failed report is retried since it became due.
    pub report_give_up_after: Duration,
    /// The window saved reports are spread over at startup.
    pub report_startup_jitter: Duration,
    /// With it, local snapshots written before the node rebooted, whose
    /// files may be torn, are deleted at start.
    pub boot_check: Option<BootCheck>,
    /// Records background work and undurable snapshots.
    pub instruments: Option<Instruments>,
    /// Reports state changes to the control plane.
    pub reporter: Option<Arc<dyn Reporter>>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            upload_delay: DEFAULT_UPLOAD_DELAY,
            detach_delay: DEFAULT_DETACH_DELAY,
            upload_timeout: DEFAULT_UPLOAD_TIMEOUT,
            upload_est_throughput: DEFAULT_UPLOAD_EST_THROUGHPUT,
            upload_workers: DEFAULT_UPLOAD_WORKERS,
            detach_workers: DEFAULT_DETACH_WORKERS,
            report_workers: DEFAULT_REPORT_WORKERS,
            report_give_up_after: DEFAULT_REPORT_GIVE_UP_AFTER,
            report_startup_jitter: report::REPORT_STARTUP_JITTER,
            boot_check: None,
            instruments: None,
            reporter: None,
        }
    }
}

/// One actor's slot under its lock: its entry while in memory.
#[derive(Default)]
pub(crate) struct Slot {
    entry: Option<ActorSnapshots>,
    /// Whether the entry stays in memory after the lock is released. An
    /// entry loaded from disk and not kept is dropped on release.
    kept: bool,
}

impl Vacant for Slot {
    fn is_vacant(&self) -> bool {
        !self.kept
    }
}

/// An actor's lock, held.
pub(crate) struct Guard {
    g: KeyedGuard<Slot>,
}

impl Guard {
    /// The actor's entry. Panics unless one is loaded.
    pub(crate) fn a(&mut self) -> &mut ActorSnapshots {
        self.g.entry.as_mut().expect("actor entry is loaded")
    }

    /// The actor's entry, read-only.
    pub(crate) fn a_ref(&self) -> &ActorSnapshots {
        self.g.entry.as_ref().expect("actor entry is loaded")
    }

    /// Whether an entry is loaded.
    pub(crate) fn has(&self) -> bool {
        self.g.entry.is_some()
    }

    fn uid(&self) -> &str {
        self.g.key()
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if !self.g.kept {
            self.g.entry = None;
        }
    }
}

/// Tracks each actor's snapshots on this node.
#[derive(Clone)]
pub struct Tracker {
    inner: Arc<Inner>,
}

pub(crate) struct Inner {
    ctx: Ctx,
    paths: Arc<NodePaths>,
    store: Option<Arc<dyn Store>>,
    clock: SharedClock,
    opts: Options,
    tally: Option<Arc<UndurableTally>>,
    retries: RateLimiter,
    queues: [WorkQueue; NUM_OP_KINDS],
    reporting: ReportQueue,
    workers: TaskTracker,
    /// Set once Wait has begun; held while a Detach call joins `workers`.
    waiting: Mutex<bool>,
    locks: KeyedLocks<Slot>,
    next_id: AtomicU64,
}

impl Tracker {
    /// A tracker over `store` that tracks no actor yet. Its workers run
    /// once [`start`](Tracker::start) is called, until `ctx` ends. Without
    /// a store nothing is uploaded or detached.
    pub fn new(
        ctx: Ctx,
        paths: Arc<NodePaths>,
        store: Option<Arc<dyn Store>>,
        clock: SharedClock,
        opts: Options,
    ) -> Self {
        let tally = opts.instruments.as_ref().map(|i| {
            let t = Arc::new(UndurableTally::new(
                clock.clone(),
                opts.upload_delay,
                store.is_some(),
            ));
            i.watch(t.clone());
            t
        });
        let reporting = ReportQueue::new(&opts);
        let inner = Arc::new(Inner {
            ctx: ctx.clone(),
            paths,
            store,
            clock,
            tally,
            retries: RateLimiter::new(SNAPSHOT_RETRY_DELAY, MAX_SNAPSHOT_RETRY_DELAY),
            queues: [WorkQueue::new(), WorkQueue::new()],
            reporting,
            workers: TaskTracker::new(),
            waiting: Mutex::new(false),
            locks: KeyedLocks::new(),
            next_id: AtomicU64::new(1),
            opts,
        });
        let weak = Arc::downgrade(&inner);
        tokio::spawn(async move {
            ctx.done().await;
            if let Some(inner) = weak.upgrade() {
                for q in &inner.queues {
                    q.shut_down();
                }
                inner.reporting.queue.shut_down();
            }
        });
        Tracker { inner }
    }

    /// What the keeper holds for the actor.
    pub async fn state(&self, actor_uid: &str) -> pb::ActorSnapshotState {
        let mut g = self.inner.lock(actor_uid).await;
        if self.inner.find(&mut g) {
            return self.inner.state_of(g.a_ref());
        }
        pb::ActorSnapshotState::default()
    }
}

impl Inner {
    async fn lock(&self, actor_uid: &str) -> Guard {
        Guard {
            g: self.locks.lock(actor_uid).await,
        }
    }

    /// Takes the actor's lock unless the tracker is shutting down first.
    async fn lock_ctx(&self, actor_uid: &str) -> Option<Guard> {
        self.locks
            .lock_ctx(&self.ctx, actor_uid)
            .await
            .ok()
            .map(|g| Guard { g })
    }

    fn now(&self) -> DateTime<Utc> {
        self.clock.now()
    }

    fn new_entry(&self, actor_uid: &str, rec: ActorRecord) -> ActorSnapshots {
        ActorSnapshots::new(self.next_id.fetch_add(1, Ordering::Relaxed), actor_uid, rec)
    }

    /// Loads the actor's entry, from memory or else from its state file.
    /// Returns whether the keeper tracks anything of the actor.
    fn find(&self, g: &mut Guard) -> bool {
        if g.g.entry.is_some() {
            return true;
        }
        match self.read_state(g.uid()) {
            Some(rec) => {
                let e = self.new_entry(g.uid(), rec);
                g.g.entry = Some(e);
                true
            }
            None => false,
        }
    }

    /// Loads the actor's entry, adding an empty one if there is none, and
    /// keeps it in memory.
    fn find_or_add(&self, g: &mut Guard) {
        if !self.find(g) {
            let e = self.new_entry(g.uid(), ActorRecord::default());
            g.g.entry = Some(e);
        }
        self.add(g);
    }

    /// Keeps the loaded entry in memory.
    fn add(&self, g: &mut Guard) {
        g.g.kept = true;
    }

    /// Lets the entry go from memory when the lock is released.
    fn drop_entry(&self, g: &mut Guard) {
        g.g.kept = false;
    }

    /// Whether a Run or Restore in progress may use a superseded local
    /// snapshot. Such an entry is always in memory.
    fn local_in_use(&self, g: &Guard) -> bool {
        g.g.kept && g.g.entry.as_ref().is_some_and(|a| a.local_in_use())
    }

    fn upload_due(&self, a: &ActorSnapshots) -> Option<DateTime<Utc>> {
        self.store.as_ref()?;
        a.upload_due(self.opts.upload_delay)
    }

    fn detach_due(&self, a: &ActorSnapshots) -> Option<DateTime<Utc>> {
        self.store.as_ref()?;
        a.detach_due()
    }

    fn state_of(&self, a: &ActorSnapshots) -> pb::ActorSnapshotState {
        let uploading = if a.keep_uploading() {
            a.rec.uploading.as_ref().map(|r| r.report())
        } else {
            None
        };
        pb::ActorSnapshotState {
            latest_local: a.rec.latest_local.as_ref().map(|r| r.report()),
            uploading,
            latest_durable: a.rec.latest_durable.as_ref().map(|r| r.report()),
            upload_due_time: self.upload_due(a).map(timestamp),
        }
    }

    /// Writes the entry to its state file. Best-effort: on failure the
    /// entry stays in memory and the keeper carries on from it.
    fn save(&self, g: &mut Guard) {
        if let Err(err) = self.write_state(g) {
            tracing::warn!(actor_uid = %g.uid(), %err, "Failed to save the actor's snapshot state");
        }
    }

    /// Writes the entry's state file. An actor whose directory is gone was
    /// terminated here, so it is not written.
    fn write_state(&self, g: &mut Guard) -> Result<(), String> {
        let uid = g.uid().to_string();
        if !self.paths.actor_dir(&uid).exists() {
            tracing::warn!(actor_uid = %uid, "Dropping the snapshot state of an actor whose directory is gone");
            g.a().unsaved = false;
            if let Some(t) = &self.tally {
                t.forget(&uid);
            }
            return Ok(());
        }
        if let Some(t) = &self.tally {
            t.observe(g.a_ref());
        }
        let res = serde_json::to_vec(&g.a_ref().rec)
            .map_err(|e| e.to_string())
            .and_then(|data| {
                crate::atomicfile::write(&self.paths.actor_snapshot_state_file(&uid), &data, 0o600)
                    .map_err(|e| e.to_string())
            });
        g.a().unsaved = res.is_err();
        res
    }

    /// Reads the actor's state file; `None` when missing or unreadable.
    fn read_state(&self, actor_uid: &str) -> Option<ActorRecord> {
        let data = match std::fs::read(self.paths.actor_snapshot_state_file(actor_uid)) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(err) => {
                tracing::warn!(actor_uid, %err, "Skipping an unreadable snapshot state file");
                return None;
            }
        };
        match serde_json::from_slice(&data) {
            Ok(rec) => Some(rec),
            Err(err) => {
                tracing::warn!(actor_uid, %err, "Skipping an unreadable snapshot state file");
                None
            }
        }
    }

    fn remove_state(&self, actor_uid: &str) -> Result<(), Status> {
        match std::fs::remove_file(self.paths.actor_snapshot_state_file(actor_uid)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Status::internal(format!(
                "while deleting the actor's snapshot state: {e}"
            ))),
        }
    }

    /// Deletes a detached actor's state file and its now-empty directory.
    fn remove_detached_actor_state(&self, actor_uid: &str) {
        if let Err(err) = self.remove_state(actor_uid) {
            tracing::warn!(actor_uid, %err, "Failed to remove the detached actor's snapshot state");
            return;
        }
        if let Err(e) = std::fs::remove_dir(self.paths.actor_dir(actor_uid)) {
            let benign = e.kind() == std::io::ErrorKind::NotFound
                || matches!(e.raw_os_error(), Some(libc::ENOTEMPTY) | Some(libc::EEXIST));
            if !benign {
                tracing::warn!(actor_uid, err = %e, "Failed to remove the detached actor's directory");
            }
        }
    }

    /// UIDs of the actors with a state file on this node.
    fn saved_actor_uids(&self) -> Result<Vec<String>, String> {
        let rd = match std::fs::read_dir(self.paths.actors_dir()) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(format!("while listing actor directories: {e}")),
        };
        let mut out = Vec::new();
        for e in rd.flatten() {
            if !e.file_type().is_ok_and(|t| t.is_dir()) {
                continue;
            }
            let uid = e.file_name().to_string_lossy().into_owned();
            if matches!(
                std::fs::metadata(self.paths.actor_snapshot_state_file(&uid)),
                Err(ref err) if err.kind() == std::io::ErrorKind::NotFound
            ) {
                continue;
            }
            out.push(uid);
        }
        Ok(out)
    }

    /// Records a failure of the actor's background work and holds it back
    /// until the returned time, longer with each consecutive failure.
    fn back_off(&self, g: &mut Guard) -> DateTime<Utc> {
        let at = crate::clock::add(self.now(), self.retries.when(g.uid()));
        g.a().retry_at = Some(at);
        at
    }

    /// Lifts the hold-back and forgets the actor's failures.
    fn reset_backoff(&self, g: &mut Guard) {
        g.a().retry_at = None;
        self.retries.forget(g.uid());
    }
}

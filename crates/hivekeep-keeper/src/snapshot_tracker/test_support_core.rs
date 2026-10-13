//! Fakes and helpers for the tracker tests that drive its queues, holds and
//! background work step by step on paused tokio time.
//!
//! Waiting helpers never sleep on tokio time: a paused runtime that goes
//! idle auto-advances to the next timer, which would make delayed work due
//! early. They yield and back off on real time instead.

use std::collections::HashSet;
use std::fs;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tonic::Status;

use super::op::{ActorOp, OpDone, OpKind};
use super::*;
use crate::clock::MockClock;
use crate::context::Ctx;
use crate::paths::NodePaths;

pub(super) const UID: &str = "actor-uid-1";
pub(super) const UPLOAD_DELAY: Duration = Duration::from_secs(3600);
pub(super) const DETACH_DELAY: Duration = Duration::from_secs(4 * 3600);
pub(super) const UPLOAD_TIMEOUT: Duration = Duration::from_secs(45 * 60);
/// Lets a local copy of a few MiB add whole seconds to an upload timeout.
pub(super) const UPLOAD_EST_THROUGHPUT: i64 = 1 << 20;
pub(super) const MINUTE: Duration = Duration::from_secs(60);
pub(super) const SECOND: Duration = Duration::from_secs(1);

/// The time every test tracker's clock starts at.
pub(super) fn t0() -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000, 0).unwrap()
}

pub(super) fn at(t: DateTime<Utc>, d: Duration) -> DateTime<Utc> {
    crate::clock::add(t, d)
}

pub(super) fn before(t: DateTime<Utc>, d: Duration) -> DateTime<Utc> {
    t - chrono::Duration::from_std(d).unwrap()
}

/// A MEMORY snapshot of the tracked actor.
pub(super) fn test_ref(uuid: &str, generation: i32, commit: Option<DateTime<Utc>>) -> SnapshotRef {
    SnapshotRef {
        snapshot: pb::Snapshot {
            snapshot_uuid: uuid.into(),
            generation,
            fidelity: pb::SnapshotFidelity::Memory as i32,
            object: Some(pb::ObjectStorage {
                snapshot_uri: format!("gs://test-bucket/hives/demo/actors/{UID}/snapshots/{uuid}"),
                sharing: 0,
            }),
        },
        commit_time: commit,
        upload_time: None,
        actor: ActorRef::new("", ""),
    }
}

/// The UUID a report names, or "".
pub(super) fn uuid_of(r: &Option<pb::SnapshotReport>) -> String {
    r.as_ref()
        .and_then(|r| r.snapshot.as_ref())
        .map(|s| s.snapshot_uuid.clone())
        .unwrap_or_default()
}

pub(super) fn time_of(ts: &Option<prost_types::Timestamp>) -> Option<DateTime<Utc>> {
    ts.as_ref()
        .map(|t| DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap())
}

/// One node's directories in a temp dir.
pub(super) struct Node {
    _dir: tempfile::TempDir,
    pub(super) paths: Arc<NodePaths>,
    pub(super) ctx: Ctx,
}

impl Node {
    /// A node with no actor directory.
    pub(super) fn bare() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let paths = Arc::new(NodePaths::new(dir.path()));
        Node {
            _dir: dir,
            paths,
            ctx: Ctx::background(),
        }
    }

    /// A node holding the tracked actor's directories, so its state is saved.
    pub(super) fn new() -> Self {
        let n = Self::bare();
        n.actor_dir(UID);
        fs::create_dir_all(n.paths.actor_snapshot_dir(UID)).unwrap();
        n
    }

    pub(super) fn actor_dir(&self, uid: &str) {
        fs::create_dir_all(self.paths.actor_dir(uid)).unwrap();
    }

    /// A tracker without a store and with default settings, its workers not
    /// started.
    pub(super) fn plain(&self) -> Tracker {
        self.tracker_at(None, t0(), Options::default())
    }

    /// The settings of the trackers [`Node::store_tracker`] builds: nothing is
    /// due until a test advances time.
    pub(super) fn opts() -> Options {
        Options {
            upload_delay: UPLOAD_DELAY,
            detach_delay: DETACH_DELAY,
            upload_timeout: UPLOAD_TIMEOUT,
            upload_est_throughput: UPLOAD_EST_THROUGHPUT,
            report_startup_jitter: Duration::ZERO,
            ..Options::default()
        }
    }

    /// A tracker over `store` with the test settings changed by `f`, its
    /// workers not started.
    pub(super) fn store_tracker(
        &self,
        store: &Arc<RecStore>,
        f: impl FnOnce(&mut Options),
    ) -> Tracker {
        let mut o = Self::opts();
        f(&mut o);
        self.tracker_at(Some(store.clone()), t0(), o)
    }

    pub(super) fn tracker_at(
        &self,
        store: Option<Arc<RecStore>>,
        start: DateTime<Utc>,
        opts: Options,
    ) -> Tracker {
        Tracker::new(
            self.ctx.child(),
            self.paths.clone(),
            store.map(|s| s as Arc<dyn Store>),
            Arc::new(MockClock::new(start)),
            opts,
        )
    }
}

/// Writes a local snapshot of `uid` holding one small file.
pub(super) fn write_local_snapshot(paths: &NodePaths, uid: &str, uuid: &str) -> std::path::PathBuf {
    let dir = paths.local_snapshot_dir(uid, uuid);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("checkpoint.img"), b"memory").unwrap();
    dir
}

#[derive(Default)]
struct StoreState {
    calls: Vec<String>,
    uploaded: HashSet<String>,
    blocking: bool,
    err: Option<Status>,
    deadline: Option<tokio::time::Instant>,
    on_success: Option<Box<dyn FnOnce() + Send>>,
}

type Release = (Option<Status>, oneshot::Sender<()>);

/// A store that records its calls. A call for a URI it holds already
/// uploads nothing. While blocking, a call waits for [`RecStore::release`]
/// or its context.
pub(super) struct RecStore {
    st: Mutex<StoreState>,
    started_tx: mpsc::UnboundedSender<String>,
    started_rx: StdMutex<mpsc::UnboundedReceiver<String>>,
    release_tx: mpsc::UnboundedSender<Release>,
    release_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<Release>>,
}

impl RecStore {
    pub(super) fn new() -> Arc<Self> {
        let (started_tx, started_rx) = mpsc::unbounded_channel();
        let (release_tx, release_rx) = mpsc::unbounded_channel();
        Arc::new(RecStore {
            st: Mutex::new(StoreState::default()),
            started_tx,
            started_rx: StdMutex::new(started_rx),
            release_tx,
            release_rx: tokio::sync::Mutex::new(release_rx),
        })
    }

    pub(super) fn set_blocking(&self, b: bool) {
        self.st.lock().blocking = b;
    }

    pub(super) fn set_err(&self, e: Option<Status>) {
        self.st.lock().err = e;
    }

    pub(super) fn mark_uploaded(&self, uri: &str) {
        self.st.lock().uploaded.insert(uri.to_string());
    }

    /// Runs `f` as the next successful call returns.
    pub(super) fn on_success(&self, f: impl FnOnce() + Send + 'static) {
        self.st.lock().on_success = Some(Box::new(f));
    }

    pub(super) fn uploads(&self) -> Vec<String> {
        self.st.lock().calls.clone()
    }

    pub(super) fn last_deadline(&self) -> Option<tokio::time::Instant> {
        self.st.lock().deadline
    }

    /// The next started call's snapshot, if one started already.
    pub(super) fn try_started(&self) -> Option<String> {
        self.started_rx.lock().unwrap().try_recv().ok()
    }

    /// Waits for the next call to start and returns its snapshot.
    pub(super) async fn wait_started(&self) -> String {
        for _ in 0..WAIT_ROUNDS {
            if let Some(u) = self.try_started() {
                return u;
            }
            pause().await;
        }
        panic!("timed out waiting for an upload to start");
    }

    /// Ends a blocked call with `err`, waiting until one takes it.
    pub(super) async fn release(&self, err: Option<Status>) {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.release_tx.send((err, ack_tx)).unwrap();
        let mut ack_rx = ack_rx;
        for _ in 0..WAIT_ROUNDS {
            match ack_rx.try_recv() {
                Ok(()) => return,
                Err(oneshot::error::TryRecvError::Empty) => pause().await,
                Err(oneshot::error::TryRecvError::Closed) => {
                    panic!("a release was dropped by a call that ended")
                }
            }
        }
        panic!("timed out waiting for a blocked upload to take a release");
    }
}

#[async_trait]
impl Store for RecStore {
    async fn upload_local_snapshot(
        &self,
        ctx: &Ctx,
        _uid: &str,
        r: &SnapshotRef,
    ) -> Result<bool, Status> {
        let (blocking, mut err) = {
            let mut st = self.st.lock();
            st.calls.push(r.uuid().to_string());
            st.deadline = ctx.deadline();
            (st.blocking, st.err.clone())
        };
        let _ = self.started_tx.send(r.uuid().to_string());
        if blocking {
            let mut rx = self.release_rx.lock().await;
            tokio::select! {
                m = rx.recv() => {
                    let (e, ack) = m.expect("release channel open");
                    let _ = ack.send(());
                    err = e;
                }
                e = ctx.done() => return Err(e.into()),
            }
        }
        if let Some(e) = err {
            return Err(e);
        }
        let hook = {
            let mut st = self.st.lock();
            st.on_success.take()
        };
        if let Some(h) = hook {
            h();
        }
        let mut st = self.st.lock();
        Ok(st.uploaded.insert(r.uri().to_string()))
    }
}

/// One report a [`RecReporter`] received.
#[derive(Debug, Clone)]
pub(super) struct SentReport {
    pub(super) actor_uid: String,
    pub(super) state: pb::ActorSnapshotState,
}

/// A reporter that records what it is sent and acknowledges it.
#[derive(Default)]
pub(super) struct RecReporter {
    pub(super) sent: Mutex<Vec<SentReport>>,
}

#[async_trait]
impl Reporter for RecReporter {
    async fn report_actor_snapshot_state(
        &self,
        _ctx: &Ctx,
        actor_uid: &str,
        _actor: &ActorRef,
        state: &pb::ActorSnapshotState,
    ) -> Result<(), Status> {
        self.sent.lock().push(SentReport {
            actor_uid: actor_uid.to_string(),
            state: state.clone(),
        });
        Ok(())
    }
}

const WAIT_ROUNDS: usize = 20_000;

/// Lets other tasks and blocking threads run without moving tokio time.
pub(super) async fn pause() {
    tokio::task::yield_now().await;
    std::thread::sleep(Duration::from_micros(250));
}

/// Waits until `cond` holds, failing the test after a few real seconds.
pub(super) async fn eventually(what: &str, mut cond: impl AsyncFnMut() -> bool) {
    for _ in 0..WAIT_ROUNDS {
        if cond().await {
            return;
        }
        pause().await;
    }
    panic!("timed out waiting until {what}");
}

/// Lets ready tasks run for a while without moving tokio time.
pub(super) async fn settle() {
    for _ in 0..200 {
        pause().await;
    }
}

/// Fails unless `f` completes within a few real seconds, without moving
/// tokio time.
pub(super) async fn returns_promptly<F: std::future::Future>(what: &str, f: F) -> F::Output {
    let mut f = std::pin::pin!(f);
    for _ in 0..WAIT_ROUNDS {
        if let std::task::Poll::Ready(v) = futures::poll!(f.as_mut()) {
            return v;
        }
        pause().await;
    }
    panic!("{what} did not return promptly");
}

/// The tracker's clock.
pub(super) fn now(t: &Tracker) -> DateTime<Utc> {
    t.inner.now()
}

/// Advances tokio time, and with it the tracker's clock, to `when`, so the
/// background work due by then comes due.
pub(super) async fn step_to(t: &Tracker, when: DateTime<Utc>) {
    let d = crate::clock::until(now(t), when);
    if !d.is_zero() {
        tokio::time::advance(d).await;
    }
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
}

/// When the actor's detach is due, if it is.
pub(super) async fn detach_due_of(t: &Tracker, uid: &str) -> Option<DateTime<Utc>> {
    let mut g = t.inner.lock(uid).await;
    if !t.inner.find(&mut g) {
        return None;
    }
    g.a_ref().detach_due()
}

/// Whether the actor's entry is held in memory.
pub(super) async fn in_memory(t: &Tracker, uid: &str) -> bool {
    let g = t.inner.lock(uid).await;
    g.g.entry.is_some()
}

/// How many actors' entries are held in memory, with no lock held.
pub(super) fn actors_in_memory(t: &Tracker) -> usize {
    t.inner.locks.len()
}

/// The actor's operation in progress.
pub(super) async fn current_op(t: &Tracker, uid: &str) -> Option<ActorOp> {
    let mut g = t.inner.lock(uid).await;
    if !t.inner.find(&mut g) {
        return None;
    }
    g.a_ref().op.clone()
}

/// The actor's operation in progress; fails without one.
pub(super) async fn op_of(t: &Tracker, uid: &str) -> ActorOp {
    current_op(t, uid).await.expect("an operation in progress")
}

/// Waits for `op` to end, failing after a few real seconds.
pub(super) async fn wait_op(op: &ActorOp, what: &str) {
    returns_promptly(what, op.wait()).await;
}

/// Begins an upload operation on `target` for the tracked actor, as a worker
/// does.
pub(super) async fn begin_op(t: &Tracker, target: SnapshotRef) -> (ActorOp, OpDone) {
    let mut g = t.inner.lock(UID).await;
    t.inner.find_or_add(&mut g);
    t.inner
        .begin_op(&mut g, t.inner.ctx.clone(), OpKind::Upload, target)
        .expect("the actor already has an operation in progress")
}

/// Ends `op` as a worker whose operation changed nothing does.
pub(super) async fn end_op(t: &Tracker, op: &ActorOp, done: OpDone) {
    {
        let mut g = t.inner.lock(UID).await;
        if t.inner.end_op_locked(&mut g, op) {
            t.inner.requeue(&mut g);
        }
    }
    done.finish(None);
}

/// Takes the next actor from `kind`'s queue of a tracker without workers and
/// runs its due work as a worker of that queue would.
pub(super) async fn process_next(t: &Tracker, kind: OpKind) {
    let q = t.inner.queues[kind as usize].clone();
    for _ in 0..WAIT_ROUNDS {
        if !q.is_empty() {
            let uid = q.get().await.expect("queue open");
            if let Some(run) = t.inner.begin_due_work(&uid, kind).await {
                run.await;
            }
            q.done(&uid);
            return;
        }
        pause().await;
    }
    panic!("timed out waiting for an actor queued for {}", kind.name());
}

/// Actors the queues hold ready for a worker, not counting delayed ones.
pub(super) fn queued_now(t: &Tracker) -> usize {
    t.inner.queues.iter().map(|q| q.len()).sum()
}

/// Drains each queue's ready actors, sorted.
pub(super) async fn drain_queues(t: &Tracker) -> [Vec<String>; 2] {
    let mut out: [Vec<String>; 2] = Default::default();
    for (k, q) in t.inner.queues.iter().enumerate() {
        while !q.is_empty() {
            let uid = q.get().await.unwrap();
            q.done(&uid);
            out[k].push(uid);
        }
        out[k].sort();
    }
    out
}

/// Runs the upload due for the tracked actor up to the store call, and
/// returns the operation.
pub(super) async fn start_upload(t: &Tracker, store: &RecStore) -> ActorOp {
    let due = time_of(&t.state(UID).await.upload_due_time).expect("no upload scheduled");
    step_to(t, due).await;
    store.wait_started().await;
    op_of(t, UID).await
}

/// Runs the tracked actor's due upload and ends it on the blocking store
/// with `err`.
pub(super) async fn upload_now(t: &Tracker, store: &RecStore, err: Option<Status>) {
    let op = start_upload(t, store).await;
    store.release(err).await;
    wait_op(&op, "the upload").await;
}

/// Advances to the tracked actor's detach and waits for a worker to detach
/// it.
pub(super) async fn detach_now(t: &Tracker) {
    let due = detach_due_of(t, UID).await.expect("no detach scheduled");
    step_to(t, due).await;
    eventually("the actor is detached", async || {
        let mut g = t.inner.lock(UID).await;
        t.inner.find(&mut g) && g.a_ref().op.is_none() && g.a_ref().rec.latest_local.is_none()
    })
    .await;
}

/// Captures the tracing events emitted on this thread while it is alive.
pub(super) struct LogCapture {
    records: Arc<Mutex<Vec<(tracing::Level, String)>>>,
    _guard: tracing::subscriber::DefaultGuard,
}

impl LogCapture {
    pub(super) fn start() -> Self {
        use tracing_subscriber::layer::SubscriberExt;
        let records = Arc::new(Mutex::new(Vec::new()));
        let sub = tracing_subscriber::registry().with(CaptureLayer {
            records: records.clone(),
        });
        LogCapture {
            records,
            _guard: tracing::subscriber::set_default(sub),
        }
    }

    /// "LEVEL message" of each record whose message starts with `prefix`.
    pub(super) fn with_prefix(&self, prefix: &str) -> Vec<String> {
        self.records
            .lock()
            .iter()
            .filter(|(_, m)| m.starts_with(prefix))
            .map(|(l, m)| format!("{l} {m}"))
            .collect()
    }
}

struct CaptureLayer {
    records: Arc<Mutex<Vec<(tracing::Level, String)>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Msg(String);
        impl tracing::field::Visit for Msg {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                if f.name() == "message" {
                    self.0 = format!("{v:?}");
                }
            }
            fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                if f.name() == "message" {
                    self.0 = v.to_string();
                }
            }
        }
        let mut m = Msg(String::new());
        event.record(&mut m);
        self.records.lock().push((*event.metadata().level(), m.0));
    }
}

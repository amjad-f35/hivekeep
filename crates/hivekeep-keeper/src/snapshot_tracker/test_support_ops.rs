//! Fakes and helpers for the upload, detach and report tests.
//!
//! The tests run on paused tokio time, which drives the tracker's clock,
//! its queues' delays and its upload timeouts together. Waiting helpers
//! spin on `yield_now` rather than sleeping, so tokio never advances time
//! on its own: time moves only when a test steps it, and due times can be
//! compared exactly.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use parking_lot::Mutex;
use prometheus::Registry;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::Status;
use tracing::field::{Field, Visit};
use tracing::Level;
use tracing_subscriber::layer::{Context as LayerCtx, SubscriberExt};
use tracing_subscriber::Layer;

use super::op::{ActorOp, OpKind};
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
/// The tracker's first hold-back after failed background work.
pub(super) const RETRY_DELAY: Duration = SNAPSHOT_RETRY_DELAY;
pub(super) const MINUTE: Duration = Duration::from_secs(60);
pub(super) const SECOND: Duration = Duration::from_secs(1);

pub(super) fn t0() -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000, 0).unwrap()
}

/// The actor committed snapshots name.
pub(super) fn test_actor() -> ActorRef {
    ActorRef::new("demo", "counter")
}

/// A MEMORY snapshot of the tracked actor, naming no actor.
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

/// `test_ref`'s snapshot as a commit records it, naming the test actor.
pub(super) fn committed_ref(
    uuid: &str,
    generation: i32,
    commit: Option<DateTime<Utc>>,
) -> SnapshotRef {
    let mut r = test_ref(uuid, generation, commit);
    r.actor = test_actor();
    r
}

pub(super) fn add(t: DateTime<Utc>, d: Duration) -> DateTime<Utc> {
    crate::clock::add(t, d)
}

pub(super) fn sub(t: DateTime<Utc>, d: Duration) -> DateTime<Utc> {
    t - chrono::Duration::from_std(d).unwrap()
}

pub(super) fn uuid_of(r: &Option<pb::SnapshotReport>) -> &str {
    r.as_ref()
        .and_then(|r| r.snapshot.as_ref())
        .map(|s| s.snapshot_uuid.as_str())
        .unwrap_or("")
}

pub(super) fn time_of(ts: &Option<prost_types::Timestamp>) -> Option<DateTime<Utc>> {
    ts.as_ref()
        .map(|t| DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap())
}

/// The error a failing fake call returns.
pub(super) fn boom() -> Status {
    Status::internal("boom")
}

// ---------------------------------------------------------------------
// Waiting without advancing time.

/// Wall-clock budget for a wait; generous, since a loaded machine can stall
/// blocking-pool threads for a while.
const WAIT_BUDGET: Duration = Duration::from_secs(20);

async fn spin(i: usize) {
    if i % 32 == 31 {
        // Lets blocking-pool threads (file cleanup) make progress.
        std::thread::sleep(Duration::from_micros(200));
    }
    tokio::task::yield_now().await;
}

/// Runs `f` to completion without advancing tokio time, failing the test
/// if it does not finish soon.
pub(super) async fn within<F: Future>(what: &str, f: F) -> F::Output {
    tokio::pin!(f);
    let deadline = std::time::Instant::now() + WAIT_BUDGET;
    let mut i = 0;
    while std::time::Instant::now() < deadline {
        if let std::task::Poll::Ready(v) = futures::poll!(f.as_mut()) {
            return v;
        }
        spin(i).await;
        i += 1;
    }
    panic!("timed out waiting for {what}");
}

/// Waits until `cond` holds, without advancing tokio time.
pub(super) async fn eventually<F, Fut>(what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + WAIT_BUDGET;
    let mut i = 0;
    while std::time::Instant::now() < deadline {
        if cond().await {
            return;
        }
        spin(i).await;
        i += 1;
    }
    panic!("timed out waiting until {what}");
}

/// Lets background tasks run for a while without advancing tokio time.
pub(super) async fn settle() {
    for i in 0..400 {
        spin(i).await;
    }
}

/// Fails the test if `h` finishes while other tasks run for a while.
pub(super) async fn still_running<T>(what: &str, h: &tokio::task::JoinHandle<T>) {
    settle().await;
    assert!(!h.is_finished(), "{what} returned early");
}

/// Waits for a spawned task's result.
pub(super) async fn join<T>(what: &str, h: tokio::task::JoinHandle<T>) -> T {
    within(what, h).await.expect("task panicked")
}

/// Advances time to `at` unless it is there already, so the work due by
/// then comes due.
pub(super) async fn step_to(t: &Tracker, at: DateTime<Utc>) {
    let d = crate::clock::until(t.inner.now(), at);
    if !d.is_zero() {
        tokio::time::advance(d).await;
    }
    tokio::task::yield_now().await;
}

// ---------------------------------------------------------------------
// The fake store.

#[derive(Default)]
struct StoreState {
    calls: Vec<String>,
    actors: HashMap<String, ActorRef>,
    uploaded: HashSet<String>,
    blocking: bool,
    ignore_cancel: bool,
    err: Option<Status>,
    deadline: Option<Instant>,
}

/// A store that records its calls. A call for a URI it already holds
/// uploads nothing.
pub(super) struct FakeStore {
    st: Mutex<StoreState>,
    started: Mutex<VecDeque<String>>,
    canceled: Mutex<VecDeque<String>>,
    release_tx: mpsc::UnboundedSender<Option<Status>>,
    release_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<Option<Status>>>,
    released: AtomicUsize,
    taken: AtomicUsize,
}

impl FakeStore {
    pub(super) fn new() -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        Arc::new(FakeStore {
            st: Mutex::new(StoreState::default()),
            started: Mutex::new(VecDeque::new()),
            canceled: Mutex::new(VecDeque::new()),
            release_tx: tx,
            release_rx: tokio::sync::Mutex::new(rx),
            released: AtomicUsize::new(0),
            taken: AtomicUsize::new(0),
        })
    }

    /// Makes later calls wait for a release, or their context.
    pub(super) fn set_blocking(&self, b: bool) {
        self.st.lock().blocking = b;
    }

    /// Makes blocked calls note their context ending on `canceled` and go
    /// on waiting for a release.
    pub(super) fn set_ignore_cancel(&self, b: bool) {
        self.st.lock().ignore_cancel = b;
    }

    /// Makes later calls fail with `err`, or succeed.
    pub(super) fn set_err(&self, err: Option<Status>) {
        self.st.lock().err = err;
    }

    pub(super) fn uploads(&self) -> Vec<String> {
        self.st.lock().calls.clone()
    }

    pub(super) fn actor_of(&self, uuid: &str) -> Option<ActorRef> {
        self.st.lock().actors.get(uuid).cloned()
    }

    /// How long the latest call's context may run from now.
    pub(super) fn last_deadline_from_now(&self) -> Option<Duration> {
        self.st
            .lock()
            .deadline
            .map(|d| d.saturating_duration_since(Instant::now()))
    }

    /// Waits for the next call to start and returns its snapshot.
    pub(super) async fn wait_started(&self) -> String {
        within("an upload to start", async {
            loop {
                if let Some(u) = self.started.lock().pop_front() {
                    return u;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
    }

    /// The next started call, if one started already.
    pub(super) fn try_started(&self) -> Option<String> {
        self.started.lock().pop_front()
    }

    /// Waits for a blocked call that ignores cancellation to see its
    /// context end, and returns its snapshot.
    pub(super) async fn wait_canceled(&self) -> String {
        within("an upload to be canceled", async {
            loop {
                if let Some(u) = self.canceled.lock().pop_front() {
                    return u;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
    }

    /// Ends a blocked call with `err`, waiting until a call takes it.
    pub(super) async fn release(&self, err: Option<Status>) {
        let n = self.released.fetch_add(1, Ordering::SeqCst) + 1;
        self.release_tx.send(err).unwrap();
        eventually("a blocked upload to take the release", || async {
            self.taken.load(Ordering::SeqCst) >= n
        })
        .await;
    }

    async fn take_release(&self) -> Option<Status> {
        let v = self.release_rx.lock().await.recv().await.expect("open");
        self.taken.fetch_add(1, Ordering::SeqCst);
        v
    }
}

#[async_trait]
impl Store for FakeStore {
    async fn upload_local_snapshot(
        &self,
        ctx: &Ctx,
        _actor_uid: &str,
        r: &SnapshotRef,
    ) -> Result<bool, Status> {
        let (blocking, ignore_cancel, mut err) = {
            let mut st = self.st.lock();
            st.calls.push(r.uuid().to_string());
            st.actors.insert(r.uuid().to_string(), r.actor.clone());
            st.deadline = ctx.deadline();
            (st.blocking, st.ignore_cancel, st.err.clone())
        };
        self.started.lock().push_back(r.uuid().to_string());
        if blocking {
            let recv = self.take_release();
            tokio::pin!(recv);
            err = tokio::select! {
                biased;
                e = &mut recv => e,
                c = ctx.done() => {
                    if !ignore_cancel {
                        return Err(c.into());
                    }
                    self.canceled.lock().push_back(r.uuid().to_string());
                    recv.await
                }
            };
        }
        if let Some(e) = err {
            return Err(e);
        }
        let mut st = self.st.lock();
        Ok(st.uploaded.insert(r.uri().to_string()))
    }
}

/// A store whose upload succeeds just after it cancels `ctx`, as Forget or
/// the tracker's shutdown may end a detach.
pub(super) struct CancelingStore(pub(super) Ctx);

#[async_trait]
impl Store for CancelingStore {
    async fn upload_local_snapshot(
        &self,
        _: &Ctx,
        _: &str,
        _: &SnapshotRef,
    ) -> Result<bool, Status> {
        self.0.cancel();
        Ok(true)
    }
}

// ---------------------------------------------------------------------
// The fake reporter.

/// One report a fake reporter received.
#[derive(Clone)]
pub(super) struct Sent {
    pub(super) actor_uid: String,
    pub(super) actor: ActorRef,
    pub(super) state: pb::ActorSnapshotState,
}

#[derive(Default)]
struct ReporterState {
    sent: Vec<Sent>,
    errs: VecDeque<Status>,
    blocking: bool,
}

/// A reporter that records its reports.
pub(super) struct FakeReporter {
    st: Mutex<ReporterState>,
    reported: Mutex<VecDeque<Sent>>,
    release_tx: mpsc::UnboundedSender<Option<Status>>,
    release_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<Option<Status>>>,
    released: AtomicUsize,
    taken: AtomicUsize,
}

impl FakeReporter {
    pub(super) fn new() -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        Arc::new(FakeReporter {
            st: Mutex::new(ReporterState::default()),
            reported: Mutex::new(VecDeque::new()),
            release_tx: tx,
            release_rx: tokio::sync::Mutex::new(rx),
            released: AtomicUsize::new(0),
            taken: AtomicUsize::new(0),
        })
    }

    /// Makes the next reports fail with `errs`, in order.
    pub(super) fn fail_next(&self, errs: impl IntoIterator<Item = Status>) {
        self.st.lock().errs.extend(errs);
    }

    /// Makes later reports wait for a release, or their context.
    pub(super) fn set_blocking(&self, b: bool) {
        self.st.lock().blocking = b;
    }

    pub(super) fn reports(&self) -> Vec<Sent> {
        self.st.lock().sent.clone()
    }

    /// Waits for the next report to start, and returns it.
    pub(super) async fn wait_reported(&self) -> Sent {
        within("a report", async {
            loop {
                if let Some(s) = self.reported.lock().pop_front() {
                    return s;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
    }

    /// The next started report, if one started already.
    pub(super) fn try_reported(&self) -> Option<Sent> {
        self.reported.lock().pop_front()
    }

    /// Ends a blocked report with `err`, waiting until a report takes it.
    pub(super) async fn release(&self, err: Option<Status>) {
        let n = self.released.fetch_add(1, Ordering::SeqCst) + 1;
        self.release_tx.send(err).unwrap();
        eventually("a blocked report to take the release", || async {
            self.taken.load(Ordering::SeqCst) >= n
        })
        .await;
    }
}

#[async_trait]
impl Reporter for FakeReporter {
    async fn report_actor_snapshot_state(
        &self,
        ctx: &Ctx,
        actor_uid: &str,
        actor: &ActorRef,
        state: &pb::ActorSnapshotState,
    ) -> Result<(), Status> {
        let s = Sent {
            actor_uid: actor_uid.to_string(),
            actor: actor.clone(),
            state: state.clone(),
        };
        let (mut err, blocking) = {
            let mut st = self.st.lock();
            st.sent.push(s.clone());
            (st.errs.pop_front(), st.blocking)
        };
        self.reported.lock().push_back(s);
        if blocking {
            tokio::select! {
                biased;
                e = async {
                    let v = self.release_rx.lock().await.recv().await.expect("open");
                    self.taken.fetch_add(1, Ordering::SeqCst);
                    v
                } => err = e,
                c = ctx.done() => {
                    if err.is_none() {
                        // As a gRPC client fails a call whose context ends.
                        err = Some(c.into());
                    }
                }
            }
        }
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

// ---------------------------------------------------------------------
// Trackers.

/// Node directories holding the tracked actor's directory.
pub(super) struct Env {
    _dir: tempfile::TempDir,
    pub(super) paths: Arc<NodePaths>,
}

impl Env {
    pub(super) fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let paths = Arc::new(NodePaths::new(dir.path()));
        fs::create_dir_all(paths.actor_dir(UID)).unwrap();
        Env { _dir: dir, paths }
    }

    /// The test delays and timeouts, without workers' reports.
    pub(super) fn opts() -> Options {
        Options {
            upload_delay: UPLOAD_DELAY,
            detach_delay: DETACH_DELAY,
            upload_timeout: UPLOAD_TIMEOUT,
            upload_est_throughput: UPLOAD_EST_THROUGHPUT,
            ..Options::default()
        }
    }

    /// A tracker over `store` whose workers have not started, on a clock
    /// reading [`t0`] now.
    pub(super) fn tracker(&self, store: Option<Arc<dyn Store>>, opts: Options) -> Tracker {
        self.tracker_ctx(Ctx::background(), store, opts)
    }

    pub(super) fn tracker_ctx(
        &self,
        ctx: Ctx,
        store: Option<Arc<dyn Store>>,
        opts: Options,
    ) -> Tracker {
        Tracker::new(
            ctx,
            self.paths.clone(),
            store,
            Arc::new(MockClock::new(t0())),
            opts,
        )
    }

    /// A tracker over `store` with the test settings, not started.
    pub(super) fn idle(&self, store: &Arc<FakeStore>) -> Tracker {
        self.tracker(Some(store.clone()), Self::opts())
    }

    /// A tracker over `store` with the test settings, started.
    pub(super) async fn started(&self, store: &Arc<FakeStore>) -> Tracker {
        let t = self.idle(store);
        t.start().await;
        t
    }

    /// A started tracker reporting through `reporter` without report
    /// workers, so a test sends each report with [`process_next_report`].
    pub(super) async fn reporting(
        &self,
        store: &Arc<FakeStore>,
        reporter: &Arc<FakeReporter>,
    ) -> Tracker {
        let t = self.tracker(Some(store.clone()), reporting_opts(reporter));
        t.start().await;
        t
    }

    pub(super) fn local_copy(&self, uuid: &str) {
        fs::create_dir_all(self.paths.local_snapshot_dir(UID, uuid)).unwrap();
    }

    pub(super) fn local_exists(&self, uuid: &str) -> bool {
        self.paths.local_snapshot_dir(UID, uuid).exists()
    }

    /// Fills the actor's external volume mount directory, which fails the
    /// cleanup of a detach, and returns the file.
    pub(super) fn still_mounted(&self) -> std::path::PathBuf {
        let f = self
            .paths
            .external_volume_host_path(UID, "data")
            .join("payload");
        fs::create_dir_all(f.parent().unwrap()).unwrap();
        fs::write(&f, b"volume contents").unwrap();
        f
    }

    /// The tracked actor's state file, if readable.
    pub(super) fn read_state(&self) -> Option<ActorRecord> {
        let data = fs::read(self.paths.actor_snapshot_state_file(UID)).ok()?;
        serde_json::from_slice(&data).ok()
    }

    pub(super) fn report_due_on_disk(&self) -> bool {
        self.read_state()
            .is_some_and(|r| r.report_due_since.is_some())
    }
}

/// The test options with `reporter` and no report workers or jitter.
pub(super) fn reporting_opts(reporter: &Arc<FakeReporter>) -> Options {
    Options {
        reporter: Some(reporter.clone() as Arc<dyn Reporter>),
        report_workers: 0,
        report_startup_jitter: Duration::ZERO,
        ..Env::opts()
    }
}

pub(super) fn dyn_store(s: &Arc<FakeStore>) -> Option<Arc<dyn Store>> {
    Some(s.clone() as Arc<dyn Store>)
}

/// Commits `s` locally.
pub(super) async fn commit_local(t: &Tracker, s: SnapshotRef) -> pb::ActorSnapshotState {
    t.commit_local(UID, s).await
}

pub(super) fn now(t: &Tracker) -> DateTime<Utc> {
    t.inner.now()
}

/// The tracked actor's entry, if tracked.
pub(super) async fn entry(t: &Tracker, uid: &str) -> Option<ActorSnapshots> {
    let mut g = t.inner.lock(uid).await;
    if !t.inner.find(&mut g) {
        return None;
    }
    Some(g.a_ref().clone())
}

/// Whether the actor's entry stays in memory.
pub(super) async fn in_memory(t: &Tracker) -> bool {
    t.inner.lock(UID).await.has()
}

pub(super) async fn detach_due_of(t: &Tracker, uid: &str) -> Option<DateTime<Utc>> {
    entry(t, uid).await.and_then(|a| a.detach_due())
}

pub(super) async fn report_due(t: &Tracker) -> bool {
    entry(t, UID).await.is_some_and(|a| a.report_due())
}

/// The actor's operation in progress.
pub(super) async fn op_of(t: &Tracker, uid: &str) -> ActorOp {
    entry(t, uid)
        .await
        .and_then(|a| a.op)
        .expect("no operation in progress")
}

/// Waits for `op` to end.
pub(super) async fn wait_closed(op: &ActorOp, what: &str) {
    within(what, op.wait()).await
}

/// Steps to when the tracked actor's upload is due and waits for a worker
/// to start it; returns the upload.
pub(super) async fn start_upload(t: &Tracker, store: &FakeStore) -> ActorOp {
    let due = time_of(&t.state(UID).await.upload_due_time).expect("no upload scheduled");
    step_to(t, due).await;
    store.wait_started().await;
    op_of(t, UID).await
}

/// Runs the tracked actor's upload once due and ends it with `err`.
pub(super) async fn upload_now(t: &Tracker, store: &FakeStore, err: Option<Status>) {
    let op = start_upload(t, store).await;
    store.release(err).await;
    wait_closed(&op, "the upload").await;
}

/// Steps to when the tracked actor's detach is due and waits for a worker
/// to detach it.
pub(super) async fn detach_now(t: &Tracker) {
    let due = detach_due_of(t, UID).await.expect("no detach scheduled");
    step_to(t, due).await;
    eventually("the actor is detached", || async {
        entry(t, UID)
            .await
            .is_some_and(|a| a.op.is_none() && a.rec.latest_local.is_none())
    })
    .await;
}

/// Runs a Detach of the tracked actor in the background.
pub(super) fn start_detach(
    t: &Tracker,
) -> tokio::task::JoinHandle<Result<pb::ActorSnapshotState, Status>> {
    let t = t.clone();
    tokio::spawn(async move { t.detach(&Ctx::background(), UID).await })
}

/// Takes the next actor from `kind`'s queue of a tracker whose workers have
/// not started, and runs its background work as a worker would.
pub(super) async fn process_next(t: &Tracker, kind: OpKind) {
    let q = t.inner.queues[kind as usize].clone();
    let uid = within("an actor to be queued", q.get())
        .await
        .expect("queue shut down");
    if let Some(run) = t.inner.begin_due_work(&uid, kind).await {
        run.await;
    }
    q.done(&uid);
}

/// Actors queued for a worker now, leaving out those queued for later.
pub(super) fn queued_now(t: &Tracker) -> usize {
    t.inner.queues.iter().map(|q| q.len()).sum()
}

pub(super) fn queue_len(t: &Tracker, kind: OpKind) -> usize {
    t.inner.queues[kind as usize].len()
}

pub(super) fn report_queue_len(t: &Tracker) -> usize {
    t.inner.reporting.queue.len()
}

/// Takes the next actor from the report queue of a tracker without report
/// workers and reports it as a report worker would, returning the queue
/// pause the report asks for.
pub(super) async fn process_next_report(t: &Tracker) -> Duration {
    let q = t.inner.reporting.queue.clone();
    let uid = within("a report to be queued", q.get())
        .await
        .expect("queue shut down");
    let d = t.inner.report(&uid).await;
    q.done(&uid);
    d
}

// ---------------------------------------------------------------------
// Logs and metrics.

/// The levels of the events logged with one message.
#[derive(Clone, Default)]
pub(super) struct LogLevels(Arc<Mutex<Vec<Level>>>);

impl LogLevels {
    pub(super) fn get(&self) -> Vec<Level> {
        self.0.lock().clone()
    }
}

thread_local! {
    /// The message this thread records the levels of, and where.
    static RECORDING: std::cell::RefCell<Option<(String, LogLevels)>> =
        const { std::cell::RefCell::new(None) };
}

/// Sends each event to the recording of the thread logging it.
struct LevelLayer;

struct MessageVisitor(Option<String>);

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = Some(format!("{value:?}"));
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0 = Some(value.to_string());
        }
    }
}

impl<S: tracing::Subscriber> Layer<S> for LevelLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _: LayerCtx<'_, S>) {
        let _ = RECORDING.try_with(|r| {
            if let Some((msg, levels)) = &*r.borrow() {
                let mut v = MessageVisitor(None);
                event.record(&mut v);
                if v.0.as_deref() == Some(msg.as_str()) {
                    levels.0.lock().push(*event.metadata().level());
                }
            }
        });
    }
}

/// Stops this thread's recording when dropped.
pub(super) struct RecordingGuard {
    _scoped: Option<tracing::subscriber::DefaultGuard>,
}

impl Drop for RecordingGuard {
    fn drop(&mut self) {
        let _ = RECORDING.try_with(|r| r.borrow_mut().take());
    }
}

/// Records the level of each event this thread logs with `msg` while the
/// returned guard lives. A process-wide subscriber feeds the recording, so
/// callsites cached by other threads' tests cannot hide events; when some
/// other process-wide subscriber is installed already, a scoped one is.
pub(super) fn record_log_levels(msg: &str) -> (RecordingGuard, LogLevels) {
    static GLOBAL: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let global = *GLOBAL.get_or_init(|| {
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(LevelLayer))
            .is_ok()
    });
    let scoped = (!global)
        .then(|| tracing::subscriber::set_default(tracing_subscriber::registry().with(LevelLayer)));
    let levels = LogLevels::default();
    RECORDING.with(|r| *r.borrow_mut() = Some((msg.to_string(), levels.clone())));
    (RecordingGuard { _scoped: scoped }, levels)
}

/// Instruments on a fresh registry.
pub(super) fn test_instruments() -> (Instruments, Registry) {
    let reg = Registry::new();
    (Instruments::new(&reg).unwrap(), reg)
}

/// The error types of the recorded background uploads, one per upload.
pub(super) fn recorded_upload_error_types(reg: &Registry) -> Vec<String> {
    let mut out = Vec::new();
    for fam in reg.gather() {
        if fam.name() != "hivekeep_keeper_snapshot_upload_duration_seconds" {
            continue;
        }
        for m in fam.get_metric() {
            let et = m
                .get_label()
                .iter()
                .find(|l| l.name() == "error_type")
                .map(|l| l.value().to_string())
                .unwrap_or_default();
            for _ in 0..m.get_histogram().get_sample_count() {
                out.push(et.clone());
            }
        }
    }
    out
}

/// Writes `size` bytes of non-zero data, so the file takes its full size.
pub(super) fn write_dense_file(path: &std::path::Path, size: usize) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, vec![0xa5u8; size]).unwrap();
}

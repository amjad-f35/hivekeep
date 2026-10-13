//! Tracker behaviour tests on paused tokio time.

use std::collections::VecDeque;
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tonic::{Code, Status};

use super::*;
use crate::clock::MockClock;
use crate::context::Ctx;
use crate::paths::NodePaths;

const UID: &str = "actor-uid-1";
const HOUR: Duration = Duration::from_secs(3600);

fn t0() -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000, 0).unwrap()
}

fn actor() -> ActorRef {
    ActorRef::new("demo", "counter")
}

fn snap(uuid: &str, generation: i32) -> SnapshotRef {
    SnapshotRef {
        snapshot: pb::Snapshot {
            snapshot_uuid: uuid.into(),
            generation,
            fidelity: pb::SnapshotFidelity::Memory as i32,
            object: Some(pb::ObjectStorage {
                snapshot_uri: format!("gs://bucket/hives/demo/actors/{UID}/snapshots/{uuid}"),
                sharing: 0,
            }),
        },
        commit_time: None,
        upload_time: None,
        actor: actor(),
    }
}

/// A store that records its calls; it fails while `fail` is set, and waits
/// for a permit while `block` is set.
struct FakeStore {
    calls: Mutex<Vec<String>>,
    fail: Mutex<Option<Status>>,
    block: AtomicBool,
    permits: Semaphore,
    uploaded: AtomicBool,
}

impl FakeStore {
    fn new() -> Arc<Self> {
        Arc::new(FakeStore {
            calls: Mutex::new(Vec::new()),
            fail: Mutex::new(None),
            block: AtomicBool::new(false),
            permits: Semaphore::new(0),
            uploaded: AtomicBool::new(true),
        })
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }
}

#[async_trait]
impl Store for FakeStore {
    async fn upload_local_snapshot(
        &self,
        ctx: &Ctx,
        _uid: &str,
        r: &SnapshotRef,
    ) -> Result<bool, Status> {
        self.calls.lock().push(r.uuid().to_string());
        if self.block.load(Ordering::SeqCst) {
            ctx.run(self.permits.acquire()).await?.unwrap().forget();
        }
        if let Some(e) = self.fail.lock().clone() {
            return Err(e);
        }
        Ok(self.uploaded.load(Ordering::SeqCst))
    }
}

/// A reporter that records what it is sent and answers from a script, then
/// with success.
#[derive(Default)]
struct FakeReporter {
    sent: Mutex<Vec<pb::ActorSnapshotState>>,
    script: Mutex<VecDeque<Status>>,
    always: Mutex<Option<Status>>,
    calls: AtomicUsize,
}

#[async_trait]
impl Reporter for FakeReporter {
    async fn report_actor_snapshot_state(
        &self,
        _ctx: &Ctx,
        _uid: &str,
        _actor: &ActorRef,
        state: &pb::ActorSnapshotState,
    ) -> Result<(), Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.sent.lock().push(state.clone());
        if let Some(e) = self.always.lock().clone() {
            return Err(e);
        }
        match self.script.lock().pop_front() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

struct Env {
    _dir: tempfile::TempDir,
    paths: Arc<NodePaths>,
    ctx: Ctx,
}

impl Env {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let paths = Arc::new(NodePaths::new(dir.path()));
        fs::create_dir_all(paths.actor_dir(UID)).unwrap();
        Env {
            _dir: dir,
            paths,
            ctx: Ctx::background(),
        }
    }

    fn opts() -> Options {
        Options {
            upload_delay: HOUR,
            detach_delay: 4 * HOUR,
            upload_timeout: Duration::from_secs(45 * 60),
            upload_est_throughput: 1 << 20,
            report_startup_jitter: Duration::ZERO,
            ..Options::default()
        }
    }

    fn tracker(&self, store: Option<Arc<FakeStore>>, opts: Options) -> Tracker {
        let store = store.map(|s| s as Arc<dyn Store>);
        Tracker::new(
            self.ctx.clone(),
            self.paths.clone(),
            store,
            Arc::new(MockClock::new(t0())),
            opts,
        )
    }

    /// A local snapshot directory with one file and a manifest.
    fn local_copy(&self, uuid: &str) {
        let dir = self.paths.local_snapshot_dir(UID, uuid);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("mem"), vec![1u8; 4096]).unwrap();
    }
}

/// Commits `s` locally at the current mock time.
async fn commit(t: &Tracker, mut s: SnapshotRef) -> pb::ActorSnapshotState {
    s.commit_time = Some(t.inner.now());
    t.commit_local(UID, s).await
}

/// Lets background tasks run, advancing paused time a little.
async fn settle() {
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..500 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

fn uuid_of(r: &Option<pb::SnapshotReport>) -> &str {
    r.as_ref()
        .and_then(|r| r.snapshot.as_ref())
        .map(|s| s.snapshot_uuid.as_str())
        .unwrap_or("")
}

#[tokio::test(start_paused = true)]
async fn state_reports_local_snapshot_and_upload_due() {
    let env = Env::new();
    let t = env.tracker(Some(FakeStore::new()), Env::opts());
    let st = commit(&t, snap("s1", 1)).await;
    assert_eq!(uuid_of(&st.latest_local), "s1");
    assert!(st.latest_durable.is_none());
    let due = st.upload_due_time.unwrap();
    assert_eq!(due.seconds, t0().timestamp() + 3600);
    // The state was saved.
    assert!(env.paths.actor_snapshot_state_file(UID).exists());
    assert_eq!(uuid_of(&t.state(UID).await.latest_local), "s1");
}

#[tokio::test(start_paused = true)]
async fn commits_skip_older_generations() {
    let env = Env::new();
    let t = env.tracker(Some(FakeStore::new()), Env::opts());
    commit(&t, snap("s2", 2)).await;
    let st = commit(&t, snap("s1", 1)).await;
    assert_eq!(uuid_of(&st.latest_local), "s2");
    let st = t.commit_durable(UID, snap("s3", 3)).await;
    assert_eq!(uuid_of(&st.latest_durable), "s3");
    assert!(
        st.latest_local.is_none(),
        "an older local snapshot is dropped"
    );
    let st = t.commit_durable(UID, snap("s1", 1)).await;
    assert_eq!(uuid_of(&st.latest_durable), "s3");
}

#[tokio::test(start_paused = true)]
async fn commit_promoted_keeps_commit_time_and_detach_schedule() {
    let env = Env::new();
    let t = env.tracker(Some(FakeStore::new()), Env::opts());
    commit(&t, snap("s1", 1)).await;
    let st = t.commit_promoted(UID, snap("s1", 1)).await;
    let local = st.latest_local.unwrap();
    let durable = st.latest_durable.unwrap();
    assert_eq!(local.local_commit_time, durable.local_commit_time);
    assert!(st.upload_due_time.is_none(), "nothing left to upload");
    let mut g = t.inner.lock(UID).await;
    assert!(t.inner.find(&mut g));
    assert!(g.a_ref().detach_scheduled());
}

#[tokio::test(start_paused = true)]
async fn begin_checkpoint_fences_generations_and_reports_committed() {
    let env = Env::new();
    let t = env.tracker(Some(FakeStore::new()), Env::opts());
    let ctx = Ctx::background();
    commit(&t, snap("s2", 2)).await;
    let mode = pb::UploadMode::Deferred;
    let err = t
        .begin_checkpoint(&ctx, UID, &snap("s1", 1).snapshot, mode)
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), Code::Aborted);
    let err = t
        .begin_checkpoint(&ctx, UID, &snap("other", 2).snapshot, mode)
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), Code::Aborted);
    let mut moved = snap("s2", 2);
    moved.snapshot.object.as_mut().unwrap().snapshot_uri = "gs://elsewhere/x".into();
    let err = t
        .begin_checkpoint(&ctx, UID, &moved.snapshot, mode)
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), Code::FailedPrecondition);
    let (committed, end) = t
        .begin_checkpoint(&ctx, UID, &snap("s2", 2).snapshot, mode)
        .await
        .unwrap();
    assert!(committed);
    end.end().await;
    let (committed, end) = t
        .begin_checkpoint(&ctx, UID, &snap("s3", 3).snapshot, mode)
        .await
        .unwrap();
    assert!(!committed);
    end.end().await;
}

#[tokio::test(start_paused = true)]
async fn not_started_since_follows_starts_and_commits() {
    let env = Env::new();
    let t = env.tracker(Some(FakeStore::new()), Env::opts());
    commit(&t, snap("s1", 1)).await;
    assert!(t.not_started_since(UID, &snap("s1", 1).snapshot).await);
    t.begin_start(&Ctx::background(), UID)
        .await
        .unwrap()
        .end()
        .await;
    assert!(!t.not_started_since(UID, &snap("s1", 1).snapshot).await);
    commit(&t, snap("s2", 2)).await;
    assert!(t.not_started_since(UID, &snap("s2", 2).snapshot).await);
}

#[tokio::test(start_paused = true)]
async fn uploads_latest_local_once_due() {
    let env = Env::new();
    let store = FakeStore::new();
    let rep = Arc::new(FakeReporter::default());
    let mut opts = Env::opts();
    opts.reporter = Some(rep.clone());
    let t = env.tracker(Some(store.clone()), opts);
    t.start().await;
    env.local_copy("s1");
    commit(&t, snap("s1", 1)).await;
    tokio::time::advance(HOUR / 2).await;
    commit(&t, snap("s2", 2)).await;
    settle().await;
    assert!(store.calls().is_empty(), "nothing is due yet");
    tokio::time::advance(HOUR / 2).await;
    eventually("the upload", || !store.calls().is_empty()).await;
    // The latest local snapshot is uploaded, once, within an upload delay
    // of the oldest undurable one.
    assert_eq!(store.calls(), vec!["s2"]);
    eventually("the report", || rep.calls.load(Ordering::SeqCst) == 1).await;
    let st = t.state(UID).await;
    assert_eq!(uuid_of(&st.latest_durable), "s2");
    assert!(st.uploading.is_none() && st.upload_due_time.is_none());
    assert!(st.latest_durable.unwrap().durable_complete_time.is_some());
    assert_eq!(uuid_of(&rep.sent.lock()[0].latest_durable), "s2");
}

#[tokio::test(start_paused = true)]
async fn retries_a_failed_upload_after_backoff() {
    let env = Env::new();
    let store = FakeStore::new();
    *store.fail.lock() = Some(Status::unavailable("down"));
    let t = env.tracker(Some(store.clone()), Env::opts());
    t.start().await;
    commit(&t, snap("s1", 1)).await;
    tokio::time::advance(HOUR).await;
    eventually("the first attempt", || store.calls().len() == 1).await;
    settle().await;
    let st = t.state(UID).await;
    assert_eq!(
        uuid_of(&st.uploading),
        "s1",
        "a failed upload is kept to retry"
    );
    *store.fail.lock() = None;
    tokio::time::advance(SNAPSHOT_RETRY_DELAY).await;
    eventually("the retry", || store.calls().len() == 2).await;
    settle().await;
    assert_eq!(uuid_of(&t.state(UID).await.latest_durable), "s1");
}

#[tokio::test(start_paused = true)]
async fn checkpoint_during_upload_lets_it_finish_then_uploads_the_newer() {
    let env = Env::new();
    let store = FakeStore::new();
    store.block.store(true, Ordering::SeqCst);
    let t = env.tracker(Some(store.clone()), Env::opts());
    t.start().await;
    env.local_copy("s1");
    commit(&t, snap("s1", 1)).await;
    tokio::time::advance(HOUR).await;
    eventually("the upload of s1", || store.calls().len() == 1).await;
    let st = commit(&t, snap("s2", 2)).await;
    assert_eq!(uuid_of(&st.uploading), "s1");
    assert_eq!(
        t.local_snapshots_to_keep(UID).await,
        vec!["s2".to_string(), "s1".to_string()],
        "the upload's copy is kept"
    );
    store.permits.add_permits(1);
    eventually("s1 durable", || {
        futures::executor::block_on(t.state(UID))
            .latest_durable
            .is_some()
    })
    .await;
    assert!(
        !env.paths.local_snapshot_dir(UID, "s1").exists(),
        "the superseded copy is removed"
    );
    store.block.store(false, Ordering::SeqCst);
    tokio::time::advance(HOUR).await;
    eventually("the upload of s2", || store.calls().len() == 2).await;
    settle().await;
    assert_eq!(uuid_of(&t.state(UID).await.latest_durable), "s2");
}

#[tokio::test(start_paused = true)]
async fn commit_durable_cancels_an_older_upload_before_returning() {
    let env = Env::new();
    let store = FakeStore::new();
    store.block.store(true, Ordering::SeqCst);
    let t = env.tracker(Some(store.clone()), Env::opts());
    t.start().await;
    commit(&t, snap("s1", 1)).await;
    tokio::time::advance(HOUR).await;
    eventually("the upload of s1", || store.calls().len() == 1).await;
    let st = t.commit_durable(UID, snap("s2", 2)).await;
    assert_eq!(uuid_of(&st.latest_durable), "s2");
    assert!(st.uploading.is_none());
    let mut g = t.inner.lock(UID).await;
    assert!(t.inner.find(&mut g));
    assert!(g.a_ref().op.is_none(), "the older upload has stopped");
}

#[tokio::test(start_paused = true)]
async fn forget_cancels_an_upload_and_removes_state() {
    let env = Env::new();
    let store = FakeStore::new();
    store.block.store(true, Ordering::SeqCst);
    let t = env.tracker(Some(store.clone()), Env::opts());
    t.start().await;
    commit(&t, snap("s1", 1)).await;
    tokio::time::advance(HOUR).await;
    eventually("the upload", || store.calls().len() == 1).await;
    let end = t.begin_terminate(UID).await;
    t.forget(UID).await.unwrap();
    end.end().await;
    assert!(!env.paths.actor_snapshot_state_file(UID).exists());
    assert_eq!(t.state(UID).await, pb::ActorSnapshotState::default());
}

#[tokio::test(start_paused = true)]
async fn background_detach_runs_when_due_and_cleans_up_after_its_report() {
    let env = Env::new();
    let store = FakeStore::new();
    let rep = Arc::new(FakeReporter::default());
    let mut opts = Env::opts();
    opts.reporter = Some(rep.clone());
    let t = env.tracker(Some(store.clone()), opts);
    t.start().await;
    env.local_copy("s1");
    commit(&t, snap("s1", 1)).await;
    tokio::time::advance(4 * HOUR).await;
    eventually("the detach", || {
        !env.paths.local_snapshot_dir(UID, "s1").exists()
    })
    .await;
    eventually("the detach report", || {
        rep.sent
            .lock()
            .iter()
            .any(|s| s.latest_local.is_none() && s.latest_durable.is_some())
    })
    .await;
    eventually("the cleanup", || !env.paths.actor_dir(UID).exists()).await;
}

#[tokio::test(start_paused = true)]
async fn a_start_unschedules_the_detach() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = env.tracker(Some(store.clone()), Env::opts());
    t.start().await;
    env.local_copy("s1");
    commit(&t, snap("s1", 1)).await;
    t.begin_start(&Ctx::background(), UID)
        .await
        .unwrap()
        .end()
        .await;
    let err = t.detach(&Ctx::background(), UID).await.unwrap_err();
    assert_eq!(err.code(), Code::Aborted);
    tokio::time::advance(5 * HOUR).await;
    settle().await;
    assert!(
        env.paths.local_snapshot_dir(UID, "s1").exists(),
        "never detached"
    );
}

#[tokio::test(start_paused = true)]
async fn detach_call_uploads_then_deletes() {
    let env = Env::new();
    let store = FakeStore::new();
    let t = env.tracker(Some(store.clone()), Env::opts());
    env.local_copy("s1");
    commit(&t, snap("s1", 1)).await;
    let st = t.detach(&Ctx::background(), UID).await.unwrap();
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    assert_eq!(store.calls(), vec!["s1"]);
    assert!(!env.paths.actor_snapshot_dir(UID).exists());
    assert!(
        env.paths.actor_snapshot_state_file(UID).exists(),
        "kept until reported"
    );
    // A repeat finds nothing local and still returns LatestDurable.
    let st = t.detach(&Ctx::background(), UID).await.unwrap();
    assert_eq!(uuid_of(&st.latest_durable), "s1");
}

#[tokio::test(start_paused = true)]
async fn failed_detach_keeps_the_local_snapshot() {
    let env = Env::new();
    let store = FakeStore::new();
    *store.fail.lock() = Some(Status::unavailable("down"));
    let t = env.tracker(Some(store.clone()), Env::opts());
    env.local_copy("s1");
    commit(&t, snap("s1", 1)).await;
    let err = t.detach(&Ctx::background(), UID).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    assert!(env.paths.local_snapshot_dir(UID, "s1").exists());
    assert_eq!(uuid_of(&t.state(UID).await.latest_local), "s1");
}

#[tokio::test(start_paused = true)]
async fn detach_without_store_is_unimplemented() {
    let env = Env::new();
    let t = env.tracker(None, Env::opts());
    assert_eq!(
        t.detach(&Ctx::background(), UID).await.unwrap_err().code(),
        Code::Unimplemented
    );
}

#[tokio::test(start_paused = true)]
async fn without_store_nothing_is_scheduled() {
    let env = Env::new();
    let t = env.tracker(None, Env::opts());
    t.start().await;
    let st = commit(&t, snap("s1", 1)).await;
    assert!(st.upload_due_time.is_none());
}

#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn begin_start_fails_if_it_cannot_save() {
    if unsafe { libc::geteuid() } == 0 {
        return; // root ignores directory modes
    }
    let env = Env::new();
    let t = env.tracker(Some(FakeStore::new()), Env::opts());
    commit(&t, snap("s1", 1)).await;
    crate::atomicfile::chmod(&env.paths.actor_dir(UID), 0o500).unwrap();
    let err = t.begin_start(&Ctx::background(), UID).await.err().unwrap();
    crate::atomicfile::chmod(&env.paths.actor_dir(UID), 0o700).unwrap();
    assert_eq!(err.code(), Code::Unavailable);
    // The detach stays scheduled.
    let mut g = t.inner.lock(UID).await;
    assert!(t.inner.find(&mut g));
    assert!(g.a_ref().detach_scheduled());
}

#[tokio::test(start_paused = true)]
async fn retries_a_failed_report_then_gives_up() {
    let env = Env::new();
    let store = FakeStore::new();
    let rep = Arc::new(FakeReporter::default());
    *rep.always.lock() = Some(Status::unavailable("control plane down"));
    let mut opts = Env::opts();
    opts.reporter = Some(rep.clone());
    opts.report_give_up_after = Duration::from_secs(60);
    let t = env.tracker(Some(store.clone()), opts);
    t.start().await;
    commit(&t, snap("s1", 1)).await;
    tokio::time::advance(HOUR).await;
    eventually("the first report", || rep.calls.load(Ordering::SeqCst) >= 1).await;
    tokio::time::advance(Duration::from_secs(30)).await;
    eventually("a retry", || rep.calls.load(Ordering::SeqCst) >= 2).await;
    tokio::time::advance(Duration::from_secs(120)).await;
    settle().await;
    tokio::time::advance(Duration::from_secs(600)).await;
    settle().await;
    let mut g = t.inner.lock(UID).await;
    assert!(t.inner.find(&mut g));
    assert!(!g.a_ref().report_due(), "gave up");
}

#[tokio::test(start_paused = true)]
async fn a_refused_report_is_dropped() {
    let env = Env::new();
    let store = FakeStore::new();
    let rep = Arc::new(FakeReporter::default());
    rep.script
        .lock()
        .push_back(Status::failed_precondition("conflict"));
    let mut opts = Env::opts();
    opts.reporter = Some(rep.clone());
    let t = env.tracker(Some(store.clone()), opts);
    t.start().await;
    commit(&t, snap("s1", 1)).await;
    tokio::time::advance(HOUR).await;
    eventually("the report", || rep.calls.load(Ordering::SeqCst) == 1).await;
    settle().await;
    tokio::time::advance(HOUR).await;
    settle().await;
    assert_eq!(rep.calls.load(Ordering::SeqCst), 1, "not retried");
    assert!(env.paths.actor_snapshot_state_file(UID).exists());
}

#[tokio::test(start_paused = true)]
async fn a_report_waits_for_the_rpc_holding_the_actor() {
    let env = Env::new();
    let store = FakeStore::new();
    let rep = Arc::new(FakeReporter::default());
    let mut opts = Env::opts();
    opts.reporter = Some(rep.clone());
    let t = env.tracker(Some(store.clone()), opts);
    t.start().await;
    commit(&t, snap("s1", 1)).await;
    let end = t.begin_start(&Ctx::background(), UID).await.unwrap();
    tokio::time::advance(HOUR).await;
    eventually("the upload", || store.calls().len() == 1).await;
    settle().await;
    assert_eq!(rep.calls.load(Ordering::SeqCst), 0, "deferred while held");
    end.end().await;
    eventually("the deferred report", || {
        rep.calls.load(Ordering::SeqCst) == 1
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_commit_cancels_a_pending_report() {
    let env = Env::new();
    let store = FakeStore::new();
    let rep = Arc::new(FakeReporter::default());
    *rep.always.lock() = Some(Status::unavailable("down"));
    let mut opts = Env::opts();
    opts.reporter = Some(rep.clone());
    let t = env.tracker(Some(store.clone()), opts);
    t.start().await;
    commit(&t, snap("s1", 1)).await;
    tokio::time::advance(HOUR).await;
    eventually("the first report", || rep.calls.load(Ordering::SeqCst) == 1).await;
    commit(&t, snap("s2", 2)).await;
    let mut g = t.inner.lock(UID).await;
    assert!(t.inner.find(&mut g));
    assert!(!g.a_ref().report_due());
}

#[tokio::test(start_paused = true)]
async fn start_resumes_an_upload_after_restart() {
    let env = Env::new();
    {
        let t = env.tracker(Some(FakeStore::new()), Env::opts());
        commit(&t, snap("s1", 1)).await;
    }
    let mut rec: ActorRecord =
        serde_json::from_slice(&fs::read(env.paths.actor_snapshot_state_file(UID)).unwrap())
            .unwrap();
    rec.uploading = rec.latest_local.clone();
    fs::write(
        env.paths.actor_snapshot_state_file(UID),
        serde_json::to_vec(&rec).unwrap(),
    )
    .unwrap();
    let store = FakeStore::new();
    let t = env.tracker(Some(store.clone()), Env::opts());
    t.start().await;
    tokio::time::advance(HOUR).await;
    eventually("the resumed upload", || store.calls() == vec!["s1"]).await;
}

#[tokio::test(start_paused = true)]
async fn start_prunes_local_snapshots_from_an_earlier_boot() {
    let env = Env::new();
    {
        let t = env.tracker(Some(FakeStore::new()), Env::opts());
        env.local_copy("s1");
        commit(&t, snap("s1", 1)).await;
        t.commit_promoted(UID, snap("s1", 1)).await;
    }
    let rep = Arc::new(FakeReporter::default());
    let mut opts = Env::opts();
    opts.boot_check = Some(Arc::new(|_, _| false));
    opts.reporter = Some(rep.clone());
    let t = env.tracker(Some(FakeStore::new()), opts);
    t.start().await;
    assert!(!env.paths.local_snapshot_dir(UID, "s1").exists());
    let st = t.state(UID).await;
    assert!(st.latest_local.is_none());
    assert_eq!(uuid_of(&st.latest_durable), "s1");
    eventually("the detach report", || {
        rep.calls.load(Ordering::SeqCst) == 1
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn an_unuploaded_snapshot_from_an_earlier_boot_is_lost() {
    let env = Env::new();
    {
        let t = env.tracker(Some(FakeStore::new()), Env::opts());
        env.local_copy("s1");
        commit(&t, snap("s1", 1)).await;
    }
    let mut opts = Env::opts();
    opts.boot_check = Some(Arc::new(|_, _| false));
    let t = env.tracker(Some(FakeStore::new()), opts);
    t.start().await;
    assert_eq!(t.state(UID).await, pb::ActorSnapshotState::default());
}

#[tokio::test(start_paused = true)]
async fn wait_returns_once_the_context_ends() {
    let env = Env::new();
    let t = env.tracker(Some(FakeStore::new()), Env::opts());
    t.start().await;
    env.ctx.cancel();
    t.wait(&Ctx::background().with_timeout(Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(
        t.detach(&Ctx::background(), UID)
            .await
            .unwrap()
            .latest_local,
        None,
        "nothing to detach"
    );
}

#[tokio::test(start_paused = true)]
async fn upload_timeout_scales_with_size_and_failures() {
    let env = Env::new();
    let t = env.tracker(Some(FakeStore::new()), Env::opts());
    let dir = env.paths.local_snapshot_dir(UID, "s1");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("mem"), vec![7u8; 3 << 20]).unwrap();
    let base = t.inner.upload_timeout_for(UID, &snap("s1", 1));
    assert!(base >= Duration::from_secs(45 * 60 + 3), "{base:?}");
    for _ in 0..5 {
        t.inner.retries.when(UID);
    }
    assert_eq!(
        t.inner.upload_timeout_for(UID, &snap("s1", 1)),
        base * 8,
        "capped at 8x"
    );
}

#[tokio::test(start_paused = true)]
async fn unreadable_state_is_skipped() {
    let env = Env::new();
    fs::write(env.paths.actor_snapshot_state_file(UID), b"{not json").unwrap();
    let t = env.tracker(Some(FakeStore::new()), Env::opts());
    assert_eq!(t.state(UID).await, pb::ActorSnapshotState::default());
}

#[tokio::test(start_paused = true)]
async fn an_actor_without_a_directory_is_not_saved() {
    let env = Env::new();
    fs::remove_dir_all(env.paths.actor_dir(UID)).unwrap();
    let t = env.tracker(Some(FakeStore::new()), Env::opts());
    commit(&t, snap("s1", 1)).await;
    assert!(!env.paths.actor_snapshot_state_file(UID).exists());
}

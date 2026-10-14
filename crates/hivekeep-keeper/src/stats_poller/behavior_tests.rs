//! Sweeping workers' usage samples into per-template aggregates, CPU
//! baselines per activation, and worker pool labels.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hivekeep_proto::worker::v1 as wpb;
use parking_lot::Mutex;
use prometheus::Registry;
use tonic::Status;

use super::*;
use crate::sandbox_syncer::WorkerPod;
use crate::sandbox_versions::SandboxConfig;
use crate::worker_dialer::WorkerApi;

/// When the fixture's keeper started. Samples default to epoch zero, an
/// activation from before it; later epochs began after.
const STARTED_AT: i64 = 1000;

/// One worker socket: a canned response or error.
#[derive(Default)]
struct FakeWorker {
    resp: Mutex<wpb::GetActiveWorkloadStatsResponse>,
    err: Mutex<Option<String>>,
    calls: Mutex<usize>,
    saw_deadline: Mutex<bool>,
}

impl FakeWorker {
    fn with(samples: Vec<wpb::WorkloadStatsSample>) -> Arc<Self> {
        let w = Arc::new(FakeWorker::default());
        w.set(samples);
        w
    }
    fn failing(err: &str) -> Arc<Self> {
        let w = Arc::new(FakeWorker::default());
        *w.err.lock() = Some(err.into());
        w
    }
    fn set(&self, samples: Vec<wpb::WorkloadStatsSample>) {
        *self.resp.lock() = wpb::GetActiveWorkloadStatsResponse { samples };
    }
    fn calls(&self) -> usize {
        *self.calls.lock()
    }
}

#[async_trait]
impl WorkerApi for FakeWorker {
    async fn run_workload(
        &self,
        _: &Ctx,
        _: wpb::RunWorkloadRequest,
    ) -> Result<wpb::RunWorkloadResponse, Status> {
        unimplemented!()
    }
    async fn checkpoint_workload(
        &self,
        _: &Ctx,
        _: wpb::CheckpointWorkloadRequest,
    ) -> Result<wpb::CheckpointWorkloadResponse, Status> {
        unimplemented!()
    }
    async fn restore_workload(
        &self,
        _: &Ctx,
        _: wpb::RestoreWorkloadRequest,
    ) -> Result<wpb::RestoreWorkloadResponse, Status> {
        unimplemented!()
    }
    async fn terminate_workload(
        &self,
        _: &Ctx,
        _: wpb::TerminateWorkloadRequest,
    ) -> Result<(), Status> {
        unimplemented!()
    }
    async fn get_active_workload_stats(
        &self,
        ctx: &Ctx,
    ) -> Result<wpb::GetActiveWorkloadStatsResponse, Status> {
        *self.calls.lock() += 1;
        *self.saw_deadline.lock() = ctx.deadline().is_some();
        if let Some(e) = self.err.lock().clone() {
            return Err(Status::unavailable(e));
        }
        Ok(self.resp.lock().clone())
    }
}

/// Dials the fakes by pod UID; a UID without one fails to dial, as a stale
/// directory whose socket is gone does.
struct Fakes(HashMap<String, Arc<FakeWorker>>);

#[async_trait]
impl WorkerConnector for Fakes {
    async fn connect(&self, pod_uid: &str) -> Result<Arc<dyn WorkerApi>, Status> {
        match self.0.get(pod_uid) {
            Some(w) => Ok(w.clone()),
            None => Err(Status::unavailable("no such socket")),
        }
    }
    fn socket_path(&self, _: &str) -> PathBuf {
        PathBuf::new()
    }
}

type PodList = Arc<Mutex<Result<Vec<WorkerPod>, String>>>;

/// A cluster whose worker pod list the test sets.
struct Pods(PodList);

#[async_trait]
impl ClusterView for Pods {
    async fn worker_pods(&self) -> Result<Vec<WorkerPod>, String> {
        self.0.lock().clone()
    }
    async fn pool_sandbox_config(&self, _: &str, _: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
    async fn sandbox_config(&self, _: &str) -> Result<Option<SandboxConfig>, String> {
        Ok(None)
    }
}

fn pod(uid: &str, ns: &str, pool: Option<&str>) -> WorkerPod {
    WorkerPod {
        uid: uid.into(),
        namespace: ns.into(),
        name: format!("worker-{uid}"),
        pool: pool.map(Into::into),
        deleting: false,
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    workers_dir: PathBuf,
    p: StatsPoller,
}

/// A poller over a workers directory with one subdirectory per UID in
/// `dirs`, dialing `fakes`.
fn fixture_with(
    dirs: &[&str],
    fakes: &[(&str, Arc<FakeWorker>)],
    pods: Option<PodList>,
) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    for uid in dirs {
        std::fs::create_dir(dir.path().join(uid)).unwrap();
    }
    let fakes = Fakes(
        fakes
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    );
    let cluster = pods.map(|p| Arc::new(Pods(p)) as Arc<dyn ClusterView>);
    let mut p = StatsPoller::new(dir.path().to_path_buf(), Arc::new(fakes), cluster, None);
    p.started_at = STARTED_AT;
    Fixture {
        workers_dir: dir.path().to_path_buf(),
        _dir: dir,
        p,
    }
}

fn fixture(fakes: &[(&str, Arc<FakeWorker>)]) -> Fixture {
    let dirs: Vec<&str> = fakes.iter().map(|(k, _)| *k).collect();
    fixture_with(&dirs, fakes, None)
}

fn executing(
    ns: &str,
    name: &str,
    class: &str,
    source: wpb::StatsSource,
    current: u64,
    ws: u64,
) -> wpb::WorkloadStatsSample {
    wpb::WorkloadStatsSample {
        actor_template_hive: ns.into(),
        actor_template_name: name.into(),
        sandbox_class: class.into(),
        source: source as i32,
        memory_current_bytes: current,
        memory_working_set_bytes: ws,
        ..Default::default()
    }
}

fn measured(
    actor: &str,
    ns: &str,
    name: &str,
    current: u64,
    ws: u64,
    cpu: u64,
) -> wpb::WorkloadStatsSample {
    wpb::WorkloadStatsSample {
        actor_uid: actor.into(),
        cpu_usage_usec: cpu,
        ..executing(ns, name, "gvisor", wpb::StatsSource::Cgroup, current, ws)
    }
}

/// Hosted but not measured: attribution only, source unspecified.
fn pending(actor: &str, ns: &str, name: &str) -> wpb::WorkloadStatsSample {
    wpb::WorkloadStatsSample {
        actor_uid: actor.into(),
        actor_template_hive: ns.into(),
        actor_template_name: name.into(),
        sandbox_class: "gvisor".into(),
        ..Default::default()
    }
}

fn cpu_sample(actor: &str, cpu: u64) -> wpb::WorkloadStatsSample {
    wpb::WorkloadStatsSample {
        actor_uid: actor.into(),
        cpu_usage_usec: cpu,
        ..executing("ns-a", "tmpl-a", "gvisor", wpb::StatsSource::Cgroup, 0, 0)
    }
}

fn at_epoch(mut s: wpb::WorkloadStatsSample, epoch: i64) -> wpb::WorkloadStatsSample {
    s.epoch_unix_nano = epoch;
    s
}

fn key(ns: &str, name: &str, class: &str, source: &str) -> TemplateKey {
    TemplateKey {
        template_hive: ns.into(),
        template_name: name.into(),
        sandbox_class: class.into(),
        source: source.into(),
        pool: PoolRef::default(),
    }
}

fn key_a() -> TemplateKey {
    key("ns-a", "tmpl-a", "gvisor", "cgroup")
}

fn pooled(k: TemplateKey, ns: &str, name: &str) -> TemplateKey {
    TemplateKey {
        pool: PoolRef {
            namespace: ns.into(),
            name: name.into(),
        },
        ..k
    }
}

fn agg(sampled: i64, current: i64, ws: i64) -> Aggregate {
    Aggregate {
        sampled_actors: sampled,
        memory_current_bytes: current,
        memory_working_set_bytes: ws,
        cpu_delta_usec: 0,
    }
}

fn baseline(p: &StatsPoller, actor: &str, epoch: i64) -> Option<u64> {
    p.state
        .lock()
        .last_cpu
        .get(&(actor.to_string(), epoch))
        .map(|b| b.usec)
}

fn baselines(p: &StatsPoller) -> usize {
    p.state.lock().last_cpu.len()
}

async fn delta(p: &StatsPoller, k: &TemplateKey) -> Option<i64> {
    p.collect(&Ctx::background())
        .await
        .get(k)
        .map(|a| a.cpu_delta_usec)
}

#[tokio::test]
async fn collect_aggregates() {
    // Two actors of one template, one of another, an idle worker, one
    // mid-boot: the pair sums, the idle and pending contribute nothing.
    let fakes = [
        (
            "uid-1",
            FakeWorker::with(vec![executing(
                "ns-a",
                "tmpl-a",
                "gvisor",
                wpb::StatsSource::Cgroup,
                1000,
                700,
            )]),
        ),
        (
            "uid-2",
            FakeWorker::with(vec![executing(
                "ns-a",
                "tmpl-a",
                "gvisor",
                wpb::StatsSource::Cgroup,
                500,
                300,
            )]),
        ),
        (
            "uid-3",
            FakeWorker::with(vec![executing(
                "ns-b",
                "tmpl-b",
                "microvm",
                wpb::StatsSource::GuestAgent,
                42,
                40,
            )]),
        ),
        ("uid-4", FakeWorker::with(vec![])),
        (
            "uid-5",
            FakeWorker::with(vec![pending("uid-5-actor", "ns-a", "tmpl-a")]),
        ),
    ];
    let f = fixture(&fakes);
    let got = f.p.collect(&Ctx::background()).await;
    let want: HashMap<_, _> = [
        (key_a(), agg(2, 1500, 1000)),
        (
            key("ns-b", "tmpl-b", "microvm", "guest-agent"),
            agg(1, 42, 40),
        ),
    ]
    .into();
    assert_eq!(got, want);
    for (uid, w) in &fakes {
        assert_eq!(w.calls(), 1, "{uid}");
        assert!(*w.saw_deadline.lock(), "{uid}: probe without a deadline");
    }
}

#[tokio::test]
async fn collect_skips_failures() {
    // A dial or call failure means "not sampled this sweep", never a failed
    // sweep.
    let fakes = [
        (
            "uid-healthy",
            FakeWorker::with(vec![executing(
                "ns-a",
                "tmpl-a",
                "gvisor",
                wpb::StatsSource::Cgroup,
                100,
                80,
            )]),
        ),
        ("uid-broken", FakeWorker::failing("connection refused")),
    ];
    let f = fixture_with(&["uid-healthy", "uid-broken", "uid-stale"], &fakes, None);
    let got = f.p.collect(&Ctx::background()).await;
    assert_eq!(got, HashMap::from([(key_a(), agg(1, 100, 80))]));
}

#[tokio::test]
async fn collect_no_workers_dir() {
    let d = tempfile::tempdir().unwrap();
    let p = StatsPoller::new(
        d.path().join("does-not-exist"),
        Arc::new(Fakes(HashMap::new())),
        None,
        None,
    );
    assert!(p.collect(&Ctx::background()).await.is_empty());
}

#[test]
fn clamp_actor_stats_poll_interval() {
    for (name, input, want) in [
        ("zero stays disabled", Duration::ZERO, Duration::ZERO),
        (
            "below floor clamps",
            Duration::from_secs(1),
            MIN_POLL_INTERVAL,
        ),
        ("at floor passes", MIN_POLL_INTERVAL, MIN_POLL_INTERVAL),
        (
            "above floor passes",
            Duration::from_secs(300),
            Duration::from_secs(300),
        ),
    ] {
        assert_eq!(clamp_poll_interval(input), want, "{name}");
    }
}

fn series(reg: &Registry, name: &str) -> usize {
    reg.gather()
        .iter()
        .find(|m| m.name() == name)
        .map_or(0, |m| m.get_metric().len())
}

#[test]
fn instruments_observe_latest_snapshot_only() {
    // Each scrape reports exactly the groups of the latest sweep: a
    // template whose actors left must disappear, not freeze.
    let reg = Registry::new();
    let inst = StatsInstruments::new(&reg).unwrap();
    inst.publish(&HashMap::from([(key_a(), agg(2, 1500, 1000))]));
    let ws = "hivekeep_actor_stats_memory_working_set_bytes";
    assert_eq!(series(&reg, ws), 1);
    inst.publish(&HashMap::new());
    assert_eq!(series(&reg, ws), 0);
}

#[tokio::test]
async fn cpu_deltas() {
    let w = FakeWorker::with(vec![cpu_sample("uid-a", 1000)]);
    let f = fixture(&[("uid-1", w.clone())]);
    assert_eq!(
        delta(&f.p, &key_a()).await,
        Some(0),
        "an activation from before the start only sets its baseline"
    );
    w.set(vec![cpu_sample("uid-a", 1600)]);
    assert_eq!(delta(&f.p, &key_a()).await, Some(600));
    // A new activation begun after the start counts in full.
    w.set(vec![at_epoch(cpu_sample("uid-a", 250), STARTED_AT + 1)]);
    assert_eq!(delta(&f.p, &key_a()).await, Some(250));
    // The actor leaves: nothing contributes, and its baseline goes.
    w.set(vec![]);
    assert!(f.p.collect(&Ctx::background()).await.is_empty());
    assert_eq!(baselines(&f.p), 0);
}

#[tokio::test]
async fn worker_pool_labels() {
    // A resolved pod groups under its pool, an unresolved one groups
    // without pool labels, and the two never merge.
    let fakes = [
        (
            "uid-pooled",
            FakeWorker::with(vec![executing(
                "ns-a",
                "tmpl-a",
                "gvisor",
                wpb::StatsSource::Cgroup,
                100,
                80,
            )]),
        ),
        (
            "uid-unpooled",
            FakeWorker::with(vec![executing(
                "ns-a",
                "tmpl-a",
                "gvisor",
                wpb::StatsSource::Cgroup,
                10,
                8,
            )]),
        ),
    ];
    let pods = Arc::new(Mutex::new(Ok(vec![pod(
        "uid-pooled",
        "pool-ns",
        Some("pool-a"),
    )])));
    let f = fixture_with(&["uid-pooled", "uid-unpooled"], &fakes, Some(pods));
    let got = f.p.collect(&Ctx::background()).await;
    let want = HashMap::from([
        (pooled(key_a(), "pool-ns", "pool-a"), agg(1, 100, 80)),
        (key_a(), agg(1, 10, 8)),
    ]);
    assert_eq!(got, want);
}

#[tokio::test]
async fn collect_saturates_corrupt_samples() {
    // One guest reporting absurd counters pins the aggregates at the
    // ceiling instead of flipping them negative.
    let fakes = [
        (
            "uid-1",
            FakeWorker::with(vec![executing(
                "ns-a",
                "tmpl-a",
                "gvisor",
                wpb::StatsSource::Cgroup,
                u64::MAX,
                u64::MAX,
            )]),
        ),
        (
            "uid-2",
            FakeWorker::with(vec![executing(
                "ns-a",
                "tmpl-a",
                "gvisor",
                wpb::StatsSource::Cgroup,
                1000,
                700,
            )]),
        ),
    ];
    let f = fixture(&fakes);
    let got = f.p.collect(&Ctx::background()).await;
    let a = &got[&key_a()];
    assert_eq!(a.memory_current_bytes, i64::MAX);
    assert_eq!(a.memory_working_set_bytes, i64::MAX);
    assert!(a.cpu_delta_usec >= 0);
}

#[tokio::test]
async fn cpu_delta_saturates_corrupt_counter() {
    let w = FakeWorker::with(vec![cpu_sample("uid-a", 1000)]);
    let f = fixture(&[("uid-1", w.clone())]);
    assert_eq!(delta(&f.p, &key_a()).await, Some(0));
    w.set(vec![cpu_sample("uid-a", u64::MAX)]);
    assert_eq!(delta(&f.p, &key_a()).await, Some(i64::MAX));
}

#[tokio::test]
async fn pool_resolution_ingestion_rules() {
    // A labeled worker maps by pod UID; an empty label names no pool; an
    // unlabeled pod is no pool's worker.
    let pods = Arc::new(Mutex::new(Ok(vec![
        pod("uid-a", "pool-ns", Some("pool-a")),
        pod("uid-empty", "pool-ns", Some("")),
        pod("uid-bystander", "other-ns", None),
    ])));
    let f = fixture_with(&[], &[], Some(pods));
    let got =
        f.p.resolve_pools(&["uid-a".into(), "uid-empty".into(), "uid-bystander".into()])
            .await;
    assert_eq!(
        got,
        HashMap::from([(
            "uid-a".to_string(),
            PoolRef {
                namespace: "pool-ns".into(),
                name: "pool-a".into()
            }
        )])
    );
}

#[tokio::test]
async fn pool_cache_survives_list_flap() {
    // A failed list answers from the cache, so the sweep's samples and CPU
    // increase stay on the pooled series.
    let w = FakeWorker::with(vec![measured("uid-a", "ns-a", "tmpl-a", 100, 80, 1000)]);
    let pods: PodList = Arc::new(Mutex::new(Ok(vec![pod(
        "uid-1",
        "pool-ns",
        Some("pool-a"),
    )])));
    let f = fixture_with(&["uid-1"], &[("uid-1", w.clone())], Some(pods.clone()));
    let k = pooled(key_a(), "pool-ns", "pool-a");
    assert!(f.p.collect(&Ctx::background()).await.contains_key(&k));

    *pods.lock() = Err("apiserver unavailable".into());
    w.set(vec![measured("uid-a", "ns-a", "tmpl-a", 100, 80, 1600)]);
    let got = f.p.collect(&Ctx::background()).await;
    assert_eq!(got.len(), 1, "no pool-less split: {got:?}");
    assert_eq!(got[&k].cpu_delta_usec, 600);
}

#[tokio::test]
async fn pool_cache_prunes() {
    let pods = Arc::new(Mutex::new(Ok(vec![
        pod("uid-1", "pool-ns", Some("pool-a")),
        pod("uid-gone", "pool-ns", Some("pool-a")),
    ])));
    let f = fixture_with(
        &["uid-1"],
        &[("uid-1", FakeWorker::with(vec![]))],
        Some(pods),
    );
    f.p.collect(&Ctx::background()).await;
    let st = f.p.state.lock();
    assert!(st.cached_pools.contains_key("uid-1"));
    assert!(
        !st.cached_pools.contains_key("uid-gone"),
        "kept an entry with no worker directory"
    );
}

#[tokio::test]
async fn pool_cache_miss_during_outage() {
    // First seen while the list fails: grouped without pool labels.
    let pods = Arc::new(Mutex::new(Err("apiserver unavailable".to_string())));
    let w = FakeWorker::with(vec![executing(
        "ns-a",
        "tmpl-a",
        "gvisor",
        wpb::StatsSource::Cgroup,
        10,
        8,
    )]);
    let f = fixture_with(&["uid-new"], &[("uid-new", w)], Some(pods));
    let got = f.p.collect(&Ctx::background()).await;
    assert_eq!(got.len(), 1);
    assert!(got.contains_key(&key_a()));
}

#[tokio::test]
async fn pool_cache_partial_list_falls_back() {
    // A successful list that omits a cached pod falls back for that pod.
    let w = FakeWorker::with(vec![measured("uid-a", "ns-a", "tmpl-a", 100, 80, 1000)]);
    let pods: PodList = Arc::new(Mutex::new(Ok(vec![pod(
        "uid-1",
        "pool-ns",
        Some("pool-a"),
    )])));
    let f = fixture_with(&["uid-1"], &[("uid-1", w.clone())], Some(pods.clone()));
    let k = pooled(key_a(), "pool-ns", "pool-a");
    assert!(f.p.collect(&Ctx::background()).await.contains_key(&k));

    *pods.lock() = Ok(vec![pod("uid-other", "pool-ns", Some("pool-b"))]);
    w.set(vec![measured("uid-a", "ns-a", "tmpl-a", 100, 80, 1250)]);
    let got = f.p.collect(&Ctx::background()).await;
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[&k].cpu_delta_usec, 250);
}

#[tokio::test]
async fn folds_multi_actor_worker() {
    let w = FakeWorker::with(vec![
        measured("actor-1", "ns-a", "tmpl-a", 1000, 700, 0),
        measured("actor-2", "ns-a", "tmpl-a", 500, 300, 0),
        measured("actor-3", "ns-b", "tmpl-b", 42, 40, 0),
        pending("actor-4", "ns-a", "tmpl-a"),
    ]);
    let f = fixture(&[("uid-w1", w.clone())]);
    let got = f.p.collect(&Ctx::background()).await;
    let want = HashMap::from([
        (key_a(), agg(2, 1500, 1000)),
        (key("ns-b", "tmpl-b", "gvisor", "cgroup"), agg(1, 42, 40)),
    ]);
    assert_eq!(got, want);
    assert_eq!(w.calls(), 1, "one probe serves every entry");
}

#[tokio::test]
async fn multi_actor_cpu_baselines() {
    let w = FakeWorker::with(vec![
        measured("actor-1", "ns-a", "tmpl-a", 1, 1, 1000),
        measured("actor-2", "ns-a", "tmpl-a", 1, 1, 5000),
        pending("actor-3", "ns-a", "tmpl-a"),
    ]);
    let f = fixture(&[("uid-w1", w.clone())]);
    assert_eq!(delta(&f.p, &key_a()).await, Some(0));
    assert_eq!(
        baseline(&f.p, "actor-3", 0),
        None,
        "pending actor got a baseline"
    );

    w.set(vec![
        measured("actor-1", "ns-a", "tmpl-a", 1, 1, 1600),
        measured("actor-2", "ns-a", "tmpl-a", 1, 1, 5250),
        pending("actor-3", "ns-a", "tmpl-a"),
    ]);
    assert_eq!(delta(&f.p, &key_a()).await, Some(850));

    // actor-2 starts a new activation: it charges its value, not a
    // negative delta.
    w.set(vec![
        measured("actor-1", "ns-a", "tmpl-a", 1, 1, 1700),
        at_epoch(
            measured("actor-2", "ns-a", "tmpl-a", 1, 1, 40),
            STARTED_AT + 1,
        ),
    ]);
    assert_eq!(delta(&f.p, &key_a()).await, Some(140));
    assert_eq!(baselines(&f.p), 2);
}

#[tokio::test]
async fn pending_keeps_cpu_baseline() {
    let w = FakeWorker::with(vec![measured("actor-1", "ns-a", "tmpl-a", 1, 1, 1000)]);
    let f = fixture(&[("uid-w1", w.clone())]);
    f.p.collect(&Ctx::background()).await;
    w.set(vec![pending("actor-1", "ns-a", "tmpl-a")]);
    assert!(f.p.collect(&Ctx::background()).await.is_empty());
    assert_eq!(baseline(&f.p, "actor-1", 0), Some(1000));
    w.set(vec![measured("actor-1", "ns-a", "tmpl-a", 1, 1, 1600)]);
    assert_eq!(delta(&f.p, &key_a()).await, Some(600));
}

#[tokio::test]
async fn cpu_epochs() {
    let s = |epoch, cpu| {
        Some(at_epoch(
            measured("actor-1", "ns-a", "tmpl-a", 1, 1, cpu),
            epoch,
        ))
    };
    let cases: Vec<(&str, Vec<Option<wpb::WorkloadStatsSample>>, i64)> = vec![
        (
            "increase within an activation",
            vec![s(2000, 1000), s(2000, 1400)],
            400,
        ),
        (
            "decrease within an activation",
            vec![s(2000, 1000), s(2000, 400)],
            0,
        ),
        (
            "new activation after start",
            vec![s(2000, 1000), s(3000, 400)],
            400,
        ),
        (
            "new activation across a pending sweep",
            vec![s(2000, 1000), None, s(3000, 400)],
            400,
        ),
        (
            "first sight of an activation after start",
            vec![s(2000, 700)],
            700,
        ),
        (
            "first sight of an activation at start",
            vec![s(STARTED_AT, 700)],
            0,
        ),
        (
            "first sight of an activation before start",
            vec![s(STARTED_AT - 1, 700)],
            0,
        ),
    ];
    for (name, sweeps, want) in cases {
        let w = FakeWorker::with(vec![]);
        let f = fixture(&[("uid-w1", w.clone())]);
        let mut got = 0;
        for sw in sweeps {
            let sample = sw.unwrap_or_else(|| at_epoch(pending("actor-1", "ns-a", "tmpl-a"), 3000));
            w.set(vec![sample]);
            got = delta(&f.p, &key_a()).await.unwrap_or(0);
        }
        assert_eq!(got, want, "{name}");
    }
}

#[tokio::test]
async fn restore_charges_each_activation_once() {
    const BEFORE: i64 = 2000;
    const AFTER: i64 = 3000;
    let src = FakeWorker::with(vec![at_epoch(
        measured("actor-1", "ns-a", "tmpl-a", 1, 1, 1000),
        BEFORE,
    )]);
    let dst = FakeWorker::with(vec![]);
    let f = fixture(&[("uid-src", src.clone()), ("uid-dst", dst.clone())]);
    f.p.collect(&Ctx::background()).await;

    src.set(vec![at_epoch(
        measured("actor-1", "ns-a", "tmpl-a", 1, 1, 1300),
        BEFORE,
    )]);
    dst.set(vec![at_epoch(pending("actor-1", "ns-a", "tmpl-a"), AFTER)]);
    assert_eq!(delta(&f.p, &key_a()).await, Some(300));

    src.set(vec![]);
    dst.set(vec![at_epoch(
        measured("actor-1", "ns-a", "tmpl-a", 1, 1, 200),
        AFTER,
    )]);
    assert_eq!(delta(&f.p, &key_a()).await, Some(200));
    assert_eq!(
        baseline(&f.p, "actor-1", BEFORE),
        None,
        "the source activation's baseline outlived its worker answering without it"
    );
}

#[tokio::test]
async fn keeps_baseline_of_silent_worker() {
    let w = FakeWorker::with(vec![at_epoch(
        measured("actor-1", "ns-a", "tmpl-a", 1, 1, 1000),
        2000,
    )]);
    let f = fixture(&[("uid-w1", w.clone())]);
    f.p.collect(&Ctx::background()).await;
    *w.err.lock() = Some("deadline exceeded".into());
    f.p.collect(&Ctx::background()).await;
    assert_eq!(baseline(&f.p, "actor-1", 2000), Some(1000));
    *w.err.lock() = None;
    w.set(vec![at_epoch(
        measured("actor-1", "ns-a", "tmpl-a", 1, 1, 1600),
        2000,
    )]);
    assert_eq!(delta(&f.p, &key_a()).await, Some(600), "not the full 1600");
}

#[tokio::test]
async fn silent_worker_keeps_baselines_until_dir_gone() {
    let w = FakeWorker::with(vec![at_epoch(
        measured("actor-1", "ns-a", "tmpl-a", 1, 1, 1000),
        2000,
    )]);
    let f = fixture(&[("uid-w1", w.clone())]);
    f.p.collect(&Ctx::background()).await;
    *w.err.lock() = Some("connection refused".into());
    for _ in 0..3 {
        f.p.collect(&Ctx::background()).await;
    }
    assert!(baseline(&f.p, "actor-1", 2000).is_some());
    std::fs::remove_dir(f.workers_dir.join("uid-w1")).unwrap();
    f.p.collect(&Ctx::background()).await;
    assert_eq!(baselines(&f.p), 0);
}

#[tokio::test]
async fn drops_baseline_of_gone_worker() {
    let w = FakeWorker::with(vec![at_epoch(
        measured("actor-1", "ns-a", "tmpl-a", 1, 1, 1000),
        2000,
    )]);
    let f = fixture(&[("uid-w1", w)]);
    f.p.collect(&Ctx::background()).await;
    std::fs::remove_dir(f.workers_dir.join("uid-w1")).unwrap();
    f.p.collect(&Ctx::background()).await;
    assert_eq!(baselines(&f.p), 0);
}

#[test]
fn add_sat_cases() {
    for (name, agg, v, want) in [
        ("normal add", 100, 50, 150),
        ("zero add", 100, 0, 100),
        // A wire value above the signed ceiling pins instead of wrapping.
        ("value above MaxInt64 saturates", 0, u64::MAX, i64::MAX),
        ("sum overflow saturates", i64::MAX - 10, 100, i64::MAX),
        ("exactly at ceiling", i64::MAX - 5, 5, i64::MAX),
    ] {
        assert_eq!(add_sat(agg, v), want, "{name}");
    }
}

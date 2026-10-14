//! The worker sandbox syncer: planning and probing each version, writing
//! and advertising the runtimes file, retries, orphan directories, and the
//! default runtime's prewarm.

#![allow(clippy::type_complexity)] // test case tables

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

use tonic::Status;

use super::*;
use crate::assets::{AssetEntry, SandboxAssetsRecord, GVISOR_ASSET_NAME};
use crate::prewarm::RuntimePrewarmer;
use crate::sandbox_assets::AssetStager;
use crate::sandbox_test_support::{
    cache_at, cached_assets, digest, gvisor_release_tarball, Bucket,
};
use crate::sandbox_versions::SandboxConfig;
use crate::sandbox_versions::{read_runtimes_file, AssetFile, SandboxVersion, VersionState};
use async_trait::async_trait;

const POD_A: &str = "0a0a0a0a-0000-4000-8000-00000000000a";
const POD_B: &str = "0b0b0b0b-0000-4000-8000-00000000000b";
const TEST_CPU_VENDOR: &str = "GenuineIntel";
const TEST_CPU_MODEL: &str = "6:143";

/// Two features this node's runsc might print; their bits are 0 and 1.
fn test_cpu_features() -> Vec<String> {
    match node_arch() {
        "arm64" => vec!["fp".into(), "asimd".into()],
        _ => vec!["pni".into(), "pclmulqdq".into()],
    }
}

/// What the cluster says, changeable while the syncer runs.
#[derive(Default)]
struct Cluster {
    pods: Mutex<Vec<WorkerPod>>,
    /// `namespace/pool` → the config it resolved; `None` for a pool that
    /// has resolved none.
    pools: Mutex<HashMap<String, Option<String>>>,
    configs: Mutex<Vec<SandboxConfig>>,
    list_err: Mutex<Option<String>>,
    changes: ClusterChangeNotifier,
}

impl Cluster {
    fn with(
        pods: Vec<WorkerPod>,
        pools: &[(&str, &str)],
        configs: Vec<SandboxConfig>,
    ) -> Arc<Cluster> {
        let c = Cluster::default();
        *c.pods.lock() = pods;
        for (pool, config) in pools {
            let cfg = (!config.is_empty()).then(|| config.to_string());
            c.pools.lock().insert(format!("workers/{pool}"), cfg);
        }
        *c.configs.lock() = configs;
        Arc::new(c)
    }

    fn set_default(&self, name: &str, version: &str) {
        for c in self.configs.lock().iter_mut() {
            if c.name == name {
                c.default_version = version.into();
                c.generation += 1;
            }
        }
    }
}

#[async_trait]
impl ClusterView for Cluster {
    fn changes(&self) -> Option<tokio::sync::watch::Receiver<ClusterChanges>> {
        Some(self.changes.subscribe())
    }
    async fn worker_pods(&self) -> Result<Vec<WorkerPod>, String> {
        match self.list_err.lock().clone() {
            Some(e) => Err(e),
            None => Ok(self.pods.lock().clone()),
        }
    }
    async fn pool_sandbox_config(
        &self,
        namespace: &str,
        pool: &str,
    ) -> Result<Option<String>, String> {
        Ok(self
            .pools
            .lock()
            .get(&format!("{namespace}/{pool}"))
            .cloned()
            .flatten())
    }
    async fn sandbox_config(&self, name: &str) -> Result<Option<SandboxConfig>, String> {
        Ok(self.configs.lock().iter().find(|c| c.name == name).cloned())
    }
}

fn worker_pod(uid: &str, pool: &str) -> WorkerPod {
    WorkerPod {
        uid: uid.into(),
        namespace: "workers".into(),
        name: format!("worker-{uid}"),
        pool: (!pool.is_empty()).then(|| pool.to_string()),
        deleting: false,
    }
}

type FetchFn =
    Arc<dyn Fn(&SandboxAssetsRecord) -> Result<BTreeMap<String, String>, Status> + Send + Sync>;

/// Hands each asset back at `/cache/<sha256>/<name>` unless told otherwise.
struct Fetcher(Mutex<FetchFn>);

fn cache_paths(rec: &SandboxAssetsRecord) -> Result<BTreeMap<String, String>, Status> {
    Ok(rec
        .assets
        .iter()
        .map(|(n, a)| (n.clone(), format!("/cache/{}/{n}", a.sha256)))
        .collect())
}

impl Fetcher {
    fn new() -> Arc<Fetcher> {
        Arc::new(Fetcher(Mutex::new(Arc::new(cache_paths))))
    }
    fn set(
        &self,
        f: impl Fn(&SandboxAssetsRecord) -> Result<BTreeMap<String, String>, Status>
            + Send
            + Sync
            + 'static,
    ) {
        *self.0.lock() = Arc::new(f);
    }
}

#[async_trait]
impl AssetFetcher for Fetcher {
    async fn ensure(
        &self,
        _: &Ctx,
        rec: &SandboxAssetsRecord,
        _: &Path,
    ) -> Result<BTreeMap<String, String>, Status> {
        let f = self.0.lock().clone();
        f(rec)
    }
    async fn stage(
        &self,
        _: &Ctx,
        rec: &SandboxAssetsRecord,
        _: &Path,
    ) -> Result<BTreeMap<String, String>, Status> {
        let f = self.0.lock().clone();
        f(rec)
    }
}

type FeaturesFn = Arc<dyn Fn(&Path) -> Result<Vec<String>, SyncError> + Send + Sync>;

/// A host whose CPU identity and runsc features are scripted. Features
/// encode as a bitmask over [`test_cpu_features`]; any other is unknown.
struct Probe {
    identity_calls: AtomicUsize,
    identity_err: Mutex<Option<String>>,
    features: Mutex<FeaturesFn>,
    exec_calls: Mutex<HashMap<String, usize>>,
}

impl Probe {
    fn new() -> Arc<Probe> {
        Arc::new(Probe {
            identity_calls: AtomicUsize::new(0),
            identity_err: Mutex::new(None),
            features: Mutex::new(Arc::new(|_| Ok(test_cpu_features()))),
            exec_calls: Mutex::new(HashMap::new()),
        })
    }
    fn set_features(
        &self,
        f: impl Fn(&Path) -> Result<Vec<String>, SyncError> + Send + Sync + 'static,
    ) {
        *self.features.lock() = Arc::new(f);
    }
}

#[async_trait]
impl HostProbe for Probe {
    fn cpu_identity(&self) -> Result<(String, String), String> {
        self.identity_calls.fetch_add(1, Ordering::SeqCst);
        match self.identity_err.lock().clone() {
            Some(e) => Err(e),
            None => Ok((TEST_CPU_VENDOR.into(), TEST_CPU_MODEL.into())),
        }
    }
    async fn cpu_features(&self, runsc: &Path) -> Result<Vec<String>, SyncError> {
        *self
            .exec_calls
            .lock()
            .entry(runsc.to_string_lossy().into_owned())
            .or_default() += 1;
        let f = self.features.lock().clone();
        f(runsc)
    }
    fn encode_cpu_features(&self, _: &str, names: &[String]) -> Result<String, String> {
        let known = test_cpu_features();
        let mut mask = 0u64;
        for n in names {
            let bit = known
                .iter()
                .position(|k| k == n)
                .ok_or_else(|| format!("unknown CPU feature {n:?}"))?;
            mask |= 1 << bit;
        }
        Ok(mask.to_string())
    }
}

/// Records what it is handed.
#[derive(Default)]
struct Recorder(Mutex<Vec<(String, PathBuf)>>);

impl Recorder {
    fn items(&self) -> Vec<(String, PathBuf)> {
        self.0.lock().clone()
    }
}

impl Prewarm for Recorder {
    fn enqueue(&self, hash: &str, dir: &Path) {
        self.0.lock().push((hash.into(), dir.into()));
    }
}

struct Env {
    _d: tempfile::TempDir,
    root: PathBuf,
    paths: Arc<NodePaths>,
    cluster: Arc<Cluster>,
    fetcher: Arc<Fetcher>,
    probe: Arc<Probe>,
    index: Arc<RuntimeIndex>,
}

impl Env {
    fn new(cluster: Arc<Cluster>) -> Env {
        let d = tempfile::tempdir().unwrap();
        let root = d.path().to_path_buf();
        Env {
            paths: Arc::new(NodePaths::new(&root)),
            _d: d,
            root,
            cluster,
            fetcher: Fetcher::new(),
            probe: Probe::new(),
            index: Arc::new(RuntimeIndex::in_memory()),
        }
    }

    fn syncer_with(
        &self,
        prewarm: Option<Arc<dyn Prewarm>>,
        assets: Option<Arc<dyn AssetFetcher>>,
    ) -> WorkerSandboxSyncer {
        WorkerSandboxSyncer::new(
            self.cluster.clone(),
            self.paths.clone(),
            self.index.clone(),
            prewarm,
            assets.unwrap_or_else(|| self.fetcher.clone()),
            self.probe.clone(),
        )
    }

    fn syncer(&self) -> WorkerSandboxSyncer {
        self.syncer_with(None, None)
    }

    fn runtimes_file(&self, uid: &str) -> PathBuf {
        self.paths.worker_sandbox_runtimes_file(uid)
    }

    fn read(&self, uid: &str) -> Option<RuntimesFile> {
        read_runtimes_file(&self.runtimes_file(uid)).ok()
    }
}

fn version(name: &str, sha: &str, state: VersionState) -> SandboxVersion {
    let mut files = BTreeMap::new();
    files.insert(
        GVISOR_ASSET_NAME.to_string(),
        AssetFile {
            url: format!("gs://bucket/{name}"),
            sha256: sha.into(),
        },
    );
    let mut assets = HashMap::new();
    assets.insert(node_arch().to_string(), files);
    SandboxVersion {
        name: name.into(),
        state,
        assets,
    }
}

/// v0 disabled, v1 enabled, one with assets for another architecture
/// only, and the default v2.
fn multi_version_config() -> SandboxConfig {
    let mut other = version("other-arch", "sha-other", VersionState::Enabled);
    let files = other.assets.remove(node_arch()).unwrap();
    other.assets.insert("other-arch".into(), files);
    SandboxConfig {
        name: "gvisor-release".into(),
        generation: 1,
        sandbox_class: "gvisor".into(),
        default_version: "v2".into(),
        versions: vec![
            version("v0", "sha-v0", VersionState::Disabled),
            version("v1", "sha-v1", VersionState::Enabled),
            other,
            version("v2", "sha-v2", VersionState::Enabled),
        ],
    }
}

fn hash_of(assets: &[(&str, &str)]) -> String {
    let mut rec = SandboxAssetsRecord::default();
    for (n, sha) in assets {
        rec.assets.insert(
            n.to_string(),
            AssetEntry {
                url: String::new(),
                sha256: sha.to_string(),
            },
        );
    }
    rec.bundle_hash()
}

fn want_runtime_with(name: &str, cpu: &str) -> BTreeMap<String, String> {
    let sha = format!("sha-{name}");
    BTreeMap::from([
        (ATTR_ARCHITECTURE.to_string(), node_arch().to_string()),
        (ATTR_CPU_FEATURES.to_string(), cpu.to_string()),
        (
            ATTR_SANDBOX_BUNDLE_HASH.to_string(),
            hash_of(&[(GVISOR_ASSET_NAME, &sha)]),
        ),
    ])
}

fn want_runtime(name: &str) -> BTreeMap<String, String> {
    want_runtime_with(name, "3")
}

fn class_file(
    class: &str,
    def: BTreeMap<String, String>,
    restorable: Vec<BTreeMap<String, String>>,
) -> RuntimesFile {
    RuntimesFile {
        sandbox_classes: vec![RuntimesClass {
            sandbox_class: class.into(),
            default_runtime: Runtime { version: def },
            restorable_runtimes: restorable
                .into_iter()
                .map(|version| Runtime { version })
                .collect(),
        }],
    }
}

fn gvisor_file(
    def: BTreeMap<String, String>,
    restorable: Vec<BTreeMap<String, String>>,
) -> RuntimesFile {
    class_file("gvisor", def, restorable)
}

fn want_versions() -> RuntimesFile {
    gvisor_file(want_runtime("v2"), vec![want_runtime("v1")])
}

fn standard(pods: Vec<WorkerPod>) -> Arc<Cluster> {
    Cluster::with(
        pods,
        &[("pool", "gvisor-release")],
        vec![multi_version_config()],
    )
}

/// Plans the pod and builds its file, as a sync does before writing.
async fn plan_and_build(
    s: &WorkerSandboxSyncer,
    pod: &WorkerPod,
) -> (Option<RuntimesFile>, Option<SyncError>) {
    let plan = match s.plan(pod).await {
        Ok(p) => p,
        Err(e) => return (None, Some(e)),
    };
    let hashed = hash_versions(&plan);
    match s.build_file(&Ctx::background(), &plan, &hashed).await {
        Ok(f) => (Some(f), None),
        Err((f, e)) => (f, Some(e)),
    }
}

fn is_permanent(e: &Option<SyncError>) -> bool {
    matches!(e, Some(SyncError::Permanent(_)))
}

#[tokio::test]
async fn resolver_plans_the_pools_config() {
    let cfg = || {
        let mut c = multi_version_config();
        c.name = "gvisor-default".into();
        c
    };
    // (name, cluster, pod, want config, want permanent, want unresolved)
    let cases: Vec<(&str, Arc<Cluster>, WorkerPod, Option<&str>, bool, bool)> = vec![
        (
            "the config the pool resolved",
            Cluster::with(vec![], &[("pool", "gvisor-default")], vec![cfg()]),
            worker_pod(POD_A, "pool"),
            Some("gvisor-default"),
            false,
            false,
        ),
        (
            "pod belongs to no pool",
            Cluster::with(vec![], &[("pool", "gvisor-default")], vec![cfg()]),
            worker_pod(POD_A, ""),
            None,
            true,
            false,
        ),
        (
            "pool not found",
            Cluster::with(vec![], &[], vec![cfg()]),
            worker_pod(POD_A, "pool"),
            None,
            false,
            true,
        ),
        (
            "pool has not resolved a config",
            Cluster::with(vec![], &[("pool", "")], vec![cfg()]),
            worker_pod(POD_A, "pool"),
            None,
            false,
            true,
        ),
        (
            "config not found",
            Cluster::with(vec![], &[("pool", "gvisor-missing")], vec![cfg()]),
            worker_pod(POD_A, "pool"),
            None,
            false,
            true,
        ),
    ];
    for (name, cluster, pod, want, permanent, unresolved) in cases {
        let env = Env::new(cluster);
        let s = env.syncer();
        let res = s.plan(&pod).await;
        assert_eq!(
            matches!(res, Err(SyncError::Permanent(_))),
            permanent,
            "{name}: {res:?}"
        );
        assert_eq!(
            matches!(res, Err(SyncError::Unresolved(_))),
            unresolved,
            "{name}: {res:?}"
        );
        if let Some(want) = want {
            assert_eq!(res.unwrap().config, want, "{name}");
        }
    }
}

#[tokio::test]
async fn plan_and_build_cases() {
    let mut default_no_arch = multi_version_config();
    let files = default_no_arch.versions[3]
        .assets
        .remove(node_arch())
        .unwrap();
    default_no_arch.versions[3]
        .assets
        .insert("other-arch".into(), files);
    let mut same_as_default = multi_version_config();
    same_as_default.versions[1].assets = same_as_default.versions[3].assets.clone();
    let mut v1_twice = multi_version_config();
    let mut v3 = v1_twice.versions[1].clone();
    v3.name = "v3".into();
    v1_twice.versions.push(v3);
    let mut unsupported = multi_version_config();
    unsupported.sandbox_class = "unknown".into();
    let mut no_gvisor = multi_version_config();
    no_gvisor.versions[3].assets = HashMap::from([(
        node_arch().to_string(),
        BTreeMap::from([(
            "other".to_string(),
            AssetFile {
                url: "gs://bucket/other".into(),
                sha256: "sha-other".into(),
            },
        )]),
    )]);
    let cases: Vec<(&str, Option<SandboxConfig>, Option<RuntimesFile>, bool)> = vec![
        (
            "default first, then the other enabled versions",
            Some(multi_version_config()),
            Some(want_versions()),
            false,
        ),
        (
            "a version with the default's binaries is left out",
            Some(same_as_default),
            Some(gvisor_file(want_runtime("v2"), vec![])),
            false,
        ),
        (
            "versions with equal binaries are listed once",
            Some(v1_twice),
            Some(want_versions()),
            false,
        ),
        (
            "default version has no assets for this architecture",
            Some(default_no_arch),
            None,
            true,
        ),
        ("unsupported sandbox class", Some(unsupported), None, true),
        (
            "gvisor version has no gvisor asset",
            Some(no_gvisor),
            None,
            true,
        ),
        ("surfaces an unresolved config", None, None, false),
    ];
    for (name, cfg, want, permanent) in cases {
        let env = Env::new(Cluster::with(
            vec![],
            &[("pool", "gvisor-release")],
            cfg.into_iter().collect(),
        ));
        let (got, err) = plan_and_build(&env.syncer(), &worker_pod(POD_A, "pool")).await;
        assert_eq!(err.is_some(), want.is_none(), "{name}: {err:?}");
        assert_eq!(is_permanent(&err), permanent, "{name}: {err:?}");
        if want.is_some() {
            assert_eq!(got, want, "{name}");
        }
    }
}

fn microvm_config(sha_v1: &str, sha_v2: &str, bucket: &str) -> SandboxConfig {
    let version = |name: &str, sha: &str| SandboxVersion {
        name: name.into(),
        state: VersionState::Enabled,
        assets: HashMap::from([(
            node_arch().to_string(),
            BTreeMap::from([(
                "kernel".to_string(),
                AssetFile {
                    url: format!("gs://{bucket}/kernel-{name}"),
                    sha256: sha.into(),
                },
            )]),
        )]),
    };
    SandboxConfig {
        name: "microvm-release".into(),
        generation: 1,
        sandbox_class: "microvm".into(),
        default_version: "v2".into(),
        versions: vec![version("v1", sha_v1), version("v2", sha_v2)],
    }
}

fn want_microvm(sha: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (ATTR_ARCHITECTURE.to_string(), node_arch().to_string()),
        (ATTR_CPU_VENDOR.to_string(), TEST_CPU_VENDOR.to_string()),
        (ATTR_CPU_MODEL.to_string(), TEST_CPU_MODEL.to_string()),
        (
            ATTR_SANDBOX_BUNDLE_HASH.to_string(),
            hash_of(&[("kernel", sha)]),
        ),
    ])
}

#[tokio::test]
async fn plan_and_build_microvm() {
    let cluster = Cluster::with(
        vec![],
        &[("microvm-pool", "microvm-release")],
        vec![microvm_config("sha-v1", "sha-v2", "bucket")],
    );
    let env = Env::new(cluster);
    let s = env.syncer();
    let fetched = Arc::new(Mutex::new(Vec::<String>::new()));
    let fail_sha = Arc::new(Mutex::new(String::new()));
    {
        let (fetched, fail_sha) = (fetched.clone(), fail_sha.clone());
        env.fetcher.set(move |rec| {
            let sha = rec.assets["kernel"].sha256.clone();
            fetched.lock().push(sha.clone());
            if *fail_sha.lock() == sha {
                return Err(Status::internal(format!("bad sha256 for {sha}")));
            }
            cache_paths(rec)
        });
    }
    let pod = |uid| worker_pod(uid, "microvm-pool");

    // A host probe failure is retryable.
    *env.probe.identity_err.lock() = Some("broken /proc/cpuinfo".into());
    let (_, err) = plan_and_build(&s, &pod(POD_A)).await;
    assert!(
        matches!(&err, Some(SyncError::Other(m)) if m.contains("broken /proc/cpuinfo")),
        "probe failure: {err:?}"
    );
    *env.probe.identity_err.lock() = None;

    // The default's assets failing is retryable, not incomplete.
    *fail_sha.lock() = "sha-v2".into();
    let (_, err) = plan_and_build(&s, &pod(POD_A)).await;
    assert!(
        matches!(err, Some(SyncError::Other(_))),
        "default fetch failure: {err:?}"
    );

    // A restorable version's failing leaves the file incomplete.
    *fail_sha.lock() = "sha-v1".into();
    let (partial, err) = plan_and_build(&s, &pod(POD_A)).await;
    assert!(matches!(err, Some(SyncError::Incomplete(_))), "{err:?}");
    assert_eq!(
        partial,
        Some(class_file("microvm", want_microvm("sha-v2"), vec![]))
    );

    fail_sha.lock().clear();
    let want = class_file(
        "microvm",
        want_microvm("sha-v2"),
        vec![want_microvm("sha-v1")],
    );
    let (got, err) = plan_and_build(&s, &pod(POD_A)).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(got.as_ref(), Some(&want));

    // A restorable version fetched once is not fetched again; the default
    // always is.
    *fail_sha.lock() = "sha-v1".into();
    let (got, err) = plan_and_build(&s, &pod(POD_B)).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(got.as_ref(), Some(&want));

    assert_eq!(
        env.probe.identity_calls.load(Ordering::SeqCst),
        2,
        "one failed probe, one reused"
    );
    assert_eq!(
        *fetched.lock(),
        vec!["sha-v2", "sha-v2", "sha-v1", "sha-v2", "sha-v1", "sha-v2"]
    );
}

/// Waits for `cond`, on real time.
async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn sync_pod_microvm_asset_cache_and_eviction() {
    let (v1, v2) = (b"kernel v1".to_vec(), b"kernel v2".to_vec());
    let (v1_sha, v2_sha) = (digest(&v1), digest(&v2));
    let cluster = Cluster::with(
        vec![],
        &[("microvm-pool", "microvm-release")],
        vec![microvm_config(&v1_sha, &v2_sha, "assets")],
    );
    let env = Env::new(cluster.clone());
    let bucket = Bucket::with(&[("assets/kernel-v1", &v1), ("assets/kernel-v2", &v2)]);
    let stager = Arc::new(AssetStager::new(
        cache_at(&env.root, Some(Duration::ZERO)),
        None,
        Some(bucket.clone()),
        "test-store",
    ));
    let rec = Arc::new(Recorder::default());
    let s = env.syncer_with(Some(rec.clone()), Some(stager.clone()));
    let pod = |uid| worker_pod(uid, "microvm-pool");
    let ctx = Ctx::background();

    s.sync_pod(&ctx, &pod(POD_A)).await.unwrap();
    let scratch = env.paths.sandbox_asset_scratch_dir();
    assert_eq!(
        std::fs::read_dir(&scratch).unwrap().count(),
        0,
        "scratch not emptied"
    );
    assert_eq!(bucket.gets(), 2);
    assert_eq!(cached_assets(&env.root), 2);

    // The default is linked into the worker's directory; v1 is not.
    let p = Arc::new(RuntimePrewarmer::new(env.index.clone(), stager.clone()));
    let pctx = Ctx::background();
    tokio::spawn(p.clone().run(pctx.clone()));
    for (hash, dir) in rec.items() {
        p.enqueue(&hash, &dir);
        wait_for("the default to be linked", || dir.join("kernel").exists()).await;
    }
    let st = stager.cache().evict_unused(1 << 40, false).await.unwrap();
    assert_eq!(
        (st.retired, st.skipped_linked, cached_assets(&env.root)),
        (1, 1, 1),
        "{st:?}"
    );

    // With the bucket offline, an evicted restorable version is still
    // advertised: it was fetched once.
    bucket.objects.lock().clear();
    let want = class_file(
        "microvm",
        want_microvm(&v2_sha),
        vec![want_microvm(&v1_sha)],
    );
    for uid in [POD_B, POD_A] {
        s.sync_pod(&ctx, &pod(uid))
            .await
            .unwrap_or_else(|e| panic!("{uid}: {e}"));
        assert_eq!(env.read(uid).as_ref(), Some(&want), "{uid}");
    }
    assert_eq!(bucket.gets(), 2, "re-downloaded");

    // The evicted v1 promoted to default must be fetched, and cannot be.
    cluster.set_default("microvm-release", "v1");
    assert!(s.sync_pod(&ctx, &pod(POD_A)).await.is_err());
    assert_eq!(
        env.read(POD_A).as_ref(),
        Some(&want),
        "a failed sync rewrote the file"
    );
    let v2_hash = want.sandbox_classes[0].default_runtime.version[ATTR_SANDBOX_BUNDLE_HASH].clone();
    assert!(
        s.default_runtime_dir(POD_A, &v2_hash).exists(),
        "a failed sync removed v2's directory"
    );
    assert_eq!(cached_assets(&env.root), 1);
    assert_eq!(bucket.gets(), 3, "the default is checked, not memoized");
    pctx.cancel();
}

#[tokio::test]
async fn gvisor_cpu_features_duplicate_binaries_are_neither_fetched_nor_probed() {
    let mut cfg = multi_version_config();
    let mut v3 = cfg.versions[1].clone();
    v3.name = "v3".into();
    v3.assets.get_mut(node_arch()).unwrap().insert(
        GVISOR_ASSET_NAME.into(),
        AssetFile {
            url: "gs://other-bucket/v1".into(),
            sha256: "sha-v1".into(),
        },
    );
    cfg.versions.push(v3);
    let env = Env::new(Cluster::with(
        vec![],
        &[("pool", "gvisor-release")],
        vec![cfg],
    ));
    env.fetcher.set(|rec| {
        let url = &rec.assets[GVISOR_ASSET_NAME].url;
        if url.starts_with("gs://other-bucket/") {
            return Err(Status::internal(format!(
                "fetched a duplicate runtime from {url}"
            )));
        }
        cache_paths(rec)
    });
    let (got, err) = plan_and_build(&env.syncer(), &worker_pod(POD_A, "pool")).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(got, Some(want_versions()));
}

#[tokio::test]
async fn gvisor_cpu_features_memoized_by_digest_across_versions_and_syncs() {
    let mut cfg = multi_version_config();
    let mut v3 = cfg.versions[1].clone();
    v3.name = "v3".into();
    v3.assets.get_mut(node_arch()).unwrap().insert(
        "extra".into(),
        AssetFile {
            url: "gs://bucket/extra".into(),
            sha256: "sha-extra".into(),
        },
    );
    cfg.versions.push(v3);
    let env = Env::new(Cluster::with(
        vec![],
        &[("pool", "gvisor-release")],
        vec![cfg],
    ));
    env.fetcher.set(|rec| {
        Ok(BTreeMap::from([(
            GVISOR_ASSET_NAME.to_string(),
            format!("/cache/{}/runsc", rec.assets[GVISOR_ASSET_NAME].sha256),
        )]))
    });
    let feats = test_cpu_features();
    env.probe.set_features(move |p| {
        if p == Path::new("/cache/sha-v2/runsc") {
            Ok(feats.clone())
        } else {
            Ok(feats[..1].to_vec())
        }
    });
    let s = env.syncer();
    let (got, err) = plan_and_build(&s, &worker_pod(POD_A, "pool")).await;
    assert!(err.is_none(), "{err:?}");
    let v3_attrs = BTreeMap::from([
        (ATTR_ARCHITECTURE.to_string(), node_arch().to_string()),
        (ATTR_CPU_FEATURES.to_string(), "1".to_string()),
        (
            ATTR_SANDBOX_BUNDLE_HASH.to_string(),
            hash_of(&[(GVISOR_ASSET_NAME, "sha-v1"), ("extra", "sha-extra")]),
        ),
    ]);
    assert_eq!(
        got,
        Some(gvisor_file(
            want_runtime_with("v2", "3"),
            vec![want_runtime_with("v1", "1"), v3_attrs]
        ))
    );
    // The second sync needs neither a fetch nor a probe.
    env.fetcher.set(|_| {
        Err(Status::internal(
            "fetched a runsc whose cpu_features are memoized",
        ))
    });
    let (_, err) = plan_and_build(&s, &worker_pod(POD_B, "pool")).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(
        *env.probe.exec_calls.lock(),
        HashMap::from([
            ("/cache/sha-v2/runsc".to_string(), 1),
            ("/cache/sha-v1/runsc".to_string(), 1)
        ])
    );
}

#[tokio::test]
async fn gvisor_cpu_features_transient_errors_are_retryable() {
    // The asset fetch failing.
    let env = Env::new(standard(vec![]));
    env.fetcher
        .set(|_| Err(Status::unavailable("temporary download failure")));
    let (_, err) = plan_and_build(&env.syncer(), &worker_pod(POD_A, "pool")).await;
    assert!(err.is_some() && !is_permanent(&err), "{err:?}");

    // runsc failing to run.
    let env = Env::new(standard(vec![]));
    env.probe
        .set_features(|_| Err(SyncError::Other("runsc failed".into())));
    let (_, err) = plan_and_build(&env.syncer(), &worker_pod(POD_A, "pool")).await;
    assert!(err.is_some() && !is_permanent(&err), "{err:?}");

    // A restorable version's fetch failing leaves the file incomplete.
    let env = Env::new(standard(vec![]));
    env.fetcher.set(|rec| {
        let sha = &rec.assets[GVISOR_ASSET_NAME].sha256;
        if sha != "sha-v2" {
            return Err(Status::unavailable(format!(
                "temporary download failure for {sha}"
            )));
        }
        Ok(BTreeMap::from([(
            GVISOR_ASSET_NAME.to_string(),
            "sha-v2".to_string(),
        )]))
    });
    let (got, err) = plan_and_build(&env.syncer(), &worker_pod(POD_A, "pool")).await;
    assert!(matches!(err, Some(SyncError::Incomplete(_))), "{err:?}");
    assert_eq!(got, Some(gvisor_file(want_runtime("v2"), vec![])));
}

#[tokio::test]
async fn gvisor_cpu_features_unknown_feature() {
    // On the default it is permanent.
    let env = Env::new(standard(vec![]));
    env.probe
        .set_features(|_| Ok(vec!["not_a_known_feature".into()]));
    let (_, err) = plan_and_build(&env.syncer(), &worker_pod(POD_A, "pool")).await;
    assert!(is_permanent(&err), "{err:?}");

    // On another version it is left out.
    let env = Env::new(standard(vec![]));
    env.fetcher.set(|rec| {
        Ok(BTreeMap::from([(
            GVISOR_ASSET_NAME.to_string(),
            rec.assets[GVISOR_ASSET_NAME].sha256.clone(),
        )]))
    });
    let feats = test_cpu_features();
    env.probe.set_features(move |p| {
        if p == Path::new("sha-v2") {
            Ok(feats.clone())
        } else {
            Ok(vec!["future_unknown_feature".into()])
        }
    });
    let (got, err) = plan_and_build(&env.syncer(), &worker_pod(POD_A, "pool")).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(got, Some(gvisor_file(want_runtime("v2"), vec![])));
}

#[tokio::test]
async fn sync_pod_advertises_what_it_wrote() {
    let ctx = Ctx::background();
    let pod = worker_pod(POD_A, "pool");
    let hash = |name: &str| want_runtime(name)[ATTR_SANDBOX_BUNDLE_HASH].clone();

    // A failed sync keeps the entry of the file on disk.
    let env = Env::new(standard(vec![]));
    let s = env.syncer();
    s.sync_pod(&ctx, &pod).await.unwrap();
    let before = env.index.worker(POD_A).unwrap()["gvisor"].clone();
    assert_eq!(before.default_hash, hash("v2"));
    assert_eq!(before.hashes, vec![hash("v2"), hash("v1")]);
    s.cpu_features.lock().clear();
    env.probe
        .set_features(|_| Ok(vec!["not_a_known_feature".into()]));
    let res = s.sync_pod(&ctx, &pod).await;
    assert!(matches!(res, Err(SyncError::Permanent(_))), "{res:?}");
    assert_eq!(
        env.index.worker(POD_A).unwrap()["gvisor"],
        before,
        "a sync that wrote nothing changed the entry"
    );

    // A partial file advertises only its runtimes; the one left out still
    // resolves, ahead of the retry that writes it.
    let env = Env::new(standard(vec![]));
    env.fetcher.set(|rec| {
        let sha = &rec.assets[GVISOR_ASSET_NAME].sha256;
        if sha != "sha-v2" {
            return Err(Status::unavailable(format!(
                "temporary download failure for {sha}"
            )));
        }
        Ok(BTreeMap::from([(
            GVISOR_ASSET_NAME.to_string(),
            "sha-v2".to_string(),
        )]))
    });
    let res = env.syncer().sync_pod(&ctx, &pod).await;
    assert!(matches!(res, Err(SyncError::Incomplete(_))), "{res:?}");
    let cr = env.index.worker(POD_A).unwrap()["gvisor"].clone();
    assert_eq!(
        (cr.default_hash.clone(), cr.hashes.clone()),
        (hash("v2"), vec![hash("v2")])
    );
    assert!(
        env.index.lookup(&hash("v1")).is_some(),
        "the runtime left out does not resolve"
    );
}

/// Every planned runtime resolves before the worker can read a file
/// naming it, and before the worker is advertised.
#[tokio::test]
async fn registers_runtimes_ahead_of_the_file() {
    let env = Env::new(standard(vec![worker_pod(POD_A, "pool")]));
    let entered = Arc::new(tokio::sync::Notify::new());
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let (entered, released) = (entered.clone(), released.clone());
        env.fetcher.set(move |rec| {
            entered.notify_one();
            while !released.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(1));
            }
            cache_paths(rec)
        });
    }
    let s = Arc::new(env.syncer());
    let pod = worker_pod(POD_A, "pool");
    let plan = s.plan(&pod).await.unwrap();
    let task = {
        let s = s.clone();
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current()
                .block_on(async move { s.sync_pod(&Ctx::background(), &pod).await })
        })
    };
    entered.notified().await;
    for v in hash_versions(&plan) {
        assert!(
            env.index.lookup(&v.hash).is_some(),
            "{} does not resolve before the file is written",
            v.name
        );
    }
    assert!(
        env.index.worker(POD_A).is_none(),
        "advertised before its file was written"
    );
    assert!(env.read(POD_A).is_none());
    released.store(true, Ordering::SeqCst);
    task.await.unwrap().unwrap();
    assert!(env.index.worker(POD_A).is_some());
}

const SYNC_MAX: u32 = SYNC_MAX_RETRIES;

struct ProcessCase {
    name: &'static str,
    cluster: Arc<Cluster>,
    existing: Option<RuntimesFile>,
    fetch: Option<FetchFn>,
    indexed: bool,
    prior_retries: u32,
    want: Option<RuntimesFile>,
    want_dir: bool,
    want_untouched: bool,
    want_retries: u32,
    want_indexed: bool,
}

impl ProcessCase {
    fn new(name: &'static str, cluster: Arc<Cluster>) -> ProcessCase {
        ProcessCase {
            name,
            cluster,
            existing: None,
            fetch: None,
            indexed: false,
            prior_retries: 0,
            want: None,
            want_dir: false,
            want_untouched: false,
            want_retries: 0,
            want_indexed: false,
        }
    }
}

#[tokio::test]
async fn process_cases() {
    let pod = || worker_pod("pod-a", "pool");
    let mut deleting = pod();
    deleting.deleting = true;
    let mut stale = want_versions();
    stale.sandbox_classes[0].default_runtime = Runtime {
        version: want_runtime("v0"),
    };
    let full = || standard(vec![pod()]);
    let no_pod = || standard(vec![]);
    let no_pool = || Cluster::with(vec![pod()], &[], vec![multi_version_config()]);
    let unresolved_pool = || Cluster::with(vec![pod()], &[("pool", "")], vec![]);
    let restorable_fails: FetchFn = Arc::new(|rec| {
        if rec.assets[GVISOR_ASSET_NAME].sha256 != "sha-v2" {
            return Err(Status::unavailable(
                "temporary download failure on restorable version",
            ));
        }
        cache_paths(rec)
    });

    let cases = vec![
        ProcessCase {
            want_indexed: true,
            want: Some(want_versions()),
            want_dir: true,
            ..ProcessCase::new("writes the file", full())
        },
        ProcessCase {
            want_indexed: true,
            existing: Some(stale.clone()),
            want: Some(want_versions()),
            want_dir: true,
            ..ProcessCase::new("replaces a stale file", full())
        },
        ProcessCase {
            want_indexed: true,
            existing: Some(want_versions()),
            want: Some(want_versions()),
            want_dir: true,
            want_untouched: true,
            ..ProcessCase::new("leaves an equal file alone", full())
        },
        ProcessCase {
            existing: Some(stale.clone()),
            indexed: true,
            want: Some(stale.clone()),
            want_dir: true,
            want_indexed: true,
            ..ProcessCase::new("pod being deleted", standard(vec![deleting]))
        },
        ProcessCase {
            existing: Some(stale.clone()),
            indexed: true,
            ..ProcessCase::new("pod gone", no_pod())
        },
        ProcessCase {
            indexed: true,
            ..ProcessCase::new("pod gone, no directory", no_pod())
        },
        ProcessCase {
            existing: Some(stale.clone()),
            indexed: true,
            want: Some(stale.clone()),
            want_dir: true,
            want_retries: 1,
            want_indexed: true,
            ..ProcessCase::new("unresolved pool keeps the existing file", no_pool())
        },
        ProcessCase {
            prior_retries: SYNC_MAX - 1,
            want_retries: SYNC_MAX,
            ..ProcessCase::new("unresolved pool retries up to the cap", no_pool())
        },
        ProcessCase {
            prior_retries: SYNC_MAX,
            ..ProcessCase::new("unresolved pool gives up after the cap", no_pool())
        },
        ProcessCase {
            prior_retries: SYNC_MAX,
            ..ProcessCase::new(
                "unresolved pool config gives up after the cap",
                unresolved_pool(),
            )
        },
        ProcessCase {
            existing: Some(stale.clone()),
            fetch: Some(Arc::new(|_| {
                Err(Status::unavailable(
                    "temporary download failure on default version",
                ))
            })),
            prior_retries: SYNC_MAX,
            want: Some(stale.clone()),
            want_dir: true,
            want_untouched: true,
            want_retries: SYNC_MAX + 1,
            ..ProcessCase::new(
                "transient failure on the default retries past the cap",
                full(),
            )
        },
        ProcessCase {
            want_indexed: true,
            fetch: Some(restorable_fails),
            prior_retries: SYNC_MAX,
            want: Some(gvisor_file(want_runtime("v2"), vec![])),
            want_dir: true,
            want_retries: SYNC_MAX + 1,
            ..ProcessCase::new(
                "incomplete restorable version writes a partial file and retries past the cap",
                full(),
            )
        },
        ProcessCase::new(
            "pod of no pool is not retried",
            Cluster::with(
                vec![worker_pod("pod-a", "")],
                &[("pool", "gvisor-release")],
                vec![multi_version_config()],
            ),
        ),
        ProcessCase {
            existing: Some(stale.clone()),
            fetch: Some(Arc::new(|_| Err(Status::unavailable("é".repeat(1000))))),
            want: Some(stale.clone()),
            want_dir: true,
            want_untouched: true,
            want_retries: 1,
            ..ProcessCase::new("a long sync error", full())
        },
    ];
    for c in cases {
        let name = c.name;
        let env = Env::new(c.cluster);
        if let Some(f) = c.fetch {
            *env.fetcher.0.lock() = f;
        }
        let s = env.syncer();
        if c.indexed {
            env.index.register("pod-a", &[]).unwrap();
        }
        for _ in 0..c.prior_retries {
            s.retries.when("pod-a");
        }
        let dir = env.paths.worker_dir("pod-a");
        let mut before = None;
        if let Some(existing) = &c.existing {
            std::fs::create_dir_all(&dir).unwrap();
            write_runtimes_file(&env.runtimes_file("pod-a"), existing).unwrap();
            before = Some(
                std::fs::metadata(env.runtimes_file("pod-a"))
                    .unwrap()
                    .modified()
                    .unwrap(),
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        s.process(&Ctx::background(), "pod-a").await;
        assert_eq!(dir.exists(), c.want_dir, "{name}: directory exists");
        assert_eq!(env.read("pod-a"), c.want, "{name}: file");
        if c.want_untouched {
            let after = std::fs::metadata(env.runtimes_file("pod-a"))
                .unwrap()
                .modified()
                .unwrap();
            assert_eq!(
                Some(after),
                before,
                "{name}: an equal or kept file was rewritten"
            );
        }
        assert_eq!(
            s.retries.num_requeues("pod-a"),
            c.want_retries,
            "{name}: retries"
        );
        assert_eq!(
            env.index.worker("pod-a").is_some(),
            c.want_indexed,
            "{name}: indexed"
        );
        let requeued = s.queue.pending_at("pod-a").is_some() || !s.queue.is_empty();
        assert_eq!(requeued, c.want_retries > 0, "{name}: requeued");
        s.queue.shut_down();
    }
}

fn set_mtime(p: &Path, t: SystemTime) {
    std::fs::File::open(p).unwrap().set_modified(t).unwrap();
}

fn mk_old_dir(p: &Path, min_age: Duration) {
    std::fs::create_dir_all(p).unwrap();
    set_mtime(p, SystemTime::now() - 2 * min_age);
}

async fn drain(s: &WorkerSandboxSyncer) -> Vec<String> {
    let mut got = Vec::new();
    while !s.queue.is_empty() {
        let k = s.queue.get().await.unwrap();
        s.queue.done(&k);
        got.push(k);
    }
    got.sort();
    got
}

#[tokio::test(start_paused = true)]
async fn existing_dirs_are_queued() {
    const OLD: &str = "old dir";
    const YOUNG: &str = "young dir";
    const FILE: &str = "file";
    let cases: Vec<(&str, Vec<(&str, &str)>, Vec<&str>, Vec<&str>)> = vec![
        (
            "worker directories",
            vec![(POD_A, OLD), (POD_B, OLD)],
            vec![POD_A, POD_B],
            vec![],
        ),
        (
            "a young directory waits for the min age",
            vec![(POD_A, OLD), (POD_B, YOUNG)],
            vec![POD_A],
            vec![POD_B],
        ),
        (
            "names that are no pod UID and files are skipped",
            vec![(POD_A, OLD), ("Not_A_UID", OLD), ("stray", FILE)],
            vec![POD_A],
            vec![],
        ),
        ("no directory yet", vec![], vec![], vec![]),
    ];
    for (name, entries, want, want_later) in cases {
        let env = Env::new(Cluster::with(vec![], &[], vec![]));
        let s = env.syncer();
        let workers = env.paths.workers_dir();
        if !entries.is_empty() {
            std::fs::create_dir_all(&workers).unwrap();
        }
        for (n, kind) in entries {
            let p = workers.join(n);
            match kind {
                FILE => std::fs::write(&p, b"").unwrap(),
                YOUNG => std::fs::create_dir(&p).unwrap(),
                _ => mk_old_dir(&p, s.min_age),
            }
        }
        s.enqueue_all().await;
        assert_eq!(drain(&s).await, want, "{name}: queued now");
        tokio::time::sleep(s.min_age + Duration::from_secs(1)).await;
        assert_eq!(
            drain(&s).await,
            want_later,
            "{name}: queued after the min age"
        );
    }
}

/// Runs `s` until the returned context is cancelled.
fn run(s: Arc<WorkerSandboxSyncer>, period: Duration) -> (Ctx, tokio::task::JoinHandle<()>) {
    let ctx = Ctx::background();
    let h = tokio::spawn(s.run(ctx.clone(), period));
    (ctx, h)
}

/// Until the node's pods can be listed nothing is removed or written;
/// then the orphan goes and the live pod's file is written.
#[tokio::test]
async fn run_waits_for_the_pod_list() {
    for listable in [false, true] {
        let env = Env::new(standard(vec![worker_pod(POD_A, "pool")]));
        let s = Arc::new(env.syncer());
        mk_old_dir(&env.paths.worker_dir(POD_A), ORPHAN_MIN_AGE);
        let orphan = env.paths.worker_dir(POD_B);
        mk_old_dir(&orphan, ORPHAN_MIN_AGE);
        if !listable {
            *env.cluster.list_err.lock() = Some("pod cache not synced".into());
        }
        let (ctx, h) = run(s, Duration::from_millis(50));
        if listable {
            wait_for("the directories to be synced", || {
                !orphan.exists() && env.runtimes_file(POD_A).exists()
            })
            .await;
        } else {
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(
                orphan.exists(),
                "an orphan was removed before the pods could be listed"
            );
            assert!(
                !env.runtimes_file(POD_A).exists(),
                "a file was written before the pods could be listed"
            );
        }
        ctx.cancel();
        tokio::time::timeout(Duration::from_secs(10), h)
            .await
            .expect("run did not stop")
            .unwrap();
    }
}

#[tokio::test]
async fn running_syncer_follows_the_node() {
    // A pod already on the node.
    let env = Env::new(standard(vec![worker_pod(POD_A, "pool")]));
    let (ctx, _h) = run(Arc::new(env.syncer()), Duration::from_millis(50));
    wait_for("pod-a's file", || env.runtimes_file(POD_A).exists()).await;
    ctx.cancel();

    // A pod bound later.
    let env = Env::new(standard(vec![]));
    let (ctx, _h) = run(Arc::new(env.syncer()), Duration::from_millis(50));
    env.cluster.pods.lock().push(worker_pod(POD_B, "pool"));
    wait_for("pod-b's file", || env.runtimes_file(POD_B).exists()).await;
    ctx.cancel();

    // A deleted pod. Its directory is found by the resync, once old
    // enough not to be a pod not yet listed.
    let env = Env::new(standard(vec![worker_pod(POD_A, "pool")]));
    let mut s = env.syncer();
    s.min_age = Duration::ZERO;
    let (ctx, _h) = run(Arc::new(s), Duration::from_millis(50));
    wait_for("pod-a's file", || env.runtimes_file(POD_A).exists()).await;
    env.cluster.pods.lock().clear();
    wait_for("pod-a's directory to be removed", || {
        !env.paths.worker_dir(POD_A).exists()
    })
    .await;
    ctx.cancel();

    // A directory left by a pod that went while the keeper was down.
    let env = Env::new(standard(vec![worker_pod(POD_A, "pool")]));
    let orphan = env.paths.worker_dir(POD_B);
    mk_old_dir(&orphan, ORPHAN_MIN_AGE);
    let (ctx, _h) = run(Arc::new(env.syncer()), Duration::from_millis(50));
    wait_for("pod-a's file", || env.runtimes_file(POD_A).exists()).await;
    wait_for("the orphan to be removed", || !orphan.exists()).await;
    ctx.cancel();
}

/// A local change (a pod bound here) syncs at once, long before the
/// resync period.
#[tokio::test]
async fn a_local_change_wakes_the_syncer() {
    let env = Env::new(standard(vec![]));
    let (ctx, _h) = run(Arc::new(env.syncer()), Duration::from_secs(3600));
    // Let the first resync pass, so only the change can find the new pod.
    tokio::time::sleep(Duration::from_millis(50)).await;
    env.cluster.pods.lock().push(worker_pod(POD_B, "pool"));
    env.cluster.changes.local();
    wait_for("pod-b's file", || env.runtimes_file(POD_B).exists()).await;
    ctx.cancel();
}

/// A fleet-wide change indexes the new default runtime right away and
/// rewrites the file once its random delay passes: immediately without
/// jitter, and not before the period-length jitter otherwise.
#[tokio::test]
async fn a_fleet_change_registers_ahead_and_spreads_the_rewrite() {
    for jittered in [false, true] {
        let env = Env::new(standard(vec![worker_pod(POD_A, "pool")]));
        let jitter = if jittered {
            Duration::from_secs(3600)
        } else {
            Duration::ZERO
        };
        let s = Arc::new(env.syncer().with_max_jitter(jitter));
        let (ctx, _h) = run(s.clone(), Duration::from_secs(3600));
        wait_for("pod-a's file", || env.runtimes_file(POD_A).exists()).await;
        let before = env.read(POD_A).unwrap();
        let old_default = env.index.worker(POD_A).unwrap()["gvisor"]
            .default_hash
            .clone();

        // A release no sync has indexed yet becomes the default.
        {
            let mut configs = env.cluster.configs.lock();
            configs[0]
                .versions
                .push(version("v3", "sha-v3", VersionState::Enabled));
            configs[0].default_version = "v3".into();
            configs[0].generation += 1;
        }
        assert!(env.index.lookup(&old_default).is_some());
        let planned = s.plan(&worker_pod(POD_A, "pool")).await.unwrap();
        let new_default = hash_versions(&planned)
            .into_iter()
            .find(|v| v.is_default)
            .unwrap()
            .hash;
        assert!(env.index.lookup(&new_default).is_none());
        env.cluster.changes.fleet();

        if jittered {
            wait_for("the new default to be indexed", || {
                env.index.lookup(&new_default).is_some()
            })
            .await;
            wait_for("pod-a's delayed sync to be queued", || {
                s.queue.pending_at(POD_A).is_some()
            })
            .await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(
                env.read(POD_A),
                Some(before),
                "the file was rewritten inside the jitter window"
            );
        } else {
            wait_for("pod-a's file to be rewritten", || {
                env.read(POD_A).is_some_and(|f| f != before)
            })
            .await;
            assert!(env.index.lookup(&new_default).is_some());
        }
        ctx.cancel();
    }
}

#[test]
fn jitter_stays_below_its_bound() {
    assert_eq!(jitter(Duration::ZERO), Duration::ZERO);
    for _ in 0..100 {
        assert!(jitter(Duration::from_secs(30)) < Duration::from_secs(30));
    }
}

#[tokio::test]
async fn run_indexes_existing_pods() {
    let env = Env::new(Cluster::with(
        vec![worker_pod(POD_A, "pool"), worker_pod(POD_B, "missing-pool")],
        &[("pool", "gvisor-release")],
        vec![multi_version_config()],
    ));
    let s = Arc::new(env.syncer());
    let (ctx, _h) = run(s.clone(), Duration::from_secs(3600));
    wait_for("pod-a to be indexed", || env.index.worker(POD_A).is_some()).await;
    let cr = env.index.worker(POD_A).unwrap()["gvisor"].clone();
    assert_eq!(cr.config, "gvisor-release");
    assert!(!cr.default_hash.is_empty());
    assert!(
        env.index.lookup(&cr.default_hash).is_some(),
        "the default runtime does not resolve"
    );
    wait_for("pod-b to be retried", || s.retries.num_requeues(POD_B) > 0).await;
    assert!(
        env.index.worker(POD_B).is_none(),
        "a pod of a missing pool was indexed"
    );
    ctx.cancel();
}

#[tokio::test]
async fn sync_pod_prewarms_the_default_runtime() {
    let ctx = Ctx::background();
    let pod = worker_pod(POD_A, "pool");
    let env = Env::new(standard(vec![]));
    let rec = Arc::new(Recorder::default());
    {
        let rec = rec.clone();
        env.fetcher.set(move |r| {
            assert!(
                rec.items().is_empty(),
                "the default was handed to the prewarmer before the build finished"
            );
            cache_paths(r)
        });
    }
    let s = env.syncer_with(Some(rec.clone()), None);
    let dirs = env.paths.worker_dir(POD_A).join(DEFAULT_RUNTIME_DIRS_NAME);
    let stale = dirs.join("sha256-stale");
    std::fs::create_dir_all(&stale).unwrap();
    s.sync_pod(&ctx, &pod).await.unwrap();
    let cr = env.index.worker(POD_A).unwrap()["gvisor"].clone();
    let want = (
        cr.default_hash.clone(),
        s.default_runtime_dir(POD_A, &cr.default_hash),
    );
    assert_eq!(rec.items(), vec![want.clone()]);
    assert!(want.1.exists(), "the default's directory was not created");
    assert!(!stale.exists(), "a retired default's directory survived");
    assert!(
        cr.hashes.len() >= 2,
        "only the default must be prewarmed of several runtimes"
    );

    // Re-syncing hands the same item again and keeps the directory.
    let marker = want.1.join("staged");
    std::fs::write(&marker, b"").unwrap();
    s.sync_pod(&ctx, &pod).await.unwrap();
    assert_eq!(rec.items(), vec![want.clone(), want.clone()]);
    assert!(
        marker.exists(),
        "a re-sync with the same default emptied its directory"
    );
    s.remove_dir(POD_A).unwrap();
    assert!(
        !dirs.exists(),
        "the default runtime directories survived the pod"
    );

    // A failed build hands nothing and removes nothing.
    let env = Env::new(standard(vec![]));
    let rec = Arc::new(Recorder::default());
    let s = env.syncer_with(Some(rec.clone()), None);
    env.probe
        .set_features(|_| Err(SyncError::Other("runsc failed".into())));
    let previous = s.default_runtime_dir(POD_A, "sha256:previous");
    std::fs::create_dir_all(&previous).unwrap();
    assert!(s.sync_pod(&ctx, &pod).await.is_err());
    assert!(
        rec.items().is_empty(),
        "prewarm handed items after a failed build"
    );
    assert!(
        previous.exists(),
        "a failed build removed the previous default's directory"
    );
    assert!(
        env.index.worker(POD_A).is_none(),
        "a failed build registered the worker"
    );
}

#[tokio::test]
async fn default_runtime_prewarm_through_the_syncer() {
    let content = gvisor_release_tarball("runsc bytes");
    let mut cfg = multi_version_config();
    cfg.versions = vec![{
        let mut v = version("v1", &digest(&content), VersionState::Enabled);
        v.assets
            .get_mut(node_arch())
            .unwrap()
            .get_mut(GVISOR_ASSET_NAME)
            .unwrap()
            .url = "gs://bucket/gvisor.tar".into();
        v
    }];
    cfg.default_version = "v1".into();
    let env = Env::new(Cluster::with(
        vec![worker_pod(POD_A, "pool")],
        &[("pool", "gvisor-release")],
        vec![cfg],
    ));
    let stager = Arc::new(AssetStager::new(
        cache_at(&env.root, None),
        None,
        Some(Bucket::with(&[("bucket/gvisor.tar", &content)])),
        "test-store",
    ));
    let p = Arc::new(RuntimePrewarmer::new(env.index.clone(), stager));
    let pctx = Ctx::background();
    tokio::spawn(p.clone().run(pctx.clone()));
    // The syncer's probe goes through the fake fetcher; the prewarm
    // through the cache.
    let s = Arc::new(env.syncer_with(Some(p), None));
    let (ctx, _h) = run(s.clone(), Duration::from_secs(3600));
    wait_for("the worker's runtimes file", || {
        env.runtimes_file(POD_A).exists()
    })
    .await;
    let cr = env.index.worker(POD_A).unwrap()["gvisor"].clone();
    let linked = s
        .default_runtime_dir(POD_A, &cr.default_hash)
        .join(GVISOR_ASSET_NAME)
        .join("runsc");
    wait_for("the default runtime to be linked", || linked.exists()).await;
    assert_eq!(cached_assets(&env.root), 1);
    let scratch = env.paths.sandbox_asset_scratch_dir();
    wait_for("the probe's scratch dir to be emptied", || {
        std::fs::read_dir(&scratch)
            .map(|r| r.count() == 0)
            .unwrap_or(false)
    })
    .await;
    let dirs = std::fs::read_dir(env.paths.worker_dir(POD_A).join(DEFAULT_RUNTIME_DIRS_NAME))
        .unwrap()
        .count();
    assert_eq!(dirs, 1, "want exactly one default runtime directory");
    ctx.cancel();
    pctx.cancel();
}

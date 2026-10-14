//! The prewarmer: retries, supersession, cache residency and bounds.

use std::path::PathBuf;

use super::*;
use crate::assets::GVISOR_ASSET_NAME;
use crate::runtime_index::{AdvertisedClass, HashedVersion};
use crate::sandbox_assets::AssetStager;
use crate::sandbox_test_support::*;

/// Indexes a gVisor runtime of `content` for pod-a and returns its hash.
fn indexed(x: &RuntimeIndex, content: &[u8]) -> String {
    let rec = gvisor_record("gs://bucket/gvisor.tar", content);
    let hash = rec.bundle_hash();
    x.register(
        "pod-a",
        &[AdvertisedClass {
            class: "gvisor".into(),
            config: "gvisor-release".into(),
            hashed: vec![HashedVersion {
                name: "v1".into(),
                is_default: true,
                rec,
                hash: hash.clone(),
            }],
        }],
    )
    .unwrap();
    hash
}

/// The default runtime dir of `hash` under pod-a, created.
fn default_dir(root: &Path, hash: &str) -> PathBuf {
    let dir = root
        .join("workers/pod-a/default-runtime")
        .join(hash.replace(':', "-"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn key(hash: &str, dir: &Path) -> String {
    format!("{hash}\0{}", dir.display())
}

fn failing_bucket() -> Arc<Bucket> {
    let b = Arc::new(Bucket::default());
    *b.err.lock() = Some("bucket unavailable".into());
    b
}

fn stager(root: &Path, anon: Arc<Bucket>, min_age: Option<Duration>) -> Arc<AssetStager> {
    Arc::new(AssetStager::new(
        cache_at(root, min_age),
        None,
        Some(anon),
        "test-store",
    ))
}

#[tokio::test]
async fn process_retries_then_gives_up() {
    let ctx = Ctx::background();
    let x = Arc::new(RuntimeIndex::in_memory());
    let hash = indexed(&x, &gvisor_release_tarball("runsc bytes"));
    let d = tempfile::tempdir().unwrap();
    let p = RuntimePrewarmer::new(x, stager(d.path(), failing_bucket(), None));
    let dir = default_dir(d.path(), &hash);
    let k = key(&hash, &dir);
    p.enqueue(&hash, &dir);
    for i in 1..=MAX_RETRIES {
        p.process(&ctx, &k).await;
        assert_eq!(p.retries.num_requeues(&k), i, "requeued with backoff");
    }
    p.process(&ctx, &k).await;
    assert_eq!(
        p.retries.num_requeues(&k),
        0,
        "not forgotten after giving up"
    );

    let unknown = "sha256:not-in-the-index";
    p.enqueue(unknown, &dir);
    let uk = key(unknown, &dir);
    p.process(&ctx, &uk).await;
    assert_eq!(
        p.retries.num_requeues(&uk),
        0,
        "an unknown hash was not forgotten"
    );
    assert!(
        !p.items.lock().contains_key(&uk),
        "an unknown hash's item was kept"
    );
}

/// Stages through `inner` after removing `remove`, as a worker removed
/// mid-staging would.
struct Removing {
    inner: Arc<AssetStager>,
    remove: PathBuf,
}

#[async_trait]
impl AssetFetcher for Removing {
    async fn ensure(
        &self,
        ctx: &Ctx,
        rec: &SandboxAssetsRecord,
        dst: &Path,
    ) -> Result<BTreeMap<String, String>, Status> {
        AssetFetcher::ensure(&*self.inner, ctx, rec, dst).await
    }
    async fn stage(
        &self,
        ctx: &Ctx,
        rec: &SandboxAssetsRecord,
        dst: &Path,
    ) -> Result<BTreeMap<String, String>, Status> {
        crate::actordirs::remove_all(&self.remove).unwrap();
        AssetFetcher::stage(&*self.inner, ctx, rec, dst).await
    }
}

#[tokio::test]
async fn superseded_when_the_directory_is_gone() {
    let ctx = Ctx::background();
    let content = gvisor_release_tarball("runsc bytes");
    let x = Arc::new(RuntimeIndex::in_memory());
    let hash = indexed(&x, &content);
    let d = tempfile::tempdir().unwrap();
    let s = stager(
        d.path(),
        Bucket::with(&[("bucket/gvisor.tar", &content)]),
        None,
    );

    // Gone before the item runs.
    let dir = default_dir(d.path(), &hash);
    std::fs::remove_dir_all(&dir).unwrap();
    let p = RuntimePrewarmer::new(x.clone(), s.clone());
    p.enqueue(&hash, &dir);
    p.process(&ctx, &key(&hash, &dir)).await;
    assert_eq!(
        p.retries.num_requeues(&key(&hash, &dir)),
        0,
        "a superseded prewarm was retried"
    );
    assert_eq!(cached_assets(d.path()), 0, "a superseded prewarm fetched");
    assert!(
        !dir.exists(),
        "a superseded prewarm recreated its directory"
    );

    // The worker goes while staging.
    let dir = default_dir(d.path(), &hash);
    let pod = d.path().join("workers/pod-a");
    let p = RuntimePrewarmer::new(
        x,
        Arc::new(Removing {
            inner: s,
            remove: pod.clone(),
        }),
    );
    p.enqueue(&hash, &dir);
    p.process(&ctx, &key(&hash, &dir)).await;
    assert_eq!(
        p.retries.num_requeues(&key(&hash, &dir)),
        0,
        "retried after the worker went"
    );
    assert!(
        !pod.exists(),
        "a prewarm recreated a removed worker's directory"
    );
}

#[tokio::test]
async fn fills_the_asset_cache() {
    let ctx = Ctx::background();
    let content = gvisor_release_tarball("runsc bytes");
    let x = Arc::new(RuntimeIndex::in_memory());
    let hash = indexed(&x, &content);
    let rec = x.lookup(&hash).unwrap().rec;
    let d = tempfile::tempdir().unwrap();
    let anon = Bucket::with(&[("bucket/gvisor.tar", &content)]);
    let s = stager(d.path(), anon.clone(), Some(Duration::ZERO));
    let p = RuntimePrewarmer::new(x, s.clone());
    let dir = default_dir(d.path(), &hash);
    p.prewarm(&ctx, &hash, &dir, &rec).await.unwrap();
    assert_eq!(cached_assets(d.path()), 1);
    assert!(
        dir.join(GVISOR_ASSET_NAME).join("runsc").exists(),
        "not linked into its directory"
    );
    // While linked, the runtime survives an eviction pass.
    let st = s.cache().evict_unused(1 << 40, false).await.unwrap();
    assert_eq!((st.retired, st.skipped_linked), (0, 1), "{st:?}");
    assert_eq!(cached_assets(d.path()), 1);

    let before = anon.gets();
    p.prewarm(&ctx, &hash, &dir, &rec).await.unwrap();
    assert_eq!(
        anon.gets(),
        before,
        "a complete directory was fetched again"
    );

    // Once the worker goes, nothing keeps it.
    std::fs::remove_dir_all(d.path().join("workers/pod-a")).unwrap();
    let st = s.cache().evict_unused(1 << 40, true).await.unwrap();
    assert_eq!(st.retired, 1, "{st:?}");
}

#[tokio::test]
async fn asset_failure_is_an_error() {
    let x = Arc::new(RuntimeIndex::in_memory());
    let hash = indexed(&x, &gvisor_release_tarball("runsc bytes"));
    let rec = x.lookup(&hash).unwrap().rec;
    let d = tempfile::tempdir().unwrap();
    let p = RuntimePrewarmer::new(x, stager(d.path(), failing_bucket(), None));
    let dir = default_dir(d.path(), &hash);
    assert!(p
        .prewarm(&Ctx::background(), &hash, &dir, &rec)
        .await
        .is_err());
}

/// A hung download cannot block the prewarm worker forever.
#[tokio::test]
async fn attempt_timeout_bounds_a_hung_download() {
    let x = Arc::new(RuntimeIndex::in_memory());
    let hash = indexed(&x, &gvisor_release_tarball("runsc bytes"));
    let rec = x.lookup(&hash).unwrap().rec;
    let d = tempfile::tempdir().unwrap();
    let hang = Arc::new(Bucket {
        hang: true,
        ..Default::default()
    });
    let mut p = RuntimePrewarmer::new(x, stager(d.path(), hang, None));
    p.attempt_timeout = Duration::from_millis(50);
    let dir = default_dir(d.path(), &hash);
    let res = tokio::time::timeout(
        Duration::from_secs(10),
        p.prewarm(&Ctx::background(), &hash, &dir, &rec),
    )
    .await
    .expect("prewarm never returned");
    assert!(
        res.is_err(),
        "prewarm succeeded despite the download hanging"
    );
}

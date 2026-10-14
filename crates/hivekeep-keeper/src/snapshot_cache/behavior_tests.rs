//! The snapshot cache: flags, opening, serving modes and cached restores.

use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;

use super::*;
use crate::objectstore::{
    FetchAsset, FetchSnapshot, LocalObjectStore, ObjectStore, UploadSnapshot,
};
use crate::paths::NodePaths;
use crate::worker_jwt::tests::FakeWorkers;
use crate::worker_jwt::WorkerActorJwts;

const URI: &str = "gs://bucket/hives/h/actors/01234567-89ab-cdef-0123-456789abcdef/snapshots/s1";

/// A local store counting downloads.
struct Counting {
    inner: LocalObjectStore,
    gets: AtomicUsize,
}

#[async_trait]
impl ObjectStore for Counting {
    async fn fetch_snapshot(&self, ctx: &Ctx, req: FetchSnapshot) -> Result<(), Status> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.fetch_snapshot(ctx, req).await
    }
    async fn upload_snapshot(&self, ctx: &Ctx, req: UploadSnapshot) -> Result<(), Status> {
        self.inner.upload_snapshot(ctx, req).await
    }
    async fn fetch_asset(&self, ctx: &Ctx, req: FetchAsset) -> Result<(), Status> {
        self.inner.fetch_asset(ctx, req).await
    }
}

struct Env {
    _d: tempfile::TempDir,
    base: PathBuf,
    paths: Arc<NodePaths>,
    store: Arc<Counting>,
    transfer: Arc<SnapshotTransfer>,
    cache: filecache::Store,
}

impl Env {
    /// A store holding `files` of the snapshot at [`URI`].
    async fn new(files: &[(&str, &str)]) -> Env {
        let d = tempfile::tempdir().unwrap();
        let base = d.path().to_path_buf();
        let paths = Arc::new(NodePaths::new(base.join("node")));
        let store = Arc::new(Counting {
            inner: LocalObjectStore::new(base.join("objects")),
            gets: AtomicUsize::new(0),
        });
        let src = base.join("payload");
        for (name, content) in files {
            let p = src.join(name);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, content).unwrap();
        }
        if !files.is_empty() {
            store
                .upload_snapshot(
                    &Ctx::background(),
                    UploadSnapshot {
                        snapshot_uri: URI.into(),
                        local_path: src,
                        files: files.iter().map(|(n, _)| n.to_string()).collect(),
                        actor_jwt: String::new(),
                    },
                )
                .await
                .unwrap();
        }
        let jwts = Arc::new(WorkerActorJwts::new(
            Arc::new(FakeWorkers::default()),
            Arc::new(crate::clock::SystemClock),
        ));
        let transfer = Arc::new(SnapshotTransfer::new(
            store.clone(),
            jwts,
            paths.clone(),
            "boot".into(),
        ));
        let cache =
            filecache::Store::open(base.join("cache"), filecache::Options::default()).unwrap();
        Env {
            _d: d,
            base,
            paths,
            store,
            transfer,
            cache,
        }
    }

    fn downloader(&self) -> SnapshotDownloader {
        SnapshotDownloader::new(self.transfer.clone(), Some(self.cache.clone()))
    }

    fn gets(&self) -> usize {
        self.store.gets.load(Ordering::SeqCst)
    }

    fn restore_dir(&self, name: &str) -> PathBuf {
        let d = self.base.join(name);
        std::fs::create_dir_all(&d).unwrap();
        d
    }
}

fn actor() -> TransferActor {
    TransferActor {
        hive: "h".into(),
        name: "a".into(),
        uid: "01234567-89ab-cdef-0123-456789abcdef".into(),
    }
}

fn files(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| s.to_string()).collect()
}

fn args(extra: &[&str]) -> Result<crate::config::Args, String> {
    use clap::Parser;
    let mut v = vec!["hivekeep-keeper"];
    v.extend_from_slice(extra);
    let a = crate::config::Args::try_parse_from(v).map_err(|e| e.to_string())?;
    a.validate()?;
    Ok(a)
}

/// A negative min age would invert the eviction veto, and a cache outside
/// the base dir could not be filled by the object store; negative
/// durations do not parse at all.
#[test]
fn validates_snapshot_cache_flags() {
    assert!(
        args(&["--snapshot-cache-min-age", "10m"]).is_ok(),
        "defaults"
    );
    assert!(args(&["--no-snapshot-cache"]).is_ok(), "disabled");
    assert!(
        args(&["--no-snapshot-cache", "--snapshot-cache-min-age=-1s"]).is_err(),
        "disabled still rejects a bad min age"
    );
    assert!(
        args(&["--snapshot-cache-min-age", "0s"]).is_ok(),
        "zero min age"
    );
    assert!(
        args(&["--snapshot-cache-dir", "/var/lib/elsewhere/snapshot-cache"]).is_err(),
        "outside the base dir"
    );
    assert!(
        args(&["--snapshot-cache-min-age=-1s"]).is_err(),
        "negative min age"
    );
    for extra in [&[][..], &["--no-snapshot-cache"][..]] {
        let mut a = extra.to_vec();
        a.push("--snapshot-cache-gc-period=-1s");
        assert!(args(&a).is_err(), "negative GC period with {extra:?}");
    }
    for t in ["0s", "-1s"] {
        let flag = format!("--snapshot-cache-fetch-timeout={t}");
        assert!(args(&[&flag]).is_err(), "fetch timeout {t}");
    }
}

#[test]
fn snapshot_cache_disabled() {
    assert_eq!(
        args(&["--no-snapshot-cache"]).unwrap().snapshot_cache_dir(),
        None
    );
}

#[tokio::test]
async fn open_creates_the_root() {
    let d = tempfile::tempdir().unwrap();
    let dir = d.path().join("nested/snapshot-cache");
    open_snapshot_cache(&dir, filecache::Options::default())
        .await
        .unwrap();
    assert!(dir.is_dir());
}

#[tokio::test]
async fn open_sweeps_debris() {
    let d = tempfile::tempdir().unwrap();
    let dir = d.path();
    let entry = dir.join("entries/aaaa");
    for sub in [
        dir.join("tmp/aaaa-12345"),
        dir.join(".rm-bbbb-xyz"),
        entry.clone(),
    ] {
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("data"), b"x").unwrap();
    }
    open_snapshot_cache(dir, filecache::Options::default())
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_dir(dir.join("tmp")).unwrap().count(),
        0,
        "tmp debris not swept"
    );
    assert!(
        !dir.join(".rm-bbbb-xyz").exists(),
        "retired debris not swept"
    );
    assert!(
        entry.join("data").exists(),
        "a published entry did not survive"
    );
}

#[test]
fn cache_mode_follows_declared_sharing() {
    let with = |sharing: pb::SnapshotSharing| pb::ObjectStorage {
        snapshot_uri: URI.into(),
        sharing: sharing as i32,
    };
    for (name, object, want) in [
        (
            "shared uses the class mode",
            Some(with(pb::SnapshotSharing::Shared)),
            CacheMode::Link,
        ),
        (
            "private is never cached",
            Some(with(pb::SnapshotSharing::Private)),
            CacheMode::Off,
        ),
        (
            "unstated is never cached",
            Some(with(pb::SnapshotSharing::Unspecified)),
            CacheMode::Off,
        ),
        ("no object", None, CacheMode::Off),
    ] {
        assert_eq!(
            snapshot_cache_mode(object.as_ref(), CacheMode::Link),
            want,
            "{name}"
        );
    }
}

/// gVisor consumes restore state read-only; a micro-VM keeps and reworks
/// its files; an unknown class is not cached.
#[test]
fn cache_mode_per_class() {
    assert_eq!(cache_mode_for("gvisor"), CacheMode::Link);
    assert_eq!(cache_mode_for("microvm"), CacheMode::Copy);
    assert_eq!(cache_mode_for(""), CacheMode::Off);
}

#[tokio::test]
async fn fetch_manifest_cached() {
    const MANIFEST: &str = r#"{"sandboxClass":"gvisor"}"#;
    let env = Env::new(&[(MANIFEST_NAME, MANIFEST)]).await;
    let dl = env.downloader();
    let ctx = Ctx::background();
    for i in 0..2 {
        let got = dl
            .fetch_manifest(&ctx, &actor(), URI, CacheMode::Link)
            .await
            .unwrap();
        assert_eq!(got, MANIFEST.as_bytes(), "read {i}");
    }
    assert_eq!(env.gets(), 1, "two cached manifest reads downloaded twice");
    let scratch = env.paths.snapshot_scratch_dir();
    assert_eq!(
        std::fs::read_dir(&scratch).unwrap().count(),
        0,
        "scratch dir not emptied"
    );
    dl.fetch_manifest(&ctx, &actor(), URI, CacheMode::Off)
        .await
        .unwrap();
    assert_eq!(
        env.gets(),
        2,
        "a private manifest read was served from the cache"
    );
}

#[tokio::test]
async fn cached_restores_link_one_download() {
    let env = Env::new(&[("memory-ranges", "golden memory")]).await;
    let dl = env.downloader();
    let ctx = Ctx::background();
    let fs = files(&["memory-ranges"]);
    let (first, second) = (env.restore_dir("first"), env.restore_dir("second"));
    for dir in [&first, &second] {
        dl.download(&ctx, &actor(), URI, dir, &fs, CacheMode::Link)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("memory-ranges")).unwrap(),
            "golden memory"
        );
    }
    assert_eq!(env.gets(), 1, "two cached restores downloaded twice");
    let (m1, m2) = (
        std::fs::metadata(first.join("memory-ranges")).unwrap(),
        std::fs::metadata(second.join("memory-ranges")).unwrap(),
    );
    assert_eq!(
        (m1.dev(), m1.ino()),
        (m2.dev(), m2.ino()),
        "cached destinations do not share an inode"
    );
    assert_eq!(
        m1.permissions().mode() & 0o222,
        0,
        "a linked restore file is writable"
    );

    let direct = env.restore_dir("direct");
    dl.download(&ctx, &actor(), URI, &direct, &fs, CacheMode::Off)
        .await
        .unwrap();
    assert_eq!(env.gets(), 2, "a direct restore did not download");
    let m3 = std::fs::metadata(direct.join("memory-ranges")).unwrap();
    assert_ne!(
        m3.ino(),
        m1.ino(),
        "a direct restore shares the cache's inode"
    );
}

#[tokio::test]
async fn cached_restores_copy_mode() {
    let env = Env::new(&[("memory-ranges", "golden memory")]).await;
    let dl = env.downloader();
    let ctx = Ctx::background();
    let fs = files(&["memory-ranges"]);
    let (first, second) = (env.restore_dir("first"), env.restore_dir("second"));
    for dir in [&first, &second] {
        dl.download(&ctx, &actor(), URI, dir, &fs, CacheMode::Copy)
            .await
            .unwrap();
    }
    assert_eq!(env.gets(), 1);
    let (m1, m2) = (
        std::fs::metadata(first.join("memory-ranges")).unwrap(),
        std::fs::metadata(second.join("memory-ranges")).unwrap(),
    );
    assert_ne!(m1.ino(), m2.ino(), "copy-mode consumers share an inode");
    // A consumer changing its copy does not change the cached bytes.
    std::fs::write(first.join("memory-ranges"), b"rewritten by the worker").unwrap();
    let third = env.restore_dir("third");
    dl.download(&ctx, &actor(), URI, &third, &fs, CacheMode::Copy)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(third.join("memory-ranges")).unwrap(),
        "golden memory"
    );
    assert_eq!(env.gets(), 1, "a consumer's change invalidated the cache");
}

#[tokio::test]
async fn without_a_cache_restores_download() {
    let env = Env::new(&[("memory-ranges", "golden memory")]).await;
    let dl = SnapshotDownloader::new(env.transfer.clone(), None);
    let dir = env.restore_dir("out");
    dl.download(
        &Ctx::background(),
        &actor(),
        URI,
        &dir,
        &files(&["memory-ranges"]),
        CacheMode::Link,
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(dir.join("memory-ranges")).unwrap(),
        "golden memory"
    );
    assert_eq!(env.gets(), 1);
}

#[tokio::test]
async fn cached_fetch_error_reaches_the_caller() {
    let env = Env::new(&[("memory-ranges", "golden memory")]).await;
    let dir = env.restore_dir("out");
    let res = env
        .downloader()
        .download(
            &Ctx::background(),
            &actor(),
            URI,
            &dir,
            &files(&["missing"]),
            CacheMode::Link,
        )
        .await;
    assert!(res.is_err(), "succeeded for a missing file");
    assert!(
        !dir.join("missing").exists(),
        "a failed fetch left a destination file"
    );
}

#[tokio::test]
async fn concurrent_restores_share_one_download() {
    let env = Env::new(&[("memory-ranges", "golden memory")]).await;
    let dl = env.downloader();
    let ctx = Ctx::background();
    let fs = files(&["memory-ranges"]);
    let dirs: Vec<PathBuf> = (0..4)
        .map(|i| env.restore_dir(&format!("actor-{i}")))
        .collect();
    let a = actor();
    let results = futures::future::join_all(
        dirs.iter()
            .map(|d| dl.download(&ctx, &a, URI, d, &fs, CacheMode::Link)),
    )
    .await;
    for (i, r) in results.into_iter().enumerate() {
        r.unwrap_or_else(|e| panic!("restore {i}: {e:?}"));
        assert_eq!(
            std::fs::read_to_string(dirs[i].join("memory-ranges")).unwrap(),
            "golden memory"
        );
    }
    assert_eq!(
        env.gets(),
        1,
        "concurrent restores downloaded more than once"
    );
}

#[tokio::test]
async fn cached_subdirectories_and_symlinks() {
    let env = Env::new(&[
        ("memory-ranges", "golden memory"),
        ("fs/data.tar", "fs delta"),
        ("fs/rootfs-upper-app.tar", "rootfs delta"),
    ])
    .await;
    let dl = env.downloader();
    let ctx = Ctx::background();
    let dir = env.restore_dir("subdir");
    dl.download(
        &ctx,
        &actor(),
        URI,
        &dir,
        &files(&["memory-ranges", "fs/data.tar", "fs/rootfs-upper-app.tar"]),
        CacheMode::Link,
    )
    .await
    .unwrap();
    for (name, want) in [
        ("fs/data.tar", "fs delta"),
        ("fs/rootfs-upper-app.tar", "rootfs delta"),
    ] {
        assert_eq!(
            std::fs::read_to_string(dir.join(name)).unwrap(),
            want,
            "{name}"
        );
    }

    let outside = env.restore_dir("outside");
    let keep = outside.join("memory-ranges");
    std::fs::write(&keep, b"keep me").unwrap();
    // A symlink in place of the restore file.
    let file_link = env.restore_dir("file-link");
    std::os::unix::fs::symlink(&keep, file_link.join("memory-ranges")).unwrap();
    assert!(
        dl.download(
            &ctx,
            &actor(),
            URI,
            &file_link,
            &files(&["memory-ranges"]),
            CacheMode::Link
        )
        .await
        .is_err(),
        "a symlink in place of the restore file was followed"
    );
    // A symlink in place of the subdirectory.
    let dir_link = env.restore_dir("dir-link");
    std::os::unix::fs::symlink(&outside, dir_link.join("fs")).unwrap();
    assert!(
        dl.download(
            &ctx,
            &actor(),
            URI,
            &dir_link,
            &files(&["fs/data.tar"]),
            CacheMode::Copy
        )
        .await
        .is_err(),
        "a symlink in place of the checkpoint subdirectory was followed"
    );
    assert_eq!(std::fs::read_to_string(&keep).unwrap(), "keep me");
    assert_eq!(
        std::fs::read_dir(&outside).unwrap().count(),
        1,
        "files appeared outside the restore dir"
    );
}

/// A link-mode restore into another mount than the cache falls back to a
/// private copy of the cached entry, not a download, and warns once.
#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "needs /dev/shm on another mount than the temp dir"]
async fn link_mode_falls_back_to_copy_across_mounts() {
    let env = Env::new(&[("memory-ranges", "golden memory")]).await;
    let dl = env.downloader();
    let ctx = Ctx::background();
    let fs = files(&["memory-ranges"]);
    let other = tempfile::Builder::new()
        .prefix("snapshot-cache-test-")
        .tempdir_in("/dev/shm")
        .unwrap();
    let probe = env.base.join("probe");
    std::fs::write(&probe, b"").unwrap();
    assert!(
        std::fs::hard_link(&probe, other.path().join("probe")).is_err(),
        "the test dirs share a filesystem"
    );
    for name in ["first", "second"] {
        let dir = other.path().join(name);
        std::fs::create_dir(&dir).unwrap();
        dl.download(&ctx, &actor(), URI, &dir, &fs, CacheMode::Link)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("memory-ranges")).unwrap(),
            "golden memory"
        );
        assert_eq!(env.gets(), 1, "{name}: the fallback downloaded");
    }
    assert!(
        dl.cross_mount_warned.load(Ordering::Relaxed),
        "the misconfiguration was not reported"
    );
}

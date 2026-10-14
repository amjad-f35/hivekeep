//! Actor directory, snapshot transfer and sandbox asset behaviour the
//! Keeper RPCs rely on.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hivekeep_core::snapshot::SnapshotUri;
use hivekeep_proto::keeper::v1 as pb;
use sha2::{Digest, Sha256};
use tonic::{Code, Status};

use crate::actordirs;
use crate::assets::{AssetEntry, SandboxAssetsRecord, MANIFEST_NAME};
use crate::context::Ctx;
use crate::filecache;
use crate::labels::fidelity;
use crate::paths::NodePaths;
use crate::sandbox_assets::{AnonymousFetcher, AssetStager};
use crate::service_stubs::{Minter, RecordingStore};
use crate::snapshot_cache::{CacheMode, SnapshotDownloader};
use crate::snapshot_transfer::SnapshotTransfer;
use crate::worker_jwt::{TransferActor, WorkerActorJwts};

const OWNER: &str = "3d7f1b62-8a04-4c19-b5e6-2f9c7a1d0e38";

fn test_snapshot_uri() -> String {
    format!("gs://bucket/root/hives/demo/actors/{OWNER}/snapshots/counter-1-snap")
}

fn paused_snapshot_uri() -> String {
    format!("gs://bucket/root/hives/demo/actors/{OWNER}/snapshots/snap-1")
}

fn test_actor() -> TransferActor {
    TransferActor {
        hive: "demo".into(),
        name: "bot".into(),
        uid: "9f1e0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a".into(),
    }
}

/// Names what is left in `dir`, if it still exists.
fn assert_dir_gone(what: &str, dir: &Path) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        let left: Vec<String> = entries
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        panic!("{what} {} survived with entries {left:?}", dir.display());
    }
}

#[test]
fn remove_actor_dirs_reclaims_the_root() {
    let d = tempfile::tempdir().unwrap();
    let paths = NodePaths::new(d.path());
    let uid = "actor-uid-1";
    actordirs::reset_actor_dirs(&paths, uid).unwrap();
    // Written at Run and kept out of the directories a reset empties.
    std::fs::write(paths.actor_sandbox_assets_file(uid), b"{}").unwrap();
    actordirs::remove_actor_dirs(&paths, uid).unwrap();
    assert_dir_gone("actor dir", &paths.actor_dir(uid));
    assert_dir_gone("actor snapshot dir", &paths.actor_snapshot_dir(uid));
}

#[test]
fn reset_actor_dirs_creates_both_trees() {
    let d = tempfile::tempdir().unwrap();
    let paths = NodePaths::new(d.path());
    let uid = "actor-uid-isolation";
    actordirs::reset_actor_dirs(&paths, uid).unwrap();
    let trees: [(PathBuf, Vec<(&str, PathBuf)>); 2] = [
        (
            paths.actor_snapshot_dir(uid),
            vec![
                ("checkpoint state dir", paths.checkpoint_state_dir(uid)),
                ("restore state dir", paths.restore_state_dir(uid)),
            ],
        ),
        (
            paths.actor_dir(uid),
            vec![
                ("OCI bundle dir", paths.oci_bundle_dir(uid)),
                (
                    "durable dir volume mounts dir",
                    paths.durable_dir_volume_mounts_dir(uid),
                ),
                (
                    "system info volume roots dir",
                    paths.system_info_volume_roots_dir(uid),
                ),
                (
                    "external volume mounts dir",
                    paths.external_volume_mounts_dir(uid),
                ),
            ],
        ),
    ];
    for (root, dirs) in trees {
        for (name, dir) in dirs {
            assert!(
                dir.starts_with(&root),
                "{name} {} not under {}",
                dir.display(),
                root.display()
            );
            assert!(
                dir.is_dir(),
                "reset did not create {name} {}",
                dir.display()
            );
        }
    }
}

#[cfg(unix)]
#[test]
fn remove_actor_dirs_keeps_actor_dir_when_snapshot_tree_remains() {
    use std::os::unix::fs::PermissionsExt;
    if unsafe { libc::geteuid() } == 0 {
        // Root ignores directory permissions, so nothing is undeletable.
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let paths = NodePaths::new(d.path());
    let uid = "actor-uid-1";
    actordirs::reset_actor_dirs(&paths, uid).unwrap();
    let record = paths.actor_sandbox_assets_file(uid);
    std::fs::write(&record, b"{}").unwrap();
    // A local snapshot dir without the write bit: its file cannot go.
    let stuck = paths.local_snapshot_dir(uid, "pause-stuck");
    std::fs::create_dir_all(&stuck).unwrap();
    std::fs::write(stuck.join("checkpoint.img"), b"").unwrap();
    std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o500)).unwrap();
    let res = actordirs::remove_actor_dirs(&paths, uid);
    std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(res.is_err(), "want an error for the undeletable snapshot");
    assert!(
        record.exists(),
        "sandbox record gone; a retried Terminate needs it"
    );
}

/// Removing the root keeps the mount safety: a populated volume directory
/// means the unmount did not take, and deleting it would delete the
/// volume's data.
#[test]
fn remove_actor_dirs_keeps_populated_volume() {
    let d = tempfile::tempdir().unwrap();
    let paths = NodePaths::new(d.path());
    let uid = "actor-uid-1";
    let still_mounted = paths.external_volume_host_path(uid, "data").join("payload");
    std::fs::create_dir_all(still_mounted.parent().unwrap()).unwrap();
    std::fs::write(&still_mounted, b"volume contents").unwrap();
    let err = actordirs::remove_actor_dirs(&paths, uid).unwrap_err();
    assert!(!err.to_string().contains("%w"), "{err}");
    assert!(
        still_mounted.exists(),
        "deleted the contents of a populated volume"
    );
}

#[cfg(unix)]
#[test]
fn reset_dir_cases() {
    use std::os::unix::fs::PermissionsExt;
    // Empties and recreates the directory with its mode.
    let d = tempfile::tempdir().unwrap();
    let dir = d.path().join("state");
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    actordirs::reset_dir("state dir", &dir, actordirs::remove_all, 0o750).unwrap();
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
    assert_eq!(
        std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
        0o750
    );

    // Names the directory when the remove fails, keeping the cause.
    fn boom(_: &Path) -> std::io::Result<()> {
        Err(std::io::Error::other("boom"))
    }
    let err = actordirs::reset_dir("state dir", d.path(), boom, 0o700).unwrap_err();
    assert_eq!(err.to_string(), "while deleting state dir: boom");
    assert_eq!(
        std::error::Error::source(&err).map(|s| s.to_string()),
        Some("boom".to_string())
    );

    // Names the directory when the create fails.
    let file = d.path().join("file");
    std::fs::write(&file, b"").unwrap();
    fn noop(_: &Path) -> std::io::Result<()> {
        Ok(())
    }
    let err = actordirs::reset_dir("state dir", &file.join("child"), noop, 0o700).unwrap_err();
    assert!(
        err.to_string().starts_with("while creating state dir: "),
        "{err}"
    );
    assert!(!err.to_string().contains("%w"), "{err}");
}

fn transfer(d: &Path, store: Arc<RecordingStore>, boot_id: &str) -> SnapshotTransfer {
    let jwts = Arc::new(WorkerActorJwts::new(
        Arc::new(Minter::new(
            "test-actor-jwt",
            chrono::Utc::now() + chrono::Duration::hours(1),
        )),
        Arc::new(crate::clock::SystemClock),
    ));
    SnapshotTransfer::new(
        store,
        jwts,
        Arc::new(NodePaths::new(d.join("node"))),
        boot_id.into(),
    )
}

#[cfg(unix)]
#[tokio::test]
async fn upload_snapshot_rejects_symlink_outside_root() {
    let d = tempfile::tempdir().unwrap();
    let checkpoint = d.path().join("checkpoint-state");
    std::fs::create_dir(&checkpoint).unwrap();
    let outside = d.path().join("outside");
    std::fs::write(&outside, b"secret").unwrap();
    std::os::unix::fs::symlink(&outside, checkpoint.join("checkpoint.img")).unwrap();
    let store = Arc::new(RecordingStore::new(&d.path().join("objects")));
    let t = transfer(d.path(), store.clone(), "boot-1");
    let uri = SnapshotUri::parse(&test_snapshot_uri()).unwrap();
    let rec = SandboxAssetsRecord {
        snapshot_files: vec!["checkpoint.img".into()],
        ..Default::default()
    };
    let res = t
        .upload_snapshot(&Ctx::background(), &test_actor(), &uri, &checkpoint, &rec)
        .await;
    assert!(
        res.is_err(),
        "followed a symlink outside the checkpoint directory"
    );
    assert!(
        store.keys(&test_snapshot_uri()).is_empty(),
        "objects uploaded"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn download_external_checkpoint_rejects_symlink_outside_root() {
    let d = tempfile::tempdir().unwrap();
    let restore = d.path().join("restore-state");
    std::fs::create_dir(&restore).unwrap();
    let outside = d.path().join("outside");
    std::fs::write(&outside, b"keep me").unwrap();
    std::os::unix::fs::symlink(&outside, restore.join("checkpoint.img")).unwrap();
    let store = Arc::new(RecordingStore::new(&d.path().join("objects")));
    let obj = store.inner.dir_for(&test_snapshot_uri()).unwrap();
    std::fs::create_dir_all(&obj).unwrap();
    std::fs::write(obj.join("checkpoint.img"), b"replacement").unwrap();
    let dl = SnapshotDownloader::new(Arc::new(transfer(d.path(), store, "boot-1")), None);
    let res = dl
        .download(
            &Ctx::background(),
            &test_actor(),
            &test_snapshot_uri(),
            &restore,
            &["checkpoint.img".into()],
            CacheMode::Off,
        )
        .await;
    assert!(
        res.is_err(),
        "followed a symlink outside the restore directory"
    );
    assert_eq!(std::fs::read(&outside).unwrap(), b"keep me");
}

fn promote_snapshot(f: pb::SnapshotFidelity) -> pb::Snapshot {
    pb::Snapshot {
        snapshot_uuid: "pause-snap-1".into(),
        generation: 1,
        fidelity: f as i32,
        object: Some(pb::ObjectStorage {
            snapshot_uri: paused_snapshot_uri(),
            sharing: 0,
        }),
    }
}

/// Lays a node-local snapshot out in `dir`: the files and the manifest.
fn write_local_snapshot(dir: &Path, rec: &SandboxAssetsRecord, contents: &[(&str, &str)]) {
    std::fs::create_dir_all(dir).unwrap();
    for (name, body) in contents {
        std::fs::write(dir.join(name), body).unwrap();
    }
    std::fs::write(dir.join(MANIFEST_NAME), rec.to_json()).unwrap();
}

fn full_rec(boot_id: &str) -> SandboxAssetsRecord {
    SandboxAssetsRecord {
        sandbox_class: "microvm".into(),
        snapshot_files: vec![
            "config.json".into(),
            "memory-ranges".into(),
            "data.tar".into(),
        ],
        data_snapshot_files: vec!["data.tar".into()],
        fidelity: fidelity::MEMORY.into(),
        boot_id: boot_id.into(),
        ..Default::default()
    }
}

const FULL_FILES: [(&str, &str); 3] = [
    ("config.json", "cfg"),
    ("memory-ranges", "mem"),
    ("data.tar", "data"),
];

struct Promote {
    _d: tempfile::TempDir,
    store: Arc<RecordingStore>,
    t: SnapshotTransfer,
    dir: PathBuf,
}

fn promote_env(put_err: Option<Status>) -> Promote {
    let d = tempfile::tempdir().unwrap();
    let mut store = RecordingStore::new(&d.path().join("objects"));
    store.put_err = put_err;
    let store = Arc::new(store);
    let t = transfer(d.path(), store.clone(), "boot-2");
    let dir = d.path().join("pause-snap-1");
    Promote {
        _d: d,
        store,
        t,
        dir,
    }
}

impl Promote {
    async fn upload(&self, f: pb::SnapshotFidelity) -> Result<(String, bool), Status> {
        let uri = SnapshotUri::parse(&paused_snapshot_uri()).unwrap();
        self.t
            .upload_local_checkpoint_dir(
                &Ctx::background(),
                &test_actor(),
                &promote_snapshot(f),
                &self.dir,
                &uri,
            )
            .await
    }

    fn keys(&self) -> Vec<String> {
        self.store.keys(&paused_snapshot_uri())
    }

    fn remote_manifest(&self) -> SandboxAssetsRecord {
        let dir = self.store.inner.dir_for(&paused_snapshot_uri()).unwrap();
        SandboxAssetsRecord::parse(&std::fs::read(dir.join(MANIFEST_NAME)).unwrap()).unwrap()
    }

    fn put_remote_manifest(&self, body: &str) {
        let dir = self.store.inner.dir_for(&paused_snapshot_uri()).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(MANIFEST_NAME), body).unwrap();
    }
}

#[tokio::test]
async fn upload_local_checkpoint_dir_matching_fidelity_uploads_all_files() {
    let p = promote_env(None);
    write_local_snapshot(&p.dir, &full_rec("boot-2"), &FULL_FILES);
    p.upload(pb::SnapshotFidelity::Memory).await.unwrap();
    assert_eq!(
        p.keys(),
        vec!["config.json", "data.tar", "manifest.json", "memory-ranges"]
    );
    let remote = p.remote_manifest();
    assert_eq!(remote.fidelity, fidelity::MEMORY);
    assert_eq!(
        remote.boot_id, "",
        "the boot ID describes the local copy only"
    );
}

#[tokio::test]
async fn upload_local_checkpoint_dir_matching_rootfs_fidelity_uploads_all_files() {
    let p = promote_env(None);
    let rec = SandboxAssetsRecord {
        sandbox_class: "microvm".into(),
        snapshot_files: vec!["rootfs-upper-app.tar".into(), "data.tar".into()],
        data_snapshot_files: vec!["data.tar".into()],
        fidelity: fidelity::ROOTFS.into(),
        boot_id: "boot-2".into(),
        ..Default::default()
    };
    write_local_snapshot(
        &p.dir,
        &rec,
        &[("rootfs-upper-app.tar", "upper"), ("data.tar", "data")],
    );
    p.upload(pb::SnapshotFidelity::Rootfs).await.unwrap();
    assert_eq!(
        p.keys(),
        vec!["data.tar", "manifest.json", "rootfs-upper-app.tar"]
    );
    assert_eq!(p.remote_manifest().fidelity, fidelity::ROOTFS);
}

/// The capture and the upload carry the template's one fidelity; a
/// mismatch either way is drift, never a conversion.
#[tokio::test]
async fn upload_local_checkpoint_dir_refuses_fidelity_drift() {
    for (name, captured, desired) in [
        (
            "volumes capture cannot become memory",
            fidelity::VOLUMES,
            pb::SnapshotFidelity::Memory,
        ),
        (
            "memory capture cannot become volumes",
            fidelity::MEMORY,
            pb::SnapshotFidelity::Volumes,
        ),
        (
            "memory capture cannot become rootfs",
            fidelity::MEMORY,
            pb::SnapshotFidelity::Rootfs,
        ),
        (
            "rootfs capture cannot become memory",
            fidelity::ROOTFS,
            pb::SnapshotFidelity::Memory,
        ),
    ] {
        let p = promote_env(None);
        let mut rec = full_rec("boot-2");
        rec.fidelity = captured.into();
        write_local_snapshot(&p.dir, &rec, &FULL_FILES);
        let err = p.upload(desired).await.unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "{name}: {err:?}");
        assert!(
            p.keys().is_empty(),
            "{name}: uploaded despite the rejection"
        );
    }
}

#[tokio::test]
async fn upload_local_checkpoint_dir_manifest_without_fidelity_is_rejected() {
    let p = promote_env(None);
    let rec = SandboxAssetsRecord {
        sandbox_class: "microvm".into(),
        snapshot_files: vec!["data.tar".into()],
        boot_id: "boot-2".into(),
        ..Default::default()
    };
    write_local_snapshot(&p.dir, &rec, &[("data.tar", "data")]);
    let err = p.upload(pb::SnapshotFidelity::Volumes).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert!(p.keys().is_empty());
}

#[tokio::test]
async fn upload_local_checkpoint_dir_gone_locally_but_already_uploaded_succeeds() {
    let p = promote_env(None);
    p.put_remote_manifest(r#"{"sandboxClass":"microvm"}"#);
    let (_, uploaded) = p.upload(pb::SnapshotFidelity::Memory).await.unwrap();
    assert!(!uploaded);
}

#[tokio::test]
async fn upload_local_checkpoint_dir_gone_locally_and_remotely_is_not_found() {
    let p = promote_env(None);
    let err = p.upload(pb::SnapshotFidelity::Memory).await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err:?}");
    assert!(
        err.message().contains("gone and no uploaded copy exists"),
        "{}",
        err.message()
    );
}

#[tokio::test]
async fn upload_local_checkpoint_dir_upload_failure_returns_the_error() {
    let p = promote_env(Some(Status::unavailable("boom")));
    write_local_snapshot(&p.dir, &full_rec("boot-2"), &FULL_FILES);
    assert!(p.upload(pb::SnapshotFidelity::Memory).await.is_err());
}

#[tokio::test]
async fn upload_local_checkpoint_dir_earlier_boot_already_uploaded_skips_upload() {
    let p = promote_env(Some(Status::internal(
        "must not upload a local copy from before the node rebooted",
    )));
    p.put_remote_manifest(r#"{"sandboxClass":"microvm"}"#);
    write_local_snapshot(
        &p.dir,
        &full_rec("boot-1"),
        &[
            ("config.json", "cfg"),
            ("memory-ranges", ""),
            ("data.tar", "data"),
        ],
    );
    let (_, uploaded) = p.upload(pb::SnapshotFidelity::Memory).await.unwrap();
    assert!(!uploaded, "uploaded a copy when the remote manifest exists");
}

#[tokio::test]
async fn upload_local_checkpoint_dir_earlier_boot_not_uploaded_is_not_found() {
    let p = promote_env(None);
    write_local_snapshot(&p.dir, &full_rec("boot-1"), &FULL_FILES);
    let err = p.upload(pb::SnapshotFidelity::Memory).await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err:?}");
    assert!(
        p.keys().is_empty(),
        "uploaded a copy from before the node rebooted"
    );
}

/// Reads fixed bytes as a public object store would, counting reads.
#[derive(Default)]
struct FakeAnon {
    data: Vec<u8>,
    err: Option<String>,
    gets: AtomicUsize,
    delay: Duration,
}

#[async_trait]
impl AnonymousFetcher for FakeAnon {
    async fn fetch(&self, _: &Ctx, _: &url::Url, path: &Path, _: i64) -> Result<(), String> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        std::fs::write(path, &self.data).map_err(|e| e.to_string())
    }
}

fn stager(root: &Path, anon: Arc<FakeAnon>, max: Option<i64>) -> AssetStager {
    let cache = filecache::Store::open(root.join("cache"), filecache::Options::default()).unwrap();
    let s = AssetStager::new(cache, None, Some(anon), "test-store");
    match max {
        Some(m) => s.with_max_bytes(m),
        None => s,
    }
}

fn cached_assets(root: &Path) -> usize {
    std::fs::read_dir(root.join("cache/entries"))
        .unwrap()
        .count()
}

/// Stages `entry` as asset "asset" into a fresh consumer dir.
async fn stage_one(
    s: &AssetStager,
    root: &Path,
    n: usize,
    entry: AssetEntry,
) -> Result<PathBuf, Status> {
    let dir = root.join("consumer").join(n.to_string());
    std::fs::create_dir_all(&dir).unwrap();
    stage_into(s, &dir, entry).await
}

async fn stage_into(s: &AssetStager, dir: &Path, entry: AssetEntry) -> Result<PathBuf, Status> {
    let mut rec = SandboxAssetsRecord::default();
    rec.assets.insert("asset".into(), entry);
    let paths: BTreeMap<String, String> = s.stage(&Ctx::background(), &rec, dir).await?;
    Ok(PathBuf::from(&paths["asset"]))
}

/// The hash is validated before the destination is looked at: a planted
/// file there would otherwise be returned as the staged asset.
#[tokio::test]
async fn stage_asset_rejects_bad_hash() {
    let d = tempfile::tempdir().unwrap();
    let dir = d.path().join("consumer");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("asset"), b"planted").unwrap();
    let s = stager(d.path(), Arc::new(FakeAnon::default()), None);
    let err = stage_into(
        &s,
        &dir,
        AssetEntry {
            url: String::new(),
            sha256: "deadbeef".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(
        err.message().contains("while validating asset hash"),
        "error did not come from hash validation: {err:?}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn stage_asset_streaming() {
    use std::os::unix::fs::PermissionsExt;
    let content = b"micro-vm kernel bytes".to_vec();
    let good = hex::encode(Sha256::digest(&content));
    let url = "gs://test-bucket/asset";
    let entry = |u: &str, h: &str| AssetEntry {
        url: u.into(),
        sha256: h.into(),
    };
    let anon = |data: &[u8]| {
        Arc::new(FakeAnon {
            data: data.to_vec(),
            ..Default::default()
        })
    };

    // A good asset is cached and staged read-only and executable.
    let d = tempfile::tempdir().unwrap();
    let s = stager(d.path(), anon(&content), None);
    let path = stage_one(&s, d.path(), 1, entry(url, &good)).await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), content);
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o555
    );
    assert_eq!(cached_assets(d.path()), 1);

    // An over-cap asset is rejected and not cached.
    let d = tempfile::tempdir().unwrap();
    let s = stager(d.path(), anon(&content), Some(4));
    assert!(stage_one(&s, d.path(), 1, entry(url, &good)).await.is_err());
    assert_eq!(
        cached_assets(d.path()),
        0,
        "over-cap download left a cache entry"
    );

    // A hash mismatch is rejected and not cached.
    let d = tempfile::tempdir().unwrap();
    let s = stager(d.path(), anon(&content), None);
    let wrong = "a".repeat(64);
    assert!(stage_one(&s, d.path(), 1, entry(url, &wrong))
        .await
        .is_err());
    assert_eq!(
        cached_assets(d.path()),
        0,
        "mismatched download left a cache entry"
    );

    // A missing object stays recognizable as one through the store's error.
    let d = tempfile::tempdir().unwrap();
    let store_root = d.path().join("objects");
    let cache =
        filecache::Store::open(d.path().join("cache"), filecache::Options::default()).unwrap();
    let s = AssetStager::new(
        cache,
        Some(Arc::new(crate::objectstore::LocalObjectStore::new(
            &store_root,
        ))),
        None,
        "test-store",
    );
    let err = stage_one(&s, d.path(), 1, entry(url, &good))
        .await
        .unwrap_err();
    assert!(
        err.message().contains("NotFound"),
        "missing-object error lost its cause: {err:?}"
    );

    // A malformed URL is rejected.
    let d = tempfile::tempdir().unwrap();
    let s = stager(d.path(), anon(&content), None);
    assert!(
        stage_one(&s, d.path(), 1, entry("gs://bucket/%zz", &good))
            .await
            .is_err(),
        "accepted a malformed URL"
    );

    // A network error keeps its context.
    let d = tempfile::tempdir().unwrap();
    let s = stager(
        d.path(),
        Arc::new(FakeAnon {
            err: Some("connection refused".into()),
            ..Default::default()
        }),
        None,
    );
    let err = stage_one(&s, d.path(), 1, entry(url, &good))
        .await
        .unwrap_err();
    assert!(
        err.message().contains("while fetching"),
        "lost its context: {err:?}"
    );

    // An asset already staged is kept without a fetch.
    let d = tempfile::tempdir().unwrap();
    let a = anon(&content);
    let s = stager(d.path(), a.clone(), None);
    let dir = d.path().join("consumer/1");
    std::fs::create_dir_all(&dir).unwrap();
    let path = stage_into(&s, &dir, entry(url, &good)).await.unwrap();
    let again = stage_into(&s, &dir, entry(url, &good)).await.unwrap();
    assert_eq!(again, path);
    assert_eq!(a.gets.load(Ordering::SeqCst), 1, "fetched again");
}

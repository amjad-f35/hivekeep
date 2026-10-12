//! Tests of the node, asset and control plugins, over a real Unix socket
//! wherever the sidecar is what is exercised.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Status};

use super::credentials::Credentials;
use super::node::{serve, MANIFEST_FILE};
use super::{AssetPlugin, ControlPlugin, NodePlugin};
use crate::error::{Error, Result, ServiceDetail, ServiceResponse};
use crate::pluginsocket::{wait_ready, PluginChannel};
use crate::proto::v1::asset_provider_client::AssetProviderClient;
use crate::proto::v1::control_provider_client::ControlProviderClient;
use crate::proto::v1::node_provider_client::NodeProviderClient;
use crate::proto::v1::*;
use crate::storage::{BytesReader, ObjectReader, ObjectStorage, UploadBody, DEFAULT_UPLOAD_MEMORY};
use crate::testing::MemObjects;

const BUCKET: &str = "bucket";
const PREFIX: &str = "root/hives/team-a/actors/uid1/snapshots/snap1";

fn test_uri() -> String {
    format!("gs://{BUCKET}/{PREFIX}")
}

/// A short socket path; Unix socket paths are length-limited.
fn sock_dir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("snapplug")
        .tempdir_in("/tmp")
        .unwrap()
}

struct Served {
    _sock: tempfile::TempDir,
    ch: PluginChannel,
    _h: tokio::task::JoinHandle<()>,
}

async fn serve_plugins(backend: Arc<MemObjects>, root: &Path) -> Served {
    let sock = sock_dir();
    let node = NodePlugin::new(backend.clone(), root, DEFAULT_UPLOAD_MEMORY).unwrap();
    let (ch, h) = serve::spawn(
        &sock.path().join("plugin.sock"),
        Some(node),
        Some(ControlPlugin::new(backend)),
        None,
    )
    .await
    .unwrap();
    Served {
        _sock: sock,
        ch,
        _h: h,
    }
}

#[tokio::test]
async fn wait_ready_against_a_serving_plugin() {
    let root = tempfile::tempdir().unwrap();
    let s = serve_plugins(Arc::new(MemObjects::new()), root.path()).await;
    tokio::time::timeout(Duration::from_secs(10), wait_ready(&s.ch))
        .await
        .unwrap()
        .unwrap();
}

fn write(dir: &Path, name: &str, content: &[u8]) {
    std::fs::write(dir.join(name), content).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_fetch_round_trip() {
    let backend = Arc::new(MemObjects::new());
    let root = tempfile::tempdir().unwrap();
    let s = serve_plugins(backend.clone(), root.path()).await;
    let mut client = NodeProviderClient::new(s.ch.clone());
    let src = root.path().join("src");
    let dst = root.path().join("dst");
    std::fs::create_dir_all(src.join("fs")).unwrap();
    std::fs::create_dir(&dst).unwrap();
    let files: Vec<(&str, Vec<u8>)> = vec![
        ("memory.img", b"abcdefgh".repeat(1 << 16)),
        ("state.bin", b"vm state".to_vec()),
        ("fs/fscheckpoint.pb", b"fs manifest".to_vec()),
        (
            MANIFEST_FILE,
            br#"{"snapshotFiles":["memory.img","state.bin","fs/fscheckpoint.pb"]}"#.to_vec(),
        ),
    ];
    for (n, c) in &files {
        write(&src, n, c);
    }
    client
        .upload_snapshot(UploadSnapshotRequest {
            snapshot_uri: test_uri(),
            local_path: src.display().to_string(),
            files: vec![
                "memory.img".into(),
                "state.bin".into(),
                "fs/fscheckpoint.pb".into(),
            ],
            ..Default::default()
        })
        .await
        .unwrap();
    client
        .upload_snapshot(UploadSnapshotRequest {
            snapshot_uri: test_uri(),
            local_path: src.display().to_string(),
            files: vec![MANIFEST_FILE.into()],
            ..Default::default()
        })
        .await
        .unwrap();
    // The stored layout.
    let want: Vec<String> = [
        "fs/fscheckpoint.pb.zstd",
        "manifest.json",
        "memory.img.zstd",
        "state.bin.zstd",
    ]
    .iter()
    .map(|n| format!("{BUCKET}/{PREFIX}/{n}"))
    .collect();
    assert_eq!(backend.keys(), want);
    assert_eq!(
        &backend
            .get(BUCKET, &format!("{PREFIX}/manifest.json"))
            .unwrap()[..],
        &files[3].1[..],
        "manifest stored uncompressed"
    );
    client
        .fetch_snapshot(FetchSnapshotRequest {
            snapshot_uri: test_uri(),
            write_path: dst.display().to_string(),
            files: vec![
                MANIFEST_FILE.into(),
                "memory.img".into(),
                "state.bin".into(),
                "fs/fscheckpoint.pb".into(),
            ],
            ..Default::default()
        })
        .await
        .unwrap();
    for (n, c) in &files {
        assert_eq!(
            &std::fs::read(dst.join(n)).unwrap(),
            c,
            "fetched {n} differs"
        );
    }
}

#[tokio::test]
async fn fetch_missing_is_not_found() {
    let root = tempfile::tempdir().unwrap();
    let s = serve_plugins(Arc::new(MemObjects::new()), root.path()).await;
    let mut client = NodeProviderClient::new(s.ch.clone());
    for file in [MANIFEST_FILE, "memory.img"] {
        let e = client
            .fetch_snapshot(FetchSnapshotRequest {
                snapshot_uri: test_uri(),
                write_path: root.path().display().to_string(),
                files: vec![file.into()],
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::NotFound, "{file}: {e:?}");
    }
}

#[tokio::test]
async fn node_request_validation() {
    let root = tempfile::tempdir().unwrap();
    let r = root.path().display().to_string();
    let s = serve_plugins(Arc::new(MemObjects::new()), root.path()).await;
    let mut client = NodeProviderClient::new(s.ch.clone());
    let parent = root.path().parent().unwrap().display().to_string();
    let cases: Vec<(&str, String, String, Vec<&str>)> = vec![
        (
            "bad uri",
            "gs://bucket/not-a-snapshot".into(),
            r.clone(),
            vec!["a"],
        ),
        ("relative dir", test_uri(), "relative/dir".into(), vec!["a"]),
        ("dir outside root", test_uri(), parent, vec!["a"]),
        (
            "dir escapes root",
            test_uri(),
            format!("{r}/../elsewhere"),
            vec!["a"],
        ),
        (
            "sibling sharing root's prefix",
            test_uri(),
            format!("{r}-other"),
            vec!["a"],
        ),
        ("no files", test_uri(), r.clone(), vec![]),
        ("empty file", test_uri(), r.clone(), vec![""]),
        ("nested file", test_uri(), r.clone(), vec!["sub/a"]),
        ("parent file", test_uri(), r.clone(), vec![".."]),
        ("duplicate file", test_uri(), r.clone(), vec!["a", "b", "a"]),
    ];
    for (name, uri, dir, files) in cases {
        let files: Vec<String> = files.into_iter().map(String::from).collect();
        let f = client
            .fetch_snapshot(FetchSnapshotRequest {
                snapshot_uri: uri.clone(),
                write_path: dir.clone(),
                files: files.clone(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        let u = client
            .upload_snapshot(UploadSnapshotRequest {
                snapshot_uri: uri,
                local_path: dir,
                files,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(f.code(), Code::InvalidArgument, "{name}: fetch {f:?}");
        assert_eq!(u.code(), Code::InvalidArgument, "{name}: upload {u:?}");
    }
}

#[tokio::test]
async fn upload_rejects_symlink_outside_dir() {
    for file in [MANIFEST_FILE, "checkpoint.img"] {
        let parent = tempfile::tempdir().unwrap();
        let backend = Arc::new(MemObjects::new());
        let s = serve_plugins(backend.clone(), parent.path()).await;
        let mut client = NodeProviderClient::new(s.ch.clone());
        let dir = parent.path().join("checkpoint-state");
        std::fs::create_dir(&dir).unwrap();
        let outside = parent.path().join("outside");
        std::fs::write(&outside, "secret").unwrap();
        std::os::unix::fs::symlink(&outside, dir.join(file)).unwrap();
        let r = client
            .upload_snapshot(UploadSnapshotRequest {
                snapshot_uri: test_uri(),
                local_path: dir.display().to_string(),
                files: vec![file.into()],
                ..Default::default()
            })
            .await;
        assert!(
            r.is_err(),
            "{file}: followed a symlink outside the snapshot directory"
        );
        assert!(
            backend.keys().is_empty(),
            "{file}: uploaded {:?}",
            backend.keys()
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_rejects_symlink_outside_dir() {
    for file in [MANIFEST_FILE, "checkpoint.img"] {
        let parent = tempfile::tempdir().unwrap();
        let backend = Arc::new(MemObjects::new());
        let s = serve_plugins(backend.clone(), parent.path()).await;
        let mut client = NodeProviderClient::new(s.ch.clone());
        let src = parent.path().join("src");
        let restore = parent.path().join("restore-state");
        std::fs::create_dir(&src).unwrap();
        std::fs::create_dir(&restore).unwrap();
        write(&src, file, b"replacement");
        client
            .upload_snapshot(UploadSnapshotRequest {
                snapshot_uri: test_uri(),
                local_path: src.display().to_string(),
                files: vec![file.into()],
                ..Default::default()
            })
            .await
            .unwrap();
        let outside = parent.path().join("outside");
        std::fs::write(&outside, "keep me").unwrap();
        std::os::unix::fs::symlink(&outside, restore.join(file)).unwrap();
        let r = client
            .fetch_snapshot(FetchSnapshotRequest {
                snapshot_uri: test_uri(),
                write_path: restore.display().to_string(),
                files: vec![file.into()],
                ..Default::default()
            })
            .await;
        assert!(
            r.is_err(),
            "{file}: followed a symlink outside the restore directory"
        );
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "keep me");
    }
}

#[tokio::test]
async fn rejects_dir_symlink_outside_root() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("root");
    let outside = parent.path().join("outside");
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("checkpoint.img"), "secret").unwrap();
    let link = root.join("link");
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    let backend = Arc::new(MemObjects::new());
    let s = serve_plugins(backend.clone(), &root).await;
    let mut client = NodeProviderClient::new(s.ch.clone());
    assert!(client
        .upload_snapshot(UploadSnapshotRequest {
            snapshot_uri: test_uri(),
            local_path: link.display().to_string(),
            files: vec!["checkpoint.img".into()],
            ..Default::default()
        })
        .await
        .is_err());
    assert!(backend.keys().is_empty());
    backend.insert(BUCKET, &format!("{PREFIX}/{MANIFEST_FILE}"), &b"m"[..]);
    assert!(client
        .fetch_snapshot(FetchSnapshotRequest {
            snapshot_uri: test_uri(),
            write_path: link.display().to_string(),
            files: vec![MANIFEST_FILE.into()],
            ..Default::default()
        })
        .await
        .is_err());
    assert!(
        !outside.join(MANIFEST_FILE).exists(),
        "created a file outside the root"
    );
}

#[tokio::test]
async fn cleanup_snapshot() {
    let backend = Arc::new(MemObjects::new());
    let root = tempfile::tempdir().unwrap();
    let s = serve_plugins(backend.clone(), root.path()).await;
    let mut client = ControlProviderClient::new(s.ch.clone());
    // uid10 shares a string prefix with uid1; it survives only if cleanup
    // stops at a path-segment boundary.
    let keep = format!("{BUCKET}/root/hives/team-a/actors/uid10/snapshots/snap1/manifest.json");
    backend.insert(BUCKET, &format!("{PREFIX}/manifest.json"), &b"m"[..]);
    backend.insert(BUCKET, &format!("{PREFIX}/memory.img.zstd"), &b"d"[..]);
    backend.insert(
        BUCKET,
        "root/hives/team-a/actors/uid10/snapshots/snap1/manifest.json",
        &b"other"[..],
    );
    let owner = format!("gs://{BUCKET}/root/hives/team-a/actors/uid1");
    for _ in 0..2 {
        client
            .cleanup_snapshot(CleanupSnapshotRequest {
                snapshot_uri: owner.clone(),
            })
            .await
            .unwrap();
        assert_eq!(backend.keys(), vec![keep.clone()]);
    }
    for uri in [
        format!("gs://{BUCKET}"),
        format!("gs://{BUCKET}/"),
        "not a uri?x=1".into(),
    ] {
        let e = client
            .cleanup_snapshot(CleanupSnapshotRequest {
                snapshot_uri: uri.clone(),
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::InvalidArgument, "{uri}");
    }
}

#[tokio::test]
async fn copy_snapshot() {
    let backend = Arc::new(MemObjects::new());
    let root = tempfile::tempdir().unwrap();
    let s = serve_plugins(backend.clone(), root.path()).await;
    let mut client = ControlProviderClient::new(s.ch.clone());
    backend.insert(BUCKET, &format!("{PREFIX}/manifest.json"), &b"m"[..]);
    let tag = format!("gs://{BUCKET}/root/hives/team-a/tags/tag1");
    client
        .copy_snapshot(CopySnapshotRequest {
            src_uri: test_uri(),
            dst_uri: tag.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        &backend
            .get(BUCKET, "root/hives/team-a/tags/tag1/manifest.json")
            .unwrap()[..],
        b"m"
    );
    let empty = format!("gs://{BUCKET}/root/hives/team-a/actors/uid9/snapshots/none");
    let e = client
        .copy_snapshot(CopySnapshotRequest {
            src_uri: empty,
            dst_uri: tag,
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition);
}

/// A store holding non-manifest uploads until released and counting how
/// many run at once.
struct SlowStore {
    inner: MemObjects,
    release: watch::Receiver<bool>,
    active: AtomicI64,
    peak: AtomicI64,
    manifests: AtomicI64,
    fail: AtomicBool,
}

#[async_trait]
impl ObjectStorage for SlowStore {
    async fn get_object(&self, bucket: &str, object: &str) -> Result<ObjectReader> {
        self.inner.get_object(bucket, object).await
    }

    async fn put_object(&self, bucket: &str, object: &str, body: UploadBody) -> Result<()> {
        if object.ends_with(&format!("/{MANIFEST_FILE}")) {
            self.manifests.fetch_add(1, Ordering::SeqCst);
            return self.inner.put_object(bucket, object, body).await;
        }
        let n = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(n, Ordering::SeqCst);
        let mut r = self.release.clone();
        while !*r.borrow() {
            if r.changed().await.is_err() {
                break;
            }
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(Error::other("upload failed"));
        }
        self.inner.put_object(bucket, object, body).await
    }
}

fn slow_store(released: bool) -> (Arc<SlowStore>, watch::Sender<bool>) {
    let (tx, rx) = watch::channel(released);
    (
        Arc::new(SlowStore {
            inner: MemObjects::new(),
            release: rx,
            active: AtomicI64::new(0),
            peak: AtomicI64::new(0),
            manifests: AtomicI64::new(0),
            fail: AtomicBool::new(false),
        }),
        tx,
    )
}

fn snapshot_upload_files(dir: &Path, n: usize) -> Vec<String> {
    std::fs::create_dir_all(dir).unwrap();
    let mut files = vec![MANIFEST_FILE.to_string()];
    files.extend((0..n).map(|i| format!("file{i}")));
    for f in &files {
        write(dir, f, b"snapshot payload");
    }
    files
}

async fn settle(cond: impl Fn() -> bool) {
    for _ in 0..200 {
        if cond() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_budget_shared_across_snapshots() {
    let root = tempfile::tempdir().unwrap();
    const SNAPSHOTS: usize = 10;
    const FILES: usize = 8;
    let reqs: Vec<UploadSnapshotRequest> = (0..SNAPSHOTS)
        .map(|i| {
            let dir = root.path().join(format!("src{i}"));
            UploadSnapshotRequest {
                snapshot_uri: format!("{}{i}", test_uri()),
                local_path: dir.display().to_string(),
                files: snapshot_upload_files(&dir, FILES),
                ..Default::default()
            }
        })
        .collect();
    let (store, gate) = slow_store(false);
    // This backend has no client buffers: eight small-file compression
    // reservations of 8 MiB fit in a 64 MiB budget.
    let node = Arc::new(NodePlugin::new(store.clone(), root.path(), 64 << 20).unwrap());
    let mut tasks = Vec::new();
    for r in reqs.clone() {
        let node = node.clone();
        tasks.push(tokio::spawn(async move {
            node.upload_snapshot(&CancellationToken::new(), &r).await
        }));
    }
    settle(|| store.active.load(Ordering::SeqCst) >= 8).await;
    assert_eq!(
        store.active.load(Ordering::SeqCst),
        8,
        "active uploads across all requests"
    );
    assert_eq!(
        store.manifests.load(Ordering::SeqCst),
        0,
        "committed manifests before data finished"
    );
    gate.send(true).unwrap();
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    assert_eq!(store.peak.load(Ordering::SeqCst), 8);
    assert_eq!(store.inner.keys().len(), SNAPSHOTS * (FILES + 1));
    let dst = root.path().join("restored");
    std::fs::create_dir(&dst).unwrap();
    node.fetch_snapshot(
        &CancellationToken::new(),
        &FetchSnapshotRequest {
            snapshot_uri: reqs[0].snapshot_uri.clone(),
            write_path: dst.display().to_string(),
            files: reqs[0].files.clone(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for name in &reqs[0].files {
        assert_eq!(
            std::fs::read(dst.join(name)).unwrap(),
            b"snapshot payload",
            "{name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_failure_does_not_commit_manifest() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("src");
    let files = snapshot_upload_files(&dir, 3);
    let (store, _gate) = slow_store(true);
    store.fail.store(true, Ordering::SeqCst);
    let node = NodePlugin::new(store.clone(), root.path(), 64 << 20).unwrap();
    let req = UploadSnapshotRequest {
        snapshot_uri: test_uri(),
        local_path: dir.display().to_string(),
        files,
        ..Default::default()
    };
    let e = node
        .upload_snapshot(&CancellationToken::new(), &req)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::Internal, "{e:?}");
    assert_eq!(store.manifests.load(Ordering::SeqCst), 0);
    store.fail.store(false, Ordering::SeqCst);
    node.upload_snapshot(&CancellationToken::new(), &req)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_canceled_waiter_returns_canceled() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("src");
    let files = snapshot_upload_files(&dir, 8);
    let (store, gate) = slow_store(false);
    let node = Arc::new(NodePlugin::new(store.clone(), root.path(), 64 << 20).unwrap());
    let req = UploadSnapshotRequest {
        snapshot_uri: test_uri(),
        local_path: dir.display().to_string(),
        files,
        ..Default::default()
    };
    let (n1, r1) = (node.clone(), req.clone());
    let first =
        tokio::spawn(async move { n1.upload_snapshot(&CancellationToken::new(), &r1).await });
    settle(|| store.active.load(Ordering::SeqCst) >= 8).await;
    let cancel = CancellationToken::new();
    let (n2, r2, c2) = (node.clone(), req.clone(), cancel.clone());
    let waiting = tokio::spawn(async move { n2.upload_snapshot(&c2, &r2).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel.cancel();
    assert_eq!(waiting.await.unwrap().unwrap_err().code(), Code::Cancelled);
    gate.send(true).unwrap();
    first.await.unwrap().unwrap();
}

/// A backend answering 403 to every object request until allowed, the way
/// GCS treats a token that no longer covers the actor's prefix.
struct Refusing {
    backend: Arc<MemObjects>,
    allowed: AtomicBool,
    refusals: AtomicUsize,
}

fn forbidden() -> Error {
    Error::service(
        ServiceResponse {
            status: 403,
            detail: ServiceDetail::GcsJson {
                reason: "forbidden".into(),
            },
        },
        "gcs: HTTP 403",
    )
}

#[async_trait]
impl ObjectStorage for Refusing {
    async fn get_object(&self, bucket: &str, object: &str) -> Result<ObjectReader> {
        if !self.allowed.load(Ordering::SeqCst) {
            self.refusals.fetch_add(1, Ordering::SeqCst);
            return Err(forbidden());
        }
        self.backend.get_object(bucket, object).await
    }

    async fn put_object(&self, bucket: &str, object: &str, body: UploadBody) -> Result<()> {
        if !self.allowed.load(Ordering::SeqCst) {
            // Storage reads the body before it answers, leaving the file at
            // its end.
            let _ = body.read_all().await;
            self.refusals.fetch_add(1, Ordering::SeqCst);
            return Err(forbidden());
        }
        self.backend.put_object(bucket, object, body).await
    }
}

/// Hands out one client per call, recording `force`; the forced client is
/// allowed, the first not, unless `stale` is false.
struct FakeCredentials {
    backend: Arc<MemObjects>,
    stale: bool,
    refuse_all: bool,
    err: Option<Status>,
    forces: parking_lot::Mutex<Vec<bool>>,
}

impl FakeCredentials {
    fn new(backend: Arc<MemObjects>, stale: bool) -> Arc<Self> {
        Arc::new(FakeCredentials {
            backend,
            stale,
            refuse_all: false,
            err: None,
            forces: Default::default(),
        })
    }
    fn calls(&self) -> Vec<bool> {
        self.forces.lock().clone()
    }
}

#[async_trait]
impl Credentials for FakeCredentials {
    async fn client(&self, _: &str, force: bool) -> Result<Arc<dyn ObjectStorage>, Status> {
        self.forces.lock().push(force);
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        Ok(Arc::new(Refusing {
            backend: self.backend.clone(),
            allowed: AtomicBool::new(!self.refuse_all && (force || !self.stale)),
            refusals: AtomicUsize::new(0),
        }))
    }
}

fn creds_plugin(creds: Arc<FakeCredentials>, root: &Path) -> NodePlugin {
    NodePlugin::new(creds.backend.clone(), root, DEFAULT_UPLOAD_MEMORY)
        .unwrap()
        .with_credentials(creds)
}

fn write_files(dir: &Path, names: &[&str]) {
    for n in names {
        write(dir, n, format!("payload {n}").as_bytes());
    }
}

fn fetch_req(dir: &Path, files: &[&str]) -> FetchSnapshotRequest {
    FetchSnapshotRequest {
        snapshot_uri: test_uri(),
        actor_jwt: "jwt".into(),
        write_path: dir.display().to_string(),
        files: files.iter().map(|s| s.to_string()).collect(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transfer_retries_once_with_a_replaced_credential() {
    let root = tempfile::tempdir().unwrap();
    let src = root.path().join("src");
    std::fs::create_dir(&src).unwrap();
    write_files(&src, &["a.img", "b.img", "c.img", MANIFEST_FILE]);
    let backend = Arc::new(MemObjects::new());
    let creds = FakeCredentials::new(backend.clone(), true);
    let p = creds_plugin(creds.clone(), root.path());
    p.upload_snapshot(
        &CancellationToken::new(),
        &UploadSnapshotRequest {
            snapshot_uri: test_uri(),
            actor_jwt: "jwt".into(),
            local_path: src.display().to_string(),
            files: vec![
                "a.img".into(),
                "b.img".into(),
                "c.img".into(),
                MANIFEST_FILE.into(),
            ],
        },
    )
    .await
    .unwrap();
    // Three files refused on the first client force one replacement between
    // them, and the manifest rides the replacement.
    assert_eq!(creds.calls(), vec![false, true]);
    assert_eq!(backend.keys().len(), 4);
    let dst = root.path().join("dst");
    std::fs::create_dir(&dst).unwrap();
    let creds2 = FakeCredentials::new(backend.clone(), true);
    let p2 = creds_plugin(creds2.clone(), root.path());
    p2.fetch_snapshot(
        &CancellationToken::new(),
        &fetch_req(&dst, &["a.img", MANIFEST_FILE]),
    )
    .await
    .unwrap();
    let calls = creds2.calls();
    assert!(
        calls.len() == 2 && calls[1],
        "fetch credential calls = {calls:?}"
    );
}

#[tokio::test]
async fn transfer_retries_the_manifest_from_its_start() {
    let root = tempfile::tempdir().unwrap();
    write_files(root.path(), &[MANIFEST_FILE]);
    let backend = Arc::new(MemObjects::new());
    let p = creds_plugin(FakeCredentials::new(backend.clone(), true), root.path());
    p.upload_snapshot(
        &CancellationToken::new(),
        &UploadSnapshotRequest {
            snapshot_uri: test_uri(),
            actor_jwt: "jwt".into(),
            local_path: root.path().display().to_string(),
            files: vec![MANIFEST_FILE.into()],
        },
    )
    .await
    .unwrap();
    assert_eq!(
        &backend
            .get(BUCKET, &format!("{PREFIX}/{MANIFEST_FILE}"))
            .unwrap()[..],
        format!("payload {MANIFEST_FILE}").as_bytes(),
        "the retry sent what was left of the file"
    );
}

#[tokio::test]
async fn transfer_reports_an_actor_it_cannot_transfer_for() {
    let root = tempfile::tempdir().unwrap();
    for code in [Code::NotFound, Code::Aborted] {
        let creds = Arc::new(FakeCredentials {
            err: Some(Status::new(code, "actor not found")),
            ..Arc::try_unwrap(FakeCredentials::new(Arc::new(MemObjects::new()), false))
                .ok()
                .unwrap()
        });
        let p = creds_plugin(creds, root.path());
        let e = p
            .fetch_snapshot(
                &CancellationToken::new(),
                &fetch_req(root.path(), &[MANIFEST_FILE]),
            )
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::FailedPrecondition, "{code:?}: {e:?}");
    }
}

#[tokio::test]
async fn transfer_fails_closed_on_a_second_refusal() {
    let root = tempfile::tempdir().unwrap();
    let creds = Arc::new(FakeCredentials {
        refuse_all: true,
        ..Arc::try_unwrap(FakeCredentials::new(Arc::new(MemObjects::new()), true))
            .ok()
            .unwrap()
    });
    let p = creds_plugin(creds.clone(), root.path());
    let e = p
        .fetch_snapshot(
            &CancellationToken::new(),
            &fetch_req(root.path(), &[MANIFEST_FILE]),
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied, "{e:?}");
    assert_eq!(creds.calls().len(), 2);
}

#[tokio::test]
async fn transfer_returns_the_credential_sources_status() {
    let root = tempfile::tempdir().unwrap();
    let creds = Arc::new(FakeCredentials {
        err: Some(Status::permission_denied(
            "actor is no longer on the token's node",
        )),
        ..Arc::try_unwrap(FakeCredentials::new(Arc::new(MemObjects::new()), false))
            .ok()
            .unwrap()
    });
    let p = creds_plugin(creds, root.path());
    let e = p
        .fetch_snapshot(
            &CancellationToken::new(),
            &fetch_req(root.path(), &[MANIFEST_FILE]),
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied);
    assert!(
        e.message()
            .contains("actor is no longer on the token's node"),
        "{e:?}"
    );
}

#[tokio::test]
async fn ambient_credentials_ignore_the_jwt() {
    let root = tempfile::tempdir().unwrap();
    let p = NodePlugin::new(
        Arc::new(MemObjects::new()),
        root.path(),
        DEFAULT_UPLOAD_MEMORY,
    )
    .unwrap();
    write_files(root.path(), &[MANIFEST_FILE]);
    for jwt in ["", "anything"] {
        p.upload_snapshot(
            &CancellationToken::new(),
            &UploadSnapshotRequest {
                snapshot_uri: test_uri(),
                actor_jwt: jwt.into(),
                local_path: root.path().display().to_string(),
                files: vec![MANIFEST_FILE.into()],
            },
        )
        .await
        .unwrap_or_else(|e| panic!("actor_jwt={jwt:?}: {e:?}"));
    }
}

/// Counts reads, so a test can tell a request was refused before anything
/// was read; with `report_size` its readers report the object's size.
struct Counting {
    inner: MemObjects,
    gets: AtomicUsize,
    report_size: AtomicBool,
}

#[async_trait]
impl ObjectStorage for Counting {
    async fn get_object(&self, bucket: &str, object: &str) -> Result<ObjectReader> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let data = self
            .inner
            .get(bucket, object)
            .ok_or_else(|| Error::not_found(format!("{bucket}/{object}")))?;
        Ok(if self.report_size.load(Ordering::SeqCst) {
            Box::new(BytesReader::sized(data))
        } else {
            Box::new(BytesReader::new(data))
        })
    }

    async fn put_object(&self, bucket: &str, object: &str, body: UploadBody) -> Result<()> {
        self.inner.put_object(bucket, object, body).await
    }
}

struct AssetFixture {
    client: AssetProviderClient<PluginChannel>,
    backend: Arc<Counting>,
    root: tempfile::TempDir,
    staging: tempfile::TempDir,
    _sock: tempfile::TempDir,
    _h: tokio::task::JoinHandle<()>,
}

const UNTOUCHED: &str = "the caller's original bytes";

async fn asset_fixture(scheme: &str) -> AssetFixture {
    let backend = Arc::new(Counting {
        inner: MemObjects::new(),
        gets: AtomicUsize::new(0),
        report_size: AtomicBool::new(false),
    });
    let (root, staging) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let plugin = AssetPlugin::new(
        backend.clone(),
        scheme,
        root.path(),
        staging.path(),
        1 << 30,
    )
    .unwrap();
    let sock = sock_dir();
    let (ch, h) = serve::spawn(&sock.path().join("plugin.sock"), None, None, Some(plugin))
        .await
        .unwrap();
    AssetFixture {
        client: AssetProviderClient::new(ch),
        backend,
        root,
        staging,
        _sock: sock,
        _h: h,
    }
}

impl AssetFixture {
    fn put(&self, object: &str, content: &[u8]) -> String {
        self.backend.inner.insert(BUCKET, object, content.to_vec());
        hex::encode(Sha256::digest(content))
    }

    fn write_file(&self, content: &str) -> PathBuf {
        let p = self
            .root
            .path()
            .join("static-files")
            .join("asset-download-1");
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, content).unwrap();
        p
    }

    fn assert_staging_empty(&self) {
        assert_eq!(
            std::fs::read_dir(self.staging.path()).unwrap().count(),
            0,
            "staging holds entries after the call"
        );
    }

    async fn fetch(&mut self, uri: &str, sum: &str, dst: &Path, max: i64) -> Result<(), Status> {
        self.client
            .fetch_asset(FetchAssetRequest {
                asset_uri: uri.into(),
                sha256: sum.into(),
                write_path: dst.display().to_string(),
                max_bytes: max,
            })
            .await
            .map(|_| ())
    }
}

fn assert_content(p: &Path, want: &str) {
    assert_eq!(std::fs::read_to_string(p).unwrap(), want, "{}", p.display());
}

#[tokio::test]
async fn fetch_asset() {
    let content = b"micro-vm kernel bytes";
    // Happy path.
    for scheme in ["gs", "s3"] {
        let mut f = asset_fixture(scheme).await;
        let sum = f.put("kata-assets/vmlinux", content);
        let dst = f.write_file(UNTOUCHED);
        f.fetch(
            &format!("{scheme}://{BUCKET}/kata-assets/vmlinux"),
            &sum,
            &dst,
            content.len() as i64,
        )
        .await
        .unwrap_or_else(|e| panic!("{scheme}: {e:?}"));
        assert_content(&dst, std::str::from_utf8(content).unwrap());
        f.assert_staging_empty();
    }
    let uri = format!("gs://{BUCKET}/kata-assets/vmlinux");
    // Digest mismatch leaves write_path untouched.
    {
        let mut f = asset_fixture("gs").await;
        f.put("kata-assets/vmlinux", content);
        let dst = f.write_file(UNTOUCHED);
        let e = f
            .fetch(&uri, &"a".repeat(64), &dst, 1 << 20)
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::FailedPrecondition);
        assert_content(&dst, UNTOUCHED);
        f.assert_staging_empty();
    }
    // max_bytes is enforced.
    {
        let mut f = asset_fixture("gs").await;
        let sum = f.put("kata-assets/vmlinux", content);
        let dst = f.write_file(UNTOUCHED);
        let e = f
            .fetch(&uri, &sum, &dst, content.len() as i64 - 1)
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::FailedPrecondition);
        assert_content(&dst, UNTOUCHED);
        f.assert_staging_empty();
    }
    // Another scheme is refused before reading.
    for (plugin, asset) in [("s3", "gs"), ("gs", "s3")] {
        let mut f = asset_fixture(plugin).await;
        let sum = f.put("kata-assets/vmlinux", content);
        let dst = f.write_file(UNTOUCHED);
        let e = f
            .fetch(
                &format!("{asset}://{BUCKET}/kata-assets/vmlinux"),
                &sum,
                &dst,
                1 << 20,
            )
            .await
            .unwrap_err();
        assert_eq!(
            e.code(),
            Code::InvalidArgument,
            "{plugin} plugin, {asset} asset"
        );
        assert_content(&dst, UNTOUCHED);
        assert_eq!(f.backend.gets.load(Ordering::SeqCst), 0);
    }
    // A reported size over max_bytes fails before staging.
    {
        let mut f = asset_fixture("gs").await;
        f.backend.report_size.store(true, Ordering::SeqCst);
        let sum = f.put("kata-assets/vmlinux", content);
        let dst = f.write_file(UNTOUCHED);
        let e = f
            .fetch(&uri, &sum, &dst, content.len() as i64 - 1)
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::FailedPrecondition);
        assert!(
            e.message().contains(&format!("is {} bytes", content.len())),
            "{e:?}"
        );
        assert_content(&dst, UNTOUCHED);
        f.assert_staging_empty();
    }
    // Not found.
    {
        let mut f = asset_fixture("gs").await;
        let dst = f.write_file(UNTOUCHED);
        let e = f
            .fetch(
                &format!("gs://{BUCKET}/kata-assets/missing"),
                &"a".repeat(64),
                &dst,
                1 << 20,
            )
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::NotFound);
        assert_content(&dst, UNTOUCHED);
        f.assert_staging_empty();
    }
    // write_path must be an existing regular file below the root.
    {
        let mut f = asset_fixture("gs").await;
        let sum = f.put("kata-assets/vmlinux", content);
        let other = tempfile::tempdir().unwrap();
        let outside = other.path().join("outside");
        std::fs::write(&outside, UNTOUCHED).unwrap();
        let dir = f.root.path().join("dir");
        std::fs::create_dir(&dir).unwrap();
        let link = f.root.path().join("link");
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        let escapes = f.root.path().join("..").join("outside");
        let missing = f.root.path().join("missing");
        for (name, path, want) in [
            ("outside root", outside.clone(), Code::InvalidArgument),
            ("escapes root", escapes, Code::InvalidArgument),
            (
                "relative",
                PathBuf::from("static-files/x"),
                Code::InvalidArgument,
            ),
            ("missing", missing.clone(), Code::FailedPrecondition),
            ("directory", dir, Code::FailedPrecondition),
            (
                "symlink to a file outside root",
                link,
                Code::FailedPrecondition,
            ),
        ] {
            let e = f.fetch(&uri, &sum, &path, 1 << 20).await.unwrap_err();
            assert_eq!(e.code(), want, "{name}: {e:?}");
        }
        assert_content(&outside, UNTOUCHED);
        assert!(!missing.exists(), "created the missing write path");
        assert_eq!(f.backend.gets.load(Ordering::SeqCst), 0);
    }
    // Invalid requests.
    {
        let mut f = asset_fixture("gs").await;
        let sum = f.put("kata-assets/vmlinux", content);
        let dst = f.write_file(UNTOUCHED);
        let long = format!(
            "gs://{BUCKET}/{}vmlinux",
            "x/".repeat(crate::assetfetch::MAX_URI_BYTES / 2)
        );
        let cases: Vec<(&str, String, String, i64)> = vec![
            (
                "https scheme",
                format!("https://storage.example.com/{BUCKET}/kata-assets/vmlinux"),
                sum.clone(),
                1 << 20,
            ),
            ("no object", format!("gs://{BUCKET}"), sum.clone(), 1 << 20),
            (
                "no bucket",
                "gs:///kata-assets/vmlinux".into(),
                sum.clone(),
                1 << 20,
            ),
            (
                "query",
                format!("gs://{BUCKET}/kata-assets/vmlinux?sig=secret"),
                sum.clone(),
                1 << 20,
            ),
            (
                "user info",
                format!("gs://usr0:secret@{BUCKET}/kata-assets/vmlinux"),
                sum.clone(),
                1 << 20,
            ),
            (
                "fragment",
                format!("gs://{BUCKET}/kata-assets/vmlinux#x"),
                sum.clone(),
                1 << 20,
            ),
            (
                "dot segment",
                format!("gs://{BUCKET}/kata-assets/../kata-assets/vmlinux"),
                sum.clone(),
                1 << 20,
            ),
            (
                "empty segment",
                format!("gs://{BUCKET}/kata-assets//vmlinux"),
                sum.clone(),
                1 << 20,
            ),
            (
                "trailing slash",
                format!("gs://{BUCKET}/kata-assets/"),
                sum.clone(),
                1 << 20,
            ),
            (
                "padded segment",
                format!("gs://{BUCKET}/kata-assets /vmlinux"),
                sum.clone(),
                1 << 20,
            ),
            (
                "backslash",
                format!("gs://{BUCKET}/kata-assets\\vmlinux"),
                sum.clone(),
                1 << 20,
            ),
            (
                "upper-case sha256",
                uri.clone(),
                sum.to_uppercase(),
                1 << 20,
            ),
            ("short sha256", uri.clone(), sum[..63].to_string(), 1 << 20),
            ("zero max_bytes", uri.clone(), sum.clone(), 0),
            ("over-long URI", long, sum.clone(), 1 << 20),
        ];
        for (name, u, s, max) in cases {
            let e = f.fetch(&u, &s, &dst, max).await.unwrap_err();
            assert_eq!(e.code(), Code::InvalidArgument, "{name}: {e:?}");
            for secret in ["secret", "usr0"] {
                assert!(
                    !e.message().contains(secret),
                    "{name}: {e:?} repeats credentials"
                );
            }
        }
        assert_content(&dst, UNTOUCHED);
        assert_eq!(f.backend.gets.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn fetch_asset_refuses_snapshots() {
    for scheme in ["gs", "s3"] {
        let mut f = asset_fixture(scheme).await;
        let objects = [
            format!("{PREFIX}/manifest.json"),
            format!("{PREFIX}/checkpoint.img.zstd"),
            "root/hives/team-a/tags/tag1/manifest.json".into(),
            "root/hives/team-a/tags/tag1/checkpoint.img.zstd".into(),
            "hives/team-a/actors/uid1/snapshots/snap1/manifest.json".into(),
            PREFIX.to_string(),
            "root/hives/team-a/tags/tag1".into(),
        ];
        let sums: Vec<String> = objects
            .iter()
            .map(|o| f.put(o, format!("snapshot bytes of {o}").as_bytes()))
            .collect();
        let asset_sum = f.put("kata-assets/vmlinux", b"kernel");
        let dst = f.write_file(UNTOUCHED);
        for (o, s) in objects.iter().zip(&sums) {
            let e = f
                .fetch(&format!("{scheme}://{BUCKET}/{o}"), s, &dst, 1 << 20)
                .await
                .unwrap_err();
            assert_eq!(e.code(), Code::PermissionDenied, "{scheme}://{o}");
        }
        assert_content(&dst, UNTOUCHED);
        assert_eq!(f.backend.gets.load(Ordering::SeqCst), 0);
        for o in [
            "kata-assets/vmlinux",
            "hives/readme",
            "root/hives/team-a/actors/uid1",
        ] {
            let s = if o == "kata-assets/vmlinux" {
                asset_sum.clone()
            } else {
                f.put(o, format!("asset {o}").as_bytes())
            };
            f.fetch(&format!("{scheme}://{BUCKET}/{o}"), &s, &dst, 1 << 20)
                .await
                .unwrap_or_else(|e| panic!("{o}: {e:?}"));
        }
    }
}

#[tokio::test]
async fn fetch_asset_many_segment_uris() {
    let mut f = asset_fixture("s3").await;
    let dst = f.write_file(UNTOUCHED);
    let max = crate::assetfetch::MAX_URI_BYTES;
    let pad = |unit: &str, suffix: &str| -> String {
        let head = format!("s3://{BUCKET}/");
        let avail = max - head.len() - suffix.len();
        let n = (avail - 1) / unit.len();
        let uri = format!(
            "{head}{}{}{suffix}",
            unit.repeat(n),
            "y".repeat(avail - n * unit.len())
        );
        assert_eq!(uri.len(), max);
        uri
    };
    let served = pad("x/", "/vmlinux");
    let object = served
        .trim_start_matches(&format!("s3://{BUCKET}/"))
        .to_string();
    let sum = f.put(&object, b"kernel");
    f.fetch(&served, &sum, &dst, 1 << 20).await.unwrap();
    assert_content(&dst, "kernel");
    let gets = f.backend.gets.load(Ordering::SeqCst);
    for (name, uri, want) in [
        (
            "snapshot after many owner-like segments",
            pad(
                "hives/team-a/",
                "/hives/team-a/actors/uid1/snapshots/snap1/manifest.json",
            ),
            Code::PermissionDenied,
        ),
        (
            "tag after many owner-like segments",
            pad(
                "hives/team-a/actors/",
                "/hives/team-a/tags/tag1/manifest.json",
            ),
            Code::PermissionDenied,
        ),
        (
            "one byte over the limit",
            format!("{served}x"),
            Code::InvalidArgument,
        ),
    ] {
        let e = f.fetch(&uri, &sum, &dst, 1 << 20).await.unwrap_err();
        assert_eq!(e.code(), want, "{name}");
    }
    assert_eq!(
        f.backend.gets.load(Ordering::SeqCst),
        gets,
        "refused requests read objects"
    );
}

#[test]
fn new_asset_plugin_rejects_staging_below_root() {
    let root = tempfile::tempdir().unwrap();
    let mem: Arc<dyn ObjectStorage> = Arc::new(MemObjects::new());
    let staging = root.path().join("staging");
    std::fs::create_dir(&staging).unwrap();
    for s in [root.path().to_path_buf(), staging] {
        assert!(AssetPlugin::new(mem.clone(), "gs", root.path(), &s, 1 << 30).is_err());
    }
    let other = tempfile::tempdir().unwrap();
    assert!(AssetPlugin::new(
        mem.clone(),
        "gs",
        Path::new("relative"),
        other.path(),
        1 << 30
    )
    .is_err());
    assert!(AssetPlugin::new(
        mem.clone(),
        "gs",
        root.path(),
        Path::new("relative"),
        1 << 30
    )
    .is_err());
    assert!(AssetPlugin::new(mem, "gs", root.path(), other.path(), 1 << 30).is_ok());
}

#[test]
fn new_asset_plugin_rejects_unknown_scheme() {
    for scheme in ["", "https", "GS"] {
        let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        assert!(
            AssetPlugin::new(
                Arc::new(MemObjects::new()),
                scheme,
                a.path(),
                b.path(),
                1 << 30
            )
            .is_err(),
            "{scheme:?}"
        );
    }
}

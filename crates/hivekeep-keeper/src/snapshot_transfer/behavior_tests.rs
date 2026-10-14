//! Snapshot transfers: commit order, empty lists, the codes and messages
//! of store and mint failures.

use std::sync::atomic::{AtomicUsize, Ordering};

use hivekeep_proto::keeper::v1 as pb;
use parking_lot::Mutex;

use super::*;
use crate::objectstore::{FetchAsset, LocalObjectStore};
use crate::sandbox_test_support::Logs;
use crate::snapshot_cache::{CacheMode, SnapshotDownloader};
use crate::worker_jwt::tests::FakeWorkers;

const URI: &str =
    "gs://bucket/root/hives/hk-demo/actors/3d7f1b62-8a04-4c19-b5e6-2f9c7a1d0e38/snapshots/snap-1";

fn actor() -> TransferActor {
    TransferActor {
        hive: "hk-demo".into(),
        name: "counter".into(),
        uid: "3d7f1b62-8a04-4c19-b5e6-2f9c7a1d0e38".into(),
    }
}

/// A local store recording each uploaded file in order, or failing every
/// call with `err`.
struct Recording {
    inner: LocalObjectStore,
    puts: Mutex<Vec<String>>,
    calls: AtomicUsize,
    err: Option<Status>,
}

impl Recording {
    fn new(root: &Path, err: Option<Status>) -> Arc<Recording> {
        Arc::new(Recording {
            inner: LocalObjectStore::new(root),
            puts: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            err,
        })
    }
}

#[async_trait]
impl ObjectStore for Recording {
    async fn fetch_snapshot(&self, ctx: &Ctx, req: FetchSnapshot) -> Result<(), Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        self.inner.fetch_snapshot(ctx, req).await
    }
    async fn upload_snapshot(&self, ctx: &Ctx, req: UploadSnapshot) -> Result<(), Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        self.puts.lock().extend(req.files.iter().cloned());
        self.inner.upload_snapshot(ctx, req).await
    }
    async fn fetch_asset(&self, ctx: &Ctx, req: FetchAsset) -> Result<(), Status> {
        self.inner.fetch_asset(ctx, req).await
    }
}

struct Env {
    d: tempfile::TempDir,
    store: Arc<Recording>,
    workers: Arc<FakeWorkers>,
    transfer: Arc<SnapshotTransfer>,
}

fn env(err: Option<Status>, mint_err: Option<Status>) -> Env {
    let d = tempfile::tempdir().unwrap();
    let store = Recording::new(&d.path().join("objects"), err);
    let workers = Arc::new(FakeWorkers::default());
    *workers.mint_err.lock() = mint_err;
    let jwts = Arc::new(WorkerActorJwts::new(
        workers.clone(),
        Arc::new(crate::clock::SystemClock),
    ));
    let paths = Arc::new(NodePaths::new(d.path().join("node")));
    let transfer = Arc::new(SnapshotTransfer::new(
        store.clone(),
        jwts,
        paths,
        "boot-1".into(),
    ));
    Env {
        d,
        store,
        workers,
        transfer,
    }
}

impl Env {
    fn dir(&self, name: &str) -> PathBuf {
        let p = self.d.path().join(name);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn downloader(&self) -> SnapshotDownloader {
        let cache = crate::filecache::Store::open(
            self.d.path().join("cache"),
            crate::filecache::Options::default(),
        )
        .unwrap();
        SnapshotDownloader::new(self.transfer.clone(), Some(cache))
    }
}

fn memory_rec(files: &[&str]) -> SandboxAssetsRecord {
    SandboxAssetsRecord {
        sandbox_class: "gvisor".into(),
        snapshot_files: files.iter().map(|s| s.to_string()).collect(),
        fidelity: "memory".into(),
        boot_id: "boot-1".into(),
        ..Default::default()
    }
}

fn promote_snapshot() -> pb::Snapshot {
    pb::Snapshot {
        snapshot_uuid: "snap-1".into(),
        generation: 1,
        fidelity: pb::SnapshotFidelity::Memory as i32,
        object: Some(pb::ObjectStorage {
            snapshot_uri: URI.into(),
            sharing: 0,
        }),
    }
}

/// A local snapshot of `rec` in `dir`, every file holding its name.
fn write_local(dir: &Path, rec: &SandboxAssetsRecord) {
    for f in &rec.snapshot_files {
        std::fs::write(dir.join(f), f).unwrap();
    }
    std::fs::write(dir.join(MANIFEST_NAME), rec.to_json()).unwrap();
}

#[tokio::test]
async fn upload_snapshot_manifest_last() {
    let e = env(None, None);
    let uri = SnapshotUri::parse(URI).unwrap();
    let dir = e.dir("src");
    let rec = memory_rec(&["a", "b", "c"]);
    for f in &rec.snapshot_files {
        std::fs::write(dir.join(f), f).unwrap();
    }
    e.transfer
        .upload_snapshot(&Ctx::background(), &actor(), &uri, &dir, &rec)
        .await
        .unwrap();
    let puts = e.store.puts.lock().clone();
    assert_eq!(puts.len(), 4, "{puts:?}: want 3 files then the manifest");
    for (i, p) in puts.iter().enumerate() {
        assert_eq!(
            p == MANIFEST_NAME,
            i == 3,
            "{puts:?}: want the manifest last and only last"
        );
    }
    let got = e
        .transfer
        .fetch_manifest(&Ctx::background(), &actor(), URI)
        .await
        .unwrap();
    assert_eq!(
        SandboxAssetsRecord::parse(&got).unwrap().snapshot_files,
        rec.snapshot_files
    );
    let scratch = e.d.path().join("node").join("snapshot-scratch");
    assert_eq!(
        std::fs::read_dir(scratch).unwrap().count(),
        0,
        "scratch dir not cleaned up"
    );
}

#[tokio::test]
async fn transfers_skip_empty_file_lists() {
    let e = env(None, None);
    let ctx = Ctx::background();
    e.downloader()
        .download(&ctx, &actor(), URI, &e.dir("dst"), &[], CacheMode::Off)
        .await
        .unwrap();
    e.transfer
        .upload_snapshot_files(&ctx, &actor(), URI, &e.dir("src"), &[])
        .await
        .unwrap();
    assert_eq!(
        e.store.calls.load(Ordering::SeqCst),
        0,
        "the store was called for no files"
    );
    assert_eq!(
        e.workers.mints.load(Ordering::SeqCst),
        0,
        "a JWT was minted for no files"
    );
}

/// The transfers a failing store or mint can break, by name.
const TRANSFERS: [&str; 5] = [
    "download",
    "cached download, link",
    "cached download, copy",
    "paused upload",
    "checkpoint upload",
];

async fn run_transfer(e: &Env, name: &str, uri: &str) -> Result<(), Status> {
    let ctx = Ctx::background();
    let a = actor();
    let one = vec!["a".to_string()];
    match name {
        "download" => {
            e.downloader()
                .download(&ctx, &a, uri, &e.dir("dst-direct"), &one, CacheMode::Off)
                .await
        }
        "cached download, link" => {
            e.downloader()
                .download(&ctx, &a, uri, &e.dir("dst-link"), &one, CacheMode::Link)
                .await
        }
        "cached download, copy" => {
            e.downloader()
                .download(&ctx, &a, uri, &e.dir("dst-copy"), &one, CacheMode::Copy)
                .await
        }
        "upload" => {
            e.transfer
                .upload_snapshot_files(&ctx, &a, uri, &e.dir("src-files"), &one)
                .await
        }
        "manifest fetch" => e.transfer.fetch_manifest(&ctx, &a, uri).await.map(|_| ()),
        "paused upload" => {
            let dir = e.dir("paused");
            write_local(&dir, &memory_rec(&["a"]));
            let u = SnapshotUri::parse(URI).unwrap();
            e.transfer
                .upload_local_checkpoint_dir(&ctx, &a, &promote_snapshot(), &dir, &u)
                .await
                .map(|_| ())
        }
        "checkpoint upload" => {
            let dir = e.dir("checkpoint");
            std::fs::write(dir.join("a"), b"a").unwrap();
            let u = SnapshotUri::parse(URI).unwrap();
            e.transfer
                .upload_snapshot(&ctx, &a, &u, &dir, &memory_rec(&["a"]))
                .await
        }
        other => panic!("unknown transfer {other}"),
    }
}

/// A store's code reaches the caller unchanged: Unavailable stays
/// retryable, and nothing else turns into it.
#[tokio::test]
async fn store_error_code_reaches_the_caller() {
    for code in [Code::Unavailable, Code::Internal, Code::NotFound] {
        for name in TRANSFERS {
            let e = env(Some(Status::new(code, "plugin failed")), None);
            let err = run_transfer(&e, name, URI).await.unwrap_err();
            assert_eq!(err.code(), code, "{code:?}/{name}: {err:?}");
        }
    }
}

/// A mint failure is mapped like a store failure: a control plane that
/// cannot be reached stays retryable, anything else does not.
#[tokio::test]
async fn mint_error_code_reaches_the_caller() {
    for mint in [
        Code::Unavailable,
        Code::NotFound,
        Code::Aborted,
        Code::Internal,
    ] {
        for name in ["download", "paused upload", "checkpoint upload"] {
            let e = env(
                Some(Status::internal("the store must not be reached")),
                Some(Status::new(mint, "mint failed")),
            );
            let err = run_transfer(&e, name, URI).await.unwrap_err();
            assert_eq!(
                e.store.calls.load(Ordering::SeqCst),
                0,
                "{mint:?}/{name}: the store was reached"
            );
            assert_eq!(
                err.code() == Code::Unavailable,
                mint == Code::Unavailable,
                "{mint:?}/{name}: {err:?}"
            );
        }
    }
}

#[tokio::test]
async fn store_error_keeps_credentials_out() {
    const SECRET: &str = "SECRET-SIG";
    let signed = format!("s3://bucket/snapshots/a?sig={SECRET}");
    let shown = "s3://bucket/snapshots/a";
    let cases: [(&str, bool); 6] = [
        ("download", true),
        ("cached download, link", true),
        ("cached download, copy", true),
        ("upload", true),
        ("manifest fetch", true),
        ("paused upload", false),
    ];
    for code in [Code::Unavailable, Code::Internal, Code::NotFound] {
        for (name, names_uri) in cases {
            let logs = Logs::capture();
            let e = env(
                Some(Status::new(
                    code,
                    format!("get {signed}: AccountKey={SECRET}"),
                )),
                None,
            );
            let err = run_transfer(&e, name, &signed).await.unwrap_err();
            tracing::info!(code = ?err.code(), err = %err.message(), "RPC failed");
            for (what, text) in [("error", err.message().to_string()), ("logs", logs.text())] {
                for leak in [SECRET, "sig=", "AccountKey"] {
                    assert!(
                        !text.contains(leak),
                        "{code:?}/{name}: {what} carries {leak:?}:\n{text}"
                    );
                }
            }
            if names_uri {
                assert!(
                    err.message().contains(shown),
                    "{code:?}/{name}: {err:?} does not name {shown}"
                );
            }
        }
    }
}

#[test]
fn store_call_error_keeps_the_code() {
    for code in [
        Code::Unavailable,
        Code::NotFound,
        Code::Internal,
        Code::Cancelled,
        Code::DeadlineExceeded,
    ] {
        let err = store_call_error("fetch", "s3://b/o?sig=x", Status::new(code, "secret"));
        assert_eq!(err.code(), code);
        assert!(
            !err.message().contains("secret") && !err.message().contains("sig="),
            "{err:?}"
        );
    }
}

#[tokio::test]
async fn fetch_manifest_keeps_not_found() {
    let e = env(Some(Status::not_found("no manifest")), None);
    let err = e
        .transfer
        .fetch_manifest(&Ctx::background(), &actor(), URI)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err:?}");
}

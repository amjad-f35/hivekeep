//! Test fakes for staging sandbox assets: an in-memory bucket read
//! anonymously, an object store serving assets the way a storage plugin
//! does, and a log recorder.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tonic::Status;

use crate::assets::{AssetEntry, SandboxAssetsRecord};
use crate::context::Ctx;
use crate::filecache;
use crate::objectstore::{FetchAsset, FetchSnapshot, ObjectStore, UploadSnapshot};
use crate::sandbox_assets::{AnonymousFetcher, AssetStager};

/// The hex sha256 of `b`.
pub(crate) fn digest(b: &[u8]) -> String {
    hex::encode(Sha256::digest(b))
}

/// An in-memory bucket keyed `bucket/object`, counting reads.
#[derive(Default)]
pub(crate) struct Bucket {
    pub(crate) objects: Mutex<HashMap<String, Vec<u8>>>,
    pub(crate) gets: AtomicUsize,
    /// Fails every read with this text.
    pub(crate) err: Mutex<Option<String>>,
    /// Every read hangs until its context ends.
    pub(crate) hang: bool,
}

impl Bucket {
    pub(crate) fn with(objects: &[(&str, &[u8])]) -> Arc<Bucket> {
        let b = Bucket::default();
        for (k, v) in objects {
            b.objects.lock().insert(k.to_string(), v.to_vec());
        }
        Arc::new(b)
    }

    pub(crate) fn put(&self, key: &str, data: &[u8]) {
        self.objects.lock().insert(key.to_string(), data.to_vec());
    }

    pub(crate) fn gets(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }

    /// Reads `bucket/object`, as a storage client would.
    pub(crate) async fn get(&self, ctx: &Ctx, key: &str) -> Result<Vec<u8>, String> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        if self.hang {
            return Err(ctx.done().await.to_string());
        }
        if let Some(e) = self.err.lock().clone() {
            return Err(e);
        }
        self.objects
            .lock()
            .get(key)
            .cloned()
            .ok_or_else(|| format!("object not found: {key}"))
    }
}

/// `bucket/object` of a `scheme://bucket/object` URL.
pub(crate) fn bucket_key(u: &url::Url) -> String {
    format!(
        "{}/{}",
        u.host_str().unwrap_or(""),
        u.path().trim_start_matches('/')
    )
}

#[async_trait]
impl AnonymousFetcher for Bucket {
    async fn fetch(
        &self,
        ctx: &Ctx,
        u: &url::Url,
        path: &Path,
        max_bytes: i64,
    ) -> Result<(), String> {
        let data = self.get(ctx, &bucket_key(u)).await?;
        if data.len() as i64 > max_bytes {
            return Err(format!("asset exceeds {max_bytes}-byte cap"));
        }
        std::fs::write(path, data).map_err(|e| e.to_string())
    }
}

/// What a fake object store's asset fetch does.
pub(crate) enum AssetMode {
    /// Serves objects of `scheme` from the bucket as a storage plugin
    /// does: other schemes are InvalidArgument, a snapshot path is
    /// PermissionDenied, a missing object NotFound, wrong bytes
    /// FailedPrecondition.
    Serve { scheme: String, bucket: Arc<Bucket> },
    /// Writes `data`, if any, into the destination and fails with `err`.
    Fixed {
        data: Option<Vec<u8>>,
        err: Option<Status>,
    },
    /// Cancels `cancel`, then fails with `err`.
    Canceling { cancel: Ctx, err: Status },
}

/// An object store serving assets only, counting asset fetches.
pub(crate) struct AssetStore {
    pub(crate) mode: Mutex<AssetMode>,
    pub(crate) calls: AtomicUsize,
}

impl AssetStore {
    pub(crate) fn new(mode: AssetMode) -> Arc<AssetStore> {
        Arc::new(AssetStore {
            mode: Mutex::new(mode),
            calls: AtomicUsize::new(0),
        })
    }

    pub(crate) fn serving(scheme: &str, bucket: Arc<Bucket>) -> Arc<AssetStore> {
        Self::new(AssetMode::Serve {
            scheme: scheme.into(),
            bucket,
        })
    }

    pub(crate) fn failing(err: Status) -> Arc<AssetStore> {
        Self::new(AssetMode::Fixed {
            data: None,
            err: Some(err),
        })
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStore for AssetStore {
    async fn fetch_snapshot(&self, _: &Ctx, _: FetchSnapshot) -> Result<(), Status> {
        Err(Status::unimplemented("assets only"))
    }

    async fn upload_snapshot(&self, _: &Ctx, _: UploadSnapshot) -> Result<(), Status> {
        Err(Status::unimplemented("assets only"))
    }

    async fn fetch_asset(&self, ctx: &Ctx, req: FetchAsset) -> Result<(), Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (scheme, bucket) = match &*self.mode.lock() {
            AssetMode::Fixed { data, err } => {
                if let Some(d) = data {
                    std::fs::write(&req.write_path, d).unwrap();
                }
                return match err {
                    Some(e) => Err(e.clone()),
                    None => Ok(()),
                };
            }
            AssetMode::Canceling { cancel, err } => {
                cancel.cancel();
                return Err(err.clone());
            }
            AssetMode::Serve { scheme, bucket } => (scheme.clone(), bucket.clone()),
        };
        let u =
            url::Url::parse(&req.asset_uri).map_err(|_| Status::invalid_argument("not a URL"))?;
        if u.scheme() != scheme {
            return Err(Status::invalid_argument(format!(
                "this store serves {scheme}:// assets only"
            )));
        }
        let key = bucket_key(&u);
        if key.contains("/actors/") && key.contains("/snapshots/") {
            return Err(Status::permission_denied(
                "path is under a snapshot location",
            ));
        }
        let data = bucket.get(ctx, &key).await.map_err(Status::not_found)?;
        if req.max_bytes > 0 && data.len() as i64 > req.max_bytes {
            return Err(Status::failed_precondition("asset exceeds the size limit"));
        }
        if digest(&data) != req.sha256 {
            return Err(Status::failed_precondition("asset digest mismatch"));
        }
        std::fs::write(&req.write_path, data).map_err(|e| Status::internal(e.to_string()))
    }
}

/// A cache at `root/cache`.
pub(crate) fn cache_at(root: &Path, min_age: Option<std::time::Duration>) -> filecache::Store {
    let mut opts = filecache::Options::default();
    if let Some(m) = min_age {
        opts.min_age = m;
    }
    filecache::Store::open(root.join("cache"), opts).unwrap()
}

/// How many entries the cache at `root/cache` publishes.
pub(crate) fn cached_assets(root: &Path) -> usize {
    std::fs::read_dir(root.join("cache/entries"))
        .unwrap()
        .count()
}

/// A stager over a cache at `root/cache`.
pub(crate) fn stager_at(
    root: &Path,
    store: Option<Arc<dyn ObjectStore>>,
    anon: Option<Arc<dyn AnonymousFetcher>>,
) -> AssetStager {
    AssetStager::new(cache_at(root, None), store, anon, "test-store")
}

static STAGE_SEQ: AtomicUsize = AtomicUsize::new(0);

/// Stages `entry` as the asset "asset" of a fresh consumer dir under
/// `root/consumer` and returns its path.
pub(crate) async fn stage_test_asset(
    s: &AssetStager,
    ctx: &Ctx,
    root: &Path,
    entry: AssetEntry,
) -> Result<PathBuf, Status> {
    let n = STAGE_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = root.join("consumer").join(n.to_string());
    std::fs::create_dir_all(&dir).unwrap();
    let mut rec = SandboxAssetsRecord::default();
    rec.assets.insert("asset".into(), entry);
    let paths = s.stage(ctx, &rec, &dir).await?;
    Ok(PathBuf::from(&paths["asset"]))
}

/// An uncompressed gVisor release tarball whose runsc holds `runsc`.
pub(crate) fn gvisor_release_tarball(runsc: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut b = tar::Builder::new(&mut buf);
        for (name, data) in [
            ("runsc", runsc.as_bytes()),
            ("gvisor-bin/containerd-shim", &b"shim bytes"[..]),
        ] {
            let mut h = tar::Header::new_gnu();
            h.set_size(data.len() as u64);
            h.set_mode(0o755);
            b.append_data(&mut h, name, data).unwrap();
        }
        b.finish().unwrap();
    }
    buf
}

/// A record of one gVisor asset at `url` with `content`'s digest.
pub(crate) fn gvisor_record(url: &str, content: &[u8]) -> SandboxAssetsRecord {
    let mut rec = SandboxAssetsRecord {
        sandbox_class: "gvisor".into(),
        ..Default::default()
    };
    rec.assets.insert(
        crate::assets::GVISOR_ASSET_NAME.into(),
        AssetEntry {
            url: url.into(),
            sha256: digest(content),
        },
    );
    rec
}

/// One recorded log event: its level, message and other fields.
#[derive(Debug, Clone)]
pub(crate) struct LogLine {
    pub(crate) level: tracing::Level,
    pub(crate) message: String,
    pub(crate) fields: Vec<(String, String)>,
}

impl LogLine {
    pub(crate) fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The whole line as text.
    pub(crate) fn text(&self) -> String {
        let mut s = format!("{} {}", self.level, self.message);
        for (k, v) in &self.fields {
            s.push_str(&format!(" {k}={v}"));
        }
        s
    }
}

/// Records every log event of the current thread until dropped.
pub(crate) struct Logs {
    lines: Arc<Mutex<Vec<LogLine>>>,
    _guard: tracing::subscriber::DefaultGuard,
}

impl Logs {
    pub(crate) fn capture() -> Logs {
        use tracing_subscriber::layer::SubscriberExt;
        let lines = Arc::new(Mutex::new(Vec::new()));
        let sub = tracing_subscriber::registry().with(Layer {
            lines: lines.clone(),
        });
        Logs {
            lines,
            _guard: tracing::subscriber::set_default(sub),
        }
    }

    pub(crate) fn lines(&self) -> Vec<LogLine> {
        self.lines.lock().clone()
    }

    /// Every line as text, one per line.
    pub(crate) fn text(&self) -> String {
        self.lines()
            .iter()
            .map(LogLine::text)
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The last line whose message contains `what`.
    pub(crate) fn last(&self, what: &str) -> Option<LogLine> {
        self.lines()
            .into_iter()
            .rev()
            .find(|l| l.message.contains(what))
    }
}

struct Layer {
    lines: Arc<Mutex<Vec<LogLine>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Layer {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct V(String, Vec<(String, String)>);
        impl tracing::field::Visit for V {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                if f.name() == "message" {
                    self.0 = format!("{v:?}");
                } else {
                    self.1.push((f.name().to_string(), format!("{v:?}")));
                }
            }
            fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                if f.name() == "message" {
                    self.0 = v.to_string();
                } else {
                    self.1.push((f.name().to_string(), v.to_string()));
                }
            }
        }
        let mut v = V(String::new(), Vec::new());
        event.record(&mut v);
        self.lines.lock().push(LogLine {
            level: *event.metadata().level(),
            message: v.0,
            fields: v.1,
        });
    }
}

/// A WorkerService recording every request, failing each with `err` when
/// set.
#[derive(Default)]
pub(crate) struct RecordingWorkers {
    pub(crate) err: Mutex<Option<Status>>,
    pub(crate) epoch: i64,
    pub(crate) mint_certs: Vec<Vec<u8>>,
    pub(crate) acquired: Mutex<Vec<hivekeep_proto::api::v1::AcquireWorkerEpochRequest>>,
    pub(crate) registered: Mutex<Vec<hivekeep_proto::api::v1::RegisterWorkerRequest>>,
    pub(crate) runtimes: Mutex<Vec<hivekeep_proto::api::v1::SetWorkerSandboxRuntimesRequest>>,
    pub(crate) minted: Mutex<Vec<hivekeep_proto::api::v1::MintWorkerActorCertificateRequest>>,
    pub(crate) suspends: Mutex<Vec<hivekeep_proto::api::v1::RequestActorSuspendRequest>>,
    pub(crate) reports: Mutex<Vec<hivekeep_proto::api::v1::ReportActorSnapshotStateRequest>>,
    pub(crate) crashes: Mutex<Vec<hivekeep_proto::api::v1::ReportActorCrashedRequest>>,
    /// Signalled on every snapshot state report.
    pub(crate) reported: tokio::sync::Notify,
}

impl RecordingWorkers {
    fn fail(&self) -> Result<(), Status> {
        match self.err.lock().clone() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl crate::control::WorkerServiceApi for RecordingWorkers {
    async fn acquire_worker_epoch(
        &self,
        _: &Ctx,
        req: hivekeep_proto::api::v1::AcquireWorkerEpochRequest,
    ) -> Result<hivekeep_proto::api::v1::AcquireWorkerEpochResponse, Status> {
        self.fail()?;
        self.acquired.lock().push(req);
        Ok(hivekeep_proto::api::v1::AcquireWorkerEpochResponse { epoch: self.epoch })
    }
    async fn register_worker(
        &self,
        _: &Ctx,
        req: hivekeep_proto::api::v1::RegisterWorkerRequest,
    ) -> Result<(), Status> {
        self.fail()?;
        self.registered.lock().push(req);
        Ok(())
    }
    async fn set_worker_sandbox_runtimes(
        &self,
        _: &Ctx,
        req: hivekeep_proto::api::v1::SetWorkerSandboxRuntimesRequest,
    ) -> Result<(), Status> {
        self.fail()?;
        self.runtimes.lock().push(req);
        Ok(())
    }
    async fn mint_worker_actor_certificate(
        &self,
        _: &Ctx,
        req: hivekeep_proto::api::v1::MintWorkerActorCertificateRequest,
    ) -> Result<hivekeep_proto::api::v1::MintWorkerActorCertificateResponse, Status> {
        self.fail()?;
        self.minted.lock().push(req);
        Ok(
            hivekeep_proto::api::v1::MintWorkerActorCertificateResponse {
                actor_certificates: self.mint_certs.clone(),
            },
        )
    }
    async fn mint_worker_actor_jwt(
        &self,
        _: &Ctx,
        _: hivekeep_proto::api::v1::MintWorkerActorJwtRequest,
    ) -> Result<hivekeep_proto::api::v1::MintWorkerActorJwtResponse, Status> {
        Err(Status::unimplemented("not in these tests"))
    }
    async fn request_actor_suspend(
        &self,
        _: &Ctx,
        req: hivekeep_proto::api::v1::RequestActorSuspendRequest,
    ) -> Result<(), Status> {
        self.fail()?;
        self.suspends.lock().push(req);
        Ok(())
    }
    async fn report_actor_snapshot_state(
        &self,
        _: &Ctx,
        req: hivekeep_proto::api::v1::ReportActorSnapshotStateRequest,
    ) -> Result<(), Status> {
        self.reports.lock().push(req);
        self.reported.notify_one();
        self.fail()
    }
    async fn report_actor_crashed(
        &self,
        _: &Ctx,
        req: hivekeep_proto::api::v1::ReportActorCrashedRequest,
    ) -> Result<(), Status> {
        self.fail()?;
        self.crashes.lock().push(req);
        Ok(())
    }
}

//! Staging sandbox runtime binaries for an actor, from the node's
//! content-addressed asset cache.
//!
//! Each asset is served from the cache as a hard link, so it is downloaded
//! once per node, concurrent callers share the download, and what an actor
//! was handed stays valid whatever the cache evicts later. An asset already
//! present in the destination is kept: the destination is the actor's own
//! directory, reset at Run and Restore, so a Checkpoint or Terminate finds
//! exactly what its activation staged.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tonic::{Code, Status};

use crate::assets::{
    extract_tar_archive, plain_asset_name, redact_url, AssetEntry, SandboxAssetsRecord,
    GVISOR_ASSET_NAME, MAX_ASSET_BYTES,
};
use crate::atomicfile::{chmod, create_dir_all_mode};
use crate::context::Ctx;
use crate::filecache::{self, CacheError, Key};
use crate::objectstore::{FetchAsset, ObjectStore};
use crate::status::fs_err;

/// Store codes after which a `gs://` asset is read anonymously: the store
/// has no such object, cannot serve the URL, does not serve assets, cannot
/// be reached, or failed unexpectedly. Any other code is final.
const ANONYMOUS_FALLBACK_CODES: [Code; 5] = [
    Code::NotFound,
    Code::InvalidArgument,
    Code::Unimplemented,
    Code::Unavailable,
    Code::Internal,
];

/// Reads public objects without credentials.
#[async_trait]
pub trait AnonymousFetcher: Send + Sync + 'static {
    /// Writes the object at `url` into the existing file at `path`,
    /// replacing its contents.
    async fn fetch(
        &self,
        ctx: &Ctx,
        url: &url::Url,
        path: &Path,
        max_bytes: i64,
    ) -> Result<(), String>;
}

/// Reads public `gs://` objects over Cloud Storage's anonymous HTTPS
/// endpoint, which serves public buckets such as the gVisor releases.
pub struct PublicGcsFetcher {
    client: reqwest::Client,
    endpoint: String,
}

impl PublicGcsFetcher {
    /// A fetcher against the public endpoint.
    pub fn new() -> Result<Self, String> {
        Ok(PublicGcsFetcher {
            client: reqwest::Client::builder()
                .build()
                .map_err(|e| e.to_string())?,
            endpoint: "https://storage.googleapis.com".into(),
        })
    }
}

#[async_trait]
impl AnonymousFetcher for PublicGcsFetcher {
    async fn fetch(
        &self,
        ctx: &Ctx,
        url: &url::Url,
        path: &Path,
        max_bytes: i64,
    ) -> Result<(), String> {
        let bucket = url.host_str().ok_or("gs URL has no bucket")?;
        let http = format!("{}/{}{}", self.endpoint, bucket, url.path());
        let work = async {
            let mut resp = self
                .client
                .get(&http)
                .send()
                .await
                .and_then(|r| r.error_for_status())
                .map_err(|e| format!("while fetching: {}", e.without_url()))?;
            let mut out = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(path)
                .map_err(|e| format!("while opening download file: {e}"))?;
            let mut total: i64 = 0;
            while let Some(chunk) = resp
                .chunk()
                .await
                .map_err(|e| format!("while downloading: {}", e.without_url()))?
            {
                total += chunk.len() as i64;
                if total > max_bytes {
                    return Err(format!("asset exceeds {max_bytes}-byte cap"));
                }
                std::io::Write::write_all(&mut out, &chunk)
                    .map_err(|e| format!("while writing download: {e}"))?;
            }
            Ok(())
        };
        match ctx.run(work).await {
            Ok(r) => r,
            Err(c) => Err(c.to_string()),
        }
    }
}

/// Stages sandbox assets through the asset cache.
pub struct AssetStager {
    cache: filecache::Store,
    store: Option<Arc<dyn ObjectStore>>,
    anonymous: Option<Arc<dyn AnonymousFetcher>>,
    store_name: String,
    max_bytes: i64,
}

impl AssetStager {
    /// A stager filling `cache` from `store` first and, for a `gs://` asset
    /// the store does not serve, from `anonymous`. `store_name` names the
    /// store in errors.
    pub fn new(
        cache: filecache::Store,
        store: Option<Arc<dyn ObjectStore>>,
        anonymous: Option<Arc<dyn AnonymousFetcher>>,
        store_name: impl Into<String>,
    ) -> Self {
        AssetStager {
            cache,
            store,
            anonymous,
            store_name: store_name.into(),
            max_bytes: MAX_ASSET_BYTES,
        }
    }

    /// Lowers the size cap, for tests.
    pub fn with_max_bytes(mut self, max: i64) -> Self {
        self.max_bytes = max;
        self
    }

    /// The cache underneath.
    pub fn cache(&self) -> &filecache::Store {
        &self.cache
    }

    /// Stages every asset of `rec` under `dst_dir` (created owner-only if
    /// needed) and returns each asset's local path by name: for the gVisor
    /// release, its `runsc`.
    pub async fn ensure(
        &self,
        ctx: &Ctx,
        rec: &SandboxAssetsRecord,
        dst_dir: &Path,
    ) -> Result<BTreeMap<String, String>, Status> {
        create_dir_all_mode(dst_dir, 0o700)
            .map_err(|e| fs_err("while creating sandbox assets dir", &e))?;
        self.stage(ctx, rec, dst_dir).await
    }

    /// [`ensure`](Self::ensure) into an existing `dst_dir`: it creates
    /// nothing above the assets, so a caller whose directory may be removed
    /// under it fails instead of recreating it.
    pub async fn stage(
        &self,
        ctx: &Ctx,
        rec: &SandboxAssetsRecord,
        dst_dir: &Path,
    ) -> Result<BTreeMap<String, String>, Status> {
        let mut paths = BTreeMap::new();
        for (name, entry) in &rec.assets {
            if !plain_asset_name(name) {
                return Err(Status::invalid_argument(format!(
                    "sandbox asset name {name:?} is not a plain file name"
                )));
            }
            let dst = dst_dir.join(name);
            let p = if name == GVISOR_ASSET_NAME {
                self.stage_gvisor_release(ctx, entry, &dst).await
            } else {
                self.stage_asset(ctx, entry, &dst).await
            }
            .map_err(|e| {
                crate::status::wrap(format!("while fetching sandbox asset {name:?}"), e)
            })?;
            paths.insert(name.clone(), p.to_string_lossy().into_owned());
        }
        Ok(paths)
    }

    async fn stage_asset(
        &self,
        ctx: &Ctx,
        entry: &AssetEntry,
        dst: &Path,
    ) -> Result<PathBuf, Status> {
        let key = asset_key(entry)?;
        if staged(dst)? {
            return Ok(dst.to_path_buf());
        }
        let this = self.downloader();
        let entry2 = entry.clone();
        let fetch = filecache::fetcher(move |ctx: Ctx, path: PathBuf| async move {
            tracing::info!(url = %entry2.display_url(), sha256 = %entry2.sha256, "Sandbox asset cache miss; downloading");
            let started = std::time::Instant::now();
            this.download_verified(&ctx, &entry2, &path).await?;
            // Executable for everyone: a runtime may re-execute its binary
            // after dropping privileges. The cache publishes it read-only
            // and the directory it is linked into is the owner's alone.
            chmod(&path, 0o755).map_err(|e| fs_err("while setting file mode", &e))?;
            tracing::info!(url = %entry2.display_url(), duration_ms = started.elapsed().as_millis() as u64, "Sandbox asset download complete");
            Ok(())
        });
        match self.cache.get_file_to(ctx, &key, dst, fetch).await {
            Ok(()) => Ok(dst.to_path_buf()),
            Err(e) if staged_meanwhile(&e, dst) => Ok(dst.to_path_buf()),
            Err(e) => Err(e.into()),
        }
    }

    async fn stage_gvisor_release(
        &self,
        ctx: &Ctx,
        entry: &AssetEntry,
        dst: &Path,
    ) -> Result<PathBuf, Status> {
        let runsc = dst.join("runsc");
        let key = asset_key(entry)?;
        if staged(dst)? {
            return Ok(runsc);
        }
        let this = self.downloader();
        let entry2 = entry.clone();
        let fetch = filecache::fetcher(move |ctx: Ctx, dir: PathBuf| async move {
            tracing::info!(url = %entry2.display_url(), sha256 = %entry2.sha256, "gVisor release cache miss; downloading");
            // Downloaded into the tree it is extracted into and removed
            // before the cache publishes the tree.
            let tarball = dir.join(".gvisor-release-download");
            this.download_verified(&ctx, &entry2, &tarball).await?;
            let shown = entry2.display_url();
            let (ctx2, tarball2, dir2) = (ctx.clone(), tarball.clone(), dir.clone());
            tokio::task::spawn_blocking(move || {
                extract_tar_archive(&ctx2, &tarball2, &shown, &dir2)
            })
            .await
            .map_err(crate::status::join_err)??;
            match std::fs::metadata(dir.join("runsc")) {
                Ok(m) if m.is_file() => {}
                _ => {
                    return Err(Status::failed_precondition(format!(
                        "gvisor tarball {} contains no runsc binary",
                        entry2.display_url()
                    )))
                }
            }
            std::fs::remove_file(&tarball)
                .map_err(|e| fs_err("while removing the downloaded tarball", &e))
        });
        match self.cache.get_dir_to(ctx, &key, dst, fetch).await {
            Ok(()) => Ok(runsc),
            Err(e) if staged_meanwhile(&e, dst) => Ok(runsc),
            Err(e) => Err(e.into()),
        }
    }

    fn downloader(&self) -> Downloader {
        Downloader {
            store: self.store.clone(),
            anonymous: self.anonymous.clone(),
            store_name: self.store_name.clone(),
            max_bytes: self.max_bytes,
        }
    }
}

/// Opens the asset cache at `dir`, sweeps the debris a crash may have
/// left, and empties `scratch_dir`, where fetches without an actor to link
/// into stage. It refuses a cache from which any of `link_dirs` cannot be
/// hard-linked: the actors dir, where every hit is such a link, and the
/// workers dir, where each worker's default runtime is kept by one.
pub async fn open_sandbox_asset_cache(
    dir: &Path,
    opts: filecache::Options,
    scratch_dir: &Path,
    link_dirs: &[&Path],
) -> Result<filecache::Store, String> {
    let store = filecache::Store::open(dir, opts).map_err(|e| {
        format!(
            "while opening sandbox asset cache at {}: {e}",
            dir.display()
        )
    })?;
    for link_dir in link_dirs {
        can_hard_link(dir, link_dir).map_err(|e| format!("sandbox asset cache: {e}"))?;
    }
    let stats = match store.sweep_debris().await {
        Ok(s) => s,
        Err((s, err)) => {
            // Leftover debris wastes space but no lookup sees it.
            tracing::warn!(%err, "Sandbox asset cache debris sweep incomplete");
            s
        }
    };
    crate::actordirs::reset_dir(
        "sandbox asset scratch dir",
        scratch_dir,
        crate::actordirs::remove_all,
        0o700,
    )
    .map_err(|e| e.to_string())?;
    tracing::info!(
        dir = %dir.display(),
        min_age = ?store.min_age(),
        fetch_timeout = ?store.fetch_timeout(),
        tmp_removed = stats.tmp_removed,
        retired_removed = stats.retired_removed,
        "Sandbox asset cache open"
    );
    Ok(store)
}

/// Fails unless a file under `cache_dir` can be hard-linked into
/// `link_dir` (created if missing), which every cache hit there relies on.
/// It links a probe file and removes both again. Only the probe is exact:
/// a link across two mounts fails even when both are bind mounts of one
/// filesystem, which no path or device check can see.
pub fn can_hard_link(cache_dir: &Path, link_dir: &Path) -> std::io::Result<()> {
    create_dir_all_mode(link_dir, 0o700).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("while creating {}: {e}", link_dir.display()),
        )
    })?;
    let name = format!(".linkprobe-{}", crate::rand::suffix());
    let probe = cache_dir.join(&name);
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "while creating a link probe in {}: {e}",
                    cache_dir.display()
                ),
            )
        })?;
    let link = link_dir.join(&name);
    let res = std::fs::hard_link(&probe, &link);
    let _ = std::fs::remove_file(&probe);
    match res {
        Ok(()) => std::fs::remove_file(&link),
        Err(e) if e.raw_os_error() == Some(libc::EXDEV) => Err(std::io::Error::new(
            e.kind(),
            format!(
                "{} and {} are on different mounts, so assets cannot be hard-linked under {}; put the cache on the mount that holds it: {e}",
                cache_dir.display(),
                link_dir.display(),
                link_dir.display()
            ),
        )),
        Err(e) => Err(std::io::Error::new(
            e.kind(),
            format!(
                "while probing a hard link from {} into {}: {e}",
                cache_dir.display(),
                link_dir.display()
            ),
        )),
    }
}

/// The asset's cache key: its content hash.
fn asset_key(entry: &AssetEntry) -> Result<Key, Status> {
    Key::sha256(&entry.sha256)
        .map_err(|e| Status::invalid_argument(format!("while validating asset hash: {e}")))
}

/// Whether `dst` already holds the asset from this activation.
fn staged(dst: &Path) -> Result<bool, Status> {
    match std::fs::symlink_metadata(dst) {
        Ok(_) => {
            tracing::debug!(path = %dst.display(), "Sandbox asset already staged");
            Ok(true)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(fs_err("while stat-ing staged asset", &e)),
    }
}

/// Whether the cache reported `dst` exists because a concurrent call for
/// the same actor staged it first. A link is complete once it exists, and
/// a tree is placed by rename, so what is there is whole.
fn staged_meanwhile(err: &CacheError, dst: &Path) -> bool {
    err.is_exists() && std::fs::symlink_metadata(dst).is_ok()
}

/// Downloads and verifies one asset; detached from the stager so a fetch
/// can outlive the call that started it.
#[derive(Clone)]
struct Downloader {
    store: Option<Arc<dyn ObjectStore>>,
    anonymous: Option<Arc<dyn AnonymousFetcher>>,
    store_name: String,
    max_bytes: i64,
}

impl Downloader {
    /// Fetches `entry` into a new file at `path` and checks its size and
    /// digest; the keeper executes these bytes, so it verifies them
    /// whichever path fetched them. On error the file is removed.
    async fn download_verified(
        &self,
        ctx: &Ctx,
        entry: &AssetEntry,
        path: &Path,
    ) -> Result<(), Status> {
        let want = hex::decode(&entry.sha256)
            .map_err(|e| Status::invalid_argument(format!("while parsing sha256 hash: {e}")))?;
        let u = url::Url::parse(&entry.url).map_err(|e| {
            Status::invalid_argument(format!("sandbox asset URL is not a valid URL: {e}"))
        })?;
        // The URL parser passes a malformed percent-escape through; it
        // would name a different object than the one meant.
        if !valid_percent_escapes(u.as_str()) {
            return Err(Status::invalid_argument(
                "sandbox asset URL is not a valid URL: malformed percent-escape",
            ));
        }
        let shown = redact_url(&u);
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| fs_err("while creating download file", &e))?;
        let res = self
            .download_into(ctx, entry, &u, &shown, path, &want)
            .await;
        if res.is_err() {
            let _ = std::fs::remove_file(path);
        }
        res
    }

    async fn download_into(
        &self,
        ctx: &Ctx,
        entry: &AssetEntry,
        u: &url::Url,
        shown: &str,
        path: &Path,
        want: &[u8],
    ) -> Result<(), Status> {
        let anon_fallback = u.scheme() == "gs" && self.anonymous.is_some();
        let mut store_code = Code::Ok;
        if let Some(store) = &self.store {
            let res = store
                .fetch_asset(
                    ctx,
                    FetchAsset {
                        asset_uri: entry.url.clone(),
                        sha256: entry.sha256.clone(),
                        write_path: path.to_path_buf(),
                        max_bytes: self.max_bytes,
                    },
                )
                .await;
            // The store's message is dropped: it may quote a signed URL.
            store_code = match res {
                Ok(()) => Code::Ok,
                Err(e) => e.code(),
            };
            if store_code == Code::Ok {
                return verify_asset_file(path, shown, want, self.max_bytes);
            }
            if !anon_fallback || !ANONYMOUS_FALLBACK_CODES.contains(&store_code) {
                tracing::warn!(url = shown, code = ?store_code, "Object store failed to fetch sandbox asset");
                return Err(self.store_error(ctx, shown, entry, store_code, None));
            }
            if let Some(c) = ctx.err() {
                return Err(Status::new(
                    Status::from(c).code(),
                    format!("sandbox asset {shown}: {c}"),
                ));
            }
            if matches!(store_code, Code::Internal | Code::Unavailable) {
                tracing::warn!(url = shown, code = ?store_code, "Object store did not serve sandbox asset; downloading it anonymously");
            } else {
                tracing::info!(url = shown, code = ?store_code, "Object store did not serve sandbox asset; downloading it anonymously");
            }
        } else if !anon_fallback {
            return Err(Status::failed_precondition(format!(
                "sandbox asset {shown}: no object store is configured to fetch it"
            )));
        }
        let anon = self.anonymous.as_ref().expect("checked above");
        let res = match anon.fetch(ctx, u, path, self.max_bytes).await {
            Ok(()) => verify_asset_file(path, shown, want, self.max_bytes),
            Err(e) => Err(Status::internal(format!("while fetching {shown}: {e}"))),
        };
        match res {
            Ok(()) => Ok(()),
            Err(e) if self.store.is_none() => Err(e),
            Err(e) => Err(self.store_error(ctx, shown, entry, store_code, Some(e.message()))),
        }
    }

    /// The keeper's own description of the store failing with `code`;
    /// `anon` says why the anonymous read that followed failed.
    fn store_error(
        &self,
        ctx: &Ctx,
        shown: &str,
        entry: &AssetEntry,
        code: Code,
        anon: Option<&str>,
    ) -> Status {
        let store = &self.store_name;
        let (code_out, mut msg) = match code {
            Code::Unimplemented => (
                Code::FailedPrecondition,
                format!("the object store {store} does not serve assets; private sandbox assets need one that does"),
            ),
            Code::Unavailable => (Code::Unavailable, format!("the object store {store} is unavailable")),
            Code::Cancelled | Code::DeadlineExceeded => {
                if let Some(c) = ctx.err() {
                    return Status::new(Status::from(c).code(), format!("sandbox asset {shown}: {c}"));
                }
                (Code::Unavailable, format!("the object store returned {code:?}"))
            }
            Code::PermissionDenied => (
                Code::FailedPrecondition,
                format!("the object store refused it ({code:?}); assets must not live under a snapshot location"),
            ),
            Code::NotFound => (Code::FailedPrecondition, format!("the object store did not find it ({code:?})")),
            Code::FailedPrecondition => (
                Code::FailedPrecondition,
                format!(
                    "the object store rejected its content ({code:?}): larger than {} bytes or sha256 is not {}",
                    self.max_bytes, entry.sha256
                ),
            ),
            Code::InvalidArgument => (Code::FailedPrecondition, format!("the object store rejected the request ({code:?})")),
            _ => (Code::Internal, format!("the object store failed ({code:?})")),
        };
        if let Some(a) = anon {
            msg.push_str(&format!("; anonymous download failed: {a}"));
        }
        Status::new(code_out, format!("sandbox asset {shown}: {msg}"))
    }
}

/// Checks the size cap and sha256 of the file at `path`.
/// Whether every `%` in `s` starts a two-hex-digit escape.
fn valid_percent_escapes(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            if i + 2 >= b.len() {
                return false;
            }
            if !(b[i + 1].is_ascii_hexdigit() && b[i + 2].is_ascii_hexdigit()) {
                return false;
            }
            i += 3;
        } else {
            i += 1;
        }
    }
    true
}

fn verify_asset_file(path: &Path, shown: &str, want: &[u8], max_bytes: i64) -> Result<(), Status> {
    let mut f = std::fs::File::open(path).map_err(|e| fs_err("while opening fetched asset", &e))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut total: i64 = 0;
    loop {
        let n = f
            .read(&mut buf)
            .map_err(|e| fs_err("while hashing fetched asset", &e))?;
        if n == 0 {
            break;
        }
        total += n as i64;
        if total > max_bytes {
            return Err(Status::failed_precondition(format!(
                "asset {shown} exceeds {max_bytes}-byte cap"
            )));
        }
        hasher.update(&buf[..n]);
    }
    let got = hasher.finalize();
    if got.as_slice() != want {
        return Err(Status::failed_precondition(format!(
            "asset {shown} sha256 mismatch; got={} want={}",
            hex::encode(got),
            hex::encode(want)
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objectstore::LocalObjectStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn digest(b: &[u8]) -> String {
        hex::encode(Sha256::digest(b))
    }

    struct Setup {
        _d: tempfile::TempDir,
        root: PathBuf,
        objects: Arc<LocalObjectStore>,
        cache: filecache::Store,
    }

    fn setup() -> Setup {
        let d = tempfile::tempdir().unwrap();
        let root = d.path().to_path_buf();
        let objects = Arc::new(LocalObjectStore::new(root.join("objects")));
        let cache =
            filecache::Store::open(root.join("cache"), filecache::Options::default()).unwrap();
        Setup {
            _d: d,
            root,
            objects,
            cache,
        }
    }

    fn put(s: &Setup, url: &str, data: &[u8]) {
        let p = s.objects.dir_for(url).unwrap();
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, data).unwrap();
    }

    fn record(entries: &[(&str, &str, &[u8])]) -> SandboxAssetsRecord {
        let mut rec = SandboxAssetsRecord {
            sandbox_class: "microvm".into(),
            ..Default::default()
        };
        for (name, url, data) in entries {
            rec.assets.insert(
                name.to_string(),
                AssetEntry {
                    url: url.to_string(),
                    sha256: digest(data),
                },
            );
        }
        rec
    }

    #[tokio::test]
    async fn stages_assets_as_links_and_reuses_the_cache() {
        let s = setup();
        put(&s, "https://releases.example/kernel", b"kernel-bytes");
        let stager = AssetStager::new(s.cache.clone(), Some(s.objects.clone()), None, "local");
        let rec = record(&[("kernel", "https://releases.example/kernel", b"kernel-bytes")]);
        let a = s.root.join("actor-a");
        let paths = stager.ensure(&Ctx::background(), &rec, &a).await.unwrap();
        assert_eq!(std::fs::read(&paths["kernel"]).unwrap(), b"kernel-bytes");
        // A second actor is served from the cache even with the object gone.
        std::fs::remove_file(
            s.objects
                .dir_for("https://releases.example/kernel")
                .unwrap(),
        )
        .unwrap();
        let b = s.root.join("actor-b");
        stager.ensure(&Ctx::background(), &rec, &b).await.unwrap();
        // Already staged files are kept as they are.
        stager.ensure(&Ctx::background(), &rec, &b).await.unwrap();
    }

    #[tokio::test]
    async fn digest_mismatch_fails_and_caches_nothing() {
        let s = setup();
        put(&s, "https://releases.example/vmm", b"tampered");
        let stager = AssetStager::new(s.cache.clone(), Some(s.objects.clone()), None, "local");
        let rec = record(&[("vmm", "https://releases.example/vmm", b"original")]);
        let err = stager
            .ensure(&Ctx::background(), &rec, &s.root.join("a"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert_eq!(s.cache.total_bytes().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn refuses_bad_names_and_missing_store() {
        let s = setup();
        let stager = AssetStager::new(s.cache.clone(), None, None, "none");
        let rec = record(&[("../x", "https://h/x", b"x")]);
        assert_eq!(
            stager
                .ensure(&Ctx::background(), &rec, &s.root.join("a"))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        let rec = record(&[("x", "https://h/x", b"x")]);
        assert_eq!(
            stager
                .ensure(&Ctx::background(), &rec, &s.root.join("a"))
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
    }

    struct FakeAnon {
        calls: AtomicUsize,
        body: Vec<u8>,
    }

    #[async_trait]
    impl AnonymousFetcher for FakeAnon {
        async fn fetch(
            &self,
            _ctx: &Ctx,
            _u: &url::Url,
            path: &Path,
            _max: i64,
        ) -> Result<(), String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::fs::write(path, &self.body).map_err(|e| e.to_string())
        }
    }

    #[tokio::test]
    async fn gs_assets_fall_back_to_anonymous_reads() {
        let s = setup();
        let anon = Arc::new(FakeAnon {
            calls: AtomicUsize::new(0),
            body: b"public".to_vec(),
        });
        let stager = AssetStager::new(
            s.cache.clone(),
            Some(s.objects.clone()),
            Some(anon.clone()),
            "local",
        );
        let rec = record(&[("img", "gs://public-bucket/img", b"public")]);
        stager
            .ensure(&Ctx::background(), &rec, &s.root.join("a"))
            .await
            .unwrap();
        assert_eq!(anon.calls.load(Ordering::SeqCst), 1);
        // https assets never fall back.
        let rec = record(&[("img2", "https://h/img2", b"other")]);
        assert!(stager
            .ensure(&Ctx::background(), &rec, &s.root.join("b"))
            .await
            .is_err());
        assert_eq!(anon.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stages_gvisor_release_trees() {
        let s = setup();
        let tarball = {
            let mut buf = Vec::new();
            {
                let gz = flate2::write::GzEncoder::new(&mut buf, flate2::Compression::fast());
                let mut b = tar::Builder::new(gz);
                for (name, data) in [
                    ("runsc", &b"runsc"[..]),
                    ("gvisor-bin/helper", &b"helper"[..]),
                ] {
                    let mut h = tar::Header::new_gnu();
                    h.set_size(data.len() as u64);
                    h.set_mode(0o755);
                    b.append_data(&mut h, name, data).unwrap();
                }
                b.into_inner().unwrap().finish().unwrap();
            }
            buf
        };
        put(&s, "https://releases.example/gvisor.tar.gz", &tarball);
        let stager = AssetStager::new(s.cache.clone(), Some(s.objects.clone()), None, "local");
        let rec = record(&[("gvisor", "https://releases.example/gvisor.tar.gz", &tarball)]);
        let paths = stager
            .ensure(&Ctx::background(), &rec, &s.root.join("a"))
            .await
            .unwrap();
        assert!(paths["gvisor"].ends_with("gvisor/runsc"));
        assert_eq!(std::fs::read(&paths["gvisor"]).unwrap(), b"runsc");
        assert!(s.root.join("a/gvisor/gvisor-bin/helper").exists());
        assert!(!s.root.join("a/gvisor/.gvisor-release-download").exists());
    }
}

#[cfg(test)]
mod behavior_tests;

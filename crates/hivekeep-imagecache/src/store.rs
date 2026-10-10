//! The node-local layer pool and its pull path.
//!
//! On-disk layout under the cache root:
//!
//! ```text
//! version                              layout version marker
//! layers/sha256/<diffid-hex>/
//!     fs/                              the unpacked tree (an overlay lowerdir)
//!     whiteouts.json                   whiteout state recorded at unpack
//!     finalized                        written by the worker's finalize step
//!     size                             byte count recorded at unpack
//! layers/sha256/.tmp-*/                an unpack in flight (swept at startup)
//! layers/sha256/.rm-*/                 retired by eviction (swept at startup)
//! manifests/sha256/<digest-hex>.json   image config + ordered diffIDs; the
//!                                      file's mtime is the image's last use
//! ```
//!
//! A layer directory that exists is always complete: unpack streams into a
//! `.tmp-*` sibling and renames it into place. An image is nothing but a
//! record listing diffIDs in order, so a layer shared by N images exists
//! once.
//!
//! Pulls write the image record before unpacking anything, so every layer is
//! referenced (and safe from eviction) from before it exists on disk; a
//! record therefore means "known image, possibly partial", and an
//! interrupted pull's record is resumable progress.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use futures::StreamExt;
use hivekeep_node::context::Ctx;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use crate::backoff::{retry_backoff_for, Backoff};
use crate::digest::Digest;
use crate::error::{Error, ErrorKind, IoContext, Result, ResultContext};
use crate::keychain::Keychain;
use crate::layer::{Layer, RemoteLayer};
use crate::manifest::{ImageConfig, Platform};
use crate::metrics::{RequestMetrics, OUTCOME_HIT, OUTCOME_MISS};
use crate::reference::{is_local_registry, rewrite_local_registry, Pin, Reference};
use crate::registry::Session;
use crate::removeall::remove_all_writable;
use crate::singleflight::Flights;
use crate::spec::LAYER_FS_DIR_NAME;
use crate::unpack::LAYER_WHITEOUTS_FILE_NAME;

/// Version 1 layers may not have been checked against their diffID, so a
/// version 1 pool is refused.
pub const LAYOUT_VERSION: &str = "2";
/// The layout version marker's file name.
pub const VERSION_FILE_NAME: &str = "version";
/// The marker the worker writes once a layer's whiteouts exist.
pub const LAYER_FINALIZED_MARKER_NAME: &str = "finalized";
/// A layer's recorded byte count.
pub const LAYER_SIZE_FILE_NAME: &str = "size";
/// Prefix of an in-flight unpack directory.
pub const TEMP_PREFIX: &str = ".tmp-";

/// Default eviction minimum age.
pub const DEFAULT_MIN_AGE: Duration = Duration::from_secs(120);
/// Default bound on one pull.
pub const DEFAULT_PULL_TIMEOUT: Duration = Duration::from_secs(600);
/// Concurrent layer streams per pull; memory is O(stream buffers) per slot.
pub const LAYER_PULL_CONCURRENCY: usize = 4;

/// A cached, ready-to-compose image.
#[derive(Debug, Clone, PartialEq)]
pub struct Image {
    /// The manifest digest the reference resolved to (for a multi-platform
    /// reference, the index digest as requested).
    pub digest: Digest,
    /// The image's runtime config.
    pub config: ImageConfig,
    /// The cached layer directories, bottom-most first, each holding its
    /// tree under `fs/`.
    pub layer_dirs: Vec<PathBuf>,
}

/// The persisted form of a cached image.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ImageRecord {
    /// The record format, 1.
    pub version: u32,
    /// The image's runtime config.
    #[serde(default)]
    pub config: ImageConfig,
    /// The layers' diffIDs, bottom-most first.
    #[serde(rename = "diffIDs", default)]
    pub diff_ids: Vec<String>,
}

/// How a store is configured.
#[derive(Clone)]
pub struct StoreOptions {
    /// Consulted for every pull; it decides which registries it has
    /// credentials for.
    pub keychain: Option<Arc<dyn Keychain>>,
    /// Rewrites localhost/loopback registries to this endpoint.
    pub localhost_registry_replacement: String,
    /// The pull platform; `linux` on the host architecture by default.
    pub platform: Option<Platform>,
    /// The node's actors directory, scanned for bundle specs that root
    /// images against eviction. `None` disables the scan.
    pub actors_dir: Option<PathBuf>,
    /// Layers and records younger than this are never evicted, covering
    /// the window between a pull and the bundle spec that roots it.
    pub min_age: Duration,
    /// Bounds each pull, which runs detached from its callers.
    pub pull_timeout: Duration,
    /// Where lookups are counted; nothing is recorded without it.
    pub metrics: Option<RequestMetrics>,
    /// Replaces the per-registry retry backoff.
    pub backoff: Option<Backoff>,
}

impl Default for StoreOptions {
    fn default() -> Self {
        StoreOptions {
            keychain: None,
            localhost_registry_replacement: String::new(),
            platform: None,
            actors_dir: None,
            min_age: DEFAULT_MIN_AGE,
            pull_timeout: DEFAULT_PULL_TIMEOUT,
            metrics: None,
            backoff: None,
        }
    }
}

impl StoreOptions {
    /// Sets the keychain.
    pub fn with_keychain(mut self, k: Arc<dyn Keychain>) -> Self {
        self.keychain = Some(k);
        self
    }
    /// Sets the localhost registry replacement.
    pub fn with_localhost_registry_replacement(mut self, r: impl Into<String>) -> Self {
        self.localhost_registry_replacement = r.into();
        self
    }
    /// Sets the pull platform.
    pub fn with_platform(mut self, p: Platform) -> Self {
        self.platform = Some(p);
        self
    }
    /// Sets the actors directory.
    pub fn with_actors_dir(mut self, d: impl Into<PathBuf>) -> Self {
        self.actors_dir = Some(d.into());
        self
    }
    /// Sets the eviction minimum age.
    pub fn with_min_age(mut self, d: Duration) -> Self {
        self.min_age = d;
        self
    }
    /// Sets the per-pull timeout.
    pub fn with_pull_timeout(mut self, d: Duration) -> Self {
        self.pull_timeout = d;
        self
    }
    /// Sets the request counter.
    pub fn with_metrics(mut self, m: RequestMetrics) -> Self {
        self.metrics = Some(m);
        self
    }
    /// Sets one retry backoff for every registry.
    pub fn with_backoff(mut self, b: Backoff) -> Self {
        self.backoff = Some(b);
        self
    }
}

/// The store's shared state.
pub struct Inner {
    pub(crate) root: PathBuf,
    pub(crate) opts: StoreOptions,
    http: reqwest::Client,
    image_flights: Flights<Image>,
    pub(crate) layer_flights: Flights<()>,
    /// Serializes eviction passes.
    pub(crate) evict_mu: Mutex<()>,
    /// Held shared by the cache-hit path and exclusively by eviction's
    /// record removal, so a hit's last-use touch and eviction's final
    /// re-check never interleave.
    pub(crate) hit_mu: RwLock<()>,
    /// In-flight pulls using each layer, by diffID hex. Entries are removed
    /// at zero.
    pub(crate) pins: Mutex<HashMap<String, usize>>,
}

/// The keeper's handle to the on-disk layer pool. Cheap to clone; safe for
/// concurrent use. It assumes it is the only writer on the node.
#[derive(Clone)]
pub struct Store(Arc<Inner>);

impl std::ops::Deref for Store {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        &self.0
    }
}

/// Unpins its layers when dropped.
pub struct PinGuard {
    store: Store,
    hexes: Vec<String>,
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        let mut pins = self.store.pins.lock();
        for h in &self.hexes {
            if let Some(n) = pins.get_mut(h) {
                *n -= 1;
                if *n == 0 {
                    pins.remove(h);
                }
            }
        }
    }
}

fn create_temp_in(dir: &Path, prefix: &str) -> io::Result<tempfile::NamedTempFile> {
    tempfile::Builder::new().prefix(prefix).tempfile_in(dir)
}

impl Store {
    /// Opens (creating if needed) the pool at `root` and runs startup
    /// recovery: the layout version check, the sweep of temp and retired
    /// directories, and the one-time orphan scan.
    pub fn open(root: impl Into<PathBuf>, opts: StoreOptions) -> Result<Store> {
        let root = root.into();
        let store = Store(Arc::new(Inner {
            root,
            opts,
            http: crate::registry::http_client()?,
            image_flights: Flights::default(),
            layer_flights: Flights::default(),
            evict_mu: Mutex::new(()),
            hit_mu: RwLock::new(()),
            pins: Mutex::new(HashMap::new()),
        }));
        for d in [store.layers_dir(), store.manifests_dir()] {
            mkdir_all_private(&d).ctx(format!("while creating image cache dir {d:?}"))?;
        }
        let version_path = store.root.join(VERSION_FILE_NAME);
        match fs::read_to_string(&version_path) {
            Ok(v) if v.trim() == LAYOUT_VERSION => {}
            Ok(v) => {
                return Err(Error::msg(format!(
                    "image cache at {:?} has layout version {:?} and this keeper supports {LAYOUT_VERSION:?}, so delete it once no actor runs on this node",
                    store.root,
                    v.trim()
                )))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                write_private(&version_path, format!("{LAYOUT_VERSION}\n").as_bytes())
                    .ctx("while writing image cache version marker")?;
            }
            Err(e) => return Err(Error::io("while reading image cache version marker", e)),
        }
        store.sweep_temp_dirs()?;
        // Never fatal: a corrupt record must not keep the keeper from
        // serving actors.
        if let Err(err) = store.recover_orphans(&Ctx::background()) {
            if err.is(ErrorKind::IncompleteEnumeration) {
                tracing::error!(%err, "Image cache startup orphan scan skipped");
            } else {
                tracing::warn!(%err, "Image cache startup orphan recovery incomplete");
            }
        }
        Ok(store)
    }

    /// The cache root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The options the store runs with.
    pub fn options(&self) -> &StoreOptions {
        &self.opts
    }

    pub(crate) fn layers_dir(&self) -> PathBuf {
        self.root.join("layers").join("sha256")
    }

    pub(crate) fn manifests_dir(&self) -> PathBuf {
        self.root.join("manifests").join("sha256")
    }

    pub(crate) fn layer_dir(&self, diff_id: &Digest) -> PathBuf {
        self.root
            .join("layers")
            .join(&diff_id.algorithm)
            .join(&diff_id.hex)
    }

    pub(crate) fn record_path(&self, digest: &Digest) -> PathBuf {
        self.root
            .join("manifests")
            .join(&digest.algorithm)
            .join(format!("{}.json", digest.hex))
    }

    /// Removes unpack temp dirs, retired layer dirs and record temp files
    /// left by a crash. Everything else in the pool is complete.
    fn sweep_temp_dirs(&self) -> Result<()> {
        let mut swept = 0;
        for e in fs::read_dir(self.layers_dir()).ctx("while listing layer pool")? {
            let e = e.ctx("while listing layer pool")?;
            let name = e.file_name().to_string_lossy().into_owned();
            if !name.starts_with(TEMP_PREFIX) && !name.starts_with(crate::retire::RETIRED_PREFIX) {
                continue;
            }
            tracing::info!(dir = %name, "Image cache sweeping orphaned layer dir");
            remove_all_writable(&e.path())
                .ctx(format!("while sweeping orphaned layer dir {:?}", e.path()))?;
            swept += 1;
        }
        if swept > 0 {
            tracing::info!(
                count = swept,
                "Image cache startup sweep removed orphaned layer dirs"
            );
        }
        for e in fs::read_dir(self.manifests_dir()).ctx("while listing manifest records")? {
            let e = e.ctx("while listing manifest records")?;
            if e.file_name().to_string_lossy().starts_with('.') {
                fs::remove_file(e.path()).ctx(format!(
                    "while sweeping orphaned manifest temp file {:?}",
                    e.path()
                ))?;
            }
        }
        Ok(())
    }

    /// Makes `reference`'s image available in the pool. Digest references
    /// hit the cache with no network I/O; a tag costs one HEAD to resolve
    /// to a manifest digest, so tags are cacheable and a moved tag is
    /// picked up on the next call.
    pub async fn ensure_image(&self, ctx: &Ctx, reference: &str) -> Result<Image> {
        let mut outcome = OUTCOME_MISS;
        let res = self.ensure_image_inner(ctx, reference, &mut outcome).await;
        if let Some(m) = &self.opts.metrics {
            m.record(outcome, res.as_ref().err());
        }
        res
    }

    async fn ensure_image_inner(
        &self,
        ctx: &Ctx,
        reference: &str,
        outcome: &mut &'static str,
    ) -> Result<Image> {
        let parsed = self
            .parse_ref(reference)
            .context("while parsing reference")?;
        let digest = match &parsed.pin {
            Pin::Digest(d) => d.clone(),
            Pin::Tag(_) => self
                .with_credentials(ctx, &parsed, |s| {
                    Box::pin(async move { s.head_digest(ctx).await })
                })
                .await
                .context(format!("while resolving tag {reference:?} to a digest"))?,
        };

        if let Some(img) = self.cached_image_hit(&digest)? {
            *outcome = OUTCOME_HIT;
            tracing::info!(reference, %digest, "Image cache hit");
            return Ok(img);
        }
        tracing::info!(reference, %digest, "Image cache miss");

        // Concurrent pulls of one digest share a flight. Callers with very
        // different lifetimes share it, so the pull runs detached from
        // whichever started it, bounded only by the pull timeout: a waiter
        // only ever sees a real pull failure, never another caller's
        // cancellation.
        let store = self.clone();
        let (p, d) = (parsed.clone(), digest.clone());
        let flight = self
            .image_flights
            .run(&digest.to_string(), move || async move {
                let pull_ctx = Ctx::background().with_timeout(store.opts.pull_timeout);
                pull_ctx.run(store.pull(&pull_ctx, p, d)).await?
            });
        ctx.run(flight)
            .await
            .map_err(|e| Error::from(e).context(format!("while waiting for pull of {digest}")))?
    }

    /// The cache-hit side of the hit/evict contract: verifies the cached
    /// image and records last use, atomically with respect to eviction's
    /// record removal. The touch also renews the min-age veto, so an image
    /// in use cannot age into eviction before the worker mounts it.
    pub(crate) fn cached_image_hit(&self, digest: &Digest) -> Result<Option<Image>> {
        let _g = self.hit_mu.read();
        let img = self.cached_image(digest)?;
        if img.is_some() {
            self.touch_record(digest);
        }
        Ok(img)
    }

    /// The cached image for `digest`, or `None` if the record or any layer
    /// is missing (the caller re-pulls; only missing layers cost anything).
    pub(crate) fn cached_image(&self, digest: &Digest) -> Result<Option<Image>> {
        let b = match fs::read(self.record_path(digest)) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(Error::io(
                    format!("while reading image record for {digest}"),
                    e,
                ))
            }
        };
        let rec: ImageRecord = serde_json::from_slice(&b)
            .map_err(|e| Error::msg(format!("while decoding image record for {digest}: {e}")))?;
        let mut layer_dirs = Vec::with_capacity(rec.diff_ids.len());
        for d in &rec.diff_ids {
            let diff_id = Digest::parse(d)
                .context(format!("invalid diffID {d:?} in image record for {digest}"))?;
            let dir = self.layer_dir(&diff_id);
            if fs::metadata(dir.join(LAYER_FS_DIR_NAME)).is_err() {
                return Ok(None);
            }
            layer_dirs.push(dir);
        }
        Ok(Some(Image {
            digest: digest.clone(),
            config: rec.config,
            layer_dirs,
        }))
    }

    /// Pulls by resolved digest, so what is unpacked is exactly what was
    /// recorded.
    async fn pull(&self, ctx: &Ctx, reference: Reference, digest: Digest) -> Result<Image> {
        // A racing pull may have finished between our miss and this flight.
        if let Some(img) = self.cached_image_hit(&digest)? {
            return Ok(img);
        }
        // Streams are aborted once the pull's context ends, deadline
        // included, so no blocking unpack outlives it.
        let streams = ctx.child();
        let watchdog = {
            let (ctx, streams) = (ctx.clone(), streams.clone());
            crate::spawn::task(async move {
                ctx.done().await;
                streams.cancel();
            })
        };
        // Retried whole per credential: finished layers are reused.
        let r = &reference;
        let d = &digest;
        let s = &streams;
        let res = self
            .with_credentials(ctx, &reference, |session| {
                Box::pin(async move { self.pull_with(s, session, r, d).await })
            })
            .await;
        watchdog.abort();
        res
    }

    async fn pull_with(
        &self,
        ctx: &Ctx,
        session: Arc<Session>,
        reference: &Reference,
        digest: &Digest,
    ) -> Result<Image> {
        let started = Instant::now();
        let platform = self
            .opts
            .platform
            .clone()
            .unwrap_or_else(Platform::host_linux);
        let resolved = session
            .image(ctx, digest, &platform)
            .await
            .context("while fetching image manifest")?;
        let diff_ids = resolved.config.rootfs.diff_ids.clone();
        let layers = &resolved.manifest.layers;
        if diff_ids.len() != layers.len() {
            return Err(Error::msg(format!(
                "image {digest} config lists {} diffIDs but manifest has {} layers",
                diff_ids.len(),
                layers.len()
            )));
        }
        // Pinned until this pull returns: nothing it unpacked or reused can
        // be retired from under it, whatever happens to its record.
        let _pins = self.pin_layers(diff_ids.iter().map(|d| d.hex.clone()).collect());

        // Every layer of this pull is referenced before it can exist.
        let rec = ImageRecord {
            version: 1,
            config: resolved.config.config.clone(),
            diff_ids: diff_ids.iter().map(|d| d.to_string()).collect(),
        };
        self.write_record(digest, &rec)?;
        // For a multi-platform reference the layers belong to the platform
        // child: record the image under its digest too, so references
        // pinned either way hit.
        let twin = (&resolved.manifest_digest != digest).then(|| resolved.manifest_digest.clone());
        if let Some(t) = &twin {
            if let Err(err) = self.write_record(t, &rec) {
                tracing::warn!(digest = %t, %err, "Failed to record image under platform manifest digest");
            }
        }

        let group = ctx.child();
        let pairs: Vec<(crate::manifest::Descriptor, Digest)> = layers
            .iter()
            .cloned()
            .zip(diff_ids.iter().cloned())
            .collect();
        let group_ref = &group;
        let jobs = pairs.into_iter().map(|(desc, diff_id)| {
            let layer: Arc<dyn Layer> = Arc::new(RemoteLayer::new(
                session.clone(),
                desc,
                group_ref.token().clone(),
            ));
            let group = group_ref;
            async move {
                let diff_id = &diff_id;
                let res = self.ensure_layer(group, diff_id, layer).await;
                match res {
                    Ok(dir) => {
                        // Progress keeps the record fresh for as long as the
                        // pull advances.
                        self.touch_record(digest);
                        Ok(dir)
                    }
                    Err(err) => {
                        if err.is(ErrorKind::DiffIdMismatch) {
                            tracing::warn!(reference = %reference, %digest, diffid = %diff_id, %err,
                                "Image layer does not match its claimed diffID");
                        }
                        group.cancel();
                        Err(err.context(format!("while unpacking layer {diff_id}")))
                    }
                }
            }
        });
        let results: Vec<Result<PathBuf>> = futures::stream::iter(jobs)
            .buffered(LAYER_PULL_CONCURRENCY)
            .collect()
            .await;
        // The first failure in layer order, preferring one that is not a
        // cancellation caused by it.
        let mut layer_dirs = Vec::with_capacity(results.len());
        let mut first_err: Option<Error> = None;
        for r in results {
            match r {
                Ok(d) => layer_dirs.push(d),
                Err(e) => {
                    let replace = match &first_err {
                        None => true,
                        Some(f) => f.is(ErrorKind::Canceled) && !e.is(ErrorKind::Canceled),
                    };
                    if replace {
                        first_err = Some(e);
                    }
                }
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }

        // Eviction may legitimately remove the record mid-pull (a pull with
        // no progress for min-age reads as wedged), and the pins lapse when
        // this returns.
        self.write_record(digest, &rec)
            .context("while rewriting image record after unpack")?;
        if let Some(t) = &twin {
            if let Err(err) = self.write_record(t, &rec) {
                tracing::warn!(digest = %t, %err, "Failed to rewrite platform manifest record after unpack");
            }
        }
        // Never return layer dirs that are not on disk right now.
        for dir in &layer_dirs {
            if let Err(e) = fs::metadata(dir.join(LAYER_FS_DIR_NAME)) {
                return Err(Error::io("layer dir vanished during pull (evicted?)", e));
            }
        }
        tracing::info!(%digest, layers = layer_dirs.len(), took_ms = started.elapsed().as_millis() as u64,
            "Image pulled into layer cache");
        Ok(Image {
            digest: digest.clone(),
            config: resolved.config.config,
            layer_dirs,
        })
    }

    /// Pins layers for an in-flight pull; the guard unpins. Taken for all
    /// of a pull's layers before any layer flight, so a pull that joins
    /// another's flight is protected by its own pin once the leader's
    /// lapses.
    pub fn pin_layers(&self, hexes: Vec<String>) -> PinGuard {
        let mut pins = self.pins.lock();
        for h in &hexes {
            *pins.entry(h.clone()).or_insert(0) += 1;
        }
        PinGuard {
            store: self.clone(),
            hexes,
        }
    }

    /// Whether an in-flight pull holds the layer.
    pub fn pinned(&self, hex: &str) -> bool {
        self.pins.lock().get(hex).is_some_and(|n| *n > 0)
    }

    /// Makes `diff_id`'s tree present in the pool, collapsing concurrent
    /// requests for the same blob. The caller must hold a pin on the layer:
    /// the pin, not the flight, keeps eviction away.
    pub(crate) async fn ensure_layer(
        &self,
        ctx: &Ctx,
        diff_id: &Digest,
        layer: Arc<dyn Layer>,
    ) -> Result<PathBuf> {
        let dir = self.layer_dir(diff_id);
        let key = layer_flight_key(diff_id, &layer.digest());
        let (store, ctx, diff_id, d) = (self.clone(), ctx.clone(), diff_id.clone(), dir.clone());
        self.layer_flights
            .run(&key, move || async move {
                if fs::metadata(d.join(LAYER_FS_DIR_NAME)).is_ok() {
                    // Refresh for eviction's LRU order; the pull's pin is
                    // what protects it.
                    if let Err(err) = filetime::set_file_times(&d, now(), now()) {
                        tracing::warn!(diffid = %diff_id, %err, "Failed to refresh layer mtime on reuse");
                    }
                    return Ok(());
                }
                store.unpack_layer_to_pool(&ctx, &diff_id, layer.as_ref()).await
            })
            .await?;
        Ok(dir)
    }

    /// Streams the layer (download, decompress, untar) into a temp dir and
    /// renames it into place, so a layer dir exists complete or not at all.
    pub(crate) async fn unpack_layer_to_pool(
        &self,
        ctx: &Ctx,
        diff_id: &Digest,
        layer: &dyn Layer,
    ) -> Result<()> {
        let parent = self.layers_dir();
        let tmp = tempfile::Builder::new()
            .prefix(&format!(
                "{TEMP_PREFIX}{}-",
                &diff_id.hex[..12.min(diff_id.hex.len())]
            ))
            .tempdir_in(&parent)
            .ctx("while creating layer temp dir")?;
        let tmp_path = tmp.path().to_path_buf();
        // The tempdir guard removes plain trees; read-only ones need the
        // writable removal, so do that ourselves and disarm the guard.
        let tmp_path_guard = scopeguard(tmp_path.clone());
        let _ = tmp.keep();

        let fs_dir = tmp_path.join(LAYER_FS_DIR_NAME);
        mkdir_mode(&fs_dir, 0o755).ctx("while creating layer fs dir")?;
        let root = crate::confined::open_root(&fs_dir).ctx("while opening layer fs dir")?;

        let wh = Arc::new(Mutex::new(None));
        let wh2 = wh.clone();
        // The tar bytes the unpacker read are the recorded size: cheap to
        // capture here, and close to disk usage.
        let size = crate::verify::read_verified_layer(ctx, layer, diff_id, move |r| {
            *wh2.lock() = Some(crate::unpack::unpack_layer(r, &root)?);
            Ok(())
        })
        .await?;
        let wh = wh.lock().take().unwrap_or_default();

        // Non-fatal: a missing size file is backfilled lazily.
        if let Err(err) = write_private(
            &tmp_path.join(LAYER_SIZE_FILE_NAME),
            format!("{size}\n").as_bytes(),
        ) {
            tracing::warn!(diffid = %diff_id, %err, "Failed to record layer size; will backfill lazily");
        }
        let meta = serde_json::to_vec(&wh)
            .map_err(|e| Error::msg(format!("while encoding layer whiteouts: {e}")))?;
        write_private(&tmp_path.join(LAYER_WHITEOUTS_FILE_NAME), &meta)
            .ctx("while writing layer whiteouts")?;

        let dst = self.layer_dir(diff_id);
        if let Err(e) = fs::rename(&tmp_path, &dst) {
            // A concurrent unpack of the same diffID may have won; its
            // layer is as good as ours.
            if fs::metadata(dst.join(LAYER_FS_DIR_NAME)).is_ok() {
                return Ok(());
            }
            return Err(Error::io("while moving layer into pool", e));
        }
        tmp_path_guard.disarm();
        Ok(())
    }

    /// Writes `rec` under `digest` atomically.
    pub(crate) fn write_record(&self, digest: &Digest, rec: &ImageRecord) -> Result<()> {
        let b = serde_json::to_vec_pretty(rec)
            .map_err(|e| Error::msg(format!("while encoding image record: {e}")))?;
        self.write_record_bytes(digest, &b)
    }

    /// Writes raw record bytes under `digest` atomically.
    pub(crate) fn write_record_bytes(&self, digest: &Digest, b: &[u8]) -> Result<()> {
        let path = self.record_path(digest);
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let mut tmp = create_temp_in(&self.manifests_dir(), &format!(".{name}.tmp-"))
            .ctx("while creating image record temp file")?;
        tmp.write_all(b).ctx("while writing image record")?;
        tmp.persist(&path)
            .map_err(|e| Error::io("while moving image record into place", e.error))?;
        Ok(())
    }

    fn parse_ref(&self, reference: &str) -> Result<Reference> {
        let replacement = &self.opts.localhost_registry_replacement;
        let mut rewritten = false;
        let mut r = reference.to_string();
        if !replacement.is_empty() {
            let n = rewrite_local_registry(&r, replacement);
            if n != r {
                r = n;
                rewritten = true;
            }
        }
        let insecure = rewritten || is_local_registry(&r);
        Reference::parse(&r, insecure).map_err(|e| Error::new(ErrorKind::InvalidReference, e))
    }

    fn backoff_for(&self, reference: &Reference) -> Backoff {
        self.opts
            .backoff
            .unwrap_or_else(|| retry_backoff_for(&reference.registry))
    }

    /// Runs `fetch` with each candidate credential until one succeeds, or
    /// once anonymously when there are none.
    async fn with_credentials<'a, T, F>(
        &'a self,
        ctx: &Ctx,
        reference: &Reference,
        fetch: F,
    ) -> Result<T>
    where
        F: Fn(Arc<Session>) -> futures::future::BoxFuture<'a, Result<T>>,
    {
        let creds = match &self.opts.keychain {
            None => Vec::new(),
            Some(k) => k
                .candidates(ctx, &reference.repository_name())
                .await
                .context("while resolving pull credentials")?,
        };
        let backoff = self.backoff_for(reference);
        let session = |c| {
            Arc::new(Session::new(
                self.http.clone(),
                reference.clone(),
                c,
                backoff,
            ))
        };
        if creds.is_empty() {
            return fetch(session(None)).await;
        }
        let mut errs = Vec::new();
        for c in creds {
            match fetch(session(Some(c))).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    // Another credential cannot change the image's content.
                    let stop = ctx.is_done() || e.is(ErrorKind::DiffIdMismatch);
                    errs.push(e);
                    if stop {
                        break;
                    }
                }
            }
        }
        Err(Error::join(errs).unwrap_or_else(|| Error::msg("no credential succeeded")))
    }
}

/// Keys a layer flight by blob digest as well as diffID: a caller joining a
/// flight gets the leader's result, so an image lying about a common
/// layer's diffID leads its own flight and fails only its own pull.
pub(crate) fn layer_flight_key(diff_id: &Digest, blob: &Digest) -> String {
    format!("{diff_id}@{blob}")
}

pub(crate) fn now() -> filetime::FileTime {
    filetime::FileTime::from_system_time(SystemTime::now())
}

/// Removes a temp tree on drop unless disarmed.
struct TempTree(Option<PathBuf>);

fn scopeguard(p: PathBuf) -> TempTree {
    TempTree(Some(p))
}

impl TempTree {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for TempTree {
    fn drop(&mut self) {
        if let Some(p) = self.0.take() {
            if p.exists() {
                if let Err(err) = remove_all_writable(&p) {
                    tracing::warn!(dir = %p.display(), %err, "Failed to clean up layer temp dir");
                }
            }
        }
    }
}

fn mkdir_all_private(d: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new().recursive(true).mode(0o700).create(d)
}

fn mkdir_mode(d: &Path, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new().mode(mode).create(d)
}

pub(crate) fn write_private(path: &Path, data: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(data)
}

#[cfg(test)]
#[path = "store_tests.rs"]
pub(crate) mod tests;

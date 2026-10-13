//! A node-local disk cache of downloaded artifacts, keyed by identity.
//!
//! An artifact wanted by many concurrent callers is fetched once, published
//! into the cache atomically, and evicted under byte-budget pressure without
//! breaking anyone holding it.
//!
//! Layout under a store's root, which the store owns:
//!
//! ```text
//! entries/<sha256(key)>/
//!   data       the cached file, or directory tree
//!   meta.json  canonical key and creation time, for operators only
//! tmp/         in-flight fetches, on the entries' filesystem
//! .rm-*        retired entries awaiting removal
//! ```
//!
//! An entry directory's mtime is its last-use clock. Everything under
//! `entries/` is complete; [`Store::sweep_debris`] clears crash leftovers
//! from the other two places at startup.
//!
//! Eviction never takes what a caller holds:
//! - [`Store::get_file_to`] serves a hard link made under the same lock that
//!   resolves the hit; an entry with a consumer link is never evicted.
//! - [`Store::get_dir_to`] serves a directory the consumer owns whose files
//!   are hard links; the entry counts as linked while any file is.
//! - [`Store::get_file_copy_to`] serves a private copy read from a held-open
//!   handle, so a concurrent eviction cannot corrupt it.
//! - Entries younger than the store's min age are never evicted, covering
//!   the window between publication and a consumer's first link.
//!
//! Files are published read-only (keeping execute bits), so a consumer
//! writing through a link fails instead of corrupting the shared bytes.

mod evict;
mod get;
mod key;

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use parking_lot::{Mutex, RwLock};
use tokio::sync::watch;

pub use evict::{EvictStats, SweepStats};
pub use key::{Key, KeyError};

use crate::atomicfile::create_dir_all_mode;
use crate::context::{Ctx, CtxError};

const ENTRIES_DIR: &str = "entries";
const TMP_DIR: &str = "tmp";
const RM_PREFIX: &str = ".rm-";
const DATA_NAME: &str = "data";
const META_NAME: &str = "meta.json";

/// The default eviction min age.
pub const DEFAULT_MIN_AGE: Duration = Duration::from_secs(10 * 60);
/// The default bound on one fetch.
pub const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Fills a fetch destination: a file the fetcher creates for a file entry,
/// or an empty directory the store made for a tree entry. It runs at most
/// once per key across concurrent callers, detached from their contexts
/// and bounded by the store's fetch timeout.
pub type Fetcher =
    Box<dyn FnOnce(Ctx, PathBuf) -> BoxFuture<'static, Result<(), tonic::Status>> + Send>;

/// Wraps an async closure as a [`Fetcher`].
pub fn fetcher<F, Fut>(f: F) -> Fetcher
where
    F: FnOnce(Ctx, PathBuf) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), tonic::Status>> + Send + 'static,
{
    Box::new(move |ctx, path| Box::pin(f(ctx, path)))
}

/// Why a cache call failed.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// The fetcher, or the publication that follows it, failed. The
    /// fetcher's own code is kept.
    #[error("while fetching {key}: {status}")]
    Fetch {
        /// The key fetched.
        key: String,
        /// The failure.
        status: tonic::Status,
    },
    /// The destination exists already.
    #[error("destination {0} already exists")]
    Exists(PathBuf),
    /// The destination is on another filesystem than the cache, so a hard
    /// link cannot reach it.
    #[error("destination {0} is not on the cache's filesystem (link-out requires one filesystem)")]
    CrossDevice(PathBuf),
    /// A local filesystem failure.
    #[error("{what}: {source}")]
    Io {
        /// What the store was doing.
        what: String,
        /// The failure.
        #[source]
        source: io::Error,
    },
    /// The caller's arguments are unusable.
    #[error("filecache: {0}")]
    Invalid(String),
    /// The caller's context ended while it waited.
    #[error(transparent)]
    Canceled(#[from] CtxError),
}

impl CacheError {
    fn io(what: impl Into<String>, source: io::Error) -> Self {
        CacheError::Io {
            what: what.into(),
            source,
        }
    }

    /// Whether the destination already existed.
    pub fn is_exists(&self) -> bool {
        matches!(self, CacheError::Exists(_))
    }

    /// Whether the destination was on another filesystem.
    pub fn is_cross_device(&self) -> bool {
        matches!(self, CacheError::CrossDevice(_))
    }
}

impl From<CacheError> for tonic::Status {
    fn from(e: CacheError) -> Self {
        match e {
            CacheError::Fetch { key, status } => tonic::Status::new(
                status.code(),
                format!("while fetching {key}: {}", status.message()),
            ),
            CacheError::Exists(p) => {
                tonic::Status::already_exists(format!("destination {} already exists", p.display()))
            }
            CacheError::Canceled(c) => c.into(),
            CacheError::Invalid(m) => tonic::Status::invalid_argument(format!("filecache: {m}")),
            other => tonic::Status::internal(other.to_string()),
        }
    }
}

/// What a running flight on a key is doing.
#[derive(Clone)]
enum Flight {
    /// A fetch; waiters get its result.
    Fetch(watch::Receiver<Option<Result<(), tonic::Status>>>),
    /// An eviction retiring the entry.
    Retire(watch::Receiver<bool>),
}

pub(crate) struct Inner {
    root: PathBuf,
    min_age: Duration,
    fetch_timeout: Duration,
    flights: Mutex<HashMap<String, Flight>>,
    /// Held shared while serving a hit, exclusively by eviction's final
    /// re-check and retire, so an entry cannot vanish mid-hit.
    hit_mu: RwLock<()>,
    /// Serializes eviction passes.
    evict_mu: Mutex<()>,
}

/// One on-disk cache. Safe for concurrent use; assumes it is the only
/// writer under its root.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}

/// Options for [`Store::open`].
#[derive(Debug, Clone, Copy)]
pub struct Options {
    /// Entries younger than this are never evicted.
    pub min_age: Duration,
    /// Bound on each fetch.
    pub fetch_timeout: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            min_age: DEFAULT_MIN_AGE,
            fetch_timeout: DEFAULT_FETCH_TIMEOUT,
        }
    }
}

impl Store {
    /// Opens, creating if needed, the store rooted at `root`.
    pub fn open(root: impl Into<PathBuf>, opts: Options) -> Result<Store, CacheError> {
        let root = root.into();
        let inner = Inner {
            root,
            min_age: opts.min_age,
            fetch_timeout: opts.fetch_timeout,
            flights: Mutex::new(HashMap::new()),
            hit_mu: RwLock::new(()),
            evict_mu: Mutex::new(()),
        };
        // Entries are world-readable, since consumers read them through
        // links; tmp holds unpublished fetches and stays private.
        create_dir_all_mode(&inner.entries_dir(), 0o755)
            .map_err(|e| CacheError::io("while creating entries dir", e))?;
        create_dir_all_mode(&inner.tmp_dir(), 0o700)
            .map_err(|e| CacheError::io("while creating tmp dir", e))?;
        Ok(Store {
            inner: Arc::new(inner),
        })
    }

    /// The store's root.
    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    /// The eviction min age.
    pub fn min_age(&self) -> Duration {
        self.inner.min_age
    }

    /// The bound on one fetch.
    pub fn fetch_timeout(&self) -> Duration {
        self.inner.fetch_timeout
    }

    #[cfg(test)]
    pub(crate) fn entry_dir_for(&self, key: &Key) -> PathBuf {
        self.inner.entry_dir(key)
    }
}

impl Inner {
    fn entries_dir(&self) -> PathBuf {
        self.root.join(ENTRIES_DIR)
    }

    fn tmp_dir(&self) -> PathBuf {
        self.root.join(TMP_DIR)
    }

    fn entry_dir(&self, key: &Key) -> PathBuf {
        self.entries_dir().join(key.dir())
    }

    fn data_path(&self, key: &Key) -> PathBuf {
        self.entry_dir(key).join(DATA_NAME)
    }
}

/// The mode a fetched file is published with: read-only for everyone,
/// keeping the fetcher's execute bits so a cached binary stays runnable.
fn read_only_mode(perm: u32) -> u32 {
    0o444 | (perm & 0o111)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct EntryMeta {
    key: String,
    #[serde(rename = "createdAt")]
    created_at: chrono::DateTime<chrono::Utc>,
}

fn write_entry_meta(entry_dir: &Path, key: &Key) -> Result<(), CacheError> {
    let data = serde_json::to_vec(&EntryMeta {
        key: key.canonical().to_string(),
        created_at: chrono::Utc::now(),
    })
    .map_err(|e| CacheError::io("while marshaling entry meta", io::Error::other(e)))?;
    std::fs::write(entry_dir.join(META_NAME), data)
        .map_err(|e| CacheError::io("while writing entry meta", e))
}

/// Sets an entry directory's last-use clock to now. Best-effort.
fn touch(dir: &Path) {
    if let Ok(f) = std::fs::File::open(dir) {
        let now = std::time::SystemTime::now();
        let _ = f.set_times(
            std::fs::FileTimes::new()
                .set_accessed(now)
                .set_modified(now),
        );
    }
}

/// Whether an I/O error is EXDEV.
fn is_exdev(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::CrossesDevices || e.raw_os_error() == Some(libc::EXDEV)
}

#[cfg(test)]
mod behavior_tests;
#[cfg(test)]
mod tests;

//! Pools loaded from a state file (typically a mounted Secret) and reloaded
//! periodically, so key rotation needs no restart.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::ca::{CaPool, CertificateAuthority, LeafSpec};
use crate::error::{IdentityError, Result};
use crate::key::PublicKey;

/// How long a loaded state file is used before it is read again.
pub const RELOAD_PERIOD: Duration = Duration::from_secs(60);

/// A pool that can be read from a state file.
pub trait StateFile: Sized + Send + Sync + 'static {
    /// Parses the file's contents.
    fn parse(bytes: &[u8]) -> Result<Self>;
}

impl StateFile for CaPool {
    fn parse(bytes: &[u8]) -> Result<Self> {
        CaPool::from_json(bytes)
    }
}

/// A `T` read from `path`, re-read once [`RELOAD_PERIOD`] has passed.
/// A failed re-read is an error rather than a silent fallback, so a broken
/// rotation is noticed.
#[derive(Debug)]
pub struct Refreshing<T> {
    path: PathBuf,
    period: Duration,
    loaded: Mutex<(Instant, Arc<T>)>,
}

impl<T: StateFile> Refreshing<T> {
    /// Loads `path` now; fails if it cannot be read or parsed.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_period(path, RELOAD_PERIOD)
    }

    /// Like [`open`](Self::open) with a custom reload period.
    pub fn with_period(path: impl AsRef<Path>, period: Duration) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let value = Arc::new(load(&path)?);
        Ok(Refreshing {
            path,
            period,
            loaded: Mutex::new((Instant::now(), value)),
        })
    }

    /// The current pool, reloading it first if it is due.
    pub fn current(&self) -> Result<Arc<T>> {
        let mut loaded = self.loaded.lock();
        if loaded.0.elapsed() >= self.period {
            *loaded = (Instant::now(), Arc::new(load(&self.path)?));
        }
        Ok(loaded.1.clone())
    }
}

fn load<T: StateFile>(path: &Path) -> Result<T> {
    let bytes = std::fs::read(path).map_err(|source| IdentityError::Io {
        path: path.display().to_string(),
        source,
    })?;
    T::parse(&bytes)
}

/// A [`CaPool`] backed by its state file.
pub type RefreshingCaPool = Refreshing<CaPool>;

impl CertificateAuthority for Refreshing<CaPool> {
    fn sign(&self, leaf: &LeafSpec, subject: &PublicKey) -> Result<Vec<Vec<u8>>> {
        self.current()?.sign(leaf, subject)
    }

    fn trust_anchors(&self) -> Result<Vec<Vec<u8>>> {
        self.current()?.trust_anchors()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ca::Ca;
    use crate::key::KeyAlgorithm;

    #[test]
    fn reloads_after_the_period() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pool.json");
        let a = Ca::generate("a", KeyAlgorithm::EcdsaP256, Duration::from_secs(3600)).unwrap();
        let b = Ca::generate("b", KeyAlgorithm::EcdsaP256, Duration::from_secs(3600)).unwrap();
        std::fs::write(&path, CaPool::new(vec![a.clone()], "a").to_json().unwrap()).unwrap();

        let pool = RefreshingCaPool::with_period(&path, Duration::ZERO).unwrap();
        assert_eq!(pool.trust_anchors().unwrap().len(), 1);
        std::fs::write(&path, CaPool::new(vec![a, b], "b").to_json().unwrap()).unwrap();
        assert_eq!(pool.trust_anchors().unwrap().len(), 2);
        assert_eq!(pool.current().unwrap().active().unwrap().id(), "b");

        std::fs::write(&path, b"garbage").unwrap();
        assert!(pool.current().is_err());
        assert!(RefreshingCaPool::open(dir.path().join("missing")).is_err());
    }

    #[test]
    fn caches_within_the_period() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pool.json");
        let a = Ca::generate("a", KeyAlgorithm::EcdsaP256, Duration::from_secs(3600)).unwrap();
        std::fs::write(&path, CaPool::new(vec![a], "a").to_json().unwrap()).unwrap();
        let pool = RefreshingCaPool::open(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert!(pool.current().is_ok(), "re-read before the period elapsed");
    }
}

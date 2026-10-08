//! Distributed leases.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

type Release = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// A held lease, renewed by its backend until it is closed.
///
/// If renewal cannot keep the lease alive, [`lost`](Self::lost) completes so
/// the holder can stop work it may no longer have exclusive access to.
pub struct Lease {
    key: String,
    held: CancellationToken,
    release: Mutex<Option<Release>>,
}

impl fmt::Debug for Lease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lease")
            .field("key", &self.key)
            .field("held", &self.is_held())
            .finish()
    }
}

impl Lease {
    /// A lease on `key`. `held` is cancelled when the lease is lost or
    /// closed; `release` gives it up and runs at most once, after `held` is
    /// cancelled.
    pub fn new(
        key: impl Into<String>,
        held: CancellationToken,
        release: impl Future<Output = ()> + Send + 'static,
    ) -> Self {
        Lease {
            key: key.into(),
            held,
            release: Mutex::new(Some(Box::pin(release))),
        }
    }

    /// The leased key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Whether the lease is still held.
    pub fn is_held(&self) -> bool {
        !self.held.is_cancelled()
    }

    /// A token cancelled when the lease is lost or closed, for wiring into
    /// work that must stop with it.
    pub fn token(&self) -> CancellationToken {
        self.held.clone()
    }

    /// Completes when the lease is lost or closed.
    pub async fn lost(&self) {
        self.held.cancelled().await
    }

    /// Stops renewal and releases the lease. Safe to call repeatedly.
    pub async fn close(&self) {
        self.held.cancel();
        let release = self.release.lock().take();
        if let Some(release) = release {
            release.await;
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.held.cancel();
        if let Some(release) = self.release.get_mut().take() {
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                rt.spawn(release);
            }
        }
    }
}

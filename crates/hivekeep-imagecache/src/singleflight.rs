//! Collapsing concurrent identical work.
//!
//! The first caller for a key starts the work as a detached task; callers
//! arriving while it runs join it and receive the same result. Because the
//! work is a task of its own, it runs to completion even if every caller
//! stops waiting, so an abandoned pull still warms the cache for the next
//! attempt.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::future::{BoxFuture, FutureExt, Shared};
use parking_lot::Mutex;

use crate::error::{Error, Result};

type Flight<T> = Shared<BoxFuture<'static, Result<T>>>;
type FlightMap<T> = Arc<Mutex<HashMap<String, (u64, Flight<T>)>>>;

/// A set of in-flight operations keyed by string.
pub struct Flights<T: Clone + Send + Sync + 'static> {
    map: FlightMap<T>,
    next: AtomicU64,
}

impl<T: Clone + Send + Sync + 'static> Default for Flights<T> {
    fn default() -> Self {
        Flights {
            map: Arc::default(),
            next: AtomicU64::new(0),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> Flights<T> {
    /// Joins the flight for `key`, or starts `work` as one. The returned
    /// future may be dropped without affecting the work.
    pub fn run<F, Fut>(&self, key: &str, work: F) -> Flight<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let mut map = self.map.lock();
        if let Some((_, f)) = map.get(key) {
            return f.clone();
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let fut = work();
        let (map_ref, key_owned) = (self.map.clone(), key.to_string());
        let handle = crate::spawn::task(async move {
            let out = fut.await;
            let mut map = map_ref.lock();
            if map.get(&key_owned).is_some_and(|(i, _)| *i == id) {
                map.remove(&key_owned);
            }
            out
        });
        let flight: Flight<T> = async move {
            handle
                .await
                .unwrap_or_else(|e| Err(Error::msg(format!("flight task failed: {e}"))))
        }
        .boxed()
        .shared();
        map.insert(key.to_string(), (id, flight.clone()));
        flight
    }

    /// Whether a flight for `key` is running.
    pub fn in_flight(&self, key: &str) -> bool {
        self.map.lock().contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn joiners_share_one_run() {
        let flights: Flights<usize> = Flights::default();
        let runs = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let r = runs.clone();
        let a = flights.run("k", move || async move {
            r.fetch_add(1, Ordering::SeqCst);
            let _ = rx.await;
            Ok(7)
        });
        let b = flights.run("k", || async { Ok(9) });
        assert!(flights.in_flight("k"));
        tx.send(()).unwrap();
        assert_eq!(a.await.unwrap(), 7);
        assert_eq!(b.await.unwrap(), 7);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        tokio::task::yield_now().await;
        assert!(!flights.in_flight("k"));
        assert_eq!(flights.run("k", || async { Ok(9) }).await.unwrap(), 9);
    }

    #[tokio::test]
    async fn abandoned_flight_still_completes() {
        let flights: Flights<()> = Flights::default();
        let done = Arc::new(AtomicUsize::new(0));
        let d = done.clone();
        drop(flights.run("k", move || async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            d.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(done.load(Ordering::SeqCst), 1);
    }
}

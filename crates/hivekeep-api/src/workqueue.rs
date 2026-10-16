//! A deduplicating work queue with per-item exponential retry backoff, for
//! the reconcilers.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const BASE_DELAY: Duration = Duration::from_millis(5);
const MAX_DELAY: Duration = Duration::from_secs(1000);

struct Shared<K> {
    queued: Mutex<HashSet<K>>,
    failures: Mutex<HashMap<K, u32>>,
    tx: mpsc::UnboundedSender<K>,
    stop: CancellationToken,
}

/// The queue. Cheap to clone.
pub struct WorkQueue<K> {
    shared: Arc<Shared<K>>,
    rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<K>>>,
}

impl<K> Clone for WorkQueue<K> {
    fn clone(&self) -> Self {
        WorkQueue {
            shared: self.shared.clone(),
            rx: self.rx.clone(),
        }
    }
}

impl<K: Clone + Eq + Hash + Send + Sync + 'static> WorkQueue<K> {
    /// A queue that stops handing out work when `stop` is cancelled.
    pub fn new(stop: CancellationToken) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        WorkQueue {
            shared: Arc::new(Shared {
                queued: Mutex::new(HashSet::new()),
                failures: Mutex::new(HashMap::new()),
                tx,
                stop,
            }),
            rx: Arc::new(tokio::sync::Mutex::new(rx)),
        }
    }

    /// Queues `k` unless it is already queued.
    pub fn add(&self, k: K) {
        if self.shared.queued.lock().insert(k.clone()) {
            let _ = self.shared.tx.send(k);
        }
    }

    /// Queues `k` after `delay`.
    pub fn add_after(&self, k: K, delay: Duration) {
        let me = self.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = me.shared.stop.cancelled() => {}
                _ = tokio::time::sleep(delay) => me.add(k),
            }
        });
    }

    /// Queues `k` after its next backoff delay.
    pub fn add_rate_limited(&self, k: K) {
        let n = {
            let mut f = self.shared.failures.lock();
            let n = f.entry(k.clone()).or_insert(0);
            *n += 1;
            *n
        };
        let delay = BASE_DELAY
            .checked_mul(1u32 << n.min(31).saturating_sub(1))
            .unwrap_or(MAX_DELAY)
            .min(MAX_DELAY);
        self.add_after(k, delay);
    }

    /// Resets `k`'s backoff.
    pub fn forget(&self, k: &K) {
        self.shared.failures.lock().remove(k);
    }

    /// The next item, or `None` once stopped.
    pub async fn get(&self) -> Option<K> {
        let mut rx = self.rx.lock().await;
        tokio::select! {
            _ = self.shared.stop.cancelled() => None,
            k = rx.recv() => {
                let k = k?;
                self.shared.queued.lock().remove(&k);
                Some(k)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dedups_and_retries() {
        let stop = CancellationToken::new();
        let q = WorkQueue::new(stop.clone());
        q.add("a");
        q.add("a");
        q.add("b");
        assert_eq!(q.get().await, Some("a"));
        assert_eq!(q.get().await, Some("b"));
        q.add_rate_limited("a");
        assert_eq!(q.get().await, Some("a"));
        q.forget(&"a");
        stop.cancel();
        assert_eq!(q.get().await, None);
    }
}

//! A work queue of object keys.
//!
//! A key is held at most once while waiting, and is never handed to two
//! workers at once: a key added while it is being processed is queued again
//! once the worker reports it done. Failed keys come back after a per-key
//! exponential backoff that resets when the key succeeds.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;

/// Names one object: namespace (empty for cluster-scoped) and name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectKey {
    /// The namespace, or "" for a cluster-scoped object.
    pub namespace: String,
    /// The name.
    pub name: String,
}

impl ObjectKey {
    /// The key of a namespaced object.
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        ObjectKey {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    /// The key of a cluster-scoped object.
    pub fn cluster(name: impl Into<String>) -> Self {
        Self::new("", name)
    }

    /// The key of `obj`.
    pub fn of<K: kube::ResourceExt>(obj: &K) -> Self {
        Self::new(obj.namespace().unwrap_or_default(), obj.name_any())
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.namespace.is_empty() {
            f.write_str(&self.name)
        } else {
            write!(f, "{}/{}", self.namespace, self.name)
        }
    }
}

/// Backoff of the first retry of a failing key.
const BASE_DELAY: Duration = Duration::from_millis(5);
/// Longest backoff of a failing key.
const MAX_DELAY: Duration = Duration::from_secs(1000);

struct State<K> {
    queue: VecDeque<K>,
    dirty: HashSet<K>,
    processing: HashSet<K>,
    failures: HashMap<K, u32>,
    shut_down: bool,
}

/// A deduplicating, rate-limited work queue.
pub struct WorkQueue<K> {
    state: Mutex<State<K>>,
    notify: Notify,
}

impl<K: Clone + Eq + Hash + Send + 'static> WorkQueue<K> {
    /// An empty queue.
    pub fn new() -> Arc<Self> {
        Arc::new(WorkQueue {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                dirty: HashSet::new(),
                processing: HashSet::new(),
                failures: HashMap::new(),
                shut_down: false,
            }),
            notify: Notify::new(),
        })
    }

    /// Queues `key` unless it is already waiting.
    pub fn add(&self, key: K) {
        let mut st = self.state.lock();
        if st.shut_down || !st.dirty.insert(key.clone()) {
            return;
        }
        if st.processing.contains(&key) {
            return;
        }
        st.queue.push_back(key);
        drop(st);
        self.notify.notify_one();
    }

    /// Queues `key` after `delay`.
    pub fn add_after(self: &Arc<Self>, key: K, delay: Duration) {
        if delay.is_zero() {
            self.add(key);
            return;
        }
        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if let Some(q) = this.upgrade() {
                q.add(key);
            }
        });
    }

    /// Queues `key` after its backoff, which doubles each call until
    /// [`WorkQueue::forget`].
    pub fn add_rate_limited(self: &Arc<Self>, key: K) {
        let delay = {
            let mut st = self.state.lock();
            let n = st.failures.entry(key.clone()).or_insert(0);
            let delay = BASE_DELAY
                .checked_mul(1u32.checked_shl(*n).unwrap_or(u32::MAX))
                .unwrap_or(MAX_DELAY)
                .min(MAX_DELAY);
            *n = n.saturating_add(1);
            delay
        };
        self.add_after(key, delay);
    }

    /// Resets the backoff of `key`.
    pub fn forget(&self, key: &K) {
        self.state.lock().failures.remove(key);
    }

    /// How many times `key` failed since it last succeeded.
    pub fn failures(&self, key: &K) -> u32 {
        self.state.lock().failures.get(key).copied().unwrap_or(0)
    }

    /// The next key, waiting for one. `None` once the queue is shut down.
    pub async fn get(&self) -> Option<K> {
        loop {
            let notified = self.notify.notified();
            {
                let mut st = self.state.lock();
                if let Some(key) = st.queue.pop_front() {
                    st.dirty.remove(&key);
                    st.processing.insert(key.clone());
                    let more = !st.queue.is_empty();
                    drop(st);
                    if more {
                        self.notify.notify_one();
                    }
                    return Some(key);
                }
                if st.shut_down {
                    return None;
                }
            }
            notified.await;
        }
    }

    /// Reports `key`, handed out by [`WorkQueue::get`], processed. A key
    /// added meanwhile is queued again.
    pub fn done(&self, key: &K) {
        let mut st = self.state.lock();
        st.processing.remove(key);
        if st.dirty.contains(key) {
            st.queue.push_back(key.clone());
            drop(st);
            self.notify.notify_one();
        }
    }

    /// Stops handing out keys; waiting [`WorkQueue::get`] calls return
    /// `None`.
    pub fn shut_down(&self) {
        self.state.lock().shut_down = true;
        self.notify.notify_waiters();
        self.notify.notify_one();
    }

    /// The number of keys waiting.
    pub fn len(&self) -> usize {
        self.state.lock().queue.len()
    }

    /// Whether no key is waiting.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dedupes_waiting_keys() {
        let q = WorkQueue::new();
        q.add("a");
        q.add("a");
        q.add("b");
        assert_eq!(q.len(), 2);
        assert_eq!(q.get().await, Some("a"));
        assert_eq!(q.get().await, Some("b"));
        assert!(q.is_empty());
    }

    #[tokio::test]
    async fn key_added_while_processing_waits_for_done() {
        let q = WorkQueue::new();
        q.add("a");
        assert_eq!(q.get().await, Some("a"));
        q.add("a");
        assert!(
            q.is_empty(),
            "a key being processed is not handed out twice"
        );
        q.done(&"a");
        assert_eq!(q.get().await, Some("a"));
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limited_backoff_doubles_and_resets() {
        let q = WorkQueue::new();
        q.add_rate_limited("a");
        q.add_rate_limited("a");
        assert_eq!(q.failures(&"a"), 2);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(q.get().await, Some("a"));
        q.forget(&"a");
        assert_eq!(q.failures(&"a"), 0);
    }

    #[tokio::test]
    async fn shut_down_wakes_waiters() {
        let q: Arc<WorkQueue<&str>> = WorkQueue::new();
        let waiter = tokio::spawn({
            let q = q.clone();
            async move { q.get().await }
        });
        tokio::task::yield_now().await;
        q.shut_down();
        assert_eq!(waiter.await.unwrap(), None);
    }
}

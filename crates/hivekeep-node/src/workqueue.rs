//! Work queues of string keys, with delays and per-key failure backoff.
//!
//! A queue holds each key at most once, and hands it to one worker at a
//! time: a key added again while a worker processes it is queued again once
//! the worker calls [`WorkQueue::done`]. A delayed add keeps the earliest
//! time the key was asked for. Delays run on tokio time.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;

#[derive(Default)]
struct State {
    queue: VecDeque<String>,
    dirty: HashSet<String>,
    processing: HashSet<String>,
    waiting: HashMap<String, Instant>,
    shutdown: bool,
}

struct Inner {
    state: Mutex<State>,
    ready: Notify,
    timer: Arc<Notify>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Lets the timer task notice the queue is gone.
        self.timer.notify_one();
    }
}

/// A delaying, deduplicating work queue.
#[derive(Clone)]
pub struct WorkQueue {
    inner: Arc<Inner>,
}

impl Default for WorkQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkQueue {
    /// A new queue. Delayed adds need a tokio runtime, which runs the
    /// queue's timer.
    pub fn new() -> Self {
        let inner = Arc::new(Inner {
            state: Mutex::new(State::default()),
            ready: Notify::new(),
            timer: Arc::new(Notify::new()),
        });
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(run_timer(Arc::downgrade(&inner), inner.timer.clone()));
        }
        WorkQueue { inner }
    }

    /// Queues `key` now, unless it is queued already.
    pub fn add(&self, key: &str) {
        add_locked(&mut self.inner.state.lock(), key, &self.inner.ready);
    }

    /// Queues `key` after `delay`, or keeps an earlier pending time.
    pub fn add_after(&self, key: &str, delay: Duration) {
        if delay.is_zero() {
            self.add(key);
            return;
        }
        let at = Instant::now() + delay;
        let mut st = self.inner.state.lock();
        if st.shutdown {
            return;
        }
        let entry = st.waiting.entry(key.to_string()).or_insert(at);
        if at < *entry {
            *entry = at;
        }
        drop(st);
        self.inner.timer.notify_one();
    }

    /// Waits for the next key, or `None` once the queue shuts down.
    pub async fn get(&self) -> Option<String> {
        loop {
            let notified = self.inner.ready.notified();
            {
                let mut st = self.inner.state.lock();
                if let Some(key) = st.queue.pop_front() {
                    st.dirty.remove(&key);
                    st.processing.insert(key.clone());
                    return Some(key);
                }
                if st.shutdown {
                    return None;
                }
            }
            notified.await;
        }
    }

    /// Marks `key` processed, queueing it again if it was added meanwhile.
    pub fn done(&self, key: &str) {
        let mut st = self.inner.state.lock();
        st.processing.remove(key);
        if st.dirty.contains(key) && !st.shutdown {
            st.queue.push_back(key.to_string());
            drop(st);
            self.inner.ready.notify_one();
        }
    }

    /// Stops the queue: pending keys are dropped and `get` returns `None`.
    pub fn shut_down(&self) {
        let mut st = self.inner.state.lock();
        st.shutdown = true;
        st.queue.clear();
        st.waiting.clear();
        drop(st);
        self.inner.ready.notify_waiters();
        self.inner.timer.notify_one();
    }

    /// Whether the queue has shut down.
    pub fn is_shut_down(&self) -> bool {
        self.inner.state.lock().shutdown
    }

    /// Keys queued now, for tests.
    pub fn len(&self) -> usize {
        self.inner.state.lock().queue.len()
    }

    /// Whether nothing is queued now.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// When `key` is due on the delayed list, for tests.
    pub fn pending_at(&self, key: &str) -> Option<Instant> {
        self.inner.state.lock().waiting.get(key).copied()
    }
}

fn add_locked(st: &mut State, key: &str, ready: &Notify) {
    if st.shutdown || st.dirty.contains(key) {
        return;
    }
    st.dirty.insert(key.to_string());
    if st.processing.contains(key) {
        return;
    }
    st.queue.push_back(key.to_string());
    ready.notify_one();
}

async fn run_timer(inner: Weak<Inner>, timer: Arc<Notify>) {
    loop {
        let notified = timer.clone().notified_owned();
        let Some(this) = inner.upgrade() else { return };
        let next = {
            let mut st = this.state.lock();
            if st.shutdown {
                return;
            }
            let now = Instant::now();
            let due: Vec<String> = st
                .waiting
                .iter()
                .filter(|(_, at)| **at <= now)
                .map(|(k, _)| k.clone())
                .collect();
            for k in due {
                st.waiting.remove(&k);
                add_locked(&mut st, &k, &this.ready);
            }
            st.waiting.values().min().copied()
        };
        drop(this);
        match next {
            Some(at) => {
                tokio::select! {
                    _ = tokio::time::sleep_until(at) => {}
                    _ = notified => {}
                }
            }
            None => notified.await,
        }
    }
}

/// Per-key exponential backoff: each consecutive failure of a key doubles
/// its delay, from `base` up to `max`.
pub struct RateLimiter {
    base: Duration,
    max: Duration,
    failures: Mutex<HashMap<String, u32>>,
}

impl RateLimiter {
    /// A limiter from `base` to `max`.
    pub fn new(base: Duration, max: Duration) -> Self {
        RateLimiter {
            base,
            max,
            failures: Mutex::new(HashMap::new()),
        }
    }

    /// Records another failure of `key` and returns how long to hold it
    /// back.
    pub fn when(&self, key: &str) -> Duration {
        let mut f = self.failures.lock();
        let n = f.entry(key.to_string()).or_insert(0);
        let exp = *n;
        *n = n.saturating_add(1);
        let factor = 2u64.checked_pow(exp).unwrap_or(u64::MAX);
        let nanos = (self.base.as_nanos() as u64).saturating_mul(factor);
        Duration::from_nanos(nanos).min(self.max)
    }

    /// How many consecutive failures `key` has.
    pub fn num_requeues(&self, key: &str) -> u32 {
        self.failures.lock().get(key).copied().unwrap_or(0)
    }

    /// Forgets `key`'s failures.
    pub fn forget(&self, key: &str) {
        self.failures.lock().remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dedups_and_requeues_after_done() {
        let q = WorkQueue::new();
        q.add("a");
        q.add("a");
        assert_eq!(q.len(), 1);
        let k = q.get().await.unwrap();
        q.add("a");
        assert_eq!(q.len(), 0, "processing keys wait for done");
        q.done(&k);
        assert_eq!(q.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn add_after_keeps_earliest() {
        let q = WorkQueue::new();
        q.add_after("a", Duration::from_secs(60));
        q.add_after("a", Duration::from_secs(10));
        q.add_after("a", Duration::from_secs(30));
        let got = tokio::time::timeout(Duration::from_secs(11), q.get()).await;
        assert_eq!(got.unwrap().as_deref(), Some("a"));
    }

    #[tokio::test]
    async fn shut_down_wakes_getters() {
        let q = WorkQueue::new();
        let q2 = q.clone();
        let h = tokio::spawn(async move { q2.get().await });
        tokio::task::yield_now().await;
        q.shut_down();
        assert_eq!(h.await.unwrap(), None);
    }

    #[test]
    fn rate_limiter_doubles_to_max() {
        let r = RateLimiter::new(Duration::from_secs(1), Duration::from_secs(5));
        assert_eq!(r.when("a"), Duration::from_secs(1));
        assert_eq!(r.when("a"), Duration::from_secs(2));
        assert_eq!(r.when("a"), Duration::from_secs(4));
        assert_eq!(r.when("a"), Duration::from_secs(5));
        assert_eq!(r.num_requeues("a"), 4);
        r.forget("a");
        assert_eq!(r.when("a"), Duration::from_secs(1));
    }
}

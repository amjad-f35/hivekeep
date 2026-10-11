//! A FIFO weighted semaphore over byte counts.
//!
//! Waiters are served strictly in arrival order, so a large reservation is
//! never starved by a stream of small ones. Capacities are `u64` because
//! memory and disk budgets run past `u32`.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;

#[derive(Debug)]
struct Waiter {
    id: u64,
    n: u64,
    ready: oneshot::Sender<()>,
}

#[derive(Debug)]
struct State {
    size: u64,
    cur: u64,
    next_id: u64,
    waiters: VecDeque<Waiter>,
}

impl State {
    fn notify(&mut self) {
        while let Some(front) = self.waiters.front() {
            if self.size - self.cur < front.n {
                // Strict FIFO: a later, smaller waiter does not jump the queue.
                break;
            }
            let w = self.waiters.pop_front().expect("front exists");
            self.cur += w.n;
            if w.ready.send(()).is_err() {
                // The waiter went away between being chosen and woken; its
                // guard could not release what it never saw, so do it here.
                self.cur -= w.n;
            }
        }
    }
}

/// A weighted semaphore.
#[derive(Debug)]
pub struct WeightedSemaphore {
    state: Mutex<State>,
}

/// Units held from a [`WeightedSemaphore`], returned on drop.
#[derive(Debug)]
pub struct WeightedPermit {
    sem: Arc<WeightedSemaphore>,
    n: u64,
}

impl WeightedPermit {
    /// How many units the permit holds.
    pub fn units(&self) -> u64 {
        self.n
    }
}

impl Drop for WeightedPermit {
    fn drop(&mut self) {
        self.sem.release(self.n);
    }
}

/// Removes a queued waiter if its acquire is abandoned.
struct QueueGuard<'a> {
    sem: &'a WeightedSemaphore,
    id: u64,
    n: u64,
    armed: bool,
}

impl Drop for QueueGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut st = self.sem.state.lock();
        if let Some(pos) = st.waiters.iter().position(|w| w.id == self.id) {
            let was_front = pos == 0;
            st.waiters.remove(pos);
            if was_front && st.size > st.cur {
                st.notify();
            }
        } else {
            // Granted after the caller gave up: put the units back.
            st.cur -= self.n;
            st.notify();
        }
    }
}

impl WeightedSemaphore {
    /// A semaphore with `size` units.
    pub fn new(size: u64) -> Arc<Self> {
        Arc::new(WeightedSemaphore {
            state: Mutex::new(State {
                size,
                cur: 0,
                next_id: 0,
                waiters: VecDeque::new(),
            }),
        })
    }

    /// The total number of units.
    pub fn size(&self) -> u64 {
        self.state.lock().size
    }

    /// Takes `n` units if they are free and nobody is waiting.
    pub fn try_acquire(self: &Arc<Self>, n: u64) -> Option<WeightedPermit> {
        let mut st = self.state.lock();
        if st.size - st.cur >= n && st.waiters.is_empty() {
            st.cur += n;
            return Some(WeightedPermit {
                sem: self.clone(),
                n,
            });
        }
        None
    }

    /// Waits for `n` units. A request larger than the semaphore never
    /// completes; callers bound the wait by dropping the future.
    pub async fn acquire(self: &Arc<Self>, n: u64) -> WeightedPermit {
        enum Start {
            Granted,
            Never,
            Queued(u64, oneshot::Receiver<()>),
        }
        let start = {
            let mut st = self.state.lock();
            if st.size - st.cur >= n && st.waiters.is_empty() {
                st.cur += n;
                Start::Granted
            } else if n > st.size {
                Start::Never
            } else {
                let (tx, rx) = oneshot::channel();
                let id = st.next_id;
                st.next_id += 1;
                st.waiters.push_back(Waiter { id, n, ready: tx });
                Start::Queued(id, rx)
            }
        };
        let (id, rx) = match start {
            Start::Granted => {
                return WeightedPermit {
                    sem: self.clone(),
                    n,
                }
            }
            Start::Never => return std::future::pending().await,
            Start::Queued(id, rx) => (id, rx),
        };
        let mut guard = QueueGuard {
            sem: self,
            id,
            n,
            armed: true,
        };
        // The sender is only dropped after a send, or with the semaphore.
        let _ = rx.await;
        guard.armed = false;
        WeightedPermit {
            sem: self.clone(),
            n,
        }
    }

    fn release(&self, n: u64) {
        let mut st = self.state.lock();
        assert!(st.cur >= n, "semaphore released more than held");
        st.cur -= n;
        st.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn fifo_and_release() {
        let s = WeightedSemaphore::new(10);
        let a = s.acquire(8).await;
        let s2 = s.clone();
        let big = tokio::spawn(async move { s2.acquire(5).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        // A small request queues behind the big one.
        assert!(s.try_acquire(1).is_none());
        drop(a);
        let b = big.await.unwrap();
        assert_eq!(b.units(), 5);
        assert!(s.try_acquire(5).is_some());
    }

    #[tokio::test]
    async fn canceled_waiter_does_not_leak() {
        let s = WeightedSemaphore::new(4);
        let held = s.acquire(4).await;
        let waiting = tokio::time::timeout(Duration::from_millis(20), s.acquire(2)).await;
        assert!(waiting.is_err());
        drop(held);
        assert!(s.try_acquire(4).is_some());
    }
}

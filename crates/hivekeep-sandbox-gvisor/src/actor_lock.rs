//! Per-Actor serialization of lifecycle RPCs, and the set of RPCs in flight
//! that graceful shutdown cancels or waits for.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{Notify, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;

struct Held {
    mutex: Arc<tokio::sync::Mutex<()>>,
    /// Holders and waiters.
    refs: usize,
}

/// One lock per Actor uid. An entry exists while anyone holds or waits for
/// the lock, which is what [`Locks::busy`] reports.
#[derive(Default)]
pub struct Locks {
    held: Arc<Mutex<HashMap<String, Held>>>,
}

/// Holds an Actor's lock until dropped.
pub struct ActorGuard {
    _guard: OwnedMutexGuard<()>,
    release: Release,
}

impl std::fmt::Debug for ActorGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorGuard")
            .field("uid", &self.release.uid)
            .finish()
    }
}

/// Drops one reference to an entry, removing it with the last.
struct Release {
    held: Arc<Mutex<HashMap<String, Held>>>,
    uid: String,
}

impl Drop for Release {
    fn drop(&mut self) {
        let mut held = self.held.lock();
        if let Some(e) = held.get_mut(&self.uid) {
            e.refs -= 1;
            if e.refs == 0 {
                held.remove(&self.uid);
            }
        }
    }
}

impl Locks {
    /// No locks held.
    pub fn new() -> Self {
        Self::default()
    }

    /// Waits for the Actor's lock. Dropping the future gives up the wait.
    pub async fn lock(&self, uid: &str) -> ActorGuard {
        let (mutex, release) = {
            let mut held = self.held.lock();
            let e = held.entry(uid.to_string()).or_insert_with(|| Held {
                mutex: Arc::new(tokio::sync::Mutex::new(())),
                refs: 0,
            });
            e.refs += 1;
            (
                e.mutex.clone(),
                Release {
                    held: self.held.clone(),
                    uid: uid.to_string(),
                },
            )
        };
        let guard = mutex.lock_owned().await;
        ActorGuard {
            _guard: guard,
            release,
        }
    }

    /// Waits for the Actor's lock unless `cancel` fires first.
    pub async fn lock_cancellable(
        &self,
        uid: &str,
        cancel: &CancellationToken,
    ) -> Option<ActorGuard> {
        if cancel.is_cancelled() {
            return None;
        }
        tokio::select! {
            g = self.lock(uid) => Some(g),
            _ = cancel.cancelled() => None,
        }
    }

    /// Whether anyone holds or waits for the Actor's lock.
    pub fn busy(&self, uid: &str) -> bool {
        self.held.lock().contains_key(uid)
    }
}

/// The kind of lifecycle RPC in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rpc {
    /// RunWorkload.
    Run,
    /// RestoreWorkload.
    Restore,
    /// CheckpointWorkload.
    Checkpoint,
}

impl Rpc {
    /// The RPC's method name.
    pub fn name(self) -> &'static str {
        match self {
            Rpc::Run => "RunWorkload",
            Rpc::Restore => "RestoreWorkload",
            Rpc::Checkpoint => "CheckpointWorkload",
        }
    }
}

struct Entry {
    rpc: Rpc,
    /// Set for startups, which shutdown cancels.
    cancel: Option<CancellationToken>,
    id: u64,
}

/// Lifecycle RPCs in flight, at most one per Actor.
#[derive(Default)]
pub struct InFlight {
    rpcs: Mutex<(u64, HashMap<String, Entry>)>,
    changed: Notify,
}

/// Unregisters an RPC when dropped.
pub struct InFlightGuard<'a> {
    owner: &'a InFlight,
    uid: String,
    id: u64,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        let mut g = self.owner.rpcs.lock();
        if g.1.get(&self.uid).is_some_and(|e| e.id == self.id) {
            g.1.remove(&self.uid);
        }
        drop(g);
        self.owner.changed.notify_waiters();
    }
}

impl InFlight {
    /// Nothing in flight.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an RPC until the guard drops. `cancel` marks it a startup
    /// that shutdown may cancel.
    pub fn add(&self, uid: &str, rpc: Rpc, cancel: Option<CancellationToken>) -> InFlightGuard<'_> {
        let mut g = self.rpcs.lock();
        g.0 += 1;
        let id = g.0;
        g.1.insert(uid.to_string(), Entry { rpc, cancel, id });
        drop(g);
        self.changed.notify_waiters();
        InFlightGuard {
            owner: self,
            uid: uid.to_string(),
            id,
        }
    }

    /// Cancels every startup, returning their Actor uids. Checkpoints keep
    /// running.
    pub fn cancel_startups(&self) -> Vec<String> {
        let g = self.rpcs.lock();
        g.1.iter()
            .filter_map(|(uid, e)| {
                e.cancel.as_ref().map(|c| {
                    c.cancel();
                    uid.clone()
                })
            })
            .collect()
    }

    /// Waits until nothing is in flight; false if `cancel` fires first.
    pub async fn wait_idle(&self, cancel: &CancellationToken) -> bool {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.rpcs.lock().1.is_empty() {
                return true;
            }
            tokio::select! {
                _ = notified => {}
                _ = cancel.cancelled() => return false,
            }
        }
    }

    /// The RPC in flight for each Actor.
    pub fn names(&self) -> HashMap<String, &'static str> {
        self.rpcs
            .lock()
            .1
            .iter()
            .map(|(u, e)| (u.clone(), e.rpc.name()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn locks_serialize_and_report_busy() {
        let locks = Arc::new(Locks::new());
        assert!(!locks.busy("a"));
        let g = locks.lock("a").await;
        assert!(locks.busy("a"));
        let l2 = locks.clone();
        let waiter = tokio::spawn(async move {
            let _g = l2.lock("a").await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        // Another Actor is independent.
        let _b = locks.lock("b").await;
        drop(g);
        waiter.await.unwrap();
        assert!(!locks.busy("a"));
        assert!(locks.busy("b"));
    }

    #[tokio::test]
    async fn cancelled_waits_leave_nothing_behind() {
        let locks = Locks::new();
        let g = locks.lock("a").await;
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(locks.lock_cancellable("a", &cancel).await.is_none());
        let soon = CancellationToken::new();
        let s2 = soon.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            s2.cancel();
        });
        assert!(locks.lock_cancellable("a", &soon).await.is_none());
        drop(g);
        assert!(!locks.busy("a"));
    }

    #[tokio::test]
    async fn in_flight_waits_and_cancels() {
        let f = InFlight::new();
        let token = CancellationToken::new();
        let run = f.add("a", Rpc::Run, Some(token.clone()));
        let ckpt = f.add("b", Rpc::Checkpoint, None);
        assert_eq!(f.names().get("b"), Some(&"CheckpointWorkload"));
        assert_eq!(f.cancel_startups(), vec!["a".to_string()]);
        assert!(token.is_cancelled());
        let give_up = CancellationToken::new();
        give_up.cancel();
        assert!(!f.wait_idle(&give_up).await);
        drop(run);
        let never = CancellationToken::new();
        let wait = f.wait_idle(&never);
        tokio::pin!(wait);
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut wait)
            .await
            .is_err());
        drop(ckpt);
        assert!(tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .unwrap());
        assert!(f.names().is_empty());
    }
}

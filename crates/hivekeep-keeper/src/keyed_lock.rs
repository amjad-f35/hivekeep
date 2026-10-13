//! Per-key async mutexes, each guarding a value of its own.
//!
//! The keeper serializes work per actor: lifecycle RPCs hold one lock per
//! actor UID for their whole run, and the snapshot tracker holds another
//! for each short critical section on an actor's state. A key's slot lives
//! only while some task holds or waits for it, or while its value is not
//! [`Vacant::is_vacant`].

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::context::{Ctx, CtxError};

/// Whether a slot's value holds nothing worth keeping.
pub trait Vacant {
    /// True when the slot may be dropped.
    fn is_vacant(&self) -> bool;
}

impl Vacant for () {
    fn is_vacant(&self) -> bool {
        true
    }
}

type Slots<T> = Arc<Mutex<HashMap<String, Arc<AsyncMutex<T>>>>>;

/// A set of per-key locks.
pub struct KeyedLocks<T: Vacant + Default + Send + 'static> {
    slots: Slots<T>,
}

impl<T: Vacant + Default + Send + 'static> Default for KeyedLocks<T> {
    fn default() -> Self {
        KeyedLocks {
            slots: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<T: Vacant + Default + Send + 'static> KeyedLocks<T> {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    fn slot(&self, key: &str) -> Arc<AsyncMutex<T>> {
        self.slots
            .lock()
            .entry(key.to_string())
            .or_default()
            .clone()
    }

    /// Takes `key`'s lock, waiting as long as it takes.
    pub async fn lock(&self, key: &str) -> KeyedGuard<T> {
        let slot = self.slot(key);
        let guard = slot.lock_owned().await;
        KeyedGuard {
            key: key.to_string(),
            guard: Some(guard),
            slots: self.slots.clone(),
        }
    }

    /// Takes `key`'s lock, or gives up when `ctx` ends first.
    pub async fn lock_ctx(&self, ctx: &Ctx, key: &str) -> Result<KeyedGuard<T>, CtxError> {
        ctx.run(self.lock(key)).await
    }

    /// The number of live slots, for tests.
    pub fn len(&self) -> usize {
        self.slots.lock().len()
    }

    /// Whether no slot is live.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A held key lock, giving access to the key's value.
pub struct KeyedGuard<T: Vacant + Send + 'static> {
    key: String,
    guard: Option<OwnedMutexGuard<T>>,
    slots: Slots<T>,
}

impl<T: Vacant + Send + 'static> KeyedGuard<T> {
    /// The key this guard holds.
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl<T: Vacant + Send + 'static> Deref for KeyedGuard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard.as_ref().expect("guard is held until drop")
    }
}

impl<T: Vacant + Send + 'static> DerefMut for KeyedGuard<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.guard.as_mut().expect("guard is held until drop")
    }
}

impl<T: Vacant + Send + 'static> Drop for KeyedGuard<T> {
    fn drop(&mut self) {
        let vacant = self.guard.as_ref().is_some_and(|g| g.is_vacant());
        drop(self.guard.take());
        if !vacant {
            return;
        }
        let mut slots = self.slots.lock();
        // Only the map holds it: nobody waits, and the value is empty.
        let remove = slots.get(&self.key).is_some_and(|slot| {
            Arc::strong_count(slot) == 1 && slot.try_lock().is_ok_and(|g| g.is_vacant())
        });
        if remove {
            slots.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn serializes_one_key() {
        let locks: Arc<KeyedLocks<()>> = Arc::new(KeyedLocks::new());
        let g = locks.lock("a").await;
        let l2 = locks.clone();
        let h = tokio::spawn(async move {
            let _g = l2.lock("a").await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!h.is_finished());
        drop(g);
        h.await.unwrap();
        assert!(locks.is_empty());
    }

    #[tokio::test]
    async fn other_keys_do_not_wait() {
        let locks: KeyedLocks<()> = KeyedLocks::new();
        let _a = locks.lock("a").await;
        let _b = locks.lock("b").await;
        assert_eq!(locks.len(), 2);
    }

    #[tokio::test]
    async fn gives_up_with_ctx() {
        let locks: KeyedLocks<()> = KeyedLocks::new();
        let _a = locks.lock("a").await;
        let ctx = Ctx::background();
        ctx.cancel();
        assert!(locks.lock_ctx(&ctx, "a").await.is_err());
    }

    #[derive(Default)]
    struct Val(Option<u32>);
    impl Vacant for Val {
        fn is_vacant(&self) -> bool {
            self.0.is_none()
        }
    }

    #[tokio::test]
    async fn keeps_non_vacant_values() {
        let locks: KeyedLocks<Val> = KeyedLocks::new();
        locks.lock("a").await.0 = Some(3);
        assert_eq!(locks.lock("a").await.0, Some(3));
        locks.lock("a").await.0 = None;
        assert!(locks.is_empty());
    }
}

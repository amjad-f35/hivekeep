//! Per-relay admission limits: queries in flight and TCP connections.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

/// Most UDP queries one relay forwards at once.
pub const MAX_IN_FLIGHT: i32 = 64;
/// Most TCP connections one relay serves at once.
pub const MAX_CONNECTIONS: i32 = 16;

/// A counting semaphore without waiting: acquisition either succeeds now
/// or fails. `max == None` never limits.
#[derive(Debug, Default)]
pub struct Slots {
    max: Option<i32>,
    used: AtomicI32,
}

impl Slots {
    /// Slots allowing `max` concurrent holders.
    pub fn new(max: i32) -> Self {
        Slots {
            max: Some(max),
            used: AtomicI32::new(0),
        }
    }

    /// Slots that never refuse.
    pub fn unlimited() -> Self {
        Slots::default()
    }

    /// The configured maximum.
    pub fn max(&self) -> Option<i32> {
        self.max
    }

    /// Takes a slot if one is free.
    pub fn try_acquire(&self) -> bool {
        let Some(max) = self.max else {
            return true;
        };
        if self.used.fetch_add(1, Ordering::AcqRel) + 1 > max {
            self.used.fetch_sub(1, Ordering::AcqRel);
            return false;
        }
        true
    }

    /// Returns a slot.
    ///
    /// # Panics
    /// When more slots are returned than were taken.
    pub fn release(&self) {
        if self.max.is_none() {
            return;
        }
        if self.used.fetch_sub(1, Ordering::AcqRel) - 1 < 0 {
            panic!("dns: slot released more times than acquired");
        }
    }

    /// Slots currently held.
    pub fn occupied(&self) -> i32 {
        self.used.load(Ordering::Acquire)
    }
}

/// A held slot, returned on drop unless [`SlotGuard::keep`] hands it over.
#[must_use]
pub struct SlotGuard {
    slots: Option<Arc<Slots>>,
}

impl SlotGuard {
    /// Takes a slot from `slots`, or `None` when none is free.
    pub fn acquire(slots: &Arc<Slots>) -> Option<SlotGuard> {
        slots.try_acquire().then(|| SlotGuard {
            slots: Some(slots.clone()),
        })
    }

    /// Keeps the slot held past the guard; whoever took it over releases it.
    pub fn keep(mut self) {
        self.slots = None;
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        if let Some(s) = self.slots.take() {
            s.release();
        }
    }
}

/// The limits of one relay. Each Actor's relay has its own, so one Actor
/// cannot starve another.
#[derive(Debug)]
pub struct Limiter {
    /// UDP queries forwarded and not yet answered.
    pub in_flight: Arc<Slots>,
    /// TCP connections being served.
    pub connections: Arc<Slots>,
}

impl Limiter {
    /// The default limits.
    pub fn new() -> Self {
        Limiter::with_limits(MAX_IN_FLIGHT, MAX_CONNECTIONS)
    }

    /// Custom limits.
    pub fn with_limits(in_flight: i32, connections: i32) -> Self {
        Limiter {
            in_flight: Arc::new(Slots::new(in_flight)),
            connections: Arc::new(Slots::new(connections)),
        }
    }
}

impl Default for Limiter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_limiter() {
        let l = Limiter::new();
        assert_eq!(l.in_flight.max(), Some(MAX_IN_FLIGHT));
        assert_eq!(l.connections.max(), Some(MAX_CONNECTIONS));
        assert_eq!(l.in_flight.occupied(), 0);
        assert_eq!(l.connections.occupied(), 0);
    }

    #[test]
    fn acquire_and_release() {
        let s = Slots::new(3);
        for i in 1..=3 {
            assert!(s.try_acquire(), "acquire {i}");
            assert_eq!(s.occupied(), i);
        }
        assert!(!s.try_acquire(), "acquired at capacity");
        assert_eq!(s.occupied(), 3);
        s.release();
        assert_eq!(s.occupied(), 2);
        assert!(s.try_acquire());
        for i in (0..3).rev() {
            s.release();
            assert_eq!(s.occupied(), i);
        }
    }

    #[test]
    fn unlimited_slots_never_refuse() {
        let s = Slots::unlimited();
        assert!(s.try_acquire());
        s.release();
    }

    #[test]
    #[should_panic(expected = "released more times")]
    fn release_underflow_panics() {
        Slots::new(1).release();
    }

    #[test]
    fn concurrent() {
        let s = Arc::new(Slots::new(8));
        let active = Arc::new(AtomicI32::new(0));
        let max_seen = Arc::new(AtomicI32::new(0));
        let handles: Vec<_> = (0..32)
            .map(|_| {
                let (s, active, max_seen) = (s.clone(), active.clone(), max_seen.clone());
                std::thread::spawn(move || {
                    for _ in 0..200 {
                        if !s.try_acquire() {
                            continue;
                        }
                        let cur = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_seen.fetch_max(cur, Ordering::SeqCst);
                        active.fetch_sub(1, Ordering::SeqCst);
                        s.release();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert!(max_seen.load(Ordering::SeqCst) <= 8);
        assert_eq!(s.occupied(), 0);
    }

    #[test]
    fn guard_releases_unless_kept() {
        let s = Arc::new(Slots::new(1));
        drop(SlotGuard::acquire(&s).unwrap());
        assert_eq!(s.occupied(), 0);
        SlotGuard::acquire(&s).unwrap().keep();
        assert_eq!(s.occupied(), 1);
        assert!(SlotGuard::acquire(&s).is_none());
    }
}

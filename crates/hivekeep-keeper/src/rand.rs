//! Small, non-cryptographic randomness: unique name suffixes, jitter, and a
//! seeded shuffle. Nothing here guards a secret.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A fresh random 64-bit value.
pub fn u64() -> u64 {
    let mut h = RandomState::new().build_hasher();
    h.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    if let Ok(d) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        h.write_u128(d.as_nanos());
    }
    h.finish()
}

/// A random value in `[0, n)`; zero when `n` is zero.
pub fn below(n: u64) -> u64 {
    if n == 0 {
        0
    } else {
        u64() % n
    }
}

/// A random duration in `[0, d)`.
pub fn duration_below(d: Duration) -> Duration {
    let nanos = d.as_nanos().min(u64::MAX as u128) as u64;
    Duration::from_nanos(below(nanos))
}

/// A random lowercase base-36 suffix for temporary names.
pub fn suffix() -> String {
    let mut v = u64();
    let mut out = String::with_capacity(13);
    for _ in 0..12 {
        let d = (v % 36) as u8;
        out.push(if d < 10 {
            (b'0' + d) as char
        } else {
            (b'a' + d - 10) as char
        });
        v /= 36;
    }
    out
}

/// A deterministic generator for a seeded shuffle (splitmix64).
#[derive(Debug, Clone)]
pub struct Seeded(u64);

impl Seeded {
    /// A generator starting at `seed`.
    pub fn new(seed: u64) -> Self {
        Seeded(seed)
    }

    /// The next value.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Shuffles `items` in place (Fisher-Yates).
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = (self.next_u64() % (i as u64 + 1)) as usize;
            items.swap(i, j);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn below_stays_in_range() {
        for _ in 0..100 {
            assert!(below(7) < 7);
        }
        assert_eq!(below(0), 0);
    }

    #[test]
    fn seeded_shuffle_is_deterministic() {
        let mut a = vec![1, 2, 3, 4, 5, 6];
        let mut b = a.clone();
        Seeded::new(42).shuffle(&mut a);
        Seeded::new(42).shuffle(&mut b);
        assert_eq!(a, b);
        let mut sorted = a.clone();
        sorted.sort();
        assert_eq!(sorted, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn suffixes_differ() {
        assert_ne!(suffix(), suffix());
    }
}

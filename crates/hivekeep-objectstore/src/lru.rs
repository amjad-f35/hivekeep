//! A small bounded least-recently-used map for credential caches.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

/// A map holding at most `capacity` entries, evicting the least recently
/// used one when full.
#[derive(Debug)]
pub struct LruCache<K, V> {
    capacity: usize,
    tick: u64,
    entries: HashMap<K, (V, u64)>,
    order: BTreeMap<u64, K>,
}

impl<K: Hash + Eq + Clone, V: Clone> LruCache<K, V> {
    /// A cache of `capacity` entries; zero is treated as one so the cache is
    /// always bounded.
    pub fn new(capacity: usize) -> Self {
        LruCache {
            capacity: capacity.max(1),
            tick: 0,
            entries: HashMap::new(),
            order: BTreeMap::new(),
        }
    }

    fn touch(&mut self, key: &K) {
        if let Some((_, t)) = self.entries.get_mut(key) {
            self.order.remove(t);
            self.tick += 1;
            *t = self.tick;
            self.order.insert(self.tick, key.clone());
        }
    }

    /// The value for `key`, marking it used.
    pub fn get(&mut self, key: &K) -> Option<V> {
        self.touch(key);
        self.entries.get(key).map(|(v, _)| v.clone())
    }

    /// Inserts or replaces `key`, evicting the oldest entry when full.
    pub fn add(&mut self, key: K, value: V) {
        self.remove(&key);
        while self.entries.len() >= self.capacity {
            let Some((&t, _)) = self.order.iter().next() else {
                break;
            };
            if let Some(k) = self.order.remove(&t) {
                self.entries.remove(&k);
            }
        }
        self.tick += 1;
        self.order.insert(self.tick, key.clone());
        self.entries.insert(key, (value, self.tick));
    }

    /// Removes `key`.
    pub fn remove(&mut self, key: &K) {
        if let Some((_, t)) = self.entries.remove(key) {
            self.order.remove(&t);
        }
    }

    /// The number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_least_recently_used() {
        let mut c = LruCache::new(2);
        c.add("a", 1);
        c.add("b", 2);
        assert_eq!(c.get(&"a"), Some(1));
        c.add("c", 3);
        assert_eq!(c.get(&"b"), None);
        assert_eq!(c.get(&"a"), Some(1));
        assert_eq!(c.len(), 2);
        let mut z = LruCache::new(0);
        z.add(1, 1);
        z.add(2, 2);
        assert_eq!(z.len(), 1);
    }
}

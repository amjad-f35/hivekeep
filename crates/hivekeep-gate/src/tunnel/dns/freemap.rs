//! A three-level bitmap over the 65536 DNS transaction IDs that finds a
//! free ID in a handful of word operations even when nearly full.
//!
//! Level 0 holds one bit per ID; a level-1 bit is set when its level-0 word
//! is full, and a level-2 bit when its level-1 word is.

const BITS: usize = 65536;
const WORD: usize = 64;
const L0_WORDS: usize = BITS / WORD;
const L1_WORDS: usize = L0_WORDS / WORD;
const L2_MASK: u64 = (1 << L1_WORDS) - 1;

/// A set of `u16` values with fast lookup of the first absent one.
#[derive(Clone)]
pub struct Map64k {
    l0: Box<[u64; L0_WORDS]>,
    l1: [u64; L1_WORDS],
    l2: u64,
    count: usize,
}

impl Default for Map64k {
    fn default() -> Self {
        Map64k {
            l0: Box::new([0; L0_WORDS]),
            l1: [0; L1_WORDS],
            l2: 0,
            count: 0,
        }
    }
}

impl std::fmt::Debug for Map64k {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Map64k")
            .field("count", &self.count)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BitIndex {
    l0: usize,
    l0_bit: usize,
    l1: usize,
    l1_bit: usize,
}

fn index(i: u16) -> BitIndex {
    let i = i as usize;
    BitIndex {
        l0: i / WORD,
        l0_bit: i % WORD,
        l1: i / WORD / WORD,
        l1_bit: (i / WORD) % WORD,
    }
}

impl Map64k {
    /// An empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of members.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Whether every value is a member.
    pub fn is_full(&self) -> bool {
        self.count == BITS
    }

    /// Whether `i` is a member.
    pub fn get(&self, i: u16) -> bool {
        let x = index(i);
        self.l0[x.l0] & (1 << x.l0_bit) != 0
    }

    /// Adds `i`.
    pub fn set(&mut self, i: u16) {
        let x = index(i);
        if self.l0[x.l0] & (1 << x.l0_bit) != 0 {
            return;
        }
        self.count += 1;
        self.l0[x.l0] |= 1 << x.l0_bit;
        if self.l0[x.l0] == u64::MAX {
            self.l1[x.l1] |= 1 << x.l1_bit;
            if self.l1[x.l1] == u64::MAX {
                self.l2 |= 1 << x.l1;
            }
        }
    }

    /// Removes `i`.
    pub fn clear(&mut self, i: u16) {
        let x = index(i);
        if self.l0[x.l0] & (1 << x.l0_bit) == 0 {
            return;
        }
        self.count -= 1;
        let was_full = self.l0[x.l0] == u64::MAX;
        self.l0[x.l0] &= !(1 << x.l0_bit);
        if was_full {
            let l1_was_full = self.l1[x.l1] == u64::MAX;
            self.l1[x.l1] &= !(1 << x.l1_bit);
            if l1_was_full {
                self.l2 &= !(1 << x.l1);
            }
        }
    }

    /// The smallest non-member, or `None` when full.
    pub fn find_first_unset(&self) -> Option<u16> {
        if self.is_full() {
            return None;
        }
        let l1 = ((!self.l2) & L2_MASK).trailing_zeros() as usize;
        let l0 = l1 * WORD + (!self.l1[l1]).trailing_zeros() as usize;
        Some((l0 * WORD + (!self.l0[l0]).trailing_zeros() as usize) as u16)
    }

    /// The first non-member at or after `start`, wrapping around to the
    /// beginning; `None` when full.
    pub fn find_first_unset_from(&self, start: u16) -> Option<u16> {
        self.find_first_unset_range(start, BITS)
            .map(|i| i as u16)
            .or_else(|| self.find_first_unset())
    }

    /// The first non-member in `[start, end)`, or `None`.
    fn find_first_unset_range(&self, start: u16, end: usize) -> Option<usize> {
        if start as usize >= end || self.is_full() {
            return None;
        }
        let within = |i: usize| (i < end).then_some(i);
        let x = index(start);
        // Rest of the starting level-0 word.
        let w0 = self.l0[x.l0] | ((1u64 << x.l0_bit) - 1);
        if !w0 != 0 {
            return within(x.l0 * WORD + (!w0).trailing_zeros() as usize);
        }
        // Later level-0 words under the same level-1 word.
        let next_l1_bit = x.l1_bit + 1;
        if next_l1_bit < WORD && (x.l0 + 1) * WORD < end {
            let w1 = self.l1[x.l1] | ((1u64 << next_l1_bit) - 1);
            if !w1 != 0 {
                let l0 = x.l1 * WORD + (!w1).trailing_zeros() as usize;
                return within(l0 * WORD + (!self.l0[l0]).trailing_zeros() as usize);
            }
        }
        // Later level-1 words.
        let next_l1 = x.l1 + 1;
        if next_l1 < L1_WORDS && next_l1 * WORD * WORD < end {
            let w2 = self.l2 | ((1u64 << next_l1) - 1);
            let inv2 = !w2 & L2_MASK;
            if inv2 != 0 {
                let l1 = inv2.trailing_zeros() as usize;
                let l0 = l1 * WORD + (!self.l1[l1]).trailing_zeros() as usize;
                return within(l0 * WORD + (!self.l0[l0]).trailing_zeros() as usize);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    fn as_int(v: Option<u16>) -> i64 {
        v.map_or(-1, i64::from)
    }
    fn range_int(v: Option<usize>) -> i64 {
        v.map_or(-1, |v| v as i64)
    }
    fn fill(m: &mut Map64k, start: usize, end: usize) {
        for i in start..end {
            m.set(i as u16);
        }
    }

    struct RefMap {
        m: Map64k,
        occupied: Vec<bool>,
        want_count: usize,
    }

    impl RefMap {
        fn new() -> Self {
            RefMap {
                m: Map64k::new(),
                occupied: vec![false; BITS],
                want_count: 0,
            }
        }
        fn set(&mut self, i: u16) {
            if !self.occupied[i as usize] {
                self.occupied[i as usize] = true;
                self.want_count += 1;
            }
            self.m.set(i);
        }
        fn clear(&mut self, i: u16) {
            if self.occupied[i as usize] {
                self.occupied[i as usize] = false;
                self.want_count -= 1;
            }
            self.m.clear(i);
        }
        fn naive_range(&self, start: usize, end: usize) -> i64 {
            (start..end)
                .find(|i| !self.occupied[*i])
                .map_or(-1, |i| i as i64)
        }
        fn naive_from(&self, start: u16) -> i64 {
            match self.naive_range(start as usize, BITS) {
                -1 => self.naive_range(0, start as usize),
                i => i,
            }
        }
        fn check_range(&self, step: usize, start: u16, end: usize) {
            assert_eq!(
                range_int(self.m.find_first_unset_range(start, end)),
                self.naive_range(start as usize, end),
                "step {step}: range({start}, {end})"
            );
        }
        fn check_from(&self, step: usize, start: u16) {
            assert_eq!(
                as_int(self.m.find_first_unset_from(start)),
                self.naive_from(start),
                "step {step}: from({start})"
            );
            assert_eq!(
                as_int(self.m.find_first_unset()),
                self.naive_range(0, BITS),
                "step {step}: first"
            );
        }
        fn check_invariants(&self, step: usize) {
            assert_eq!(self.m.count(), self.want_count, "step {step}");
            assert_eq!(self.m.is_full(), self.want_count == BITS, "step {step}");
        }
    }

    #[test]
    fn get_index() {
        let b = |l0, l0_bit, l1, l1_bit| BitIndex {
            l0,
            l0_bit,
            l1,
            l1_bit,
        };
        for (i, want) in [
            (0, b(0, 0, 0, 0)),
            (63, b(0, 63, 0, 0)),
            (64, b(1, 0, 0, 1)),
            (127, b(1, 63, 0, 1)),
            (4095, b(63, 63, 0, 63)),
            (4096, b(64, 0, 1, 0)),
            (65535, b(1023, 63, 15, 63)),
        ] {
            assert_eq!(index(i), want, "{i}");
        }
    }

    #[test]
    fn basic() {
        let mut m = Map64k::new();
        assert_eq!(m.count(), 0);
        assert!(!m.is_full());
        assert_eq!(m.find_first_unset(), Some(0));
        assert!(!m.get(0));
        m.set(0);
        assert_eq!(m.count(), 1);
        assert!(!m.is_full());
        assert!(m.get(0));
        assert_eq!(m.find_first_unset(), Some(1));
        m.set(0);
        assert_eq!(m.count(), 1, "redundant set");
        m.clear(0);
        assert_eq!(m.count(), 0);
        assert!(!m.get(0));
        assert_eq!(m.find_first_unset(), Some(0));
        m.clear(0);
        assert_eq!(m.count(), 0, "redundant clear");
    }

    #[test]
    fn get() {
        let mut m = Map64k::new();
        for i in [0u16, 1, 63, 64, 127, 1024, 4095, 4096, 65534, 65535] {
            assert!(!m.get(i));
            m.set(i);
            assert!(m.get(i));
            m.clear(i);
            assert!(!m.get(i));
        }
    }

    #[test]
    fn hierarchy_propagation() {
        let mut m = Map64k::new();
        fill(&mut m, 0, 64);
        assert_ne!(m.l1[0] & 1, 0);
        assert_eq!(m.l2, 0);
        m.clear(10);
        assert_eq!(m.l1[0] & 1, 0);
        m.set(10);
        assert_ne!(m.l1[0] & 1, 0);
        fill(&mut m, 64, 4096);
        assert_ne!(m.l2 & 1, 0);
        m.clear(2000);
        assert_eq!(m.l2 & 1, 0);
    }

    #[test]
    fn find_first_unset_from_empty() {
        let m = Map64k::new();
        for i in [0u16, 1, 63, 64, 100, 1023, 1024, 4095, 4096, 65535] {
            assert_eq!(m.find_first_unset_from(i), Some(i));
        }
    }

    #[test]
    fn find_first_unset_from_full() {
        let mut m = Map64k::new();
        fill(&mut m, 0, BITS);
        assert_eq!(m.count(), BITS);
        assert!(m.is_full());
        assert_eq!(m.find_first_unset(), None);
        for i in [0u16, 1, 100, 4096, 65535] {
            assert_eq!(m.find_first_unset_from(i), None);
        }
    }

    #[test]
    fn find_first_unset_from_single_hole() {
        let mut m = Map64k::new();
        fill(&mut m, 0, BITS);
        for hole in [0u16, 1, 63, 64, 127, 128, 4095, 4096, 32768, 65534, 65535] {
            m.clear(hole);
            assert!(!m.is_full());
            assert_eq!(m.find_first_unset(), Some(hole));
            for start in [0, hole, hole.wrapping_add(1), 65535] {
                assert_eq!(
                    m.find_first_unset_from(start),
                    Some(hole),
                    "hole {hole} start {start}"
                );
            }
            m.set(hole);
            assert!(m.is_full());
        }
    }

    #[test]
    fn find_first_unset_from_levels() {
        let check = |m: &Map64k, start: u16, want: u16| {
            assert_eq!(m.find_first_unset_from(start), Some(want), "from {start}");
        };
        // Same level-0 word.
        let mut m = Map64k::new();
        fill(&mut m, 0, 10);
        for s in [0, 5, 10] {
            check(&m, s, 10);
        }
        // Next level-0 word in the same level-1 word.
        let mut m = Map64k::new();
        fill(&mut m, 0, 69);
        for s in [0, 63, 64] {
            check(&m, s, 69);
        }
        // Next level-1 word from level 2.
        let mut m = Map64k::new();
        fill(&mut m, 0, 4100);
        for s in [0, 2000, 4095, 4096] {
            check(&m, s, 4100);
        }
        fill(&mut m, 4100, 8200);
        for s in [0, 4095, 4096, 8191, 8192] {
            check(&m, s, 8200);
        }
        // Wrap around.
        let mut m = Map64k::new();
        fill(&mut m, 100, BITS);
        fill(&mut m, 0, 10);
        for s in [100, 65535] {
            check(&m, s, 10);
        }
        // Wrap around with the last bit set.
        let mut m = Map64k::new();
        m.set(65535);
        check(&m, 65535, 0);
    }

    #[test]
    fn find_first_unset_range() {
        let mut m = Map64k::new();
        assert_eq!(m.find_first_unset_range(5, 5), None);
        assert_eq!(m.find_first_unset_range(10, 5), None);
        fill(&mut m, 10, 20);
        assert_eq!(m.find_first_unset_range(10, 20), None);
        assert_eq!(m.find_first_unset_range(10, 25), Some(20));
        assert_eq!(m.find_first_unset_range(5, 20), Some(5));
        fill(&mut m, 60, 64);
        assert_eq!(m.find_first_unset_range(60, 64), None);
        assert_eq!(m.find_first_unset_range(60, 70), Some(64));
        fill(&mut m, 4090, 4096);
        assert_eq!(m.find_first_unset_range(4090, 4096), None);
        assert_eq!(m.find_first_unset_range(4090, 4100), Some(4096));
        assert_eq!(m.find_first_unset_range(0, BITS), Some(0));
        fill(&mut m, 0, BITS);
        assert_eq!(m.find_first_unset_range(0, BITS), None);
        assert_eq!(m.find_first_unset_range(100, 200), None);
    }

    #[test]
    fn find_first_unset_range_cutoffs() {
        let mut m = Map64k::new();
        fill(&mut m, 0, 128);
        assert_eq!(m.find_first_unset_range(0, 100), None);
        assert_eq!(m.find_first_unset_range(0, 128), None);
        assert_eq!(m.find_first_unset_range(0, 129), Some(128));
        let mut m = Map64k::new();
        fill(&mut m, 0, 8192);
        assert_eq!(m.find_first_unset_range(100, 5000), None);
        assert_eq!(m.find_first_unset_range(100, 8192), None);
        assert_eq!(m.find_first_unset_range(100, 9000), Some(8192));
    }

    fn random_range(r: &mut StdRng) -> (u16, usize) {
        let start = r.random_range(0..BITS);
        let end = start + r.random_range(0..=BITS - start);
        (start as u16, end)
    }

    #[test]
    fn differential() {
        let mut rf = RefMap::new();
        let mut r = StdRng::seed_from_u64(42);
        for step in 0..5000 {
            match r.random_range(0..12) {
                0..3 => {
                    let i = r.random_range(0..BITS) as u16;
                    rf.set(i);
                    assert!(rf.m.get(i));
                }
                3..5 => {
                    let i = r.random_range(0..BITS) as u16;
                    rf.clear(i);
                    assert!(!rf.m.get(i));
                }
                5 => {
                    let base = r.random_range(0..L0_WORDS) * WORD;
                    (0..WORD).for_each(|i| rf.set((base + i) as u16));
                }
                6 => {
                    let base = r.random_range(0..L0_WORDS) * WORD;
                    (0..WORD).for_each(|i| rf.clear((base + i) as u16));
                }
                7 => {
                    let base = r.random_range(0..L1_WORDS) * WORD * WORD;
                    let set = r.random_range(0..2) == 0;
                    for i in 0..WORD * WORD {
                        if set {
                            rf.set((base + i) as u16)
                        } else {
                            rf.clear((base + i) as u16)
                        }
                    }
                }
                8..10 => {
                    let (s, e) = random_range(&mut r);
                    rf.check_range(step, s, e);
                }
                _ => rf.check_from(step, r.random_range(0..BITS) as u16),
            }
            rf.check_invariants(step);
        }
    }

    #[test]
    fn differential_dense() {
        let mut rf = RefMap::new();
        let mut r = StdRng::seed_from_u64(99);
        for i in 0..BITS {
            rf.set(i as u16);
        }
        let mut holes: Vec<u16> = Vec::new();
        for _ in 0..16 {
            let i = r.random_range(0..BITS) as u16;
            rf.clear(i);
            holes.push(i);
        }
        for step in 0..5000 {
            match r.random_range(0..10) {
                0..3 => {
                    if holes.is_empty() {
                        rf.set(r.random_range(0..BITS) as u16);
                    } else {
                        let h = r.random_range(0..holes.len());
                        rf.set(holes.swap_remove(h));
                    }
                }
                3..5 => {
                    let i = r.random_range(0..BITS) as u16;
                    rf.clear(i);
                    holes.push(i);
                }
                5..7 => {
                    let (s, e) = random_range(&mut r);
                    rf.check_range(step, s, e);
                }
                _ => rf.check_from(step, r.random_range(0..BITS) as u16),
            }
            rf.check_invariants(step);
        }
    }
}

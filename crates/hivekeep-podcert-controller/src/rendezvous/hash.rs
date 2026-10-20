//! Rendezvous (highest random weight) hashing.

const FNV64_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV64_PRIME: u64 = 0x0000_0100_0000_01b3;

/// 64-bit FNV-1a.
pub fn fnv1a64(data: &[u8]) -> u64 {
    data.iter().fold(FNV64_OFFSET, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(FNV64_PRIME)
    })
}

/// The replica `item` belongs to: the one whose key hashed together with
/// the item weighs most, ties going to the lexicographically smallest
/// replica. Empty when there are no replicas.
pub fn assign<'a>(item: &str, replicas: impl IntoIterator<Item = &'a str>) -> &'a str {
    let mut best: Option<(u64, &str)> = None;
    for replica in replicas {
        let weight = fnv1a64(format!("{item}{replica}").as_bytes());
        best = match best {
            Some((w, r)) if w > weight || (w == weight && r <= replica) => Some((w, r)),
            _ => Some((weight, replica)),
        };
    }
    best.map(|(_, r)| r).unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a64_matches_reference_values() {
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a64(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn assignment_is_order_independent_and_stable() {
        let replicas = ["a", "b", "c", "d"];
        let mut reversed = replicas;
        reversed.reverse();
        for i in 0..100 {
            let item = format!("ns/item-{i}");
            let owner = assign(&item, replicas);
            assert!(replicas.contains(&owner));
            assert_eq!(owner, assign(&item, reversed));
            // Removing another replica never moves the item.
            let others: Vec<&str> = replicas.iter().copied().filter(|r| *r != owner).collect();
            let fewer: Vec<&str> = replicas
                .iter()
                .copied()
                .filter(|r| *r != others[0])
                .collect();
            assert_eq!(owner, assign(&item, fewer));
        }
    }

    #[test]
    fn assignment_without_replicas_is_empty() {
        assert_eq!(assign("item", []), "");
        assert_eq!(assign("item", ["only"]), "only");
    }

    #[test]
    fn ties_go_to_the_smallest_replica() {
        // The same name twice weighs the same; either order picks it.
        assert_eq!(assign("item", ["x", "x"]), "x");
    }
}

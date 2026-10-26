//! What the two programs of the fake data plane share: how a fake Worker's
//! stand-in pod is named and placed, and how each side checks the other's
//! identity on the capacity relay between them.

use sha2::{Digest, Sha256};

pub use crate::tls::peer_id::verify_peer_id;

/// Bounds the run prefix so every generated name stays well inside the
/// 63-character limit on pod names.
pub const MAX_RUN_LENGTH: usize = 8;

/// How many Actors a fake Worker holds: the worker's own `--max-actors`
/// default, which no WorkerPool field sets for a real worker.
pub const DEFAULT_MAX_ACTORS: i32 = 1000;

/// On a WorkerPool, overrides [`DEFAULT_MAX_ACTORS`] for its fake Workers.
pub const MAX_ACTORS_ANNOTATION: &str = "hivekeep.io/fake-max-actors";

/// The label key a pool's worker pods are selected by; a fake pool's
/// `status.selector` names it the same way.
pub const WORKER_POOL_LABEL: &str = "hivekeep.io/worker-pool";

/// Default `--relay-port` of both the fake keeper, which serves capacity
/// reports on it, and fake-workersync, which sends them there.
pub const DEFAULT_RELAY_PORT: u16 = 8086;

/// Why a run prefix was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("run prefix {0:?} must be 1 to {MAX_RUN_LENGTH} lowercase letters or digits")]
pub struct InvalidRun(pub String);

/// Checks that `run` is usable as a run prefix.
pub fn validate_run(run: &str) -> Result<(), InvalidRun> {
    let ok = !run.is_empty()
        && run.len() <= MAX_RUN_LENGTH
        && run
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    if ok {
        Ok(())
    } else {
        Err(InvalidRun(run.to_string()))
    }
}

/// The pod name prefix every fake Worker of `run` shares.
pub fn prefix(run: &str) -> String {
    format!("fake-{run}-")
}

/// The stand-in pod name of the `index`-th fake Worker of a WorkerPool, the
/// same for every Worker that fills that index. The pool enters as a hash
/// because its namespace and name together can exceed the 63-character
/// limit on pod names.
pub fn name(run: &str, namespace: &str, pool: &str, index: usize) -> String {
    format!("{}{index}", pool_prefix(run, namespace, pool))
}

/// The index of the fake Worker whose pod is `name` in a WorkerPool, or
/// `None` when the name is not one of that pool's fake pods. Only the
/// canonical decimal form matches.
pub fn index(run: &str, namespace: &str, pool: &str, name: &str) -> Option<usize> {
    let rest = name.strip_prefix(&pool_prefix(run, namespace, pool))?;
    let i: usize = rest.parse().ok()?;
    (i.to_string() == rest).then_some(i)
}

fn pool_prefix(run: &str, namespace: &str, pool: &str) -> String {
    let sum = Sha256::digest(format!("{namespace}/{pool}").as_bytes());
    format!("{}{}-", prefix(run), &hex::encode(sum)[..8])
}

/// The node the `index`-th fake Worker of a pool is placed on: round robin
/// over `nodes`, which the caller keeps sorted and non-empty.
pub fn node(index: usize, nodes: &[String]) -> &str {
    &nodes[index % nodes.len()]
}

/// The address recorded for the `index`-th fake Worker of a pool: a
/// documentation-range address nothing answers on. Addresses repeat past
/// 254 Workers, which hivekeep-api allows.
pub fn ip(index: usize) -> String {
    format!("192.0.2.{}", index % 254 + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Names are pinned: a change renames every fake Worker's pod, which
    // strands the Workers a previous fake-workersync registered.
    #[test]
    fn name_pinned() {
        assert_eq!(
            name("r1", "benchmark-workloads", "benchmark-worker", 0),
            "fake-r1-4ce21861-0"
        );
    }

    fn is_short_name(s: &str) -> bool {
        let b = s.as_bytes();
        let ok = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit();
        !b.is_empty() && ok(b[0]) && ok(b[b.len() - 1]) && b.iter().all(|&c| ok(c) || c == b'-')
    }

    #[test]
    fn name_is_a_short_name_at_the_bounds() {
        let n = name(
            &"z".repeat(MAX_RUN_LENGTH),
            &"n".repeat(63),
            &"p".repeat(253),
            99999,
        );
        assert!(
            n.len() <= 63 && is_short_name(&n),
            "{n} ({} chars)",
            n.len()
        );
    }

    #[test]
    fn names_distinct_across_pools() {
        let a = name("r1", "ns", "pool-a", 0);
        let b = name("r1", "ns", "pool-b", 0);
        let c = name("r1", "other", "pool-a", 0);
        assert!(a != b && a != c && b != c, "{a} {b} {c}");
    }

    #[test]
    fn index_round_trips() {
        for i in [0, 7, 1234] {
            assert_eq!(
                index("r1", "ns", "pool", &name("r1", "ns", "pool", i)),
                Some(i)
            );
        }
        for n in [
            name("r1", "ns", "other", 3),
            format!("{}x", name("r1", "ns", "pool", 3)),
            format!("{}01", name("r1", "ns", "pool", 0).trim_end_matches('0')),
            name("r2", "ns", "pool", 3),
            "benchmark-worker-5d8f-xyz".to_string(),
        ] {
            assert_eq!(index("r1", "ns", "pool", &n), None, "{n}");
        }
    }

    #[test]
    fn node_round_robin() {
        let nodes: Vec<String> = ["a", "b", "c"].map(String::from).to_vec();
        let got: Vec<&str> = (0..5).map(|i| node(i, &nodes)).collect();
        assert_eq!(got.join(","), "a,b,c,a,b");
    }

    #[test]
    fn ip_in_documentation_range() {
        for i in [0, 253, 254, 1000] {
            let addr: std::net::Ipv4Addr = ip(i).parse().unwrap();
            let o = addr.octets();
            assert!(
                o[..3] == [192, 0, 2] && o[3] != 0 && o[3] != 255,
                "ip({i}) = {addr}"
            );
        }
        assert_eq!(ip(0), ip(254));
    }

    #[test]
    fn validate_run_cases() {
        for run in ["r", "abc12345"] {
            assert!(validate_run(run).is_ok(), "{run}");
        }
        for run in ["", "abc123456", "Run", "a-b", "a_b"] {
            assert!(validate_run(run).is_err(), "{run}");
        }
    }

    #[test]
    fn verify_peer_id_cases() {
        const WANT: &str = "spiffe://cluster.local/ns/hivekeep-system/sa/fake-workersync";
        let cert = |uri: Option<&str>| {
            let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
            if let Some(u) = uri {
                p.subject_alt_names = vec![rcgen::SanType::URI(u.try_into().unwrap())];
            }
            let key = rcgen::KeyPair::generate().unwrap();
            p.self_signed(&key).unwrap().der().to_vec()
        };
        assert!(verify_peer_id(&[cert(Some(WANT))], WANT).is_ok());
        assert!(verify_peer_id(
            &[cert(Some(
                "spiffe://cluster.local/ns/hivekeep-system/sa/keeper"
            ))],
            WANT
        )
        .is_err());
        assert!(verify_peer_id(&[cert(None)], WANT).is_err());
        assert!(verify_peer_id(&[] as &[Vec<u8>], WANT).is_err());
    }
}

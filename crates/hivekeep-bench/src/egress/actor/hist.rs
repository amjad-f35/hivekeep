//! The distributions an actor reports: fixed log-spaced bucket bounds and
//! the [`Point`] a driver merges across actors and reads quantiles from.

use std::cmp::Ordering;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

const BUCKETS_PER_DECADE: usize = 20;
/// The first bound at or past this many seconds is the last; slower samples
/// count in the overflow bucket.
const MAX_BUCKET_SECONDS: f64 = 60.0;

/// 10^(j/20) for j = 0..19, to four significant digits.
const MANTISSAS: [&str; BUCKETS_PER_DECADE] = [
    "1", "1.122", "1.259", "1.413", "1.585", "1.778", "1.995", "2.239", "2.512", "2.818", "3.162",
    "3.548", "3.981", "4.467", "5.012", "5.623", "6.31", "7.079", "7.943", "8.913",
];

/// Upper bounds, in seconds, of every distribution: 20 per decade from 1 µs
/// to 63.1 s. Each is parsed from a decimal string, so every build holds the
/// same 157 values; neighbours differ by about 12%, the resolution of a
/// quantile read from buckets.
pub static BOUNDS: LazyLock<Vec<f64>> = LazyLock::new(|| {
    let mut bounds = Vec::new();
    for k in 0.. {
        let exp = (k / BUCKETS_PER_DECADE) as i64 - 6;
        let b: f64 = format!("{}e{exp}", MANTISSAS[k % BUCKETS_PER_DECADE])
            .parse()
            .expect("bound parses");
        bounds.push(b);
        if b >= MAX_BUCKET_SECONDS {
            break;
        }
    }
    bounds
});

/// One of the four distributions of a Stats, the key of its [`Point`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Distribution {
    /// End-to-end time of an endpoint's first request, when it succeeded.
    Opening,
    /// End-to-end time of later successful requests.
    Latency,
    /// Lookup time, for requests that did a lookup.
    Dns,
    /// Handshake time, for handshakes that completed.
    Tls,
}

impl Distribution {
    /// Every distribution.
    pub const ALL: [Distribution; 4] = [
        Distribution::Opening,
        Distribution::Latency,
        Distribution::Dns,
        Distribution::Tls,
    ];

    /// The wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Distribution::Opening => "opening",
            Distribution::Latency => "latency",
            Distribution::Dns => "dns",
            Distribution::Tls => "tls",
        }
    }

    /// Index into a per-distribution array.
    pub(crate) fn index(self) -> usize {
        self as usize
    }
}

// Maps keyed by distribution sort by wire name, as the drivers' JSON does.
impl Ord for Distribution {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialOrd for Distribution {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn is_zero_f64(v: &f64) -> bool {
    *v == 0.0
}

/// One distribution over one window: what a driver adds across actors. The
/// default Point is empty.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Point {
    /// Upper bounds of the finite buckets; `bucket_counts` has one more
    /// entry, the overflow bucket. Counts are per bucket, not cumulative.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bounds: Vec<f64>,
    /// Samples per bucket.
    #[serde(
        default,
        rename = "bucketCounts",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub bucket_counts: Vec<u64>,
    /// Number of samples.
    #[serde(default)]
    pub count: u64,
    /// Sum of the samples, in seconds.
    #[serde(default)]
    pub sum: f64,
    /// Smallest sample.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub min: f64,
    /// Largest sample.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub max: f64,
}

impl Point {
    /// Adds `o` into `self`. A point whose bounds differ from this one's, or
    /// whose counts do not fit its bounds, is refused and `self` is left as
    /// it was.
    pub fn merge(&mut self, o: &Point) -> Result<(), String> {
        if o.count == 0 {
            return Ok(());
        }
        if o.bucket_counts.len() != o.bounds.len() + 1 {
            return Err(format!(
                "{} bucket counts for {} bounds",
                o.bucket_counts.len(),
                o.bounds.len()
            ));
        }
        if self.count == 0 {
            *self = o.clone();
            return Ok(());
        }
        if self.bucket_counts.len() != self.bounds.len() + 1 {
            return Err(format!(
                "receiver has {} bucket counts for {} bounds",
                self.bucket_counts.len(),
                self.bounds.len()
            ));
        }
        if self.bounds.len() != o.bounds.len() {
            return Err(format!(
                "{} bounds, want {}",
                o.bounds.len(),
                self.bounds.len()
            ));
        }
        for (i, (a, b)) in self.bounds.iter().zip(&o.bounds).enumerate() {
            if a != b {
                return Err(format!(
                    "bounds differ at index {i}: {} vs {}",
                    go_g(*a),
                    go_g(*b)
                ));
            }
        }
        for (a, b) in self.bucket_counts.iter_mut().zip(&o.bucket_counts) {
            *a += b;
        }
        self.count += o.count;
        self.sum += o.sum;
        self.min = self.min.min(o.min);
        self.max = self.max.max(o.max);
        Ok(())
    }

    /// The upper bound of the bucket holding the q-th sample, capped at the
    /// largest sample seen; `None` for an empty point.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let rank = ((q * self.count as f64).ceil() as u64).max(1);
        let mut seen = 0;
        for (i, c) in self.bucket_counts.iter().enumerate() {
            seen += c;
            if seen < rank {
                continue;
            }
            if i == self.bounds.len() {
                return Some(self.max); // the overflow bucket: only the maximum bounds it
            }
            return Some(self.bounds[i].min(self.max));
        }
        Some(self.max)
    }

    /// The average sample, or 0 for an empty point.
    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    /// Adds one sample, bucketed as the recorder buckets it: into the first
    /// bucket whose bound is at least the sample.
    pub fn record(&mut self, seconds: f64) {
        if self.count == 0 {
            self.bounds = BOUNDS.clone();
            self.bucket_counts = vec![0; BOUNDS.len() + 1];
            self.min = seconds;
            self.max = seconds;
        }
        let i = bucket_index(&self.bounds, seconds);
        self.bucket_counts[i] += 1;
        self.count += 1;
        self.sum += seconds;
        self.min = self.min.min(seconds);
        self.max = self.max.max(seconds);
    }
}

/// The bucket a sample goes in: the first bound at or above it, or the
/// overflow bucket past the last bound.
pub(crate) fn bucket_index(bounds: &[f64], seconds: f64) -> usize {
    bounds.partition_point(|b| *b < seconds)
}

/// Formats a float in the `%v` style used in merge errors: shortest digits,
/// exponent form with a two-digit exponent below 1e-4 or from 1e21 up.
fn go_g(f: f64) -> String {
    let abs = f.abs();
    if abs != 0.0 && !(1e-4..1e21).contains(&abs) {
        let s = format!("{f:e}");
        let (m, e) = s.split_once('e').unwrap_or((&s, "0"));
        let (sign, digits) = match e.strip_prefix('-') {
            Some(d) => ('-', d),
            None => ('+', e),
        };
        format!("{m}e{sign}{digits:0>2}")
    } else {
        format!("{f}")
    }
}

#[cfg(test)]
pub(crate) fn point_with(samples: &[f64]) -> Point {
    let mut p = Point::default();
    for s in samples {
        p.record(*s);
    }
    p
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn bounds() {
        assert_eq!(BOUNDS.len(), 157);
        assert_eq!((BOUNDS[0], BOUNDS[60], BOUNDS[156]), (1e-6, 0.001, 63.1));
        for (i, b) in BOUNDS.iter().enumerate() {
            if i > 0 {
                assert!(*b > BOUNDS[i - 1], "bound {i} not increasing");
            }
            let again: f64 = format!("{:.3e}", b).parse().unwrap();
            assert_eq!(
                again, *b,
                "bound {i} = {b} has more than four significant digits"
            );
        }
    }

    #[test]
    fn point_json_round_trip() {
        let mut input = BTreeMap::new();
        input.insert(Distribution::Tls, point_with(&[0.006, 0.009]));
        input.insert(Distribution::Dns, Point::default());
        let b = serde_json::to_vec(&input).unwrap();
        let back: BTreeMap<Distribution, Point> = serde_json::from_slice(&b).unwrap();
        assert_eq!(back, input);
    }

    #[test]
    fn point_merge_and_quantile() {
        let mut a = Point::default();
        let mut b = Point::default();
        for i in 1..=90 {
            a.record(i as f64 * 0.001);
        }
        for _ in 0..10 {
            b.record(5.0);
        }
        let mut sum = Point::default();
        for p in [&a, &b, &Point::default()] {
            sum.merge(p).unwrap();
        }
        assert_eq!((sum.count, sum.max, sum.min), (100, 5.0, 0.001));
        let p50 = sum.quantile(0.5).unwrap();
        let p99 = sum.quantile(0.99).unwrap();
        let p999 = sum.quantile(0.999).unwrap();
        assert!(
            (0.045..=0.0502).contains(&p50) && p99 == 5.0 && p999 == 5.0,
            "{p50} {p99} {p999}"
        );
        let mean = sum.mean();
        assert!((0.54..=0.55).contains(&mean), "{mean}");
        assert_eq!(Point::default().mean(), 0.0);
    }

    #[test]
    fn point_quantile() {
        let edge = point_with(&[0.001, 0.001, 0.002, 0.002, 0.002]);
        let cases: Vec<(&str, Point, f64, Option<f64>)> = vec![
            ("empty", Point::default(), 0.5, None),
            (
                "one sample is its own bound",
                point_with(&[0.0015]),
                0.5,
                Some(0.0015),
            ),
            ("rank on a bucket edge", edge.clone(), 0.4, Some(0.001)),
            (
                "rank just past a bucket edge",
                edge.clone(),
                0.41,
                Some(0.002),
            ),
            ("q of 0 is the first bucket", edge.clone(), 0.0, Some(0.001)),
            ("q of 1 is the last bucket", edge.clone(), 1.0, Some(0.002)),
            (
                "overflow bucket is the maximum",
                point_with(&[0.001, 100.0]),
                0.99,
                Some(100.0),
            ),
            (
                "capped at the maximum",
                point_with(&[0.001, 0.0021]),
                1.0,
                Some(0.0021),
            ),
        ];
        for (name, p, q, want) in cases {
            assert_eq!(p.quantile(q), want, "{name}");
        }
    }

    #[test]
    fn point_merge() {
        let full = point_with(&[0.25, 0.5]);
        let mut other = point_with(&[0.125]);
        other.bounds[0] = 0.5e-6;
        let mut short = point_with(&[0.125]);
        short.bucket_counts.pop();
        let mut fewer = point_with(&[0.125]);
        fewer.bounds.truncate(10);
        fewer.bucket_counts.truncate(11);
        let cases: Vec<(&str, Point, Point, Point, &str)> = vec![
            (
                "empty into empty",
                Point::default(),
                Point::default(),
                Point::default(),
                "",
            ),
            (
                "empty into full",
                full.clone(),
                Point::default(),
                full.clone(),
                "",
            ),
            (
                "full into empty",
                Point::default(),
                full.clone(),
                full.clone(),
                "",
            ),
            (
                "equal bounds",
                full.clone(),
                point_with(&[0.125, 2.0]),
                point_with(&[0.25, 0.5, 0.125, 2.0]),
                "",
            ),
            (
                "different bounds refused, naming the index",
                full.clone(),
                other,
                full.clone(),
                "bounds differ at index 0: 1e-06 vs 5e-07",
            ),
            (
                "fewer bounds refused, naming both lengths",
                full.clone(),
                fewer,
                full.clone(),
                "10 bounds, want 157",
            ),
            (
                "counts that do not fit the bounds refused",
                full.clone(),
                short.clone(),
                full.clone(),
                "157 bucket counts for 157 bounds",
            ),
            (
                "receiver that does not fit its bounds refused",
                short.clone(),
                full.clone(),
                short,
                "receiver has 157 bucket counts for 157 bounds",
            ),
        ];
        for (name, into, from, want, want_err) in cases {
            let mut got = into;
            let res = got.merge(&from);
            match res {
                Ok(()) => assert!(want_err.is_empty(), "{name}: no error, want {want_err:?}"),
                Err(e) => assert!(
                    !want_err.is_empty() && e.contains(want_err),
                    "{name}: {e:?}, want {want_err:?}"
                ),
            }
            assert_eq!(got, want, "{name}");
        }
        // A merge into an empty point copies, so the source's buckets stay its own.
        let mut sum = Point::default();
        sum.merge(&full).unwrap();
        let i = bucket_index(&BOUNDS, 0.25);
        sum.bucket_counts[i] += 1;
        assert_eq!(full.bucket_counts[i], 1);
    }
}

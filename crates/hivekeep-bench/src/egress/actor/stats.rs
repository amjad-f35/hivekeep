//! What one actor's loop measured, as `/stats`, `/collect` and `/stop`
//! serve it. Durations are integer nanoseconds on the wire.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::hist::{Distribution, Point};

/// Serde for a [`Duration`] as integer nanoseconds.
pub mod nanos {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    /// Writes `d` as nanoseconds.
    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_i64(d.as_nanos().min(i64::MAX as u128) as i64)
    }

    /// Reads nanoseconds; negative values read as zero.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let n = i64::deserialize(d)?;
        Ok(Duration::from_nanos(n.max(0) as u64))
    }
}

/// A loop's counters (cumulative since `/start`) and, on `/collect` and
/// `/stop`, the distributions recorded since the previous cut.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    /// The scheme the loop requested endpoints over.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub scheme: String,
    /// Time since `/start`.
    #[serde(default, with = "nanos")]
    pub elapsed: Duration,
    /// Every request the loop finished, failed ones included.
    #[serde(default)]
    pub requests: i64,
    /// Requests answered 200.
    #[serde(default)]
    pub successes: i64,
    /// Requests that went out on a connection opened for them rather than a
    /// kept-alive one. A tunnel the gateway reset shows up here rather than
    /// in `errors`: the client redials and retries a GET that failed before
    /// any of the response arrived.
    #[serde(default)]
    pub new_conns: i64,
    /// Endpoints whose first request has completed.
    #[serde(default)]
    pub opened: i64,
    /// `elapsed` when the last endpoint's first request completed; zero
    /// until every endpoint has opened.
    #[serde(default, with = "nanos")]
    pub opening_done: Duration,
    /// `requests` at that same moment, so this actor's opening can be priced
    /// alone; zero until every endpoint has opened.
    #[serde(default)]
    pub requests_at_opening_done: i64,
    /// Requests completed by the least-visited endpoint: whether the actor is
    /// still making progress.
    #[serde(default)]
    pub rounds: i64,
    /// Longest time between the starts of two consecutive requests on one
    /// endpoint: how quiet the loop has left a tunnel. Keep the period under
    /// the gateway's stream idle timeout.
    #[serde(default, with = "nanos")]
    pub longest_gap: Duration,
    /// Failed requests by class (one low-cardinality class per failure mode).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub errors: BTreeMap<String, i64>,
    /// Per-endpoint counts, by endpoint number; only with `?endpoints=1`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<Endpoint>,
    /// Samples since the previous cut; a distribution with no sample in the
    /// window is absent. `/stats` leaves them out.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub distributions: BTreeMap<Distribution, Point>,
}

/// The counts for one endpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Endpoint {
    /// Requests finished.
    #[serde(default)]
    pub requests: i64,
    /// Requests failed.
    #[serde(default)]
    pub errors: i64,
    /// [`Stats::longest_gap`] for this endpoint alone, so a quiet tunnel can
    /// be named.
    #[serde(default, with = "nanos")]
    pub longest_gap: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::egress::actor::api::{SCHEME_HTTP, SCHEME_HTTPS};
    use crate::egress::json;

    /// Pretty JSON with each array of numbers folded onto one line.
    fn marshal_golden(v: &Stats) -> String {
        let b = String::from_utf8(json::to_vec_pretty(v).unwrap()).unwrap();
        let re = regex::Regex::new(r"\[\s+(?:-?[0-9.e+-]+,?\s+)+\]").unwrap();
        re.replace_all(&b, |c: &regex::Captures| {
            let m = &c[0];
            format!(
                "[{}]",
                m[1..m.len() - 1]
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            )
        })
        .into_owned()
    }

    /// A fixed run: 60 requests over 4 endpoints, a few 503s and timeouts,
    /// and two distributions in the window.
    fn scripted_stats() -> Stats {
        const C: usize = 4;
        let mut s = Stats {
            scheme: SCHEME_HTTPS.into(),
            elapsed: Duration::from_millis(61200),
            opening_done: Duration::from_millis(350),
            requests_at_opening_done: C as i64,
            longest_gap: Duration::from_millis(400),
            endpoints: vec![Endpoint::default(); C],
            ..Default::default()
        };
        let (mut opening, mut latency) = (Point::default(), Point::default());
        for n in 0..60usize {
            let i = n % C;
            s.requests += 1;
            s.endpoints[i].requests += 1;
            s.endpoints[i].longest_gap = Duration::from_millis(400);
            if n < C {
                s.new_conns += 1;
                s.opened += 1;
            }
            let seconds = Duration::from_millis(1 + n as u64).as_secs_f64();
            if n % 17 == 3 {
                *s.errors.entry("HTTP 503".into()).or_default() += 1;
                s.endpoints[i].errors += 1;
            } else if n % 23 == 5 {
                *s.errors.entry("timeout".into()).or_default() += 1;
                s.endpoints[i].errors += 1;
            } else if n < C {
                s.successes += 1;
                opening.record(seconds);
            } else {
                s.successes += 1;
                latency.record(seconds);
            }
        }
        s.rounds = 60 / C as i64;
        s.distributions.insert(Distribution::Opening, opening);
        s.distributions.insert(Distribution::Latency, latency);
        s
    }

    #[test]
    fn stats_golden() {
        let input = scripted_stats();
        let got = marshal_golden(&input);
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/egress/collect.json");
        if std::env::var_os("UPDATE_GOLDEN").is_some() {
            std::fs::write(path, &got).unwrap();
        }
        let want = std::fs::read_to_string(path).unwrap();
        assert_eq!(
            got, want,
            "Stats JSON differs from {path} (set UPDATE_GOLDEN=1 to accept)"
        );
        let back: Stats = serde_json::from_str(&want).unwrap();
        assert_eq!(back, input);
    }

    #[test]
    fn stats_without_distributions_omits_them() {
        let s = Stats {
            scheme: SCHEME_HTTP.into(),
            requests: 3,
            ..Default::default()
        };
        let b = String::from_utf8(json::to_vec(&s).unwrap()).unwrap();
        for absent in ["distributions", "endpoints", "errors"] {
            assert!(
                !b.contains(absent),
                "counters-only Stats carries {absent:?}: {b}"
            );
        }
    }
}

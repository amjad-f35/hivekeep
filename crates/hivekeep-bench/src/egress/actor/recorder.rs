//! A loop's four distributions as delta histograms: each [`Recorder::collect`]
//! returns what was recorded since the previous one, so each call is one
//! window. Nothing is exported anywhere; the points travel inside Stats.

use std::collections::BTreeMap;

use super::hist::{bucket_index, Distribution, Point, BOUNDS};

#[derive(Debug, Clone, Default)]
struct Hist {
    counts: Vec<u64>,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

/// One loop's distributions.
#[derive(Debug)]
pub struct Recorder {
    hists: [Hist; 4],
    shut_down: bool,
}

/// A collect after [`Recorder::shutdown`].
#[derive(Debug, thiserror::Error)]
#[error("the recorder is shut down")]
pub struct ShutDown;

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

impl Recorder {
    /// A recorder with every distribution empty.
    pub fn new() -> Self {
        Recorder {
            hists: Default::default(),
            shut_down: false,
        }
    }

    /// Adds one sample, in seconds, to `name`. A no-op once shut down.
    pub fn record(&mut self, name: Distribution, seconds: f64) {
        if self.shut_down {
            return;
        }
        let h = &mut self.hists[name.index()];
        if h.count == 0 {
            h.counts = vec![0; BOUNDS.len() + 1];
            h.min = seconds;
            h.max = seconds;
        }
        h.counts[bucket_index(&BOUNDS, seconds)] += 1;
        h.count += 1;
        h.sum += seconds;
        h.min = h.min.min(seconds);
        h.max = h.max.max(seconds);
    }

    /// Each distribution's samples since the previous collect; one with no
    /// new sample is absent.
    pub fn collect(&mut self) -> Result<BTreeMap<Distribution, Point>, ShutDown> {
        if self.shut_down {
            return Err(ShutDown);
        }
        let mut out = BTreeMap::new();
        for name in Distribution::ALL {
            let h = std::mem::take(&mut self.hists[name.index()]);
            if h.count == 0 {
                continue;
            }
            out.insert(
                name,
                Point {
                    bounds: BOUNDS.clone(),
                    bucket_counts: h.counts,
                    count: h.count,
                    sum: h.sum,
                    min: h.min,
                    max: h.max,
                },
            );
        }
        Ok(out)
    }

    /// Releases the recorder: later records are dropped and collects fail.
    pub fn shutdown(&mut self) {
        self.shut_down = true;
        self.hists = Default::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_is_one_window() {
        let mut r = Recorder::new();
        for ms in [1u64, 2, 3] {
            r.record(
                Distribution::Latency,
                std::time::Duration::from_millis(ms).as_secs_f64(),
            );
        }
        let first = r.collect().unwrap();
        let p = &first[&Distribution::Latency];
        assert_eq!((p.count, p.min, p.max), (3, 0.001, 0.003));
        r.record(Distribution::Latency, 0.5);
        let second = r.collect().unwrap();
        let p = &second[&Distribution::Latency];
        assert_eq!((p.count, p.min, p.max), (1, 0.5, 0.5));
        assert!(r.collect().unwrap().is_empty());
    }

    #[test]
    fn idle_distributions_are_absent() {
        let mut r = Recorder::new();
        assert!(r.collect().unwrap().is_empty());
        r.record(Distribution::Dns, 0.002);
        let got = r.collect().unwrap();
        for name in Distribution::ALL {
            assert_eq!(
                got.contains_key(&name),
                name == Distribution::Dns,
                "{name:?}"
            );
        }
    }

    #[test]
    fn one_sample_lands_in_one_bucket() {
        let mut r = Recorder::new();
        r.record(Distribution::Opening, 0.001);
        let got = r.collect().unwrap();
        let p = &got[&Distribution::Opening];
        // 1 ms is a bound: the sample goes in the bucket whose bound is 0.001.
        assert_eq!(p.bucket_counts.len(), BOUNDS.len() + 1);
        assert_eq!((p.bucket_counts[60], p.count, p.sum), (1, 1, 0.001));
        assert_eq!(p.bounds, *BOUNDS);
        let b = serde_json::to_vec(&got).unwrap();
        let back: BTreeMap<Distribution, Point> = serde_json::from_slice(&b).unwrap();
        assert_eq!(back, got);
    }

    #[test]
    fn record_after_shutdown_does_not_panic() {
        let mut r = Recorder::new();
        r.record(Distribution::Tls, 0.01);
        r.shutdown();
        r.record(Distribution::Tls, 0.01);
        assert!(r.collect().is_err());
    }
}

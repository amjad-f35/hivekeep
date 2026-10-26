//! Request statistics in the form the locust master aggregates: per
//! (name, method) entries plus a total, with response times bucketed the
//! way locust rounds them, sent as deltas and reset after each report.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use rmpv::Value;
use sha2::{Digest, Sha256};

use super::message::map;

fn now_secs_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Rounds a response time the way locust buckets them: exact below
/// 100 ms, then to 2 significant digits up to 10 s, then to the second.
pub fn round_response_time(ms: i64) -> i64 {
    fn round_to(v: i64, step: i64) -> i64 {
        ((v as f64 / step as f64).round() as i64) * step
    }
    match ms {
        m if m < 100 => m,
        m if m < 1_000 => round_to(m, 10),
        m if m < 10_000 => round_to(m, 100),
        m => round_to(m, 1_000),
    }
}

/// One (name, method) row.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Entry {
    /// Request name.
    pub name: String,
    /// Request method (`grpc`, `GET`, ...).
    pub method: String,
    /// Requests, failures included.
    pub num_requests: i64,
    /// Failed requests.
    pub num_failures: i64,
    /// Sum of response times, ms.
    pub total_response_time: i64,
    /// Fastest response, ms; `None` before the first request.
    pub min_response_time: Option<i64>,
    /// Slowest response, ms.
    pub max_response_time: i64,
    /// Sum of response sizes.
    pub total_content_length: i64,
    /// Rounded response time -> count.
    pub response_times: BTreeMap<i64, i64>,
    /// Unix second -> requests.
    pub num_reqs_per_sec: BTreeMap<i64, i64>,
    /// Unix second -> failures.
    pub num_fail_per_sec: BTreeMap<i64, i64>,
    /// When this row started counting.
    pub start_time: f64,
    /// When the last request was logged.
    pub last_request_timestamp: Option<f64>,
}

impl Entry {
    fn new(name: &str, method: &str) -> Self {
        Entry {
            name: name.to_string(),
            method: method.to_string(),
            start_time: now_secs_f64(),
            ..Default::default()
        }
    }

    fn log(&mut self, response_time: i64, content_length: i64, now: f64) {
        self.num_requests += 1;
        *self.num_reqs_per_sec.entry(now as i64).or_default() += 1;
        self.last_request_timestamp = Some(now);
        self.total_response_time += response_time;
        self.min_response_time = Some(
            self.min_response_time
                .map_or(response_time, |m| m.min(response_time)),
        );
        self.max_response_time = self.max_response_time.max(response_time);
        self.total_content_length += content_length;
        *self
            .response_times
            .entry(round_response_time(response_time))
            .or_default() += 1;
    }

    fn log_failure(&mut self, now: f64) {
        self.num_failures += 1;
        *self.num_fail_per_sec.entry(now as i64).or_default() += 1;
    }

    /// The row's wire form.
    pub fn to_value(&self) -> Value {
        let int_map = |m: &BTreeMap<i64, i64>| {
            Value::Map(
                m.iter()
                    .map(|(k, v)| (Value::from(*k), Value::from(*v)))
                    .collect(),
            )
        };
        map([
            ("name", Value::from(self.name.as_str())),
            ("method", Value::from(self.method.as_str())),
            (
                "last_request_timestamp",
                self.last_request_timestamp
                    .map(Value::from)
                    .unwrap_or(Value::Nil),
            ),
            ("start_time", Value::from(self.start_time)),
            ("num_requests", Value::from(self.num_requests)),
            ("num_none_requests", Value::from(0)),
            ("num_failures", Value::from(self.num_failures)),
            ("total_response_time", Value::from(self.total_response_time)),
            ("max_response_time", Value::from(self.max_response_time)),
            (
                "min_response_time",
                self.min_response_time
                    .map(Value::from)
                    .unwrap_or(Value::Nil),
            ),
            (
                "total_content_length",
                Value::from(self.total_content_length),
            ),
            ("response_times", int_map(&self.response_times)),
            ("num_reqs_per_sec", int_map(&self.num_reqs_per_sec)),
            ("num_fail_per_sec", int_map(&self.num_fail_per_sec)),
        ])
    }
}

/// One distinct error.
#[derive(Debug, Clone, PartialEq)]
pub struct ErrorEntry {
    /// Request name.
    pub name: String,
    /// Request method.
    pub method: String,
    /// The error text.
    pub error: String,
    /// How often it happened.
    pub occurrences: i64,
}

/// Everything recorded since the last report.
#[derive(Debug, Clone)]
pub struct RequestStats {
    entries: BTreeMap<(String, String), Entry>,
    total: Entry,
    errors: BTreeMap<String, ErrorEntry>,
}

impl Default for RequestStats {
    fn default() -> Self {
        RequestStats {
            entries: BTreeMap::new(),
            total: Entry::new("Aggregated", ""),
            errors: BTreeMap::new(),
        }
    }
}

impl RequestStats {
    /// A successful request.
    pub fn log_success(
        &mut self,
        method: &str,
        name: &str,
        response_time: i64,
        content_length: i64,
    ) {
        let now = now_secs_f64();
        self.entry(method, name)
            .log(response_time, content_length, now);
        self.total.log(response_time, content_length, now);
    }

    /// A failed request: it counts as a request and as a failure.
    pub fn log_failure(&mut self, method: &str, name: &str, response_time: i64, error: &str) {
        let now = now_secs_f64();
        let e = self.entry(method, name);
        e.log(response_time, 0, now);
        e.log_failure(now);
        self.total.log(response_time, 0, now);
        self.total.log_failure(now);
        let key = hex::encode(Sha256::digest(
            format!("{method}.{name}.{error}").as_bytes(),
        ));
        self.errors
            .entry(key)
            .or_insert_with(|| ErrorEntry {
                name: name.to_string(),
                method: method.to_string(),
                error: error.to_string(),
                occurrences: 0,
            })
            .occurrences += 1;
    }

    fn entry(&mut self, method: &str, name: &str) -> &mut Entry {
        self.entries
            .entry((name.to_string(), method.to_string()))
            .or_insert_with(|| Entry::new(name, method))
    }

    /// The rows with activity, the total, and the errors.
    pub fn rows(&self) -> (Vec<&Entry>, &Entry, Vec<&ErrorEntry>) {
        (
            self.entries
                .values()
                .filter(|e| e.num_requests > 0 || e.num_failures > 0)
                .collect(),
            &self.total,
            self.errors.values().collect(),
        )
    }

    /// The `stats` message payload (without the user counts), resetting
    /// everything so the next report carries only what follows.
    pub fn take_report(&mut self) -> Vec<(String, Value)> {
        let stats = Value::Array(
            self.entries
                .values()
                .filter(|e| e.num_requests > 0 || e.num_failures > 0)
                .map(Entry::to_value)
                .collect(),
        );
        let total = self.total.to_value();
        let errors = Value::Map(
            self.errors
                .iter()
                .map(|(k, e)| {
                    (
                        Value::from(k.as_str()),
                        map([
                            ("name", Value::from(e.name.as_str())),
                            ("method", Value::from(e.method.as_str())),
                            ("error", Value::from(e.error.as_str())),
                            ("occurrences", Value::from(e.occurrences)),
                        ]),
                    )
                })
                .collect(),
        );
        *self = RequestStats::default();
        vec![
            ("stats".to_string(), stats),
            ("stats_total".to_string(), total),
            ("errors".to_string(), errors),
        ]
    }
}

static ENABLED: AtomicBool = AtomicBool::new(false);
static GLOBAL: LazyLock<Mutex<RequestStats>> =
    LazyLock::new(|| Mutex::new(RequestStats::default()));

/// Starts (or stops) collecting process-wide stats for the master. Off,
/// records are dropped: no run is reporting them.
pub fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::SeqCst);
    if !on {
        *GLOBAL.lock() = RequestStats::default();
    }
}

/// Records a successful request for the master's stats.
pub fn record_success(method: &str, name: &str, response_time_ms: i64, content_length: i64) {
    if ENABLED.load(Ordering::Relaxed) {
        GLOBAL
            .lock()
            .log_success(method, name, response_time_ms, content_length);
    }
}

/// Records a failed request for the master's stats.
pub fn record_failure(method: &str, name: &str, response_time_ms: i64, error: &str) {
    if ENABLED.load(Ordering::Relaxed) {
        GLOBAL
            .lock()
            .log_failure(method, name, response_time_ms, error);
    }
}

/// Takes the process-wide report (see [`RequestStats::take_report`]).
pub fn take_report() -> Vec<(String, Value)> {
    GLOBAL.lock().take_report()
}

#[cfg(test)]
mod tests {
    use super::super::message::{map_get, number};
    use super::*;

    #[test]
    fn rounding_matches_locust_buckets() {
        for (ms, want) in [
            (0, 0),
            (99, 99),
            (100, 100),
            (147, 150),
            (999, 1000),
            (1_234, 1_200),
            (9_950, 10_000),
            (12_345, 12_000),
        ] {
            assert_eq!(round_response_time(ms), want, "{ms}");
        }
    }

    #[test]
    fn report_carries_rows_total_and_errors_then_resets() {
        let mut s = RequestStats::default();
        s.log_success("grpc", "Ping", 5, 10);
        s.log_success("grpc", "Ping", 7, 10);
        s.log_failure("grpc", "Ping", 9, "boom");
        s.log_failure("grpc", "Ping", 9, "boom");
        {
            let (rows, total, errors) = s.rows();
            assert_eq!(rows.len(), 1);
            let r = rows[0];
            assert_eq!((r.num_requests, r.num_failures), (4, 2));
            assert_eq!(r.min_response_time, Some(5));
            assert_eq!(r.max_response_time, 9);
            assert_eq!(r.total_response_time, 30);
            assert_eq!(r.total_content_length, 20);
            assert_eq!(r.response_times.get(&9), Some(&2));
            assert_eq!(total.num_requests, 4);
            assert_eq!(errors.len(), 1);
            assert_eq!(errors[0].occurrences, 2);
        }
        let report = s.take_report();
        let get = |k: &str| {
            report
                .iter()
                .find(|(n, _)| n == k)
                .map(|(_, v)| v.clone())
                .unwrap()
        };
        let Value::Array(rows) = get("stats") else {
            panic!()
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(
            map_get(&rows[0], "num_requests").and_then(number),
            Some(4.0)
        );
        assert_eq!(
            map_get(&get("stats_total"), "name")
                .and_then(super::super::message::text)
                .as_deref(),
            Some("Aggregated")
        );
        let Value::Map(errs) = get("errors") else {
            panic!()
        };
        assert_eq!(errs.len(), 1);

        let (rows, total, errors) = s.rows();
        assert!(rows.is_empty() && errors.is_empty());
        assert_eq!(total.num_requests, 0);
        assert_eq!(total.min_response_time, None);
        assert_eq!(
            map_get(&total.to_value(), "min_response_time"),
            Some(&Value::Nil)
        );
    }
}

//! `hk.actor.usage_sampled` records (see `hivekeep_core::actor_event`): their attributes, the sink that
//! writes them, and the timer that drives the periodic sweep.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hivekeep_core::attr;
use hivekeep_proto::worker::v1 as pb;
use hivekeep_sandbox_gvisor::ports::{UsageKind, UsageSink};
use parking_lot::Mutex;
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

/// The record's event name.
pub const EVENT: &str = hivekeep_core::actor_event::USAGE_SAMPLED.name;

/// Attribute keys every record carries.
pub const KEYS: [&str; 11] = [
    attr::HIVE_KEY,
    attr::ACTOR_NAME_KEY,
    attr::ACTOR_UID_KEY,
    attr::TEMPLATE_HIVE_KEY,
    attr::TEMPLATE_NAME_KEY,
    WORKER_POOL_NAMESPACE_KEY,
    WORKER_POOL_NAME_KEY,
    SANDBOX_CLASS_KEY,
    STATS_SOURCE_KEY,
    STATS_KIND_KEY,
    ACTOR_EPOCH_KEY,
];
/// Attribute keys only a measured record carries (the peak also only when
/// the source reports one).
pub const CONDITIONAL_KEYS: [&str; 4] = [
    STATS_MEMORY_USAGE_KEY,
    STATS_MEMORY_PEAK_KEY,
    STATS_MEMORY_WORKING_SET_KEY,
    STATS_CPU_TIME_KEY,
];

/// The worker pool's namespace.
pub const WORKER_POOL_NAMESPACE_KEY: &str = attr::WORKER_POOL_NAMESPACE_KEY;
/// The worker pool's name.
pub const WORKER_POOL_NAME_KEY: &str = attr::WORKER_POOL_NAME_KEY;
/// The sandbox class, normalized.
pub const SANDBOX_CLASS_KEY: &str = attr::SANDBOX_CLASS_KEY;
/// Where the numbers came from.
pub const STATS_SOURCE_KEY: &str = attr::STATS_SOURCE_KEY;
/// Initial, periodic or final.
pub const STATS_KIND_KEY: &str = attr::STATS_KIND_KEY;
/// The activation's epoch, Unix nanoseconds.
pub const ACTOR_EPOCH_KEY: &str = attr::ACTOR_EPOCH_KEY;
/// Memory charged, bytes.
pub const STATS_MEMORY_USAGE_KEY: &str = attr::STATS_MEMORY_USAGE_KEY;
/// Peak memory, bytes.
pub const STATS_MEMORY_PEAK_KEY: &str = attr::STATS_MEMORY_PEAK_KEY;
/// Working set, bytes.
pub const STATS_MEMORY_WORKING_SET_KEY: &str = attr::STATS_MEMORY_WORKING_SET_KEY;
/// CPU time, seconds.
pub const STATS_CPU_TIME_KEY: &str = attr::STATS_CPU_TIME_KEY;

/// The `hk.stats.source` values.
pub use hivekeep_core::attr::stats_source as source;

/// The class label for anything not a known class.
pub const SANDBOX_CLASS_UNKNOWN: &str = attr::SANDBOX_CLASS_UNKNOWN;

/// The WorkerPool of the worker's pod.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Pool {
    /// Namespace.
    pub namespace: String,
    /// Name.
    pub name: String,
}

/// Maps the wire enum onto the source label.
pub fn stats_source_label(s: i32) -> &'static str {
    match pb::StatsSource::try_from(s) {
        Ok(pb::StatsSource::Cgroup) => source::CGROUP,
        Ok(pb::StatsSource::GuestAgent) => source::GUEST_AGENT,
        _ => source::UNSPECIFIED,
    }
}

/// A known sandbox class as itself, anything else as `unknown`, so no wire
/// value widens the label set.
pub fn normalize_sandbox_class(class: &str) -> &str {
    attr::normalize_sandbox_class(class)
}

/// The attributes of one record. Measurements are absent while `s` has no
/// source, and the peak also when the source reports none.
pub fn usage_attrs(
    pool: &Pool,
    kind: UsageKind,
    s: &pb::WorkloadStatsSample,
) -> Map<String, Value> {
    let mut m = Map::new();
    let mut put = |k: &str, v: Value| {
        m.insert(k.to_string(), v);
    };
    put(attr::HIVE_KEY, s.hive.clone().into());
    put(attr::ACTOR_NAME_KEY, s.actor_name.clone().into());
    put(attr::ACTOR_UID_KEY, s.actor_uid.clone().into());
    put(
        attr::TEMPLATE_HIVE_KEY,
        s.actor_template_hive.clone().into(),
    );
    put(
        attr::TEMPLATE_NAME_KEY,
        s.actor_template_name.clone().into(),
    );
    put(WORKER_POOL_NAMESPACE_KEY, pool.namespace.clone().into());
    put(WORKER_POOL_NAME_KEY, pool.name.clone().into());
    put(
        SANDBOX_CLASS_KEY,
        normalize_sandbox_class(&s.sandbox_class).into(),
    );
    put(STATS_SOURCE_KEY, stats_source_label(s.source).into());
    put(STATS_KIND_KEY, kind.as_str().into());
    put(ACTOR_EPOCH_KEY, s.epoch_unix_nano.into());
    if s.source == pb::StatsSource::Unspecified as i32 {
        return m;
    }
    put(STATS_MEMORY_USAGE_KEY, s.memory_current_bytes.into());
    put(
        STATS_MEMORY_WORKING_SET_KEY,
        s.memory_working_set_bytes.into(),
    );
    put(STATS_CPU_TIME_KEY, (s.cpu_usage_usec as f64 / 1e6).into());
    if s.memory_peak_bytes > 0 {
        put(STATS_MEMORY_PEAK_KEY, s.memory_peak_bytes.into());
    }
    m
}

/// One record: dated when its sample was read, not when it is written.
#[derive(Debug, Clone, PartialEq)]
pub struct UsageRecord {
    /// When the sample was read.
    pub time: SystemTime,
    /// The attributes.
    pub attrs: Map<String, Value>,
}

/// The record for `s`.
pub fn record(pool: &Pool, kind: UsageKind, s: &pb::WorkloadStatsSample) -> UsageRecord {
    let time = if s.observed_at_unix_nano >= 0 {
        SystemTime::UNIX_EPOCH + Duration::from_nanos(s.observed_at_unix_nano as u64)
    } else {
        SystemTime::UNIX_EPOCH
    };
    UsageRecord {
        time,
        attrs: usage_attrs(pool, kind, s),
    }
}

/// Writes records as JSON lines on the worker's log, at a fixed level so the
/// log level does not silence them.
#[derive(Debug, Clone, Default)]
pub struct JsonUsageSink {
    /// The pool the records name.
    pub pool: Pool,
}

impl UsageSink for JsonUsageSink {
    fn emit(&self, kind: UsageKind, s: &pb::WorkloadStatsSample) {
        let r = record(&self.pool, kind, s);
        let mut line = r.attrs;
        line.insert("event".into(), EVENT.into());
        line.insert("time_unix_nano".into(), s.observed_at_unix_nano.into());
        let line_value = Value::Object(line);
        tracing::info!(target: "hivekeep::usage", record = %line_value, "{EVENT}");
    }
}

/// Keeps every record, for tests.
#[derive(Debug, Default)]
pub struct RecordingSink {
    pool: Pool,
    records: Mutex<Vec<UsageRecord>>,
}

impl RecordingSink {
    /// A recorder naming `pool`.
    pub fn new(pool: Pool) -> Self {
        RecordingSink {
            pool,
            records: Mutex::default(),
        }
    }

    /// Every record, in order.
    pub fn records(&self) -> Vec<UsageRecord> {
        self.records.lock().clone()
    }

    fn values(&self, key: &str) -> Vec<String> {
        self.records
            .lock()
            .iter()
            .map(|r| {
                r.attrs
                    .get(key)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            })
            .collect()
    }

    /// The kind of each record.
    pub fn kinds(&self) -> Vec<String> {
        self.values(STATS_KIND_KEY)
    }

    /// The source of each record.
    pub fn sources(&self) -> Vec<String> {
        self.values(STATS_SOURCE_KEY)
    }
}

impl UsageSink for RecordingSink {
    fn emit(&self, kind: UsageKind, s: &pb::WorkloadStatsSample) {
        self.records.lock().push(record(&self.pool, kind, s));
    }
}

/// Writes a record when a sink is set; no sink writes nothing.
pub fn emit(sink: Option<&Arc<dyn UsageSink>>, kind: UsageKind, s: &pb::WorkloadStatsSample) {
    if let Some(sink) = sink {
        sink.emit(kind, s);
    }
}

/// A running sampler. Stopping waits for the sweep in flight.
pub struct Sampler {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Sampler {
    /// Stops the sampler and waits for the sweep in flight.
    pub async fn stop(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

/// Calls `sweep` every `interval`. A sweep that panics is logged and skips its
/// tick: the sampler is a background job, and a bug in it must not take the
/// worker and every Actor on it down.
pub fn start_sampler<F, Fut>(interval: Duration, sweep: F) -> Sampler
where
    F: Fn(CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let task = tokio::spawn(async move {
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tick.tick() => {}
            }
            if let Err(e) = tokio::spawn(sweep(token.clone())).await {
                if e.is_panic() {
                    tracing::error!(panic = ?e, "Usage sweep panicked; skipping this tick");
                }
            }
        }
    });
    Sampler { cancel, task }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn pool() -> Pool {
        Pool {
            namespace: "team-a".into(),
            name: "default".into(),
        }
    }

    fn measured() -> pb::WorkloadStatsSample {
        pb::WorkloadStatsSample {
            hive: "ns".into(),
            actor_name: "a".into(),
            actor_uid: "uid-1".into(),
            actor_template_hive: "ns".into(),
            actor_template_name: "t".into(),
            sandbox_class: "gvisor".into(),
            source: pb::StatsSource::Cgroup as i32,
            memory_current_bytes: 40 << 20,
            memory_peak_bytes: 48 << 20,
            memory_working_set_bytes: 32 << 20,
            cpu_usage_usec: 1_500_000,
            observed_at_unix_nano: 1_700_000_000_000_000_000,
            epoch_unix_nano: 1_699_999_990_000_000_000,
        }
    }

    fn check_shape(m: &Map<String, Value>) {
        for k in KEYS {
            assert!(m.contains_key(k), "required key {k} is missing");
        }
        for k in m.keys() {
            assert!(
                KEYS.contains(&k.as_str()) || CONDITIONAL_KEYS.contains(&k.as_str()),
                "undeclared key {k}"
            );
        }
    }

    #[test]
    fn measured_attrs() {
        let m = usage_attrs(&pool(), UsageKind::Periodic, &measured());
        check_shape(&m);
        for k in CONDITIONAL_KEYS {
            assert!(m.contains_key(k), "measured record is missing {k}");
        }
        assert_eq!(m[STATS_CPU_TIME_KEY].as_f64(), Some(1.5));
        for (k, want) in [
            (WORKER_POOL_NAMESPACE_KEY, "team-a"),
            (WORKER_POOL_NAME_KEY, "default"),
            (SANDBOX_CLASS_KEY, "gvisor"),
            (STATS_SOURCE_KEY, source::CGROUP),
            (STATS_KIND_KEY, "periodic"),
            ("hk.actor.uid", "uid-1"),
        ] {
            assert_eq!(m[k], want, "{k}");
        }
        assert_eq!(m[ACTOR_EPOCH_KEY].as_i64(), Some(1_699_999_990_000_000_000));
    }

    #[test]
    fn pending_attrs_have_no_measurements() {
        let mut s = measured();
        s.source = pb::StatsSource::Unspecified as i32;
        let m = usage_attrs(&pool(), UsageKind::Periodic, &s);
        check_shape(&m);
        for k in CONDITIONAL_KEYS {
            assert!(!m.contains_key(k), "pending record carries {k}");
        }
    }

    #[test]
    fn normalizes_the_sandbox_class() {
        for (class, want) in [
            ("microvm", "microvm"),
            ("gvisor", "gvisor"),
            ("gVisor", SANDBOX_CLASS_UNKNOWN),
            ("", SANDBOX_CLASS_UNKNOWN),
            ("kata", SANDBOX_CLASS_UNKNOWN),
        ] {
            let mut s = measured();
            s.sandbox_class = class.into();
            assert_eq!(
                usage_attrs(&pool(), UsageKind::Periodic, &s)[SANDBOX_CLASS_KEY],
                want
            );
        }
    }

    #[test]
    fn omits_an_unreported_peak() {
        let mut s = measured();
        s.memory_peak_bytes = 0;
        let m = usage_attrs(&pool(), UsageKind::Periodic, &s);
        assert!(!m.contains_key(STATS_MEMORY_PEAK_KEY));
        assert!(m.contains_key(STATS_MEMORY_USAGE_KEY));
    }

    #[test]
    fn records_are_dated_at_read_time() {
        let sink = RecordingSink::new(pool());
        let s = measured();
        sink.emit(UsageKind::Initial, &s);
        let r = sink.records();
        assert_eq!(r.len(), 1);
        assert_eq!(
            r[0].time,
            SystemTime::UNIX_EPOCH + Duration::from_nanos(s.observed_at_unix_nano as u64)
        );
    }

    #[test]
    fn no_sink_writes_nothing() {
        emit(None, UsageKind::Initial, &measured());
    }

    #[tokio::test(start_paused = true)]
    async fn sampler_sweeps_on_schedule() {
        let interval = Duration::from_secs(60);
        let n = Arc::new(AtomicUsize::new(0));
        let c = n.clone();
        let s = start_sampler(interval, move |_| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });
        tokio::time::sleep(6 * interval + Duration::from_secs(1)).await;
        s.stop().await;
        tokio::time::sleep(2 * interval).await;
        assert_eq!(n.load(Ordering::SeqCst), 6);
    }

    #[tokio::test(start_paused = true)]
    async fn sampler_survives_a_panic() {
        let interval = Duration::from_secs(60);
        let n = Arc::new(AtomicUsize::new(0));
        let c = n.clone();
        let s = start_sampler(interval, move |_| {
            let c = c.clone();
            async move {
                if c.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("first sweep fails");
                }
            }
        });
        tokio::time::sleep(3 * interval + Duration::from_secs(1)).await;
        s.stop().await;
        assert_eq!(n.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn source_labels() {
        assert_eq!(
            stats_source_label(pb::StatsSource::Cgroup as i32),
            source::CGROUP
        );
        assert_eq!(
            stats_source_label(pb::StatsSource::GuestAgent as i32),
            source::GUEST_AGENT
        );
        assert_eq!(
            stats_source_label(pb::StatsSource::Unspecified as i32),
            source::UNSPECIFIED
        );
    }
}

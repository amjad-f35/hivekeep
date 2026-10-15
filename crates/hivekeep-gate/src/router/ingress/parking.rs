//! The parking lot: a bounded set of requests waiting for their Actor's
//! resume to get past a transient condition, with its metrics.

use std::time::{Duration, Instant};

use parking_lot::Mutex;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry};
use serde::Serialize;

/// Default park budget per resume flight.
pub const DEFAULT_PARKED_REQUEST_BUDGET: Duration = Duration::from_secs(5);
/// Default lot capacity.
pub const DEFAULT_PARKED_REQUEST_MAX: usize = 1024;
/// Default delay before the first retry.
pub const DEFAULT_PARKED_REQUEST_RETRY_INTERVAL: Duration = Duration::from_millis(100);
/// Default retry delay multiplier.
pub const DEFAULT_PARKED_REQUEST_RETRY_FACTOR: f64 = 1.1;
/// Default random fraction added to each retry delay.
pub const DEFAULT_PARKED_REQUEST_RETRY_JITTER: f64 = 0.1;

/// Parking settings. `max == 0` disables parking.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ParkedRequestConfig {
    /// Park budget per flight.
    pub budget: Duration,
    /// Lot capacity.
    pub max: usize,
    /// Delay before the first retry.
    pub retry_interval: Duration,
    /// Delay multiplier per retry (>= 1).
    pub retry_factor: f64,
    /// Random fraction in [0, 1) added per retry.
    pub retry_jitter: f64,
}

impl Default for ParkedRequestConfig {
    /// The default (enabled) configuration.
    fn default() -> Self {
        ParkedRequestConfig {
            budget: DEFAULT_PARKED_REQUEST_BUDGET,
            max: DEFAULT_PARKED_REQUEST_MAX,
            retry_interval: DEFAULT_PARKED_REQUEST_RETRY_INTERVAL,
            retry_factor: DEFAULT_PARKED_REQUEST_RETRY_FACTOR,
            retry_jitter: DEFAULT_PARKED_REQUEST_RETRY_JITTER,
        }
    }
}

impl ParkedRequestConfig {
    /// A disabled configuration (everything else unset).
    pub fn disabled() -> Self {
        ParkedRequestConfig {
            budget: Duration::ZERO,
            max: 0,
            retry_interval: Duration::ZERO,
            retry_factor: 0.0,
            retry_jitter: 0.0,
        }
    }

    /// A configuration with `max` slots and `budget`, other fields unset.
    pub fn with(max: usize, budget: Duration) -> Self {
        ParkedRequestConfig {
            max,
            budget,
            ..Self::disabled()
        }
    }

    /// Whether parking is on.
    pub fn enabled(&self) -> bool {
        self.max > 0
    }

    /// Unset fields replaced by defaults.
    pub fn normalized(mut self) -> Self {
        if self.budget.is_zero() {
            self.budget = DEFAULT_PARKED_REQUEST_BUDGET;
        }
        if self.retry_interval.is_zero() {
            self.retry_interval = DEFAULT_PARKED_REQUEST_RETRY_INTERVAL;
        }
        if self.retry_factor == 0.0 {
            self.retry_factor = DEFAULT_PARKED_REQUEST_RETRY_FACTOR;
        }
        self
    }

    /// Checks the retry tuning.
    pub fn validate(&self) -> Result<(), String> {
        if self.retry_factor != 0.0 && self.retry_factor < 1.0 {
            return Err(format!(
                "parked-request retry factor must be >= 1.0, got {}",
                self.retry_factor
            ));
        }
        if !(0.0..1.0).contains(&self.retry_jitter) {
            return Err(format!(
                "parked-request retry jitter must be in [0, 1), got {}",
                self.retry_jitter
            ));
        }
        Ok(())
    }
}

/// The lot's state for the status page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ParkingStatus {
    /// Whether parking is on.
    pub enabled: bool,
    /// Requests parked now.
    pub active: usize,
    /// Capacity.
    pub max_parked: usize,
    /// The park budget, human readable.
    pub max_wait: String,
}

/// How a park ended (the `outcome` label of the wait histogram).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkOutcome {
    /// Resumed and routed.
    Served,
    /// The budget elapsed while blocked on a retryable condition.
    BudgetExhausted,
    /// The request's own deadline passed while parked.
    Timeout,
    /// The client went away while parked.
    Canceled,
    /// The resume failed otherwise.
    Error,
}

impl ParkOutcome {
    /// The label value.
    pub fn as_str(self) -> &'static str {
        match self {
            ParkOutcome::Served => "served",
            ParkOutcome::BudgetExhausted => "budget_exhausted",
            ParkOutcome::Timeout => "timeout",
            ParkOutcome::Canceled => "canceled",
            ParkOutcome::Error => "error",
        }
    }
}

/// Name of the parked-requests gauge.
pub const PARKING_ACTIVE_METRIC: &str = "hivekeep_gate_router_parking_active";
/// Name of the park-wait histogram.
pub const PARKING_WAIT_METRIC: &str = "hivekeep_gate_router_parking_wait_duration_seconds";
/// Name of the shed-requests counter.
pub const PARKING_REJECTED_METRIC: &str = "hivekeep_gate_router_parking_rejected_total";

/// The lot's metrics.
#[derive(Clone)]
pub struct ParkingMetrics {
    active: IntGauge,
    wait: HistogramVec,
    rejected: IntCounterVec,
}

impl ParkingMetrics {
    /// Registers the lot's metrics.
    pub fn new(registry: &Registry) -> prometheus::Result<Self> {
        let active = IntGauge::new(
            PARKING_ACTIVE_METRIC,
            "number of requests currently parked in the router awaiting Actor resume",
        )?;
        let wait = HistogramVec::new(
            HistogramOpts::new(PARKING_WAIT_METRIC, "time a request spent parked in the router before being served, timing out, or failing")
                .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 30.0, 45.0, 60.0]),
            &["outcome"],
        )?;
        let rejected = IntCounterVec::new(
            Opts::new(
                PARKING_REJECTED_METRIC,
                "number of requests shed because the router parking lot was full",
            ),
            &[crate::attr::ROUTER_OUTCOME],
        )?;
        registry.register(Box::new(active.clone()))?;
        registry.register(Box::new(wait.clone()))?;
        registry.register(Box::new(rejected.clone()))?;
        Ok(ParkingMetrics {
            active,
            wait,
            rejected,
        })
    }
}

/// The lot.
pub struct ParkingLot {
    cfg: ParkedRequestConfig,
    metrics: Option<ParkingMetrics>,
    active: Mutex<usize>,
}

/// A held slot. Released once, with an outcome; dropping it unreleased
/// counts as the client going away.
#[must_use]
pub struct Slot<'a> {
    lot: Option<&'a ParkingLot>,
    start: Instant,
}

impl Slot<'_> {
    /// Releases the slot, recording how the park ended.
    pub fn release(mut self, outcome: ParkOutcome) {
        self.finish(outcome);
    }

    fn finish(&mut self, outcome: ParkOutcome) {
        let Some(lot) = self.lot.take() else {
            return;
        };
        {
            let mut a = lot.active.lock();
            if *a > 0 {
                *a -= 1;
            } else {
                tracing::error!("parking lot slot released more times than acquired");
            }
        }
        if let Some(m) = &lot.metrics {
            m.active.dec();
            m.wait
                .with_label_values(&[outcome.as_str()])
                .observe(self.start.elapsed().as_secs_f64());
        }
    }
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.finish(ParkOutcome::Canceled);
    }
}

impl ParkingLot {
    /// A lot for `cfg`.
    pub fn new(cfg: ParkedRequestConfig, metrics: Option<ParkingMetrics>) -> Self {
        ParkingLot {
            cfg,
            metrics,
            active: Mutex::new(0),
        }
    }

    /// Takes a slot, or `None` (counted as shed with `shed_outcome`) when
    /// full. A disabled lot admits everything without accounting.
    pub fn enter(&self, shed_outcome: &str) -> Option<Slot<'_>> {
        if !self.cfg.enabled() {
            return Some(Slot {
                lot: None,
                start: Instant::now(),
            });
        }
        {
            let mut a = self.active.lock();
            if *a >= self.cfg.max {
                drop(a);
                if let Some(m) = &self.metrics {
                    m.rejected.with_label_values(&[shed_outcome]).inc();
                }
                return None;
            }
            *a += 1;
        }
        if let Some(m) = &self.metrics {
            m.active.inc();
        }
        Some(Slot {
            lot: Some(self),
            start: Instant::now(),
        })
    }

    /// Slots held now.
    pub fn active_count(&self) -> usize {
        *self.active.lock()
    }

    /// The lot's state.
    pub fn status(&self) -> ParkingStatus {
        ParkingStatus {
            enabled: self.cfg.enabled(),
            active: self.active_count(),
            max_parked: self.cfg.max,
            max_wait: humantime::format_duration(self.cfg.budget).to_string(),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::attr::outcome;
    use std::collections::HashMap;

    pub(crate) fn rejected_by_outcome(reg: &Registry) -> HashMap<String, u64> {
        let mut out = HashMap::new();
        for mf in reg.gather() {
            if mf.name() != PARKING_REJECTED_METRIC {
                continue;
            }
            for m in mf.get_metric() {
                let label = m
                    .get_label()
                    .iter()
                    .find(|l| l.name() == crate::attr::ROUTER_OUTCOME)
                    .expect("outcome label");
                *out.entry(label.value().to_string()).or_default() +=
                    m.get_counter().value() as u64;
            }
        }
        out
    }

    #[test]
    fn capacity_and_release() {
        let lot = ParkingLot::new(ParkedRequestConfig::with(2, Duration::from_secs(1)), None);
        let r1 = lot.enter(outcome::UNAVAILABLE).expect("first");
        let r2 = lot.enter(outcome::UNAVAILABLE).expect("second");
        assert_eq!(lot.active_count(), 2);
        assert!(
            lot.enter(outcome::UNAVAILABLE).is_none(),
            "third admitted into a full lot"
        );
        r1.release(ParkOutcome::Served);
        assert_eq!(lot.active_count(), 1);
        let r3 = lot.enter(outcome::UNAVAILABLE).expect("after release");
        r2.release(ParkOutcome::Served);
        r3.release(ParkOutcome::Served);
        assert_eq!(lot.active_count(), 0);
    }

    #[test]
    fn release_is_idempotent() {
        let lot = ParkingLot::new(ParkedRequestConfig::with(1, Duration::from_secs(1)), None);
        let slot = lot.enter(outcome::UNAVAILABLE).unwrap();
        slot.release(ParkOutcome::Served); // consumes: a second release cannot happen
        assert_eq!(lot.active_count(), 0);
    }

    #[test]
    fn disabled_always_admits() {
        let lot = ParkingLot::new(ParkedRequestConfig::disabled(), None);
        for _ in 0..5 {
            lot.enter(outcome::UNAVAILABLE)
                .unwrap()
                .release(ParkOutcome::Served);
        }
        assert_eq!(lot.active_count(), 0);
        assert!(!lot.status().enabled);
    }

    #[test]
    fn concurrent_entry_respects_capacity() {
        let lot = ParkingLot::new(ParkedRequestConfig::with(8, Duration::from_secs(1)), None);
        let admitted = std::sync::atomic::AtomicUsize::new(0);
        let barrier = std::sync::Barrier::new(100);
        std::thread::scope(|s| {
            for _ in 0..100 {
                s.spawn(|| {
                    let slot = lot.enter(outcome::UNAVAILABLE);
                    if slot.is_some() {
                        admitted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    barrier.wait();
                    if let Some(s) = slot {
                        s.release(ParkOutcome::Served);
                    }
                });
            }
        });
        assert_eq!(admitted.into_inner(), 8);
        assert_eq!(lot.active_count(), 0);
    }

    #[test]
    fn rejected_labeled_by_shed_outcome() {
        let reg = Registry::new();
        let lot = ParkingLot::new(
            ParkedRequestConfig::with(1, Duration::from_secs(1)),
            Some(ParkingMetrics::new(&reg).unwrap()),
        );
        let _held = lot.enter(outcome::NO_CAPACITY).unwrap();
        for o in [
            outcome::NO_CAPACITY,
            outcome::NO_CAPACITY,
            outcome::UNAVAILABLE,
        ] {
            assert!(lot.enter(o).is_none());
        }
        let got = rejected_by_outcome(&reg);
        assert_eq!(
            got,
            HashMap::from([
                (outcome::NO_CAPACITY.to_string(), 2),
                (outcome::UNAVAILABLE.to_string(), 1)
            ])
        );
    }

    #[test]
    fn disabled_records_no_rejection() {
        let reg = Registry::new();
        let lot = ParkingLot::new(
            ParkedRequestConfig::disabled(),
            Some(ParkingMetrics::new(&reg).unwrap()),
        );
        assert!(lot.enter(outcome::NO_CAPACITY).is_some());
        assert!(rejected_by_outcome(&reg).is_empty());
    }

    #[test]
    fn dropped_slot_counts_as_canceled() {
        let reg = Registry::new();
        let lot = ParkingLot::new(
            ParkedRequestConfig::with(1, Duration::from_secs(1)),
            Some(ParkingMetrics::new(&reg).unwrap()),
        );
        drop(lot.enter(outcome::UNAVAILABLE).unwrap());
        assert_eq!(lot.active_count(), 0);
        let mf = reg
            .gather()
            .into_iter()
            .find(|m| m.name() == PARKING_WAIT_METRIC)
            .unwrap();
        assert_eq!(mf.get_metric()[0].get_label()[0].value(), "canceled");
    }
}

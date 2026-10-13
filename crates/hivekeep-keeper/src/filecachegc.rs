//! The eviction loop of a node-local file cache: a byte budget, evicted
//! least recently used first down to a low-water fraction of it, never
//! taking an entry a consumer still links.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::FutureExt;
use parking_lot::Mutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry};

use crate::context::Ctx;
use crate::filecache::{self, EvictStats};

/// The fraction of the budget a pass evicts down to once over it, so passes
/// do not fire on every tick while usage hovers at the budget.
const LOW_WATER: f64 = 0.9;
/// Consecutive shortfalls that warn before backing off to reminders.
const SHORTFALL_WARN_LIMIT: u32 = 3;
/// How often a persistent shortfall is repeated.
const SHORTFALL_REMINDER_EVERY: u32 = 12;

/// Name of the sandbox asset cache, as a metric label.
pub const CACHE_SANDBOX_ASSET: &str = "sandbox_asset";
/// Name of the snapshot cache, as a metric label.
pub const CACHE_SNAPSHOT: &str = "snapshot";

/// What the loop needs from a cache.
#[async_trait]
pub trait GcStore: Send + Sync + 'static {
    /// The cache's allocated bytes.
    async fn total_bytes(&self) -> Result<i64, String>;
    /// The cache's allocated bytes, measured on the calling thread: the
    /// size metric reads it on every scrape.
    fn total_bytes_now(&self) -> Result<i64, String>;
    /// One eviction pass; the stats are valid even with an error.
    async fn evict_unused(&self, target: i64, dry_run: bool) -> (EvictStats, Option<String>);
}

#[async_trait]
impl GcStore for filecache::Store {
    async fn total_bytes(&self) -> Result<i64, String> {
        filecache::Store::total_bytes(self)
            .await
            .map_err(|e| e.to_string())
    }

    fn total_bytes_now(&self) -> Result<i64, String> {
        filecache::Store::total_bytes_blocking(self).map_err(|e| e.to_string())
    }

    async fn evict_unused(&self, target: i64, dry_run: bool) -> (EvictStats, Option<String>) {
        match filecache::Store::evict_unused(self, target, dry_run).await {
            Ok(s) => (s, None),
            Err((s, e)) => (s, Some(e.to_string())),
        }
    }
}

/// One cache's eviction settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcConfig {
    /// How often a pass runs; zero disables the loop.
    pub period: Duration,
    /// The budget in allocated bytes; zero means none.
    pub max_bytes: i64,
    /// Log decisions without deleting.
    pub dry_run: bool,
}

impl Default for GcConfig {
    fn default() -> Self {
        GcConfig {
            period: Duration::from_secs(300),
            max_bytes: 0,
            dry_run: false,
        }
    }
}

impl GcConfig {
    /// Rejects values the loop could not act on. `prefix` names the flags.
    pub fn validate(&self, prefix: &str) -> Result<(), String> {
        if self.max_bytes < 0 {
            return Err(format!(
                "--{prefix}-max-bytes {} must be >= 0",
                self.max_bytes
            ));
        }
        Ok(())
    }
}

/// The bytes a pass should free: none within budget, else enough to reach
/// the low-water fraction of it. A zero budget asks for nothing.
pub fn target(total: i64, max_bytes: i64) -> i64 {
    if max_bytes <= 0 || total <= max_bytes {
        return 0;
    }
    (total - (max_bytes as f64 * LOW_WATER) as i64).min(total)
}

/// The caches the size metric reports, by name.
type ObservedCaches = Vec<(String, Arc<dyn GcStore>)>;

/// The size metric: each observed cache is measured when scraped, so the
/// series exists (and is current) whether or not its loop runs.
#[derive(Clone)]
struct SizeCollector {
    gauge: IntGaugeVec,
    stores: Arc<Mutex<ObservedCaches>>,
}

impl Collector for SizeCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.gauge.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        // Held across the whole collection so concurrent scrapes do not
        // interleave their resets.
        let stores = self.stores.lock();
        self.gauge.reset();
        for (name, store) in stores.iter() {
            match store.total_bytes_now() {
                Ok(bytes) => self.gauge.with_label_values(&[name]).set(bytes),
                Err(err) => {
                    tracing::warn!(cache = %name, %err, "File cache size unavailable for metrics")
                }
            }
        }
        self.gauge.collect()
    }
}

/// Metrics of every file cache on the node, labeled by cache name.
#[derive(Clone)]
pub struct Instruments {
    size: SizeCollector,
    evictions: IntCounterVec,
    passes: IntCounterVec,
}

impl Instruments {
    /// Registers the instruments on `registry`.
    pub fn new(registry: &Registry) -> prometheus::Result<Self> {
        let size = IntGaugeVec::new(
            Opts::new(
                "hivekeep_filecache_size_bytes",
                "Bytes of disk a node-local file cache occupies, as the sum of its entries' allocated sizes.",
            ),
            &["cache"],
        )?;
        let evictions = IntCounterVec::new(
            Opts::new(
                "hivekeep_filecache_evictions_total",
                "Entries eviction passes removed from a node-local file cache.",
            ),
            &["cache"],
        )?;
        let passes = IntCounterVec::new(
            Opts::new(
                "hivekeep_filecache_gc_passes_total",
                "Eviction passes over a node-local file cache that had work to do, by outcome.",
            ),
            &["cache", "outcome"],
        )?;
        let size = SizeCollector {
            gauge: size,
            stores: Arc::new(Mutex::new(Vec::new())),
        };
        registry.register(Box::new(size.clone()))?;
        registry.register(Box::new(evictions.clone()))?;
        registry.register(Box::new(passes.clone()))?;
        Ok(Instruments {
            size,
            evictions,
            passes,
        })
    }

    fn record_pass(&self, cache: &str, outcome: &str, evicted: usize) {
        self.passes.with_label_values(&[cache, outcome]).inc();
        if evicted > 0 {
            self.evictions
                .with_label_values(&[cache])
                .inc_by(evicted as u64);
        }
    }

    /// Reports `store`'s size under `cache` on every scrape from now on.
    fn observe(&self, cache: &str, store: Arc<dyn GcStore>) {
        self.size.stores.lock().push((cache.to_string(), store));
    }
}

/// One cache's eviction loop.
pub struct Gc {
    name: String,
    store: Arc<dyn GcStore>,
    instruments: Option<Instruments>,
    config: GcConfig,
    consecutive_shortfalls: u32,
}

/// Outcome of a pass that had work to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassOutcome {
    /// Freed what the budget asked.
    Complete,
    /// Freed less than asked.
    Shortfall,
    /// Could not size the cache, so did nothing.
    Skipped,
    /// Within budget.
    Quiet,
}

impl PassOutcome {
    fn label(self) -> &'static str {
        match self {
            PassOutcome::Complete => "complete",
            PassOutcome::Shortfall => "shortfall",
            PassOutcome::Skipped => "skipped",
            PassOutcome::Quiet => "quiet",
        }
    }
}

impl Gc {
    /// The loop for the cache called `name`.
    pub fn new(
        name: &str,
        store: Arc<dyn GcStore>,
        config: GcConfig,
        instruments: Option<Instruments>,
    ) -> Self {
        Gc {
            name: name.into(),
            store,
            instruments,
            config,
            consecutive_shortfalls: 0,
        }
    }

    /// Runs passes every period until `ctx` ends, the first one at once.
    pub async fn run(mut self, ctx: Ctx) {
        loop {
            self.run_pass().await;
            if ctx.sleep(self.config.period).await.is_err() {
                return;
            }
        }
    }

    /// One pass. A panic in it is logged and the pass skipped: a janitor
    /// must not take the node agent down.
    pub async fn run_pass(&mut self) -> PassOutcome {
        match std::panic::AssertUnwindSafe(self.pass())
            .catch_unwind()
            .await
        {
            Ok(o) => o,
            Err(_) => {
                tracing::error!(cache = %self.name, "File cache GC pass panicked; skipping this pass");
                PassOutcome::Skipped
            }
        }
    }

    async fn pass(&mut self) -> PassOutcome {
        let total = match self.store.total_bytes().await {
            Ok(t) => t,
            Err(err) => {
                tracing::warn!(cache = %self.name, %err, "File cache GC: sizing the cache failed; skipping this pass");
                if let Some(i) = &self.instruments {
                    i.record_pass(&self.name, PassOutcome::Skipped.label(), 0);
                }
                return PassOutcome::Skipped;
            }
        };
        let target = target(total, self.config.max_bytes);
        if target == 0 {
            self.consecutive_shortfalls = 0;
            return PassOutcome::Quiet;
        }
        let started = std::time::Instant::now();
        let (stats, err) = self.store.evict_unused(target, self.config.dry_run).await;
        // A dry run's retired count is what a real pass would take.
        let evicted = if self.config.dry_run {
            0
        } else {
            stats.retired
        };
        if let Some(err) = &err {
            tracing::warn!(cache = %self.name, %err, "File cache GC pass finished with errors");
        }
        let outcome = if stats.freed_bytes < target {
            self.consecutive_shortfalls += 1;
            let n = self.consecutive_shortfalls;
            if n <= SHORTFALL_WARN_LIMIT || n.is_multiple_of(SHORTFALL_REMINDER_EVERY) {
                tracing::warn!(
                    cache = %self.name, cache_bytes = total, max_bytes = self.config.max_bytes,
                    target_bytes = target, freed_bytes = stats.freed_bytes, evicted = stats.retired,
                    skipped_linked = stats.skipped_linked, skipped_young = stats.skipped_young,
                    skipped_busy = stats.skipped_busy, dry_run = self.config.dry_run, consecutive = n,
                    "File cache GC could not reach its budget; what remains is linked, young, or busy"
                );
            }
            PassOutcome::Shortfall
        } else {
            self.consecutive_shortfalls = 0;
            tracing::info!(
                cache = %self.name, cache_bytes = total, target_bytes = target,
                freed_bytes = stats.freed_bytes, evicted = stats.retired, dry_run = self.config.dry_run,
                took_ms = started.elapsed().as_millis() as u64, "File cache GC pass complete"
            );
            PassOutcome::Complete
        };
        if let Some(i) = &self.instruments {
            i.record_pass(&self.name, outcome.label(), evicted);
        }
        outcome
    }
}

/// Validates `config` and starts the cache's loop in the background when it
/// has a budget and a period. Without a budget it warns once.
pub fn start(
    ctx: &Ctx,
    name: &str,
    store: Arc<dyn GcStore>,
    config: GcConfig,
    prefix: &str,
    instruments: Option<Instruments>,
) -> Result<(), String> {
    config.validate(prefix)?;
    if let Some(i) = &instruments {
        i.observe(name, store.clone());
    }
    if config.max_bytes == 0 {
        tracing::warn!(cache = name, flag = %format!("--{prefix}-max-bytes"), "File cache has no budget; it only grows");
        return Ok(());
    }
    if !config.period.is_zero() {
        let gc = Gc::new(name, store, config, instruments);
        tokio::spawn(gc.run(ctx.clone()));
    }
    Ok(())
}

#[cfg(test)]
mod behavior_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;

    #[test]
    fn target_math() {
        assert_eq!(target(50, 0), 0);
        assert_eq!(target(90, 100), 0);
        assert_eq!(target(100, 100), 0);
        assert_eq!(target(120, 100), 30);
    }

    struct Fake {
        total: Result<i64, String>,
        freed: i64,
        calls: Mutex<Vec<(i64, bool)>>,
    }

    #[async_trait]
    impl GcStore for Fake {
        async fn total_bytes(&self) -> Result<i64, String> {
            self.total.clone()
        }
        fn total_bytes_now(&self) -> Result<i64, String> {
            self.total.clone()
        }
        async fn evict_unused(&self, target: i64, dry_run: bool) -> (EvictStats, Option<String>) {
            self.calls.lock().push((target, dry_run));
            (
                EvictStats {
                    retired: 1,
                    freed_bytes: self.freed,
                    ..Default::default()
                },
                None,
            )
        }
    }

    fn gc(fake: Arc<Fake>, reg: &Registry) -> Gc {
        Gc::new(
            "snapshot",
            fake,
            GcConfig {
                period: Duration::from_secs(1),
                max_bytes: 100,
                dry_run: false,
            },
            Some(Instruments::new(reg).unwrap()),
        )
    }

    #[tokio::test]
    async fn pass_outcomes() {
        let reg = Registry::new();
        let fake = Arc::new(Fake {
            total: Ok(200),
            freed: 200,
            calls: Mutex::new(vec![]),
        });
        assert_eq!(
            gc(fake.clone(), &reg).run_pass().await,
            PassOutcome::Complete
        );
        assert_eq!(fake.calls.lock()[0], (110, false));

        let reg = Registry::new();
        let short = Arc::new(Fake {
            total: Ok(200),
            freed: 10,
            calls: Mutex::new(vec![]),
        });
        let mut g = gc(short, &reg);
        assert_eq!(g.run_pass().await, PassOutcome::Shortfall);
        assert_eq!(g.consecutive_shortfalls, 1);

        let reg = Registry::new();
        let broken = Arc::new(Fake {
            total: Err("io".into()),
            freed: 0,
            calls: Mutex::new(vec![]),
        });
        let mut g = gc(broken.clone(), &reg);
        assert_eq!(g.run_pass().await, PassOutcome::Skipped);
        assert!(broken.calls.lock().is_empty());
    }

    #[tokio::test]
    async fn within_budget_is_quiet() {
        let reg = Registry::new();
        let fake = Arc::new(Fake {
            total: Ok(50),
            freed: 0,
            calls: Mutex::new(vec![]),
        });
        assert_eq!(gc(fake.clone(), &reg).run_pass().await, PassOutcome::Quiet);
        assert!(fake.calls.lock().is_empty());
    }
}

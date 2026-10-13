//! Tests of the eviction loop over a fake cache, asserting what each pass
//! asks of the cache and records in the metrics.

use std::collections::HashMap;

use prometheus::proto::{MetricFamily, MetricType};

use super::*;

const TEST_CACHE: &str = "test_cache";
const SIZE_METRIC: &str = "hivekeep_filecache_size_bytes";
const EVICTIONS_METRIC: &str = "hivekeep_filecache_evictions_total";
const PASSES_METRIC: &str = "hivekeep_filecache_gc_passes_total";

#[derive(Default)]
struct FakeState {
    size: i64,
    size_err: Option<String>,
    size_calls: usize,
    evict_calls: usize,
    got_target: i64,
    got_dry_run: bool,
    stats: EvictStats,
    panic_on_evict: bool,
}

/// Stands in for the cache: a size to report and a canned eviction result.
#[derive(Default)]
struct FakeStore(Mutex<FakeState>);

impl FakeStore {
    fn new(f: impl FnOnce(&mut FakeState)) -> Arc<Self> {
        let s = FakeStore::default();
        f(&mut s.0.lock());
        Arc::new(s)
    }
    fn size(&self) -> Result<i64, String> {
        let mut st = self.0.lock();
        st.size_calls += 1;
        match &st.size_err {
            Some(e) => Err(e.clone()),
            None => Ok(st.size),
        }
    }
}

#[async_trait]
impl GcStore for FakeStore {
    async fn total_bytes(&self) -> Result<i64, String> {
        self.size()
    }
    fn total_bytes_now(&self) -> Result<i64, String> {
        self.size()
    }
    async fn evict_unused(&self, target: i64, dry_run: bool) -> (EvictStats, Option<String>) {
        let (stats, err, panic) = {
            let mut st = self.0.lock();
            st.evict_calls += 1;
            st.got_target = target;
            st.got_dry_run = dry_run;
            (st.stats, None, st.panic_on_evict)
        };
        if panic {
            panic!("boom");
        }
        (stats, err)
    }
}

/// A loop over `fake` with instruments on a fresh registry, the cache's
/// size observed as `start` would.
fn new_test_gc(fake: &Arc<FakeStore>, max_bytes: i64) -> (Gc, Registry) {
    let reg = Registry::new();
    let inst = Instruments::new(&reg).unwrap();
    inst.observe(TEST_CACHE, fake.clone());
    let gc = Gc::new(
        TEST_CACHE,
        fake.clone(),
        GcConfig {
            period: Duration::from_secs(3600),
            max_bytes,
            dry_run: false,
        },
        Some(inst),
    );
    (gc, reg)
}

fn family(reg: &Registry, name: &str) -> Option<MetricFamily> {
    reg.gather()
        .into_iter()
        .find(|f| f.name() == name && !f.get_metric().is_empty())
}

fn labels(m: &prometheus::proto::Metric) -> HashMap<String, String> {
    m.get_label()
        .iter()
        .map(|l| (l.name().to_string(), l.value().to_string()))
        .collect()
}

/// A counter's values keyed by outcome, checking every series carries the
/// cache's name.
fn passes_by_outcome(f: &MetricFamily) -> HashMap<String, u64> {
    assert_eq!(f.get_field_type(), MetricType::COUNTER);
    let mut out = HashMap::new();
    for m in f.get_metric() {
        let l = labels(m);
        assert_eq!(
            l.get("cache").map(String::as_str),
            Some(TEST_CACHE),
            "{l:?}"
        );
        let outcome = l.get("outcome").expect("series without an outcome").clone();
        *out.entry(outcome).or_default() += m.get_counter().value() as u64;
    }
    out
}

#[test]
fn target_table() {
    for (name, total, max, want) in [
        ("no budget", 500, 0, 0),
        ("within budget", 500, 1000, 0),
        ("at budget", 1000, 1000, 0),
        ("over budget frees down to the low water", 1500, 1000, 600),
        (
            "far over budget frees down to the low water",
            10_000,
            1000,
            9100,
        ),
    ] {
        assert_eq!(target(total, max), want, "{name}");
    }
}

#[test]
fn config_validate() {
    // A negative period cannot be expressed: periods are unsigned durations.
    for (name, period, max_bytes, want_err) in [
        (
            "defaults",
            GcConfig::default().period,
            GcConfig::default().max_bytes,
            false,
        ),
        ("disabled period", Duration::ZERO, 1 << 30, false),
        ("negative budget", Duration::from_secs(60), -1, true),
    ] {
        let c = GcConfig {
            period,
            max_bytes,
            dry_run: false,
        };
        assert_eq!(c.validate("test-cache").is_err(), want_err, "{name}");
    }
}

#[tokio::test]
async fn pass_within_budget_does_nothing() {
    let fake = FakeStore::new(|s| s.size = 500);
    let (mut g, reg) = new_test_gc(&fake, 1000);
    g.run_pass().await;
    assert_eq!(fake.0.lock().evict_calls, 0);
    assert_eq!(fake.0.lock().size_calls, 1);
    assert!(
        family(&reg, PASSES_METRIC).is_none(),
        "a quiet pass recorded"
    );
}

#[tokio::test]
async fn pass_skips_when_sizing_fails() {
    let fake = FakeStore::new(|s| s.size_err = Some("unreadable".into()));
    let (mut g, reg) = new_test_gc(&fake, 1000);
    assert_eq!(g.run_pass().await, PassOutcome::Skipped);
    assert_eq!(fake.0.lock().evict_calls, 0);
    fake.0.lock().size_err = None;
    let got = passes_by_outcome(&family(&reg, PASSES_METRIC).expect("skip not recorded"));
    assert_eq!(got, HashMap::from([("skipped".to_string(), 1)]));
}

#[tokio::test]
async fn pass_evicts_down_to_the_low_water() {
    let fake = FakeStore::new(|s| {
        s.size = 1500;
        s.stats = EvictStats {
            retired: 3,
            freed_bytes: 600,
            ..Default::default()
        };
    });
    let (mut g, reg) = new_test_gc(&fake, 1000);
    g.run_pass().await;
    {
        let st = fake.0.lock();
        assert_eq!(
            (st.evict_calls, st.got_target, st.got_dry_run),
            (1, 600, false)
        );
    }
    let passes = family(&reg, PASSES_METRIC).expect("pass not recorded");
    assert_eq!(
        passes_by_outcome(&passes),
        HashMap::from([("complete".to_string(), 1)])
    );
    let ev = family(&reg, EVICTIONS_METRIC).expect("evictions not recorded");
    assert_eq!(ev.get_field_type(), MetricType::COUNTER);
    assert_eq!(ev.get_metric().len(), 1);
    assert_eq!(ev.get_metric()[0].get_counter().value(), 3.0);
    assert_eq!(
        labels(&ev.get_metric()[0]),
        HashMap::from([("cache".to_string(), TEST_CACHE.to_string())])
    );
}

#[tokio::test]
async fn dry_run_counts_no_evictions() {
    let fake = FakeStore::new(|s| {
        s.size = 1500;
        s.stats = EvictStats {
            retired: 3,
            freed_bytes: 600,
            ..Default::default()
        };
    });
    let (mut g, reg) = new_test_gc(&fake, 1000);
    g.config.dry_run = true;
    g.run_pass().await;
    {
        let st = fake.0.lock();
        assert_eq!((st.evict_calls, st.got_dry_run), (1, true));
    }
    let passes = family(&reg, PASSES_METRIC).expect("pass not recorded");
    assert_eq!(
        passes_by_outcome(&passes),
        HashMap::from([("complete".to_string(), 1)])
    );
    assert!(
        family(&reg, EVICTIONS_METRIC).is_none(),
        "a dry run counted evictions"
    );
}

#[tokio::test]
async fn pass_shortfall_streak() {
    let fake = FakeStore::new(|s| {
        s.size = 1500;
        s.stats = EvictStats {
            retired: 1,
            freed_bytes: 100,
            skipped_linked: 4,
            ..Default::default()
        };
    });
    let (mut g, reg) = new_test_gc(&fake, 1000);
    for _ in 0..3 {
        assert_eq!(g.run_pass().await, PassOutcome::Shortfall);
    }
    assert_eq!(g.consecutive_shortfalls, 3);
    let got = passes_by_outcome(&family(&reg, PASSES_METRIC).unwrap());
    assert_eq!(got.get("shortfall"), Some(&3));
    fake.0.lock().size = 500;
    g.run_pass().await;
    assert_eq!(g.consecutive_shortfalls, 0);
}

#[tokio::test]
async fn size_is_observed_per_cache_on_collection() {
    let fake = FakeStore::new(|s| s.size = 4096);
    // No budget and no pass: the size is still reported on every scrape.
    let (_g, reg) = new_test_gc(&fake, 0);
    let f = family(&reg, SIZE_METRIC).expect("size not observed");
    assert_eq!(f.get_field_type(), MetricType::GAUGE);
    assert_eq!(f.get_metric().len(), 1);
    assert_eq!(f.get_metric()[0].get_gauge().value(), 4096.0);
    assert_eq!(
        labels(&f.get_metric()[0]),
        HashMap::from([("cache".to_string(), TEST_CACHE.to_string())])
    );
    // Read afresh each time.
    fake.0.lock().size = 8192;
    let f = family(&reg, SIZE_METRIC).unwrap();
    assert_eq!(f.get_metric()[0].get_gauge().value(), 8192.0);
}

#[test]
fn two_caches_share_the_instruments() {
    let reg = Registry::new();
    let inst = Instruments::new(&reg).unwrap();
    inst.observe("a", FakeStore::new(|s| s.size = 1));
    inst.observe("b", FakeStore::new(|s| s.size = 2));
    let f = family(&reg, SIZE_METRIC).expect("size not observed");
    let got: HashMap<String, f64> = f
        .get_metric()
        .iter()
        .map(|m| (labels(m)["cache"].clone(), m.get_gauge().value()))
        .collect();
    assert_eq!(
        got,
        HashMap::from([("a".to_string(), 1.0), ("b".to_string(), 2.0)])
    );
}

#[tokio::test]
async fn start_rejects_invalid_config_and_observes_nothing() {
    let reg = Registry::new();
    let inst = Instruments::new(&reg).unwrap();
    let fake = FakeStore::new(|s| s.size = 1);
    let config = GcConfig {
        period: Duration::from_secs(60),
        max_bytes: -1,
        dry_run: false,
    };
    assert!(start(
        &Ctx::background(),
        TEST_CACHE,
        fake,
        config,
        "test-cache",
        Some(inst)
    )
    .is_err());
    assert!(
        family(&reg, SIZE_METRIC).is_none(),
        "start observed a cache whose config it rejected"
    );
}

#[tokio::test]
async fn start_without_budget_still_observes_size() {
    let reg = Registry::new();
    let inst = Instruments::new(&reg).unwrap();
    let fake = FakeStore::new(|s| s.size = 7);
    start(
        &Ctx::background(),
        TEST_CACHE,
        fake.clone(),
        GcConfig::default(),
        "test-cache",
        Some(inst),
    )
    .unwrap();
    let f = family(&reg, SIZE_METRIC).expect("size not observed without a budget");
    assert_eq!(f.get_metric()[0].get_gauge().value(), 7.0);
    assert_eq!(fake.0.lock().evict_calls, 0);
}

#[tokio::test]
async fn run_first_pass_is_immediate() {
    let fake = FakeStore::new(|s| s.size = 1500);
    let (g, _reg) = new_test_gc(&fake, 1000);
    let ctx = Ctx::background();
    ctx.cancel();
    g.run(ctx).await;
    assert_eq!(
        fake.0.lock().evict_calls,
        1,
        "only the immediate first pass"
    );
}

#[tokio::test]
async fn run_ticks() {
    let fake = FakeStore::new(|s| s.size = 1500);
    let (mut g, _reg) = new_test_gc(&fake, 1000);
    g.config.period = Duration::from_millis(10);
    g.run(Ctx::background().with_timeout(Duration::from_millis(150)))
        .await;
    assert!(fake.0.lock().evict_calls >= 2, "first pass plus a tick");
}

#[tokio::test]
async fn pass_recovers_panic() {
    let fake = FakeStore::new(|s| {
        s.size = 1500;
        s.panic_on_evict = true;
    });
    let (mut g, _reg) = new_test_gc(&fake, 1000);
    assert_eq!(g.run_pass().await, PassOutcome::Skipped);
}

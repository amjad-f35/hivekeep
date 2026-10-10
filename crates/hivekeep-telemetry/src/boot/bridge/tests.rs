use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use opentelemetry::metrics::MeterProvider as _;

use super::*;
use crate::boot::otlp::{METRICS_EXPORTER_ENV, OTLP_ENDPOINT_ENV};
use crate::env::MapEnv;
use crate::testutil::{start_fake_collector, FakeCollector};

/// A collector and an environment pointing at it with
/// `OTEL_METRICS_EXPORTER` set to `value` (unset when empty).
async fn collector(value: &str) -> (FakeCollector, MapEnv) {
    let (c, addr) = start_fake_collector().await;
    let mut env = MapEnv::new([(OTLP_ENDPOINT_ENV, format!("http://{addr}").as_str())]);
    if !value.is_empty() {
        env.set(METRICS_EXPORTER_ENV, value);
    }
    (c, env)
}

/// Stands in for a registry the binary does not own: a family recorded
/// outside the OpenTelemetry SDK.
fn bridged_registry() -> prometheus::Registry {
    let reg = prometheus::Registry::new();
    let gauge = prometheus::Gauge::new("test_bridged_family", "recorded outside the SDK").unwrap();
    reg.register(Box::new(gauge.clone())).unwrap();
    gauge.set(1.0);
    reg
}

fn gathered(reg: &prometheus::Registry) -> HashMap<String, bool> {
    reg.gather()
        .iter()
        .map(|f| (f.name().to_string(), true))
        .collect()
}

fn add_one(mp: &SdkMeterProvider, name: &'static str) {
    mp.meter("test").u64_counter(name).build().add(1, &[]);
}

async fn flush(c: &FakeCollector, mp: SdkMeterProvider) -> Option<HashMap<String, usize>> {
    tokio::task::spawn_blocking(move || {
        let _ = mp.force_flush();
        let _ = mp.shutdown();
    })
    .await
    .unwrap();
    c.first_metric_names()
}

fn scope(names: &[&str]) -> ScopeMetrics {
    ScopeMetrics {
        metrics: names
            .iter()
            .map(|n| Metric {
                name: n.to_string(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn names(scopes: &[ScopeMetrics]) -> Vec<String> {
    scopes
        .iter()
        .flat_map(|s| s.metrics.iter().map(|m| m.name.clone()))
        .collect()
}

/// With the OTLP push, the bridged family and the instrument are each
/// pushed once; with the scraped endpoint on too, the instrument is also
/// served from the bridged registry and must not be pushed again under its
/// Prometheus name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_metrics_bridged_pushes_each_metric_once() {
    for (name, value, served) in [
        ("unset", "", true),
        ("otlp", "otlp", false),
        ("list", "otlp,prometheus", true),
    ] {
        let (c, env) = collector(value).await;
        let reg = bridged_registry();
        let b =
            init_metrics_bridged_with_env(&env, "test-bridged-otlp", reg.clone(), None).unwrap();
        assert_eq!(b.serve_pull, served, "{name}");
        add_one(&b.provider, "hk.test.bridged.count");
        let got = gathered(&reg);
        assert_eq!(
            got.contains_key("hk_test_bridged_count_total"),
            served,
            "{name}: {got:?}"
        );
        assert!(
            got.contains_key("test_bridged_family"),
            "{name}: the registry lost its own family"
        );
        let pushed = flush(&c, b.provider).await.unwrap_or_default();
        assert_eq!(
            pushed.get("hk.test.bridged.count"),
            Some(&1),
            "{name}: {pushed:?}"
        );
        assert_eq!(
            pushed.get("test_bridged_family"),
            Some(&1),
            "{name}: {pushed:?}"
        );
        assert!(
            !pushed.contains_key("hk_test_bridged_count_total")
                && !pushed.contains_key("target_info"),
            "{name}: the bridge pushed what the SDK reader already covers: {pushed:?}"
        );
    }
}

struct FailingGatherer;

impl Gatherer for FailingGatherer {
    fn gather(&self) -> Result<Vec<MetricFamily>, ProduceError> {
        Err(ProduceError("boom".into()))
    }
}

/// A skip list that cannot be gathered drops the batch, so nothing is
/// pushed twice; inner's own error is still returned.
#[test]
fn skip_families_drops_batch_on_gather_error() {
    for inner_err in [None, Some(ProduceError("inner failed".into()))] {
        let e = inner_err.clone();
        let inner: Arc<dyn Producer> = Arc::new(move || (vec![scope(&["kept"])], e.clone()));
        let (scopes, err) = SkipFamilies::new(inner, Arc::new(FailingGatherer)).produce();
        assert!(scopes.is_empty(), "{scopes:?}");
        assert_eq!(err, inner_err);
    }
}

/// A family that reaches the skip registry while inner produces is still
/// dropped.
#[test]
fn skip_families_covers_families_registered_during_produce() {
    let skip = prometheus::Registry::new();
    let s = skip.clone();
    let inner: Arc<dyn Producer> = Arc::new(move || {
        let c = prometheus::Counter::new("late_total", "late").unwrap();
        c.inc();
        let _ = s.register(Box::new(c));
        (vec![scope(&["late_total", "kept"])], None)
    });
    let (scopes, err) = SkipFamilies::new(inner, Arc::new(skip)).produce();
    assert!(err.is_none());
    assert_eq!(names(&scopes), vec!["kept".to_string()]);
}

/// The wrapper sits between the bridge and the OTLP push.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_metrics_bridged_wraps_pushed_producer() {
    let (c, env) = collector("otlp").await;
    let produced = Arc::new(AtomicBool::new(false));
    let p = produced.clone();
    let wrap: WrapProducer = Box::new(move |inner: Arc<dyn Producer>| {
        Arc::new(move || {
            p.store(true, Ordering::Relaxed);
            inner.produce()
        }) as Arc<dyn Producer>
    });
    let b =
        init_metrics_bridged_with_env(&env, "test-bridged-wrap", bridged_registry(), Some(wrap))
            .unwrap();
    let pushed = flush(&c, b.provider).await.unwrap_or_default();
    assert!(
        produced.load(Ordering::Relaxed),
        "the push did not collect through the wrapper"
    );
    assert_eq!(pushed.get("test_bridged_family"), Some(&1), "{pushed:?}");
}

/// With `prometheus` alone the instrument is served from the bridged
/// registry next to its own families, and nothing is pushed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_metrics_bridged_prometheus_serves_from_registry() {
    let (c, env) = collector("prometheus").await;
    let reg = bridged_registry();
    let b =
        init_metrics_bridged_with_env(&env, "test-bridged-prometheus", reg.clone(), None).unwrap();
    assert!(b.serve_pull);
    add_one(&b.provider, "hk.test.bridgedprom.count");
    let got = gathered(&reg);
    assert!(
        got.contains_key("hk_test_bridgedprom_count_total")
            && got.contains_key("test_bridged_family"),
        "{got:?}"
    );
    let pushed = flush(&c, b.provider).await;
    assert!(pushed.is_none(), "prometheus alone still pushed {pushed:?}");
}

/// With `none` there is no push and no endpoint, so the instrument is not
/// registered anywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_metrics_bridged_exporter_none_exports_nothing() {
    let (c, env) = collector("none").await;
    let reg = bridged_registry();
    let b = init_metrics_bridged_with_env(&env, "test-bridged-none", reg.clone(), None).unwrap();
    assert!(!b.serve_pull);
    add_one(&b.provider, "hk.test.bridgednone.count");
    assert!(!gathered(&reg).contains_key("hk_test_bridgednone_count_total"));
    let pushed = flush(&c, b.provider).await;
    assert!(pushed.is_none(), "none still pushed {pushed:?}");
}

/// `serve_pull` and the push follow the exporter table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_metrics_bridged_serve_pull() {
    for (name, value, want_push, want_pull) in [
        ("unset", "", true, true),
        ("otlp", "otlp", true, false),
        ("prometheus", "prometheus", false, true),
        ("list", "otlp,prometheus", true, true),
        ("none", "none", false, false),
    ] {
        let (c, env) = collector(value).await;
        let b = init_metrics_bridged_with_env(&env, "test-bridged-table", bridged_registry(), None)
            .unwrap();
        assert_eq!(b.serve_pull, want_pull, "{name}");
        let pushed = flush(&c, b.provider).await.is_some();
        assert_eq!(pushed, want_push, "{name}");
    }
}

#[test]
fn init_metrics_bridged_requires_service_name() {
    assert!(init_metrics_bridged_with_env(
        &MapEnv::default(),
        "",
        prometheus::Registry::new(),
        None
    )
    .is_err());
}

#[test]
fn families_convert_by_type() {
    let reg = prometheus::Registry::new();
    let c = prometheus::IntCounterVec::new(prometheus::Opts::new("c_total", "c"), &["k"]).unwrap();
    let h = prometheus::Histogram::with_opts(
        prometheus::HistogramOpts::new("h_seconds", "h").buckets(vec![1.0, 2.0]),
    )
    .unwrap();
    reg.register(Box::new(c.clone())).unwrap();
    reg.register(Box::new(h.clone())).unwrap();
    c.with_label_values(&["v"]).inc_by(3);
    h.observe(0.5);
    h.observe(1.5);
    h.observe(5.0);
    let s = families_to_scope(&reg.gather());
    assert_eq!(s.scope.as_ref().unwrap().name, BRIDGE_SCOPE);
    let by_name: HashMap<_, _> = s.metrics.iter().map(|m| (m.name.as_str(), m)).collect();
    match by_name["c_total"].data.as_ref().unwrap() {
        metric::Data::Sum(sum) => {
            assert!(sum.is_monotonic);
            let p = &sum.data_points[0];
            assert_eq!(p.value, Some(number_data_point::Value::AsDouble(3.0)));
            assert_eq!(p.attributes[0].key, "k");
        }
        other => panic!("{other:?}"),
    }
    match by_name["h_seconds"].data.as_ref().unwrap() {
        metric::Data::Histogram(hist) => {
            let p = &hist.data_points[0];
            assert_eq!(p.count, 3);
            assert_eq!(p.explicit_bounds, vec![1.0, 2.0]);
            assert_eq!(p.bucket_counts, vec![1, 1, 1]);
        }
        other => panic!("{other:?}"),
    }
}

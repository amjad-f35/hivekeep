//! Pushing a Prometheus registry over OTLP.
//!
//! Several components record into a `prometheus::Registry` they serve on
//! `/metrics`. [`init_metrics_bridged_with_env`] makes those families reach
//! the OTLP collector too, next to the OpenTelemetry instruments, so every
//! metric reaches the backend exactly once on whichever path is active:
//!
//! - With the OTLP push, the registry is gathered on every collection and
//!   its families are appended to the same export request as the
//!   OpenTelemetry instruments.
//! - With the scraped endpoint on as well, the instruments also register on
//!   the registry (through a registry of their own), and the bridge skips
//!   those families so the push does not carry them twice.
//! - With `prometheus` alone nothing is pushed, and the instruments register
//!   on the registry directly, so its endpoint serves both.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    metric, number_data_point, summary_data_point, AggregationTemporality, Gauge, Histogram,
    HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary,
    SummaryDataPoint,
};
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::metrics::data::ResourceMetrics as SdkResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{SdkMeterProvider, Temporality};
use prometheus::core::{Collector, Desc};
use prometheus::proto::{MetricFamily, MetricType};
use tonic::transport::Channel;

use super::error::BootError;
use super::exporters::{EXPORTER_OTLP, EXPORTER_PROMETHEUS};
use super::metrics::new_meter_provider_with;
use super::otlp::metrics_exporters;
use crate::env::{Env, ProcessEnv};

/// The instrumentation scope bridged families are pushed under.
pub const BRIDGE_SCOPE: &str = "hivekeep.prometheus.bridge";

/// The gauge that keeps every bridged collection non-empty.
pub const BRIDGE_UP_METRIC: &str = "hivekeep.telemetry.bridge.up";

/// Why a producer returned no (or partial) data.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ProduceError(pub String);

/// What a [`Producer`] hands the push: the metrics it has, and the error
/// that cut them short, if any. A producer may return metrics and an error
/// together.
pub type Produced = (Vec<ScopeMetrics>, Option<ProduceError>);

/// A source of metrics recorded outside the OpenTelemetry SDK, read on
/// every OTLP collection.
pub trait Producer: Send + Sync + 'static {
    /// The current metrics.
    fn produce(&self) -> Produced;
}

impl<F> Producer for F
where
    F: Fn() -> Produced + Send + Sync + 'static,
{
    fn produce(&self) -> Produced {
        self()
    }
}

/// A gatherable set of Prometheus families. `prometheus::Registry` never
/// fails to gather; the trait leaves room for one that does.
pub trait Gatherer: Send + Sync + 'static {
    /// The current families.
    fn gather(&self) -> Result<Vec<MetricFamily>, ProduceError>;
}

impl Gatherer for prometheus::Registry {
    fn gather(&self) -> Result<Vec<MetricFamily>, ProduceError> {
        Ok(prometheus::Registry::gather(self))
    }
}

/// Converts a registry's families into OTLP metrics, keeping each family's
/// Prometheus name.
pub struct RegistryProducer<G> {
    gatherer: G,
}

impl<G: Gatherer> RegistryProducer<G> {
    /// A producer over `gatherer`.
    pub fn new(gatherer: G) -> Self {
        RegistryProducer { gatherer }
    }
}

impl<G: Gatherer> Producer for RegistryProducer<G> {
    fn produce(&self) -> Produced {
        match self.gatherer.gather() {
            Ok(families) => (vec![families_to_scope(&families)], None),
            Err(e) => (Vec::new(), Some(e)),
        }
    }
}

/// Drops the metrics of `inner` named like a family `skip` gathers. A
/// failed gather drops the whole batch rather than risk pushing a metric
/// twice; `inner`'s own error is still returned.
pub struct SkipFamilies {
    inner: Arc<dyn Producer>,
    skip: Arc<dyn Gatherer>,
}

impl SkipFamilies {
    /// Wraps `inner`.
    pub fn new(inner: Arc<dyn Producer>, skip: Arc<dyn Gatherer>) -> Self {
        SkipFamilies { inner, skip }
    }
}

impl Producer for SkipFamilies {
    fn produce(&self) -> Produced {
        let (mut scopes, err) = self.inner.produce();
        // Gathered after inner, so the skip list covers every family inner
        // saw, including ones registered while it produced.
        let families = match self.skip.gather() {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(err = %e, "Gather the metric families to skip failed; dropping the bridged batch");
                return (Vec::new(), err);
            }
        };
        let skip: HashSet<&str> = families.iter().map(|f| f.name()).collect();
        for s in &mut scopes {
            s.metrics.retain(|m| !skip.contains(m.name.as_str()));
        }
        (scopes, err)
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default()
}

/// The start of every cumulative series: the first time the bridge ran.
fn start_nanos() -> u64 {
    static START: OnceLock<u64> = OnceLock::new();
    *START.get_or_init(now_nanos)
}

fn attrs(m: &prometheus::proto::Metric) -> Vec<KeyValue> {
    m.get_label()
        .iter()
        .map(|l| KeyValue {
            key: l.name().to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(l.value().to_string())),
            }),
            ..Default::default()
        })
        .collect()
}

fn number_point(
    m: &prometheus::proto::Metric,
    value: f64,
    start: u64,
    now: u64,
) -> NumberDataPoint {
    NumberDataPoint {
        attributes: attrs(m),
        start_time_unix_nano: start,
        time_unix_nano: now,
        value: Some(number_data_point::Value::AsDouble(value)),
        ..Default::default()
    }
}

/// One family as an OTLP metric; `None` for an empty family.
pub fn family_to_metric(f: &MetricFamily, start: u64, now: u64) -> Option<Metric> {
    let metrics = f.get_metric();
    if metrics.is_empty() {
        return None;
    }
    let cumulative = AggregationTemporality::Cumulative as i32;
    let data = match f.get_field_type() {
        MetricType::COUNTER => metric::Data::Sum(Sum {
            data_points: metrics
                .iter()
                .map(|m| number_point(m, m.get_counter().value(), start, now))
                .collect(),
            aggregation_temporality: cumulative,
            is_monotonic: true,
        }),
        MetricType::GAUGE => metric::Data::Gauge(Gauge {
            data_points: metrics
                .iter()
                .map(|m| number_point(m, m.get_gauge().value(), start, now))
                .collect(),
        }),
        MetricType::UNTYPED => metric::Data::Gauge(Gauge {
            data_points: metrics
                .iter()
                .map(|m| number_point(m, m.untyped.value(), start, now))
                .collect(),
        }),
        MetricType::HISTOGRAM => metric::Data::Histogram(Histogram {
            data_points: metrics
                .iter()
                .map(|m| {
                    let h = m.get_histogram();
                    let mut bounds = Vec::new();
                    let mut counts = Vec::new();
                    let mut prev = 0u64;
                    for b in h.get_bucket() {
                        if b.upper_bound().is_infinite() {
                            continue;
                        }
                        bounds.push(b.upper_bound());
                        counts.push(b.cumulative_count().saturating_sub(prev));
                        prev = b.cumulative_count();
                    }
                    counts.push(h.get_sample_count().saturating_sub(prev));
                    HistogramDataPoint {
                        attributes: attrs(m),
                        start_time_unix_nano: start,
                        time_unix_nano: now,
                        count: h.get_sample_count(),
                        sum: Some(h.get_sample_sum()),
                        bucket_counts: counts,
                        explicit_bounds: bounds,
                        ..Default::default()
                    }
                })
                .collect(),
            aggregation_temporality: cumulative,
        }),
        MetricType::SUMMARY => metric::Data::Summary(Summary {
            data_points: metrics
                .iter()
                .map(|m| {
                    let s = m.get_summary();
                    SummaryDataPoint {
                        attributes: attrs(m),
                        start_time_unix_nano: start,
                        time_unix_nano: now,
                        count: s.sample_count(),
                        sum: s.sample_sum(),
                        quantile_values: s
                            .get_quantile()
                            .iter()
                            .map(|q| summary_data_point::ValueAtQuantile {
                                quantile: q.quantile(),
                                value: q.value(),
                            })
                            .collect(),
                        flags: 0,
                    }
                })
                .collect(),
        }),
    };
    Some(Metric {
        name: f.name().to_string(),
        description: f.help().to_string(),
        data: Some(data),
        ..Default::default()
    })
}

/// Every non-empty family under the bridge scope.
pub fn families_to_scope(families: &[MetricFamily]) -> ScopeMetrics {
    let (start, now) = (start_nanos(), now_nanos());
    ScopeMetrics {
        scope: Some(InstrumentationScope {
            name: BRIDGE_SCOPE.to_string(),
            ..Default::default()
        }),
        metrics: families
            .iter()
            .filter_map(|f| family_to_metric(f, start, now))
            .collect(),
        schema_url: String::new(),
    }
}

/// An OTLP metric exporter that appends a [`Producer`]'s metrics to every
/// export of the SDK's own.
pub struct BridgeExporter {
    client: MetricsServiceClient<Channel>,
    producer: Option<Arc<dyn Producer>>,
    shut_down: AtomicBool,
}

impl BridgeExporter {
    /// Exports over `channel`, appending `producer`'s metrics.
    pub fn new(channel: Channel, producer: Option<Arc<dyn Producer>>) -> Self {
        BridgeExporter {
            client: MetricsServiceClient::new(channel),
            producer,
            shut_down: AtomicBool::new(false),
        }
    }

    /// The request for one collection: the SDK's metrics, then the
    /// producer's under the same resource.
    pub fn request(&self, metrics: &SdkResourceMetrics) -> ExportMetricsServiceRequest {
        let mut req = ExportMetricsServiceRequest::from(metrics);
        let Some(p) = &self.producer else { return req };
        let (scopes, err) = p.produce();
        if let Some(e) = err {
            tracing::warn!(err = %e, "Produce bridged metrics failed");
        }
        let scopes: Vec<ScopeMetrics> = scopes
            .into_iter()
            .filter(|s| !s.metrics.is_empty())
            .collect();
        if scopes.is_empty() {
            return req;
        }
        match req.resource_metrics.first_mut() {
            Some(rm) => rm.scope_metrics.extend(scopes),
            None => req.resource_metrics.push(ResourceMetrics {
                scope_metrics: scopes,
                ..Default::default()
            }),
        }
        req
    }
}

impl PushMetricExporter for BridgeExporter {
    async fn export(&self, metrics: &SdkResourceMetrics) -> OTelSdkResult {
        if self.shut_down.load(Ordering::Relaxed) {
            return Err(OTelSdkError::AlreadyShutdown);
        }
        let req = self.request(metrics);
        self.client
            .clone()
            .export(req)
            .await
            .map(|_| ())
            .map_err(|s| OTelSdkError::InternalFailure(format!("export metrics: {s}")))
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.shut_down.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

/// Serves one registry's families from another, so the bridged registry's
/// endpoint also shows the OpenTelemetry instruments.
struct RegistryCollector {
    registry: prometheus::Registry,
    desc: Desc,
}

impl RegistryCollector {
    fn new(registry: prometheus::Registry) -> Result<Self, BootError> {
        // A registry keys collectors by their descriptors; one without any
        // would collide with every other descriptor-less collector (the
        // OpenTelemetry exporter is one), so this one carries a marker that
        // never appears in a gather.
        let desc = Desc::new(
            "hivekeep_otel_instruments".into(),
            "OpenTelemetry instruments served from this registry.".into(),
            vec![],
            Default::default(),
        )
        .map_err(|e| BootError::Exporter {
            what: "bridged registry collector",
            source: Box::new(e),
        })?;
        Ok(RegistryCollector { registry, desc })
    }
}

impl Collector for RegistryCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.registry.gather()
    }
}

/// Rewrites the bridge producer before it reaches the OTLP push.
pub type WrapProducer = Box<dyn FnOnce(Arc<dyn Producer>) -> Arc<dyn Producer> + Send>;

/// The provider [`init_metrics_bridged`] installed, and whether the caller
/// must keep serving the registry's scraped endpoint: `OTEL_METRICS_EXPORTER`
/// unset, `prometheus`, or a list with it. `otlp` and `none` turn it off,
/// and with `none` no reader is installed at all.
pub struct Bridged {
    /// The global meter provider.
    pub provider: SdkMeterProvider,
    /// Whether the scraped endpoint stays on.
    pub serve_pull: bool,
}

/// [`init_metrics_bridged_with_env`] reading the process environment.
pub fn init_metrics_bridged(
    service_name: &str,
    registry: prometheus::Registry,
    wrap: Option<WrapProducer>,
) -> Result<Bridged, BootError> {
    init_metrics_bridged_with_env(&ProcessEnv, service_name, registry, wrap)
}

/// Installs a meter provider for a binary whose scraped endpoint serves
/// `registry`; see the module docs. `wrap`, when set, wraps the bridge
/// producer on the OTLP path, for a rewrite the push backend needs but the
/// scraped endpoint does not. Must be called inside a Tokio runtime when
/// the push is on.
pub fn init_metrics_bridged_with_env(
    env: &dyn Env,
    service_name: &str,
    registry: prometheus::Registry,
    wrap: Option<WrapProducer>,
) -> Result<Bridged, BootError> {
    if service_name.is_empty() {
        return Err(BootError::Missing("service name"));
    }
    let exporters = metrics_exporters(env, true);
    let serve_pull = exporters.has(EXPORTER_PROMETHEUS);
    let push = exporters.has(EXPORTER_OTLP);

    let mut bridge: Option<Arc<dyn Producer>> =
        push.then(|| Arc::new(RegistryProducer::new(registry.clone())) as Arc<dyn Producer>);
    let mut prom_reader = None;
    if serve_pull {
        let mut target = registry.clone();
        let mut own = None;
        if push {
            // `own` holds only the instruments, so the bridge can tell which
            // of the registry's families are not its to push.
            let r = prometheus::Registry::new();
            target = r.clone();
            own = Some(r);
        }
        prom_reader = Some(
            opentelemetry_prometheus::exporter()
                .with_registry(target)
                .build()
                .map_err(|e| BootError::Exporter {
                    what: "Prometheus metric exporter",
                    source: Box::new(e),
                })?,
        );
        if let Some(own) = own {
            registry
                .register(Box::new(RegistryCollector::new(own.clone())?))
                .map_err(|e| BootError::Exporter {
                    what: "serving the instruments from the bridged registry",
                    source: Box::new(e),
                })?;
            bridge =
                bridge.map(|b| Arc::new(SkipFamilies::new(b, Arc::new(own))) as Arc<dyn Producer>);
        }
    }
    if let Some(wrap) = wrap {
        bridge = bridge.map(wrap);
    }
    let provider =
        new_meter_provider_with(
            env,
            service_name,
            push,
            false,
            None,
            bridge,
            |b| match prom_reader {
                Some(r) => b.with_reader(r),
                None => b,
            },
        )?;
    if push {
        // The SDK skips a collection with no instruments of its own, which
        // would starve the bridge in a binary that records only into the
        // registry; this gauge keeps every collection non-empty.
        use opentelemetry::metrics::MeterProvider as _;
        let _up = provider
            .meter(BRIDGE_SCOPE)
            .u64_observable_gauge(BRIDGE_UP_METRIC)
            .with_description("1 while the registry bridge pushes over OTLP.")
            .with_callback(|o| o.observe(1, &[]))
            .build();
    }
    Ok(Bridged {
        provider,
        serve_pull,
    })
}

#[cfg(test)]
mod tests;

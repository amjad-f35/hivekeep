//! The global meter provider: a Prometheus reader for the scraped
//! `/metrics` endpoint and an OTLP periodic reader for the push.

use opentelemetry::global;
use opentelemetry_otlp::{MetricExporter, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::metrics::{Instrument, MeterProviderBuilder, SdkMeterProvider, Stream};
use tonic::transport::Channel;

use std::sync::Arc;

use super::bridge::{BridgeExporter, Producer};
use super::error::BootError;
use super::otlp::{
    has_otlp_destination, metrics_push_enabled, otlp_endpoint, otlp_export_blocked,
    METRICS_ENDPOINT_ENV,
};
use super::resource::{new_resource, relay_attrs};
use crate::env::{Env, ProcessEnv};

/// The instrumentation scope of the gRPC client and server metrics. Its
/// instruments keep only [`RPC_METRIC_KEYS`]; see [`rpc_metrics_view`].
pub const GRPC_METRICS_SCOPE: &str = "hivekeep.grpc";

/// The attributes the gRPC metrics keep. `server.address` is left out: a
/// client reads it from the remote address of its connection, and a socket
/// path holding a worker pod UID or the IP of one server pod has one value
/// per pod. `server.port` stays, because it has one value per service.
pub const RPC_METRIC_KEYS: &[&str] = &[
    "rpc.system",
    "rpc.service",
    "rpc.method",
    "rpc.grpc.status_code",
    "rpc.response.status_code",
    "server.port",
    "network.transport",
    "error.type",
];

/// The view applied to every meter provider: gRPC instruments keep only
/// [`RPC_METRIC_KEYS`].
pub fn rpc_metrics_view(inst: &Instrument) -> Option<Stream> {
    if inst.scope().name() != GRPC_METRICS_SCOPE {
        return None;
    }
    Stream::builder()
        .with_allowed_attribute_keys(
            RPC_METRIC_KEYS
                .iter()
                .map(|k| opentelemetry::Key::from_static_str(k)),
        )
        .build()
        .ok()
}

/// Builds and installs a meter provider with a Prometheus reader registered
/// on the default registry (served by [`super::health::metrics_response`])
/// and, while `OTEL_METRICS_EXPORTER` selects `otlp`, an OTLP reader. Must
/// be called inside a Tokio runtime when the OTLP reader is installed.
pub fn init_metrics(service_name: &str) -> Result<SdkMeterProvider, BootError> {
    init_metrics_with_env(
        &ProcessEnv,
        service_name,
        prometheus::default_registry().clone(),
    )
}

/// [`init_metrics`] reading configuration from `env` and registering the
/// Prometheus reader on `registry`.
pub fn init_metrics_with_env(
    env: &dyn Env,
    service_name: &str,
    registry: prometheus::Registry,
) -> Result<SdkMeterProvider, BootError> {
    if service_name.is_empty() {
        return Err(BootError::Missing("service name"));
    }
    let prom = opentelemetry_prometheus::exporter()
        .with_registry(registry)
        .build()
        .map_err(|e| BootError::Exporter {
            what: "Prometheus metric exporter",
            source: Box::new(e),
        })?;
    let push = metrics_push_enabled(env, true);
    new_meter_provider(env, service_name, push, false, None, |b| {
        b.with_reader(prom)
    })
}

/// A meter provider without the Prometheus reader, for a binary that runs
/// no metrics server of its own (a sandbox worker). `channel` is the
/// metrics counterpart of [`super::tracing::TracingOptions::exporter_channel`].
/// Calling this marks the component relay-capable, so its metrics carry the
/// relay attribute either way.
pub fn init_metrics_push_only_via(
    service_name: &str,
    channel: Option<Channel>,
) -> Result<SdkMeterProvider, BootError> {
    init_metrics_push_only_with_env(&ProcessEnv, service_name, channel)
}

/// [`init_metrics_push_only_via`] reading configuration from `env`.
pub fn init_metrics_push_only_with_env(
    env: &dyn Env,
    service_name: &str,
    channel: Option<Channel>,
) -> Result<SdkMeterProvider, BootError> {
    let push = metrics_push_enabled(env, false);
    new_meter_provider(env, service_name, push, true, channel, |b| b)
}

/// The shared provider construction. `customize` adds readers beyond the
/// OTLP one.
pub(crate) fn new_meter_provider(
    env: &dyn Env,
    service_name: &str,
    push: bool,
    relay_capable: bool,
    channel: Option<Channel>,
    customize: impl FnOnce(MeterProviderBuilder) -> MeterProviderBuilder,
) -> Result<SdkMeterProvider, BootError> {
    new_meter_provider_with(
        env,
        service_name,
        push,
        relay_capable,
        channel,
        None,
        customize,
    )
}

/// [`new_meter_provider`] whose OTLP push also carries `bridge`'s metrics
/// (see [`super::bridge`]).
pub(crate) fn new_meter_provider_with(
    env: &dyn Env,
    service_name: &str,
    push: bool,
    relay_capable: bool,
    channel: Option<Channel>,
    bridge: Option<Arc<dyn Producer>>,
    customize: impl FnOnce(MeterProviderBuilder) -> MeterProviderBuilder,
) -> Result<SdkMeterProvider, BootError> {
    if service_name.is_empty() {
        return Err(BootError::Missing("service name"));
    }
    let has_relay = channel.is_some();
    let resource = new_resource(env, service_name, relay_attrs(relay_capable, has_relay));
    let mut builder = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_view(rpc_metrics_view);
    let (reason, level) = otlp_export_blocked(
        push,
        has_otlp_destination(env, METRICS_ENDPOINT_ENV, relay_capable, has_relay),
    );
    if reason.is_none() && bridge.is_some() {
        let ch = match channel {
            Some(ch) => ch,
            None => {
                tonic::transport::Endpoint::from_shared(otlp_endpoint(env, METRICS_ENDPOINT_ENV))
                    .map_err(|e| BootError::Exporter {
                        what: "OTLP metric endpoint",
                        source: Box::new(e),
                    })?
                    .connect_lazy()
            }
        };
        builder = builder.with_periodic_exporter(BridgeExporter::new(ch, bridge));
    } else if reason.is_none() {
        let exporter = match channel {
            Some(ch) => MetricExporter::builder()
                .with_tonic()
                .with_channel(ch)
                .build(),
            None => MetricExporter::builder()
                .with_tonic()
                .with_endpoint(otlp_endpoint(env, METRICS_ENDPOINT_ENV))
                .build(),
        }
        .map_err(|e| BootError::Exporter {
            what: "OTLP metric exporter",
            source: Box::new(e),
        })?;
        builder = builder.with_periodic_exporter(exporter);
    }
    let provider = customize(builder).build();
    global::set_meter_provider(provider.clone());
    let otlp_export = reason.is_none();
    let reason = reason.unwrap_or("");
    if level == tracing::Level::WARN {
        tracing::warn!(otlp_export, reason, "Metrics initialized");
    } else {
        tracing::info!(otlp_export, reason, "Metrics initialized");
    }
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attrs::OTLP_RELAY_KEY;
    use crate::boot::otlp::{METRICS_EXPORTER_ENV, OTLP_ENDPOINT_ENV};
    use crate::env::MapEnv;
    use crate::testutil::{channel_to, lazy_channel, start_fake_collector, FakeCollector};
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader};

    /// The resource a provider stamps on its exports, read back through an
    /// in-memory reader next to the OTLP one.
    fn collected_resource(
        relay_capable: bool,
        channel: Option<Channel>,
    ) -> std::collections::HashMap<String, String> {
        let mem = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(mem.clone()).build();
        let mp = new_meter_provider(
            &MapEnv::default(),
            "hivekeep-sandbox-gvisor",
            false,
            relay_capable,
            channel,
            |b| b.with_reader(reader),
        )
        .unwrap();
        mp.meter("test").u64_counter("c").build().add(1, &[]);
        mp.force_flush().unwrap();
        let got = mem.get_finished_metrics().unwrap();
        let _ = mp.shutdown();
        crate::boot::resource::tests::resource_attrs(got[0].resource())
    }

    #[tokio::test]
    async fn meter_provider_relay_attribute() {
        assert_eq!(
            collected_resource(true, Some(lazy_channel()))
                .get(OTLP_RELAY_KEY)
                .map(String::as_str),
            Some("relay")
        );
        assert_eq!(
            collected_resource(true, None)
                .get(OTLP_RELAY_KEY)
                .map(String::as_str),
            Some("direct")
        );
        assert!(!collected_resource(false, None).contains_key(OTLP_RELAY_KEY));
    }

    #[test]
    fn meter_provider_drops_rpc_server_address() {
        let mem = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(mem.clone()).build();
        let mp = new_meter_provider(&MapEnv::default(), "keeper", false, false, None, |b| {
            b.with_reader(reader)
        })
        .unwrap();
        let meter = mp.meter(GRPC_METRICS_SCOPE);
        meter
            .f64_histogram("rpc.client.call.duration")
            .build()
            .record(
                0.5,
                &[
                    KeyValue::new("server.address", "/run/hivekeep/pod-123.sock"),
                    KeyValue::new("server.port", 0),
                    KeyValue::new("rpc.method", "Check"),
                ],
            );
        mp.force_flush().unwrap();
        let got = mem.get_finished_metrics().unwrap();
        let mut points = Vec::new();
        for rm in &got {
            for sm in rm.scope_metrics() {
                for m in sm.metrics() {
                    if m.name() == "rpc.client.call.duration" {
                        if let AggregatedMetrics::F64(MetricData::Histogram(h)) = m.data() {
                            for p in h.data_points() {
                                points.push(
                                    p.attributes()
                                        .map(|kv| kv.key.to_string())
                                        .collect::<Vec<_>>(),
                                );
                            }
                        }
                    }
                }
            }
        }
        let _ = mp.shutdown();
        assert_eq!(points.len(), 1, "{points:?}");
        assert!(!points[0].contains(&"server.address".to_string()));
        for key in ["server.port", "rpc.method"] {
            assert!(points[0].contains(&key.to_string()), "{key} absent");
        }
    }

    fn add_one(mp: &SdkMeterProvider, name: &'static str) {
        mp.meter("test").u64_counter(name).build().add(1, &[]);
    }

    fn flush(
        c: &FakeCollector,
        mp: SdkMeterProvider,
    ) -> Option<std::collections::HashMap<String, usize>> {
        let _ = mp.force_flush();
        let _ = mp.shutdown();
        c.first_metric_names()
    }

    fn gathered(reg: &prometheus::Registry) -> Vec<String> {
        reg.gather().iter().map(|f| f.name().to_string()).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn init_metrics_push_only_has_no_prometheus_surface() {
        let mp = init_metrics_push_only_with_env(
            &MapEnv::default(),
            "test-pushonly",
            Some(lazy_channel()),
        )
        .unwrap();
        add_one(&mp, "hk.test.pushonly.count");
        let names = gathered(prometheus::default_registry());
        assert!(
            !names.iter().any(|n| n.contains("hk_test_pushonly")),
            "{names:?}"
        );
        tokio::task::spawn_blocking(move || {
            let _ = mp.shutdown_with_timeout(std::time::Duration::from_secs(1));
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn init_metrics_exports_over_otlp_by_default() {
        let (c, addr) = start_fake_collector().await;
        let env = MapEnv::new([(OTLP_ENDPOINT_ENV, format!("http://{addr}").as_str())]);
        let mp = init_metrics_with_env(&env, "test-default", prometheus::Registry::new()).unwrap();
        add_one(&mp, "hk.test.default.count");
        let pushed = tokio::task::spawn_blocking(move || flush(&c, mp))
            .await
            .unwrap();
        assert_eq!(
            pushed.unwrap_or_default().get("hk.test.default.count"),
            Some(&1)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn init_metrics_exporter_none_keeps_prometheus_only() {
        let (c, addr) = start_fake_collector().await;
        let env = MapEnv::new([
            (OTLP_ENDPOINT_ENV, format!("http://{addr}").as_str()),
            (METRICS_EXPORTER_ENV, "none"),
        ]);
        let reg = prometheus::Registry::new();
        let mp = init_metrics_with_env(&env, "test-none", reg.clone()).unwrap();
        add_one(&mp, "hk.test.none.count");
        assert!(
            gathered(&reg).contains(&"hk_test_none_count_total".to_string()),
            "{:?}",
            gathered(&reg)
        );
        let pushed = tokio::task::spawn_blocking(move || flush(&c, mp))
            .await
            .unwrap();
        assert!(
            pushed.is_none(),
            "OTEL_METRICS_EXPORTER=none still pushed {pushed:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn init_metrics_push_only_exporter_none_exports_nothing() {
        let (c, addr) = start_fake_collector().await;
        let env = MapEnv::new([
            (OTLP_ENDPOINT_ENV, format!("http://{addr}").as_str()),
            (METRICS_EXPORTER_ENV, "none"),
        ]);
        let mp = init_metrics_push_only_with_env(&env, "test-pushonly-none", None).unwrap();
        add_one(&mp, "hk.test.pushonlynone.count");
        let pushed = tokio::task::spawn_blocking(move || flush(&c, mp))
            .await
            .unwrap();
        assert!(pushed.is_none());
    }

    #[test]
    fn init_metrics_requires_service_name() {
        assert!(
            init_metrics_with_env(&MapEnv::default(), "", prometheus::Registry::new()).is_err()
        );
        assert!(init_metrics_push_only_with_env(&MapEnv::default(), "", None).is_err());
    }

    #[test]
    fn init_metrics_push_only_without_destination_exports_nothing() {
        let out = crate::contextlogging::tests::capture(|| {
            let mp =
                init_metrics_push_only_with_env(&MapEnv::default(), "test-metrics-nowhere", None)
                    .unwrap();
            let _ = mp.shutdown();
        });
        let line = out
            .lines()
            .find(|l| l.contains("Metrics initialized"))
            .expect(&out);
        assert!(
            line.contains(r#""level":"WARN""#) && line.contains(r#""otlp_export":false"#),
            "{line}"
        );
        assert!(
            line.contains(r#""reason":"no OTLP relay or endpoint""#),
            "{line}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn init_metrics_push_only_exports_through_channel_without_endpoint() {
        let (c, addr) = start_fake_collector().await;
        let mp = init_metrics_push_only_with_env(
            &MapEnv::default(),
            "test-metrics-relay",
            Some(channel_to(addr)),
        )
        .unwrap();
        add_one(&mp, "hk.test.relay.count");
        let pushed = tokio::task::spawn_blocking(move || flush(&c, mp))
            .await
            .unwrap();
        assert_eq!(
            pushed.unwrap_or_default().get("hk.test.relay.count"),
            Some(&1)
        );
    }
}

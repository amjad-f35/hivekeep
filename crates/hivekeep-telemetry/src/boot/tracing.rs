//! The global tracer provider.

use opentelemetry::global;
use opentelemetry_otlp::{SpanExporter, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tonic::transport::Channel;

use super::error::BootError;
use super::otlp::{
    has_otlp_destination, otlp_endpoint, otlp_export_blocked, traces_push_enabled,
    TRACES_ENDPOINT_ENV,
};
use super::resource::{new_resource, relay_attrs};
use super::sampling::TraceSampling;
use crate::env::{Env, ProcessEnv};

/// Configures [`init_tracing`].
#[derive(Debug, Clone, Default)]
pub struct TracingOptions {
    /// Required; the resource's `service.name`.
    pub service_name: String,
    /// Required. Build it with [`super::sampling::resolve_trace_sampling`]
    /// so the environment can override the component default.
    pub sampling: TraceSampling,
    /// When set, the connection the OTLP exporter sends over instead of
    /// dialing the configured endpoint. Sandbox workers pass the node relay
    /// socket (see [`crate::otlprelay::dial`]) so a worker pod exports
    /// without a network path of its own.
    pub exporter_channel: Option<Channel>,
    /// Marks a component meant to export through the relay, whether or not
    /// it managed to, so its resource says which path it took. Separate
    /// from `exporter_channel` because an absent channel alone cannot tell
    /// "tried and fell back" from "never had a relay".
    pub relay_capable: bool,
}

/// Builds and installs the global tracer provider with the W3C trace
/// context propagator. Must be called inside a Tokio runtime when an OTLP
/// exporter is installed.
pub fn init_tracing(opts: TracingOptions) -> Result<SdkTracerProvider, BootError> {
    init_tracing_with_env(&ProcessEnv, opts)
}

/// [`init_tracing`] reading configuration from `env`.
pub fn init_tracing_with_env(
    env: &dyn Env,
    opts: TracingOptions,
) -> Result<SdkTracerProvider, BootError> {
    if opts.service_name.is_empty() {
        return Err(BootError::Missing("TracingOptions.service_name"));
    }
    let Some(sampler) = opts.sampling.sampler().cloned() else {
        return Err(BootError::Missing("TracingOptions.sampling"));
    };
    let has_relay = opts.exporter_channel.is_some();
    let resource = new_resource(
        env,
        &opts.service_name,
        relay_attrs(opts.relay_capable, has_relay),
    );

    let mut builder = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_sampler(sampler.to_sdk());
    // Without an exporter the provider still samples and propagates trace
    // context, so a component that exports nothing keeps the traces of the
    // components around it whole.
    let (reason, level) = otlp_export_blocked(
        traces_push_enabled(env),
        has_otlp_destination(env, TRACES_ENDPOINT_ENV, opts.relay_capable, has_relay),
    );
    if reason.is_none() {
        let exporter = match opts.exporter_channel {
            Some(ch) => SpanExporter::builder()
                .with_tonic()
                .with_channel(ch)
                .build(),
            None => SpanExporter::builder()
                .with_tonic()
                .with_endpoint(otlp_endpoint(env, TRACES_ENDPOINT_ENV))
                .build(),
        }
        .map_err(|e| BootError::Exporter {
            what: "OTLP span exporter",
            source: Box::new(e),
        })?;
        builder = builder.with_batch_exporter(exporter);
    }
    let provider = builder.build();
    global::set_tracer_provider(provider.clone());
    global::set_text_map_propagator(TraceContextPropagator::new());

    let export = reason.is_none();
    let reason = reason.unwrap_or("");
    if level == tracing::Level::WARN {
        tracing::warn!(sampler = %sampler, export, reason, "Tracing initialized");
    } else {
        tracing::info!(sampler = %sampler, export, reason, "Tracing initialized");
    }
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attrs::OTLP_RELAY_KEY;
    use crate::boot::otlp::{OTLP_ENDPOINT_ENV, TRACES_EXPORTER_ENV};
    use crate::boot::sampling::parent_ratio_sampling;
    use crate::env::MapEnv;
    use crate::testutil::{channel_to, start_fake_collector};
    use opentelemetry::trace::{Span, Tracer, TracerProvider};

    /// Ends one sampled span, then flushes and shuts the provider down.
    pub(crate) fn end_one_span(env: &MapEnv, mut opts: TracingOptions) {
        opts.sampling = parent_ratio_sampling(1.0);
        let tp = init_tracing_with_env(env, opts).unwrap();
        let mut span = tp.tracer("test").start("test");
        assert!(span.span_context().is_sampled());
        span.end();
        let _ = tp.force_flush();
        let _ = tp.shutdown();
    }

    #[test]
    fn init_tracing_requires_sampling() {
        let err = init_tracing_with_env(
            &MapEnv::default(),
            TracingOptions {
                service_name: "x".into(),
                ..Default::default()
            },
        );
        assert!(err.is_err());
        assert!(init_tracing_with_env(&MapEnv::default(), TracingOptions::default()).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn init_tracing_exports_over_otlp_by_default() {
        let (c, addr) = start_fake_collector().await;
        let env = MapEnv::new([(OTLP_ENDPOINT_ENV, format!("http://{addr}").as_str())]);
        let opts = TracingOptions {
            service_name: "test-traces-default".into(),
            ..Default::default()
        };
        tokio::task::spawn_blocking(move || end_one_span(&env, opts))
            .await
            .unwrap();
        assert_eq!(c.span_count(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn init_tracing_exporter_none_exports_nothing() {
        let (c, addr) = start_fake_collector().await;
        let env = MapEnv::new([
            (OTLP_ENDPOINT_ENV, format!("http://{addr}").as_str()),
            (TRACES_EXPORTER_ENV, "none"),
        ]);
        let opts = TracingOptions {
            service_name: "test-traces-none".into(),
            ..Default::default()
        };
        tokio::task::spawn_blocking(move || end_one_span(&env, opts))
            .await
            .unwrap();
        assert_eq!(c.span_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn init_tracing_relay_capable_exports_through_channel_without_endpoint() {
        let (c, addr) = start_fake_collector().await;
        let opts = TracingOptions {
            service_name: "test-traces-relay".into(),
            exporter_channel: Some(channel_to(addr)),
            relay_capable: true,
            ..Default::default()
        };
        tokio::task::spawn_blocking(move || end_one_span(&MapEnv::default(), opts))
            .await
            .unwrap();
        assert_eq!(c.span_count(), 1);
        let r = c.received.lock().unwrap();
        let attrs = &r.traces[0].resource_spans[0]
            .resource
            .as_ref()
            .unwrap()
            .attributes;
        assert!(attrs.iter().any(|kv| kv.key == OTLP_RELAY_KEY));
    }

    #[test]
    fn init_tracing_relay_capable_without_destination_exports_nothing() {
        let out = crate::contextlogging::tests::capture(|| {
            end_one_span(
                &MapEnv::default(),
                TracingOptions {
                    service_name: "test-traces-nowhere".into(),
                    relay_capable: true,
                    ..Default::default()
                },
            );
        });
        let line = out
            .lines()
            .find(|l| l.contains("Tracing initialized"))
            .expect(&out);
        assert!(line.contains(r#""level":"WARN""#), "{line}");
        assert!(
            line.contains(r#""export":false"#)
                && line.contains(r#""reason":"no OTLP relay or endpoint""#),
            "{line}"
        );
    }
}

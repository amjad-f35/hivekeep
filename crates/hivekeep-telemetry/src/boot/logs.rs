//! The OTLP logger provider for the structured records components emit
//! (Actor lifecycle events, usage samples), and whether those records also
//! go to stdout.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, RwLock};

use opentelemetry_otlp::{LogExporter, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::logs::{LoggerProviderBuilder, SdkLoggerProvider};
use tonic::transport::Channel;

use super::error::BootError;
use super::exporters::{Exporters, EXPORTER_CONSOLE, EXPORTER_OTLP};
use super::otlp::{has_otlp_destination, otlp_endpoint, otlp_export_blocked, LOGS_ENDPOINT_ENV};
use super::resource::{new_resource, relay_attrs};
use crate::env::{Env, ProcessEnv};

/// Configures [`init_logging`].
#[derive(Debug, Clone, Default)]
pub struct LoggingOptions {
    /// Required; the resource's `service.name`.
    pub service_name: String,
    /// Required; the empty set is `none`. Build it with
    /// [`super::otlp::resolve_logs_exporter`].
    pub exporter: Option<Exporters>,
    /// The logs counterpart of
    /// [`super::tracing::TracingOptions::exporter_channel`].
    pub exporter_channel: Option<Channel>,
    /// The logs counterpart of
    /// [`super::tracing::TracingOptions::relay_capable`].
    pub relay_capable: bool,
}

static CONSOLE: AtomicBool = AtomicBool::new(false);
static PROVIDER: LazyLock<RwLock<Option<SdkLoggerProvider>>> = LazyLock::new(|| RwLock::new(None));

/// Whether structured records are also written to stdout as JSON lines.
pub fn console_enabled() -> bool {
    CONSOLE.load(Ordering::Relaxed)
}

/// Sets [`console_enabled`].
pub fn set_console(on: bool) {
    CONSOLE.store(on, Ordering::Relaxed);
}

/// The logger provider [`init_logging`] installed, if any.
pub fn logger_provider() -> Option<SdkLoggerProvider> {
    PROVIDER.read().expect("logger provider lock").clone()
}

/// Installs the process logger provider and sets [`console_enabled`].
///
/// Returns `None`, leaving any installed provider alone, when the exporter
/// does not include `otlp`, or when a relay-capable component has neither a
/// relay connection nor an OTLP endpoint: emitting then costs one check.
///
/// The processor batches. These records sit on the Actor resume and
/// suspend path, and exporting inside the emit call would put a blocking
/// round trip there, so an unreachable collector would surface as
/// control-plane latency. The cost is that an ungraceful exit loses the
/// records still queued, unless `console` also wrote them to stdout.
pub fn init_logging(opts: LoggingOptions) -> Result<Option<SdkLoggerProvider>, BootError> {
    init_logging_with_env(&ProcessEnv, opts, |b| b)
}

/// [`init_logging`] reading configuration from `env`; `customize` can add
/// processors.
pub fn init_logging_with_env(
    env: &dyn Env,
    opts: LoggingOptions,
    customize: impl FnOnce(LoggerProviderBuilder) -> LoggerProviderBuilder,
) -> Result<Option<SdkLoggerProvider>, BootError> {
    if opts.service_name.is_empty() {
        return Err(BootError::Missing("LoggingOptions.service_name"));
    }
    let Some(exporter) = opts.exporter.clone() else {
        return Err(BootError::Missing("LoggingOptions.exporter"));
    };
    set_console(exporter.has(EXPORTER_CONSOLE));
    let has_relay = opts.exporter_channel.is_some();
    let (reason, level) = otlp_export_blocked(
        exporter.has(EXPORTER_OTLP),
        has_otlp_destination(env, LOGS_ENDPOINT_ENV, opts.relay_capable, has_relay),
    );
    if let Some(reason) = reason {
        if level == tracing::Level::WARN {
            tracing::warn!(exporter = %exporter, reason, "OTLP logs export disabled");
        } else {
            tracing::info!(exporter = %exporter, reason, "OTLP logs export disabled");
        }
        return Ok(None);
    }
    let lp = new_logger_provider(env, opts, customize)?;
    *PROVIDER.write().expect("logger provider lock") = Some(lp.clone());
    tracing::info!(exporter = %exporter, "Logging initialized");
    Ok(Some(lp))
}

/// Builds the provider [`init_logging`] installs.
pub(crate) fn new_logger_provider(
    env: &dyn Env,
    opts: LoggingOptions,
    customize: impl FnOnce(LoggerProviderBuilder) -> LoggerProviderBuilder,
) -> Result<SdkLoggerProvider, BootError> {
    let has_relay = opts.exporter_channel.is_some();
    let resource = new_resource(
        env,
        &opts.service_name,
        relay_attrs(opts.relay_capable, has_relay),
    );
    let exporter = match opts.exporter_channel {
        Some(ch) => LogExporter::builder().with_tonic().with_channel(ch).build(),
        None => LogExporter::builder()
            .with_tonic()
            .with_endpoint(otlp_endpoint(env, LOGS_ENDPOINT_ENV))
            .build(),
    }
    .map_err(|e| BootError::Exporter {
        what: "OTLP log exporter",
        source: Box::new(e),
    })?;
    let builder = SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter);
    Ok(customize(builder).build())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::attrs::OTLP_RELAY_KEY;
    use crate::boot::otlp::OTLP_ENDPOINT_ENV;
    use crate::env::MapEnv;
    use crate::testutil::lazy_channel;
    use opentelemetry::logs::{LogRecord, Logger, LoggerProvider};
    use opentelemetry::InstrumentationScope;
    use opentelemetry_sdk::logs::{LogProcessor, SdkLogRecord};
    use opentelemetry_sdk::Resource;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// Serializes the tests that touch the process-wide console flag and
    /// installed provider.
    pub(crate) static GLOBAL: Mutex<()> = Mutex::new(());

    #[derive(Debug, Clone, Default)]
    struct CaptureProcessor(Arc<Mutex<Option<HashMap<String, String>>>>);

    impl LogProcessor for CaptureProcessor {
        fn emit(&self, _: &mut SdkLogRecord, _: &InstrumentationScope) {}
        fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
            Ok(())
        }
        fn set_resource(&mut self, resource: &Resource) {
            *self.0.lock().unwrap() = Some(crate::boot::resource::tests::resource_attrs(resource));
        }
    }

    fn opts(exporter: Option<Exporters>) -> LoggingOptions {
        LoggingOptions {
            service_name: "test".into(),
            exporter,
            ..Default::default()
        }
    }

    #[test]
    fn init_logging_disabled() {
        let _g = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        let before = logger_provider().is_some();
        let lp = init_logging_with_env(&MapEnv::default(), opts(Some(Exporters::none())), |b| b)
            .unwrap();
        assert!(lp.is_none());
        assert_eq!(
            logger_provider().is_some(),
            before,
            "the installed provider changed"
        );
    }

    /// Shuts a provider down off the runtime threads: its batch processor
    /// blocks while the exporter flushes through the runtime.
    async fn shut(lp: SdkLoggerProvider) {
        tokio::task::spawn_blocking(move || {
            let _ = lp.shutdown_with_timeout(std::time::Duration::from_secs(1));
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn logger_provider_relay_attribute() {
        for (name, capable, channel, want) in [
            ("relay", true, Some(lazy_channel()), Some("relay")),
            ("direct", true, None, Some("direct")),
            ("not relay capable", false, None, None),
        ] {
            let proc = CaptureProcessor::default();
            let p = proc.clone();
            let lp = new_logger_provider(
                &MapEnv::default(),
                LoggingOptions {
                    service_name: "hivekeep-sandbox-gvisor".into(),
                    exporter: Some(Exporters::of([EXPORTER_OTLP])),
                    exporter_channel: channel,
                    relay_capable: capable,
                },
                move |b| b.with_log_processor(p),
            )
            .unwrap();
            let logger = lp.logger("test");
            let mut rec = logger.create_log_record();
            rec.set_body("x".into());
            logger.emit(rec);
            let attrs = proc
                .0
                .lock()
                .unwrap()
                .clone()
                .expect("no resource reached the processor");
            assert_eq!(
                attrs.get(OTLP_RELAY_KEY).map(String::as_str),
                want,
                "{name}"
            );
            shut(lp).await;
        }
    }

    #[test]
    fn init_logging_requires_options() {
        let _g = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        let no_name = LoggingOptions {
            exporter: Some(Exporters::of([EXPORTER_OTLP])),
            ..Default::default()
        };
        assert!(init_logging_with_env(&MapEnv::default(), no_name, |b| b).is_err());
        assert!(init_logging_with_env(&MapEnv::default(), opts(None), |b| b).is_err());
    }

    #[test]
    fn init_logging_sets_console() {
        let _g = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        for (exporter, want) in [
            (Exporters::of([EXPORTER_CONSOLE]), true),
            (Exporters::none(), false),
        ] {
            init_logging_with_env(&MapEnv::default(), opts(Some(exporter.clone())), |b| b).unwrap();
            assert_eq!(console_enabled(), want, "{exporter}");
        }
        set_console(false);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn init_logging_otlp_and_console() {
        let g = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        let proc = CaptureProcessor::default();
        let p = proc.clone();
        let lp = init_logging_with_env(
            &MapEnv::default(),
            LoggingOptions {
                service_name: "test".into(),
                exporter: Some(Exporters::of([EXPORTER_OTLP, EXPORTER_CONSOLE])),
                exporter_channel: Some(lazy_channel()),
                relay_capable: true,
            },
            move |b| b.with_log_processor(p),
        )
        .unwrap()
        .expect("otlp,console returned no provider");
        assert!(logger_provider().is_some());
        assert!(console_enabled());
        let attrs = proc.0.lock().unwrap().clone().unwrap();
        assert_eq!(attrs[OTLP_RELAY_KEY], "relay");
        assert_eq!(attrs["service.name"], "test");
        set_console(false);
        *PROVIDER.write().unwrap() = None;
        drop(g);
        shut(lp).await;
    }

    #[test]
    fn init_logging_console_alone() {
        let _g = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        let lp = init_logging_with_env(
            &MapEnv::default(),
            opts(Some(Exporters::of([EXPORTER_CONSOLE]))),
            |b| b,
        )
        .unwrap();
        assert!(lp.is_none());
        set_console(false);
    }

    #[test]
    fn init_logging_relay_capable_without_destination_returns_none() {
        let env = MapEnv::new([(OTLP_ENDPOINT_ENV, "")]);
        let lp = init_logging_with_env(
            &env,
            LoggingOptions {
                service_name: "test-logs-nowhere".into(),
                exporter: Some(Exporters::of([EXPORTER_OTLP])),
                relay_capable: true,
                ..Default::default()
            },
            |b| b,
        )
        .unwrap();
        assert!(lp.is_none());
    }
}

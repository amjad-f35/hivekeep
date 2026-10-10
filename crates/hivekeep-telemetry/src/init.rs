//! One-call process telemetry setup for Hivekeep binaries.
//!
//! Every binary calls [`init`] once at startup. It installs the global log
//! subscriber (JSON lines by default, or human-readable text, at a dynamic
//! minimum level), a tracer provider whose spans feed trace correlation in
//! the log lines, a meter provider, and the optional OTLP log export.
//!
//! OTLP export is off unless configured: an endpoint (`--otlp-endpoint`,
//! `HK_TELEMETRY_OTLP_ENDPOINT` or the standard `OTEL_EXPORTER_OTLP_*ENDPOINT`
//! variables), a node relay socket that exists, or an explicit
//! `OTEL_*_EXPORTER` selection turns it on. Without any of them a binary
//! exports nothing and only its scraped `/metrics` endpoint, if it has one,
//! carries metrics.

use std::collections::HashMap;
use std::path::PathBuf;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tonic::transport::Channel;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

use crate::boot::bridge::init_metrics_bridged_with_env;
use crate::boot::logger::{dynamic_level_filter, set_log_level};
use crate::boot::logs::{init_logging_with_env, LoggingOptions};
use crate::boot::metrics::{init_metrics_push_only_with_env, init_metrics_with_env};
use crate::boot::otlp::{
    resolve_logs_exporter, LOGS_ENDPOINT_ENV, LOGS_EXPORTER_ENV, METRICS_ENDPOINT_ENV,
    METRICS_EXPORTER_ENV, OTLP_ENDPOINT_ENV, TRACES_ENDPOINT_ENV, TRACES_EXPORTER_ENV,
};
use crate::boot::sampling::{
    parent_ratio_sampling, resolve_trace_sampling, TraceSampling, CONTROL_PLANE_TRACE_RATIO,
};
use crate::boot::tracing::{init_tracing_with_env, TracingOptions};
use crate::boot::BootError;
use crate::contextlogging::JsonLayer;
use crate::env::{Env, ProcessEnv};

/// The Hivekeep alias of `OTEL_EXPORTER_OTLP_ENDPOINT`, read by
/// `--otlp-endpoint`.
pub const HK_OTLP_ENDPOINT_ENV: &str = "HK_TELEMETRY_OTLP_ENDPOINT";
/// The environment behind `--log-level`.
pub const HK_LOG_LEVEL_ENV: &str = "HK_LOG_LEVEL";
/// The environment behind `--log-format`.
pub const HK_LOG_FORMAT_ENV: &str = "HK_LOG_FORMAT";
/// The standard filter directive variable. Honoured when no level is set
/// explicitly, for per-module filtering while debugging.
pub const RUST_LOG_ENV: &str = "RUST_LOG";

/// How log lines are rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum LogFormat {
    /// One JSON object per line with trace correlation and redaction; the
    /// format every server uses in a cluster.
    #[default]
    Json,
    /// Human-readable text, for running a tool in a terminal.
    Text,
}

impl LogFormat {
    /// Parses a flag or environment value (`json` or `text`, any case).
    pub fn parse(s: &str) -> Result<Self, BootError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(LogFormat::Json),
            "text" | "console" | "pretty" => Ok(LogFormat::Text),
            other => Err(BootError::InvalidLogFormat(other.to_string())),
        }
    }
}

/// Where log lines go.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogTarget {
    /// Standard output: servers, whose stdout is the container log.
    #[default]
    Stdout,
    /// Standard error: command-line tools, whose stdout is their result.
    Stderr,
}

/// The logging and export flags every binary shares. Flatten it into a
/// clap parser and hand it to [`InitOptions::from_args`].
#[derive(Debug, Clone, Default, clap::Args)]
pub struct TelemetryArgs {
    /// Minimum log level: error, warn, info or debug, or a filter
    /// directive such as `info,hivekeep_gate=debug`. Unset uses `RUST_LOG`
    /// when that is set, else info.
    #[arg(long = "log-level", env = HK_LOG_LEVEL_ENV, global = true)]
    pub log_level: Option<String>,
    /// Log line format.
    #[arg(long = "log-format", env = HK_LOG_FORMAT_ENV, value_enum, global = true)]
    pub log_format: Option<LogFormat>,
    /// OTLP gRPC collector endpoint for traces, metrics and logs (for
    /// example `http://otel-collector:4317`). Setting it turns the OTLP
    /// export on; the standard `OTEL_EXPORTER_OTLP_*` variables also work.
    #[arg(long = "otlp-endpoint", env = HK_OTLP_ENDPOINT_ENV, global = true)]
    pub otlp_endpoint: Option<String>,
}

/// How a binary exposes its metrics.
#[derive(Debug, Clone, Default)]
pub enum MetricsMode {
    /// A Prometheus reader on the default registry, for a binary that
    /// serves `/metrics` (see [`crate::boot::health::serve_metrics`]), plus
    /// the OTLP push.
    #[default]
    PullAndPush,
    /// The OTLP push only, for a binary with no metrics server.
    PushOnly,
    /// The binary serves this registry on `/metrics`; its families are
    /// bridged onto the OTLP push (see [`crate::boot::bridge`]).
    Bridged(prometheus::Registry),
}

impl MetricsMode {
    fn pull_served(&self) -> bool {
        !matches!(self, MetricsMode::PushOnly)
    }
}

/// Configures [`init`].
#[derive(Debug, Clone, Default)]
pub struct InitOptions {
    /// Required; the resource's `service.name`.
    pub service_name: String,
    /// The component's default sampling, overridable through the
    /// `OTEL_TRACES_SAMPLER*` variables. Unset uses a parent-based 10%
    /// ratio.
    pub default_sampling: TraceSampling,
    /// The minimum log level flag value; empty uses `RUST_LOG` when set,
    /// else `info`.
    pub log_level: String,
    /// The log format; `None` reads `HK_LOG_FORMAT`, else
    /// `default_log_format`.
    pub log_format: Option<LogFormat>,
    /// The format used when neither the flag nor `HK_LOG_FORMAT` picks one.
    pub default_log_format: LogFormat,
    /// Where log lines go.
    pub log_target: LogTarget,
    /// The OTLP endpoint flag value; `None` reads `HK_TELEMETRY_OTLP_ENDPOINT`.
    pub otlp_endpoint: Option<String>,
    /// How metrics are exposed.
    pub metrics: MetricsMode,
    /// The node OTLP relay socket to export through. Setting it marks the
    /// component relay-capable; an absent socket falls back to the
    /// configured endpoint.
    pub relay_socket: Option<PathBuf>,
}

impl InitOptions {
    /// Options for `service_name` from the shared flags.
    pub fn from_args(service_name: impl Into<String>, args: &TelemetryArgs) -> Self {
        InitOptions {
            service_name: service_name.into(),
            log_level: args.log_level.clone().unwrap_or_default(),
            log_format: args.log_format,
            otlp_endpoint: args.otlp_endpoint.clone(),
            ..Default::default()
        }
    }

    /// Options for a command-line tool: text lines on stderr (its stdout
    /// is its output) unless asked otherwise, and no metrics endpoint.
    pub fn cli(service_name: impl Into<String>, args: &TelemetryArgs) -> Self {
        let mut opts = Self::from_args(service_name, args)
            .stderr()
            .metrics(MetricsMode::PushOnly);
        opts.default_log_format = LogFormat::Text;
        opts
    }

    /// Sends log lines to stderr, for command-line tools.
    pub fn stderr(mut self) -> Self {
        self.log_target = LogTarget::Stderr;
        self
    }

    /// Sets the metrics mode.
    pub fn metrics(mut self, mode: MetricsMode) -> Self {
        self.metrics = mode;
        self
    }

    /// Sets the relay socket.
    pub fn relay_socket(mut self, sock: impl Into<PathBuf>) -> Self {
        self.relay_socket = Some(sock.into());
        self
    }
}

/// The providers [`init`] installed. Call [`Telemetry::shutdown`] on the way
/// out of `main` so queued telemetry is flushed.
pub struct Telemetry {
    /// The global tracer provider.
    pub tracer_provider: SdkTracerProvider,
    /// The global meter provider.
    pub meter_provider: SdkMeterProvider,
    /// The logger provider for structured records, when OTLP logs are on.
    pub logger_provider: Option<SdkLoggerProvider>,
    /// The relay channel, when one was dialed.
    pub relay_channel: Option<Channel>,
    /// Whether the scraped `/metrics` endpoint should be served (always
    /// true except for [`MetricsMode::Bridged`] with an exporter selection
    /// that leaves Prometheus out).
    pub serve_pull: bool,
}

impl Telemetry {
    /// Flushes and shuts every provider down, logging failures. Blocks
    /// while the exporters flush; call it from `spawn_blocking` or after
    /// the runtime's work is done.
    pub fn shutdown(self) {
        crate::boot::shutdown_provider("TracerProvider", || self.tracer_provider.shutdown());
        crate::boot::shutdown_provider("MeterProvider", || self.meter_provider.shutdown());
        if let Some(lp) = self.logger_provider {
            crate::boot::shutdown_provider("LoggerProvider", || lp.shutdown());
        }
    }

    /// [`Telemetry::shutdown`] off the async worker threads, for the end of
    /// an async `main`.
    pub async fn shutdown_async(self) {
        let _ = tokio::task::spawn_blocking(move || self.shutdown()).await;
    }
}

/// The process environment with values that take precedence over it.
struct Overlay<'a> {
    base: &'a dyn Env,
    over: HashMap<String, String>,
}

impl Env for Overlay<'_> {
    fn lookup(&self, key: &str) -> Option<String> {
        self.over
            .get(key)
            .cloned()
            .or_else(|| self.base.lookup(key))
    }
}

/// Whether anything configures an OTLP destination.
fn export_configured(env: &dyn Env, relay_socket: Option<&PathBuf>) -> bool {
    let endpoint = [
        OTLP_ENDPOINT_ENV,
        TRACES_ENDPOINT_ENV,
        METRICS_ENDPOINT_ENV,
        LOGS_ENDPOINT_ENV,
    ]
    .iter()
    .any(|k| !env.trimmed(k).is_empty());
    endpoint || relay_socket.is_some_and(|s| s.exists())
}

/// Applies the endpoint flag and the default-off rule on top of `base`.
fn effective_env<'a>(base: &'a dyn Env, opts: &InitOptions) -> Overlay<'a> {
    let mut over = HashMap::new();
    let flag = opts
        .otlp_endpoint
        .clone()
        .or_else(|| base.lookup(HK_OTLP_ENDPOINT_ENV))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    if let Some(endpoint) = flag {
        over.insert(OTLP_ENDPOINT_ENV.to_string(), endpoint);
    }
    let env = Overlay { base, over };
    // The exporters run on Tokio; a synchronous binary (a CLI tool) keeps
    // export off whatever the environment says.
    let in_runtime = tokio::runtime::Handle::try_current().is_ok();
    if in_runtime && export_configured(&env, opts.relay_socket.as_ref()) {
        return env;
    }
    let mut over = env.over;
    let unset = |k: &str| !in_runtime || base.lookup(k).is_none_or(|v| v.trim().is_empty());
    if unset(TRACES_EXPORTER_ENV) {
        over.insert(TRACES_EXPORTER_ENV.into(), "none".into());
    }
    if unset(METRICS_EXPORTER_ENV) {
        let v = if opts.metrics.pull_served() {
            "prometheus"
        } else {
            "none"
        };
        over.insert(METRICS_EXPORTER_ENV.into(), v.into());
    }
    if unset(LOGS_EXPORTER_ENV) {
        over.insert(LOGS_EXPORTER_ENV.into(), "none".into());
    }
    Overlay { base, over }
}

type BoxedLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;

fn log_layer_to<W>(writer: W, format: LogFormat, env_filter: Option<EnvFilter>) -> BoxedLayer
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    match (format, env_filter) {
        (LogFormat::Json, None) => JsonLayer::new(writer)
            .with_filter(dynamic_level_filter::<Registry>())
            .boxed(),
        (LogFormat::Json, Some(f)) => JsonLayer::new(writer).with_filter(f).boxed(),
        (LogFormat::Text, None) => tracing_subscriber::fmt::layer()
            .with_writer(writer)
            .with_filter(dynamic_level_filter::<Registry>())
            .boxed(),
        (LogFormat::Text, Some(f)) => tracing_subscriber::fmt::layer()
            .with_writer(writer)
            .with_filter(f)
            .boxed(),
    }
}

/// The log layer `opts` asks for, reading `HK_LOG_FORMAT` and `RUST_LOG`
/// from `env`.
fn log_layer(env: &dyn Env, opts: &InitOptions) -> Result<BoxedLayer, BootError> {
    let format = match opts.log_format {
        Some(f) => f,
        None => match env
            .lookup(HK_LOG_FORMAT_ENV)
            .filter(|v| !v.trim().is_empty())
        {
            Some(v) => LogFormat::parse(&v)?,
            None => opts.default_log_format,
        },
    };
    let level = if opts.log_level.is_empty() {
        env.trimmed(HK_LOG_LEVEL_ENV)
    } else {
        opts.log_level.clone()
    };
    let env_filter = if level.is_empty() {
        match env.lookup(RUST_LOG_ENV).filter(|v| !v.trim().is_empty()) {
            Some(directives) => Some(EnvFilter::try_new(&directives).map_err(|e| {
                BootError::InvalidLogLevel(format!("{RUST_LOG_ENV}={directives}: {e}"))
            })?),
            None => None,
        }
    } else if level.contains('=') || level.contains(',') {
        // A filter directive such as `info,hivekeep_gate=debug`.
        Some(
            EnvFilter::try_new(&level)
                .map_err(|e| BootError::InvalidLogLevel(format!("{level}: {e}")))?,
        )
    } else {
        set_log_level(&level)?;
        None
    };
    Ok(match opts.log_target {
        LogTarget::Stdout => log_layer_to(std::io::stdout, format, env_filter),
        LogTarget::Stderr => log_layer_to(std::io::stderr, format, env_filter),
    })
}

/// Sets up logging, tracing, metrics and structured-record export for a
/// binary from the process environment, and installs the global log
/// subscriber with trace correlation. Call once, inside a Tokio runtime
/// whenever OTLP export may be on.
pub fn init(opts: InitOptions) -> Result<Telemetry, BootError> {
    init_with_env(&ProcessEnv, opts)
}

/// [`init`] reading configuration from `env`.
pub fn init_with_env(base: &dyn Env, opts: InitOptions) -> Result<Telemetry, BootError> {
    if opts.service_name.is_empty() {
        return Err(BootError::Missing("InitOptions.service_name"));
    }
    let log = log_layer(base, &opts)?;
    let env = effective_env(base, &opts);
    let env: &dyn Env = &env;
    let relay_capable = opts.relay_socket.is_some();
    #[cfg(unix)]
    let relay_channel = match &opts.relay_socket {
        Some(sock) => crate::otlprelay::dial(sock).unwrap_or_else(|e| {
            tracing::warn!(socket = %sock.display(), err = %e, "OTLP relay unusable, exporting directly");
            None
        }),
        None => None,
    };
    #[cfg(not(unix))]
    let relay_channel: Option<Channel> = None;

    let default_sampling = if opts.default_sampling.sampler().is_some() {
        opts.default_sampling.clone()
    } else {
        parent_ratio_sampling(CONTROL_PLANE_TRACE_RATIO)
    };
    let sampling = resolve_trace_sampling(env, default_sampling);
    let tracer_provider = init_tracing_with_env(
        env,
        TracingOptions {
            service_name: opts.service_name.clone(),
            sampling,
            exporter_channel: relay_channel.clone(),
            relay_capable,
        },
    )?;
    let (meter_provider, serve_pull) = match &opts.metrics {
        MetricsMode::PullAndPush => (
            init_metrics_with_env(
                env,
                &opts.service_name,
                prometheus::default_registry().clone(),
            )?,
            true,
        ),
        MetricsMode::PushOnly => (
            init_metrics_push_only_with_env(env, &opts.service_name, relay_channel.clone())?,
            false,
        ),
        MetricsMode::Bridged(reg) => {
            let b = init_metrics_bridged_with_env(env, &opts.service_name, reg.clone(), None)?;
            (b.provider, b.serve_pull)
        }
    };
    let logger_provider = init_logging_with_env(
        env,
        LoggingOptions {
            service_name: opts.service_name.clone(),
            exporter: Some(resolve_logs_exporter(env)),
            exporter_channel: relay_channel.clone(),
            relay_capable,
        },
        |b| b,
    )?;

    let tracer = tracer_provider.tracer(opts.service_name.clone());
    tracing_subscriber::registry()
        .with(log)
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()
        .map_err(|e| BootError::Subscriber(e.to_string()))?;

    Ok(Telemetry {
        tracer_provider,
        meter_provider,
        logger_provider,
        relay_channel,
        serve_pull,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boot::sampling::parent_never_sampling;
    use crate::env::MapEnv;
    use clap::Parser;

    #[test]
    fn init_requires_a_service_name() {
        let _g = crate::boot::logs::tests::GLOBAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        assert!(init_with_env(&MapEnv::default(), InitOptions::default()).is_err());
    }

    #[test]
    fn init_rejects_a_bad_log_level() {
        let _g = crate::boot::logs::tests::GLOBAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let opts = InitOptions {
            service_name: "x".into(),
            default_sampling: parent_never_sampling(),
            log_level: "verbose".into(),
            ..Default::default()
        };
        assert!(matches!(
            init_with_env(&MapEnv::default(), opts),
            Err(BootError::InvalidLogLevel(_))
        ));
    }

    #[test]
    fn log_level_accepts_filter_directives() {
        let opts = InitOptions {
            service_name: "x".into(),
            log_level: "info,hivekeep_gate=debug".into(),
            ..Default::default()
        };
        assert!(log_layer(&MapEnv::default(), &opts).is_ok());
        let bad = InitOptions {
            log_level: "info,=[".into(),
            ..opts
        };
        assert!(matches!(
            log_layer(&MapEnv::default(), &bad),
            Err(BootError::InvalidLogLevel(_))
        ));
    }

    #[test]
    fn init_rejects_a_bad_log_format() {
        let _g = crate::boot::logs::tests::GLOBAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let opts = InitOptions {
            service_name: "x".into(),
            ..Default::default()
        };
        let env = MapEnv::new([(HK_LOG_FORMAT_ENV, "xml")]);
        assert!(matches!(
            init_with_env(&env, opts),
            Err(BootError::InvalidLogFormat(_))
        ));
    }

    #[tokio::test]
    async fn export_is_off_unless_configured() {
        let opts = InitOptions {
            service_name: "x".into(),
            ..Default::default()
        };
        let base = MapEnv::default();
        let env = effective_env(&base, &opts);
        assert_eq!(env.get(TRACES_EXPORTER_ENV), "none");
        assert_eq!(env.get(METRICS_EXPORTER_ENV), "prometheus");
        assert_eq!(env.get(LOGS_EXPORTER_ENV), "none");

        let push_only = opts.clone().metrics(MetricsMode::PushOnly);
        assert_eq!(
            effective_env(&base, &push_only).get(METRICS_EXPORTER_ENV),
            "none"
        );

        // An explicit selection is kept.
        let base = MapEnv::new([(TRACES_EXPORTER_ENV, "otlp")]);
        assert_eq!(effective_env(&base, &opts).get(TRACES_EXPORTER_ENV), "otlp");

        // An endpoint from the environment turns every signal on.
        let base = MapEnv::new([(METRICS_ENDPOINT_ENV, "http://c:4317")]);
        let env = effective_env(&base, &opts);
        assert_eq!(env.lookup(TRACES_EXPORTER_ENV), None);
        assert_eq!(env.lookup(METRICS_EXPORTER_ENV), None);

        // So does the flag, which becomes the generic endpoint.
        let flagged = InitOptions {
            otlp_endpoint: Some("http://flag:4317".into()),
            ..opts.clone()
        };
        let base = MapEnv::default();
        let env = effective_env(&base, &flagged);
        assert_eq!(env.get(OTLP_ENDPOINT_ENV), "http://flag:4317");
        assert_eq!(env.lookup(TRACES_EXPORTER_ENV), None);

        // And the Hivekeep alias variable.
        let base = MapEnv::new([(HK_OTLP_ENDPOINT_ENV, "http://alias:4317")]);
        assert_eq!(
            effective_env(&base, &opts).get(OTLP_ENDPOINT_ENV),
            "http://alias:4317"
        );

        // A relay socket counts only when it exists.
        let dir = tempfile::tempdir().unwrap();
        let missing = opts.clone().relay_socket(dir.path().join("absent.sock"));
        let base = MapEnv::default();
        assert_eq!(
            effective_env(&base, &missing).get(TRACES_EXPORTER_ENV),
            "none"
        );
        let present = opts.clone().relay_socket(dir.path());
        assert_eq!(
            effective_env(&base, &present).lookup(TRACES_EXPORTER_ENV),
            None
        );
    }

    #[test]
    fn export_stays_off_outside_a_runtime() {
        let opts = InitOptions {
            service_name: "x".into(),
            ..Default::default()
        };
        let base = MapEnv::new([
            (OTLP_ENDPOINT_ENV, "http://c:4317"),
            (TRACES_EXPORTER_ENV, "otlp"),
        ]);
        let env = effective_env(&base, &opts);
        assert_eq!(env.get(TRACES_EXPORTER_ENV), "none");
        assert_eq!(env.get(METRICS_EXPORTER_ENV), "prometheus");
    }

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        telemetry: TelemetryArgs,
    }

    #[test]
    fn telemetry_args_parse() {
        let cli = Cli::try_parse_from([
            "bin",
            "--log-level=debug",
            "--log-format=text",
            "--otlp-endpoint",
            "http://c:4317",
        ])
        .unwrap();
        let opts = InitOptions::from_args("svc", &cli.telemetry).stderr();
        assert_eq!(opts.log_level, "debug");
        assert_eq!(opts.log_format, Some(LogFormat::Text));
        assert_eq!(opts.otlp_endpoint.as_deref(), Some("http://c:4317"));
        assert_eq!(opts.log_target, LogTarget::Stderr);
        assert!(Cli::try_parse_from(["bin", "--log-format=xml"]).is_err());
        assert_eq!(LogFormat::parse(" JSON ").unwrap(), LogFormat::Json);
        let cli_opts = InitOptions::cli("tool", &TelemetryArgs::default());
        assert_eq!(cli_opts.default_log_format, LogFormat::Text);
        assert_eq!(cli_opts.log_target, LogTarget::Stderr);
        assert!(matches!(cli_opts.metrics, MetricsMode::PushOnly));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn init_installs_providers_once() {
        let opts = InitOptions {
            service_name: "hivekeep-test".into(),
            default_sampling: parent_never_sampling(),
            metrics: MetricsMode::PushOnly,
            ..Default::default()
        };
        let env = MapEnv::default();
        // init touches the process-wide logging state the logs tests check.
        let g = crate::boot::logs::tests::GLOBAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let t = init_with_env(&env, opts.clone()).expect("first init");
        assert!(t.logger_provider.is_none(), "logs export is off by default");
        assert!(t.relay_channel.is_none());
        assert!(!t.serve_pull);
        // The global subscriber can be installed only once.
        assert!(matches!(
            init_with_env(&env, opts),
            Err(BootError::Subscriber(_))
        ));
        drop(g);
        tokio::task::spawn_blocking(move || t.shutdown())
            .await
            .unwrap();
    }
}

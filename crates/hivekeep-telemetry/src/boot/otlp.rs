//! Where and whether a signal is exported over OTLP.

use tracing::Level;

use super::exporters::{
    resolve_exporters_env, Exporters, EXPORTER_CONSOLE, EXPORTER_OTLP, EXPORTER_PROMETHEUS,
};
use crate::env::Env;

/// Selects the metrics exporters.
pub const METRICS_EXPORTER_ENV: &str = "OTEL_METRICS_EXPORTER";
/// Selects the trace exporters.
pub const TRACES_EXPORTER_ENV: &str = "OTEL_TRACES_EXPORTER";
/// Selects the log exporters.
pub const LOGS_EXPORTER_ENV: &str = "OTEL_LOGS_EXPORTER";

/// The collector every signal exports to unless overridden.
pub const OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
/// The traces collector.
pub const TRACES_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
/// The metrics collector.
pub const METRICS_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";
/// The logs collector.
pub const LOGS_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT";

/// The collector address used when no endpoint is configured.
pub const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4317";

/// Why a signal is not exported: OTLP is not selected.
pub const REASON_OTLP_NOT_SELECTED: &str = "otlp exporter not selected";
/// Why a signal is not exported: it has nowhere to go.
pub const REASON_NO_DESTINATION: &str = "no OTLP relay or endpoint";

/// Why a signal does not export over OTLP (`None` when it does), and the
/// level to log that at: `WARN` when OTLP is selected but has nowhere to
/// go, since the component then loses that signal.
pub fn otlp_export_blocked(selected: bool, has_destination: bool) -> (Option<&'static str>, Level) {
    match (selected, has_destination) {
        (false, _) => (Some(REASON_OTLP_NOT_SELECTED), Level::INFO),
        (true, false) => (Some(REASON_NO_DESTINATION), Level::WARN),
        (true, true) => (None, Level::INFO),
    }
}

/// Whether a component has a collector to export to. A relay-capable
/// component (a sandbox worker) without a relay connection needs an OTLP
/// endpoint, the generic one or `signal_endpoint_env`: a worker pod runs no
/// collector beside it, so the `localhost:4317` default reaches nothing.
/// Every other component keeps that default.
pub fn has_otlp_destination(
    env: &dyn Env,
    signal_endpoint_env: &str,
    relay_capable: bool,
    has_relay: bool,
) -> bool {
    if !relay_capable || has_relay {
        return true;
    }
    !env.trimmed(OTLP_ENDPOINT_ENV).is_empty() || !env.trimmed(signal_endpoint_env).is_empty()
}

/// The collector URI for a signal: the signal-specific endpoint, else the
/// generic one, else [`DEFAULT_OTLP_ENDPOINT`]. The export is plaintext, as
/// the managed collectors it targets do not present certificates the pod
/// can validate, so an `https` scheme is read as the address only.
pub fn otlp_endpoint(env: &dyn Env, signal_endpoint_env: &str) -> String {
    let mut raw = env.trimmed(signal_endpoint_env);
    if raw.is_empty() {
        raw = env.trimmed(OTLP_ENDPOINT_ENV);
    }
    if raw.is_empty() {
        return DEFAULT_OTLP_ENDPOINT.to_string();
    }
    let rest = raw
        .strip_prefix("http://")
        .or_else(|| raw.strip_prefix("https://"))
        .unwrap_or(&raw);
    format!("http://{rest}")
}

/// Applies `OTEL_METRICS_EXPORTER`. `prometheus` is known only when
/// `pull_served`, i.e. the component serves its own `/metrics`; elsewhere
/// it is skipped as unknown, so a push-only component keeps exporting.
/// Unset selects every exporter the component has.
pub fn metrics_exporters(env: &dyn Env, pull_served: bool) -> Exporters {
    let mut known = vec![EXPORTER_OTLP];
    let mut def = vec![EXPORTER_OTLP];
    if pull_served {
        known.push(EXPORTER_PROMETHEUS);
        def.push(EXPORTER_PROMETHEUS);
    }
    let exporters = resolve_exporters_env(env, METRICS_EXPORTER_ENV, &known, &Exporters::of(def));
    tracing::info!(exporters = %exporters, "Metrics exporters resolved");
    exporters
}

/// Whether the OTLP metric reader is installed: while `otlp` is selected,
/// as it is when the variable is unset.
pub fn metrics_push_enabled(env: &dyn Env, pull_served: bool) -> bool {
    metrics_exporters(env, pull_served).has(EXPORTER_OTLP)
}

/// Applies `OTEL_TRACES_EXPORTER`: the OTLP span exporter is installed
/// while `otlp`, the default, is selected.
pub fn traces_push_enabled(env: &dyn Env) -> bool {
    resolve_exporters_env(
        env,
        TRACES_EXPORTER_ENV,
        &[EXPORTER_OTLP],
        &Exporters::of([EXPORTER_OTLP]),
    )
    .has(EXPORTER_OTLP)
}

/// Reads `OTEL_LOGS_EXPORTER`. The default is `none` rather than the spec's
/// `otlp`, so turning it on is one setting per environment. `console` is
/// the component's own JSON log line.
pub fn resolve_logs_exporter(env: &dyn Env) -> Exporters {
    resolve_exporters_env(
        env,
        LOGS_EXPORTER_ENV,
        &[EXPORTER_OTLP, EXPORTER_CONSOLE],
        &Exporters::none(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contextlogging::tests::capture;
    use crate::env::MapEnv;

    fn warned(out: &str) -> bool {
        out.contains("\"level\":\"WARN\"")
    }

    #[test]
    fn metrics_push_enabled_table() {
        // (value, (push, warn) when pull is served, (push, warn) push-only)
        let cases = [
            ("", (true, false), (true, false)),
            ("otlp", (true, false), (true, false)),
            (" OTLP ", (true, false), (true, false)),
            ("none", (false, false), (false, false)),
            (" None ", (false, false), (false, false)),
            ("prometheus", (false, false), (true, true)),
            (" Prometheus ", (false, false), (true, true)),
            ("console", (true, true), (true, true)),
            ("otlp,prometheus", (true, false), (true, true)),
            ("prometheus, otlp", (true, false), (true, true)),
            ("prometheus,console", (false, true), (true, true)),
            ("none,otlp", (true, true), (true, true)),
            ("none,prometheus", (true, true), (false, true)),
        ];
        for (value, pull, push_only) in cases {
            for (pull_served, (want_push, want_warn)) in [(true, pull), (false, push_only)] {
                let env = MapEnv::new([(METRICS_EXPORTER_ENV, value)]);
                let mut push = false;
                let out = capture(|| push = metrics_push_enabled(&env, pull_served));
                assert_eq!(push, want_push, "{value:?} pull_served={pull_served}");
                assert_eq!(
                    warned(&out),
                    want_warn,
                    "{value:?} pull_served={pull_served}: {out}"
                );
            }
        }
    }

    #[test]
    fn traces_push_enabled_table() {
        for (value, want_push, want_warn) in [
            ("", true, false),
            ("otlp", true, false),
            (" OTLP ", true, false),
            ("none", false, false),
            (" None ", false, false),
            ("console", true, true),
            ("otlp,console", true, true),
            ("none,otlp", true, true),
        ] {
            let env = MapEnv::new([(TRACES_EXPORTER_ENV, value)]);
            let mut push = false;
            let out = capture(|| push = traces_push_enabled(&env));
            assert_eq!(push, want_push, "{value:?}");
            assert_eq!(warned(&out), want_warn, "{value:?}: {out}");
        }
    }

    #[test]
    fn has_otlp_destination_table() {
        for (name, capable, relay, generic, signal, want) in [
            ("not relay capable", false, false, "", "", true),
            ("relay connection", true, true, "", "", true),
            (
                "generic endpoint",
                true,
                false,
                "http://collector:4317",
                "",
                true,
            ),
            (
                "signal endpoint",
                true,
                false,
                "",
                "http://collector:4317",
                true,
            ),
            ("neither", true, false, "", "", false),
            ("blank endpoints", true, false, " ", " ", false),
        ] {
            let env = MapEnv::new([(OTLP_ENDPOINT_ENV, generic), (TRACES_ENDPOINT_ENV, signal)]);
            assert_eq!(
                has_otlp_destination(&env, TRACES_ENDPOINT_ENV, capable, relay),
                want,
                "{name}"
            );
        }
    }

    #[test]
    fn resolve_logs_exporter_table() {
        for (value, want) in [
            ("", "none"),
            ("otlp", "otlp"),
            ("otlp,console", "console,otlp"),
            ("console", "console"),
            ("prometheus", "none"),
            ("otlp,prometheus", "otlp"),
        ] {
            let env = MapEnv::new([(LOGS_EXPORTER_ENV, value)]);
            assert_eq!(resolve_logs_exporter(&env).to_string(), want, "{value:?}");
        }
    }

    #[test]
    fn otlp_endpoint_resolution() {
        assert_eq!(
            otlp_endpoint(&MapEnv::default(), TRACES_ENDPOINT_ENV),
            DEFAULT_OTLP_ENDPOINT
        );
        let env = MapEnv::new([
            (OTLP_ENDPOINT_ENV, "collector:4317"),
            (METRICS_ENDPOINT_ENV, "https://m:4317"),
        ]);
        assert_eq!(
            otlp_endpoint(&env, TRACES_ENDPOINT_ENV),
            "http://collector:4317"
        );
        assert_eq!(otlp_endpoint(&env, METRICS_ENDPOINT_ENV), "http://m:4317");
    }
}

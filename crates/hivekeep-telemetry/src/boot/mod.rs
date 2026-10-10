//! Startup shared by the long-running Hivekeep servers: the JSON logger,
//! OpenTelemetry tracer, meter and logger providers, the Prometheus and
//! probe endpoints, and fail-fast helpers.

pub mod bridge;
pub mod error;
pub mod exporters;
pub mod health;
pub mod logger;
pub mod logs;
pub mod metrics;
pub mod otlp;
pub mod resource;
pub mod sampling;
pub mod tracing;

pub use error::BootError;

/// Logs `msg` with `err` and exits with status 1. For startup-time
/// fail-fast where there is no recovery path.
pub fn fatal(msg: &str, err: &dyn std::fmt::Display) -> ! {
    ::tracing::error!(err = %err, "{msg}");
    std::process::exit(1)
}

/// Runs a provider's shutdown and logs a failure. Call on the way out of
/// `main`.
pub fn shutdown_provider<E: std::fmt::Display>(
    name: &str,
    shutdown: impl FnOnce() -> Result<(), E>,
) {
    if let Err(err) = shutdown() {
        ::tracing::error!(err = %err, "Failed to shutdown {name}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_provider_logs_failures() {
        let out = crate::contextlogging::tests::capture(|| {
            shutdown_provider("TracerProvider", || Err::<(), _>("boom"));
            shutdown_provider("MeterProvider", || Ok::<(), String>(()));
        });
        assert!(
            out.contains("Failed to shutdown TracerProvider") && out.contains("boom"),
            "{out}"
        );
        assert!(!out.contains("MeterProvider"));
    }
}

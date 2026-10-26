//! A fake data plane for benchmarking the control plane in isolation.
//!
//! Two programs stand in for the node agent and the worker fleet:
//!
//! - [`fake_keeper`] runs where the keeper runs. It answers every keeper
//!   call with success after a configurable delay, without running or
//!   saving any workload, and relays fake Workers' capacity reports to
//!   hivekeep-api under its own identity.
//! - [`fake_workersync`] stands in for the WorkerPool controller and the
//!   worker registry sync: it keeps each pool's `spec.replicas` fake Workers
//!   registered, with no pod behind any of them, and reports their capacity
//!   through the fake keeper on their node.
//!
//! Actors the fake data plane "runs" do not exist. Never run it on a
//! cluster that serves real Actors.

pub mod fake_keeper;
pub mod fake_workersync;
pub mod fakeworker;
pub mod health;
pub mod quantity;

/// Starts process telemetry for `service` with JSON logging at `level`
/// (`debug`, `info`, `warn` or `error`). A second call keeps the first
/// subscriber and returns `None`.
pub fn init_logging(
    service: &str,
    level: &str,
) -> Result<Option<hivekeep_telemetry::Telemetry>, String> {
    let level = match level.to_ascii_lowercase().as_str() {
        l @ ("debug" | "info" | "warn" | "error") => l.to_string(),
        other => {
            return Err(format!(
                "unknown log level {other:?}: want debug, info, warn or error"
            ))
        }
    };
    match hivekeep_telemetry::init(hivekeep_telemetry::InitOptions {
        service_name: service.to_string(),
        log_level: level,
        metrics: hivekeep_telemetry::MetricsMode::PushOnly,
        ..Default::default()
    }) {
        Ok(t) => Ok(Some(t)),
        Err(hivekeep_telemetry::boot::BootError::Subscriber(_)) => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

/// Strips a gRPC `dns:///` scheme from a dial target, leaving `host:port`.
pub fn host_port(target: &str) -> &str {
    target.strip_prefix("dns:///").unwrap_or(target)
}

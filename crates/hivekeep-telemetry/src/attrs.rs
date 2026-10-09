//! Attribute keys and log field names shared by the telemetry modules.

/// Log field carrying the active trace ID, named as the OpenTelemetry spec
/// fixes it for non-OTLP log formats so a collector can lift it onto the
/// record's own trace fields.
pub const LOG_TRACE_ID_FIELD: &str = "trace_id";
/// Log field carrying the active span ID.
pub const LOG_SPAN_ID_FIELD: &str = "span_id";
/// Log field carrying the active trace flags, two hex digits.
pub const LOG_TRACE_FLAGS_FIELD: &str = "trace_flags";

/// Resource attribute describing the export path a relay-capable component
/// took: `relay` through the node OTLP relay, `direct` when it fell back.
pub const OTLP_RELAY_KEY: &str = "hk.otlp.relay";

/// `service.name`, the resource attribute that names a component.
pub const SERVICE_NAME_KEY: &str = "service.name";
/// `service.instance.id`, unique per process.
pub const SERVICE_INSTANCE_ID_KEY: &str = "service.instance.id";
/// `server.address`, dropped from RPC metrics (see `boot::metrics`).
pub const SERVER_ADDRESS_KEY: &str = "server.address";

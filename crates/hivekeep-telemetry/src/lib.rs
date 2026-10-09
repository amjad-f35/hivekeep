//! Hivekeep process telemetry: build identity, JSON logging with trace
//! correlation and protobuf redaction, OpenTelemetry setup, health and
//! metrics endpoints, gRPC handler wrappers, and the node OTLP relay.

pub mod attrs;
pub mod contextlogging;
pub mod env;
pub mod interceptors;
pub mod protoredact;
#[cfg(test)]
pub(crate) mod testutil;
pub mod version;

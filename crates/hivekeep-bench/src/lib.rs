//! Load and benchmark tooling for Hivekeep.
//!
//! - [`glutton`]: the glutton workload, a sandboxed server that consumes RAM,
//!   disk, CPU and file descriptors on request, over gRPC or
//!   protobuf-over-HTTP.
//! - [`loadgen`]: a locust-protocol load generator whose user classes drive
//!   Actors through their lifecycle and traffic through the gate.
//! - [`egress`]: the egress-gateway benchmark (traffic actor and target).
//! - [`isolate`]: a fake data plane (fake keeper, fake worker registry sync)
//!   for benchmarking the control plane in isolation.

pub mod boot;
pub mod pb;
pub mod wire;

pub mod api_client;

#[cfg(test)]
pub(crate) mod testutil;

pub mod glutton;

pub mod egress;

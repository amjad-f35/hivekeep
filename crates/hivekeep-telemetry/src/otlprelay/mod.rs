//! Carries sandbox workers' OTLP telemetry to the collector over a unix
//! socket served by the keeper, so a worker pod needs no network path of
//! its own to export spans, metrics and log records.
//!
//! The worker pod runs untrusted Actor code. Exporting over the pod network
//! means allowing it egress to the collector, reachable to anything that
//! escapes the sandbox; a unix socket cannot leave the node, so the pod can
//! be denied network egress entirely. It also collapses the per-worker
//! collector connections of a heavily oversubscribed node into the keeper's
//! single one, keeps the worker's own export traffic clear of the egress
//! redirect it installs for the Actor, and outlives the worker pod, so the
//! spans describing teardown still get out.

pub mod client;
pub mod config;
pub mod gate;
pub mod server;

#[cfg(unix)]
pub use client::dial;
pub use gate::WORKER_SERVICES;
pub use server::{RelayError, Server, DEFAULT_SOCKET_PATH};

#[cfg(all(test, unix))]
mod tests;

//! A locust-protocol load generator. It joins a locust master as a regular
//! worker, runs one user class's virtual users, and reports request stats
//! to the master and to a local Prometheus endpoint.

pub mod duration;
pub mod events;
pub mod locust;
pub mod trace;

pub mod dynconfig;
pub mod metrics;
pub mod telemetry;

pub mod control;
pub mod lifecycle;
pub mod userclass;

pub mod client;

#[cfg(test)]
pub(crate) mod fake_control;

pub mod classes;

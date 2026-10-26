//! The fake keeper: stands in for the keeper in control-plane benchmarks.
//!
//! It runs in the keeper's DaemonSet, so hivekeep-api dials it as the
//! node's keeper. Every keeper call succeeds after a delay without running
//! or saving any workload ([`herder`]), and fake-workersync's capacity
//! reports for the fake Workers on its node are relayed to hivekeep-api
//! ([`relay`]), as the keeper relays a worker's.

pub mod app;
pub mod config;
pub mod epochs;
pub mod herder;
pub mod relay;
pub mod storage;

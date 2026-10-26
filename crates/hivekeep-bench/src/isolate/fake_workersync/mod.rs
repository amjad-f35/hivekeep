//! fake-workersync: stands in for the WorkerPool controller and the worker
//! registry sync in control-plane benchmarks.
//!
//! It honors every WorkerPool's `spec.replicas` with fake Workers that no
//! pod backs, placed round robin on the nodes that run a fake-keeper pod,
//! and has each Worker's capacity reported through the fake keeper on its
//! node, as a worker reports through the keeper. It writes each pool's
//! status, so the scale subresource and `kubectl get workerpool` work. It
//! leaves its Workers registered when it shuts down and adopts them when it
//! starts, so a restart keeps every Actor placed; a deleted pool's Workers
//! are retired while it runs.
//!
//! The real controller must not run alongside it: its WorkerPool controller
//! would create real worker pods for the same pools, and its registry sync
//! deletes every Worker with no live pod.

pub mod app;
pub mod controller;
pub mod kube_cluster;
pub mod ports;
pub mod relay_client;
pub mod template;

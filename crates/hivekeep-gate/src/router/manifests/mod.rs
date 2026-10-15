//! Checks of the shipped install manifests against what the router and its
//! Envoy sidecars rely on. A drift on either side fails closed at runtime and
//! would otherwise only show up in a live cluster.

pub(crate) mod support;

mod egress_logs;
mod egress_policy;
mod envoy_admin;
mod router;

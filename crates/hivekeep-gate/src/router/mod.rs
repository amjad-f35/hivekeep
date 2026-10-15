//! The gate router: Envoy's control plane (xDS), external processor for
//! ingress and egress requests, and the router daemon around them.

pub mod api_dial;
pub mod config;
pub mod dataplane;
pub mod drain;
pub mod egress;
pub mod envoy_drain;
pub mod extproc;
pub mod health;
pub mod ingress;
pub mod loopback;
#[cfg(test)]
pub(crate) mod manifests;
pub mod server;
pub mod status;
pub mod xds;

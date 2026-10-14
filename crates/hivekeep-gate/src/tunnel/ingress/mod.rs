//! The sandbox side of ingress: a TLS listener on the worker that only the
//! gate router may connect to, forwarding each request (or CONNECT tunnel)
//! for an active Actor into that Actor's network namespace.
//!
//! Requests name their Actor in [`TARGET_ACTOR_HEADER`]; a request for an
//! Actor that is not active here is answered `421 Misdirected Request`
//! with [`STALE_ASSIGNMENT_HEADER`], telling the router its view of the
//! Actor's placement is stale.

pub mod relay;
pub mod server;
pub mod tls;
pub mod transport;

pub use crate::headers::TARGET_ACTOR_HEADER;
pub use server::{Config, Dial, Server};

/// The worker port the router dials for CONNECT tunnels.
pub const DEFAULT_CONNECT_PORT: u16 = 8443;

/// Response header set on `421` answers: the worker does not host the
/// Actor (any more), so the router should re-resolve its placement.
pub const STALE_ASSIGNMENT_HEADER: &str = "x-hk-assignment-stale";

/// Request header naming the Actor port a proxied request goes to,
/// overriding the upstream's default port.
pub const TARGET_PORT_HEADER: &str = "x-hk-target-port";

/// Parses a decimal port in 1..=65535.
pub fn parse_port(s: &str) -> Option<u16> {
    crate::tunnel::egress::parse_port(s)
}

#[cfg(test)]
mod tests;

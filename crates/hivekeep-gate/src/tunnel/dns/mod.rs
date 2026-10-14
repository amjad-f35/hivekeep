//! The sandbox DNS relay.
//!
//! Each Actor gets a relay on port 53 of its network namespace. A query
//! is forwarded to the worker's resolvers only when its name matches a
//! hostname pattern of the Actor's egress policy; everything else is
//! answered NXDOMAIN with a zero negative-caching TTL, so a policy change
//! takes effect on the next lookup. Zone transfers and non-standard
//! opcodes are refused, malformed queries get FORMERR, and each relay
//! bounds its UDP queries in flight and its TCP connections.
//!
//! UDP queries are re-identified for the upstream leg and matched back by
//! ID, upstream address and question; SERVFAIL, NOTIMP and REFUSED answers
//! and silent upstreams fail over to the next resolver.

pub mod evaluator;
pub mod freemap;
pub mod handler;
pub mod limiter;
pub mod pending;
pub mod policy;
pub mod protocol;
pub mod relay;
pub mod resolv_conf;
pub mod server;
pub mod tcp;
#[cfg(test)]
pub(crate) mod test_support;
pub mod udp;

use std::time::Duration;

pub use policy::{PolicyError, PolicySource};
pub use relay::Relay;
pub use server::{Server, ServerSockets};

/// The DNS port the relay serves on inside the Actor namespace.
pub const DNS_PORT: u16 = 53;
/// Budget of one query across all upstream attempts.
pub const DNS_EXCHANGE_TIMEOUT: Duration = Duration::from_secs(5);
/// Cap on an attempt that still has fallbacks left.
pub const DEFAULT_UPSTREAM_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
/// How often expired UDP attempts are swept.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_millis(50);
/// Idle timeout of a TCP connection pair.
pub const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
/// Largest UDP message relayed.
pub const MAX_DATAGRAM_SIZE: usize = 4 * 1024;

#[cfg(test)]
mod tests;

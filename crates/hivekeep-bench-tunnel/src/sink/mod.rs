//! The peers the tunnel talks to in the benchmark: the ingress actor in each
//! Actor's sandbox, behind ingress, and the fake egress gateway, behind
//! egress. Both answer with a fixed body and count what they see.

pub mod gateway;
pub mod ingress_actor;

pub use gateway::{Gateway, GatewayConfig, GatewayStats, ANSWER_PORT, DISCARD_PORT};
pub use ingress_actor::{IngressActor, IngressActorStats};

/// `n` bytes of `abc...zabc...`.
pub fn fixed_body(n: usize) -> Vec<u8> {
    (0..n).map(|i| b'a' + (i % 26) as u8).collect()
}

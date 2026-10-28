//! Envoy-independent half of the egress-policy dynamic module.
//!
//! The gate's egress Envoy splits one actor connection over two legs. The
//! outer CONNECT leg learns the actor's SNI rules from the gate's ext_proc
//! answer; the inner listener, which sees the tunnelled TLS ClientHello, has
//! to apply them. Rather than copy the rules onto every connection, the
//! outer leg interns them in a per-worker-thread store keyed by a 64-bit
//! hash and hands only the hash across (see [`store`]). The inner leg looks
//! the rules up by that hash and picks a filter chain (see [`verdict`]).
//!
//! Layering: [`keys`] (wire names) → [`policy`] (rules + hashing) →
//! [`store`] (interning) → [`verdict`] (chain selection).

pub mod keys;
pub mod policy;
pub mod store;
pub mod verdict;

pub use policy::{EgressPolicy, SniMode, SniRule};
pub use store::{Admission, PolicyStore};
pub use verdict::{connection_verdict, stored_policy_verdict, transport_verdict, Chain};

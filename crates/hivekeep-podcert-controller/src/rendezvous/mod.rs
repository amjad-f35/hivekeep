//! Work sharding across controller replicas.
//!
//! Each replica keeps a Lease alive; an item belongs to the live replica
//! with the highest rendezvous hash for it ([`hash::assign`]), so replicas
//! agree on owners without coordinating, and only the items of a replica
//! that joins or dies move.

pub mod hash;
pub mod hasher;

pub use hash::assign;
pub use hasher::LeaseHasher;

/// Decides whether this replica handles an item.
pub trait Hasher: Send + Sync {
    /// Whether `item` is assigned to this replica.
    fn assigned_to_this_replica(&self, item: &str) -> bool;
}

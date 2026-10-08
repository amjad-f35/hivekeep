//! The store contract, one trait per resource family, plus the [`Store`]
//! umbrella the control plane holds as `Arc<dyn Store>`.
//!
//! Shared conventions:
//!
//! * Creates copy their input, stamp server-owned metadata (fresh uid,
//!   version 1, create/update time) and return the stored object; the input
//!   is never modified.
//! * Updates are read-modify-writes guarded by a [`Precondition`] that must
//!   carry both uid and version. The `mutate` closure edits the stored
//!   object in place; it is written only if the closure returns `Ok`, and
//!   any error it returns comes back unchanged. Edits to server-owned
//!   metadata are discarded. `mutate` may run more than once.
//! * `mark_*_for_deletion` is an update whose guards are waivable
//!   ([`DeletePreconditions`]) and that sets `metadata.delete_time` to the
//!   write's update time if it is unset. No other update sets or clears it.
//! * Deletes return the removed object; guards are waivable.
//!
//! [`Precondition`]: crate::Precondition
//! [`DeletePreconditions`]: crate::DeletePreconditions

mod access_policy;
mod actor;
mod actor_template;
mod assignment;
mod egress_policy;
mod health;
mod hive;
mod lease;
mod store;
mod tag;
mod worker;

pub use access_policy::AccessPolicyStore;
pub use actor::ActorStore;
pub use actor_template::ActorTemplateStore;
pub use assignment::WorkerAssignmentStore;
pub use egress_policy::EgressPolicyStore;
pub use health::HealthCheck;
pub use hive::HiveStore;
pub use lease::LeaseStore;
pub use store::Store;
pub use tag::TagStore;
pub use worker::WorkerStore;

use crate::error::Result;
pub use crate::resource::Mutate;

/// An admission check run against a Worker, with the Worker locked, before an
/// Actor is bound to it.
pub type Admit<'a> = &'a (dyn Fn(&hivekeep_proto::api::v1::Worker) -> Result<()> + Send + Sync);

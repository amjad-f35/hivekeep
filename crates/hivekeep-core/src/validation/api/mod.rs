//! Validation of the public API (`hivekeep.api.v1`).
//!
//! Request validators are named `validate_<rpc>_request` and take the
//! request's [`Operation`](crate::validation::Operation) context (its preview
//! gates and opt-in); they always validate as a create. Stored resources are
//! re-validated against their previous value with the `validate_*_update`
//! functions, which enforce immutability and set-once fields.

/// Validators for the workload messages shared with the keeper API.
pub mod workload {
    use hivekeep_proto::api::v1 as pb;
    crate::validation::workload::workload_validators!();
}

pub mod access_policy;
pub mod actor;
pub mod actor_template;
pub mod common;
pub mod egress_policy;
pub mod hive;
pub mod tag;
pub mod worker;

pub use access_policy::*;
pub use actor::*;
pub use actor_template::*;
pub use egress_policy::*;
pub use hive::*;
pub use tag::*;
pub use worker::*;

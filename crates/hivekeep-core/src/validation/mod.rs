//! Request and resource validation.
//!
//! [`rules`] holds the declarative building blocks (presence, bounds,
//! formats, update rules, unions), [`checks`] the field-specific checks, and
//! [`api`] / [`keeper`] the validators for each wire API. Every validator
//! returns an [`ErrorList`](crate::field::ErrorList); call
//! [`into_result`](crate::field::ErrorList::into_result) to turn it into a
//! `Result` that converts into an `INVALID_ARGUMENT` status.

pub mod api;
pub mod checks;
pub mod keeper;
pub mod rules;
pub(crate) mod workload;

pub use rules::{Format, HiveRule, OpKind, Operation};

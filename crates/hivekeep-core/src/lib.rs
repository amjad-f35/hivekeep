//! Shared domain logic for Hivekeep: resource names and references, request
//! validation, defaults, snapshot addressing and the Actor lifecycle.
//!
//! Used by the API server, the node agent (keeper) and the CLI. Modules are
//! layered from the bottom up: formats ([`names`], [`uri`]), identities
//! ([`resource`], [`spiffe`], [`snapshot`]), domain rules ([`compute`],
//! [`actor_state`], [`preview`], ...), and finally [`validation`] and
//! [`defaults`], which operate on the wire messages of `hivekeep-proto`.

pub mod compute;
pub mod field;
pub mod names;
pub mod resource;
pub mod snapshot;
pub mod spiffe;
pub mod uri;

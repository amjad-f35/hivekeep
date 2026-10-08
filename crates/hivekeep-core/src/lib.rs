//! Shared domain logic for Hivekeep: resource names and references, request
//! validation, defaults, snapshot addressing and the Actor lifecycle.
//!
//! Used by the API server, the node agent (keeper) and the CLI. Modules are
//! layered from the bottom up: formats ([`names`], [`uri`]), identities
//! ([`resource`], [`spiffe`], [`snapshot`]), domain rules ([`compute`],
//! [`actor_state`], [`preview`], ...), and finally [`validation`] and
//! [`defaults`], which operate on the wire messages of `hivekeep-proto`.

pub mod access;
pub mod actor_event;
pub mod actor_log;
pub mod actor_state;
pub mod attr;
pub mod compute;
pub mod cpu_features;
pub mod defaults;
pub mod field;
pub mod hardware;
pub mod hostname;
pub mod image_ref;
pub mod names;
pub mod preview;
pub mod resource;
pub mod sandbox_class;
pub mod snapshot;
pub mod spiffe;
pub mod uri;
pub mod validation;
pub mod volume_path;

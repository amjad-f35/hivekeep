//! The object-store plugin API on GCS or S3, served on a Unix socket: the
//! node plugin (snapshot transfers and sandbox assets) next to the keeper,
//! and the control plugin (cleanup, copy, and on GCS the credential
//! exchange) next to hivekeep-api.

pub mod actorlookup;
pub mod actorscope;
pub mod asset;
pub mod control;
pub mod credentials;
pub mod exchange;
pub mod exchangeclient;
pub mod gcsboundary;
pub mod gcsissuer;
pub mod node;
pub mod status;

pub use asset::AssetPlugin;
pub use control::ControlPlugin;
pub use credentials::{Ambient, Credentials};
pub use node::NodePlugin;

#[cfg(test)]
mod tests;

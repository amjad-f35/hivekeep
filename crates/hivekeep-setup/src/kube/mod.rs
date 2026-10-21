//! Cluster access for the installer.
//!
//! [`Cluster`] is the raw API surface (resolve, apply, get, list, patch,
//! delete, exec); [`Client`] builds the installer's operations on it:
//! discovery-cached mapping, retrying server-side apply, tolerant deletes,
//! rollout and condition waits, and helpers for the core objects it manages.
//! [`fake::FakeCluster`] is an in-memory implementation for tests.

mod capool;
mod client;
mod cluster;
mod error;
pub mod fake;
mod gvk;
pub mod live;
mod manifest;
mod resources;
mod wait;

pub use capool::{pem_encode_certs, pool_root_pem};
pub use client::{Backoff, Client, POLL_INTERVAL};
pub use cluster::{Cluster, ExecOutput, Patch, Propagation, ResourceInfo};
pub use error::{classify_transport, KubeError, TransportKind};
pub use gvk::{describe, stub, Gvk, Object, ObjectExt};
pub use manifest::{
    decode_manifest, decode_manifest_bytes, encode_manifest, load_path, normalize_octal_modes,
    read_path,
};
pub use wait::WorkloadKind;

/// Field manager every server-side apply uses, so ownership of the fields
/// the installer sets is attributable and re-applies converge.
pub const FIELD_MANAGER: &str = "hivekeep-setup";

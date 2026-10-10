//! The node-local OCI image layer cache.
//!
//! A content-addressed pool of unpacked image layers, stored once per node
//! and shared by every actor on it, plus what composes an actor's rootfs
//! from those layers as an overlay mount instead of extracting the image on
//! every run.
//!
//! The work splits along the node's privilege boundary:
//!
//! - the keeper (plain root, every capability dropped) pulls and unpacks
//!   layers into the pool ([`Store::ensure_image`]) and writes an overlay
//!   spec into each bundle. Whiteouts are recorded in per-layer metadata
//!   rather than materialized, since overlayfs whiteouts are device nodes
//!   and opaque markers are `trusted.*` xattrs.
//! - the privileged worker finalizes layers (materializing that recorded
//!   state, once per layer) and mounts the overlay rootfs just before the
//!   sandbox starts (`bundle::setup_bundle_rootfs`, Linux only).
//!
//! The halves meet only through the filesystem: the shared cache directory
//! and the per-bundle spec file. [`Store`] implements the keeper's
//! `ImageCache` and `ImageGcStore` traits.
//!
//! The crate also carries the archive helpers the workers snapshot volumes
//! and overlay uppers with ([`tarutil`]) and the runtime-neutral OCI spec
//! builder for actor containers ([`ocispec`]).

pub mod auth;
pub mod backoff;
pub mod confined;
pub mod digest;
pub mod error;
pub mod implicitdirs;
pub mod keychain;
pub mod layer;
pub mod manifest;
pub mod metrics;
pub mod mountinfo;
pub mod reference;
pub mod registry;
pub mod removeall;
pub mod singleflight;
pub mod spawn;
pub mod spec;
pub mod unpack;
pub mod verify;

#[cfg(test)]
pub(crate) mod testutil;

pub use error::{Error, ErrorKind, Result};

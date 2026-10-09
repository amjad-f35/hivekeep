//! Node-agent seams shared by the keeper, the backends that plug into it
//! (image cache, object store, cluster view) and the controllers that reuse
//! its async plumbing.
//!
//! Modules, bottom up:
//!
//! - Async plumbing: [`context`] (cancellation plus deadline), [`clock`]
//!   (wall clock with a mock), [`workqueue`] (deduplicating, rate-limited
//!   keys).
//! - Node layout: [`paths`].
//! - Registry credentials: [`credential_provider`] (kubelet credential
//!   provider exec plugins).
//! - Backend traits: [`image`] and [`image_gc`] (the image layer cache),
//!   [`objectstore`] (snapshot and asset transfers), [`sandbox_config`] and
//!   [`cluster_view`] (the node's worker pods and their sandbox configs).
//! - External volumes: [`csi`] (the CSI driver client both the keeper and
//!   the control plane use).

pub mod clock;
pub mod cluster_view;
pub mod context;
pub mod credential_provider;
pub mod csi;
pub mod image;
pub mod image_gc;
pub mod objectstore;
pub mod paths;
pub mod sandbox_config;
pub mod workqueue;

#[cfg(test)]
mod test_support;

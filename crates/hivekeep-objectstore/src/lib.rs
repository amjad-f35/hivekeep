//! Hivekeep object storage.
//!
//! - [`storage`]: snapshot content in object storage: sparse-extent zstd
//!   compression, budgeted parallel uploads, ranged downloads.
//! - [`gcs`] and [`s3`]: the two cloud backends.
//! - [`store`]: listing, copying and deleting whole snapshots by name.
//! - [`plugin`]: the object-store plugin API served on a Unix socket, its
//!   credential exchange, and the keeper's [`ObjectStore`] over it.
//! - [`control_client`]: the control plane's cleanup and copy client.
//!
//! [`ObjectStore`]: hivekeep_node::objectstore::ObjectStore

pub mod error;
pub mod gcp;
pub mod http;
pub mod lru;
pub mod pluginsocket;
pub mod proto;
pub mod retry;
pub mod semaphore;
pub mod storage;
pub mod tls;

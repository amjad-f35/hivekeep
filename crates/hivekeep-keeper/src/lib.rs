//! The Hivekeep node agent ("keeper").
//!
//! The keeper runs on every node. It serves the `Keeper` service the
//! control plane drives actor lifecycles through (Run, Checkpoint, Restore,
//! PromoteSnapshot, DetachActor, Terminate) and the `WorkerSupport` service
//! the node's sandbox workers reach the control plane through. It stages
//! sandbox runtimes and OCI bundles, keeps node-local snapshots and moves
//! them to and from object storage in the background, and caches shared
//! snapshot files and runtime assets on the node.

pub mod actordirs;
pub mod assets;
pub mod atomicfile;
pub mod boot_id;
pub use hivekeep_node::clock;
pub use hivekeep_node::context;
pub mod control;
pub mod csi_volumes;
pub use hivekeep_node::credential_provider;
pub mod device_plugin;
pub mod drain;
pub mod egress_policy;
pub mod filecache;
pub mod filecachegc;
pub mod http_endpoints;
pub mod keeper_metrics;
pub mod keyed_lock;
pub mod labels;
pub mod objectstore;
pub mod oci;
pub use hivekeep_node::paths;
pub mod peer_identity;
pub mod rand;
pub mod runtime_index;
pub mod sandbox_versions;
pub mod snapshot_tracker;
pub mod sparsefile;
pub mod status;
pub mod system_info;
pub mod trustbundle;
pub mod volumes;
pub mod worker_dialer;
pub mod worker_jwt;
pub use hivekeep_node::workqueue;

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
pub use hivekeep_node::credential_provider;
pub mod egress_policy;
pub mod filecache;
pub mod filecachegc;
pub mod keyed_lock;
pub mod labels;
pub mod objectstore;
pub use hivekeep_node::paths;
pub mod rand;
pub mod runtime_index;
pub mod sandbox_versions;
pub mod snapshot_tracker;
pub mod sparsefile;
pub mod status;
pub mod worker_dialer;
pub mod worker_jwt;
pub use hivekeep_node::workqueue;

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
pub use hivekeep_node::clock;
pub use hivekeep_node::context;
pub use hivekeep_node::credential_provider;
pub mod filecache;
pub mod filecachegc;
pub mod keyed_lock;
pub mod labels;
pub mod objectstore;
pub use hivekeep_node::paths;
pub mod rand;
pub mod snapshot_tracker;
pub mod sparsefile;
pub mod status;
pub use hivekeep_node::workqueue;

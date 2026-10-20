//! Reconciles Kubernetes worker pods into the Worker registry behind the
//! hivekeep-api Control API.

pub mod informers;
pub mod key;
pub mod registry;
pub mod syncer;

#[cfg(test)]
mod fake_registry;
#[cfg(test)]
mod tests;

pub use key::WorkerKey;
pub use registry::WorkerRegistry;
pub use syncer::WorkerPoolSyncer;

/// Names the WorkerPool a worker pod belongs to; its presence is what marks
/// a pod as a worker pod, so it doubles as the pod informer's selector.
pub const WORKER_POD_LABEL: &str = "hivekeep.io/worker-pool";

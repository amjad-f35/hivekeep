//! The `hivekeep.io/v1` custom resources.

pub mod csidriverconfig;
pub mod group;
pub mod manifests;
pub mod sandbox_class;
pub mod sandboxconfig;
pub mod validation;
pub mod workerpool;

pub use csidriverconfig::{CSIDriverConfig, CsiDriverConfigSpec, CsiDriverTlsConfig};
pub use group::{API_VERSION, GROUP, VERSION};
pub use sandbox_class::SandboxClass;
pub use sandboxconfig::{
    AssetFile, SandboxConfig, SandboxConfigSpec, SandboxVersionConfig, SandboxVersionState,
};
pub use workerpool::{
    SandboxConfigReference, WorkerPool, WorkerPoolPodTemplate, WorkerPoolSandboxClass,
    WorkerPoolSandboxClassStatus, WorkerPoolSpec, WorkerPoolStatus,
};

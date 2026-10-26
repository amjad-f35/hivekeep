//! HkEnvUser: drives Actors running the sandbox guest environment. Each
//! iteration resumes one of the user's Actors, runs `echo hello world` in
//! it through the guest process API (reached over gRPC through the gate
//! router), checks the output, and suspends it again.

use std::sync::Arc;

use crate::loadgen::userclass::{Entry, UserTask};

pub mod knobs;
pub mod target;

pub mod actor;
pub mod runtime;

/// The locust class name.
pub const USER_CLASS: &str = "HkEnvUser";
/// The ActorTemplate Actors are created from.
pub const TEMPLATE_NAME: &str = "hk-env";
/// The Hive the template lives in.
pub const TEMPLATE_HIVE: &str = "benchmark-workloads";

/// The hkenv class.
pub fn entries() -> Vec<Entry> {
    vec![Entry {
        name: "hkenv",
        locust_file: "hk_env.py",
        user_class: USER_CLASS,
        config: Some(knobs::codec),
        init: |cfg| Arc::new(runtime::TaskRuntime::new(cfg)) as Arc<dyn UserTask>,
    }]
}

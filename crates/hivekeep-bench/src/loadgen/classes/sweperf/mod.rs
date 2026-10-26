//! The SweperfUser class: each user creates an Actor from a SWE-bench
//! workload template and drives it through a trajectory of steps split into
//! cycles (resume, replay a slice of steps in the sandbox, suspend). When
//! the trajectory is done the Actor is deleted and a fresh one runs it
//! again. Any SWE-bench-style workload works, given its template and
//! config.

pub mod chunks;
pub mod ctx;
mod events;
pub mod knobs;
pub mod runtime;
pub mod session;
pub mod wire;

#[cfg(test)]
mod test_support;

use std::sync::Arc;
use std::time::Duration;

use crate::loadgen::userclass::{Config, Entry, UserTask};

/// The locust user class name.
pub const USER_CLASS: &str = "SweperfUser";
/// The Hive holding the workload templates.
pub const TEMPLATE_HIVE: &str = "benchmark-workloads";
/// The template used when the config names none.
pub const DEFAULT_TEMPLATE: &str = "sweperf-astropy-7336";
/// Trajectory steps when the config sets none.
pub const DEFAULT_TOTAL_STEPS: usize = 21;
/// Cycles when the config sets none.
pub const DEFAULT_NUM_CYCLES: usize = 4;
/// The `/status` poll interval when the config sets none; it trades
/// accuracy of the cycle timings against load on the gate.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Derived rows span more than one call, so they use this method.
pub const DERIVED_METHOD: &str = "actor";
/// From resume to the sandbox accepting a cycle's work.
pub const RESUME_TO_FIRST_EXEC: &str = "ResumeToFirstExec";
/// One cycle's in-container command time.
pub const CYCLE_CEL: &str = "CycleCEL";
/// A trajectory's summed in-container command time.
pub const TASK_CEL: &str = "TaskCEL";
/// A trajectory's summed cycle wall clock.
pub const TASK_WALL_CLOCK: &str = "TaskWallClock";

fn init(cfg: Arc<Config>) -> Arc<dyn UserTask> {
    Arc::new(runtime::SweperfRuntime::new(cfg, USER_CLASS))
}

/// The class's registry entry.
pub fn entries() -> Vec<Entry> {
    vec![Entry {
        name: "sweperf",
        locust_file: "sweperf.py",
        user_class: USER_CLASS,
        config: Some(|| Box::new(knobs::codec())),
        init,
    }]
}

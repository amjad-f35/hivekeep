//! The epoch each Worker on this node registered with through the relay.

use std::collections::HashMap;

use parking_lot::Mutex;

/// Records the epoch each Worker registered with through the relay, for the
/// herder to report from Run and Restore as the worker process would:
/// hivekeep-api commits a resumed Actor as RUNNING only when that epoch is
/// the Worker's active one.
///
/// It lives in memory only. A fake keeper that restarts after its Workers
/// registered reports epoch 0 for them, and fake-workersync never registers
/// a Worker again, so every resume on those Workers fails with Unavailable
/// until they are replaced.
#[derive(Debug, Default)]
pub struct WorkerEpochs {
    epochs: Mutex<HashMap<String, i64>>,
}

impl WorkerEpochs {
    /// An empty record.
    pub fn new() -> WorkerEpochs {
        WorkerEpochs::default()
    }

    /// Records `epoch` for the Worker named `name`.
    pub fn set(&self, name: &str, epoch: i64) {
        self.epochs.lock().insert(name.to_string(), epoch);
    }

    /// The epoch the Worker named `name` registered with, or 0 if it has not
    /// registered through this fake keeper.
    pub fn get(&self, name: &str) -> i64 {
        self.epochs.lock().get(name).copied().unwrap_or(0)
    }
}

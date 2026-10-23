//! Process-wide state that lives in memory, and therefore survives only a
//! memory-fidelity snapshot.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

/// Seconds the process lingers after SIGTERM by default.
pub const DEFAULT_SIGTERM_SLEEP_SECS: i64 = 15;

/// Where each counter is kept.
#[derive(Clone, Debug)]
pub struct Paths {
    pub file_counter_dir: PathBuf,
    pub second_file_counter_dir: Option<PathBuf>,
    pub rootfs_counter_file: PathBuf,
    pub validate_existing_file: Option<PathBuf>,
}

/// Shared handler state.
#[derive(Debug)]
pub struct State {
    pub paths: Paths,
    requests: AtomicU64,
    ready: AtomicBool,
    sigterm_sleep_secs: AtomicI64,
}

impl State {
    pub fn new(paths: Paths) -> Self {
        Self {
            paths,
            requests: AtomicU64::new(0),
            ready: AtomicBool::new(false),
            sigterm_sleep_secs: AtomicI64::new(DEFAULT_SIGTERM_SLEEP_SECS),
        }
    }

    /// Bumps and returns the in-memory request counter.
    pub fn next_request(&self) -> u64 {
        self.requests.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::SeqCst);
    }

    pub fn sigterm_sleep_secs(&self) -> i64 {
        self.sigterm_sleep_secs.load(Ordering::SeqCst)
    }

    pub fn set_sigterm_sleep_secs(&self, secs: i64) {
        self.sigterm_sleep_secs.store(secs, Ordering::SeqCst);
    }
}

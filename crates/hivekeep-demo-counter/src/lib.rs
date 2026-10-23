//! The counter demo workload.
//!
//! Every request to `/` bumps three counters that live in different places:
//! process memory, a file on a durable volume, and a file on the container's
//! root filesystem. Comparing them before and after a suspend/resume shows
//! exactly which state a snapshot fidelity preserves.

pub mod app;
pub mod file_counter;
pub mod net_addr;
pub mod random_file;
pub mod state;

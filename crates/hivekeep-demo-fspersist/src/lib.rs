//! The fspersist demo workload.
//!
//! It shares a WorkerPool with the counter demo to show that two templates
//! with entirely different binaries can be packed onto the same workers. Where
//! the counter proves memory survives suspend/resume, this one proves the root
//! filesystem does: each request prepends a line with the current address and
//! a running count to a history file, and the count is read back from that
//! file rather than kept in memory. The addresses in the history reveal every
//! move to a new worker.

pub mod app;
pub mod history;
pub mod net_addr;

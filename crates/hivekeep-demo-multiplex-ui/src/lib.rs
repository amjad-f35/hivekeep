//! Dashboard for the Claude Code multiplex demo.
//!
//! Three agents share a two-worker pool; the page shows which worker hosts
//! which Actor right now, tails each worker pod's logs, and lets a viewer
//! hand out canned tasks to watch them move through queued, running and
//! completed while Hivekeep suspends and resumes the agents underneath.

pub mod inventory;
pub mod logs;
pub mod server;
pub mod summaries;
pub mod tasks;

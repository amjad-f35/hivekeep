//! The sandbox demo.
//!
//! The workload (`sandbox`) runs inside an Actor and executes whatever command
//! it is sent on `POST /process`. The client (`sandbox-client`) resumes the
//! Actor, offers a `sandbox>` prompt that forwards each line through the gate,
//! and suspends the Actor again on exit. Because the Actor is snapshotted
//! between sessions, files created in one session are there in the next.
//!
//! There is no authorization of any kind: never expose this to untrusted
//! callers.

pub mod client;
pub mod duration;
pub mod exec;
pub mod server;
pub mod wire;

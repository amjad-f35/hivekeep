//! The Hivekeep sandbox worker for micro-VMs.
//!
//! One worker pod hosts many Actors, each in its own Cloud Hypervisor
//! micro-VM booted with a kata guest image. The worker drives the VMM over
//! its REST API and the guest through the kata agent (ttrpc over the VMM's
//! hybrid vsock), and serves `hivekeep.worker.v1.Worker` to the keeper.
//!
//! Layers, bottom up:
//!
//! - Shared encodings (re-exported from `hivekeep-core`): [`cpu_features`],
//!   [`hardware`]. Host plumbing:
//!   [`sizing`], [`cdi`], [`pidfd`], [`pid1`], [`tarball`], [`child`],
//!   [`actor_cgroup`], [`etc_files`].
//! - Usage bookkeeping: [`activation`], [`usage`], [`agent_stats`].
//! - The guest: [`ttrpc`], [`agentpb`], [`kata`]; the VMM: [`ch`].
//! - Keeper integration: [`keeper_dial`], [`register`], [`crash_report`].
//! - The worker: [`service`], [`stats`], [`boot`], [`restore`],
//!   [`checkpoint`], [`crash`], [`shutdown`], [`grpc`].

/// CPU feature encoding, shared with the keeper and control plane.
pub use hivekeep_core::cpu_features;
/// Runtime compatibility matching, shared with the control plane.
pub use hivekeep_core::hardware;

pub mod activation;
pub mod actor_cgroup;
pub mod actor_path;
pub mod agent_stats;
pub mod agentpb;
pub mod cdi;
pub mod ch;
pub mod child;
pub mod config;
/// Shared with the gVisor worker.
pub use hivekeep_sandbox_gvisor::crash_report;
pub mod etc_files;
pub mod fidelity;
/// Shared with the gVisor worker.
pub use hivekeep_sandbox_gvisor::file_watch;
pub mod guest_spec;
pub mod guestpb;
pub mod kata;
/// Shared with the gVisor worker.
pub use hivekeep_sandbox_gvisor::keeper_dial;
pub mod original_dst;
pub mod phases;
pub mod pid1;
pub mod pidfd;
/// Shared with the gVisor worker.
pub use hivekeep_sandbox_gvisor::register;
/// Shared with the gVisor worker.
pub use hivekeep_sandbox_gvisor::runtimes_file;
pub mod sizing;
pub mod snapshot_files;
pub mod tarball;
pub mod ttrpc;
pub mod usage;
pub mod wakeup_probe;

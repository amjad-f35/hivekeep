//! The Hivekeep sandbox worker for gVisor.
//!
//! One worker pod hosts many Actors, each in its own gVisor sandbox driven
//! through the `runsc` CLI. The keeper on the node calls the worker's
//! `hivekeep.worker.v1.Worker` service to run, checkpoint, restore and
//! terminate workloads, and polls it for usage.
//!
//! Layers, bottom up: request validation and node paths; runsc command
//! construction ([`flags`], [`oci`], [`runsc`]); checkpoint and cgroup
//! mechanics ([`fs_checkpoint`], [`ckpt_cgroup`], [`memory_watch`],
//! [`cgroup_stats`]); the service state ([`service`], [`hosted`]); and the
//! RPC handlers ([`lifecycle`], [`stats`], [`grpc`]).

pub mod activation;
pub mod actor_lock;
pub mod actor_path;
pub mod attribution;
pub mod cgroup;
pub mod cgroup_stats;
pub mod checkpoint_record;
pub mod confined_dir;
pub mod crash_report;
pub mod elf_soname;
pub mod error;
pub mod file_watch;
pub mod flags;
pub mod fs_checkpoint;
pub mod keeper_dial;
pub mod memory_watch;
pub mod mounts;
pub mod node_path;
pub mod oci;
pub mod phaselog;
pub mod ports;
pub mod register;
pub mod request_checks;
pub mod rootfs;
pub mod runsc;
pub mod runtimes_file;
pub mod sandbox_process;
pub mod sizing;
pub mod snapshot_cache;
#[cfg(test)]
mod test_log;
pub mod validation;
pub mod wakeup_probe;

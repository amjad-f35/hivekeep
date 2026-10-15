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

pub mod actor_path;
pub mod error;
pub mod flags;
pub mod mounts;
pub mod node_path;
pub mod request_checks;
pub mod validation;

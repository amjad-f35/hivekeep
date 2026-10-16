//! Driving Cloud Hypervisor over its REST API on a unix socket: the VMM's
//! version and what follows from it, the VM config, boot, pause, snapshot and
//! restore, and merging the sparse memory images an on-demand restore leaves.

pub mod client;
pub mod http;
pub mod merge;
pub mod vm_config;
pub mod vmm_info;

pub use client::{launch_vmm, Client, LaunchOptions, RestoredNet, SnapshotNetDevice};
pub use vm_config::*;
pub use vmm_info::{VmmInfo, MEM_RESTORE_EAGER, MEM_RESTORE_ON_DEMAND};

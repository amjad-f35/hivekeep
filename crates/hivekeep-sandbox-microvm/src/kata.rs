//! Driving the kata guest: host paths of a sandbox, guest kernel parameters,
//! OCI spec conversion, the agent client, the host-side rootfs staging and
//! the cleanup of leftover sandbox state.

pub mod agent;
pub mod cleanup;
pub mod config;
pub mod overlay;
pub mod paths;
pub mod specconv;

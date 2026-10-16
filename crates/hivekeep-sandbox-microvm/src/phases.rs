//! Phase names of the checkpoint and restore timing records.
//!
//! Checkpoint: snapshot, durable_dir and rootfs_upper run concurrently on the
//! paused guest, so the paused window costs their max; prep, pause and
//! teardown are sequential around them. Restore: the phases are sequential
//! and partition the total.

/// Before the guest is paused.
pub const PREP: &str = "prep";
/// Pausing the guest.
pub const PAUSE: &str = "pause";
/// The VMM's snapshot.
pub const SNAPSHOT: &str = "snapshot";
/// Archiving the durable-dir volumes.
pub const DURABLE_DIR: &str = "durable_dir";
/// Archiving the rootfs uppers.
pub const ROOTFS_UPPER: &str = "rootfs_upper";
/// Tearing the VM down.
pub const TEARDOWN: &str = "teardown";
/// Building the container bundles.
pub const BUNDLES: &str = "bundles";
/// Joining the background upper restore.
pub const UPPER_JOIN: &str = "upper_join";
/// Mounting the merged rootfs trees and starting virtiofsd.
pub const LOWERS: &str = "lowers";
/// Building the tap devices.
pub const TAP: &str = "tap";
/// Starting the VMM.
pub const VMM_LAUNCH: &str = "vmm_launch";
/// The VMM reading guest RAM back.
pub const VM_RESTORE: &str = "vm_restore";
/// Resuming the guest.
pub const RESUME: &str = "resume";
/// From resume until the wakeup probes pass.
pub const WAKEUP_PROBE: &str = "wakeup_probe";
/// The whole operation; shared with the other layers' records.
pub const TOTAL: &str = "total";

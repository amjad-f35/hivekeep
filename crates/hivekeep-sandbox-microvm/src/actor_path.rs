//! Paths derived from the Actor directories the keeper sends. The rootfs
//! upper directory under the root dir is the worker's own: the keeper does
//! not know it.

use std::path::{Path, PathBuf};

use hivekeep_proto::worker::v1 as pb;

/// A container's OCI bundle.
pub fn oci_bundle_path(d: &pb::ActorDirs, container: &str) -> PathBuf {
    Path::new(&d.oci_bundle_dir).join(container)
}

/// The host directory of the Actor's rootfs overlay uppers, one
/// subdirectory per container.
pub fn rootfs_upper_dir(d: &pb::ActorDirs) -> PathBuf {
    Path::new(&d.root_dir).join("rootfs-upper")
}

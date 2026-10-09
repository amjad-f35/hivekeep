//! The keeper's on-node layout.
//!
//! Everything lives under one base directory that the node's worker pods
//! mount at the same path, so a path the keeper hands a worker resolves the
//! same on both sides. Snapshots may live on a separate mount (the
//! snapshots dir), which is why an actor has two roots: its actor dir and
//! its snapshot dir. Within the snapshot dir, checkpoints link and rename
//! files between subdirectories, so that tree must stay on one filesystem.

use std::path::{Path, PathBuf};

use hivekeep_proto::worker::v1 as wpb;

/// The default base directory on the host.
pub const DEFAULT_BASE_DIR: &str = "/var/lib/hivekeep";

/// File name of a worker's sandbox runtimes file, under its worker dir.
pub const SANDBOX_RUNTIMES_FILE_NAME: &str = "sandbox-runtimes.json";

/// The node's directory layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePaths {
    base: PathBuf,
    snapshots: PathBuf,
}

impl NodePaths {
    /// The layout rooted at `base`, with snapshots under `base/snapshots`.
    pub fn new(base: impl Into<PathBuf>) -> Self {
        let base = base.into();
        let snapshots = base.join("snapshots");
        NodePaths { base, snapshots }
    }

    /// The same layout with snapshots under `dir`.
    pub fn with_snapshots_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.snapshots = dir.into();
        self
    }

    /// The base directory.
    pub fn base(&self) -> &Path {
        &self.base
    }

    /// Whether `dir` lies strictly under the base directory. Relative paths
    /// are resolved against the current directory, lexically.
    pub fn under_base(&self, dir: &Path) -> bool {
        let abs = if dir.is_absolute() {
            dir.to_path_buf()
        } else {
            match std::env::current_dir() {
                Ok(cwd) => cwd.join(dir),
                Err(_) => return false,
            }
        };
        let clean = lexical_clean(&abs);
        clean != self.base && clean.starts_with(&self.base)
    }

    /// Parent of every per-actor directory.
    pub fn actors_dir(&self) -> PathBuf {
        self.base.join("actors")
    }

    /// Parent of every per-actor snapshot directory.
    pub fn snapshots_dir(&self) -> &Path {
        &self.snapshots
    }

    /// Parent of every per-worker directory.
    pub fn workers_dir(&self) -> PathBuf {
        self.base.join("workers")
    }

    /// The node-local OCI image layer cache.
    pub fn image_cache_dir(&self) -> PathBuf {
        self.base.join("image-cache")
    }

    /// Short-lived directories for snapshot manifests moved through object
    /// storage.
    pub fn snapshot_scratch_dir(&self) -> PathBuf {
        self.base.join("snapshot-scratch")
    }

    /// The node-local content-addressed sandbox asset cache. It shares a
    /// mount with the actors dir: an actor's assets are hard links from it.
    pub fn sandbox_asset_cache_dir(&self) -> PathBuf {
        self.base.join("sandbox-asset-cache")
    }

    /// Destinations for asset fetches that link into no actor or worker,
    /// wiped at startup.
    pub fn sandbox_asset_scratch_dir(&self) -> PathBuf {
        self.base.join("sandbox-asset-scratch")
    }

    /// Every sandbox runtime a worker on this node advertised, by hash.
    pub fn sandbox_runtime_index_file(&self) -> PathBuf {
        self.base.join("sandbox-runtime-index.json")
    }

    /// The node-local cache of shared snapshot files.
    pub fn snapshot_cache_dir(&self) -> PathBuf {
        self.base.join("snapshot-cache")
    }

    /// Where CSI node drivers stage volumes before publishing them,
    /// one directory per storage volume ID.
    pub fn csi_staging_dir(&self) -> PathBuf {
        self.base.join("staging")
    }

    /// Socket the WorkerSupport service listens on for the node's workers.
    pub fn worker_support_socket(&self) -> PathBuf {
        self.base.join("worker-support.sock")
    }

    /// One worker pod's directory.
    pub fn worker_dir(&self, pod_uid: &str) -> PathBuf {
        self.workers_dir().join(pod_uid)
    }

    /// The socket a worker pod serves the Worker API on.
    pub fn worker_socket(&self, pod_uid: &str) -> PathBuf {
        self.worker_dir(pod_uid).join("worker.sock")
    }

    /// The sandbox runtimes file the keeper writes for a worker pod.
    pub fn worker_sandbox_runtimes_file(&self, pod_uid: &str) -> PathBuf {
        self.worker_dir(pod_uid).join(SANDBOX_RUNTIMES_FILE_NAME)
    }

    /// An actor's directory.
    pub fn actor_dir(&self, actor_uid: &str) -> PathBuf {
        self.actors_dir().join(actor_uid)
    }

    /// An actor's snapshot staging directory, possibly on another mount.
    pub fn actor_snapshot_dir(&self, actor_uid: &str) -> PathBuf {
        self.snapshots.join(actor_uid)
    }

    /// The record of the sandbox runtime an actor is running. It sits
    /// directly under the actor dir, outside the subdirectories a reset
    /// empties, so it survives from a Run to the Checkpoint that follows.
    pub fn actor_sandbox_assets_file(&self, actor_uid: &str) -> PathBuf {
        self.actor_dir(actor_uid).join("sandbox-assets.json")
    }

    /// What the keeper tracks of an actor's snapshots, kept across restarts.
    pub fn actor_snapshot_state_file(&self, actor_uid: &str) -> PathBuf {
        self.actor_dir(actor_uid).join(SNAPSHOT_STATE_FILE_NAME)
    }

    /// The sandbox runtime binaries an actor runs, hard links from the asset
    /// cache. The worker derives the same path to check requests against.
    pub fn sandbox_assets_dir(&self, actor_uid: &str) -> PathBuf {
        self.actor_dir(actor_uid).join("sandbox-assets")
    }

    /// Where a Run stages an activation's assets before swapping them in.
    pub fn sandbox_assets_staging_dir(&self, actor_uid: &str) -> PathBuf {
        self.actor_dir(actor_uid).join("sandbox-assets.staging")
    }

    /// Parent of an actor's OCI bundles.
    pub fn oci_bundle_dir(&self, actor_uid: &str) -> PathBuf {
        self.actor_dir(actor_uid).join("bundles")
    }

    /// One container's OCI bundle.
    pub fn oci_bundle_path(&self, actor_uid: &str, container: &str) -> PathBuf {
        self.oci_bundle_dir(actor_uid).join(container)
    }

    /// Where the worker writes a checkpoint.
    pub fn checkpoint_state_dir(&self, actor_uid: &str) -> PathBuf {
        self.actor_snapshot_dir(actor_uid).join("checkpoint-state")
    }

    /// Parent of an actor's node-local snapshots.
    pub fn local_checkpoints_dir(&self, actor_uid: &str) -> PathBuf {
        self.actor_snapshot_dir(actor_uid).join("local-checkpoint")
    }

    /// One node-local snapshot: its files plus its manifest.
    pub fn local_snapshot_dir(&self, actor_uid: &str, snapshot: &str) -> PathBuf {
        self.local_checkpoints_dir(actor_uid).join(snapshot)
    }

    /// Where a restore stages a downloaded snapshot. It differs from the
    /// checkpoint dir because a sandbox restoring in the background may
    /// still be paging from it when the next checkpoint is written.
    pub fn restore_state_dir(&self, actor_uid: &str) -> PathBuf {
        self.actor_snapshot_dir(actor_uid).join("restore-state")
    }

    /// Parent of an actor's durable-dir volumes.
    pub fn durable_dir_volume_mounts_dir(&self, actor_uid: &str) -> PathBuf {
        self.actor_dir(actor_uid).join("durable-dir")
    }

    /// One durable-dir volume.
    pub fn durable_dir_volume_mount_point(&self, actor_uid: &str, volume: &str) -> PathBuf {
        self.durable_dir_volume_mounts_dir(actor_uid).join(volume)
    }

    /// Parent of an actor's system-info volume roots. Kept apart from the
    /// durable-dir volumes so a sandbox that captures that whole directory
    /// never captures generated files.
    pub fn system_info_volume_roots_dir(&self, actor_uid: &str) -> PathBuf {
        self.actor_dir(actor_uid).join("system-info")
    }

    /// One system-info volume's root.
    pub fn system_info_volume_root(&self, actor_uid: &str, volume: &str) -> PathBuf {
        self.system_info_volume_roots_dir(actor_uid).join(volume)
    }

    /// Parent of an actor's external volume mount points.
    pub fn external_volume_mounts_dir(&self, actor_uid: &str) -> PathBuf {
        self.actor_dir(actor_uid).join("volumes")
    }

    /// One external volume's mount point.
    pub fn external_volume_host_path(&self, actor_uid: &str, volume: &str) -> PathBuf {
        self.external_volume_mounts_dir(actor_uid).join(volume)
    }

    /// The directory set handed to the worker for an actor.
    pub fn actor_dirs(&self, actor_uid: &str) -> wpb::ActorDirs {
        let s = |p: PathBuf| p.to_string_lossy().into_owned();
        wpb::ActorDirs {
            root_dir: s(self.actor_dir(actor_uid)),
            oci_bundle_dir: s(self.oci_bundle_dir(actor_uid)),
            checkpoint_dir: s(self.checkpoint_state_dir(actor_uid)),
            restore_dir: s(self.restore_state_dir(actor_uid)),
            durable_dir_volume_mounts_dir: s(self.durable_dir_volume_mounts_dir(actor_uid)),
            system_info_volume_roots_dir: s(self.system_info_volume_roots_dir(actor_uid)),
            external_volume_mounts_dir: s(self.external_volume_mounts_dir(actor_uid)),
            sandbox_assets_dir: s(self.sandbox_assets_dir(actor_uid)),
        }
    }
}

impl Default for NodePaths {
    fn default() -> Self {
        Self::new(DEFAULT_BASE_DIR)
    }
}

/// File name of an actor's snapshot state file.
pub const SNAPSHOT_STATE_FILE_NAME: &str = "snapshot-state.json";

/// Resolves `.` and `..` components without touching the filesystem.
pub fn lexical_clean(p: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Whether `rel` is a relative path that stays below its base: not empty,
/// not absolute, and never climbing out with `..`.
pub fn is_local(rel: &Path) -> bool {
    use std::path::Component;
    if rel.as_os_str().is_empty() {
        return false;
    }
    let mut depth: i64 = 0;
    for c in rel.components() {
        match c {
            Component::Normal(_) => depth += 1,
            Component::CurDir => {}
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            Component::RootDir | Component::Prefix(_) => return false,
        }
    }
    true
}

#[cfg(test)]
mod behavior_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn proper_sub(parent: &Path, dir: &Path) -> bool {
        dir != parent && dir.starts_with(parent)
    }

    #[test]
    fn actor_dirs_layout() {
        let p = NodePaths::new("/var/lib/hivekeep");
        let uid = "actor-uid-1";
        let dirs = p.actor_dirs(uid);
        let root = PathBuf::from(&dirs.root_dir);
        assert_eq!(root, p.actor_dir(uid));
        let snap_root = p.actor_snapshot_dir(uid);
        assert!(!snap_root.starts_with(&root) && !root.starts_with(&snap_root));

        let under_root = [
            ("oci_bundle_dir", dirs.oci_bundle_dir.clone()),
            ("durable", dirs.durable_dir_volume_mounts_dir.clone()),
            ("system_info", dirs.system_info_volume_roots_dir.clone()),
            ("external", dirs.external_volume_mounts_dir.clone()),
            ("assets", dirs.sandbox_assets_dir.clone()),
        ];
        let under_snap = [
            ("checkpoint", dirs.checkpoint_dir.clone()),
            ("restore", dirs.restore_dir.clone()),
            (
                "local",
                p.local_checkpoints_dir(uid).to_string_lossy().into_owned(),
            ),
            (
                "local_snap",
                p.local_snapshot_dir(uid, "snap-1")
                    .to_string_lossy()
                    .into_owned(),
            ),
        ];
        let mut seen = HashMap::new();
        for (name, d) in under_root {
            assert!(proper_sub(&root, Path::new(&d)), "{name} = {d}");
            assert!(seen.insert(d, name).is_none());
        }
        for (name, d) in under_snap {
            assert!(proper_sub(&snap_root, Path::new(&d)), "{name} = {d}");
            assert!(seen.insert(d, name).is_none());
        }
    }

    #[test]
    fn under_base_is_strict() {
        let p = NodePaths::new("/var/lib/hivekeep");
        assert!(p.under_base(Path::new("/var/lib/hivekeep/snapshot-cache")));
        assert!(!p.under_base(Path::new("/var/lib/hivekeep")));
        assert!(!p.under_base(Path::new("/var/lib/hivekeep/../other")));
        assert!(!p.under_base(Path::new("/tmp/cache")));
    }

    #[test]
    fn local_paths() {
        assert!(is_local(Path::new("a/b")));
        assert!(is_local(Path::new("a/../b")));
        assert!(!is_local(Path::new("../b")));
        assert!(!is_local(Path::new("/abs")));
        assert!(!is_local(Path::new("")));
    }
}

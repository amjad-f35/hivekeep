//! Where a sandbox's host-side state lives.

use std::path::{Path, PathBuf};

use crate::guest_spec::{GUEST_SHARED_DIR, SHARE_VOLUMES};

/// The per-sandbox runtime directories: the VMM's API socket, the hybrid
/// vsock socket, the virtiofsd socket and the guest console logs.
pub const VC_VM_DIR: &str = "/run/vc/vm";
/// Where the shared virtio-fs trees of all sandboxes live.
pub const SHARED_SANDBOXES_DIR: &str = "/run/kata-containers/shared/sandboxes";
/// The virtio-fs tag of the shared tree; the VMM's fs device and the agent's
/// mount source must both use it.
pub const FS_TAG: &str = "kataShared";

/// The sandbox's runtime directory.
pub fn vm_dir(id: &str) -> PathBuf {
    Path::new(VC_VM_DIR).join(id)
}

/// The default VMM API socket; the worker records the one it launched on.
pub fn clh_socket_path(id: &str) -> PathBuf {
    vm_dir(id).join("clh-api.sock")
}

/// The hybrid-vsock socket the VMM listens on for the guest. Snapshots
/// reference it by absolute path, so restore repoints it.
pub fn vsock_socket_path(id: &str) -> PathBuf {
    vm_dir(id).join("clh.sock")
}

/// The guest's virtio-console log: the kernel from virtio-console probe on,
/// and everything the agent prints.
pub fn console_log_path(id: &str) -> PathBuf {
    vm_dir(id).join("console.log")
}

/// The emulated UART's log, wired up only in debug mode.
pub fn serial_log_path(id: &str) -> PathBuf {
    vm_dir(id).join("serial.log")
}

/// The vhost-user-fs socket the VMM connects to.
pub fn virtiofsd_socket_path(id: &str) -> PathBuf {
    vm_dir(id).join("virtiofsd.sock")
}

/// The host directory virtiofsd serves to the guest. Its layout
/// (`<cid>/rootfs`) is what find-paths migration re-opens on restore.
pub fn shared_dir(id: &str) -> PathBuf {
    Path::new(SHARED_SANDBOXES_DIR).join(id).join("shared")
}

/// The host overlay upperdir and workdir for one container under the Actor's
/// rootfs-upper base: siblings `<name>/fs` and `<name>/work`, since the
/// kernel wants both on one filesystem and refuses a nested workdir.
pub fn upper_work_dirs(upper_base: &Path, name: &str) -> (PathBuf, PathBuf) {
    (
        upper_base.join(name).join("fs"),
        upper_base.join(name).join("work"),
    )
}

/// The guest path a container's merged rootfs appears at.
pub fn guest_shared_rootfs(container_id: &str) -> String {
    format!("{GUEST_SHARED_DIR}/{container_id}/rootfs")
}

/// The guest path one image volume appears at, beside the rootfs.
pub fn guest_shared_volume_dir(container_id: &str, volume: &str) -> String {
    format!("{GUEST_SHARED_DIR}/{container_id}/{SHARE_VOLUMES}/{volume}")
}

/// The host path under the served tree that [`guest_shared_volume_dir`]
/// resolves to.
pub fn shared_volume_dir(id: &str, container_id: &str, volume: &str) -> PathBuf {
    shared_dir(id)
        .join(container_id)
        .join(SHARE_VOLUMES)
        .join(volume)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upper_and_work_dirs_are_siblings() {
        let base = Path::new("/var/lib/worker/actors/uid/rootfs-upper");
        let (upper, work) = upper_work_dirs(base, "app");
        let dir = base.join("app");
        assert_eq!(upper.parent(), Some(dir.as_path()));
        assert_eq!(work.parent(), Some(dir.as_path()));
        assert_ne!(upper, work);
        assert!(!work.starts_with(&upper), "work is nested inside upper");
    }

    /// find-paths re-opens files by their path relative to the share, so each
    /// host subtree must sit at the guest's relative path.
    #[test]
    fn volume_subtrees_match_the_guest_share() {
        use crate::guest_spec::{SHARE_CSI, SHARE_DURABLE, SHARE_SYSTEM_INFO};
        let (uid, cid, vol) = ("uid", "app", "data");
        for (name, host, guest) in [
            (
                "durable",
                shared_dir(uid).join(SHARE_DURABLE).join(vol),
                format!("{GUEST_SHARED_DIR}/{SHARE_DURABLE}/{vol}"),
            ),
            (
                "csi",
                shared_dir(uid).join(SHARE_CSI).join(vol),
                format!("{GUEST_SHARED_DIR}/{SHARE_CSI}/{vol}"),
            ),
            (
                "system-info",
                shared_dir(uid).join(SHARE_SYSTEM_INFO).join(vol),
                format!("{GUEST_SHARED_DIR}/{SHARE_SYSTEM_INFO}/{vol}"),
            ),
            (
                "image",
                shared_volume_dir(uid, cid, vol),
                guest_shared_volume_dir(cid, vol),
            ),
        ] {
            let host_rel = host
                .strip_prefix(shared_dir(uid))
                .unwrap()
                .to_string_lossy()
                .to_string();
            let guest_rel = guest.strip_prefix(&format!("{GUEST_SHARED_DIR}/")).unwrap();
            assert_eq!(host_rel, guest_rel, "{name}");
        }
    }
}

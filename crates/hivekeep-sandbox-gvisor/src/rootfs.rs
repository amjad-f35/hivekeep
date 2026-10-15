//! Composing an application container's bundle rootfs and its `/etc` files.

use std::io;
use std::path::Path;

use hivekeep_proto::worker::v1::ActorDirs;

use crate::actor_path::oci_bundle_path;
use crate::error::{Context, Error, Result};
use crate::oci::HOSTNAME;
use crate::wakeup_probe::{ACTOR_VETH_GATEWAY, ACTOR_VETH_IP};

/// The file the keeper writes into a bundle when its rootfs is an overlay of
/// cached image layers rather than a plain directory.
pub const OVERLAY_SPEC_FILE: &str = "rootfs-overlay.json";

/// Prepares a container bundle's rootfs before runsc creates the container.
/// The keeper runs without capabilities, so mounting is the worker's job.
pub trait RootfsComposer: Send + Sync {
    /// Composes the rootfs of `bundle`.
    fn setup_bundle_rootfs(&self, bundle: &Path) -> Result<()>;
}

/// Accepts bundles whose rootfs the keeper prepared as a plain directory.
/// Overlay bundles need the image cache's composer.
#[derive(Debug, Default)]
pub struct PlainRootfs;

impl RootfsComposer for PlainRootfs {
    fn setup_bundle_rootfs(&self, bundle: &Path) -> Result<()> {
        if bundle.join(OVERLAY_SPEC_FILE).exists() {
            return Err(Error::msg(format!(
                "bundle {} has an overlay rootfs, which this worker has no image cache to compose",
                bundle.display()
            )));
        }
        std::fs::create_dir_all(bundle.join("rootfs")).context("while creating the bundle rootfs")
    }
}

/// Composes the container's rootfs and writes its `/etc` files.
pub fn setup_app_rootfs(
    composer: &dyn RootfsComposer,
    actor_dirs: &ActorDirs,
    container: &str,
) -> Result<()> {
    let bundle = oci_bundle_path(actor_dirs, container);
    composer.setup_bundle_rootfs(&bundle)?;
    let pod_resolv = std::fs::read_to_string("/etc/resolv.conf").unwrap_or_default();
    write_rootfs_etc_files(&bundle.join("rootfs"), HOSTNAME, &pod_resolv)
        .context("while writing the rootfs /etc files")
}

/// The sandbox's resolv.conf: the pod's search and options, resolved through
/// the sandbox gateway's DNS relay.
pub fn sandbox_resolv_conf(pod: &str) -> String {
    let mut out = format!("nameserver {ACTOR_VETH_GATEWAY}\n");
    for line in pod.lines() {
        let l = line.trim_start();
        if l.starts_with("search ") || l.starts_with("options ") {
            out.push_str(l);
            out.push('\n');
        }
    }
    out
}

/// The sandbox's hosts file.
pub fn sandbox_hosts(hostname: &str) -> String {
    format!("127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n{ACTOR_VETH_IP}\t{hostname}\n")
}

/// Writes `/etc/hostname`, `/etc/hosts` and `/etc/resolv.conf` into `rootfs`.
pub fn write_rootfs_etc_files(rootfs: &Path, hostname: &str, pod_resolv: &str) -> io::Result<()> {
    let etc = rootfs.join("etc");
    std::fs::create_dir_all(&etc)?;
    for (name, content) in [
        ("hostname", format!("{hostname}\n")),
        ("hosts", sandbox_hosts(hostname)),
        ("resolv.conf", sandbox_resolv_conf(pod_resolv)),
    ] {
        let p = etc.join(name);
        // A symlink in the image must not redirect the write.
        if p.symlink_metadata()
            .is_ok_and(|m| m.file_type().is_symlink())
        {
            std::fs::remove_file(&p)?;
        }
        std::fs::write(p, content)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_etc_files() {
        let d = tempfile::tempdir().unwrap();
        let rootfs = d.path().join("rootfs");
        std::fs::create_dir_all(rootfs.join("etc")).unwrap();
        std::os::unix::fs::symlink("/etc/passwd", rootfs.join("etc/hosts")).unwrap();
        write_rootfs_etc_files(
            &rootfs,
            "actor",
            "nameserver 10.0.0.10\nsearch a.svc b.svc\noptions ndots:5\n",
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(rootfs.join("etc/hostname")).unwrap(),
            "actor\n"
        );
        assert!(std::fs::read_to_string(rootfs.join("etc/hosts"))
            .unwrap()
            .contains("169.254.17.2\tactor"));
        assert_eq!(
            std::fs::read_to_string(rootfs.join("etc/resolv.conf")).unwrap(),
            "nameserver 169.254.17.1\nsearch a.svc b.svc\noptions ndots:5\n"
        );
    }

    #[test]
    fn plain_rootfs_refuses_overlays() {
        let d = tempfile::tempdir().unwrap();
        PlainRootfs.setup_bundle_rootfs(d.path()).unwrap();
        assert!(d.path().join("rootfs").is_dir());
        std::fs::write(d.path().join(OVERLAY_SPEC_FILE), "{}").unwrap();
        assert!(PlainRootfs.setup_bundle_rootfs(d.path()).is_err());
    }
}

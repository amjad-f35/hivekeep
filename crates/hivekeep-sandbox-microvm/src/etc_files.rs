//! The guest's `/etc/resolv.conf` and `/etc/hosts`, written into a container
//! rootfs before it is served to the guest.
//!
//! The rootfs is untrusted (it is the Actor's image), so every write goes
//! through descriptors opened without following symlinks: a link planted at
//! `etc/resolv.conf` is replaced, never written through.

use std::io::{self, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::path::Path;

use nix::errno::Errno;
use nix::fcntl::{openat, OFlag};
use nix::sys::stat::{mkdirat, Mode};
use nix::unistd::{unlinkat, UnlinkatFlags};

use hivekeep_sandbox_gvisor::wakeup_probe::{ACTOR_VETH_GATEWAY, ACTOR_VETH_IP};

/// The Actor's resolv.conf: the gateway's DNS relay as the only resolver,
/// keeping the pod's search list and options. Any pod resolver left in would
/// let the Actor's DNS bypass the relay.
pub fn sandbox_resolv_conf(nameserver: &str, pod: &str) -> String {
    let mut out = format!("nameserver {nameserver}\n");
    for line in pod.split('\n') {
        let t = line.trim();
        if t.is_empty() || t.starts_with("nameserver") {
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

/// The Actor's hosts file.
pub fn sandbox_hosts(hostname: &str, ip: &str) -> String {
    format!(
        "# Hivekeep-managed hosts file.\n127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n{ip}\t{hostname}\n"
    )
}

/// Writes `etc/resolv.conf` under `rootfs`.
pub fn write_rootfs_resolv_conf(rootfs: &Path, content: &str) -> io::Result<()> {
    write_rootfs_etc_file(rootfs, "resolv.conf", content)
}

/// Writes `etc/hosts` under `rootfs`.
pub fn write_rootfs_hosts(rootfs: &Path, content: &str) -> io::Result<()> {
    write_rootfs_etc_file(rootfs, "hosts", content)
}

fn dir_flags() -> OFlag {
    OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC
}

fn write_rootfs_etc_file(rootfs: &Path, name: &str, content: &str) -> io::Result<()> {
    if content.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("refusing to write an empty {name}"),
        ));
    }
    let root: OwnedFd = nix::fcntl::open(rootfs, dir_flags(), Mode::empty())
        .map_err(|e| io::Error::other(format!("opening rootfs {}: {e}", rootfs.display())))?;
    match mkdirat(root.as_fd(), "etc", Mode::from_bits_truncate(0o755)) {
        Ok(()) | Err(Errno::EEXIST) => {}
        Err(e) => return Err(io::Error::other(format!("creating etc: {e}"))),
    }
    let etc = openat(root.as_fd(), "etc", dir_flags(), Mode::empty())
        .map_err(|e| io::Error::other(format!("opening etc: {e}")))?;
    match unlinkat(etc.as_fd(), name, UnlinkatFlags::NoRemoveDir) {
        Ok(()) | Err(Errno::ENOENT) => {}
        Err(e) => return Err(io::Error::other(format!("removing existing {name}: {e}"))),
    }
    let fd = openat(
        etc.as_fd(),
        name,
        OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o644),
    )
    .map_err(|e| io::Error::other(format!("creating {name}: {e}")))?;
    std::fs::File::from(fd)
        .write_all(content.as_bytes())
        .map_err(|e| io::Error::other(format!("writing {name}: {e}")))
}

/// Writes the guest's resolv.conf (from the worker pod's) and hosts file.
pub fn write_rootfs_etc_files(rootfs: &Path, hostname: &str) -> io::Result<()> {
    let pod = std::fs::read_to_string("/etc/resolv.conf")
        .map_err(|e| io::Error::other(format!("reading the worker pod resolv.conf: {e}")))?;
    write_rootfs_resolv_conf(rootfs, &sandbox_resolv_conf(ACTOR_VETH_GATEWAY, &pod))?;
    write_rootfs_hosts(rootfs, &sandbox_hosts(hostname, ACTOR_VETH_IP))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolv_conf_points_at_the_gateway() {
        let pod = "nameserver 10.96.0.10\nsearch demo.svc.cluster.local svc.cluster.local cluster.local\noptions ndots:5\n";
        assert_eq!(
            sandbox_resolv_conf("169.254.17.1", pod),
            "nameserver 169.254.17.1\nsearch demo.svc.cluster.local svc.cluster.local cluster.local\noptions ndots:5\n"
        );
    }

    #[test]
    fn resolv_conf_keeps_search_and_options() {
        let pod = "search svc.cluster.local\nnameserver 10.96.0.10\nnameserver 10.96.0.11\noptions ndots:5 timeout:1\n";
        let got = sandbox_resolv_conf("169.254.17.1", pod);
        assert!(
            !got.contains("10.96.0.10") && !got.contains("10.96.0.11"),
            "{got}"
        );
        assert!(got.contains("search svc.cluster.local"));
        assert!(got.contains("options ndots:5 timeout:1"));
        assert_eq!(got.matches("nameserver").count(), 1);
    }

    #[test]
    fn writes_resolv_conf() {
        let d = tempfile::tempdir().unwrap();
        write_rootfs_resolv_conf(d.path(), "nameserver 169.254.17.1\n").unwrap();
        assert_eq!(
            std::fs::read_to_string(d.path().join("etc/resolv.conf")).unwrap(),
            "nameserver 169.254.17.1\n"
        );
        write_rootfs_resolv_conf(d.path(), "nameserver 169.254.17.1\nsearch x\n").unwrap();
        assert!(write_rootfs_resolv_conf(d.path(), "").is_err());
    }

    #[test]
    fn does_not_follow_a_planted_symlink() {
        let d = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let victim = outside.path().join("victim");
        std::fs::write(&victim, "original").unwrap();
        std::fs::create_dir(d.path().join("etc")).unwrap();
        std::os::unix::fs::symlink(&victim, d.path().join("etc/resolv.conf")).unwrap();
        write_rootfs_resolv_conf(d.path(), "nameserver 169.254.17.1\n").unwrap();
        assert_eq!(std::fs::read_to_string(&victim).unwrap(), "original");
    }

    #[test]
    fn writes_hosts() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir(d.path().join("etc")).unwrap();
        std::fs::write(d.path().join("etc/hosts"), "").unwrap();
        write_rootfs_hosts(d.path(), &sandbox_hosts("actor", "169.254.17.2")).unwrap();
        assert_eq!(
            std::fs::read_to_string(d.path().join("etc/hosts")).unwrap(),
            "# Hivekeep-managed hosts file.\n127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n169.254.17.2\tactor\n"
        );
    }
}

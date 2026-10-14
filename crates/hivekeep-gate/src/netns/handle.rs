//! Namespace handles and named namespaces.

use std::io;
use std::net::TcpListener;

use super::validate_name;

#[cfg(target_os = "linux")]
mod imp {
    use std::fs::File;
    use std::io;
    use std::os::fd::{AsFd, AsRawFd, OwnedFd};
    use std::path::Path;

    use nix::mount::{mount, umount2, MntFlags, MsFlags};
    use nix::sched::{setns, unshare, CloneFlags};

    use super::super::NAMED_NETNS_DIR;

    pub fn errno(e: nix::Error) -> io::Error {
        io::Error::from_raw_os_error(e as i32)
    }

    #[derive(Debug)]
    pub struct Fd(pub OwnedFd);

    impl Fd {
        pub fn open(path: &Path) -> io::Result<Fd> {
            Ok(Fd(File::open(path)?.into()))
        }

        pub fn try_clone(&self) -> io::Result<Fd> {
            Ok(Fd(self.0.try_clone()?))
        }

        pub fn raw(&self) -> i32 {
            self.0.as_raw_fd()
        }

        /// Runs `f` on a fresh thread joined to this namespace.
        pub fn run<T: Send>(&self, f: impl FnOnce() -> io::Result<T> + Send) -> io::Result<T> {
            let fd = self.0.as_fd();
            std::thread::scope(|s| {
                s.spawn(|| {
                    setns(fd, CloneFlags::CLONE_NEWNET).map_err(errno)?;
                    f()
                })
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("netns worker thread panicked")))
            })
        }
    }

    pub fn remove(name: &str) -> io::Result<()> {
        let path = Path::new(NAMED_NETNS_DIR).join(name);
        match umount2(&path, MntFlags::MNT_DETACH | MntFlags::UMOUNT_NOFOLLOW) {
            Ok(()) | Err(nix::Error::ENOENT) | Err(nix::Error::EINVAL) => {}
            Err(e) => return Err(errno(e)),
        }
        match std::fs::remove_file(&path) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    pub fn create(name: &str) -> io::Result<Fd> {
        let dir = Path::new(NAMED_NETNS_DIR);
        std::fs::create_dir_all(dir)?;
        let path = dir.join(name);
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        let created = std::thread::scope(|s| {
            s.spawn(|| -> io::Result<Fd> {
                unshare(CloneFlags::CLONE_NEWNET).map_err(errno)?;
                let own = Path::new("/proc/thread-self/ns/net");
                mount(
                    Some(own),
                    &path,
                    None::<&str>,
                    MsFlags::MS_BIND,
                    None::<&str>,
                )
                .map_err(errno)?;
                Fd::open(own)
            })
            .join()
            .unwrap_or_else(|_| Err(io::Error::other("netns worker thread panicked")))
        });
        if created.is_err() {
            let _ = remove(name);
        }
        created
    }
}

/// An open handle to a network namespace. The namespace lives at least as
/// long as the handle, even after its name is removed.
#[derive(Debug)]
pub struct NetNs {
    #[cfg(target_os = "linux")]
    fd: imp::Fd,
}

#[cfg(not(target_os = "linux"))]
fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "network namespaces are only supported on Linux",
    )
}

impl NetNs {
    /// The namespace of the calling thread.
    pub fn current() -> io::Result<NetNs> {
        #[cfg(target_os = "linux")]
        {
            imp::Fd::open(std::path::Path::new("/proc/thread-self/ns/net")).map(|fd| NetNs { fd })
        }
        #[cfg(not(target_os = "linux"))]
        Err(unsupported())
    }

    /// Opens the namespace at `path` (e.g. `/proc/<pid>/ns/net`).
    pub fn open(path: &std::path::Path) -> io::Result<NetNs> {
        #[cfg(target_os = "linux")]
        {
            imp::Fd::open(path).map(|fd| NetNs { fd })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Err(unsupported())
        }
    }

    /// Another handle to the same namespace.
    pub fn try_clone(&self) -> io::Result<NetNs> {
        #[cfg(target_os = "linux")]
        {
            self.fd.try_clone().map(|fd| NetNs { fd })
        }
        #[cfg(not(target_os = "linux"))]
        Err(unsupported())
    }

    /// The raw descriptor of the handle.
    #[cfg(target_os = "linux")]
    pub fn as_raw_fd(&self) -> i32 {
        self.fd.raw()
    }

    /// Whether both handles refer to the same namespace.
    pub fn same_namespace(&self, other: &NetNs) -> io::Result<bool> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;
            let meta = |fd: i32| std::fs::metadata(format!("/proc/self/fd/{fd}"));
            let (a, b) = (meta(self.as_raw_fd())?, meta(other.as_raw_fd())?);
            Ok(a.dev() == b.dev() && a.ino() == b.ino())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = other;
            Err(unsupported())
        }
    }

    /// Runs `f` on a fresh OS thread inside this namespace and returns its
    /// result. Sockets created by `f` stay in the namespace.
    pub fn run<T: Send>(&self, f: impl FnOnce() -> io::Result<T> + Send) -> io::Result<T> {
        #[cfg(target_os = "linux")]
        {
            self.fd.run(f)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = f;
            Err(unsupported())
        }
    }
}

/// Opens the namespace named `name` under `/run/netns`.
pub fn get_from_name(name: &str) -> io::Result<NetNs> {
    validate_name(name)?;
    NetNs::open(&std::path::Path::new(super::NAMED_NETNS_DIR).join(name))
}

/// Creates a fresh namespace named `name`, replacing a leftover of the same
/// name (mounted or not). Handles to the leftover stay usable.
pub fn create_named(name: &str) -> io::Result<NetNs> {
    validate_name(name)?;
    remove_named(name).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("while removing the leftover netns {name}: {e}"),
        )
    })?;
    #[cfg(target_os = "linux")]
    {
        imp::create(name).map(|fd| NetNs { fd })
    }
    #[cfg(not(target_os = "linux"))]
    Err(unsupported())
}

/// Unmounts and removes the name `name`, without following symlinks. A
/// missing name is not an error.
pub fn remove_named(name: &str) -> io::Result<()> {
    validate_name(name)?;
    #[cfg(target_os = "linux")]
    {
        imp::remove(name)
    }
    #[cfg(not(target_os = "linux"))]
    Err(unsupported())
}

/// Opens TCP listeners on `0.0.0.0:<port>` for each port inside `ns`. On
/// failure every listener opened so far is closed.
pub fn listen(ns: &NetNs, ports: &[u16]) -> io::Result<Vec<TcpListener>> {
    ns.run(|| {
        ports
            .iter()
            .map(|port| {
                TcpListener::bind(("0.0.0.0", *port)).map_err(|e| {
                    io::Error::new(e.kind(), format!("while listening on port {port}: {e}"))
                })
            })
            .collect()
    })
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    #[ignore = "needs root: creates network namespaces"]
    fn remove_named_does_not_follow_symlinks() {
        use std::os::unix::fs::MetadataExt;
        let target_name = "actornet-symlink-target-test";
        let link_name = "actornet-symlink-test";
        let target_path = Path::new(super::super::NAMED_NETNS_DIR).join(target_name);
        let link_path = Path::new(super::super::NAMED_NETNS_DIR).join(link_name);
        let _target = create_named(target_name).unwrap();
        let before = std::fs::metadata(&target_path).unwrap();
        std::os::unix::fs::symlink(&target_path, &link_path).unwrap();
        remove_named(link_name).unwrap();
        assert!(std::fs::symlink_metadata(&link_path).is_err());
        let after = std::fs::metadata(&target_path).unwrap();
        assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
        let _ = remove_named(target_name);
    }

    #[test]
    #[ignore = "needs root: creates network namespaces"]
    fn create_named_replaces_a_leftover() {
        for state in ["mounted", "unmounted"] {
            let name = format!("actornet-leftover-test-{state}");
            let path = Path::new(super::super::NAMED_NETNS_DIR).join(&name);
            let first = create_named(&name).unwrap();
            if state == "unmounted" {
                nix::mount::umount2(&path, nix::mount::MntFlags::MNT_DETACH).unwrap();
            }
            assert!(path.exists(), "the leftover must remain");
            let second = create_named(&name).expect("name wedged by its own leftover");
            assert!(!second.same_namespace(&first).unwrap());
            first
                .run(|| std::net::UdpSocket::bind("0.0.0.0:0").map(drop))
                .expect("the retained handle is no longer usable");
            for _ in 0..2 {
                remove_named(&name).unwrap();
                assert!(!path.exists());
            }
        }
    }
}

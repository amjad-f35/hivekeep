//! Pinning the runsc sandbox process, so waiting on it cannot be fooled by a
//! reused pid.
//!
//! runsc treats a sandbox as live while its pid exists, zombie included, so
//! after a MEMORY checkpoint stops the sandbox, `runsc delete` fails until the
//! process is reaped.

use std::io;
use std::time::Duration;

use hivekeep_proto::worker::v1::ActorDirs;

use crate::runsc::sandbox_pid_file;

/// How long to wait for a checkpointed sandbox to go away.
pub const SANDBOX_EXIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Reads the sandbox's pid from the pid file runsc writes.
pub fn sandbox_pid(actor_dirs: &ActorDirs) -> io::Result<i32> {
    let path = sandbox_pid_file(actor_dirs);
    let b = std::fs::read_to_string(&path)?;
    let pid: i32 = b.trim().parse().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parsing sandbox pid {:?} in {}: {e}", b, path.display()),
        )
    })?;
    if pid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid sandbox pid {pid} in {}", path.display()),
        ));
    }
    Ok(pid)
}

#[cfg(target_os = "linux")]
mod imp {
    use super::*;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use tokio::io::unix::AsyncFd;

    /// The sandbox process, held by pidfd.
    #[derive(Debug)]
    pub struct SandboxProcess {
        /// Its pid when pinned.
        pub pid: i32,
        fd: OwnedFd,
    }

    fn pidfd_open(pid: i32) -> io::Result<OwnedFd> {
        // SAFETY: plain syscall; the returned fd is owned below.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fd is a fresh descriptor this process owns.
        Ok(unsafe { OwnedFd::from_raw_fd(fd as i32) })
    }

    impl SandboxProcess {
        /// Pins the process with pid `pid`.
        pub fn open_pid(pid: i32) -> io::Result<Self> {
            let fd = pidfd_open(pid).map_err(|e| {
                io::Error::new(e.kind(), format!("opening pidfd for sandbox {pid}: {e}"))
            })?;
            Ok(SandboxProcess { pid, fd })
        }

        /// Resolves once the sandbox exits. Must be called inside a Tokio
        /// runtime; the returned future owns a duplicate of the pidfd.
        pub fn exited(&self) -> io::Result<impl std::future::Future<Output = ()> + Send + 'static> {
            let dup = self.fd.try_clone()?;
            // SAFETY: valid fd; switching it to non-blocking for the reactor.
            unsafe {
                let flags = libc::fcntl(dup.as_raw_fd(), libc::F_GETFL);
                libc::fcntl(dup.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
            // SAFETY: `dup` is an owned, valid descriptor that the AsyncFd
            // takes ownership of and nothing else closes or replaces.
            let afd =
                unsafe { AsyncFd::register_with_interest(dup, tokio::io::Interest::READABLE)? };
            Ok(async move {
                // A pidfd turns readable when the process exits.
                let _ = afd.readable().await;
            })
        }

        /// Whether the process still exists (a zombie counts).
        pub fn present(&self) -> io::Result<bool> {
            // SAFETY: plain syscall on an owned fd.
            let rc = unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    self.fd.as_raw_fd(),
                    0,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                )
            };
            if rc == 0 {
                return Ok(true);
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::ESRCH) => Ok(false),
                Some(libc::EPERM) => Ok(true),
                _ => Err(err),
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use super::*;

    /// The sandbox process. Pinning needs pidfds, which only Linux has.
    #[derive(Debug)]
    pub struct SandboxProcess {
        /// Its pid when pinned.
        pub pid: i32,
    }

    impl SandboxProcess {
        /// Unsupported off Linux.
        pub fn open_pid(pid: i32) -> io::Result<Self> {
            let _ = pid;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "pidfds need Linux",
            ))
        }

        /// Unsupported off Linux.
        pub fn exited(&self) -> io::Result<std::future::Pending<()>> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "pidfds need Linux",
            ))
        }

        /// Unsupported off Linux.
        pub fn present(&self) -> io::Result<bool> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "pidfds need Linux",
            ))
        }
    }
}

pub use imp::SandboxProcess;

impl SandboxProcess {
    /// Pins the sandbox named by its pid file before it can exit.
    pub fn open(actor_dirs: &ActorDirs) -> io::Result<Self> {
        Self::open_pid(sandbox_pid(actor_dirs)?)
    }

    /// Waits until the sandbox has exited and been reaped.
    pub async fn wait_gone(&self, timeout: Duration) -> io::Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        // Usually gone within milliseconds of the checkpoint returning.
        let mut backoff = Duration::from_millis(1);
        loop {
            if !self.present().map_err(|e| {
                io::Error::new(e.kind(), format!("signaling sandbox {}: {e}", self.pid))
            })? {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("sandbox {} still present after {timeout:?}", self.pid),
                ));
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_millis(50));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dirs_with_pid(pid: &str) -> (tempfile::TempDir, ActorDirs) {
        let root = tempfile::tempdir().unwrap();
        let d = ActorDirs {
            root_dir: root.path().display().to_string(),
            ..Default::default()
        };
        let f = sandbox_pid_file(&d);
        std::fs::create_dir_all(f.parent().unwrap()).unwrap();
        std::fs::write(f, pid).unwrap();
        (root, d)
    }

    #[test]
    fn reads_the_pid_file() {
        assert_eq!(sandbox_pid(&dirs_with_pid("42\n").1).unwrap(), 42);
        assert!(sandbox_pid(&dirs_with_pid("x").1).is_err());
        assert!(sandbox_pid(&dirs_with_pid("0").1).is_err());
    }

    #[cfg(target_os = "linux")]
    mod linux {
        use super::*;
        use std::process::Command;

        #[tokio::test]
        async fn wait_gone_after_reap() {
            let mut child = Command::new("true").spawn().unwrap();
            let p = SandboxProcess::open_pid(child.id() as i32).unwrap();
            child.wait().unwrap();
            p.wait_gone(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn wait_gone_waits_for_the_reap() {
            let mut child = Command::new("true").spawn().unwrap();
            let p = SandboxProcess::open_pid(child.id() as i32).unwrap();
            // Exited but not reaped: still present.
            p.exited().unwrap().await;
            assert!(p.wait_gone(Duration::from_millis(50)).await.is_err());
            child.wait().unwrap();
            p.wait_gone(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn wait_gone_times_out_while_running() {
            let mut child = Command::new("sleep").arg("10").spawn().unwrap();
            let p = SandboxProcess::open_pid(child.id() as i32).unwrap();
            assert!(p.wait_gone(Duration::from_millis(30)).await.is_err());
            child.kill().unwrap();
            child.wait().unwrap();
        }

        #[tokio::test]
        async fn exited_fires() {
            let mut child = Command::new("true").spawn().unwrap();
            let p = SandboxProcess::open_pid(child.id() as i32).unwrap();
            tokio::time::timeout(Duration::from_secs(5), p.exited().unwrap())
                .await
                .unwrap();
            child.wait().unwrap();
        }
    }
}

//! Process exit notification through pidfds. A pidfd pins the process when it
//! is opened, so a later reuse of the pid is never mistaken for it, and
//! waiting on one parks a task rather than a thread.

use std::io;

use tokio::sync::watch;

/// Fires once the watched process exits. Clones share the notification.
#[derive(Debug, Clone)]
pub struct Exited(watch::Receiver<bool>);

impl Exited {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    fn done() -> Self {
        let (tx, rx) = watch::channel(true);
        drop(tx);
        Exited(rx)
    }

    /// Whether the process has exited.
    pub fn has_exited(&self) -> bool {
        *self.0.borrow()
    }

    /// Waits until the process has exited.
    pub async fn wait(&self) {
        let mut rx = self.0.clone();
        let _ = rx.wait_for(|done| *done).await;
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use std::io;
    use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};

    use tokio::io::unix::AsyncFd;
    use tokio::sync::watch;

    use super::Exited;

    fn pidfd_open(pid: i32, flags: libc::c_uint) -> io::Result<OwnedFd> {
        // SAFETY: plain syscall; a non-negative return is a new descriptor.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, flags) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the kernel just returned this descriptor to us.
        Ok(unsafe { OwnedFd::from_raw_fd(fd as i32) })
    }

    /// Polls `fd` for readability for up to `timeout_ms` (-1: forever).
    pub(super) fn readable(fd: i32, timeout_ms: i32) -> bool {
        let mut p = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        loop {
            // SAFETY: one valid pollfd.
            let n = unsafe { libc::poll(&mut p, 1, timeout_ms) };
            if n < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return n > 0;
        }
    }

    pub fn exited(pid: i32) -> io::Result<Exited> {
        let fd = match pidfd_open(pid, libc::PIDFD_NONBLOCK as libc::c_uint) {
            // PIDFD_NONBLOCK is Linux 5.10+; the watch falls back to polling.
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => pidfd_open(pid, 0),
            other => other,
        };
        match fd {
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => Ok(Exited::done()),
            Err(e) => Err(e),
            Ok(fd) => watch(fd),
        }
    }

    pub fn exited_fd(pidfd: BorrowedFd<'_>) -> io::Result<Exited> {
        let fd = pidfd.try_clone_to_owned()?;
        // The duplicate shares the status flags, which makes the caller's
        // descriptor nonblocking too; signalling through it does not mind.
        // SAFETY: fcntl on a descriptor we own.
        unsafe {
            let flags = libc::fcntl(fd.as_raw_fd(), libc::F_GETFL);
            if flags < 0 || libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        watch(fd)
    }

    /// Fires once `fd` turns readable, taking ownership of it. Needs a tokio
    /// runtime.
    pub fn watch(fd: OwnedFd) -> io::Result<Exited> {
        let (tx, rx) = watch::channel(false);
        // SAFETY: the descriptor is owned and stays open while registered.
        match unsafe { AsyncFd::register_with_interest(fd, tokio::io::Interest::READABLE) } {
            Ok(afd) => {
                tokio::spawn(async move {
                    loop {
                        // Check the descriptor itself rather than trusting a
                        // wakeup: readiness that arrived before the wait
                        // started must not be lost.
                        if readable(afd.as_raw_fd(), 0) {
                            break;
                        }
                        match afd.readable().await {
                            Ok(mut g) => {
                                if readable(afd.as_raw_fd(), 0) {
                                    break;
                                }
                                g.clear_ready();
                            }
                            Err(e) => {
                                tracing::warn!(err = %e, "pidfd: falling back to a blocking poll");
                                let fd = afd.into_inner();
                                let _ = tokio::task::spawn_blocking(move || blocking(fd)).await;
                                break;
                            }
                        }
                    }
                    let _ = tx.send(true);
                });
            }
            Err(e) => {
                let (fd, err) = e.into_parts();
                tracing::warn!(err = %err, "pidfd: falling back to a blocking poll");
                tokio::task::spawn_blocking(move || {
                    blocking(fd);
                    let _ = tx.send(true);
                });
            }
        }
        Ok(Exited(rx))
    }

    fn blocking(fd: OwnedFd) {
        while !readable(fd.as_raw_fd(), -1) {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }

    /// Fires once `fd` turns readable, waiting in a blocking poll on a thread
    /// of its own: the path taken when the reactor cannot watch the fd.
    pub fn watch_blocking(fd: OwnedFd) -> Exited {
        let (tx, rx) = watch::channel(false);
        tokio::task::spawn_blocking(move || {
            blocking(fd);
            let _ = tx.send(true);
        });
        Exited(rx)
    }

    pub fn open(pid: i32) -> io::Result<OwnedFd> {
        pidfd_open(pid, 0)
    }
}

#[cfg(target_os = "linux")]
pub use imp::{exited_fd, open, watch, watch_blocking};

/// Fires once `pid` exits; already fired when it is gone. The pid is pinned
/// now, so a later reuse of the number is never mistaken for it.
pub fn exited(pid: i32) -> io::Result<Exited> {
    #[cfg(target_os = "linux")]
    return imp::exited(pid);
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::os::fd::AsFd;
    use std::time::Duration;
    use tokio::process::Command;

    async fn fires_within(e: &Exited, d: Duration) -> bool {
        tokio::time::timeout(d, e.wait()).await.is_ok()
    }

    #[tokio::test]
    async fn fires_when_the_process_exits() {
        let mut c = Command::new("sleep").arg("30").spawn().unwrap();
        let e = exited(c.id().unwrap() as i32).unwrap();
        assert!(!fires_within(&e, Duration::from_millis(100)).await);
        c.start_kill().unwrap();
        assert!(fires_within(&e, Duration::from_secs(10)).await);
        let _ = c.wait().await;
    }

    #[tokio::test]
    async fn a_reaped_process_has_already_exited() {
        let mut c = Command::new("true").spawn().unwrap();
        let pid = c.id().unwrap() as i32;
        c.wait().await.unwrap();
        let e = exited(pid).unwrap();
        assert!(fires_within(&e, Duration::from_secs(10)).await);
    }

    #[tokio::test]
    async fn watches_a_duplicate_of_a_pinned_process() {
        let mut c = Command::new("sleep").arg("30").spawn().unwrap();
        let fd = open(c.id().unwrap() as i32).unwrap();
        let e = exited_fd(fd.as_fd()).unwrap();
        drop(fd);
        c.start_kill().unwrap();
        assert!(fires_within(&e, Duration::from_secs(10)).await);
        let _ = c.wait().await;
    }

    #[tokio::test]
    async fn the_blocking_fallback_fires_too() {
        let mut c = Command::new("sleep").arg("30").spawn().unwrap();
        let e = watch_blocking(open(c.id().unwrap() as i32).unwrap());
        assert!(!fires_within(&e, Duration::from_millis(200)).await);
        c.start_kill().unwrap();
        assert!(fires_within(&e, Duration::from_secs(10)).await);
        let _ = c.wait().await;
    }

    #[tokio::test]
    async fn never_misses_an_exit_racing_the_watch() {
        for _ in 0..200 {
            let mut c = Command::new("true").spawn().unwrap();
            let e = exited(c.id().unwrap() as i32).unwrap();
            assert!(
                fires_within(&e, Duration::from_secs(10)).await,
                "exit missed"
            );
            let _ = c.wait().await;
        }
    }

    fn threads() -> usize {
        std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .find_map(|l| l.strip_prefix("Threads:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn holds_no_threads() {
        let before = threads();
        let mut children = Vec::new();
        for _ in 0..200 {
            let c = Command::new("sleep").arg("30").spawn().unwrap();
            exited(c.id().unwrap() as i32).unwrap();
            children.push(c);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let after = threads();
        for c in &mut children {
            let _ = c.start_kill();
        }
        for mut c in children {
            let _ = c.wait().await;
        }
        assert!(
            after.saturating_sub(before) <= 20,
            "threads grew from {before} to {after}"
        );
    }
}

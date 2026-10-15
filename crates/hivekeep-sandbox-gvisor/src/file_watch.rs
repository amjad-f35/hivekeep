//! Signalling when a file is created, written or replaced, so its reader can
//! re-read it. The parent directory is watched, so an atomic replacement by
//! rename is seen even when the file does not exist yet.
//!
//! Where the kernel's file events are unavailable (another platform, or the
//! node's inotify budget exhausted), the watch falls back to signalling on a
//! timer.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// The default interval of the polling fallback.
pub const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// What happened to a path in a watched directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// Created, or renamed into place.
    Create,
    /// Written.
    Write,
    /// Removed.
    Remove,
    /// Renamed away.
    Rename,
    /// Its metadata changed.
    Chmod,
}

/// One file event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// The path the event is about.
    pub path: PathBuf,
    /// What happened.
    pub ops: Vec<Op>,
}

/// Sends a signal unless one is already pending.
fn signal(changes: &mpsc::Sender<()>) {
    let _ = changes.try_send(());
}

/// Signals on `changes` for every create or write of `path` among `events`,
/// and for every watcher error (an overflowed queue, say) so the caller
/// re-reads. Ends when `cancel` fires or either stream ends.
pub async fn watch_file_loop(
    path: &Path,
    mut events: mpsc::Receiver<Event>,
    mut errs: mpsc::Receiver<String>,
    changes: mpsc::Sender<()>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            ev = events.recv() => {
                let Some(ev) = ev else { return };
                if ev.path == path && ev.ops.iter().any(|o| matches!(o, Op::Create | Op::Write)) {
                    signal(&changes);
                }
            }
            e = errs.recv() => {
                let Some(e) = e else { return };
                tracing::warn!(path = %path.display(), err = %e, "File watch error; signalling a re-read");
                signal(&changes);
            }
        }
    }
}

/// Signals on the returned channel every `interval` until `cancel` fires.
pub fn poll(interval: Duration, cancel: CancellationToken) -> mpsc::Receiver<()> {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let mut t = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = t.tick() => signal(&tx),
            }
        }
    });
    rx
}

/// Watches `path`, falling back to polling every `poll_interval`. The
/// channel closes when `cancel` fires.
pub fn watch_file_path(
    path: &Path,
    poll_interval: Duration,
    cancel: CancellationToken,
) -> mpsc::Receiver<()> {
    #[cfg(target_os = "linux")]
    match inotify::watch(path, cancel.clone()) {
        Ok(rx) => return rx,
        Err(e) => {
            tracing::warn!(path = %path.display(), interval = ?poll_interval, err = %e, "Could not watch the file; falling back to polling");
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = path;
    poll(poll_interval, cancel)
}

#[cfg(target_os = "linux")]
mod inotify {
    use super::*;
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use tokio::io::unix::AsyncFd;

    /// Watches `path`'s parent directory with inotify.
    pub fn watch(path: &Path, cancel: CancellationToken) -> io::Result<mpsc::Receiver<()>> {
        let path = path.to_path_buf();
        let dir = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .to_path_buf();
        // SAFETY: plain syscall; a non-negative return is a new descriptor.
        let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the kernel just returned this descriptor.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let c = std::ffi::CString::new(dir.as_os_str().as_bytes()).map_err(io::Error::other)?;
        let mask = libc::IN_CREATE
            | libc::IN_MODIFY
            | libc::IN_CLOSE_WRITE
            | libc::IN_MOVED_TO
            | libc::IN_DELETE
            | libc::IN_MOVED_FROM
            | libc::IN_ATTRIB;
        // SAFETY: valid descriptor and path.
        if unsafe { libc::inotify_add_watch(fd.as_raw_fd(), c.as_ptr(), mask) } < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the descriptor is owned and stays open while registered.
        let afd = unsafe { AsyncFd::register_with_interest(fd, tokio::io::Interest::READABLE) }
            .map_err(|e| e.into_parts().1)?;
        let (ev_tx, ev_rx) = mpsc::channel(64);
        let (err_tx, err_rx) = mpsc::channel(4);
        let (tx, rx) = mpsc::channel(1);
        let reader_cancel = cancel.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let ready = tokio::select! {
                    _ = reader_cancel.cancelled() => return,
                    r = afd.readable() => r,
                };
                let Ok(mut g) = ready else { return };
                // SAFETY: reading into a buffer we own.
                let n = unsafe { libc::read(afd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
                if n < 0 {
                    let e = io::Error::last_os_error();
                    if e.kind() == io::ErrorKind::WouldBlock {
                        g.clear_ready();
                        continue;
                    }
                    let _ = err_tx.send(e.to_string()).await;
                    return;
                }
                let mut off = 0usize;
                let hdr = std::mem::size_of::<libc::inotify_event>();
                while off + hdr <= n as usize {
                    // SAFETY: the kernel wrote whole events into buf.
                    let ev: libc::inotify_event =
                        unsafe { std::ptr::read_unaligned(buf.as_ptr().add(off).cast()) };
                    let raw = &buf[off + hdr..off + hdr + ev.len as usize];
                    let evname = raw.split(|b| *b == 0).next().unwrap_or_default();
                    off += hdr + ev.len as usize;
                    if ev.mask & libc::IN_Q_OVERFLOW != 0 {
                        let _ = err_tx.send("event queue overflow".into()).await;
                        continue;
                    }
                    let mut ops = Vec::new();
                    if ev.mask & (libc::IN_CREATE | libc::IN_MOVED_TO) != 0 {
                        ops.push(Op::Create);
                    }
                    if ev.mask & (libc::IN_MODIFY | libc::IN_CLOSE_WRITE) != 0 {
                        ops.push(Op::Write);
                    }
                    if ev.mask & libc::IN_DELETE != 0 {
                        ops.push(Op::Remove);
                    }
                    if ev.mask & libc::IN_MOVED_FROM != 0 {
                        ops.push(Op::Rename);
                    }
                    if ev.mask & libc::IN_ATTRIB != 0 {
                        ops.push(Op::Chmod);
                    }
                    let p = dir.join(std::ffi::OsStr::from_bytes(evname));
                    if ev_tx.send(Event { path: p, ops }).await.is_err() {
                        return;
                    }
                }
            }
        });
        tokio::spawn(watch_file_loop_owned(path, ev_rx, err_rx, tx, cancel));
        Ok(rx)
    }

    async fn watch_file_loop_owned(
        path: PathBuf,
        events: mpsc::Receiver<Event>,
        errs: mpsc::Receiver<String>,
        changes: mpsc::Sender<()>,
        cancel: CancellationToken,
    ) {
        watch_file_loop(&path, events, errs, changes, cancel).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn falls_back_to_polling() {
        let d = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        let missing = d.path().join("missing-parent/sandbox-runtimes.json");
        let mut rx = watch_file_path(&missing, Duration::from_millis(5), cancel.clone());
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("no polling signal")
            .expect("channel closed before a polling signal");
        cancel.cancel();
        while rx.recv().await.is_some() {}
    }

    #[tokio::test]
    async fn filters_events_and_signals_errors() {
        let target = PathBuf::from("/var/lib/hivekeep/workers/pod-uid/sandbox-runtimes.json");
        let (ev_tx, ev_rx) = mpsc::channel(8);
        let (err_tx, err_rx) = mpsc::channel(8);
        let (tx, mut rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let task = tokio::spawn({
            let (target, cancel) = (target.clone(), cancel.clone());
            async move { watch_file_loop(&target, ev_rx, err_rx, tx, cancel).await }
        });
        let ev = |p: &str, ops: &[Op]| Event {
            path: PathBuf::from(p),
            ops: ops.to_vec(),
        };
        let none = |rx: &mut mpsc::Receiver<()>| {
            let r = rx.try_recv();
            assert!(r.is_err(), "unexpected signal");
        };
        async fn some(rx: &mut mpsc::Receiver<()>) {
            tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap()
                .unwrap();
        }
        ev_tx
            .send(ev(
                "/var/lib/hivekeep/workers/pod-uid/worker.sock",
                &[Op::Create],
            ))
            .await
            .unwrap();
        ev_tx
            .send(ev(
                "/var/lib/hivekeep/workers/pod-uid/.sandbox-runtimes-123456",
                &[Op::Create, Op::Write],
            ))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        none(&mut rx);
        let t = target.to_str().unwrap();
        for op in [Op::Chmod, Op::Remove, Op::Rename] {
            ev_tx.send(ev(t, &[op])).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        none(&mut rx);
        ev_tx.send(ev(t, &[Op::Create])).await.unwrap();
        some(&mut rx).await;
        ev_tx.send(ev(t, &[Op::Write])).await.unwrap();
        some(&mut rx).await;
        err_tx.send("queue overflow".into()).await.unwrap();
        some(&mut rx).await;
        cancel.cancel();
        task.await.unwrap();
    }
}

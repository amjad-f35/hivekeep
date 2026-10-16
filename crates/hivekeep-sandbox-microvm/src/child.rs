//! A started subprocess that is waited on from the start, so it never
//! lingers as a zombie whether it is stopped or dies on its own.

use std::process::ExitStatus;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::process::Command;
use tokio::sync::{watch, Notify};

/// A running or exited child process. Cheap to share.
#[derive(Debug)]
pub struct Process {
    pid: u32,
    kill: Arc<Notify>,
    done: watch::Receiver<Option<Option<ExitStatus>>>,
    stopping: AtomicBool,
}

impl Process {
    /// Starts `cmd` and begins waiting on it. The process is not tied to the
    /// caller: dropping the handle does not kill it.
    pub fn start(mut cmd: Command) -> std::io::Result<Arc<Process>> {
        cmd.kill_on_drop(false);
        let mut child = cmd.spawn()?;
        let pid = child.id().unwrap_or_default();
        let kill = Arc::new(Notify::new());
        let (tx, rx) = watch::channel(None);
        let k = kill.clone();
        tokio::spawn(async move {
            let status = tokio::select! {
                s = child.wait() => s.ok(),
                _ = k.notified() => {
                    let _ = child.start_kill();
                    child.wait().await.ok()
                }
            };
            let _ = tx.send(Some(status));
        });
        Ok(Arc::new(Process {
            pid,
            kill,
            done: rx,
            stopping: AtomicBool::new(false),
        }))
    }

    /// The process id.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Waits until the process has exited and been reaped.
    pub async fn done(&self) {
        let mut rx = self.done.clone();
        let _ = rx.wait_for(Option::is_some).await;
    }

    /// Whether the process has exited and been reaped.
    pub fn exited(&self) -> bool {
        self.done.borrow().is_some()
    }

    /// Kills the process and waits until it is reaped. Afterwards
    /// [`Process::stopped`] reports true, so a watcher of [`Process::done`]
    /// can tell this exit from a crash. Stopping a reaped process does
    /// nothing.
    pub async fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        if !self.exited() {
            self.kill.notify_one();
        }
        self.done().await;
    }

    /// Whether [`Process::stop`] was called.
    pub fn stopped(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// How the process exited, once it has.
    pub fn exit(&self) -> String {
        match *self.done.borrow() {
            Some(Some(s)) => describe(s),
            _ => "exited".into(),
        }
    }
}

/// Describes an exit status in the usual shell words.
pub fn describe(s: ExitStatus) -> String {
    use std::os::unix::process::ExitStatusExt;
    if let Some(code) = s.code() {
        return format!("exit status {code}");
    }
    match s.signal() {
        Some(sig) => format!("signal: {}", signal_name(sig)),
        None => "exited".into(),
    }
}

fn signal_name(sig: i32) -> String {
    match sig {
        libc::SIGHUP => "hangup".into(),
        libc::SIGINT => "interrupt".into(),
        libc::SIGQUIT => "quit".into(),
        libc::SIGILL => "illegal instruction".into(),
        libc::SIGABRT => "aborted".into(),
        libc::SIGBUS => "bus error".into(),
        libc::SIGFPE => "floating point exception".into(),
        libc::SIGKILL => "killed".into(),
        libc::SIGSEGV => "segmentation fault".into(),
        libc::SIGPIPE => "broken pipe".into(),
        libc::SIGTERM => "terminated".into(),
        other => format!("signal {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn reaped_when_it_dies_on_its_own() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "kill -KILL $$"]);
        let p = Process::start(cmd).unwrap();
        tokio::time::timeout(Duration::from_secs(10), p.done())
            .await
            .expect("process was not reaped");
        assert!(!p.stopped());
        assert_eq!(p.exit(), "signal: killed");
    }

    #[tokio::test]
    async fn stop_kills_and_reaps() {
        let mut cmd = Command::new("sleep");
        cmd.arg("30");
        let p = Process::start(cmd).unwrap();
        p.stop().await;
        assert!(p.exited(), "stop returned before the process was reaped");
        assert!(p.stopped());
        // A second stop of a reaped process neither blocks nor signals.
        tokio::time::timeout(Duration::from_secs(1), p.stop())
            .await
            .unwrap();
    }
}

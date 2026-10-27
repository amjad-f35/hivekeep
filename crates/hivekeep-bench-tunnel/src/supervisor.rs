//! The rest of a worker pod around the tunnel, in a privileged container:
//! the fake keeper on the node-local socket, the tunnel (the `tunnel`
//! subcommand) in a cgroup of its own with the CPU and memory limits, an
//! ingress actor and an egress actor in every Actor's sandbox, and a fresh
//! tunnel whenever one dies, with why it died logged.

use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio_util::sync::CancellationToken;

use crate::fake_keeper::FakeKeeper;
use crate::load::stats::Admin;
use crate::workerhost::Info;
use crate::{Error, Result};

/// The cgroup v2 mount.
pub const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// Runs the tunnel and its sandbox processes.
#[derive(Debug, Clone)]
pub struct Supervisor {
    /// This program, re-run for every child.
    pub program: PathBuf,
    /// Arguments for the `tunnel` subcommand.
    pub tunnel_args: Vec<String>,
    /// The tunnel's admin base URL.
    pub admin: String,
    /// Shared with the load generator: the actors' sockets and the death log.
    pub shared: PathBuf,
    /// Response body size of the ingress actors.
    pub body_bytes: usize,
    /// CPU limit for the tunnel, in cores; zero for none.
    pub cpus: f64,
    /// Memory limit for the tunnel without swap; zero for none.
    pub mem_bytes: u64,
    /// Where cgroups are made.
    pub cgroup_root: PathBuf,
    /// Where the fake keeper listens.
    pub keeper_socket: PathBuf,
}

impl Supervisor {
    /// Serves the fake keeper and keeps a tunnel running until `cancel`.
    pub async fn run(&self, pki_dir: &Path, cancel: CancellationToken) -> Result<()> {
        let keeper = FakeKeeper::new(pki_dir)?;
        let (dir, socket, c) = (
            pki_dir.to_path_buf(),
            self.keeper_socket.clone(),
            cancel.clone(),
        );
        tokio::spawn(async move {
            if let Err(err) = keeper.serve(&dir, &socket, c).await {
                tracing::error!(%err, "fake keeper stopped");
            }
        });
        delegate_cgroups(&self.cgroup_root)
            .map_err(|e| Error::msg(format!("setting up cgroups: {e}")))?;
        let mut incarnation = 1;
        while !cancel.is_cancelled() {
            self.run_once(incarnation, &cancel).await?;
            incarnation += 1;
        }
        Ok(())
    }

    /// Runs one tunnel, with its sandbox processes, until it exits. Returns
    /// `Ok` when the tunnel died and should be replaced.
    async fn run_once(&self, incarnation: u32, cancel: &CancellationToken) -> Result<()> {
        let dir = self.cgroup_root.join(format!("tunnel-{incarnation}"));
        self.create_cgroup(&dir)?;
        let mut cmd = Command::new(&self.program);
        cmd.arg("tunnel").args(&self.tunnel_args).kill_on_drop(true);
        start_in_cgroup(&mut cmd, &dir)?;
        let mut child = cmd
            .spawn()
            .map_err(|e| Error::msg(format!("starting the tunnel: {e}")))?;

        let mut helpers: Vec<Child> = Vec::new();
        let startup = tokio::select! {
            r = self.wait_for_sandboxes(cancel) => r.and_then(|info| self.start_sandbox_processes(&info, &mut helpers)),
            status = child.wait() => {
                // The tunnel failed to start; its exit says why.
                stop_all(&mut helpers).await;
                if cancel.is_cancelled() {
                    return Ok(());
                }
                self.log_death(&dir, &format!("during startup: {}", describe(status)));
                return remove_cgroup(&dir).await;
            }
        };
        if let Err(e) = startup {
            let _ = child.start_kill();
            let _ = child.wait().await;
            stop_all(&mut helpers).await;
            return if cancel.is_cancelled() {
                Ok(())
            } else {
                Err(e)
            };
        }
        let status = tokio::select! {
            s = child.wait() => s,
            _ = cancel.cancelled() => {
                terminate(&child);
                child.wait().await
            }
        };
        stop_all(&mut helpers).await;
        if cancel.is_cancelled() {
            return Ok(());
        }
        self.log_death(&dir, &describe(status));
        remove_cgroup(&dir).await
    }

    /// Polls the tunnel's `/stats` until it hosts its Actors.
    async fn wait_for_sandboxes(&self, cancel: &CancellationToken) -> Result<Info> {
        let admin = Admin::http(&self.admin)?;
        loop {
            if let Ok((http::StatusCode::OK, body)) = admin.0.get("/stats").await {
                #[derive(serde::Deserialize)]
                struct Body {
                    tunnel: Option<Info>,
                }
                if let Ok(Body { tunnel: Some(info) }) = serde_json::from_slice(&body) {
                    if !info.sandboxes.is_empty() {
                        return Ok(info);
                    }
                }
            }
            tokio::select! {
                _ = cancel.cancelled() => return Err(Error::msg("canceled")),
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }
    }

    /// Places an ingress actor and an egress actor in every sandbox.
    fn start_sandbox_processes(&self, info: &Info, helpers: &mut Vec<Child>) -> Result<()> {
        let program = self.program.to_string_lossy().to_string();
        for (i, sb) in info.sandboxes.iter().enumerate() {
            let netns = sb.netns.to_string_lossy().to_string();
            let ingress = self
                .shared
                .join(format!("ingress-actor-{i}.sock"))
                .to_string_lossy()
                .to_string();
            let egress = self
                .shared
                .join(format!("egress-actor-{i}.sock"))
                .to_string_lossy()
                .to_string();
            let port = info.actor_port.to_string();
            let body = self.body_bytes.to_string();
            for args in [
                vec![
                    "ingress-actor",
                    "--port",
                    &port,
                    "--body-bytes",
                    &body,
                    "--stats-socket",
                    &ingress,
                ],
                vec!["egress-actor", "--socket", &egress],
            ] {
                let child = Command::new(&self.program)
                    .args(["netns-exec", &netns, &program])
                    .args(args)
                    .kill_on_drop(true)
                    .spawn()
                    .map_err(|e| Error::msg(format!("starting a sandbox process: {e}")))?;
                helpers.push(child);
            }
        }
        eprintln!(
            "worker: {} sandboxes, each with an ingress actor and an egress actor",
            info.sandboxes.len()
        );
        Ok(())
    }

    /// Records why the tunnel stopped, with its cgroup's OOM kills, in the
    /// shared death log.
    fn log_death(&self, dir: &Path, why: &str) {
        let line = format!(
            "{} the tunnel exited ({why}); OOM kills: {}\n",
            chrono::Local::now().format("%H:%M:%S"),
            oom_kills(dir)
        );
        eprint!("worker: {line}");
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.shared.join("tunnel-deaths.log"))
        {
            let _ = f.write_all(line.as_bytes());
        }
    }

    /// Makes a cgroup with the tunnel's limits.
    pub fn create_cgroup(&self, dir: &Path) -> io::Result<()> {
        match std::fs::create_dir(dir) {
            Err(e) if e.kind() != io::ErrorKind::AlreadyExists => return Err(e),
            _ => {}
        }
        if self.cpus > 0.0 {
            const PERIOD: u64 = 100_000;
            std::fs::write(
                dir.join("cpu.max"),
                format!("{} {PERIOD}", (self.cpus * PERIOD as f64) as u64),
            )?;
        }
        if self.mem_bytes > 0 {
            std::fs::write(dir.join("memory.max"), self.mem_bytes.to_string())?;
            // No swap, as in a Kubernetes Pod: over the limit is an OOM kill.
            match std::fs::write(dir.join("memory.swap.max"), "0") {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
        }
        Ok(())
    }
}

fn describe(status: io::Result<ExitStatus>) -> String {
    match status {
        Ok(s) => s.to_string(),
        Err(e) => e.to_string(),
    }
}

fn terminate(child: &Child) {
    if let Some(pid) = child.id() {
        // SAFETY: plain kill(2) on a child we started.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    }
}

async fn stop_all(helpers: &mut Vec<Child>) {
    for mut h in helpers.drain(..) {
        let _ = h.start_kill();
        let _ = h.wait().await;
    }
}

/// Makes the child join the cgroup at `dir` before it runs, so nothing it
/// does is counted elsewhere.
fn start_in_cgroup(cmd: &mut Command, dir: &Path) -> io::Result<()> {
    let procs =
        CString::new(dir.join("cgroup.procs").as_os_str().as_bytes()).map_err(io::Error::other)?;
    // SAFETY: the closure only calls async-signal-safe functions (open,
    // write, close) on memory prepared before fork.
    unsafe {
        cmd.pre_exec(move || {
            let fd = libc::open(procs.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            // "0" moves the writing process itself.
            let n = libc::write(fd, b"0".as_ptr().cast(), 1);
            libc::close(fd);
            if n != 1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}

/// Moves every process out of the root cgroup, so its controllers can be
/// handed to child cgroups (cgroup v2 puts processes only in leaves), and
/// enables the CPU and memory controllers for them.
pub fn delegate_cgroups(root: &Path) -> io::Result<()> {
    let leaf = root.join("supervisor");
    std::fs::create_dir_all(&leaf)?;
    let procs = std::fs::read_to_string(root.join("cgroup.procs"))?;
    for pid in procs.split_whitespace() {
        let _ = std::fs::write(leaf.join("cgroup.procs"), pid);
    }
    std::fs::write(root.join("cgroup.subtree_control"), "+cpu +memory")
}

/// The OOM kills `memory.events` in `dir` counts; zero without it.
pub fn oom_kills(dir: &Path) -> u64 {
    std::fs::read_to_string(dir.join("memory.events"))
        .ok()
        .and_then(|events| {
            events.lines().find_map(|l| {
                l.strip_prefix("oom_kill ")
                    .and_then(|v| v.trim().parse().ok())
            })
        })
        .unwrap_or(0)
}

/// Removes an emptied cgroup, waiting briefly for the kernel to finish
/// with the dead process.
pub async fn remove_cgroup(dir: &Path) -> Result<()> {
    let mut last = None;
    for _ in 0..50 {
        match std::fs::remove_dir(dir) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(Error::msg(format!(
        "removing {}: {}",
        dir.display(),
        last.map(|e| e.to_string()).unwrap_or_default()
    )))
}

/// Parses `512`, `4k`, `200m`, `1G` into bytes; empty is zero.
pub fn parse_bytes(s: &str) -> Result<u64> {
    if s.is_empty() {
        return Ok(0);
    }
    let (digits, mult) = match s.as_bytes()[s.len() - 1].to_ascii_lowercase() {
        b'k' => (&s[..s.len() - 1], 1u64 << 10),
        b'm' => (&s[..s.len() - 1], 1 << 20),
        b'g' => (&s[..s.len() - 1], 1 << 30),
        _ => (s, 1),
    };
    let n: u64 = digits
        .parse()
        .map_err(|_| Error::msg(format!("{s:?} is not a size")))?;
    Ok(n * mult)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bytes() {
        for (input, want) in [
            ("", 0),
            ("512", 512),
            ("4k", 4 << 10),
            ("200m", 200 << 20),
            ("1G", 1 << 30),
        ] {
            assert_eq!(parse_bytes(input).unwrap(), want, "{input:?}");
        }
        assert!(parse_bytes("lots").is_err(), "accepted a non-number");
    }

    fn supervisor(cpus: f64, mem_bytes: u64) -> Supervisor {
        Supervisor {
            program: PathBuf::new(),
            tunnel_args: Vec::new(),
            admin: String::new(),
            shared: PathBuf::new(),
            body_bytes: 0,
            cpus,
            mem_bytes,
            cgroup_root: PathBuf::new(),
            keeper_socket: PathBuf::new(),
        }
    }

    /// Writes the limits into a plain directory standing in for a cgroup.
    #[test]
    fn creates_cgroups() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("tunnel-1");
        supervisor(1.5, 1 << 30).create_cgroup(&dir).unwrap();
        for (file, want) in [
            ("cpu.max", "150000 100000"),
            ("memory.max", "1073741824"),
            ("memory.swap.max", "0"),
        ] {
            assert_eq!(
                std::fs::read_to_string(dir.join(file)).unwrap(),
                want,
                "{file}"
            );
        }
        let unlimited = tmp.path().join("tunnel-2");
        supervisor(0.0, 0).create_cgroup(&unlimited).unwrap();
        assert!(
            !unlimited.join("cpu.max").exists(),
            "cpu.max written with no CPU limit"
        );
    }

    #[test]
    fn counts_oom_kills() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(oom_kills(dir.path()), 0, "without memory.events");
        std::fs::write(
            dir.path().join("memory.events"),
            "low 0\nhigh 0\nmax 12\noom 3\noom_kill 2\noom_group_kill 0\n",
        )
        .unwrap();
        assert_eq!(oom_kills(dir.path()), 2);
    }
}

//! A cgroup of its own for each Actor's host processes (the VMM and
//! virtiofsd), so Actors on one worker share its CPU fairly and each stays
//! within its declared limit.
//!
//! Leaves live directly under the worker pod's delegated cgroup scope (see
//! [`hivekeep_sandbox_gvisor::cgroup::delegate`]).

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

pub use hivekeep_sandbox_gvisor::cgroup::{DEFAULT_CGROUP_ROOT, WORKER_LEAF};

/// The CFS period the quota is expressed against.
pub const CPU_PERIOD_US: i64 = 100_000;
/// The smallest quota the kernel accepts.
pub const CPU_MIN_QUOTA_US: i64 = 1000;

/// The cgroup v2 `cpu.max` value for a limit in millicores.
pub fn cpu_max(milli_cpu: i64) -> String {
    if milli_cpu <= 0 {
        return format!("max {CPU_PERIOD_US}");
    }
    let quota = (milli_cpu * CPU_PERIOD_US / 1000).max(CPU_MIN_QUOTA_US);
    format!("{quota} {CPU_PERIOD_US}")
}

/// The leaf directory of `actor_uid` under `root`. The uid becomes a path
/// element, so anything that could escape or collide is refused.
pub fn leaf_path(root: &Path, actor_uid: &str) -> io::Result<PathBuf> {
    if actor_uid.is_empty()
        || actor_uid == "."
        || actor_uid == ".."
        || actor_uid == WORKER_LEAF
        || actor_uid.contains(['/', '\0'])
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid actor cgroup name {actor_uid:?}"),
        ));
    }
    Ok(root.join(actor_uid))
}

/// Writes a cgroup interface file without creating it: cgroupfs refuses
/// creation with EACCES, which would hide a file that is simply absent.
fn write_existing(path: &Path, value: &str) -> io::Result<()> {
    let mut f = OpenOptions::new().write(true).truncate(true).open(path)?;
    f.write_all(value.as_bytes())
}

/// An open Actor leaf.
#[derive(Debug)]
pub struct ActorLeaf {
    path: PathBuf,
}

impl ActorLeaf {
    /// Creates the Actor's leaf under `root`, or reuses a leftover one, and
    /// caps it at `milli_cpu` (uncapped when 0). The cap is written even when
    /// unset, so a reused leaf does not keep an old one; a node that did not
    /// delegate the cpu controller has no `cpu.max` to write.
    pub fn open(root: &Path, actor_uid: &str, milli_cpu: i64) -> io::Result<ActorLeaf> {
        let path = leaf_path(root, actor_uid)?;
        match std::fs::create_dir(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("while creating cgroup {}: {e}", path.display()),
                ))
            }
        }
        match write_existing(&path.join("cpu.max"), &cpu_max(milli_cpu)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("while limiting cgroup {}: {e}", path.display()),
                ))
            }
        }
        Ok(ActorLeaf { path })
    }

    /// The leaf's directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Makes `cmd` start inside the leaf: the child moves itself into the
    /// leaf between fork and exec, before it can fork anything of its own.
    #[cfg(unix)]
    pub fn configure(&self, cmd: &mut tokio::process::Command) -> io::Result<()> {
        use std::os::fd::{AsRawFd, OwnedFd};
        let procs: OwnedFd = OpenOptions::new()
            .write(true)
            .open(self.path.join("cgroup.procs"))?
            .into();
        // SAFETY: the hook only calls write(2), which is async-signal-safe,
        // on a descriptor it owns for the lifetime of the closure.
        unsafe {
            cmd.pre_exec(move || {
                let fd = procs.as_raw_fd();
                if libc::write(fd, b"0".as_ptr().cast(), 1) != 1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(())
    }
}

/// Kills every process in the leaf, including any they forked, and waits
/// until it is empty. A missing leaf, or a kernel without `cgroup.kill`, has
/// nothing done.
pub async fn kill_leaf(
    root: &Path,
    name: &str,
    timeout: Duration,
    cancel: &CancellationToken,
) -> io::Result<()> {
    let path = leaf_path(root, name)?;
    match write_existing(&path.join("cgroup.kill"), "1") {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(io::Error::new(
                e.kind(),
                format!("while killing cgroup {}: {e}", path.display()),
            ))
        }
    }
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let events = std::fs::read_to_string(path.join("cgroup.events")).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("while waiting for cgroup {} to empty: {e}", path.display()),
            )
        })?;
        if events.contains("populated 0") {
            return Ok(());
        }
        tokio::select! {
            _ = cancel.cancelled() => {
                return Err(io::Error::new(io::ErrorKind::Interrupted, format!("while waiting for cgroup {} to empty: cancelled", path.display())))
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(io::Error::new(io::ErrorKind::TimedOut, format!("while waiting for cgroup {} to empty: timed out", path.display())))
            }
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
}

/// Deletes the Actor's leaf. Its processes must have exited.
pub fn remove_leaf(root: &Path, actor_uid: &str) -> io::Result<()> {
    let path = leaf_path(root, actor_uid)?;
    match std::fs::remove_dir(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io::Error::new(
            e.kind(),
            format!("while removing cgroup {}: {e}", path.display()),
        )),
    }
}

/// The cgroup v2 path in a `/proc/<pid>/cgroup` file (`0::<path>`). It reads
/// exactly `/` only inside a private cgroup namespace.
pub fn parse_cgroup_path(proc_cgroup: &str) -> io::Result<String> {
    proc_cgroup
        .trim()
        .lines()
        .find_map(|l| l.strip_prefix("0::"))
        .map(String::from)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "no cgroup v2 (0::) entry in /proc/self/cgroup",
            )
        })
}

/// Delegates the worker's cgroup scope after checking that it is a private
/// cgroup namespace: outside one `/sys/fs/cgroup` is the host's root, which
/// must not be carved up.
pub fn delegate(proc_self_cgroup: &Path, root: &Path) -> io::Result<()> {
    let content = std::fs::read_to_string(proc_self_cgroup)?;
    let path = parse_cgroup_path(&content)?;
    if path != "/" {
        return Err(io::Error::other(format!(
            "worker needs a private cgroup namespace (cgroup path {path:?}, want /): workers must run unprivileged"
        )));
    }
    hivekeep_sandbox_gvisor::cgroup::delegate(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_max_values() {
        for (milli, want) in [
            (0, "max 100000"),
            (500, "50000 100000"),
            (2000, "200000 100000"),
            (5, "1000 100000"),
        ] {
            assert_eq!(cpu_max(milli), want, "{milli}");
        }
    }

    #[test]
    fn reuses_and_resets_a_leftover_leaf() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("uid-a")).unwrap();
        std::fs::write(root.path().join("uid-a/cpu.max"), "").unwrap();
        for milli in [500, 0] {
            ActorLeaf::open(root.path(), "uid-a", milli).unwrap();
            assert_eq!(
                std::fs::read_to_string(root.path().join("uid-a/cpu.max")).unwrap(),
                cpu_max(milli)
            );
        }
        std::fs::remove_file(root.path().join("uid-a/cpu.max")).unwrap();
        for _ in 0..2 {
            remove_leaf(root.path(), "uid-a").unwrap();
        }
    }

    #[test]
    fn works_without_the_cpu_controller() {
        let root = tempfile::tempdir().unwrap();
        ActorLeaf::open(root.path(), "uid-a", 500).unwrap();
        assert!(!root.path().join("uid-a/cpu.max").exists());
    }

    #[test]
    fn rejects_unsafe_names() {
        for name in ["", ".", "..", "a/b", "../escape", WORKER_LEAF, "nul\0"] {
            let root = tempfile::tempdir().unwrap();
            assert!(ActorLeaf::open(root.path(), name, 0).is_err(), "{name:?}");
        }
    }

    #[test]
    fn parses_cgroup_paths() {
        for (input, want) in [
            ("0::/\n", Some("/")),
            (
                "0::/kubepods.slice/kubepods-pod1.slice/cri-containerd-abc.scope\n",
                Some("/kubepods.slice/kubepods-pod1.slice/cri-containerd-abc.scope"),
            ),
            ("12:pids:/\n0::/\n", Some("/")),
            ("12:pids:/\n11:memory:/\n", None),
        ] {
            assert_eq!(parse_cgroup_path(input).ok().as_deref(), want, "{input:?}");
        }
    }

    #[test]
    fn delegate_rejects_a_shared_namespace() {
        let d = tempfile::tempdir().unwrap();
        let proc_cgroup = d.path().join("cgroup");
        std::fs::write(
            &proc_cgroup,
            "0::/kubepods.slice/kubepods-pod1.slice/cri-containerd-abc.scope\n",
        )
        .unwrap();
        let root = d.path().join("root");
        std::fs::create_dir(&root).unwrap();
        assert!(delegate(&proc_cgroup, &root).is_err());
        assert!(!root.join(WORKER_LEAF).exists());
    }

    /// The current process's own cgroup, under which test leaves go.
    #[cfg(target_os = "linux")]
    fn own_cgroup() -> Option<PathBuf> {
        let p = parse_cgroup_path(&std::fs::read_to_string("/proc/self/cgroup").ok()?).ok()?;
        Some(Path::new(DEFAULT_CGROUP_ROOT).join(p.trim_start_matches('/')))
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[ignore = "needs root: creates cgroups"]
    async fn processes_start_inside_the_leaf() {
        let root = own_cgroup().unwrap();
        let leaf = ActorLeaf::open(&root, "cgroup-test", 500).unwrap();
        let mut cmd = tokio::process::Command::new("sleep");
        cmd.arg("30");
        leaf.configure(&mut cmd).unwrap();
        let mut child = cmd.spawn().unwrap();
        let got = std::fs::read_to_string(format!("/proc/{}/cgroup", child.id().unwrap())).unwrap();
        let _ = child.kill().await;
        let rel = root.join("cgroup-test");
        let rel = rel.strip_prefix(DEFAULT_CGROUP_ROOT).unwrap();
        assert!(got.contains(&format!("0::/{}", rel.display())), "{got}");
        remove_leaf(&root, "cgroup-test").unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[ignore = "needs root: creates cgroups"]
    async fn kill_empties_the_leaf() {
        let root = own_cgroup().unwrap();
        let leaf = ActorLeaf::open(&root, "cgroup-kill-test", 0).unwrap();
        let mut cmd = tokio::process::Command::new("sh");
        cmd.args(["-c", "sleep 30 & wait"]);
        leaf.configure(&mut cmd).unwrap();
        let mut child = cmd.spawn().unwrap();
        let cancel = CancellationToken::new();
        kill_leaf(&root, "cgroup-kill-test", Duration::from_secs(5), &cancel)
            .await
            .unwrap();
        let _ = child.wait().await;
        remove_leaf(&root, "cgroup-kill-test").unwrap();
        kill_leaf(&root, "cgroup-kill-test", Duration::from_secs(5), &cancel)
            .await
            .unwrap();
    }
}

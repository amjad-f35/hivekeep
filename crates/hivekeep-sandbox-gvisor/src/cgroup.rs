//! Operations on cgroup v2 leaves under the worker pod's delegated scope.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The worker pod's own cgroup scope. The worker runs in a private cgroup
/// namespace, so this is the pod's cgroup rather than the host root, and
/// runsc's per-container leaves are its direct children.
pub const DEFAULT_CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// The leaf the worker process itself moves into, so its parent scope may
/// delegate controllers to the sandbox leaves.
pub const WORKER_LEAF: &str = "worker";

/// The path of leaf `name` under `root`, refusing names that would escape it.
pub fn leaf_path(root: &Path, name: &str) -> io::Result<PathBuf> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name == WORKER_LEAF
        || name.contains(['/', '\0'])
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid actor cgroup name {name:?}"),
        ));
    }
    Ok(root.join(name))
}

/// Writes `value` to an existing control file; a missing file is `NotFound`.
pub fn write_existing(path: &Path, value: &str) -> io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)?;
    f.write_all(value.as_bytes())
}

/// Kills every process in leaf `name` under `root` with `cgroup.kill`, which
/// the systemd scope above ignores, and waits for the leaf to empty. A leaf
/// that does not exist is already dead.
pub async fn kill_leaf_under(root: &Path, name: &str, timeout: Duration) -> io::Result<()> {
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
    let wait = async {
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
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(timeout, wait).await.map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "while waiting for cgroup {} to empty: timed out",
                path.display()
            ),
        )
    })?
}

/// Prepares the pod scope so runsc can create sandbox leaves with real
/// accounting: the worker moves into its own leaf and the scope delegates the
/// cpu and memory controllers to its children, best effort per controller.
#[cfg(target_os = "linux")]
pub fn delegate(root: &Path) -> io::Result<()> {
    let worker = root.join(WORKER_LEAF);
    match std::fs::create_dir(&worker) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => {
            return Err(io::Error::new(
                e.kind(),
                format!("while creating {}: {e}", worker.display()),
            ))
        }
    }
    let procs = std::fs::read_to_string(root.join("cgroup.procs"))?;
    for pid in procs.split_whitespace() {
        // A process that exited meanwhile is fine.
        let _ = write_existing(&worker.join("cgroup.procs"), pid);
    }
    for controller in ["cpu", "memory", "pids"] {
        if let Err(e) = write_existing(
            &root.join("cgroup.subtree_control"),
            &format!("+{controller}"),
        ) {
            tracing::warn!(controller, err = %e, "Failed to delegate cgroup controller");
        }
    }
    Ok(())
}

/// Without cgroups there is nothing to delegate.
#[cfg(not(target_os = "linux"))]
pub fn delegate(_root: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_names_cannot_escape() {
        let root = Path::new("/cg");
        assert_eq!(
            leaf_path(root, "uid-_sandbox").unwrap(),
            PathBuf::from("/cg/uid-_sandbox")
        );
        for bad in ["", ".", "..", "a/b", WORKER_LEAF] {
            assert!(leaf_path(root, bad).is_err(), "{bad:?}");
        }
    }

    #[tokio::test]
    async fn kill_waits_for_the_leaf_to_empty() {
        let root = tempfile::tempdir().unwrap();
        // No leaf at all is already dead.
        kill_leaf_under(root.path(), "gone", Duration::from_secs(1))
            .await
            .unwrap();
        let leaf = root.path().join("uid-_sandbox");
        std::fs::create_dir(&leaf).unwrap();
        std::fs::write(leaf.join("cgroup.kill"), "").unwrap();
        std::fs::write(leaf.join("cgroup.events"), "populated 1\n").unwrap();
        assert!(
            kill_leaf_under(root.path(), "uid-_sandbox", Duration::from_millis(30))
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read_to_string(leaf.join("cgroup.kill")).unwrap(),
            "1"
        );
        std::fs::write(leaf.join("cgroup.events"), "populated 0\n").unwrap();
        kill_leaf_under(root.path(), "uid-_sandbox", Duration::from_secs(1))
            .await
            .unwrap();
    }
}

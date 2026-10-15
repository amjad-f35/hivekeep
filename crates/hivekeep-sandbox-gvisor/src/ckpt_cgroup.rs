//! Keeping checkpoint page cache out of the Actor's memory budget.
//!
//! While a checkpoint writes its image, the sentry moves from its sandbox
//! leaf (`<uid>-_sandbox`) into a sibling `<uid>-_ckpt` cgroup with no memory
//! limit and the leaf's `cpu.max`, so the freshly written pages neither evict
//! the Actor's clean pages nor throttle the sentry at `memory.high`. Removing
//! the sibling after teardown reparents the cache to the worker scope, where a
//! local resume finds it. After a restore, [`drop_actor_checkpoint_cache_async`]
//! releases the restore directory's pages once background loading is done.

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::future::BoxFuture;
use hivekeep_proto::worker::v1::ActorDirs;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::oci::{gvisor_cgroup_leaf, SANDBOX_ID};
use crate::sandbox_process::sandbox_pid;
use crate::snapshot_cache::{self, File};

/// Suffix of the checkpoint cgroup. The leading underscore, as in
/// `_sandbox`, keeps it apart from any DNS-label container leaf.
pub const CHECKPOINT_CGROUP_SUFFIX: &str = "-_ckpt";

/// Moves a pid into a cgroup. Replaceable so tests can model cgroup v2
/// migration on a plain directory.
pub type MovePid = Arc<dyn Fn(&Path, i32) -> io::Result<()> + Send + Sync>;

/// Writes `pid` into `dir/cgroup.procs`.
pub fn write_cgroup_pid(dir: &Path, pid: i32) -> io::Result<()> {
    std::fs::write(dir.join("cgroup.procs"), pid.to_string())
}

/// The default [`MovePid`].
pub fn default_move_pid() -> MovePid {
    Arc::new(write_cgroup_pid)
}

/// The checkpoint cgroup of `actor_uid` under `root`.
pub fn checkpoint_cgroup_dir(root: &Path, actor_uid: &str) -> PathBuf {
    root.join(format!("{actor_uid}{CHECKPOINT_CGROUP_SUFFIX}"))
}

/// A sentry moved into its checkpoint cgroup.
pub struct CheckpointCgroup {
    /// The sandbox leaf it came from.
    pub leaf: PathBuf,
    /// The checkpoint cgroup.
    pub dir: PathBuf,
    /// The sentry.
    pub pid: i32,
    move_pid: MovePid,
}

fn cgroup_has_pid(dir: &Path, pid: i32) -> bool {
    std::fs::read_to_string(dir.join("cgroup.procs"))
        .map(|s| s.split_whitespace().any(|p| p == pid.to_string()))
        .unwrap_or(false)
}

/// Moves the Actor's sentry from its sandbox leaf into the checkpoint cgroup,
/// copying the leaf's `cpu.max` so serialization stays within the Actor's
/// CPU limit. Refuses a pid that is not in the sandbox leaf, so a reused pid
/// is never moved.
pub fn isolate_checkpoint_cache(
    root: &Path,
    actor_dirs: &ActorDirs,
    actor_uid: &str,
    move_pid: MovePid,
) -> io::Result<CheckpointCgroup> {
    let pid = sandbox_pid(actor_dirs)?;
    let leaf = root.join(gvisor_cgroup_leaf(actor_uid, SANDBOX_ID));
    if !cgroup_has_pid(&leaf, pid) {
        return Err(io::Error::other(format!(
            "sandbox pid {pid} is not in cgroup {}",
            leaf.display()
        )));
    }
    let dir = checkpoint_cgroup_dir(root, actor_uid);
    match std::fs::create_dir(&dir) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => {
            return Err(io::Error::new(
                e.kind(),
                format!("while creating cgroup {}: {e}", dir.display()),
            ))
        }
    }
    if let Ok(cpu_max) = std::fs::read(leaf.join("cpu.max")) {
        if let Err(e) = std::fs::write(dir.join("cpu.max"), cpu_max) {
            tracing::debug!(cgroup = %dir.display(), err = %e, "Failed to copy cpu.max to checkpoint cgroup");
        }
    }
    let c = CheckpointCgroup {
        leaf,
        dir,
        pid,
        move_pid,
    };
    if let Err(e) = (c.move_pid)(&c.dir, pid) {
        let e = io::Error::new(
            e.kind(),
            format!(
                "while moving PID {pid} into cgroup {}: {e}",
                c.dir.display()
            ),
        );
        return Err(match c.remove() {
            Ok(()) => e,
            Err(re) => io::Error::other(format!("{e}; {re}")),
        });
    }
    Ok(c)
}

impl CheckpointCgroup {
    /// Moves the sentry back into its sandbox leaf after a failed
    /// checkpoint. Succeeds once the sentry has exited; any other failure
    /// leaves it alive and unlimited, so the caller must terminate it.
    pub fn restore(&self) -> io::Result<()> {
        if !cgroup_has_pid(&self.dir, self.pid) {
            return Ok(());
        }
        match (self.move_pid)(&self.leaf, self.pid) {
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => Ok(()),
            other => other,
        }
    }

    /// Deletes the (empty) checkpoint cgroup, reparenting its page cache.
    pub fn remove(&self) -> io::Result<()> {
        match std::fs::remove_dir(&self.dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io::Error::new(
                e.kind(),
                format!("while removing cgroup {}: {e}", self.dir.display()),
            )),
        }
    }
}

/// Runs `checkpoint` with the sentry isolated, then `terminate`s the workload
/// before removing the checkpoint cgroup, so its cache reparents cleanly. If
/// the checkpoint fails, the sentry goes back to its sandbox leaf and the
/// workload stays up; if that rollback fails too, it is terminated rather than
/// left unlimited. Without a known sandbox pid it checkpoints unisolated.
pub async fn checkpoint_isolated<C, T, TF>(
    root: &Path,
    actor_uid: &str,
    actor_dirs: &ActorDirs,
    move_pid: MovePid,
    checkpoint: C,
    terminate: T,
) -> Result<()>
where
    C: Future<Output = Result<()>>,
    T: FnOnce() -> TF,
    TF: Future<Output = Result<()>>,
{
    let cg = match isolate_checkpoint_cache(root, actor_dirs, actor_uid, move_pid) {
        Ok(c) => Some(c),
        Err(e) => {
            tracing::warn!(actor_uid, err = %e, "Checkpointing without page cache isolation");
            None
        }
    };
    let result = checkpoint.await;
    let mut terminate_after = true;
    if result.is_err() {
        match cg.as_ref().map_or(Ok(()), CheckpointCgroup::restore) {
            Ok(()) => terminate_after = false,
            Err(e) => tracing::warn!(
                actor_uid,
                err = %e,
                "Failed to move the sentry back into its sandbox leaf; terminating workload"
            ),
        }
    }
    if terminate_after {
        // Failures are logged by the caller's terminate.
        let _ = terminate().await;
    }
    if let Some(cg) = &cg {
        let mut removed = cg.remove();
        if removed.is_err() {
            // Terminate left the sentry alive: back under its memory limit.
            match cg.restore() {
                Ok(()) => removed = cg.remove(),
                Err(e) => {
                    tracing::warn!(actor_uid, err = %e, "Failed to move the sentry back into its sandbox leaf")
                }
            }
        }
        if let Err(e) = removed {
            tracing::warn!(actor_uid, err = %e, "Failed to remove the checkpoint cgroup");
        }
    }
    result
}

/// Removes the empty checkpoint cgroups a previous worker process left.
pub fn remove_stale_checkpoint_cgroups(root: &Path) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for e in entries.flatten() {
        if !e
            .file_name()
            .to_string_lossy()
            .ends_with(CHECKPOINT_CGROUP_SUFFIX)
        {
            continue;
        }
        if let Err(err) = std::fs::remove_dir(e.path()) {
            if err.kind() != io::ErrorKind::NotFound {
                tracing::warn!(cgroup = %e.path().display(), err = %err, "Failed to remove a stale checkpoint cgroup");
            }
        }
    }
}

/// Whether `name` is snapshot data whose pages are worth releasing.
pub fn is_checkpoint_data_file(name: &str) -> bool {
    name.ends_with(".img") || name.ends_with(".tar")
}

/// Waits for background restore page loading, bounded by the token.
pub type WaitRestore = Box<dyn FnOnce(CancellationToken) -> BoxFuture<'static, Result<()>> + Send>;

/// Evicts files from the page cache. Replaceable so tests can observe it.
pub type Evict = Arc<dyn Fn(Vec<File>) + Send + Sync>;

/// The default [`Evict`]: evict with retries.
pub fn default_evict() -> Evict {
    Arc::new(|files| snapshot_cache::evict_with_retries(&files))
}

/// Releases the restore directory's snapshot data once the sandbox is done
/// reading it. Files are found now, while the caller holds the Actor's lock.
///
/// An unpreserved directory whose files nothing else links is unlinked at
/// once: the kernel frees the pages when the sentry closes them. Otherwise
/// (the directory is preserved, or a file is also linked from the keeper's
/// snapshot cache, so unlinking frees nothing) this waits in the background,
/// for no longer than `actor` lives, for page loading to finish, evicts the
/// clean pages, and unlinks the files unless the directory is preserved.
pub fn drop_actor_checkpoint_cache_async(
    actor: CancellationToken,
    actor_uid: &str,
    restore_dir: &Path,
    preserve_restore_dir: bool,
    wait_restore: Option<WaitRestore>,
    evict: Evict,
) {
    let targets = snapshot_cache::find_files(restore_dir, is_checkpoint_data_file);
    if targets.is_empty() {
        return;
    }
    if !preserve_restore_dir && !targets.iter().any(|f| f.nlink > 1) {
        snapshot_cache::drop_files(&targets);
        return;
    }
    let actor_uid = actor_uid.to_string();
    tokio::spawn(async move {
        if let Some(wait) = wait_restore {
            let waited = tokio::select! {
                r = wait(actor.clone()) => r,
                _ = actor.cancelled() => Err(Error::Cancelled),
            };
            if let Err(e) = waited {
                tracing::warn!(
                    actor_uid,
                    err = %e,
                    "Skipping checkpoint page cache eviction after restore wait error"
                );
                if !preserve_restore_dir {
                    snapshot_cache::drop_files(&targets);
                }
                return;
            }
        }
        let t = targets.clone();
        let _ = tokio::task::spawn_blocking(move || evict(t)).await;
        if !preserve_restore_dir {
            snapshot_cache::drop_files(&targets);
        }
    });
}

#[cfg(test)]
mod tests;

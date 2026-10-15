//! Sandbox memory limits enforced by throttling and a watch, never the OOM
//! killer (see [`crate::memory_watch`]).

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use crate::cgroup;
use crate::error::{Error, Result};
use crate::hosted::HostedActor;
use crate::memory_watch::{swap_memory_max_for_high, MemoryWatch};
use crate::service::WorkerService;

/// How often sandbox leaves are checked.
pub const MEMORY_WATCH_INTERVAL: Duration = Duration::from_millis(250);

/// How long a kill may take to empty a leaf.
const KILL_TIMEOUT: Duration = Duration::from_secs(10);

impl WorkerService {
    /// Swaps the `memory.max` runsc set on the sandbox leaf for a
    /// `memory.high` at the same limit. runsc sizes the sentry from
    /// `memory.max` when the sandbox is created, so this runs after create.
    /// On failure the leaf keeps its `memory.max`.
    pub(crate) fn throttle_sandbox_memory(&self, hosted: &HostedActor) {
        if !self.cfg.throttle_memory {
            return;
        }
        match swap_memory_max_for_high(&self.sandbox_leaf(&hosted.attribution.uid)) {
            Ok(limit) => hosted.memory_limit.store(limit, Ordering::SeqCst),
            Err(e) => tracing::warn!(
                actor_uid = %hosted.attribution.uid,
                err = %e,
                "Sandbox keeps its memory.max, an OOM kill in it will restart the worker"
            ),
        }
    }

    /// Removes a sandbox leaf an earlier activation left when its `runsc
    /// delete` never ran. runsc joins an existing cgroup as is, so the new
    /// sandbox would get no limit and a sentry sized to the host.
    pub(crate) async fn remove_stale_sandbox_leaf(&self, uid: &str) -> Result<()> {
        let dir = self.sandbox_leaf(uid);
        let name = dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        cgroup::kill_leaf_under(&self.cfg.cgroup_root, &name, KILL_TIMEOUT)
            .await
            .map_err(|e| Error::io("while clearing a leftover sandbox cgroup", e))?;
        match std::fs::remove_dir(&dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::io(
                format!("while removing leftover sandbox cgroup {}", dir.display()),
                e,
            )),
        }
    }

    /// Kills sandboxes stuck over their limit until `cancel` fires.
    pub async fn watch_memory(self: Arc<Self>, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(MEMORY_WATCH_INTERVAL);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tick.tick() => {}
            }
            for h in self.hosted_actors() {
                self.check_memory(&h);
            }
        }
    }

    /// Kills `h`'s sandbox if the watch finds it stuck at its limit. The kill
    /// runs in the background, holding the watch until the sandbox is gone.
    pub(crate) fn check_memory(&self, h: &Arc<HostedActor>) {
        // Busy: being killed, or paused for a checkpoint.
        let Ok(mut watch) = h.memory_watch.clone().try_lock_owned() else {
            return;
        };
        let limit = h.memory_limit.load(Ordering::SeqCst);
        if limit == 0 || h.stopping.load(Ordering::SeqCst) || h.oom_killed.load(Ordering::SeqCst) {
            return;
        }
        let dir = self.sandbox_leaf(&h.attribution.uid);
        // A failed read means the sandbox is going away.
        let Ok((true, current)) = watch.check(&dir, limit, Instant::now()) else {
            return;
        };
        h.oom_killed.store(true, Ordering::SeqCst);
        let uid = h.attribution.uid.clone();
        tracing::warn!(actor_uid = %uid, memory_limit_bytes = limit, memory_usage_bytes = current, "Killing a sandbox over its memory limit");
        let root = self.cfg.cgroup_root.clone();
        let name = dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        tokio::spawn(async move {
            let _held = watch;
            if let Err(e) = cgroup::kill_leaf_under(&root, &name, KILL_TIMEOUT).await {
                tracing::error!(actor_uid = %uid, err = %e, "Failed to kill a sandbox over its memory limit");
            }
        });
    }
}

/// Stops watching `h`'s leaf (its sentry is moving to the checkpoint
/// cgroup), waiting out a kill in progress. Returns the limit to resume with.
pub async fn pause_memory_watch(h: &HostedActor) -> u64 {
    let _g = h.memory_watch.lock().await;
    h.memory_limit.swap(0, Ordering::SeqCst)
}

/// Watches `h`'s leaf again, from a fresh window.
pub async fn resume_memory_watch(h: &HostedActor, limit: u64) {
    let mut g = h.memory_watch.lock().await;
    *g = MemoryWatch::default();
    h.memory_limit.store(limit, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attribution::uid_only;
    use crate::service::ServiceConfig;

    fn service(root: &std::path::Path) -> Arc<WorkerService> {
        WorkerService::new(ServiceConfig {
            cgroup_root: root.to_path_buf(),
            throttle_memory: true,
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn removes_a_stale_leaf() {
        let root = tempfile::tempdir().unwrap();
        let s = service(root.path());
        let leaf = s.sandbox_leaf("uid-a");
        std::fs::create_dir(&leaf).unwrap();
        s.remove_stale_sandbox_leaf("uid-a").await.unwrap();
        assert!(!leaf.exists());
        s.remove_stale_sandbox_leaf("uid-a").await.unwrap();
    }

    /// A sandbox past the overshoot cap is killed once, and pausing the
    /// watch for a checkpoint waits for the kill.
    #[tokio::test]
    async fn kills_once_and_pauses_for_checkpoint() {
        let root = tempfile::tempdir().unwrap();
        let s = service(root.path());
        let h = Arc::new(HostedActor::new(uid_only("uid-a")));
        let leaf = s.sandbox_leaf("uid-a");
        std::fs::create_dir(&leaf).unwrap();
        const LIMIT: u64 = 256 << 20;
        std::fs::write(
            leaf.join("memory.current"),
            format!("{}\n", LIMIT + (32 << 20)),
        )
        .unwrap();
        std::fs::write(leaf.join("cgroup.kill"), "").unwrap();
        std::fs::write(leaf.join("cgroup.events"), "populated 0\nfrozen 0\n").unwrap();
        h.memory_limit.store(LIMIT, Ordering::SeqCst);

        s.check_memory(&h);
        assert!(h.oom_killed.load(Ordering::SeqCst));
        assert_eq!(pause_memory_watch(&h).await, LIMIT);
        assert_eq!(
            std::fs::read_to_string(leaf.join("cgroup.kill")).unwrap(),
            "1"
        );

        std::fs::write(leaf.join("cgroup.kill"), "").unwrap();
        resume_memory_watch(&h, LIMIT).await;
        s.check_memory(&h);
        pause_memory_watch(&h).await;
        assert_eq!(
            std::fs::read_to_string(leaf.join("cgroup.kill")).unwrap(),
            "",
            "killed twice"
        );
    }

    #[test]
    fn throttling_swaps_the_limit() {
        let root = tempfile::tempdir().unwrap();
        let s = service(root.path());
        let h = HostedActor::new(uid_only("uid-a"));
        let leaf = s.sandbox_leaf("uid-a");
        std::fs::create_dir(&leaf).unwrap();
        std::fs::write(leaf.join("memory.max"), "1048576\n").unwrap();
        s.throttle_sandbox_memory(&h);
        assert_eq!(h.memory_limit.load(Ordering::SeqCst), 1048576);
    }
}

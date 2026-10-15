//! Detecting and reporting sandboxes that die on their own.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use hivekeep_proto::worker::v1 as pb;

use crate::hosted::HostedActor;
use crate::service::{timeout_token, WorkerService, CLEANUP_TIMEOUT, CRASH_REPORT_TIMEOUT};

impl WorkerService {
    /// Watches the Actor's pinned sandbox. If it exits while nothing is
    /// stopping it, the Actor is torn down here (the control plane will not
    /// reach this worker for it again) and reported crashed.
    pub(crate) fn watch_sandbox(
        self: &Arc<Self>,
        hosted: Arc<HostedActor>,
        runsc_path: String,
        dirs: pb::ActorDirs,
        containers: Vec<pb::Container>,
    ) {
        let uid = hosted.attribution.uid.clone();
        let Some(sandbox) = hosted.sandbox() else {
            tracing::warn!(actor_uid = %uid, "Cannot watch the sandbox, a crash will go unreported: it was never pinned");
            return;
        };
        let pid = sandbox.pid;
        let exited = match sandbox.exited() {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(actor_uid = %uid, err = %e, "Cannot watch the sandbox, a crash will go unreported");
                return;
            }
        };
        let me = self.clone();
        tokio::spawn(async move {
            exited.await;
            if me.is_shutting_down() {
                return;
            }
            let message = if hosted.oom_killed.load(Ordering::SeqCst) {
                "killed for exceeding its memory limit"
            } else {
                "gVisor sandbox exited"
            };
            // `stopping` is checked under the Actor's lock, so an exit during
            // a checkpoint that then fails is still handled once it gives up.
            let ours = {
                let _lock = me.locks.lock(&uid).await;
                let ours = me.is_current(&hosted) && !hosted.stopping.load(Ordering::SeqCst);
                if ours {
                    tracing::warn!(actor_uid = %uid, pid, "Sandbox died");
                    let cleanup = timeout_token(CLEANUP_TIMEOUT);
                    if let Err(e) = me
                        .terminate_workload(
                            &hosted.attribution,
                            &runsc_path,
                            &dirs,
                            &containers,
                            &cleanup,
                        )
                        .await
                    {
                        tracing::warn!(actor_uid = %uid, err = %e, "Failed to tear down the dead sandbox");
                    }
                    // Not measured first: the sandbox and its cgroup are gone.
                    me.record_final_if_ended(Some(&hosted));
                }
                ours
            };
            // Reported outside the lock: a suspend in flight holds the
            // Actor's lease while it waits on the lock for its checkpoint.
            let Some(reporter) = me.cfg.crash_reporter.clone().filter(|_| ours) else {
                return;
            };
            match tokio::time::timeout(
                CRASH_REPORT_TIMEOUT,
                reporter.report_crashed(&hosted.attribution, message),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::error!(actor_uid = %uid, err = %e, "Failed to report the actor crashed")
                }
                Err(_) => {
                    tracing::error!(actor_uid = %uid, "Failed to report the actor crashed: timed out")
                }
            }
        });
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::actor_path::{pid_file_dir, pid_file_path};
    use crate::attribution::uid_only;
    use crate::error::Result;
    use crate::oci::SANDBOX_ID;
    use crate::ports::CrashReporter;
    use crate::service::ServiceConfig;
    use async_trait::async_trait;
    use hivekeep_core::resource::ActorAttribution;
    use std::time::Duration;

    struct Reports(tokio::sync::mpsc::UnboundedSender<String>);

    #[async_trait]
    impl CrashReporter for Reports {
        async fn report_crashed(&self, _: &ActorAttribution, message: &str) -> Result<()> {
            let _ = self.0.send(message.to_string());
            Ok(())
        }
    }

    /// Records a process that exits at once as the Actor's sandbox.
    fn exiting_sandbox(root: &std::path::Path) -> (std::process::Child, pb::ActorDirs) {
        let child = std::process::Command::new("true").spawn().unwrap();
        let dirs = pb::ActorDirs {
            root_dir: root.display().to_string(),
            ..Default::default()
        };
        std::fs::create_dir_all(pid_file_dir(&dirs)).unwrap();
        std::fs::write(pid_file_path(&dirs, SANDBOX_ID), child.id().to_string()).unwrap();
        (child, dirs)
    }

    async fn watch(setup: impl FnOnce(&WorkerService, &HostedActor)) -> Option<String> {
        let root = tempfile::tempdir().unwrap();
        let (mut child, dirs) = exiting_sandbox(root.path());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let svc = WorkerService::new(ServiceConfig {
            crash_reporter: Some(Arc::new(Reports(tx))),
            ..Default::default()
        });
        let hosted = Arc::new(HostedActor::new(uid_only("actor-a")));
        svc.pin_sandbox(&hosted, &dirs);
        assert!(hosted.sandbox().is_some());
        svc.table
            .write()
            .actors
            .insert("actor-a".into(), hosted.clone());
        setup(&svc, &hosted);
        svc.watch_sandbox(hosted, String::new(), dirs, Vec::new());
        let got = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .ok()
            .flatten();
        let _ = child.wait();
        got
    }

    #[tokio::test]
    async fn expected_exits_are_not_crashes() {
        assert_eq!(
            watch(|_, h| h.stopping.store(true, Ordering::SeqCst)).await,
            None
        );
        assert_eq!(
            watch(|s, _| s.shutting_down.store(true, Ordering::SeqCst)).await,
            None
        );
        assert_eq!(
            watch(|s, _| drop(s.table.write().actors.remove("actor-a"))).await,
            None
        );
    }

    #[tokio::test]
    async fn an_unexpected_exit_is_reported() {
        assert_eq!(
            watch(|_, _| {}).await.as_deref(),
            Some("gVisor sandbox exited")
        );
        assert_eq!(
            watch(|_, h| h.oom_killed.store(true, Ordering::SeqCst))
                .await
                .as_deref(),
            Some("killed for exceeding its memory limit")
        );
    }
}

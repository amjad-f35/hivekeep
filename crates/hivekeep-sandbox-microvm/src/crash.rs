//! Watching a VM for its VMM or virtiofsd dying while nothing is tearing it
//! down: the Actor is torn down here, since the control plane will not reach
//! this worker for it again, and then reported crashed.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_core::resource::ActorAttribution;
use hivekeep_proto::worker::v1 as pb;

use crate::child::Process;
use crate::service::{RunningActor, WorkerService};

/// How long a crash report keeps retrying.
pub const CRASH_REPORT_TIMEOUT: Duration = Duration::from_secs(300);

async fn exited(p: &Option<Arc<Process>>) -> String {
    match p {
        Some(p) => {
            p.done().await;
            p.exit()
        }
        None => futures::future::pending().await,
    }
}

impl WorkerService {
    /// Waits for the VM's processes; an exit nobody asked for tears the
    /// Actor down and reports it crashed.
    pub(crate) async fn watch_vm(
        self: Arc<Self>,
        a: ActorAttribution,
        dirs: pb::ActorDirs,
        ra: Arc<RunningActor>,
    ) {
        let message = tokio::select! {
            e = exited(&ra.ch_proc) => format!("cloud-hypervisor exited: {e}"),
            e = exited(&ra.vfsd_proc) => format!("virtiofsd exited: {e}"),
        };
        if ra.stopping.load(Ordering::SeqCst) || self.shutting_down.load(Ordering::SeqCst) {
            return;
        }
        tracing::warn!(actor_uid = %a.uid, reason = %message, "Micro-VM died");
        // Not reported under the lock: a suspend in flight holds the Actor's
        // lease while it waits on the lock for its checkpoint.
        let ours = {
            let _lock = self.locks.lock(&a.uid).await;
            let ours = self
                .running_vm(&a.uid)
                .is_some_and(|r| Arc::ptr_eq(&r, &ra))
                && !ra.stopping.load(Ordering::SeqCst);
            if ours {
                let hosted = self.lookup_actor(&a.uid);
                if let Err(e) = self.terminate(&a, &dirs).await {
                    tracing::warn!(actor_uid = %a.uid, err = %e, "Failed to tear down the dead micro-VM");
                }
                // Not measured first: the guest is gone.
                self.record_final_if_ended(hosted.as_ref());
            }
            ours
        };
        if !ours {
            return;
        }
        let Some(reporter) = self.cfg.crash_reporter.clone() else {
            return;
        };
        match tokio::time::timeout(CRASH_REPORT_TIMEOUT, reporter.report_crashed(&a, &message))
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!(actor_uid = %a.uid, err = %e, "Failed to report the actor crashed")
            }
            Err(_) => {
                tracing::error!(actor_uid = %a.uid, "Failed to report the actor crashed: timed out")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::{HostedActor, ServiceConfig};
    use async_trait::async_trait;
    use hivekeep_sandbox_gvisor::attribution::uid_only;
    use hivekeep_sandbox_gvisor::ports::CrashReporter;
    use tokio::process::Command;

    struct Reports(tokio::sync::mpsc::UnboundedSender<String>);

    #[async_trait]
    impl CrashReporter for Reports {
        async fn report_crashed(
            &self,
            _: &ActorAttribution,
            message: &str,
        ) -> hivekeep_sandbox_gvisor::error::Result<()> {
            let _ = self.0.send(message.into());
            Ok(())
        }
    }

    /// A VM whose VMM exits at once and whose virtiofsd runs on.
    fn fake_vm() -> Arc<RunningActor> {
        let mut ch = Command::new("sh");
        ch.args(["-c", "exit 3"]);
        let mut vfsd = Command::new("sleep");
        vfsd.arg("30");
        Arc::new(RunningActor::with_processes(
            Some(Process::start(ch).unwrap()),
            Some(Process::start(vfsd).unwrap()),
        ))
    }

    fn service() -> (
        Arc<WorkerService>,
        tokio::sync::mpsc::UnboundedReceiver<String>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            WorkerService::new(ServiceConfig {
                crash_reporter: Some(Arc::new(Reports(tx))),
                ..Default::default()
            }),
            rx,
        )
    }

    #[tokio::test]
    async fn ignores_a_teardown() {
        let (s, mut rx) = service();
        let ra = fake_vm();
        ra.stopping.store(true, Ordering::SeqCst);
        s.watch_vm(uid_only("actor-a"), Default::default(), ra.clone())
            .await;
        assert!(rx.try_recv().is_err(), "reported a VM being torn down");
        ra.vfsd_proc.as_ref().unwrap().stop().await;
    }

    #[tokio::test]
    async fn ignores_a_replaced_vm() {
        let (s, mut rx) = service();
        // Hosted with a different VM than the one that dies.
        let h = HostedActor::new(
            uid_only("actor-a"),
            crate::activation::Activation::new(std::time::SystemTime::now(), false),
        );
        h.inner.lock().vm = Some(Arc::new(RunningActor::with_processes(None, None)));
        s.state.write().actors.insert("actor-a".into(), h);
        let ra = fake_vm();
        tokio::time::timeout(
            Duration::from_secs(10),
            s.clone()
                .watch_vm(uid_only("actor-a"), Default::default(), ra.clone()),
        )
        .await
        .expect("watch_vm did not return");
        assert!(
            rx.try_recv().is_err(),
            "reported a VM the actor no longer runs"
        );
        ra.vfsd_proc.as_ref().unwrap().stop().await;
    }

    #[tokio::test]
    async fn reports_its_own_vm_dying() {
        let (s, mut rx) = service();
        let h = HostedActor::new(
            uid_only("actor-a"),
            crate::activation::Activation::new(std::time::SystemTime::now(), false),
        );
        let ra = fake_vm();
        h.inner.lock().vm = Some(ra.clone());
        s.state.write().actors.insert("actor-a".into(), h);
        tokio::time::timeout(
            Duration::from_secs(10),
            s.clone()
                .watch_vm(uid_only("actor-a"), Default::default(), ra.clone()),
        )
        .await
        .unwrap();
        assert_eq!(
            rx.try_recv().unwrap(),
            "cloud-hypervisor exited: exit status 3"
        );
        assert!(
            s.lookup_actor("actor-a").is_none(),
            "the dead actor is still hosted"
        );
    }
}

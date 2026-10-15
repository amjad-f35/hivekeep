//! Graceful shutdown: SIGTERM into every sandbox, bounded by one deadline.

use std::sync::atomic::Ordering;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::actor_lock::InFlight;
use crate::cleanup::kill_container;
use crate::service::WorkerService;

/// Cancels every boot and restore in flight; checkpoints keep running.
pub fn cancel_startups(in_flight: &InFlight) {
    for uid in in_flight.cancel_startups() {
        tracing::info!(actor_uid = %uid, "Cancelling in-progress workload startup RPC due to graceful shutdown");
    }
}

impl WorkerService {
    /// Stops accepting workloads, cancels startups, lets checkpoints finish,
    /// then sends SIGTERM to every container and waits for them, escalating
    /// to SIGKILL. One deadline covers the whole drain, so waiting for RPCs
    /// and waiting out SIGTERM split a single grace period.
    pub async fn graceful_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        // Cancelling a startup is less disruptive than letting it finish and
        // then sending SIGTERM at once.
        cancel_startups(&self.in_flight);
        let deadline = Instant::now() + self.cfg.grace_period;
        let wait_cancel = CancellationToken::new();
        let wc = wait_cancel.clone();
        let timer = tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            wc.cancel();
        });
        if !self.in_flight.wait_idle(&wait_cancel).await {
            tracing::error!(rpcs = ?self.in_flight.names(), "Giving up waiting for in-flight RPCs during graceful shutdown");
        }
        timer.abort();
        let sessions = self.hosted_sessions();
        if sessions.is_empty() {
            tracing::info!("No active workload at shutdown; exiting cleanly");
            return;
        }
        // The kill ends every activation still hosted: read each while its
        // sandbox runs, and write its final record once it is gone.
        let hosted = self.hosted_actors();
        for h in &hosted {
            self.read_final(h).await;
        }
        let never = CancellationToken::new();
        let kills = sessions.iter().flat_map(|s| {
            let never = &never;
            s.containers.iter().map(move |name| async move {
                if let Err(e) = kill_container(&*s.runtime, name, deadline, self.cfg.container_kill_timeout, never).await {
                    tracing::warn!(container = %name, err = %e, "Failed to kill container during shutdown");
                }
            })
        });
        futures::future::join_all(kills).await;
        for h in &hosted {
            self.record_final(h);
        }
        tracing::info!("Shutting down");
    }
}

#[cfg(test)]
mod tests {
    use crate::actor_lock::Rpc;
    use crate::service::{ServiceConfig, WorkerService};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn waits_for_in_flight_checkpoints() {
        let s = WorkerService::new(ServiceConfig::default());
        let s2 = s.clone();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
        let holder = tokio::spawn(async move {
            let _g = s2.in_flight.add("actor-a", Rpc::Checkpoint, None);
            let _ = rx.recv().await;
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let s3 = s.clone();
        let shutdown = tokio::spawn(async move { s3.graceful_shutdown().await });
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(
            !shutdown.is_finished(),
            "returned while a checkpoint was in flight"
        );
        tx.send(()).await.unwrap();
        holder.await.unwrap();
        tokio::time::timeout(Duration::from_secs(30), shutdown)
            .await
            .unwrap()
            .unwrap();
        assert!(s.is_shutting_down());
    }

    #[test]
    fn cancels_every_startup() {
        let s = WorkerService::new(ServiceConfig::default());
        let a = CancellationToken::new();
        let b = CancellationToken::new();
        let _ga = s.in_flight.add("actor-a", Rpc::Run, Some(a.clone()));
        let _gb = s.in_flight.add("actor-b", Rpc::Restore, Some(b.clone()));
        super::cancel_startups(&s.in_flight);
        assert!(a.is_cancelled() && b.is_cancelled());
    }
}

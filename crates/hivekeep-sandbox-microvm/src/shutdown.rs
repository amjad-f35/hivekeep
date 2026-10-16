//! Graceful shutdown: SIGTERM into every guest's workloads, a shared grace
//! period, then SIGKILL. No lifecycle lock is taken, so a suspend can still
//! land mid-drain, which is what lets an Actor checkpoint on eviction.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_sandbox_gvisor::error::{Error, Result};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::boot::dial_agent_retry;
use crate::kata::agent::GuestProcesses;
use crate::kata::paths::vsock_socket_path;
use crate::service::{HostedActor, WorkerService};

/// The whole drain budget: waiting for in-flight RPCs and the workloads'
/// SIGTERM handling both draw on it. Shorter than the pod's termination
/// grace period, so the escalation happens here.
pub const WORKLOAD_GRACE_PERIOD: Duration = Duration::from_secs(30 * 60);
/// Bounds the wait after SIGKILL; the VM teardown is what finally
/// guarantees the workload is gone.
pub const WORKLOAD_KILL_TIMEOUT: Duration = Duration::from_secs(5);
/// Bounds one signal delivery: a local call that returns at once unless the
/// agent stopped answering (a paused guest, say).
pub const SIGNAL_DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

struct DrainTarget {
    id: String,
    agent: Option<Arc<dyn GuestProcesses>>,
    workload_ids: Vec<String>,
    hosted: Arc<HostedActor>,
}

/// Delivers `sig` to the workload's init (exec id equal to container id),
/// bounded by [`SIGNAL_DELIVERY_TIMEOUT`].
async fn signal_workload(agent: &dyn GuestProcesses, wid: &str, sig: i32) -> Result<()> {
    match tokio::time::timeout(
        SIGNAL_DELIVERY_TIMEOUT,
        agent.signal_process(wid, wid, sig as u32),
    )
    .await
    {
        Ok(r) => r.map_err(Error::from),
        Err(_) => Err(Error::DeadlineExceeded),
    }
}

/// Stops one workload: SIGTERM, wait until `deadline`, then SIGKILL and a
/// bounded wait. `deadline` is shared, so time spent elsewhere comes out of
/// the workload's grace. A wait that fails at the RPC layer says nothing
/// about the process (a dead or wedged agent), so it is reported and not
/// escalated.
pub async fn stop_guest_workload(
    agent: &dyn GuestProcesses,
    id: &str,
    wid: &str,
    deadline: Instant,
    kill_timeout: Duration,
    cancel: &CancellationToken,
) -> Result<()> {
    tracing::info!(id, workload = wid, "Sending SIGTERM to guest workload");
    signal_workload(agent, wid, libc::SIGTERM)
        .await
        .map_err(|e| Error::Context {
            context: format!("while propagating SIGTERM to workload {wid:?}"),
            source: Box::new(e),
        })?;
    // One wait feeds both phases, so the SIGKILL path picks up the exit the
    // SIGTERM path timed out on.
    let wait = agent.wait_process(wid, wid);
    tokio::pin!(wait);
    tokio::select! {
        r = &mut wait => {
            return match r {
                Ok(_) => {
                    tracing::info!(id, workload = wid, "Guest workload exited after SIGTERM");
                    Ok(())
                }
                Err(e) => Err(Error::Context {
                    context: format!("while waiting for workload {wid:?} to exit"),
                    source: Box::new(e.into()),
                }),
            };
        }
        _ = cancel.cancelled() => return Err(Error::Cancelled),
        _ = tokio::time::sleep_until(deadline) => {}
    }
    tracing::warn!(
        id,
        workload = wid,
        "Grace period expired; killing guest workload"
    );
    if let Err(e) = signal_workload(agent, wid, libc::SIGKILL).await {
        tracing::warn!(id, workload = wid, err = %e, "Failed to SIGKILL guest workload (it might have already exited)");
    }
    tokio::select! {
        _ = &mut wait => {
            tracing::info!(id, workload = wid, "Guest workload exited after SIGKILL");
            Ok(())
        }
        _ = cancel.cancelled() => Err(Error::Cancelled),
        _ = tokio::time::sleep(kill_timeout) => Err(Error::msg(format!(
            "workload {wid:?} failed to exit even after SIGKILL: context deadline exceeded"
        ))),
    }
}

impl WorkerService {
    /// Propagates SIGTERM into every running guest and waits for the
    /// workloads to exit, then returns so the caller can stop serving.
    pub async fn graceful_shutdown(self: &Arc<Self>) {
        // First, so an RPC arriving during the drain is turned away.
        self.shutting_down.store(true, Ordering::SeqCst);
        // Aborting a boot beats finishing it only to SIGTERM its guest.
        for uid in self.in_flight.cancel_startups() {
            tracing::info!(actor_uid = %uid, "Cancelling in-progress workload startup RPC due to graceful shutdown");
        }
        let deadline = Instant::now() + WORKLOAD_GRACE_PERIOD;
        // Checkpoints finish saving state before guests stop.
        let budget = CancellationToken::new();
        let b = budget.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            b.cancel();
        });
        if !self.in_flight.wait_idle(&budget).await {
            tracing::error!(rpcs = ?self.in_flight.names(), "Giving up waiting for in-flight RPCs during graceful shutdown");
        }
        let targets: Vec<DrainTarget> = self
            .hosted_actors()
            .into_iter()
            .filter_map(|h| {
                let vm = h.inner.lock().vm.clone()?;
                let agent = vm
                    .guest_agent
                    .lock()
                    .clone()
                    .map(|a| Arc::new(a) as Arc<dyn GuestProcesses>);
                Some(DrainTarget {
                    id: h.attribution.uid.clone(),
                    agent,
                    workload_ids: vm.workload_ids.clone(),
                    hosted: h,
                })
            })
            .collect();
        if targets.is_empty() {
            tracing::info!("No active actor sessions at shutdown; exiting cleanly");
            return;
        }
        // Concurrently: they share one deadline, so in series the last would
        // get no grace at all.
        let drains = targets.into_iter().map(|t| {
            let s = self.clone();
            async move {
                // The drain ends the activation: read it while the guest
                // answers, record it once the workloads are down.
                s.read_final(&t.hosted).await;
                gracefully_stop_actor(&t, deadline).await;
                s.record_final(&t.hosted);
            }
        });
        futures::future::join_all(drains).await;
        tracing::info!("Shutting down");
    }
}

async fn gracefully_stop_actor(t: &DrainTarget, deadline: Instant) {
    let cancel = CancellationToken::new();
    let agent = match &t.agent {
        Some(a) => a.clone(),
        None => match dial_agent_retry(&vsock_socket_path(&t.id), Duration::from_secs(15), &cancel)
            .await
        {
            Ok(a) => Arc::new(a) as Arc<dyn GuestProcesses>,
            Err(e) => {
                tracing::warn!(id = %t.id, err = %e, "Could not dial kata-agent for graceful stop; leaving the workload to pod teardown");
                return;
            }
        },
    };
    let stops = t.workload_ids.iter().map(|wid| {
        let (agent, cancel) = (agent.clone(), cancel.clone());
        async move {
            if let Err(e) = stop_guest_workload(agent.as_ref(), &t.id, wid, deadline, WORKLOAD_KILL_TIMEOUT, &cancel).await {
                tracing::warn!(id = %t.id, workload = %wid, err = %e, "Failed to stop guest workload during shutdown");
            }
        }
    });
    futures::future::join_all(stops).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kata::agent::AgentError;
    use crate::ttrpc::TtrpcError;
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    struct FakeGuest {
        signals: Mutex<Vec<i32>>,
        exit_on: i32,
        wait_err: bool,
        signal_err: HashMap<i32, String>,
        exited: tokio::sync::Notify,
        done: std::sync::atomic::AtomicBool,
    }

    impl FakeGuest {
        fn new(exit_on: i32, wait_err: bool, signal_err: &[(i32, &str)]) -> FakeGuest {
            FakeGuest {
                signals: Mutex::default(),
                exit_on,
                wait_err,
                signal_err: signal_err
                    .iter()
                    .map(|(s, m)| (*s, m.to_string()))
                    .collect(),
                exited: tokio::sync::Notify::new(),
                done: Default::default(),
            }
        }
    }

    fn agent_err(m: &str) -> AgentError {
        AgentError {
            method: "fake",
            source: TtrpcError::Codec(m.into()),
        }
    }

    #[async_trait]
    impl GuestProcesses for FakeGuest {
        async fn signal_process(
            &self,
            _: &str,
            _: &str,
            signal: u32,
        ) -> std::result::Result<(), AgentError> {
            let sig = signal as i32;
            self.signals.lock().push(sig);
            if self.exit_on != 0 && sig == self.exit_on {
                self.done.store(true, Ordering::SeqCst);
                self.exited.notify_waiters();
            }
            match self.signal_err.get(&sig) {
                Some(m) => Err(agent_err(m)),
                None => Ok(()),
            }
        }

        async fn wait_process(&self, _: &str, _: &str) -> std::result::Result<i32, AgentError> {
            loop {
                let n = self.exited.notified();
                if self.done.load(Ordering::SeqCst) {
                    break;
                }
                n.await;
            }
            if self.wait_err {
                return Err(agent_err("ttrpc: closed"));
            }
            Ok(0)
        }
    }

    #[tokio::test]
    async fn escalation() {
        let (term, kill) = (libc::SIGTERM, libc::SIGKILL);
        for (name, exit_on, wait_err, signal_err, deadline_in, want_signals, want_err) in [
            (
                "workload exits on SIGTERM inside the deadline",
                term,
                false,
                vec![],
                60_000i64,
                vec![term],
                None,
            ),
            (
                "workload ignoring SIGTERM is killed at the deadline",
                kill,
                false,
                vec![],
                20,
                vec![term, kill],
                None,
            ),
            (
                "deadline already spent leaves no grace",
                kill,
                false,
                vec![],
                -1000,
                vec![term, kill],
                None,
            ),
            (
                "workload surviving SIGKILL is reported",
                0,
                false,
                vec![],
                20,
                vec![term, kill],
                Some("failed to exit even after SIGKILL"),
            ),
            (
                "undeliverable SIGTERM does not escalate",
                0,
                false,
                vec![(term, "ttrpc closed")],
                60_000,
                vec![term],
                Some("while propagating SIGTERM to workload"),
            ),
            (
                "failed wait does not escalate",
                term,
                true,
                vec![],
                60_000,
                vec![term],
                Some(r#"while waiting for workload "counter" to exit"#),
            ),
        ] {
            let f = FakeGuest::new(exit_on, wait_err, &signal_err);
            let now = Instant::now();
            let deadline = if deadline_in >= 0 {
                now + Duration::from_millis(deadline_in as u64)
            } else {
                now.checked_sub(Duration::from_millis((-deadline_in) as u64))
                    .unwrap_or(now)
            };
            let r = stop_guest_workload(
                &f,
                "actor-1",
                "counter",
                deadline,
                Duration::from_millis(20),
                &CancellationToken::new(),
            )
            .await;
            match want_err {
                None => assert!(r.is_ok(), "{name}: {r:?}"),
                Some(w) => {
                    let e = r.unwrap_err().to_string();
                    assert!(e.contains(w), "{name}: {e}");
                }
            }
            assert_eq!(*f.signals.lock(), want_signals, "{name}");
        }
    }

    #[tokio::test]
    async fn honours_parent_cancellation() {
        let f = Arc::new(FakeGuest::new(0, false, &[]));
        let cancel = CancellationToken::new();
        let (f2, c2) = (f.clone(), cancel.clone());
        tokio::spawn(async move {
            while f2.signals.lock().is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            c2.cancel();
        });
        let r = stop_guest_workload(
            f.as_ref(),
            "actor-1",
            "counter",
            Instant::now() + Duration::from_secs(60),
            Duration::from_millis(20),
            &cancel,
        )
        .await;
        assert!(matches!(r, Err(Error::Cancelled)), "{r:?}");
        assert_eq!(*f.signals.lock(), [libc::SIGTERM]);
    }
}

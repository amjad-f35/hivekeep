//! A worker pod deleted (evicted) under a running Actor: SIGTERM reaches the
//! sandbox, the worker drains and is removed, and the Actor ends CRASHED, or
//! SUSPENDED when a suspend wins the race.

use std::sync::Arc;
use std::time::{Duration, Instant};

use hivekeep_e2e::clients::Clients;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::namespace::create_namespace;
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_proto::api::v1 as pb;
use tonic::Code;

use crate::helpers::*;

/// Polls ListWorkers until no worker names `pod`.
async fn wait_for_worker_removed(c: &Clients, pod: &str, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(workers) = list_workers(c).await {
            if !workers.iter().any(|w| w.worker_pod == pod) {
                return Ok(());
            }
        }
        if Instant::now() > deadline {
            return Err("deadline exceeded".into());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// A running Actor bound to a worker pod.
struct Bound {
    id: String,
    status: pb::ActorStatus,
    pod_namespace: String,
    pod: String,
}

/// Creates and resumes an Actor named after `prefix` and sets its SIGTERM
/// sleep.
async fn running_with_sigterm_sleep(
    t: &T,
    c: &Arc<Clients>,
    prefix: &str,
    sleep_seconds: u32,
) -> Bound {
    let ns = create_namespace(t).await;
    let at =
        create_actor_template(t, c, &ns, TemplateKind::Plain, pb::SnapshotFidelity::Memory).await;
    let id = format!("{prefix}-{}", ns.name);
    create_actor(t, c, &id, &at).await;
    delete_on_cleanup(t, c, demo_ref(&id));
    // Bound to a pod.
    if let Err(e) = resume(t, c, &demo_ref(&id)).await {
        t.fatal(format!("failed to resume Actor: {e}"));
    }
    wait_for_actor_state(t, c, &id, pb::ActorState::Running).await;
    if let Err(e) = call_actor_path(
        t,
        c,
        &demo_actor(&id),
        http::Method::GET,
        &format!("/set-sigterm-sleep?duration={sleep_seconds}"),
    )
    .await
    {
        t.fatal(format!("failed to set sigterm sleep: {e}"));
    }
    let running = get_actor(c, &demo_ref(&id))
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to get running Actor: {e}")));
    let st = status(&running);
    let wa = st.worker_assignment.clone().unwrap_or_default();
    if wa.worker_namespace.is_empty() || wa.worker_pod.is_empty() {
        t.fatal(format!(
            "running actor has no bound worker pod: ns={:?} name={:?}",
            wa.worker_namespace, wa.worker_pod
        ));
    }
    t.log(format!(
        "Actor {id:?} bound to worker pod {}/{}",
        wa.worker_namespace, wa.worker_pod
    ));
    Bound {
        id,
        status: st,
        pod_namespace: wa.worker_namespace,
        pod: wa.worker_pod,
    }
}

/// The propagated-SIGTERM eviction flow: the Actor's pod is deleted, the
/// worker is marked draining and then removed, and the Actor ends CRASHED.
/// The counter sleeps on SIGTERM like a workload waiting out a graceful
/// shutdown; saving state is the application's business, so only the control
/// plane's state machine is checked.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn graceful_worker_termination() {
    e2e_test("TestGracefulWorkerTermination", |t| async move {
        let c = clients();
        let Bound {
            id,
            pod_namespace: pod_ns,
            pod,
            ..
        } = running_with_sigterm_sleep(&t, &c, "graceful-term", 15).await;
        if let Err(e) = delete_pod(&c, &pod_ns, &pod).await {
            t.fatal(format!("failed to delete worker pod {pod_ns}/{pod}: {e}"));
        }
        if let Err(e) = wait_for_worker_removed(&c, &pod, Duration::from_secs(60)).await {
            t.fatal(format!("worker {pod} not removed after pod deletion: {e}"));
        }
        wait_for_actor_state(&t, &c, &id, pb::ActorState::Crashed).await;
        let a = get_actor(&c, &demo_ref(&id))
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to get actor: {e}")));
        if let Some(wa) = status(&a)
            .worker_assignment
            .filter(|w| !w.worker_pod.is_empty())
        {
            t.error(format!(
                "actor still bound to worker pod {:?}, expected empty",
                wa.worker_pod
            ));
        }
    });
}

/// A suspend issued while the evicted worker is in its SIGTERM shutdown
/// either completes before the deletion crashes the Actor, or reports that
/// concurrent crash without overwriting it.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn graceful_worker_termination_suspend() {
    e2e_test("TestGracefulWorkerTerminationSuspend", |t| async move {
        let c = clients();
        let Bound {
            id,
            status: running,
            pod_namespace: pod_ns,
            pod,
        } = running_with_sigterm_sleep(&t, &c, "graceful-term-suspend", 30).await;
        let previous = durable_snapshot_uri(&running);
        if previous.is_empty() {
            t.fatal("running actor has no completed durable snapshot");
        }
        if let Err(e) = delete_pod(&c, &pod_ns, &pod).await {
            t.fatal(format!("failed to delete worker pod {pod_ns}/{pod}: {e}"));
        }
        // Let SIGTERM land so the container is in its sleep.
        tokio::time::sleep(Duration::from_secs(2)).await;

        t.log(format!("Suspending actor {id:?} during termination"));
        let mut want = pb::ActorState::Suspended;
        match suspend(&c, &demo_ref(&id), pb::DurabilityMode::Synchronous).await {
            Ok(_) => {}
            Err(e) if e.code() == Code::FailedPrecondition || e.code() == Code::Aborted => {
                // The deletion can crash the Actor before the suspend reads it,
                // or between that read and its version-guarded update; either
                // way the crash stands and the suspend reports failure.
                want = pb::ActorState::Crashed;
                t.log(format!("Suspend reported concurrent worker deletion: {e}"));
            }
            Err(e) => t.fatal(format!("failed to suspend Actor: {e}")),
        }
        if let Err(e) = wait_for_worker_removed(&c, &pod, Duration::from_secs(60)).await {
            t.fatal(format!("worker {pod} not removed after pod deletion: {e}"));
        }
        // The RPC result and the stored state agree once the worker is gone.
        wait_for_actor_state(&t, &c, &id, want).await;

        let a = get_actor(&c, &demo_ref(&id))
            .await
            .unwrap_or_else(|e| t.fatal(format!("failed to get actor: {e}")));
        let st = status(&a);
        if let Some(wa) = st
            .worker_assignment
            .as_ref()
            .filter(|w| !w.worker_pod.is_empty())
        {
            t.error(format!(
                "actor still bound to worker pod {:?}, expected empty",
                wa.worker_pod
            ));
        }
        let got = durable_snapshot_uri(&st);
        if got.is_empty() {
            t.error("actor lost its completed durable snapshot");
        } else if want == pb::ActorState::Crashed && got != previous {
            t.error(format!(
                "crashed actor snapshot URI = {got:?}, want previous snapshot {previous:?}"
            ));
        }
        if want == pb::ActorState::Crashed && st.crash.as_ref().is_none_or(|c| c.message.is_empty())
        {
            t.error("crashed actor lost its crash reason");
        }
        if want == pb::ActorState::Suspended && st.crash.is_some() {
            t.error(format!(
                "successful suspend retained a crash record: {:?}",
                st.crash
            ));
        }
    });
}

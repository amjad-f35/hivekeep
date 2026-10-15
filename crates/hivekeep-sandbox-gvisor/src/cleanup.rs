//! Stopping and deleting an Actor's runsc containers.

use std::time::Duration;

use hivekeep_proto::worker::v1 as pb;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::oci::SANDBOX_ID;
use crate::runsc::ContainerRuntime;

/// Stops the application containers. The sandbox stays up: the others are
/// deleted through it before it goes last. `runsc kill` does not apply to a
/// sandbox with no root container.
pub async fn stop_containers(
    rt: &dyn ContainerRuntime,
    containers: &[pb::Container],
    cancel: &CancellationToken,
) {
    for c in containers {
        let _ = rt.kill(cancel, &c.name, "SIGKILL").await;
        let _ = rt.wait(cancel, &c.name).await;
    }
}

/// Whether runsc no longer has a record of `name`.
async fn already_gone(
    rt: &dyn ContainerRuntime,
    name: &str,
    cancel: &CancellationToken,
) -> Result<bool> {
    Ok(!rt.list(cancel).await?.iter().any(|n| n == name))
}

/// Deletes the application containers, then the sandbox. Each is checked
/// with `runsc state` first, as containerd does, without which `runsc
/// delete` occasionally fails; one runsc has no record of is skipped.
pub async fn cleanup_containers(
    rt: &dyn ContainerRuntime,
    containers: &[pb::Container],
    cancel: &CancellationToken,
) -> Result<()> {
    let names: Vec<&str> = containers
        .iter()
        .map(|c| c.name.as_str())
        .chain([SANDBOX_ID])
        .collect();
    let mut present = Vec::new();
    for name in names {
        if let Err(e) = rt.state(cancel, name).await {
            let e = Error::Context {
                context: format!("while checking state of {name:?} container"),
                source: Box::new(e),
            };
            match already_gone(rt, name, cancel).await {
                Err(list_err) => return Err(Error::Many(vec![e, list_err])),
                Ok(true) => {
                    tracing::info!(
                        container = name,
                        "runsc container already destroyed, skipping its cleanup"
                    );
                    continue;
                }
                Ok(false) => return Err(e),
            }
        }
        present.push(name);
    }
    for name in present {
        rt.delete(cancel, name).await.map_err(|e| Error::Context {
            context: format!("while deleting {name:?} container"),
            source: Box::new(e),
        })?;
    }
    Ok(())
}

/// Deletes `containers` in reverse creation order after a failed start.
pub async fn delete_containers(
    rt: &dyn ContainerRuntime,
    containers: &[String],
    operation: &str,
    cancel: &CancellationToken,
) {
    for c in containers.iter().rev() {
        if let Err(e) = rt.delete(cancel, c).await {
            tracing::warn!(operation, container = %c, err = %e, "Failed to delete runsc container after failure");
        }
    }
}

/// Stops a container: SIGTERM, a wait until `deadline` (the shared drain
/// deadline, so time already spent elsewhere comes out of the container's
/// grace), then SIGKILL and at most `kill_timeout` more. A failed SIGTERM or a
/// failed `runsc wait` is reported rather than escalated. `cancel` aborts the
/// drain: the containers go down with the pod.
pub async fn kill_container(
    rt: &dyn ContainerRuntime,
    name: &str,
    deadline: Instant,
    kill_timeout: Duration,
    cancel: &CancellationToken,
) -> Result<()> {
    tracing::info!(container = name, "Sending SIGTERM to container");
    if let Err(e) = rt.kill(cancel, name, "SIGTERM").await {
        tracing::error!(container = name, err = %e, "Failed to propagate SIGTERM to container");
        return Err(Error::msg(format!(
            "failed to propagate SIGTERM to container {name:?}: {e}"
        )));
    }
    let wait_cancel = cancel.child_token();
    let _stop_wait = wait_cancel.clone().drop_guard();
    let wait = rt.wait(&wait_cancel, name);
    tokio::pin!(wait);

    tokio::select! {
        biased;
        r = &mut wait => {
            return match r {
                Ok(()) => {
                    tracing::info!(container = name, "Container exited successfully");
                    Ok(())
                }
                Err(_) if cancel.is_cancelled() => Err(Error::Cancelled),
                Err(e) => Err(Error::msg(format!("wait failed: {e}"))),
            };
        }
        _ = cancel.cancelled() => return Err(Error::Cancelled),
        _ = tokio::time::sleep_until(deadline) => {}
    }

    tracing::warn!(container = name, "Grace period expired; killing container");
    if let Err(e) = rt.kill(cancel, name, "SIGKILL").await {
        tracing::warn!(container = name, err = %e, "Failed to send SIGKILL to container (it might have already exited)");
    }
    tokio::select! {
        biased;
        _ = &mut wait => {}
        _ = cancel.cancelled() => return Err(Error::Cancelled),
        _ = tokio::time::sleep(kill_timeout) => {
            return Err(Error::msg(format!(
                "container {name:?} failed to exit even after SIGKILL: context deadline exceeded"
            )));
        }
    }
    tracing::info!(container = name, "Container exited after SIGKILL");
    Ok(())
}

#[cfg(test)]
mod tests;

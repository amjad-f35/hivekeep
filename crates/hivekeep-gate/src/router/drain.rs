//! The shutdown sequence: flip readiness, wait for endpoints to drop the
//! pod, drain the dataplane, drain ext_proc (force-stopping past a
//! deadline), then stop everything else, and the drain-complete marker the
//! dataplane container's preStop hook waits on.
//!
//! The order is forced by the dataplane's ext_proc filter failing closed:
//! ext_proc must outlive the dataplane's drain, because every request the
//! dataplane still accepts while draining needs an answer from it.

use std::fmt::Display;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use super::health::Readiness;

/// Where the drain sequence leaves its completion marker; both containers
/// mount a shared emptyDir here.
pub const DEFAULT_DRAIN_COMPLETE_FILE: &str = "/var/run/gate/drain-complete";

/// Deletes a leftover marker at startup. The emptyDir survives container
/// restarts, and a stale marker would release the dataplane's preStop hook
/// the instant a later drain begins, before anything has drained.
pub fn remove_stale_drain_marker(path: &str) {
    if path.is_empty() {
        return;
    }
    if let Err(e) = std::fs::remove_file(path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(path, err = %e, "failed to remove stale drain-complete marker");
        }
    }
}

/// Creates the marker, releasing the dataplane's preStop hook. Failures are
/// logged only: the kubelet still bounds the hook, so a missing marker means
/// a slower exit, not a wedge.
pub fn write_drain_marker(path: &str) {
    if path.is_empty() {
        return;
    }
    if let Some(dir) = Path::new(path).parent() {
        if let Err(e) = std::fs::create_dir_all(dir) {
            tracing::warn!(path, err = %e, "failed to create drain-complete marker directory");
            return;
        }
    }
    let stamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    if let Err(e) = std::fs::write(path, format!("{stamp}\n")) {
        tracing::warn!(path, err = %e, "failed to write drain-complete marker");
        return;
    }
    tracing::info!(path, "drain-complete marker written");
}

/// A server the drain sequence can stop.
#[async_trait]
pub trait GrpcStopper: Send + Sync {
    /// Stops accepting connections and RPCs and resolves once every
    /// in-flight RPC (parked ext_proc streams, most of all) has finished.
    async fn graceful_stop(&self);
    /// Cancels in-flight RPCs and closes connections now.
    fn stop(&self);
}

/// Drains the dataplane proxy: established connections finish before it
/// returns.
#[async_trait]
pub trait DataplaneDrainer: Send + Sync {
    /// Drains within `window`; an error reports an incomplete drain, and
    /// shutdown continues regardless.
    async fn drain(&self, window: Duration) -> Result<(), String>;
}

/// A server task with a graceful shutdown signal and a hard abort, e.g. a
/// tonic server started with `serve_with_incoming_shutdown`.
pub struct ServeHandle {
    graceful: CancellationToken,
    finished: watch::Receiver<bool>,
    abort: AbortHandle,
}

impl ServeHandle {
    /// Spawns `serve`, handing it the future that resolves when a graceful
    /// stop begins.
    pub fn spawn<F, Fut, E>(serve: F) -> Self
    where
        F: FnOnce(WaitForCancellationFutureOwned) -> Fut,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Display,
    {
        let graceful = CancellationToken::new();
        let (tx, finished) = watch::channel(false);
        let fut = serve(graceful.clone().cancelled_owned());
        let task: JoinHandle<()> = tokio::spawn(async move {
            if let Err(e) = fut.await {
                tracing::error!(err = %e, "server stopped with an error");
            }
            let _ = tx.send(true);
        });
        ServeHandle {
            graceful,
            finished,
            abort: task.abort_handle(),
        }
    }

    /// Whether the server task has exited.
    pub fn is_finished(&self) -> bool {
        *self.finished.borrow()
    }
}

#[async_trait]
impl GrpcStopper for ServeHandle {
    async fn graceful_stop(&self) {
        self.graceful.cancel();
        let mut finished = self.finished.clone();
        // An error means the task was aborted: it is stopped either way.
        let _ = finished.wait_for(|f| *f).await;
    }

    fn stop(&self) {
        self.graceful.cancel();
        self.abort.abort();
    }
}

/// What the shutdown sequence drives.
pub struct DrainParams {
    /// Flipped to not-ready first so the Service stops sending connections.
    pub readiness: Arc<Readiness>,
    /// How long to keep serving after the flip while endpoints drop the pod.
    pub delay: Duration,
    /// Drained after the delay and before ext_proc; `None` when the
    /// dataplane manages its own termination.
    pub dataplane: Option<Arc<dyn DataplaneDrainer>>,
    /// Bound on the dataplane drain.
    pub dataplane_window: Duration,
    /// The ext_proc server.
    pub extproc: Arc<dyn GrpcStopper>,
    /// Bound on the ext_proc graceful drain (at least the parking budget, so
    /// parked requests finish normally).
    pub timeout: Duration,
    /// Stops the remaining subsystems (xDS, controllers, health, status).
    pub stop_rest: Box<dyn FnOnce() + Send>,
}

/// Waits for `shutdown` (SIGTERM or interrupt), then runs the sequence. The
/// returned task completes when the sequence does, so the caller can block
/// on it before flushing telemetry.
pub fn drain_on_shutdown(shutdown: CancellationToken, p: DrainParams) -> JoinHandle<()> {
    tokio::spawn(async move {
        shutdown.cancelled().await;
        tracing::info!("shutdown signal received; draining");
        p.readiness.mark_not_ready();
        tokio::time::sleep(p.delay).await;

        if let Some(dp) = &p.dataplane {
            tracing::info!(window = ?p.dataplane_window, "draining dataplane");
            // The drainer bounds itself; the outer bound only guards against
            // one that does not.
            let outer = p.dataplane_window + Duration::from_secs(1);
            match tokio::time::timeout(outer, dp.drain(p.dataplane_window)).await {
                Ok(Ok(())) => tracing::info!("dataplane drained"),
                Ok(Err(e)) => {
                    tracing::warn!(err = %e, "dataplane drain incomplete; continuing shutdown")
                }
                Err(_) => tracing::warn!("dataplane drain overran its window; continuing shutdown"),
            }
        }

        tracing::info!("starting ext_proc drain");
        if tokio::time::timeout(p.timeout, p.extproc.graceful_stop())
            .await
            .is_ok()
        {
            tracing::info!("ext_proc drain completed within deadline");
        } else {
            // In-flight streams, parked requests included, are cancelled.
            tracing::warn!("ext_proc drain deadline exceeded; forcing stop");
            p.extproc.stop();
        }
        (p.stop_rest)();
    })
}

#[cfg(test)]
#[path = "drain_tests.rs"]
mod tests;

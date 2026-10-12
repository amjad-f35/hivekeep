//! Connecting a server to its plugin sidecar over a Unix socket.
//!
//! This depends on no plugin implementation, so a server that only calls a
//! plugin does not link the plugin's storage clients. The connection is
//! unauthenticated: the socket is reachable only from inside the pod, and
//! only by its owner.

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use hyper_util::rt::TokioIo;
use tokio::net::{UnixListener, UnixStream};
use tonic::transport::{Channel, Endpoint};
use tonic::Status;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;
use tower::Service;

/// The ready wait servers pass to [`dial`]. It rides through a sidecar
/// restart, including the first 10 s step of a crash-loop back-off, and
/// fails a plugin that stays down well before a caller's deadline would.
pub const READY_WAIT: Duration = Duration::from_secs(30);

/// Reconnect back-off to the local socket: frequent, so a plugin that comes
/// back is reached within the ready wait.
const BACKOFF_BASE: Duration = Duration::from_millis(100);
const BACKOFF_MULTIPLIER: f64 = 1.6;
const BACKOFF_MAX: Duration = Duration::from_secs(1);

/// Removes any stale socket at `path` and listens on a fresh one that only
/// its owner can connect to.
pub fn listen(path: &Path) -> io::Result<UnixListener> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    if let Some(dir) = path.parent() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(|e| {
                io::Error::new(e.kind(), format!("while creating socket directory: {e}"))
            })?;
    }
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(io::Error::new(
                e.kind(),
                format!("while removing stale socket {}: {e}", path.display()),
            ))
        }
    }
    let lis = UnixListener::bind(path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("while listening on {}: {e}", path.display()),
        )
    })?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("while restricting socket {}: {e}", path.display()),
        )
    })?;
    Ok(lis)
}

/// The listener's connections as a stream for tonic's server.
pub fn incoming(lis: UnixListener) -> tokio_stream::wrappers::UnixListenerStream {
    tokio_stream::wrappers::UnixListenerStream::new(lis)
}

async fn connect_waiting(path: PathBuf, wait: Duration) -> io::Result<TokioIo<UnixStream>> {
    let start = Instant::now();
    let mut pause = BACKOFF_BASE;
    loop {
        match UnixStream::connect(&path).await {
            Ok(s) => return Ok(TokioIo::new(s)),
            Err(e) => {
                if start.elapsed() >= wait {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        format!(
                            "plugin at {} is not ready after {wait:?} ({e})",
                            path.display()
                        ),
                    ));
                }
                tokio::time::sleep(pause.min(wait.saturating_sub(start.elapsed()))).await;
                pause = Duration::from_secs_f64(
                    (pause.as_secs_f64() * BACKOFF_MULTIPLIER).min(BACKOFF_MAX.as_secs_f64()),
                );
            }
        }
    }
}

/// A long-lived connection to a plugin socket.
///
/// A call made while the plugin is unreachable waits up to the ready wait
/// for it to come back, then fails with `UNAVAILABLE`; a brief sidecar
/// restart is ridden through. A call that starts on a live connection is not
/// bounded by the ready wait, and one in flight when the plugin goes fails
/// with `UNAVAILABLE` at once. Callers bound a call by dropping it.
#[derive(Clone)]
pub struct PluginChannel {
    inner: Channel,
    target: String,
}

/// Dials the plugin at `path`; nothing is connected until the first call.
pub fn dial(path: &Path, ready_wait: Duration) -> Result<PluginChannel, tonic::transport::Error> {
    let p = path.to_path_buf();
    let inner = Endpoint::from_static("http://plugin.local").connect_with_connector_lazy(
        tower::service_fn(move |_| connect_waiting(p.clone(), ready_wait)),
    );
    Ok(PluginChannel {
        inner,
        target: format!("unix://{}", path.display()),
    })
}

impl PluginChannel {
    /// The socket this channel dials.
    pub fn target(&self) -> &str {
        &self.target
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn unavailable(target: &str, e: tonic::transport::Error) -> BoxError {
    let mut msg = e.to_string();
    let mut src = std::error::Error::source(&e);
    while let Some(s) = src {
        msg = format!("{msg}: {s}");
        src = s.source();
    }
    Box::new(Status::unavailable(format!("plugin at {target}: {msg}")))
}

impl Service<http::Request<tonic::body::Body>> for PluginChannel {
    type Response = http::Response<tonic::body::Body>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let target = self.target.clone();
        self.inner
            .poll_ready(cx)
            .map_err(|e| unavailable(&target, e))
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let target = self.target.clone();
        let fut = self.inner.call(req);
        Box::pin(async move { fut.await.map_err(|e| unavailable(&target, e)) })
    }
}

/// Waits until the plugin reports that it is serving, however long it takes
/// to come up; the caller bounds the wait by dropping the future, which is
/// what turns a plugin that never comes up into a startup error.
pub async fn wait_ready(conn: &PluginChannel) -> Result<(), Status> {
    let mut client = HealthClient::new(conn.clone());
    loop {
        match client
            .check(HealthCheckRequest {
                service: String::new(),
            })
            .await
        {
            Ok(resp) => {
                let st = resp.into_inner().status;
                if st == ServingStatus::Serving as i32 {
                    return Ok(());
                }
                return Err(Status::unavailable(format!(
                    "plugin at {} is {:?}",
                    conn.target(),
                    ServingStatus::try_from(st).unwrap_or(ServingStatus::Unknown)
                )));
            }
            Err(s) if s.code() == tonic::Code::Unavailable => {
                tokio::time::sleep(BACKOFF_BASE).await;
            }
            Err(s) => {
                return Err(Status::new(
                    s.code(),
                    format!(
                        "while waiting for the plugin at {}: {}",
                        conn.target(),
                        s.message()
                    ),
                ))
            }
        }
    }
}

/// The error a server reports to its own caller for a failed plugin call:
/// `UNAVAILABLE`, which a call reports when the plugin cannot be reached,
/// keeps its code so the caller retries; anything else is returned as is.
pub fn call_error(s: Status) -> Status {
    if s.code() == tonic::Code::Unavailable {
        return Status::unavailable(format!("plugin: {}", s.message()));
    }
    s
}

/// A health service reporting the default service as serving.
pub async fn serving_health(
) -> tonic_health::pb::health_server::HealthServer<impl tonic_health::pb::health_server::Health> {
    let (reporter, service) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    service
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::v1::control_provider_client::ControlProviderClient;
    use crate::proto::v1::control_provider_server::{ControlProvider, ControlProviderServer};
    use crate::proto::v1::*;
    use std::sync::Arc;
    use tokio::sync::{oneshot, Notify};

    #[test]
    fn call_error_keeps_unavailable() {
        let s = call_error(Status::unavailable("connection refused"));
        assert_eq!(s.code(), tonic::Code::Unavailable);
        for code in [
            tonic::Code::NotFound,
            tonic::Code::Internal,
            tonic::Code::FailedPrecondition,
        ] {
            let s = call_error(Status::new(code, "plugin error"));
            assert_eq!((s.code(), s.message()), (code, "plugin error"));
        }
    }

    /// A ControlProvider whose cleanup takes `delay` and announces each call.
    struct SlowControl {
        delay: Duration,
        started: Arc<Notify>,
    }

    #[tonic::async_trait]
    impl ControlProvider for SlowControl {
        async fn cleanup_snapshot(
            &self,
            _: tonic::Request<CleanupSnapshotRequest>,
        ) -> Result<tonic::Response<CleanupSnapshotResponse>, Status> {
            self.started.notify_one();
            tokio::time::sleep(self.delay).await;
            Ok(tonic::Response::new(CleanupSnapshotResponse {}))
        }
        async fn copy_snapshot(
            &self,
            _: tonic::Request<CopySnapshotRequest>,
        ) -> Result<tonic::Response<CopySnapshotResponse>, Status> {
            Err(Status::unimplemented("copy"))
        }
    }

    /// Serves a ControlProvider on a socket that can be stopped and started
    /// again on the same path, as a sidecar restart does. Each run has its
    /// own runtime, so stopping it drops every connection at once, as a
    /// crash does.
    struct Sidecar {
        _dir: tempfile::TempDir,
        path: PathBuf,
        delay: Duration,
        started: Arc<Notify>,
        stop:
            parking_lot::Mutex<Option<(std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>)>>,
    }

    impl Sidecar {
        fn new(delay: Duration) -> Arc<Self> {
            // Socket paths are length-limited; keep it short.
            let dir = tempfile::Builder::new()
                .prefix("plug")
                .tempdir_in("/tmp")
                .unwrap();
            let path = dir.path().join("p.sock");
            Arc::new(Sidecar {
                _dir: dir,
                path,
                delay,
                started: Arc::new(Notify::new()),
                stop: parking_lot::Mutex::new(None),
            })
        }

        async fn start(&self) {
            let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
            let (ready_tx, ready_rx) = oneshot::channel::<()>();
            let path = self.path.clone();
            let svc = ControlProviderServer::new(SlowControl {
                delay: self.delay,
                started: self.started.clone(),
            });
            let thread = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let lis = listen(&path).unwrap();
                    let health = serving_health().await;
                    tokio::spawn(async move {
                        let _ = tonic::transport::Server::builder()
                            .add_service(svc)
                            .add_service(health)
                            .serve_with_incoming(incoming(lis))
                            .await;
                    });
                });
                let _ = ready_tx.send(());
                let _ = stop_rx.recv();
                rt.shutdown_background();
            });
            ready_rx.await.unwrap();
            *self.stop.lock() = Some((stop_tx, thread));
        }

        async fn stop(&self) {
            let s = self.stop.lock().take();
            if let Some((tx, thread)) = s {
                let _ = tx.send(());
                tokio::task::spawn_blocking(move || thread.join())
                    .await
                    .unwrap()
                    .unwrap();
            }
            let _ = std::fs::remove_file(&self.path);
        }

        /// Stops the plugin and gives the client time to see the connection
        /// go, so the next call starts on a lost connection rather than
        /// racing the loss.
        async fn lose(&self) {
            self.stop().await;
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        async fn dial_ready(&self, wait: Duration) -> ControlProviderClient<PluginChannel> {
            let ch = dial(&self.path, wait).unwrap();
            tokio::time::timeout(Duration::from_secs(10), wait_ready(&ch))
                .await
                .expect("ready in time")
                .unwrap();
            ControlProviderClient::new(ch)
        }
    }

    fn cleanup_req() -> CleanupSnapshotRequest {
        CleanupSnapshotRequest {
            snapshot_uri: "gs://bucket/root/snap1".into(),
        }
    }

    #[tokio::test]
    async fn call_fails_promptly_after_plugin_loss() {
        let wait = Duration::from_millis(300);
        let s = Sidecar::new(Duration::ZERO);
        s.start().await;
        let mut c = s.dial_ready(wait).await;
        s.lose().await;
        let start = Instant::now();
        let err = tokio::time::timeout(Duration::from_secs(10), c.cleanup_snapshot(cleanup_req()))
            .await
            .expect("well before the caller's deadline")
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable, "{err}");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "failed after {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn call_wait_ends_with_caller_context() {
        let s = Sidecar::new(Duration::ZERO);
        s.start().await;
        let mut c = s.dial_ready(Duration::from_secs(60)).await;
        s.lose().await;
        let r = tokio::time::timeout(
            Duration::from_millis(200),
            c.cleanup_snapshot(cleanup_req()),
        )
        .await;
        assert!(
            r.is_err(),
            "the call ended before the caller's deadline: {r:?}"
        );
    }

    #[tokio::test]
    async fn slow_call_outlasts_ready_wait() {
        let wait = Duration::from_millis(100);
        let s = Sidecar::new(10 * wait);
        s.start().await;
        let mut c = s.dial_ready(wait).await;
        c.cleanup_snapshot(cleanup_req()).await.unwrap();
        s.stop().await;
    }

    #[tokio::test]
    async fn call_rides_through_plugin_restart() {
        let s = Sidecar::new(Duration::ZERO);
        s.start().await;
        let mut c = s.dial_ready(Duration::from_secs(10)).await;
        s.lose().await;
        let s2 = s.clone();
        let restart = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            s2.start().await;
        });
        tokio::time::timeout(Duration::from_secs(20), c.cleanup_snapshot(cleanup_req()))
            .await
            .unwrap()
            .unwrap();
        restart.await.unwrap();
        s.stop().await;
    }

    #[tokio::test]
    async fn in_flight_call_fails_when_plugin_dies() {
        let s = Sidecar::new(Duration::from_secs(60));
        s.start().await;
        let mut c = s.dial_ready(Duration::from_secs(60)).await;
        let call = tokio::spawn(async move { c.cleanup_snapshot(cleanup_req()).await });
        s.started.notified().await;
        let start = Instant::now();
        s.stop().await;
        let err = tokio::time::timeout(Duration::from_secs(30), call)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable, "{err}");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "failed {:?} after the plugin died",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn wait_ready_outlasts_ready_wait() {
        let wait = Duration::from_millis(100);
        let s = Sidecar::new(Duration::ZERO);
        let ch = dial(&s.path, wait).unwrap();
        let s2 = s.clone();
        let started = tokio::spawn(async move {
            tokio::time::sleep(10 * wait).await;
            s2.start().await;
        });
        tokio::time::timeout(Duration::from_secs(10), wait_ready(&ch))
            .await
            .unwrap()
            .unwrap();
        started.await.unwrap();
        s.stop().await;
    }

    #[tokio::test]
    async fn wait_ready_ends_with_context() {
        let dir = tempfile::tempdir().unwrap();
        let ch = dial(&dir.path().join("missing.sock"), READY_WAIT).unwrap();
        let r = tokio::time::timeout(Duration::from_millis(200), wait_ready(&ch)).await;
        assert!(
            r.is_err(),
            "wait_ready against a missing socket returned {r:?}"
        );
    }
}

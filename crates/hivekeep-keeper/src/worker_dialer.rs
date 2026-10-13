//! Connections to the node's sandbox workers, one Unix socket per worker
//! pod.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_proto::worker::v1 as wpb;
use parking_lot::Mutex;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::Status;

use crate::context::Ctx;
use crate::paths::NodePaths;

/// The Worker API of one worker pod.
#[async_trait]
pub trait WorkerApi: Send + Sync + 'static {
    /// Starts a workload.
    async fn run_workload(
        &self,
        ctx: &Ctx,
        req: wpb::RunWorkloadRequest,
    ) -> Result<wpb::RunWorkloadResponse, Status>;
    /// Checkpoints a workload and deletes it.
    async fn checkpoint_workload(
        &self,
        ctx: &Ctx,
        req: wpb::CheckpointWorkloadRequest,
    ) -> Result<wpb::CheckpointWorkloadResponse, Status>;
    /// Restores a workload from a snapshot.
    async fn restore_workload(
        &self,
        ctx: &Ctx,
        req: wpb::RestoreWorkloadRequest,
    ) -> Result<wpb::RestoreWorkloadResponse, Status>;
    /// Tears a workload down.
    async fn terminate_workload(
        &self,
        ctx: &Ctx,
        req: wpb::TerminateWorkloadRequest,
    ) -> Result<(), Status>;
    /// Samples every running workload's resource usage.
    async fn get_active_workload_stats(
        &self,
        ctx: &Ctx,
    ) -> Result<wpb::GetActiveWorkloadStatsResponse, Status>;
}

/// Opens Worker API clients by pod UID.
#[async_trait]
pub trait WorkerConnector: Send + Sync + 'static {
    /// The client of the worker pod `pod_uid`. Connecting is lazy: a worker
    /// that is gone fails the first call with Unavailable.
    async fn connect(&self, pod_uid: &str) -> Result<Arc<dyn WorkerApi>, Status>;
    /// The socket the worker pod serves on, whose presence is its liveness.
    fn socket_path(&self, pod_uid: &str) -> PathBuf;
}

/// A Worker API client over a gRPC channel.
#[derive(Clone)]
pub struct GrpcWorker {
    channel: Channel,
}

fn request<T>(ctx: &Ctx, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    if let Some(d) = ctx.deadline() {
        req.set_timeout(d.saturating_duration_since(tokio::time::Instant::now()));
    }
    req
}

async fn call<T, F>(ctx: &Ctx, f: F) -> Result<T, Status>
where
    F: std::future::Future<Output = Result<tonic::Response<T>, Status>>,
{
    match ctx.run(f).await {
        Ok(r) => r.map(|r| r.into_inner()),
        Err(c) => Err(c.into()),
    }
}

impl GrpcWorker {
    fn client(&self) -> wpb::worker_client::WorkerClient<Channel> {
        wpb::worker_client::WorkerClient::new(self.channel.clone())
    }
}

#[async_trait]
impl WorkerApi for GrpcWorker {
    async fn run_workload(
        &self,
        ctx: &Ctx,
        req: wpb::RunWorkloadRequest,
    ) -> Result<wpb::RunWorkloadResponse, Status> {
        call(ctx, self.client().run_workload(request(ctx, req))).await
    }

    async fn checkpoint_workload(
        &self,
        ctx: &Ctx,
        req: wpb::CheckpointWorkloadRequest,
    ) -> Result<wpb::CheckpointWorkloadResponse, Status> {
        call(ctx, self.client().checkpoint_workload(request(ctx, req))).await
    }

    async fn restore_workload(
        &self,
        ctx: &Ctx,
        req: wpb::RestoreWorkloadRequest,
    ) -> Result<wpb::RestoreWorkloadResponse, Status> {
        call(ctx, self.client().restore_workload(request(ctx, req))).await
    }

    async fn terminate_workload(
        &self,
        ctx: &Ctx,
        req: wpb::TerminateWorkloadRequest,
    ) -> Result<(), Status> {
        call(ctx, self.client().terminate_workload(request(ctx, req)))
            .await
            .map(|_| ())
    }

    async fn get_active_workload_stats(
        &self,
        ctx: &Ctx,
    ) -> Result<wpb::GetActiveWorkloadStatsResponse, Status> {
        call(
            ctx,
            self.client()
                .get_active_workload_stats(request(ctx, wpb::GetActiveWorkloadStatsRequest {})),
        )
        .await
    }
}

/// A lazy channel to a Unix socket.
pub fn unix_channel(path: PathBuf) -> Result<Channel, Status> {
    let ep =
        Endpoint::try_from("http://[::1]:50051").map_err(|e| Status::internal(e.to_string()))?;
    Ok(
        ep.connect_with_connector_lazy(tower::service_fn(move |_: Uri| {
            let path = path.clone();
            async move {
                let s = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(s))
            }
        })),
    )
}

/// Dials each worker pod's socket, keeping the most recently used
/// connections. A connection pushed out of the cache is dropped, which can
/// fail an RPC still in flight on it; that failure is retryable, unlike a
/// leak.
pub struct UnixWorkerDialer {
    paths: Arc<NodePaths>,
    capacity: usize,
    cache: Mutex<Lru>,
}

#[derive(Default)]
struct Lru {
    map: HashMap<String, Channel>,
    order: VecDeque<String>,
}

impl UnixWorkerDialer {
    /// A dialer caching up to `capacity` connections.
    pub fn new(paths: Arc<NodePaths>, capacity: usize) -> Self {
        UnixWorkerDialer {
            paths,
            capacity: capacity.max(1),
            cache: Mutex::new(Lru::default()),
        }
    }

    fn channel(&self, pod_uid: &str) -> Result<Channel, Status> {
        let mut c = self.cache.lock();
        if let Some(ch) = c.map.get(pod_uid).cloned() {
            c.order.retain(|k| k != pod_uid);
            c.order.push_back(pod_uid.to_string());
            return Ok(ch);
        }
        let ch = unix_channel(self.paths.worker_socket(pod_uid)).map_err(|e| {
            Status::internal(format!(
                "while creating the worker connection: {}",
                e.message()
            ))
        })?;
        c.map.insert(pod_uid.to_string(), ch.clone());
        c.order.push_back(pod_uid.to_string());
        while c.order.len() > self.capacity {
            if let Some(old) = c.order.pop_front() {
                c.map.remove(&old);
            }
        }
        Ok(ch)
    }
}

#[async_trait]
impl WorkerConnector for UnixWorkerDialer {
    async fn connect(&self, pod_uid: &str) -> Result<Arc<dyn WorkerApi>, Status> {
        let channel = self.channel(pod_uid).map_err(|e| {
            Status::new(
                e.code(),
                format!("while getting worker conn for {pod_uid}: {}", e.message()),
            )
        })?;
        Ok(Arc::new(GrpcWorker { channel }))
    }

    fn socket_path(&self, pod_uid: &str) -> PathBuf {
        self.paths.worker_socket(pod_uid)
    }
}

/// A failed call to a worker RPC. A worker that is unavailable (refusing
/// workloads until it registers, or gone in a restart) is reported as
/// Unavailable so the caller retries; anything else as Internal.
pub fn worker_call_error(rpc: &str, err: Status) -> Status {
    if err.code() == tonic::Code::Unavailable {
        return Status::unavailable(format!(
            "worker {rpc}: the worker is unavailable: {}",
            err.message()
        ));
    }
    Status::internal(format!("while calling worker {rpc}: {}", err.message()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dialer_caches_and_evicts() {
        let d = tempfile::tempdir().unwrap();
        let dialer = UnixWorkerDialer::new(Arc::new(NodePaths::new(d.path())), 2);
        for uid in ["a", "b", "a", "c"] {
            dialer.connect(uid).await.unwrap();
        }
        let c = dialer.cache.lock();
        assert_eq!(c.map.len(), 2);
        assert!(c.map.contains_key("a") && c.map.contains_key("c"));
    }

    #[tokio::test]
    async fn missing_worker_is_unavailable() {
        let d = tempfile::tempdir().unwrap();
        let dialer = UnixWorkerDialer::new(Arc::new(NodePaths::new(d.path())), 4);
        let w = dialer.connect("gone").await.unwrap();
        let err = w
            .get_active_workload_stats(&Ctx::background())
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn call_errors() {
        assert_eq!(
            worker_call_error("RunWorkload", Status::unavailable("x")).code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            worker_call_error("RunWorkload", Status::not_found("x")).code(),
            tonic::Code::Internal
        );
    }
}

#[cfg(test)]
#[path = "worker_dialer_tests.rs"]
mod behavior_tests;

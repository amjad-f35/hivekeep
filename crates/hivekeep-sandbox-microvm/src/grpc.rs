//! The `hivekeep.worker.v1.Worker` gRPC service over a [`WorkerService`].

use std::future::Future;
use std::sync::Arc;

use hivekeep_proto::worker::v1 as pb;
use hivekeep_proto::worker::v1::worker_server::Worker;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::service::WorkerService;

/// Serves a [`WorkerService`].
#[derive(Clone)]
pub struct WorkerApi(pub Arc<WorkerService>);

/// Runs a handler on its own task, so it finishes (and cleans up) even when
/// the caller goes away. With `cancel_on_drop`, the caller going away
/// cancels the handler's token.
async fn detached<T, F>(
    cancel_on_drop: bool,
    f: impl FnOnce(CancellationToken) -> F,
) -> Result<Response<T>, Status>
where
    T: Send + 'static,
    F: Future<Output = Result<T, Status>> + Send + 'static,
{
    let token = CancellationToken::new();
    let _guard = cancel_on_drop.then(|| token.clone().drop_guard());
    tokio::spawn(f(token))
        .await
        .map_err(|e| Status::internal(format!("handler failed: {e}")))?
        .map(Response::new)
}

#[tonic::async_trait]
impl Worker for WorkerApi {
    async fn run_workload(
        &self,
        req: Request<pb::RunWorkloadRequest>,
    ) -> Result<Response<pb::RunWorkloadResponse>, Status> {
        let s = self.0.clone();
        detached(true, move |c| async move {
            s.run_workload(req.into_inner(), c).await
        })
        .await
    }

    async fn checkpoint_workload(
        &self,
        req: Request<pb::CheckpointWorkloadRequest>,
    ) -> Result<Response<pb::CheckpointWorkloadResponse>, Status> {
        let s = self.0.clone();
        // Not cancelable: it may already have stopped the workload.
        detached(false, move |c| async move {
            s.checkpoint_workload(req.into_inner(), c).await
        })
        .await
    }

    async fn restore_workload(
        &self,
        req: Request<pb::RestoreWorkloadRequest>,
    ) -> Result<Response<pb::RestoreWorkloadResponse>, Status> {
        let s = self.0.clone();
        detached(true, move |c| async move {
            s.restore_workload(req.into_inner(), c).await
        })
        .await
    }

    async fn get_workload_stats(
        &self,
        req: Request<pb::GetWorkloadStatsRequest>,
    ) -> Result<Response<pb::GetWorkloadStatsResponse>, Status> {
        self.0
            .get_workload_stats(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_active_workload_stats(
        &self,
        _: Request<pb::GetActiveWorkloadStatsRequest>,
    ) -> Result<Response<pb::GetActiveWorkloadStatsResponse>, Status> {
        Ok(Response::new(self.0.get_active_workload_stats()))
    }

    async fn terminate_workload(
        &self,
        req: Request<pb::TerminateWorkloadRequest>,
    ) -> Result<Response<pb::TerminateWorkloadResponse>, Status> {
        let s = self.0.clone();
        detached(false, move |_| async move {
            s.terminate_workload(req.into_inner()).await
        })
        .await
    }
}

//! The Keeper gRPC server.
//!
//! Each RPC runs in a task of its own, so a caller that goes away cancels
//! the RPC's context instead of dropping the work mid-step: a Checkpoint
//! that has stopped the workload still commits its snapshot.

use std::future::Future;
use std::sync::Arc;

use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::keeper::v1::keeper_server;
use tonic::{Request, Response, Status};

use super::Keeper;
use crate::context::{request_ctx, CancelOnDrop, Ctx};

/// The Keeper service over a [`Keeper`].
#[derive(Clone)]
pub struct KeeperService {
    keeper: Arc<Keeper>,
}

impl KeeperService {
    /// Serves `keeper`.
    pub fn new(keeper: Arc<Keeper>) -> Self {
        KeeperService { keeper }
    }

    /// The tonic server for this service.
    pub fn into_server(self) -> keeper_server::KeeperServer<Self> {
        keeper_server::KeeperServer::new(self)
    }
}

/// Runs `f` detached under the request's context, cancelled if the caller
/// goes away.
async fn serve<T, R, F, Fut>(
    rpc: &'static str,
    keeper: Arc<Keeper>,
    req: Request<T>,
    f: F,
) -> Result<Response<R>, Status>
where
    T: Send + 'static,
    R: Send + 'static,
    F: FnOnce(Arc<Keeper>, Ctx, T) -> Fut + Send + 'static,
    Fut: Future<Output = Result<R, Status>> + Send + 'static,
{
    let ctx = request_ctx(&req);
    let _cancel = CancelOnDrop::new(ctx.clone());
    let msg = req.into_inner();
    let task = tokio::spawn(async move { f(keeper, ctx, msg).await });
    let res = task.await.map_err(crate::status::join_err)?;
    match &res {
        Ok(_) => tracing::debug!(rpc, "RPC succeeded"),
        Err(e) => tracing::info!(rpc, code = ?e.code(), err = %e.message(), "RPC failed"),
    }
    res.map(Response::new)
}

#[tonic::async_trait]
impl keeper_server::Keeper for KeeperService {
    async fn run(&self, req: Request<pb::RunRequest>) -> Result<Response<pb::RunResponse>, Status> {
        serve("Run", self.keeper.clone(), req, |k, ctx, m| async move {
            k.run(&ctx, m).await
        })
        .await
    }

    async fn checkpoint(
        &self,
        req: Request<pb::CheckpointRequest>,
    ) -> Result<Response<pb::CheckpointResponse>, Status> {
        serve(
            "Checkpoint",
            self.keeper.clone(),
            req,
            |k, ctx, m| async move { k.checkpoint(&ctx, m).await },
        )
        .await
    }

    async fn restore(
        &self,
        req: Request<pb::RestoreRequest>,
    ) -> Result<Response<pb::RestoreResponse>, Status> {
        serve(
            "Restore",
            self.keeper.clone(),
            req,
            |k, ctx, m| async move { k.restore(&ctx, m).await },
        )
        .await
    }

    async fn promote_snapshot(
        &self,
        req: Request<pb::PromoteSnapshotRequest>,
    ) -> Result<Response<pb::PromoteSnapshotResponse>, Status> {
        serve(
            "PromoteSnapshot",
            self.keeper.clone(),
            req,
            |k, ctx, m| async move { k.promote_snapshot(&ctx, m).await },
        )
        .await
    }

    async fn detach_actor(
        &self,
        req: Request<pb::DetachActorRequest>,
    ) -> Result<Response<pb::DetachActorResponse>, Status> {
        serve(
            "DetachActor",
            self.keeper.clone(),
            req,
            |k, ctx, m| async move { k.detach_actor(&ctx, m).await },
        )
        .await
    }

    async fn terminate(
        &self,
        req: Request<pb::TerminateRequest>,
    ) -> Result<Response<pb::TerminateResponse>, Status> {
        serve(
            "Terminate",
            self.keeper.clone(),
            req,
            |k, ctx, m| async move { k.terminate(&ctx, m).await },
        )
        .await
    }
}

//! The control plugin: whole-snapshot cleanup and copy for the control
//! plane.

use std::sync::Arc;

use hivekeep_core::snapshot::StoragePrefix;
use tonic::{Request, Response, Status};

use super::status::to_status;
use crate::proto::v1::control_provider_server::ControlProvider;
use crate::proto::v1::*;
use crate::store::{copy_prefix, delete_prefix, Store};

/// Serves ControlProvider on an object store.
pub struct ControlPlugin {
    store: Arc<dyn Store>,
}

impl ControlPlugin {
    /// A plugin backed by `store`.
    pub fn new(store: Arc<dyn Store>) -> Self {
        ControlPlugin { store }
    }

    /// Deletes every object under the request's prefix.
    pub async fn cleanup(&self, req: &CleanupSnapshotRequest) -> Result<(), Status> {
        let prefix = StoragePrefix::parse(&req.snapshot_uri)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        delete_prefix(self.store.as_ref(), &prefix)
            .await
            .map_err(to_status)
    }

    /// Copies every object of `src_uri` to `dst_uri`.
    pub async fn copy(&self, req: &CopySnapshotRequest) -> Result<(), Status> {
        let src = StoragePrefix::parse(&req.src_uri)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let dst = StoragePrefix::parse(&req.dst_uri)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        copy_prefix(self.store.as_ref(), &src, &dst)
            .await
            .map_err(to_status)
    }
}

#[tonic::async_trait]
impl ControlProvider for ControlPlugin {
    async fn cleanup_snapshot(
        &self,
        req: Request<CleanupSnapshotRequest>,
    ) -> Result<Response<CleanupSnapshotResponse>, Status> {
        self.cleanup(req.get_ref()).await?;
        Ok(Response::new(CleanupSnapshotResponse {}))
    }

    async fn copy_snapshot(
        &self,
        req: Request<CopySnapshotRequest>,
    ) -> Result<Response<CopySnapshotResponse>, Status> {
        self.copy(req.get_ref()).await?;
        Ok(Response::new(CopySnapshotResponse {}))
    }
}

//! Object storage as the keeper uses it: moving snapshot files and sandbox
//! assets between the node and a storage backend.
//!
//! The keeper never talks to a backend itself: an [`ObjectStore`] does, by
//! file path, with the per-actor JWT that authorizes a snapshot transfer.
//! Its errors are gRPC statuses whose codes the keeper reads (NotFound means
//! no such object, Unavailable is retried); their messages are never passed
//! on, since they may quote a signed URL.

use std::path::PathBuf;

use async_trait::async_trait;
use tonic::Status;

use crate::context::Ctx;

/// A request to download snapshot files.
#[derive(Debug, Clone)]
pub struct FetchSnapshot {
    /// The snapshot's object storage URI.
    pub snapshot_uri: String,
    /// The local directory to write the files into.
    pub write_path: PathBuf,
    /// The snapshot-relative file names to fetch.
    pub files: Vec<String>,
    /// Authorizes the transfer for the snapshot's actor.
    pub actor_jwt: String,
}

/// A request to upload snapshot files.
#[derive(Debug, Clone)]
pub struct UploadSnapshot {
    /// The snapshot's object storage URI.
    pub snapshot_uri: String,
    /// The local directory holding the files.
    pub local_path: PathBuf,
    /// The snapshot-relative file names to upload.
    pub files: Vec<String>,
    /// Authorizes the transfer for the snapshot's actor.
    pub actor_jwt: String,
}

/// A request to download one sandbox asset.
#[derive(Debug, Clone)]
pub struct FetchAsset {
    /// The asset's URI.
    pub asset_uri: String,
    /// The hex sha256 the asset must have.
    pub sha256: String,
    /// The existing file to write into.
    pub write_path: PathBuf,
    /// The largest size accepted.
    pub max_bytes: i64,
}

/// Object storage as the keeper uses it.
#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    /// Downloads snapshot files into a local directory. A missing object
    /// fails with NotFound.
    async fn fetch_snapshot(&self, ctx: &Ctx, req: FetchSnapshot) -> Result<(), Status>;
    /// Uploads snapshot files from a local directory, overwriting objects
    /// of the same name.
    async fn upload_snapshot(&self, ctx: &Ctx, req: UploadSnapshot) -> Result<(), Status>;
    /// Downloads one sandbox asset. Unimplemented when the store serves no
    /// assets; NotFound for a missing object; FailedPrecondition when the
    /// size or digest is wrong.
    async fn fetch_asset(&self, ctx: &Ctx, req: FetchAsset) -> Result<(), Status>;
}

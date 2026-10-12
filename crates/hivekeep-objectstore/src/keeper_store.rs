//! The keeper's [`ObjectStore`] on the object-store plugin: over the
//! plugin's Unix socket ([`PluginObjectStore`], the deployed shape), or with
//! the plugins in the keeper's own process ([`DirectObjectStore`], for
//! single-binary setups and tests).
//!
//! Statuses keep their codes, which the keeper reads: NOT_FOUND is a missing
//! object and UNAVAILABLE is retried.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hivekeep_node::context::Ctx;
use hivekeep_node::objectstore::{FetchAsset, FetchSnapshot, ObjectStore, UploadSnapshot};
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::plugin::{AssetPlugin, NodePlugin};
use crate::pluginsocket::{call_error, dial, wait_ready, PluginChannel, READY_WAIT};
use crate::proto::v1::asset_provider_client::AssetProviderClient;
use crate::proto::v1::node_provider_client::NodeProviderClient;
use crate::proto::v1::{FetchAssetRequest, FetchSnapshotRequest, UploadSnapshotRequest};

fn fetch_req(req: FetchSnapshot) -> FetchSnapshotRequest {
    FetchSnapshotRequest {
        snapshot_uri: req.snapshot_uri,
        actor_jwt: req.actor_jwt,
        write_path: req.write_path.display().to_string(),
        files: req.files,
    }
}

fn upload_req(req: UploadSnapshot) -> UploadSnapshotRequest {
    UploadSnapshotRequest {
        snapshot_uri: req.snapshot_uri,
        actor_jwt: req.actor_jwt,
        local_path: req.local_path.display().to_string(),
        files: req.files,
    }
}

fn asset_req(req: FetchAsset) -> FetchAssetRequest {
    FetchAssetRequest {
        asset_uri: req.asset_uri,
        sha256: req.sha256,
        write_path: req.write_path.display().to_string(),
        max_bytes: req.max_bytes,
    }
}

/// The keeper's object store over the node plugin's socket.
#[derive(Clone)]
pub struct PluginObjectStore {
    node: NodeProviderClient<PluginChannel>,
    asset: AssetProviderClient<PluginChannel>,
    channel: PluginChannel,
}

impl PluginObjectStore {
    /// A store over the plugin at `socket`, connecting on first use; each
    /// call waits up to `ready_wait` for a restarting plugin.
    pub fn new(socket: &Path, ready_wait: Duration) -> Result<Self, String> {
        let ch = dial(socket, ready_wait)
            .map_err(|e| format!("while dialing the object-store plugin: {e}"))?;
        Ok(PluginObjectStore {
            node: NodeProviderClient::new(ch.clone()),
            asset: AssetProviderClient::new(ch.clone()),
            channel: ch,
        })
    }

    /// A store over the plugin at `socket` with the default ready wait.
    pub fn connect(socket: &Path) -> Result<Self, String> {
        Self::new(socket, READY_WAIT)
    }

    /// Waits until the plugin reports it is serving; bound it with `ctx`.
    pub async fn wait_ready(&self, ctx: &Ctx) -> Result<(), Status> {
        ctx.run(wait_ready(&self.channel)).await??;
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for PluginObjectStore {
    async fn fetch_snapshot(&self, ctx: &Ctx, req: FetchSnapshot) -> Result<(), Status> {
        let mut c = self.node.clone();
        ctx.run(c.fetch_snapshot(fetch_req(req)))
            .await?
            .map(|_| ())
            .map_err(call_error)
    }

    async fn upload_snapshot(&self, ctx: &Ctx, req: UploadSnapshot) -> Result<(), Status> {
        let mut c = self.node.clone();
        ctx.run(c.upload_snapshot(upload_req(req)))
            .await?
            .map(|_| ())
            .map_err(call_error)
    }

    async fn fetch_asset(&self, ctx: &Ctx, req: FetchAsset) -> Result<(), Status> {
        let mut c = self.asset.clone();
        ctx.run(c.fetch_asset(asset_req(req)))
            .await?
            .map(|_| ())
            .map_err(call_error)
    }
}

/// The keeper's object store with the plugins in process.
pub struct DirectObjectStore {
    node: Arc<NodePlugin>,
    asset: Option<Arc<AssetPlugin>>,
}

impl DirectObjectStore {
    /// A store calling `node`, and `asset` for assets if set (assets are
    /// otherwise UNIMPLEMENTED).
    pub fn new(node: NodePlugin, asset: Option<AssetPlugin>) -> Self {
        DirectObjectStore {
            node: Arc::new(node),
            asset: asset.map(Arc::new),
        }
    }
}

/// A cancellation token firing when `ctx` ends.
fn token(ctx: &Ctx) -> CancellationToken {
    ctx.token().clone()
}

#[async_trait]
impl ObjectStore for DirectObjectStore {
    async fn fetch_snapshot(&self, ctx: &Ctx, req: FetchSnapshot) -> Result<(), Status> {
        let r = fetch_req(req);
        ctx.run(self.node.fetch_snapshot(&token(ctx), &r)).await?
    }

    async fn upload_snapshot(&self, ctx: &Ctx, req: UploadSnapshot) -> Result<(), Status> {
        let r = upload_req(req);
        ctx.run(self.node.upload_snapshot(&token(ctx), &r)).await?
    }

    async fn fetch_asset(&self, ctx: &Ctx, req: FetchAsset) -> Result<(), Status> {
        let Some(asset) = &self.asset else {
            return Err(Status::unimplemented("this object store serves no assets"));
        };
        let r = asset_req(req);
        ctx.run(asset.fetch(&token(ctx), &r)).await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::node::serve;
    use crate::storage::DEFAULT_UPLOAD_MEMORY;
    use crate::testing::MemObjects;
    use sha2::{Digest, Sha256};

    const URI: &str = "gs://bucket/root/hives/team-a/actors/uid1/snapshots/snap1";

    async fn round_trip(store: &dyn ObjectStore, root: &Path, backend: &MemObjects) {
        let ctx = Ctx::background();
        let src = root.join("src");
        let dst = root.join("dst");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::create_dir_all(&dst).unwrap();
        std::fs::write(src.join("memory.img"), b"pages".repeat(1000)).unwrap();
        std::fs::write(src.join("manifest.json"), b"{}").unwrap();
        store
            .upload_snapshot(
                &ctx,
                UploadSnapshot {
                    snapshot_uri: URI.into(),
                    local_path: src.clone(),
                    files: vec!["memory.img".into(), "manifest.json".into()],
                    actor_jwt: "jwt".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(backend.keys().len(), 2);
        store
            .fetch_snapshot(
                &ctx,
                FetchSnapshot {
                    snapshot_uri: URI.into(),
                    write_path: dst.clone(),
                    files: vec!["manifest.json".into(), "memory.img".into()],
                    actor_jwt: "jwt".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(dst.join("memory.img")).unwrap(),
            b"pages".repeat(1000)
        );
        let e = store
            .fetch_snapshot(
                &ctx,
                FetchSnapshot {
                    snapshot_uri: URI.into(),
                    write_path: dst,
                    files: vec!["absent.img".into()],
                    actor_jwt: "jwt".into(),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::NotFound);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn plugin_store_moves_snapshots_and_assets_over_the_socket() {
        let backend = Arc::new(MemObjects::new());
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let sock = tempfile::Builder::new()
            .prefix("kstore")
            .tempdir_in("/tmp")
            .unwrap();
        let path = sock.path().join("p.sock");
        let node = NodePlugin::new(backend.clone(), root.path(), DEFAULT_UPLOAD_MEMORY).unwrap();
        let asset =
            AssetPlugin::new(backend.clone(), "gs", root.path(), staging.path(), 1 << 20).unwrap();
        let (_ch, _h) = serve::spawn(&path, Some(node), None, Some(asset))
            .await
            .unwrap();
        let store = PluginObjectStore::connect(&path).unwrap();
        store
            .wait_ready(&Ctx::background().with_timeout(Duration::from_secs(10)))
            .await
            .unwrap();
        round_trip(&store, root.path(), &backend).await;

        backend.insert("bucket", "kata/vmlinux", &b"kernel"[..]);
        let dst = root.path().join("vmlinux");
        std::fs::write(&dst, "old").unwrap();
        store
            .fetch_asset(
                &Ctx::background(),
                FetchAsset {
                    asset_uri: "gs://bucket/kata/vmlinux".into(),
                    sha256: hex::encode(Sha256::digest(b"kernel")),
                    write_path: dst.clone(),
                    max_bytes: 1 << 20,
                },
            )
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"kernel");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn plugin_store_reports_a_lost_plugin_as_unavailable() {
        let sock = tempfile::Builder::new()
            .prefix("kstore")
            .tempdir_in("/tmp")
            .unwrap();
        let store =
            PluginObjectStore::new(&sock.path().join("absent.sock"), Duration::from_millis(200))
                .unwrap();
        let e = store
            .fetch_snapshot(
                &Ctx::background(),
                FetchSnapshot {
                    snapshot_uri: URI.into(),
                    write_path: sock.path().to_path_buf(),
                    files: vec!["manifest.json".into()],
                    actor_jwt: String::new(),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::Unavailable, "{e:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn direct_store_runs_the_plugins_in_process() {
        let backend = Arc::new(MemObjects::new());
        let root = tempfile::tempdir().unwrap();
        let node = NodePlugin::new(backend.clone(), root.path(), DEFAULT_UPLOAD_MEMORY).unwrap();
        let store = DirectObjectStore::new(node, None);
        round_trip(&store, root.path(), &backend).await;
        let e = store
            .fetch_asset(
                &Ctx::background(),
                FetchAsset {
                    asset_uri: "gs://bucket/kata/vmlinux".into(),
                    sha256: "0".repeat(64),
                    write_path: root.path().join("x"),
                    max_bytes: 1,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::Unimplemented);
    }
}

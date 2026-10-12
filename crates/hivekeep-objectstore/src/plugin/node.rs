//! The node plugin: snapshot transfers between the node and object storage.
//!
//! Snapshot files are stored as zstd-compressed objects named
//! `<file>.zstd` under the snapshot URI, except the manifest, which is
//! stored as it is.

use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::{Dir, DirBuilder, OpenOptions};
use futures::future::try_join_all;
use hivekeep_core::snapshot::{validate_snapshot_file_names, SnapshotUri};
use tokio_util::sync::CancellationToken;
use tonic::Status;

use super::credentials::{Ambient, Credentials, Transfer};
use super::status::to_status;
use crate::error::{Error, Result};
use crate::proto::v1::{FetchSnapshotRequest, UploadSnapshotRequest};
use crate::storage::uploader::SendContent;
use crate::storage::{fetch_file_with_zstd, fetch_object, ObjectStorage, Uploader};

/// The manifest, stored uncompressed so it reads like any earlier
/// snapshot's.
pub const MANIFEST_FILE: &str = "manifest.json";

/// Serves NodeProvider on an object storage client.
pub struct NodePlugin {
    creds: Arc<dyn Credentials>,
    uploader: Uploader,
    root: PathBuf,
}

fn clean(p: &Path) -> PathBuf {
    let mut out = PathBuf::from("/");
    for c in p.components() {
        match c {
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(s) => out.push(s),
            _ => {}
        }
    }
    out
}

impl NodePlugin {
    /// A plugin reading and writing local files only below `root`, sharing
    /// `upload_memory` bytes of reservations across uploads. `client` is the
    /// pod's own storage client: it sizes the budget and serves every
    /// transfer unless [`with_credentials`](Self::with_credentials) supplies
    /// a per-actor one.
    pub fn new(client: Arc<dyn ObjectStorage>, root: &Path, upload_memory: i64) -> Result<Self> {
        if !root.is_absolute() {
            return Err(Error::other(format!(
                "root {:?} is not an absolute path",
                root.display().to_string()
            )));
        }
        let uploader = Uploader::new(client.clone(), upload_memory)?;
        Ok(NodePlugin {
            creds: Arc::new(Ambient(client)),
            uploader,
            root: clean(root),
        })
    }

    /// Transfers run with the client `creds` supplies for each request's
    /// actor JWT, instead of the ambient one.
    pub fn with_credentials(mut self, creds: Arc<dyn Credentials>) -> Self {
        self.creds = creds;
        self
    }

    /// Replaces the credential source, for tests.
    pub fn set_credentials(&mut self, creds: Arc<dyn Credentials>) {
        self.creds = creds;
    }

    fn begin(&self, jwt: &str) -> Transfer {
        Transfer::new(self.creds.clone(), jwt, self.uploader.clone())
    }

    /// Checks a request's URI, local directory and file names, returning the
    /// parsed URI and the directory relative to the root.
    fn validate(
        &self,
        uri: &str,
        dir: &str,
        files: &[String],
    ) -> Result<(SnapshotUri, PathBuf), Status> {
        let uri = SnapshotUri::parse(uri).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let d = Path::new(dir);
        if !d.is_absolute() {
            return Err(Status::invalid_argument(format!(
                "local path {dir:?} is not absolute"
            )));
        }
        let rel = clean(d)
            .strip_prefix(&self.root)
            .map(Path::to_path_buf)
            .map_err(|_| {
                Status::invalid_argument(format!(
                    "local path {dir:?} is outside {:?}",
                    self.root.display().to_string()
                ))
            })?;
        if files.is_empty() {
            return Err(Status::invalid_argument("no files requested"));
        }
        validate_snapshot_file_names(files).map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok((uri, rel))
    }

    /// Opens `rel` below the root, so a symlink anywhere on the path cannot
    /// lead outside it.
    fn open_dir(&self, rel: &Path) -> std::io::Result<Dir> {
        let base = Dir::open_ambient_dir(&self.root, ambient_authority())?;
        if rel.as_os_str().is_empty() {
            return Ok(base);
        }
        base.open_dir(rel)
    }

    /// Downloads the requested snapshot files into `write_path`. Cancelling
    /// `cancel` ends it with CANCELLED.
    pub async fn fetch_snapshot(
        &self,
        cancel: &CancellationToken,
        req: &FetchSnapshotRequest,
    ) -> Result<(), Status> {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Status::cancelled("context canceled")),
            r = self.fetch(req) => r,
        }
    }

    async fn fetch(&self, req: &FetchSnapshotRequest) -> Result<(), Status> {
        let (uri, rel) = self.validate(&req.snapshot_uri, &req.write_path, &req.files)?;
        let dir =
            Arc::new(self.open_dir(&rel).map_err(|e| {
                to_status(Error::from(e).context("while opening restore directory"))
            })?);
        let tr = self.begin(&req.actor_jwt);
        let jobs = req.files.iter().map(|name| {
            let (dir, uri, tr) = (dir.clone(), &uri, &tr);
            async move {
                if name == MANIFEST_FILE {
                    return fetch_manifest(tr, uri, &dir).await;
                }
                let object = uri
                    .object_uri(&format!("{name}.zstd"))
                    .map_err(|e| Error::other(format!("while addressing {name}: {e}")))?;
                if let Some(parent) = Path::new(name)
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                {
                    let mut b = DirBuilder::new();
                    b.recursive(true);
                    cap_std::fs::DirBuilderExt::mode(&mut b, 0o700);
                    dir.create_dir_with(parent, &b).map_err(|e| {
                        Error::from(e).context(format!(
                            "while creating {} in restore directory",
                            parent.display()
                        ))
                    })?;
                }
                let local = open_write(&dir, name).map_err(|e| {
                    Error::from(e).context(format!("while opening {name} in restore directory"))
                })?;
                tr.run(|client, _| {
                    let local = &local;
                    let object = &object;
                    async move {
                        local.set_len(0)?;
                        local.try_clone()?.seek(SeekFrom::Start(0))?;
                        fetch_file_with_zstd(client.as_ref(), object, local).await
                    }
                })
                .await
                .map_err(|e| e.context(format!("while downloading {name} from object storage")))
            }
        });
        try_join_all(jobs).await.map_err(to_status)?;
        Ok(())
    }

    /// Uploads the requested files from `local_path`. The manifest, when
    /// requested, commits the snapshot only after every data file succeeded.
    /// Cancelling `cancel` ends it with CANCELLED.
    pub async fn upload_snapshot(
        &self,
        cancel: &CancellationToken,
        req: &UploadSnapshotRequest,
    ) -> Result<(), Status> {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Status::cancelled("context canceled")),
            r = self.upload(req) => r,
        }
    }

    async fn upload(&self, req: &UploadSnapshotRequest) -> Result<(), Status> {
        let (uri, rel) = self.validate(&req.snapshot_uri, &req.local_path, &req.files)?;
        let dir = self
            .open_dir(&rel)
            .map_err(|e| to_status(Error::from(e).context("while opening snapshot directory")))?;
        let tr = self.begin(&req.actor_jwt);
        let data = req
            .files
            .iter()
            .filter(|f| *f != MANIFEST_FILE)
            .map(|name| upload_one(&tr, &uri, &dir, name));
        try_join_all(data).await.map_err(to_status)?;
        if req.files.iter().any(|f| f == MANIFEST_FILE) {
            upload_one(&tr, &uri, &dir, MANIFEST_FILE)
                .await
                .map_err(to_status)?;
        }
        Ok(())
    }
}

fn open_write(dir: &Dir, name: &str) -> std::io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.write(true).read(true).create(true).truncate(true);
    cap_std::fs::OpenOptionsExt::mode(&mut opts, 0o600);
    Ok(dir.open_with(name, &opts)?.into_std())
}

async fn fetch_manifest(tr: &Transfer, uri: &SnapshotUri, dir: &Dir) -> Result<()> {
    let object = uri
        .object_uri(MANIFEST_FILE)
        .map_err(|e| Error::other(e.to_string()))?;
    let data = parking_lot::Mutex::new(Vec::new());
    tr.run(|client, _| {
        let object = &object;
        let data = &data;
        async move {
            *data.lock() = fetch_object(client.as_ref(), object).await?;
            Ok(())
        }
    })
    .await
    .map_err(|e| e.context("while fetching snapshot manifest"))?;
    let f = open_write(dir, MANIFEST_FILE).map_err(|e| {
        Error::from(e).context(format!(
            "while writing {MANIFEST_FILE} in restore directory"
        ))
    })?;
    std::io::Write::write_all(&mut &f, &data.into_inner())
        .and_then(|_| f.set_permissions(std::fs::Permissions::from_mode(0o600)))
        .map_err(|e| {
            Error::from(e).context(format!(
                "while writing {MANIFEST_FILE} in restore directory"
            ))
        })
}

async fn upload_one(tr: &Transfer, uri: &SnapshotUri, dir: &Dir, name: &str) -> Result<()> {
    let local = dir
        .open(name)
        .map_err(|e| Error::from(e).context(format!("while opening {name} in snapshot directory")))?
        .into_std();
    let meta = local.metadata().map_err(|e| {
        Error::from(e).context(format!("while inspecting {name} in snapshot directory"))
    })?;
    if !meta.is_file() {
        return Err(Error::other(format!(
            "snapshot file {name} is not a regular file"
        )));
    }
    if name == MANIFEST_FILE {
        let object = uri
            .object_uri(MANIFEST_FILE)
            .map_err(|e| Error::other(e.to_string()))?;
        return tr
            .run(|_, uploader| {
                let (local, object) = (&local, &object);
                async move {
                    // A refused attempt may have read the file; the retry
                    // sends all of it.
                    let mut f = local.try_clone()?;
                    f.seek(SeekFrom::Start(0))?;
                    uploader.send(object, SendContent::File(f)).await
                }
            })
            .await
            .map_err(|e| e.context("while uploading snapshot manifest"));
    }
    let object = uri
        .object_uri(&format!("{name}.zstd"))
        .map_err(|e| Error::other(format!("while addressing {name}: {e}")))?;
    tr.run(|_, uploader| {
        let (local, object) = (&local, &object);
        async move {
            local.try_clone()?.seek(SeekFrom::Start(0))?;
            uploader.send_file(object, local).await
        }
    })
    .await
    .map_err(|e| e.context(format!("while uploading {name} to object storage")))
}

#[tonic::async_trait]
impl crate::proto::v1::node_provider_server::NodeProvider for NodePlugin {
    async fn fetch_snapshot(
        &self,
        req: tonic::Request<FetchSnapshotRequest>,
    ) -> Result<tonic::Response<crate::proto::v1::FetchSnapshotResponse>, Status> {
        NodePlugin::fetch_snapshot(self, &CancellationToken::new(), req.get_ref()).await?;
        Ok(tonic::Response::new(
            crate::proto::v1::FetchSnapshotResponse {},
        ))
    }

    async fn upload_snapshot(
        &self,
        req: tonic::Request<UploadSnapshotRequest>,
    ) -> Result<tonic::Response<crate::proto::v1::UploadSnapshotResponse>, Status> {
        NodePlugin::upload_snapshot(self, &CancellationToken::new(), req.get_ref()).await?;
        Ok(tonic::Response::new(
            crate::proto::v1::UploadSnapshotResponse {},
        ))
    }
}

/// The node, asset and control services on one Unix socket, with health,
/// for tests and the sidecar.
pub mod serve {
    use std::path::Path;
    use std::sync::Arc;

    use crate::pluginsocket::{dial, incoming, listen, serving_health, PluginChannel, READY_WAIT};
    use crate::proto::v1::asset_provider_server::AssetProviderServer;
    use crate::proto::v1::control_provider_server::ControlProviderServer;
    use crate::proto::v1::node_provider_server::NodeProviderServer;

    /// Serves the given plugins on `socket` in the background and returns a
    /// channel to them.
    pub async fn spawn(
        socket: &Path,
        node: Option<super::NodePlugin>,
        control: Option<super::super::control::ControlPlugin>,
        asset: Option<super::super::asset::AssetPlugin>,
    ) -> std::io::Result<(PluginChannel, tokio::task::JoinHandle<()>)> {
        let lis = listen(socket)?;
        let health = serving_health().await;
        let routes = tonic::transport::Server::builder()
            .add_service(health)
            .add_optional_service(node.map(|n| NodeProviderServer::from_arc(Arc::new(n))))
            .add_optional_service(control.map(ControlProviderServer::new))
            .add_optional_service(asset.map(AssetProviderServer::new));
        let h = tokio::spawn(async move {
            let _ = routes.serve_with_incoming(incoming(lis)).await;
        });
        let ch = dial(socket, READY_WAIT).map_err(std::io::Error::other)?;
        Ok((ch, h))
    }
}

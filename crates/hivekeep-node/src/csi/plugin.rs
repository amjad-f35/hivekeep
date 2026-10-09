//! A volume plugin over one CSI driver: provisioning and attachment for the
//! control plane, staging and publishing for the node.
//!
//! Optional RPCs follow the driver's advertised capabilities. Attach and
//! detach (`ControllerPublishVolume`/`ControllerUnpublishVolume`) and
//! staging (`NodeStageVolume`/`NodeUnstageVolume`) are skipped when the
//! driver does not list them, and switched off for good the first time the
//! driver answers one with `Unimplemented`. When the capability query
//! fails or lists nothing, every optional RPC is assumed supported and the
//! `Unimplemented` fallback does the pruning.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use tonic::{Code, Status};

use super::client::CsiClient;
use super::pb;
use crate::paths::{NodePaths, DEFAULT_BASE_DIR};

/// A controller RPC that drivers may or may not implement.
pub type ControllerRpc = pb::controller_service_capability::rpc::Type;
/// A node RPC that drivers may or may not implement.
pub type NodeRpc = pb::node_service_capability::rpc::Type;

/// A volume to provision.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateVolumeRequest {
    /// The name to provision under; creation is idempotent by name.
    pub name: String,
    /// The requested size in bytes.
    pub capacity_bytes: i64,
    /// Driver parameters from the storage class.
    pub parameters: HashMap<String, String>,
}

/// A provisioned volume.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateVolumeResponse {
    /// The storage system's ID for it.
    pub volume_id: String,
    /// Driver metadata the node needs to mount it.
    pub volume_context: HashMap<String, String>,
}

/// A volume to attach to a node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AttachVolumeRequest {
    /// The storage system's volume ID.
    pub volume_id: String,
    /// The node to attach to.
    pub node: String,
}

/// A completed attachment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AttachVolumeResponse {
    /// Driver attachment metadata (a device path, say) that the node's
    /// mount calls must echo back. Empty for drivers without attachment.
    pub publish_context: HashMap<String, String>,
}

/// A volume to mount on this node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MountVolumeRequest {
    /// The storage system's volume ID.
    pub volume_id: String,
    /// The host path to publish at.
    pub target_path: String,
    /// Metadata from provisioning.
    pub volume_context: BTreeMap<String, String>,
    /// Metadata from attaching.
    pub publish_context: BTreeMap<String, String>,
}

#[derive(Debug, Default)]
struct Capabilities {
    controller: HashSet<i32>,
    node: HashSet<i32>,
}

/// A CSI driver as a volume plugin.
#[derive(Debug)]
pub struct CsiPlugin {
    client: CsiClient,
    staging_dir: PathBuf,
    caps: RwLock<Capabilities>,
}

/// The access every volume is requested with: one node, read-write,
/// mounted as a filesystem.
fn standard_capability() -> pb::VolumeCapability {
    pb::VolumeCapability {
        access_mode: Some(pb::volume_capability::AccessMode {
            mode: pb::volume_capability::access_mode::Mode::SingleNodeWriter as i32,
        }),
        access_type: Some(pb::volume_capability::AccessType::Mount(
            pb::volume_capability::MountVolume::default(),
        )),
    }
}

/// `err` with what failed in front, keeping its code.
fn wrap(what: &str, err: Status) -> Status {
    Status::new(err.code(), format!("CSI {what} failed: {}", err.message()))
}

/// Every known value of an RPC enum except `Unknown` (0).
fn all_rpcs<T: TryFrom<i32>>() -> HashSet<i32> {
    (1..=64).filter(|v| T::try_from(*v).is_ok()).collect()
}

impl CsiPlugin {
    /// A plugin over `client`, staging under the default node layout. No
    /// optional RPC counts as supported until the capabilities are queried.
    pub fn new(client: CsiClient) -> Self {
        CsiPlugin {
            client,
            staging_dir: NodePaths::new(DEFAULT_BASE_DIR).csi_staging_dir(),
            caps: RwLock::new(Capabilities::default()),
        }
    }

    /// Stages volumes under `dir` instead.
    pub fn with_staging_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.staging_dir = dir.into();
        self
    }

    /// The directory volumes are staged under.
    pub fn staging_dir(&self) -> &Path {
        &self.staging_dir
    }

    /// The staging path of a volume.
    pub fn staging_path(&self, volume_id: &str) -> PathBuf {
        self.staging_dir.join(volume_id)
    }

    /// The underlying client.
    pub fn client(&self) -> &CsiClient {
        &self.client
    }

    /// Whether the driver supports a controller RPC.
    pub fn supports_controller_capability(&self, rpc: ControllerRpc) -> bool {
        self.caps.read().controller.contains(&(rpc as i32))
    }

    /// Whether the driver supports a node RPC.
    pub fn supports_node_capability(&self, rpc: NodeRpc) -> bool {
        self.caps.read().node.contains(&(rpc as i32))
    }

    fn disable_controller_cap(&self, rpc: ControllerRpc) {
        self.caps.write().controller.remove(&(rpc as i32));
    }

    fn disable_node_cap(&self, rpc: NodeRpc) {
        self.caps.write().node.remove(&(rpc as i32));
    }

    /// Queries and caches the controller capabilities.
    pub async fn init_controller_capabilities(&self) {
        let resp = self
            .client
            .controller()
            .controller_get_capabilities(pb::ControllerGetCapabilitiesRequest {})
            .await;
        let mut caps: HashSet<i32> = HashSet::new();
        if let Ok(r) = &resp {
            for c in &r.get_ref().capabilities {
                if let Some(pb::controller_service_capability::Type::Rpc(rpc)) = &c.r#type {
                    caps.insert(rpc.r#type);
                }
            }
        }
        if resp.is_err() || caps.is_empty() {
            tracing::warn!(err = ?resp.err(), "CSI ControllerGetCapabilities failed or reported no capabilities; assuming all controller RPCs are supported");
            caps = all_rpcs::<ControllerRpc>();
        }
        self.caps.write().controller = caps;
    }

    /// Queries and caches the node capabilities.
    pub async fn init_node_capabilities(&self) {
        let resp = self
            .client
            .node()
            .node_get_capabilities(pb::NodeGetCapabilitiesRequest {})
            .await;
        let mut caps: HashSet<i32> = HashSet::new();
        if let Ok(r) = &resp {
            for c in &r.get_ref().capabilities {
                if let Some(pb::node_service_capability::Type::Rpc(rpc)) = &c.r#type {
                    caps.insert(rpc.r#type);
                }
            }
        }
        if resp.is_err() || caps.is_empty() {
            tracing::warn!(err = ?resp.err(), "CSI NodeGetCapabilities failed or reported no capabilities; assuming all node RPCs are supported");
            caps = all_rpcs::<NodeRpc>();
        }
        self.caps.write().node = caps;
    }

    /// The driver's own name.
    pub async fn driver_name(&self) -> Result<String, Status> {
        let resp = self
            .client
            .identity()
            .get_plugin_info(pb::GetPluginInfoRequest {})
            .await?;
        Ok(resp.into_inner().name)
    }

    /// Provisions a volume (`CreateVolume`).
    pub async fn create_volume(
        &self,
        req: CreateVolumeRequest,
    ) -> Result<CreateVolumeResponse, Status> {
        let resp = self
            .client
            .controller()
            .create_volume(pb::CreateVolumeRequest {
                name: req.name,
                capacity_range: Some(pb::CapacityRange {
                    required_bytes: req.capacity_bytes,
                    limit_bytes: 0,
                }),
                volume_capabilities: vec![standard_capability()],
                parameters: req.parameters,
                ..Default::default()
            })
            .await
            .map_err(|e| wrap("CreateVolume", e))?
            .into_inner();
        let vol = resp
            .volume
            .ok_or_else(|| Status::internal("CSI CreateVolume response returned nil volume"))?;
        Ok(CreateVolumeResponse {
            volume_id: vol.volume_id,
            volume_context: vol.volume_context,
        })
    }

    /// Deprovisions a volume (`DeleteVolume`).
    pub async fn delete_volume(&self, volume_id: &str) -> Result<(), Status> {
        self.client
            .controller()
            .delete_volume(pb::DeleteVolumeRequest {
                volume_id: volume_id.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| wrap("DeleteVolume", e))?;
        Ok(())
    }

    /// Attaches a volume to a node (`ControllerPublishVolume`), when the
    /// driver does attachment at all.
    pub async fn attach_volume(
        &self,
        req: AttachVolumeRequest,
    ) -> Result<AttachVolumeResponse, Status> {
        if !self.supports_controller_capability(ControllerRpc::PublishUnpublishVolume) {
            tracing::debug!(volume_id = %req.volume_id, node = %req.node, "Driver does not support ControllerPublishVolume; skipping attach");
            return Ok(AttachVolumeResponse::default());
        }
        let resp = self
            .client
            .controller()
            .controller_publish_volume(pb::ControllerPublishVolumeRequest {
                volume_id: req.volume_id.clone(),
                node_id: req.node.clone(),
                volume_capability: Some(standard_capability()),
                readonly: false,
                ..Default::default()
            })
            .await;
        match resp {
            Ok(r) => Ok(AttachVolumeResponse {
                publish_context: r.into_inner().publish_context,
            }),
            Err(e) if e.code() == Code::Unimplemented => {
                tracing::warn!(volume_id = %req.volume_id, node = %req.node, "CSI ControllerPublishVolume is unimplemented by driver; skipping attach");
                self.disable_controller_cap(ControllerRpc::PublishUnpublishVolume);
                Ok(AttachVolumeResponse::default())
            }
            Err(e) => Err(wrap("ControllerPublishVolume", e)),
        }
    }

    /// Detaches a volume from a node (`ControllerUnpublishVolume`), when
    /// the driver does attachment at all.
    pub async fn detach_volume(&self, volume_id: &str, node: &str) -> Result<(), Status> {
        if !self.supports_controller_capability(ControllerRpc::PublishUnpublishVolume) {
            tracing::debug!(
                volume_id,
                node,
                "Driver does not support ControllerPublishVolume; skipping detach"
            );
            return Ok(());
        }
        let resp = self
            .client
            .controller()
            .controller_unpublish_volume(pb::ControllerUnpublishVolumeRequest {
                volume_id: volume_id.to_string(),
                node_id: node.to_string(),
                ..Default::default()
            })
            .await;
        match resp {
            Ok(_) => Ok(()),
            Err(e) if e.code() == Code::Unimplemented => {
                tracing::warn!(
                    volume_id,
                    node,
                    "CSI ControllerUnpublishVolume is unimplemented by driver; skipping detach"
                );
                self.disable_controller_cap(ControllerRpc::PublishUnpublishVolume);
                Ok(())
            }
            Err(e) => Err(wrap("ControllerUnpublishVolume", e)),
        }
    }

    /// Publishes a volume at its target path (`NodePublishVolume`), staging
    /// it first when the driver stages.
    pub async fn mount_volume(&self, req: MountVolumeRequest) -> Result<(), Status> {
        let staging = self.stage_volume(&req).await?;
        self.client
            .node()
            .node_publish_volume(pb::NodePublishVolumeRequest {
                volume_id: req.volume_id,
                staging_target_path: staging
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                target_path: req.target_path,
                volume_capability: Some(standard_capability()),
                readonly: false,
                volume_context: req.volume_context.into_iter().collect(),
                publish_context: req.publish_context.into_iter().collect(),
                ..Default::default()
            })
            .await
            .map_err(|e| wrap("NodePublishVolume", e))?;
        Ok(())
    }

    /// Stages a volume (`NodeStageVolume`) if the driver stages; the
    /// staging path, or `None` when nothing was staged.
    async fn stage_volume(&self, req: &MountVolumeRequest) -> Result<Option<PathBuf>, Status> {
        if !self.supports_node_capability(NodeRpc::StageUnstageVolume) {
            return Ok(None);
        }
        let staging = self.staging_path(&req.volume_id);
        create_staging_dir(&staging).map_err(|e| {
            Status::internal(format!(
                "failed to create staging directory {}: {e}",
                staging.display()
            ))
        })?;
        let resp = self
            .client
            .node()
            .node_stage_volume(pb::NodeStageVolumeRequest {
                volume_id: req.volume_id.clone(),
                staging_target_path: staging.to_string_lossy().into_owned(),
                volume_capability: Some(standard_capability()),
                volume_context: req.volume_context.clone().into_iter().collect(),
                publish_context: req.publish_context.clone().into_iter().collect(),
                ..Default::default()
            })
            .await;
        match resp {
            Ok(_) => Ok(Some(staging)),
            Err(e) if e.code() == Code::Unimplemented => {
                tracing::warn!(volume_id = %req.volume_id, "CSI NodeStageVolume is unimplemented by driver; skipping staging");
                self.disable_node_cap(NodeRpc::StageUnstageVolume);
                remove_staging_dir(&staging);
                Ok(None)
            }
            Err(e) => Err(wrap("NodeStageVolume", e)),
        }
    }

    /// Unpublishes a volume (`NodeUnpublishVolume`), then unstages it when
    /// the driver stages.
    pub async fn unmount_volume(&self, volume_id: &str, target_path: &str) -> Result<(), Status> {
        self.client
            .node()
            .node_unpublish_volume(pb::NodeUnpublishVolumeRequest {
                volume_id: volume_id.to_string(),
                target_path: target_path.to_string(),
            })
            .await
            .map_err(|e| wrap("NodeUnpublishVolume", e))?;
        self.unstage_volume(volume_id).await
    }

    async fn unstage_volume(&self, volume_id: &str) -> Result<(), Status> {
        if !self.supports_node_capability(NodeRpc::StageUnstageVolume) {
            return Ok(());
        }
        let staging = self.staging_path(volume_id);
        let resp = self
            .client
            .node()
            .node_unstage_volume(pb::NodeUnstageVolumeRequest {
                volume_id: volume_id.to_string(),
                staging_target_path: staging.to_string_lossy().into_owned(),
            })
            .await;
        match resp {
            Ok(_) => {}
            Err(e) if e.code() == Code::Unimplemented => {
                tracing::warn!(
                    volume_id,
                    "CSI NodeUnstageVolume is unimplemented by driver; skipping unstaging"
                );
                self.disable_node_cap(NodeRpc::StageUnstageVolume);
            }
            Err(e) => return Err(wrap("NodeUnstageVolume", e)),
        }
        remove_staging_dir(&staging);
        Ok(())
    }
}

fn create_staging_dir(path: &Path) -> std::io::Result<()> {
    let mut b = std::fs::DirBuilder::new();
    b.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        b.mode(0o750);
    }
    b.create(path)
}

/// Removes an (empty) staging directory, logging rather than failing.
fn remove_staging_dir(path: &Path) {
    match std::fs::remove_dir(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            tracing::warn!(path = %path.display(), err = %e, "failed to remove staging directory")
        }
    }
}

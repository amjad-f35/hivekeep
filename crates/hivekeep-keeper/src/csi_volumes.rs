//! External volumes through CSI node drivers.
//!
//! [`CsiVolumeDrivers`] opens a driver's node service the first time one of
//! its volumes is mounted on this node, using the driver's
//! `CSIDriverConfig` to find its socket (by default the kubelet plugin
//! socket of the driver's node DaemonSet). The [`VolumeManager`] caches
//! each opened plugin, so a driver is looked up and probed once.
//!
//! [`VolumeManager`]: crate::volumes::VolumeManager

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_node::csi::{self, CsiDriverConfigGetter, CsiPlugin, OpenOptions, Role};
use tonic::Status;

use crate::context::Ctx;
use crate::volumes::{MountVolumeRequest, VolumePlugin, VolumePluginFactory};

/// A CSI node driver as a keeper volume plugin.
pub struct CsiNodePlugin(pub Arc<CsiPlugin>);

#[async_trait]
impl VolumePlugin for CsiNodePlugin {
    async fn mount_volume(&self, _: &Ctx, req: MountVolumeRequest) -> Result<(), Status> {
        self.0
            .mount_volume(csi::MountVolumeRequest {
                volume_id: req.volume_id,
                target_path: req.target_path,
                volume_context: req.volume_context,
                publish_context: req.publish_context,
            })
            .await
    }

    async fn unmount_volume(
        &self,
        _: &Ctx,
        volume_id: &str,
        target_path: &str,
    ) -> Result<(), Status> {
        self.0.unmount_volume(volume_id, target_path).await
    }
}

/// Opens CSI node drivers named by volumes, from their driver configs.
pub struct CsiVolumeDrivers {
    configs: Arc<dyn CsiDriverConfigGetter>,
    staging_dir: PathBuf,
}

impl CsiVolumeDrivers {
    /// Drivers found through `configs`, staging volumes under
    /// `staging_dir` (the node layout's CSI staging directory).
    pub fn new(configs: Arc<dyn CsiDriverConfigGetter>, staging_dir: impl Into<PathBuf>) -> Self {
        CsiVolumeDrivers {
            configs,
            staging_dir: staging_dir.into(),
        }
    }
}

#[async_trait]
impl VolumePluginFactory for CsiVolumeDrivers {
    async fn plugin(&self, _: &Ctx, driver: &str) -> Result<Arc<dyn VolumePlugin>, Status> {
        let plugin = csi::open_csi_plugin_with(
            self.configs.as_ref(),
            driver,
            Role::Node,
            OpenOptions {
                staging_dir: Some(self.staging_dir.clone()),
                ..Default::default()
            },
        )
        .await?;
        tracing::info!(driver, endpoint = %plugin.client().endpoint(), "Opened CSI node driver");
        Ok(Arc::new(CsiNodePlugin(Arc::new(plugin))))
    }
}

#[cfg(test)]
mod tests;

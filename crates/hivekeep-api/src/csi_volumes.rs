//! External volumes through CSI controller drivers: the production
//! [`VolumeProvider`].
//!
//! A storage class names its driver (provisioner) and parameters. Classes
//! come from the `--storage-classes-file` first, then from the cluster's
//! `storage.k8s.io/v1` StorageClasses. A driver is opened the first time a
//! volume of its type is provisioned, attached, detached or deleted, from
//! its `CSIDriverConfig` (controller endpoint and TLS), checked to report
//! the requested name, and kept for the life of the process. A failed open
//! is not cached, so the next operation retries.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_node::csi::{self, CsiDriverConfigGetter, CsiPlugin, OpenOptions, Role};
use k8s_openapi::api::storage::v1::StorageClass as KubeStorageClass;
use kube::Api;
use tonic::Status;

use crate::volumes::{CreatedVolume, StaticVolumes, StorageClass, VolumePlugin, VolumeProvider};

/// A CSI controller driver as a control-plane volume plugin.
pub struct CsiControllerPlugin(pub Arc<CsiPlugin>);

#[async_trait]
impl VolumePlugin for CsiControllerPlugin {
    async fn create_volume(
        &self,
        name: &str,
        capacity_bytes: i64,
        parameters: &HashMap<String, String>,
    ) -> Result<CreatedVolume, Status> {
        let resp = self
            .0
            .create_volume(csi::CreateVolumeRequest {
                name: name.to_string(),
                capacity_bytes,
                parameters: parameters.clone(),
            })
            .await?;
        Ok(CreatedVolume {
            volume_id: resp.volume_id,
            volume_context: resp.volume_context,
        })
    }

    async fn attach_volume(
        &self,
        volume_id: &str,
        node: &str,
    ) -> Result<HashMap<String, String>, Status> {
        let resp = self
            .0
            .attach_volume(csi::AttachVolumeRequest {
                volume_id: volume_id.to_string(),
                node: node.to_string(),
            })
            .await?;
        Ok(resp.publish_context)
    }

    async fn detach_volume(&self, volume_id: &str, node: &str) -> Result<(), Status> {
        self.0.detach_volume(volume_id, node).await
    }

    async fn delete_volume(&self, volume_id: &str) -> Result<(), Status> {
        self.0.delete_volume(volume_id).await
    }
}

/// Volume plugins opened from `CSIDriverConfig`s, with storage classes
/// from a file and the cluster.
pub struct CsiVolumes {
    fixed: StaticVolumes,
    configs: Arc<dyn CsiDriverConfigGetter>,
    storage_classes: Option<Api<KubeStorageClass>>,
    open: OpenOptions,
    opened: tokio::sync::Mutex<HashMap<String, Arc<dyn VolumePlugin>>>,
}

impl CsiVolumes {
    /// A provider opening drivers through `configs`. `fixed` holds classes
    /// (and any plugins) that take precedence over the cluster's;
    /// `kube` is where StorageClasses are read, if anywhere.
    pub fn new(
        fixed: StaticVolumes,
        configs: Arc<dyn CsiDriverConfigGetter>,
        kube: Option<kube::Client>,
    ) -> Self {
        CsiVolumes {
            fixed,
            configs,
            storage_classes: kube.map(Api::all),
            open: OpenOptions::default(),
            opened: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Opens drivers with `open` (controller TLS credential paths).
    pub fn with_open_options(mut self, open: OpenOptions) -> Self {
        self.open = open;
        self
    }
}

#[async_trait]
impl VolumeProvider for CsiVolumes {
    async fn plugin(&self, driver: &str) -> Result<Arc<dyn VolumePlugin>, String> {
        if let Some(p) = self.fixed.registered_plugin(driver) {
            return Ok(p);
        }
        // Held across the open, so concurrent first uses open once.
        let mut opened = self.opened.lock().await;
        if let Some(p) = opened.get(driver) {
            return Ok(p.clone());
        }
        let plugin = csi::open_csi_plugin_with(
            self.configs.as_ref(),
            driver,
            Role::Controller,
            self.open.clone(),
        )
        .await
        .map_err(|s| s.message().to_string())?;
        tracing::info!(driver, endpoint = %plugin.client().endpoint(), "Opened CSI controller driver");
        let plugin: Arc<dyn VolumePlugin> = Arc::new(CsiControllerPlugin(Arc::new(plugin)));
        opened.insert(driver.to_string(), plugin.clone());
        Ok(plugin)
    }

    async fn storage_class(&self, name: &str) -> std::result::Result<Option<StorageClass>, Status> {
        if let Some(c) = self.fixed.registered_storage_class(name) {
            return Ok(Some(c));
        }
        let Some(api) = self.storage_classes.as_ref() else {
            return Ok(None);
        };
        match api.get_opt(name).await {
            Ok(sc) => Ok(sc.map(|sc| StorageClass {
                provisioner: sc.provisioner,
                parameters: sc.parameters.unwrap_or_default().into_iter().collect(),
            })),
            Err(e) => {
                tracing::warn!(storage_class = name, err = %e, "Cannot read StorageClass");
                Err(Status::internal(format!(
                    "reading StorageClass {name:?}: {e}"
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests;

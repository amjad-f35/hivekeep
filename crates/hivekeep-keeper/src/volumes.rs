//! External volumes: storage the control plane provisioned, published on
//! the node at an actor's volume mount points by a volume plugin.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_proto::keeper::v1 as pb;
use parking_lot::RwLock;
use tonic::{Code, Status};

use crate::context::Ctx;
use crate::paths::NodePaths;

/// A request to publish a volume at a node path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountVolumeRequest {
    /// The storage system's volume ID.
    pub volume_id: String,
    /// Where to publish it.
    pub target_path: String,
    /// Attributes from provisioning.
    pub volume_context: BTreeMap<String, String>,
    /// Attributes from attaching.
    pub publish_context: BTreeMap<String, String>,
}

/// The node side of a volume driver.
#[async_trait]
pub trait VolumePlugin: Send + Sync + 'static {
    /// Publishes a volume. Publishing again is a no-op.
    async fn mount_volume(&self, ctx: &Ctx, req: MountVolumeRequest) -> Result<(), Status>;
    /// Unpublishes a volume. One already gone fails with NotFound.
    async fn unmount_volume(
        &self,
        ctx: &Ctx,
        volume_id: &str,
        target_path: &str,
    ) -> Result<(), Status>;
}

/// Makes plugins for drivers not registered up front, such as a CSI driver
/// named by its config.
#[async_trait]
pub trait VolumePluginFactory: Send + Sync + 'static {
    /// The plugin of `driver`.
    async fn plugin(&self, ctx: &Ctx, driver: &str) -> Result<Arc<dyn VolumePlugin>, Status>;
}

/// A factory knowing no drivers.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoVolumeDrivers;

#[async_trait]
impl VolumePluginFactory for NoVolumeDrivers {
    async fn plugin(&self, _: &Ctx, driver: &str) -> Result<Arc<dyn VolumePlugin>, Status> {
        Err(Status::failed_precondition(format!(
            "no volume plugin for driver {driver:?} on this node"
        )))
    }
}

/// Mounts and unmounts actors' external volumes.
pub struct VolumeManager {
    paths: Arc<NodePaths>,
    plugins: RwLock<HashMap<String, Arc<dyn VolumePlugin>>>,
    factory: Arc<dyn VolumePluginFactory>,
}

impl VolumeManager {
    /// A manager with `plugins` registered, making others with `factory`.
    pub fn new(
        paths: Arc<NodePaths>,
        plugins: HashMap<String, Arc<dyn VolumePlugin>>,
        factory: Arc<dyn VolumePluginFactory>,
    ) -> Self {
        VolumeManager {
            paths,
            plugins: RwLock::new(plugins),
            factory,
        }
    }

    async fn plugin(&self, ctx: &Ctx, driver: &str) -> Result<Arc<dyn VolumePlugin>, Status> {
        if let Some(p) = self.plugins.read().get(driver).cloned() {
            return Ok(p);
        }
        let p = self.factory.plugin(ctx, driver).await?;
        self.plugins.write().insert(driver.to_string(), p.clone());
        Ok(p)
    }

    /// Publishes each external volume at its mount point under the actor.
    pub async fn mount_external_volumes(
        &self,
        ctx: &Ctx,
        actor_uid: &str,
        volumes: &[pb::Volume],
    ) -> Result<(), Status> {
        for v in volumes {
            let Some(ext) = &v.external else { continue };
            let host = self.paths.external_volume_host_path(actor_uid, &v.name);
            crate::atomicfile::create_dir_all_mode(&host, 0o750).map_err(|e| {
                Status::internal(format!(
                    "failed to create mount point {}: {e}",
                    host.display()
                ))
            })?;
            tracing::info!(volume_id = %ext.storage_volume_id, host_path = %host.display(), volume_type = %ext.volume_type, "Mounting volume");
            let plugin = self.plugin(ctx, &ext.volume_type).await.map_err(|e| {
                crate::status::wrap(
                    format!("failed to get volume plugin for {:?}", ext.volume_type),
                    e,
                )
            })?;
            plugin
                .mount_volume(
                    ctx,
                    MountVolumeRequest {
                        volume_id: ext.storage_volume_id.clone(),
                        target_path: host.to_string_lossy().into_owned(),
                        volume_context: ext.volume_context.clone().into_iter().collect(),
                        publish_context: ext.publish_context.clone().into_iter().collect(),
                    },
                )
                .await
                .map_err(|e| {
                    crate::status::wrap(
                        format!(
                            "failed to mount volume {:?} to {}",
                            ext.storage_volume_id,
                            host.display()
                        ),
                        e,
                    )
                })?;
        }
        Ok(())
    }

    /// Unpublishes each external volume, attempting all of them. One
    /// already gone counts as unmounted.
    pub async fn unmount_external_volumes(
        &self,
        ctx: &Ctx,
        actor_uid: &str,
        volumes: &[pb::Volume],
    ) -> Result<(), Status> {
        let mut errs = Vec::new();
        for v in volumes {
            let Some(ext) = &v.external else { continue };
            let host = self.paths.external_volume_host_path(actor_uid, &v.name);
            tracing::info!(volume_id = %ext.storage_volume_id, host_path = %host.display(), volume_type = %ext.volume_type, "Unmounting volume");
            let plugin = match self.plugin(ctx, &ext.volume_type).await {
                Ok(p) => p,
                Err(e) => {
                    errs.push(format!(
                        "failed to get volume plugin for {:?} (volume {:?}): {}",
                        ext.volume_type,
                        ext.storage_volume_id,
                        e.message()
                    ));
                    continue;
                }
            };
            if let Err(e) = plugin
                .unmount_volume(ctx, &ext.storage_volume_id, &host.to_string_lossy())
                .await
            {
                if e.code() == Code::NotFound {
                    tracing::warn!(volume_id = %ext.storage_volume_id, err = %e, "Volume not found during unmount, assuming already unmounted");
                } else {
                    errs.push(format!(
                        "failed to unmount volume {:?} from {}: {}",
                        ext.storage_volume_id,
                        host.display(),
                        e.message()
                    ));
                }
            }
        }
        if errs.is_empty() {
            Ok(())
        } else {
            Err(Status::internal(errs.join("; ")))
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use parking_lot::Mutex;

    #[derive(Default)]
    pub(crate) struct FakePlugin {
        pub(crate) mounts: Mutex<Vec<String>>,
        pub(crate) unmounts: Mutex<Vec<String>>,
        pub(crate) unmount_err: Mutex<Option<Status>>,
    }

    #[async_trait]
    impl VolumePlugin for FakePlugin {
        async fn mount_volume(&self, _: &Ctx, req: MountVolumeRequest) -> Result<(), Status> {
            self.mounts.lock().push(req.volume_id);
            Ok(())
        }
        async fn unmount_volume(&self, _: &Ctx, id: &str, _: &str) -> Result<(), Status> {
            self.unmounts.lock().push(id.into());
            match self.unmount_err.lock().clone() {
                Some(e) => Err(e),
                None => Ok(()),
            }
        }
    }

    fn vol(name: &str, driver: &str) -> pb::Volume {
        pb::Volume {
            name: name.into(),
            external: Some(pb::ExternalVolumeSource {
                storage_volume_id: format!("id-{name}"),
                volume_type: driver.into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn mounts_and_unmounts_external_volumes_only() {
        let d = tempfile::tempdir().unwrap();
        let paths = Arc::new(NodePaths::new(d.path()));
        let plugin = Arc::new(FakePlugin::default());
        let mut plugins: HashMap<String, Arc<dyn VolumePlugin>> = HashMap::new();
        plugins.insert("disk".into(), plugin.clone());
        let m = VolumeManager::new(paths.clone(), plugins, Arc::new(NoVolumeDrivers));
        let vols = vec![
            vol("a", "disk"),
            pb::Volume {
                name: "dd".into(),
                durable_dir: Some(pb::DurableDirVolumeSource {}),
                ..Default::default()
            },
        ];
        m.mount_external_volumes(&Ctx::background(), "u", &vols)
            .await
            .unwrap();
        assert_eq!(*plugin.mounts.lock(), vec!["id-a"]);
        assert!(paths.external_volume_host_path("u", "a").is_dir());
        *plugin.unmount_err.lock() = Some(Status::not_found("gone"));
        m.unmount_external_volumes(&Ctx::background(), "u", &vols)
            .await
            .unwrap();
        *plugin.unmount_err.lock() = Some(Status::internal("busy"));
        assert!(m
            .unmount_external_volumes(&Ctx::background(), "u", &vols)
            .await
            .is_err());
        let err = m
            .mount_external_volumes(&Ctx::background(), "u", &[vol("b", "unknown")])
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
    }
}

#[cfg(test)]
mod behavior_tests;

//! External (CSI) volumes of Actors.
//!
//! A template volume with an `external` source gets one storage volume per
//! Actor, created on the Actor's first resume, attached to the node before
//! each restore, detached on suspend, revert and delete, and deleted with
//! the Actor. Storage drivers sit behind [`VolumePlugin`]; storage classes
//! name the driver (provisioner) and its parameters. [`StaticVolumes`]
//! holds fixed plugins and classes (tests, development); the production
//! provider opens CSI drivers on demand (see [`crate::csi_volumes`]).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_proto::api::v1 as pb;
use parking_lot::RwLock;
use tonic::{Code, Status};

use crate::error::{status, Error, Result};

/// A storage class: which driver provisions it, with what parameters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageClass {
    /// The driver, recorded as the volume's type.
    pub provisioner: String,
    /// Driver parameters.
    pub parameters: HashMap<String, String>,
}

/// A created volume.
#[derive(Debug, Clone, Default)]
pub struct CreatedVolume {
    /// The driver's id for it.
    pub volume_id: String,
    /// Driver metadata to pass through to the node.
    pub volume_context: HashMap<String, String>,
}

/// One storage driver's control-plane operations. A `NOT_FOUND` from detach
/// or delete means the work is already done.
#[async_trait]
pub trait VolumePlugin: Send + Sync {
    /// Creates (idempotently, by `name`) a volume.
    async fn create_volume(
        &self,
        name: &str,
        capacity_bytes: i64,
        parameters: &HashMap<String, String>,
    ) -> std::result::Result<CreatedVolume, Status>;
    /// Attaches a volume to a node, returning the driver's publish context.
    async fn attach_volume(
        &self,
        volume_id: &str,
        node: &str,
    ) -> std::result::Result<HashMap<String, String>, Status>;
    /// Detaches a volume from a node.
    async fn detach_volume(&self, volume_id: &str, node: &str) -> std::result::Result<(), Status>;
    /// Deletes a volume.
    async fn delete_volume(&self, volume_id: &str) -> std::result::Result<(), Status>;
}

/// Where volume drivers and storage classes come from.
#[async_trait]
pub trait VolumeProvider: Send + Sync {
    /// The plugin for `driver`.
    async fn plugin(&self, driver: &str) -> std::result::Result<Arc<dyn VolumePlugin>, String>;
    /// The storage class `name`, `None` when it does not exist, or an error
    /// when it could not be looked up.
    async fn storage_class(&self, name: &str) -> std::result::Result<Option<StorageClass>, Status>;
}

/// A provider over fixed maps. The default has neither plugins nor classes,
/// so templates with external volumes are refused.
#[derive(Default)]
pub struct StaticVolumes {
    plugins: RwLock<HashMap<String, Arc<dyn VolumePlugin>>>,
    classes: RwLock<HashMap<String, StorageClass>>,
}

impl StaticVolumes {
    /// An empty provider.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `plugin` for `driver`.
    pub fn add_plugin(&self, driver: impl Into<String>, plugin: Arc<dyn VolumePlugin>) {
        self.plugins.write().insert(driver.into(), plugin);
    }

    /// Registers a storage class.
    pub fn add_storage_class(&self, name: impl Into<String>, class: StorageClass) {
        self.classes.write().insert(name.into(), class);
    }

    /// Loads storage classes from a JSON object of
    /// `{"name": {"provisioner": "...", "parameters": {...}}}`.
    pub fn load_storage_classes(&self, json: &[u8]) -> std::result::Result<(), String> {
        #[derive(serde::Deserialize)]
        struct Wire {
            provisioner: String,
            #[serde(default)]
            parameters: HashMap<String, String>,
        }
        let m: HashMap<String, Wire> = serde_json::from_slice(json).map_err(|e| e.to_string())?;
        for (k, v) in m {
            self.add_storage_class(
                k,
                StorageClass {
                    provisioner: v.provisioner,
                    parameters: v.parameters,
                },
            );
        }
        Ok(())
    }
}

impl StaticVolumes {
    /// The registered plugin for `driver`, if any.
    pub fn registered_plugin(&self, driver: &str) -> Option<Arc<dyn VolumePlugin>> {
        self.plugins.read().get(driver).cloned()
    }

    /// The registered storage class `name`, if any.
    pub fn registered_storage_class(&self, name: &str) -> Option<StorageClass> {
        self.classes.read().get(name).cloned()
    }
}

#[async_trait]
impl VolumeProvider for StaticVolumes {
    async fn plugin(&self, driver: &str) -> std::result::Result<Arc<dyn VolumePlugin>, String> {
        self.registered_plugin(driver)
            .ok_or_else(|| format!("no volume plugin registered for driver {driver:?}"))
    }

    async fn storage_class(&self, name: &str) -> std::result::Result<Option<StorageClass>, Status> {
        Ok(self.registered_storage_class(name))
    }
}

/// Wraps volumes for the Actor status, unset when there are none.
pub fn external_volumes_status(vols: Vec<pb::ExternalVolumeStatus>) -> Option<pb::ExternalVolumes> {
    (!vols.is_empty()).then_some(pb::ExternalVolumes { volumes: vols })
}

/// The volumes recorded on an Actor.
pub fn actor_volumes(actor: &pb::Actor) -> &[pb::ExternalVolumeStatus] {
    actor
        .status
        .as_ref()
        .and_then(|s| s.external_volumes.as_ref())
        .map(|v| v.volumes.as_slice())
        .unwrap_or(&[])
}

fn external_template(v: &pb::Volume) -> Option<&pb::ExternalVolumeTemplate> {
    v.external.as_ref().and_then(|e| e.template.as_ref())
}

/// The Actor's volume records before provisioning: one per external template
/// volume, typed by its storage class's provisioner.
pub async fn initial_external_volumes(
    provider: &dyn VolumeProvider,
    template: &pb::ActorTemplate,
) -> Result<Vec<pb::ExternalVolumeStatus>> {
    let mut out = Vec::new();
    for vol in &template.volumes {
        if let Some(t) = external_template(vol) {
            let sc = provider
                .storage_class(&t.storage_class_name)
                .await?
                .ok_or_else(|| {
                    status::failed_precondition(format!(
                        "StorageClass {:?} not found",
                        t.storage_class_name
                    ))
                })?;
            out.push(pb::ExternalVolumeStatus {
                name: vol.name.clone(),
                volume_type: sc.provisioner,
                ..Default::default()
            });
        }
    }
    Ok(out)
}

/// The storage volume name used for an Actor's volume.
pub fn actor_volume_id(actor_uid: &str, volume_name: &str) -> String {
    format!("hivekeep-{actor_uid}-{volume_name}")
}

/// Provisions the volumes that have no storage id yet. Returns the volume
/// records as far as it got, alongside the error if any, so progress can be
/// persisted.
pub async fn create_actor_volumes(
    provider: &dyn VolumeProvider,
    actor_uid: &str,
    template: &pb::ActorTemplate,
    volumes: &[pb::ExternalVolumeStatus],
) -> (Vec<pb::ExternalVolumeStatus>, Option<Error>) {
    let mut out = Vec::with_capacity(volumes.len());
    for (idx, vol) in volumes.iter().enumerate() {
        match provision_one(provider, actor_uid, template, vol).await {
            Ok(v) => out.push(v),
            Err(e) => {
                out.extend_from_slice(&volumes[idx..]);
                return (out, Some(e));
            }
        }
    }
    (out, None)
}

async fn provision_one(
    provider: &dyn VolumeProvider,
    actor_uid: &str,
    template: &pb::ActorTemplate,
    vol: &pb::ExternalVolumeStatus,
) -> Result<pb::ExternalVolumeStatus> {
    let spec = template
        .volumes
        .iter()
        .find(|t| t.name == vol.name)
        .and_then(external_template)
        .ok_or_else(|| status::not_found(format!("volume {:?} not found in template", vol.name)))?;
    if !vol.storage_volume_id.is_empty() {
        return Ok(vol.clone());
    }
    let sc = provider
        .storage_class(&spec.storage_class_name)
        .await?
        .ok_or_else(|| {
            status::failed_precondition(format!(
                "StorageClass {:?} not found",
                spec.storage_class_name
            ))
        })?;
    if sc.provisioner != vol.volume_type {
        return Err(status::failed_precondition(format!(
            "volume {:?} has mismatched type {:?} (expected {:?} from StorageClass {:?})",
            vol.name, vol.volume_type, sc.provisioner, spec.storage_class_name
        )));
    }
    let plugin = provider.plugin(&vol.volume_type).await.map_err(|m| {
        status::failed_precondition(format!(
            "failed to get volume plugin for driver {:?}: {m}",
            sc.provisioner
        ))
    })?;
    let created = plugin
        .create_volume(
            &actor_volume_id(actor_uid, &vol.name),
            spec.capacity_bytes,
            &sc.parameters,
        )
        .await
        .map_err(|s| {
            status::internal(format!(
                "failed to create volume {:?}: {}",
                vol.name,
                s.message()
            ))
        })?;
    if created.volume_id.is_empty() {
        return Err(status::internal(format!(
            "volume plugin returned no ID for volume {:?}",
            vol.name
        )));
    }
    Ok(pb::ExternalVolumeStatus {
        name: vol.name.clone(),
        storage_volume_id: created.volume_id,
        volume_type: sc.provisioner,
        volume_context: created.volume_context,
    })
}

/// Whether some container mounts `volume_name`.
pub fn is_volume_mounted(volume_name: &str, template: &pb::ActorTemplate) -> bool {
    template
        .containers
        .iter()
        .any(|c| c.volume_mounts.iter().any(|m| m.name == volume_name))
}

/// The Actor's volumes that the template declares and mounts.
pub fn mounted_actor_volumes(
    volumes: &[pb::ExternalVolumeStatus],
    template: &pb::ActorTemplate,
) -> Vec<pb::ExternalVolumeStatus> {
    volumes
        .iter()
        .filter(|v| {
            template.volumes.iter().any(|t| t.name == v.name)
                && is_volume_mounted(&v.name, template)
        })
        .cloned()
        .collect()
}

/// Deletes every volume; volumes never provisioned are deleted by their
/// requested name. Every volume is attempted.
pub async fn delete_actor_volumes(
    provider: &dyn VolumeProvider,
    actor_uid: &str,
    volumes: &[pb::ExternalVolumeStatus],
) -> std::result::Result<(), String> {
    if actor_uid.is_empty() {
        return Err("actor uid is required".into());
    }
    let mut errs = Vec::new();
    for vol in volumes {
        let id = if vol.storage_volume_id.is_empty() {
            actor_volume_id(actor_uid, &vol.name)
        } else {
            vol.storage_volume_id.clone()
        };
        match provider.plugin(&vol.volume_type).await {
            Err(m) => errs.push(format!(
                "failed to get volume plugin for {:?}: {m}",
                vol.volume_type
            )),
            Ok(p) => match p.delete_volume(&id).await {
                Ok(()) => {}
                Err(s) if s.code() == Code::NotFound => {
                    tracing::warn!(volume_id = %id, "volume not found during delete; assuming deleted")
                }
                Err(s) => errs.push(format!("failed to delete volume {id:?}: {}", s.message())),
            },
        }
    }
    if errs.is_empty() {
        Ok(())
    } else {
        Err(errs.join("; "))
    }
}

/// Detaches the Actor's volumes from its assigned node: those the template
/// mounts, or every recorded one when the template is gone.
pub async fn detach_actor_volumes(
    provider: &dyn VolumeProvider,
    actor: &pb::Actor,
    template: Option<&pb::ActorTemplate>,
) -> Result<()> {
    let node = actor
        .status
        .as_ref()
        .map(|s| s.assigned_node.as_str())
        .unwrap_or("");
    if node.is_empty() {
        return Ok(());
    }
    let vols = match template {
        Some(t) => mounted_actor_volumes(actor_volumes(actor), t),
        None => actor_volumes(actor).to_vec(),
    };
    let mut errs = Vec::new();
    for vol in vols.iter().filter(|v| !v.storage_volume_id.is_empty()) {
        match provider.plugin(&vol.volume_type).await {
            Err(m) => errs.push(format!(
                "failed to get volume plugin for {:?}: {m}",
                vol.volume_type
            )),
            Ok(p) => match p.detach_volume(&vol.storage_volume_id, node).await {
                Ok(()) => {}
                Err(s) if s.code() == Code::NotFound => {}
                Err(s) => errs.push(format!(
                    "failed to detach volume {:?} from node {node:?}: {}",
                    vol.storage_volume_id,
                    s.message()
                )),
            },
        }
    }
    if errs.is_empty() {
        Ok(())
    } else {
        Err(status::internal(errs.join("; ")))
    }
}

/// Attaches the Actor's mounted volumes to `node`, returning each driver's
/// publish context by volume name. Refuses before attaching anything if a
/// volume is not provisioned.
pub async fn attach_actor_volumes(
    provider: &dyn VolumeProvider,
    actor: &pb::Actor,
    template: &pb::ActorTemplate,
    node: &str,
) -> Result<HashMap<String, HashMap<String, String>>> {
    let vols = mounted_actor_volumes(actor_volumes(actor), template);
    if let Some(v) = vols.iter().find(|v| v.storage_volume_id.is_empty()) {
        return Err(status::failed_precondition(format!(
            "volume {:?} has no storage volume ID to attach",
            v.name
        )));
    }
    let mut out = HashMap::new();
    for vol in vols {
        let plugin = provider.plugin(&vol.volume_type).await.map_err(|m| {
            status::internal(format!(
                "failed to get volume plugin for {:?}: {m}",
                vol.volume_type
            ))
        })?;
        let ctx = plugin
            .attach_volume(&vol.storage_volume_id, node)
            .await
            .map_err(|s| {
                status::internal(format!(
                    "failed to attach volume {:?} to node {node:?}: {}",
                    vol.storage_volume_id,
                    s.message()
                ))
            })?;
        if !ctx.is_empty() {
            out.insert(vol.name.clone(), ctx);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Mock;
    #[async_trait]
    impl VolumePlugin for Mock {
        async fn create_volume(
            &self,
            name: &str,
            _: i64,
            _: &HashMap<String, String>,
        ) -> std::result::Result<CreatedVolume, Status> {
            if name.ends_with("-bad") {
                return Err(Status::internal("boom"));
            }
            Ok(CreatedVolume {
                volume_id: format!("id-{name}"),
                volume_context: HashMap::new(),
            })
        }
        async fn attach_volume(
            &self,
            id: &str,
            _: &str,
        ) -> std::result::Result<HashMap<String, String>, Status> {
            Ok([("dev".to_string(), id.to_string())].into())
        }
        async fn detach_volume(&self, _: &str, _: &str) -> std::result::Result<(), Status> {
            Err(Status::not_found("gone"))
        }
        async fn delete_volume(&self, _: &str) -> std::result::Result<(), Status> {
            Ok(())
        }
    }

    fn template(names: &[&str]) -> pb::ActorTemplate {
        pb::ActorTemplate {
            volumes: names
                .iter()
                .map(|n| pb::Volume {
                    name: n.to_string(),
                    external: Some(pb::ExternalVolumeSource {
                        template: Some(pb::ExternalVolumeTemplate {
                            capacity_bytes: 1,
                            storage_class_name: "std".into(),
                        }),
                    }),
                    ..Default::default()
                })
                .collect(),
            containers: vec![pb::Container {
                volume_mounts: names
                    .iter()
                    .map(|n| pb::VolumeMount {
                        name: n.to_string(),
                        mount_path: format!("/{n}"),
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn create_reports_partial_progress() {
        let p = StaticVolumes::new();
        p.add_plugin("mock", Arc::new(Mock));
        p.add_storage_class(
            "std",
            StorageClass {
                provisioner: "mock".into(),
                parameters: HashMap::new(),
            },
        );
        let t = template(&["a", "bad", "c"]);
        let init = initial_external_volumes(&p, &t).await.unwrap();
        assert_eq!(init.len(), 3);
        let (vols, err) = create_actor_volumes(&p, "u", &t, &init).await;
        assert!(err.is_some());
        assert_eq!(vols.len(), 3);
        assert_eq!(vols[0].storage_volume_id, "id-hivekeep-u-a");
        assert!(vols[1].storage_volume_id.is_empty());

        let t = template(&["a"]);
        let (vols, err) = create_actor_volumes(
            &p,
            "u",
            &t,
            &initial_external_volumes(&p, &t).await.unwrap(),
        )
        .await;
        assert!(err.is_none());
        let actor = pb::Actor {
            status: Some(pb::ActorStatus {
                external_volumes: external_volumes_status(vols),
                assigned_node: "n".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let ctx = attach_actor_volumes(&p, &actor, &t, "n").await.unwrap();
        assert_eq!(ctx["a"]["dev"], "id-hivekeep-u-a");
        detach_actor_volumes(&p, &actor, Some(&t)).await.unwrap();
        delete_actor_volumes(&p, "u", actor_volumes(&actor))
            .await
            .unwrap();
        assert!(initial_external_volumes(&StaticVolumes::new(), &t)
            .await
            .is_err());
    }
}

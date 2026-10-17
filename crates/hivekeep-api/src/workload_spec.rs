//! The keeper's description of an Actor's containers and volumes, built
//! from its template.

use std::collections::HashMap;

use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::keeper::v1 as kp;

use crate::convert;
use crate::error::{status, Result};
use crate::volumes::{actor_volumes, is_volume_mounted};

/// Builds the workload spec of `actor` from `template`. External volumes
/// the template mounts are included with the storage ids recorded on the
/// Actor and the publish contexts of the current attach, if any.
pub fn from_template(
    template: &pb::ActorTemplate,
    actor: &pb::Actor,
    publish_contexts: &HashMap<String, HashMap<String, String>>,
) -> Result<kp::WorkloadSpec> {
    let mut spec = kp::WorkloadSpec::default();
    for vol in &template.volumes {
        if vol.durable_dir.is_some() {
            spec.volumes.push(kp::Volume {
                name: vol.name.clone(),
                durable_dir: Some(kp::DurableDirVolumeSource {}),
                ..Default::default()
            });
        } else if let Some(si) = &vol.system_info {
            let mut out = kp::SystemInfoVolumeSource::default();
            for ds in &si.data_sources {
                if let Some(md) = &ds.actor_metadata {
                    out.data_sources.push(kp::SystemInfoDataSource {
                        actor_metadata: Some(kp::ActorMetadataDataSource {
                            items: md
                                .items
                                .iter()
                                .map(|i| kp::ActorMetadataItem {
                                    field: convert::metadata_field_to_keeper(i.field) as i32,
                                    path: i.path.clone(),
                                })
                                .collect(),
                        }),
                        trust_bundle: None,
                    });
                } else if let Some(tb) = &ds.trust_bundle {
                    out.data_sources.push(kp::SystemInfoDataSource {
                        actor_metadata: None,
                        trust_bundle: Some(kp::TrustBundleDataSource {
                            path: tb.path.clone(),
                            names: tb.names.clone(),
                        }),
                    });
                }
            }
            spec.volumes.push(kp::Volume {
                name: vol.name.clone(),
                system_info: Some(out),
                ..Default::default()
            });
        } else if let Some(img) = &vol.image {
            spec.volumes.push(kp::Volume {
                name: vol.name.clone(),
                image: Some(kp::ImageVolumeSource {
                    reference: img.reference.clone(),
                }),
                ..Default::default()
            });
        }
    }
    // Callers that must check the ExternalVolumes gate do so themselves.
    for vol in &template.volumes {
        if vol
            .external
            .as_ref()
            .and_then(|e| e.template.as_ref())
            .is_none()
            || !is_volume_mounted(&vol.name, template)
        {
            continue;
        }
        let rec = actor_volumes(actor).iter().find(|v| v.name == vol.name);
        let Some(rec) = rec.filter(|r| !r.storage_volume_id.is_empty()) else {
            let name = actor
                .metadata
                .as_ref()
                .map(|m| m.name.as_str())
                .unwrap_or("");
            return Err(status::failed_precondition(format!(
                "volume {} not found for actor {name}",
                vol.name
            )));
        };
        spec.volumes.push(kp::Volume {
            name: vol.name.clone(),
            external: Some(kp::ExternalVolumeSource {
                storage_volume_id: rec.storage_volume_id.clone(),
                volume_type: rec.volume_type.clone(),
                volume_context: rec.volume_context.clone(),
                publish_context: publish_contexts.get(&vol.name).cloned().unwrap_or_default(),
            }),
            ..Default::default()
        });
    }
    for c in &template.containers {
        spec.containers.push(kp::Container {
            name: c.name.clone(),
            image: c.image.clone(),
            command: c.command.clone(),
            args: c.args.clone(),
            env: c
                .env
                .iter()
                .map(|e| kp::EnvVar {
                    name: e.name.clone(),
                    value: e.value.clone(),
                })
                .collect(),
            wakeup_probe: c.wakeup_probe.as_ref().map(|p| kp::WakeupProbe {
                http_get: p.http_get.as_ref().map(|g| kp::HttpGetAction {
                    path: g.path.clone(),
                    port: g.port,
                }),
                timeout_seconds: p.timeout_seconds,
            }),
            volume_mounts: c
                .volume_mounts
                .iter()
                .map(|m| kp::VolumeMount {
                    name: m.name.clone(),
                    mount_path: m.mount_path.clone(),
                })
                .collect(),
            security_context: security_context(c.security_context.as_ref()),
            resources: convert::resources_to_keeper(c.resources.as_ref()),
        });
    }
    Ok(spec)
}

fn security_context(sc: Option<&pb::SecurityContext>) -> Option<kp::SecurityContext> {
    let caps = sc?.capabilities.as_ref()?;
    if caps.add.is_empty() && caps.drop.is_empty() {
        return None;
    }
    Some(kp::SecurityContext {
        capabilities: Some(kp::Capabilities {
            add: caps.add.clone(),
            drop: caps.drop.clone(),
        }),
    })
}

/// The spec to terminate an Actor with when its template cannot be
/// resolved: only its provisioned external volumes, which the keeper still
/// unmounts.
pub fn fallback(actor: &pb::Actor, external_volumes_enabled: bool) -> kp::WorkloadSpec {
    let mut spec = kp::WorkloadSpec::default();
    if external_volumes_enabled {
        for v in actor_volumes(actor)
            .iter()
            .filter(|v| !v.storage_volume_id.is_empty())
        {
            spec.volumes.push(kp::Volume {
                name: v.name.clone(),
                external: Some(kp::ExternalVolumeSource {
                    storage_volume_id: v.storage_volume_id.clone(),
                    volume_type: v.volume_type.clone(),
                    volume_context: v.volume_context.clone(),
                    publish_context: HashMap::new(),
                }),
                ..Default::default()
            });
        }
    }
    spec
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_containers_and_volumes() {
        let t = pb::ActorTemplate {
            volumes: vec![
                pb::Volume {
                    name: "d".into(),
                    durable_dir: Some(pb::DurableDirVolumeSource {}),
                    ..Default::default()
                },
                pb::Volume {
                    name: "ext".into(),
                    external: Some(pb::ExternalVolumeSource {
                        template: Some(pb::ExternalVolumeTemplate::default()),
                    }),
                    ..Default::default()
                },
            ],
            containers: vec![pb::Container {
                name: "main".into(),
                image: "img@sha256:00".into(),
                env: vec![pb::EnvVar {
                    name: "A".into(),
                    value: "$(B)".into(),
                }],
                volume_mounts: vec![pb::VolumeMount {
                    name: "ext".into(),
                    mount_path: "/x".into(),
                }],
                security_context: Some(pb::SecurityContext {
                    capabilities: Some(pb::Capabilities::default()),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut actor = pb::Actor::default();
        assert!(from_template(&t, &actor, &HashMap::new()).is_err());
        actor.status = Some(pb::ActorStatus {
            external_volumes: Some(pb::ExternalVolumes {
                volumes: vec![pb::ExternalVolumeStatus {
                    name: "ext".into(),
                    storage_volume_id: "v1".into(),
                    volume_type: "csi".into(),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        });
        let spec = from_template(&t, &actor, &HashMap::new()).unwrap();
        assert_eq!(spec.volumes.len(), 2);
        assert_eq!(spec.containers[0].env[0].value, "$(B)");
        assert!(spec.containers[0].security_context.is_none());
        assert_eq!(fallback(&actor, true).volumes.len(), 1);
        assert!(fallback(&actor, false).volumes.is_empty());
    }
}

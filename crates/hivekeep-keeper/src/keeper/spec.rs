//! The worker-facing projection of a workload, and the per-container
//! preparation Run and Restore share.

use std::collections::HashMap;
use std::path::Path;

use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::worker::v1 as wpb;
use tonic::Status;

use crate::context::Ctx;
use crate::oci::{
    prepare_oci_directory, resolve_actor_env, resolve_capabilities, resolve_process_args,
    ImageCache,
};
use crate::paths::NodePaths;

/// Projects the keeper's workload spec onto the worker's. `container_specs`
/// holds each container's process, indexed like the spec's containers, or
/// is `None` for RPCs that start no process.
pub fn build_worker_workload_spec(
    spec: Option<&pb::WorkloadSpec>,
    container_specs: Option<&[wpb::ContainerSpec]>,
) -> Result<wpb::WorkloadSpec, String> {
    let empty = pb::WorkloadSpec::default();
    let spec = spec.unwrap_or(&empty);
    let mut volumes: HashMap<&str, &pb::Volume> = HashMap::new();
    for v in &spec.volumes {
        if volumes.insert(v.name.as_str(), v).is_some() {
            return Err(format!(
                "duplicate volume name {:?} in workload spec",
                v.name
            ));
        }
    }
    let mut out = wpb::WorkloadSpec::default();
    for (i, c) in spec.containers.iter().enumerate() {
        let mut ctr = wpb::Container {
            name: c.name.clone(),
            wakeup_probe: c.wakeup_probe.as_ref().map(to_worker_wakeup_probe),
            ..Default::default()
        };
        for m in &c.volume_mounts {
            let v = volumes.get(m.name.as_str()).ok_or_else(|| {
                format!(
                    "container {:?} mounts volume {:?} which is not defined in workload volumes",
                    c.name, m.name
                )
            })?;
            let mount = wpb::VolumeMount {
                name: m.name.clone(),
                mount_path: m.mount_path.clone(),
            };
            if v.durable_dir.is_some() {
                ctr.durable_dir_volume_mounts.push(mount);
            } else if v.external.is_some() {
                ctr.external_volume_mounts.push(mount);
            } else if v.system_info.is_some() {
                ctr.system_info_volume_mounts.push(mount);
            } else if v.image.is_some() {
                ctr.image_volume_mounts.push(mount);
            } else {
                return Err(format!(
                    "container {:?} mounts volume {:?} with no source set",
                    c.name, m.name
                ));
            }
        }
        if let Some(specs) = container_specs {
            ctr.container_spec = specs.get(i).cloned();
        }
        out.containers.push(ctr);
    }
    Ok(out)
}

/// The worker's form of resource requirements.
pub fn to_worker_resources(
    r: Option<&pb::ResourceRequirements>,
) -> Option<wpb::ResourceRequirements> {
    r.map(|r| wpb::ResourceRequirements {
        limits: r.limits.as_ref().map(|l| wpb::Resources {
            cpu_millicores: l.cpu_millicores,
            memory_bytes: l.memory_bytes,
        }),
    })
}

/// The worker's form of the egress gateway.
pub fn to_worker_egress_gateway(g: Option<&pb::EgressGateway>) -> Option<wpb::EgressGateway> {
    g.map(|g| wpb::EgressGateway {
        address: g.address.clone(),
    })
}

/// The worker's form of a wakeup probe.
pub fn to_worker_wakeup_probe(p: &pb::WakeupProbe) -> wpb::WakeupProbe {
    wpb::WakeupProbe {
        http_get: p.http_get.as_ref().map(|h| wpb::HttpGetAction {
            path: h.path.clone(),
            port: h.port,
        }),
        timeout_seconds: p.timeout_seconds,
    }
}

/// Maps the keeper's fidelity onto the identical worker one; anything else
/// goes out unspecified for the worker to reject.
pub fn to_worker_fidelity(f: i32) -> i32 {
    match pb::SnapshotFidelity::try_from(f) {
        Ok(pb::SnapshotFidelity::Volumes) => wpb::SnapshotFidelity::Volumes as i32,
        Ok(pb::SnapshotFidelity::Rootfs) => wpb::SnapshotFidelity::Rootfs as i32,
        Ok(pb::SnapshotFidelity::Memory) => wpb::SnapshotFidelity::Memory as i32,
        _ => wpb::SnapshotFidelity::Unspecified as i32,
    }
}

/// Creates a durable-dir volume's directory world-writable, as an emptyDir
/// is, since one actor's containers can run as different users. A restore
/// then re-applies the mode the snapshot recorded.
pub fn prepare_durable_dir_volume(path: &Path) -> Result<(), Status> {
    crate::atomicfile::create_dir_all_mode(path, 0o777)
        .map_err(|e| Status::internal(format!("while creating {}: {e}", path.display())))?;
    crate::atomicfile::chmod(path, 0o777)
        .map_err(|e| Status::internal(format!("while setting mode of {}: {e}", path.display())))
}

/// Pulls images and assembles every container's OCI bundle in parallel,
/// returning each container's process spec in spec order.
pub async fn prepare_oci_bundles(
    ctx: &Ctx,
    images: &dyn ImageCache,
    paths: &NodePaths,
    actor_uid: &str,
    spec: Option<&pb::WorkloadSpec>,
) -> Result<Vec<wpb::ContainerSpec>, Status> {
    let empty = pb::WorkloadSpec::default();
    let spec = spec.unwrap_or(&empty);
    for v in &spec.volumes {
        if v.durable_dir.is_some() {
            prepare_durable_dir_volume(&paths.durable_dir_volume_mount_point(actor_uid, &v.name))?;
        }
    }
    let futs = spec.containers.iter().map(|c| async move {
        let envs: Vec<String> = c
            .env
            .iter()
            .map(|e| format!("{}={}", e.name, e.value))
            .collect();
        let cfg = prepare_oci_directory(ctx, images, paths, actor_uid, c, &spec.volumes)
            .await
            .map_err(|e| {
                crate::status::wrap(format!("while creating {:?} OCI bundle", c.name), e)
            })?;
        let args = resolve_process_args(Some(&cfg), &c.command, &c.args).map_err(|e| {
            Status::internal(format!(
                "while resolving process args for container {:?}: {e}",
                c.name
            ))
        })?;
        Ok::<_, Status>(wpb::ContainerSpec {
            args,
            env: resolve_actor_env(Some(&cfg), &envs),
            capabilities: resolve_capabilities(
                c.security_context
                    .as_ref()
                    .and_then(|s| s.capabilities.as_ref()),
            ),
            resources: to_worker_resources(c.resources.as_ref()),
        })
    });
    futures::future::try_join_all(futs).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> pb::WorkloadSpec {
        pb::WorkloadSpec {
            containers: vec![pb::Container {
                name: "app".into(),
                image: "img".into(),
                volume_mounts: vec![
                    pb::VolumeMount {
                        name: "dd".into(),
                        mount_path: "/data".into(),
                    },
                    pb::VolumeMount {
                        name: "info".into(),
                        mount_path: "/info".into(),
                    },
                ],
                wakeup_probe: Some(pb::WakeupProbe {
                    http_get: Some(pb::HttpGetAction {
                        path: "/healthz".into(),
                        port: 8080,
                    }),
                    timeout_seconds: 5,
                }),
                ..Default::default()
            }],
            volumes: vec![
                pb::Volume {
                    name: "dd".into(),
                    durable_dir: Some(pb::DurableDirVolumeSource {}),
                    ..Default::default()
                },
                pb::Volume {
                    name: "info".into(),
                    system_info: Some(pb::SystemInfoVolumeSource::default()),
                    ..Default::default()
                },
            ],
        }
    }

    #[test]
    fn projects_mounts_by_volume_kind() {
        let s = spec();
        let out = build_worker_workload_spec(Some(&s), None).unwrap();
        let c = &out.containers[0];
        assert_eq!(c.durable_dir_volume_mounts[0].mount_path, "/data");
        assert_eq!(c.system_info_volume_mounts[0].mount_path, "/info");
        assert!(c.external_volume_mounts.is_empty() && c.image_volume_mounts.is_empty());
        assert_eq!(
            c.wakeup_probe
                .as_ref()
                .unwrap()
                .http_get
                .as_ref()
                .unwrap()
                .port,
            8080
        );
        assert!(c.container_spec.is_none());
        let specs = vec![wpb::ContainerSpec {
            args: vec!["x".into()],
            ..Default::default()
        }];
        let out = build_worker_workload_spec(Some(&s), Some(&specs)).unwrap();
        assert_eq!(
            out.containers[0].container_spec.as_ref().unwrap().args,
            vec!["x"]
        );
    }

    #[test]
    fn rejects_bad_volume_references() {
        let mut s = spec();
        s.volumes.push(s.volumes[0].clone());
        assert!(build_worker_workload_spec(Some(&s), None)
            .unwrap_err()
            .contains("duplicate"));
        let mut s = spec();
        s.volumes.truncate(1);
        assert!(build_worker_workload_spec(Some(&s), None)
            .unwrap_err()
            .contains("not defined"));
        let mut s = spec();
        s.volumes[0].durable_dir = None;
        assert!(build_worker_workload_spec(Some(&s), None)
            .unwrap_err()
            .contains("no source"));
    }

    #[test]
    fn conversions() {
        assert_eq!(
            to_worker_fidelity(pb::SnapshotFidelity::Rootfs as i32),
            wpb::SnapshotFidelity::Rootfs as i32
        );
        assert_eq!(to_worker_fidelity(9), 0);
        assert!(to_worker_resources(None).is_none());
        let r = to_worker_resources(Some(&pb::ResourceRequirements {
            limits: Some(pb::Resources {
                cpu_millicores: 500,
                memory_bytes: 1 << 30,
            }),
        }))
        .unwrap();
        assert_eq!(r.limits.unwrap().cpu_millicores, 500);
        assert_eq!(
            to_worker_egress_gateway(Some(&pb::EgressGateway {
                address: "a:1".into()
            }))
            .unwrap()
            .address,
            "a:1"
        );
    }
}

#[cfg(test)]
#[path = "spec_behavior_tests.rs"]
mod behavior_tests;

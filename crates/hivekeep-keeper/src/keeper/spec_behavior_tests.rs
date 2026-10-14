use super::*;
use crate::context::Ctx;
use crate::oci::behavior_tests::DirImageCache;
use crate::oci::ImageConfig;

fn ctr(name: &str) -> pb::Container {
    pb::Container {
        name: name.into(),
        ..Default::default()
    }
}

fn mount(name: &str, path: &str) -> pb::VolumeMount {
    pb::VolumeMount {
        name: name.into(),
        mount_path: path.into(),
    }
}

fn wmount(name: &str, path: &str) -> wpb::VolumeMount {
    wpb::VolumeMount {
        name: name.into(),
        mount_path: path.into(),
    }
}

fn vol(name: &str) -> pb::Volume {
    pb::Volume {
        name: name.into(),
        ..Default::default()
    }
}

fn durable(name: &str) -> pb::Volume {
    pb::Volume {
        durable_dir: Some(pb::DurableDirVolumeSource {}),
        ..vol(name)
    }
}

fn external(name: &str) -> pb::Volume {
    pb::Volume {
        external: Some(Default::default()),
        ..vol(name)
    }
}

/// Each container's spec carries its resolved args, env, capabilities and
/// resource limits.
#[tokio::test]
async fn prepare_oci_bundles_fills_container_spec() {
    let d = tempfile::tempdir().unwrap();
    let paths = NodePaths::new(d.path().join("node"));
    let mut images = DirImageCache::new(&d.path().join("layers"));
    images.push(
        "registry.test/actor:v1",
        ImageConfig::default(),
        &["bin/app"],
    );
    let spec = pb::WorkloadSpec {
        containers: vec![pb::Container {
            name: "app".into(),
            image: "registry.test/actor:v1".into(),
            command: vec!["/bin/app".into()],
            args: vec!["serve".into()],
            env: vec![pb::EnvVar {
                name: "GREETING".into(),
                value: "hello".into(),
            }],
            security_context: Some(pb::SecurityContext {
                capabilities: Some(pb::Capabilities {
                    add: vec!["NET_ADMIN".into()],
                    drop: vec!["KILL".into()],
                }),
            }),
            resources: Some(pb::ResourceRequirements {
                limits: Some(pb::Resources {
                    memory_bytes: 1 << 30,
                    cpu_millicores: 500,
                }),
            }),
            ..Default::default()
        }],
        volumes: vec![],
    };
    let got = prepare_oci_bundles(
        &Ctx::background(),
        &images,
        &paths,
        "actor-uid-1",
        Some(&spec),
    )
    .await
    .unwrap();
    let want = vec![wpb::ContainerSpec {
        args: vec!["/bin/app".into(), "serve".into()],
        env: vec![
            "GREETING=hello".into(),
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".into(),
        ],
        capabilities: vec![
            "CAP_AUDIT_WRITE".into(),
            "CAP_NET_ADMIN".into(),
            "CAP_NET_BIND_SERVICE".into(),
        ],
        resources: Some(wpb::ResourceRequirements {
            limits: Some(wpb::Resources {
                memory_bytes: 1 << 30,
                cpu_millicores: 500,
            }),
        }),
    }];
    assert_eq!(got, want);
}

/// Every valid fidelity maps to its worker counterpart.
#[test]
fn to_worker_fidelity_cases() {
    for (input, want) in [
        (
            pb::SnapshotFidelity::Memory as i32,
            wpb::SnapshotFidelity::Memory,
        ),
        (
            pb::SnapshotFidelity::Volumes as i32,
            wpb::SnapshotFidelity::Volumes,
        ),
        (
            pb::SnapshotFidelity::Rootfs as i32,
            wpb::SnapshotFidelity::Rootfs,
        ),
        (
            pb::SnapshotFidelity::Unspecified as i32,
            wpb::SnapshotFidelity::Unspecified,
        ),
        (99, wpb::SnapshotFidelity::Unspecified),
    ] {
        assert_eq!(to_worker_fidelity(input), want as i32, "{input}");
    }
}

#[test]
fn build_worker_workload_spec_forwards_wakeup_probe() {
    let input = pb::WorkloadSpec {
        containers: vec![
            pb::Container {
                name: "with-probe".into(),
                image: "main".into(),
                wakeup_probe: Some(pb::WakeupProbe {
                    http_get: Some(pb::HttpGetAction {
                        path: "/health".into(),
                        port: 8080,
                    }),
                    timeout_seconds: 45,
                }),
                ..Default::default()
            },
            ctr("without-probe"),
        ],
        volumes: vec![],
    };
    let want = wpb::WorkloadSpec {
        containers: vec![
            wpb::Container {
                name: "with-probe".into(),
                wakeup_probe: Some(wpb::WakeupProbe {
                    http_get: Some(wpb::HttpGetAction {
                        path: "/health".into(),
                        port: 8080,
                    }),
                    timeout_seconds: 45,
                }),
                ..Default::default()
            },
            wpb::Container {
                name: "without-probe".into(),
                ..Default::default()
            },
        ],
    };
    assert_eq!(
        build_worker_workload_spec(Some(&input), None).unwrap(),
        want
    );
}

#[test]
fn build_worker_workload_spec_forwards_container_specs() {
    let input = pb::WorkloadSpec {
        containers: vec![ctr("main"), ctr("sidecar")],
        volumes: vec![],
    };
    let specs = vec![
        wpb::ContainerSpec {
            args: vec!["/app".into(), "serve".into()],
            ..Default::default()
        },
        wpb::ContainerSpec {
            args: vec!["/sidecar".into()],
            ..Default::default()
        },
    ];
    let want = wpb::WorkloadSpec {
        containers: vec![
            wpb::Container {
                name: "main".into(),
                container_spec: Some(specs[0].clone()),
                ..Default::default()
            },
            wpb::Container {
                name: "sidecar".into(),
                container_spec: Some(specs[1].clone()),
                ..Default::default()
            },
        ],
    };
    assert_eq!(
        build_worker_workload_spec(Some(&input), Some(&specs)).unwrap(),
        want
    );
}

#[test]
fn build_worker_workload_spec_forwards_durable_dir_mounts() {
    let input = pb::WorkloadSpec {
        volumes: vec![
            durable("data"),
            durable("cache"),
            external("scratch"),
            pb::Volume {
                system_info: Some(Default::default()),
                ..vol("system-info")
            },
        ],
        containers: vec![
            pb::Container {
                volume_mounts: vec![
                    mount("data", "/home/counter"),
                    mount("cache", "/var/cache"),
                    mount("scratch", "/scratch"),
                    mount("system-info", "/run/hk"),
                ],
                ..ctr("main")
            },
            pb::Container {
                volume_mounts: vec![mount("data", "/shared")],
                ..ctr("sidecar")
            },
            ctr("no-volumes"),
        ],
    };
    // The worker needs the volume name as well as the path: the name picks
    // the per-volume directory on the host.
    let want = wpb::WorkloadSpec {
        containers: vec![
            wpb::Container {
                name: "main".into(),
                durable_dir_volume_mounts: vec![
                    wmount("data", "/home/counter"),
                    wmount("cache", "/var/cache"),
                ],
                external_volume_mounts: vec![wmount("scratch", "/scratch")],
                system_info_volume_mounts: vec![wmount("system-info", "/run/hk")],
                ..Default::default()
            },
            wpb::Container {
                name: "sidecar".into(),
                durable_dir_volume_mounts: vec![wmount("data", "/shared")],
                ..Default::default()
            },
            wpb::Container {
                name: "no-volumes".into(),
                ..Default::default()
            },
        ],
    };
    assert_eq!(
        build_worker_workload_spec(Some(&input), None).unwrap(),
        want
    );
}

#[test]
fn build_worker_workload_spec_validation() {
    let one = |volumes: Vec<pb::Volume>, m: &str| pb::WorkloadSpec {
        volumes,
        containers: vec![pb::Container {
            volume_mounts: vec![mount(m, "/data")],
            ..ctr("ctr")
        }],
    };
    for (name, input, want) in [
        (
            "missing volume definition",
            one(vec![durable("data")], "missing-vol"),
            r#"container "ctr" mounts volume "missing-vol" which is not defined in workload volumes"#,
        ),
        (
            "unsupported volume source",
            one(vec![vol("data")], "data"),
            r#"container "ctr" mounts volume "data" with no source set"#,
        ),
        (
            "duplicate volume names",
            one(vec![durable("data"), external("data")], "data"),
            r#"duplicate volume name "data" in workload spec"#,
        ),
    ] {
        let err = build_worker_workload_spec(Some(&input), None).unwrap_err();
        assert!(err.contains(want), "{name}: {err}");
    }
}

#[test]
fn to_worker_egress_gateway_cases() {
    assert!(to_worker_egress_gateway(None).is_none());
    let got = to_worker_egress_gateway(Some(&pb::EgressGateway {
        address: "egress.example:443".into(),
    }));
    assert_eq!(
        got,
        Some(wpb::EgressGateway {
            address: "egress.example:443".into()
        })
    );
}

/// Image volumes go on their own mount list, apart from durable-dir ones.
#[test]
fn build_worker_workload_spec_image_volume_mounts() {
    let input = pb::WorkloadSpec {
        volumes: vec![
            pb::Volume {
                image: Some(Default::default()),
                ..vol("agent")
            },
            durable("data"),
            external("ext"),
        ],
        containers: vec![pb::Container {
            volume_mounts: vec![
                mount("agent", "/hk"),
                mount("data", "/var/data"),
                mount("ext", "/mnt/ext"),
            ],
            ..ctr("app")
        }],
    };
    let got = build_worker_workload_spec(Some(&input), None).unwrap();
    assert_eq!(got.containers.len(), 1);
    let c = &got.containers[0];
    assert_eq!(c.image_volume_mounts, vec![wmount("agent", "/hk")]);
    assert_eq!(
        c.durable_dir_volume_mounts,
        vec![wmount("data", "/var/data")]
    );
}

/// The mode is set explicitly, since the umask narrows a fresh directory.
#[cfg(unix)]
#[test]
fn prepare_durable_dir_volume_is_world_writable() {
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("vol");
    prepare_durable_dir_volume(&p).unwrap();
    let mode = std::fs::metadata(&p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o777, "mode = {mode:o}");
}

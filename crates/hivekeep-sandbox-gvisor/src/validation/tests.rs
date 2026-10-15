use super::*;
use hivekeep_core::field::ErrorKind;

type Want = Vec<(ErrorKind, String, Option<&'static str>)>;

fn got(errs: &ErrorList) -> Want {
    errs.iter()
        .map(|e| (e.kind, e.path.as_str().to_string(), e.origin))
        .collect()
}

fn w(
    kind: ErrorKind,
    path: &str,
    origin: Option<&'static str>,
) -> (ErrorKind, String, Option<&'static str>) {
    (kind, path.to_string(), origin)
}

use ErrorKind::*;
const DNS: Option<&str> = Some("format=dns-label");

fn mount(name: &str, path: &str) -> pb::VolumeMount {
    pb::VolumeMount {
        name: name.into(),
        mount_path: path.into(),
    }
}

fn full_container() -> pb::Container {
    pb::Container {
        name: "main".into(),
        wakeup_probe: Some(pb::WakeupProbe {
            http_get: Some(pb::HttpGetAction {
                path: "/healthz".into(),
                port: 8080,
            }),
            timeout_seconds: 30,
        }),
        durable_dir_volume_mounts: vec![mount("data", "/data")],
        external_volume_mounts: vec![mount("ext", "/ext")],
        system_info_volume_mounts: vec![mount("sysinfo", "/etc/sysinfo")],
        image_volume_mounts: vec![mount("img", "/img")],
        container_spec: None,
    }
}

fn full_spec() -> pb::WorkloadSpec {
    pb::WorkloadSpec {
        containers: vec![full_container()],
    }
}

fn full_actor_dirs() -> pb::ActorDirs {
    pb::ActorDirs {
        root_dir: "/actors/uid".into(),
        oci_bundle_dir: "/actors/uid/bundles".into(),
        checkpoint_dir: "/actors/uid/checkpoint".into(),
        restore_dir: "/actors/uid/restore".into(),
        durable_dir_volume_mounts_dir: "/actors/uid/durable".into(),
        system_info_volume_roots_dir: "/actors/uid/sysinfo".into(),
        external_volume_mounts_dir: "/actors/uid/volumes".into(),
        sandbox_assets_dir: "/actors/uid/sandbox-assets".into(),
    }
}

const UID: &str = "01234567-89ab-cdef-0123-456789abcdef";

fn valid_run(mutate: impl FnOnce(&mut pb::RunWorkloadRequest)) -> pb::RunWorkloadRequest {
    let mut r = pb::RunWorkloadRequest {
        hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: UID.into(),
        actor_template_hive: "team-a".into(),
        actor_template_name: "template-1".into(),
        runsc_path: "/opt/runsc".into(),
        spec: Some(full_spec()),
        runtime_asset_paths: [
            ("kata-kernel", "/opt/kernel"),
            ("virtiofsd", "/opt/virtiofsd"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect(),
        egress_gateway: Some(pb::EgressGateway {
            address: "gateway:443".into(),
        }),
        resources: Some(limits(1500, 1 << 30)),
        actor_dirs: Some(full_actor_dirs()),
    };
    mutate(&mut r);
    r
}

fn limits(cpu: i64, mem: i64) -> pb::ResourceRequirements {
    pb::ResourceRequirements {
        limits: Some(pb::Resources {
            cpu_millicores: cpu,
            memory_bytes: mem,
        }),
    }
}

fn probe(r: &mut pb::RunWorkloadRequest) -> &mut pb::WakeupProbe {
    r.spec.as_mut().unwrap().containers[0]
        .wakeup_probe
        .as_mut()
        .unwrap()
}

#[test]
fn run_workload_request_cases() {
    const PROBE: &str = "spec.containers[0].wakeup_probe";
    let cases: Vec<(&str, pb::RunWorkloadRequest, Want)> = vec![
        ("valid", valid_run(|_| {}), vec![]),
        (
            "valid: gVisor shape, no assets, no egress, unset size",
            valid_run(|r| {
                r.runtime_asset_paths.clear();
                r.egress_gateway = None;
                r.resources = None;
            }),
            vec![],
        ),
        (
            "missing hive",
            valid_run(|r| r.hive.clear()),
            vec![w(Required, "hive", None)],
        ),
        (
            "invalid hive: uppercase",
            valid_run(|r| r.hive = "Team-A".into()),
            vec![w(Invalid, "hive", DNS)],
        ),
        (
            "missing actor_name",
            valid_run(|r| r.actor_name.clear()),
            vec![w(Required, "actor_name", None)],
        ),
        (
            "missing actor_uid",
            valid_run(|r| r.actor_uid.clear()),
            vec![w(Required, "actor_uid", None)],
        ),
        (
            "invalid actor_uid",
            valid_run(|r| r.actor_uid = "uid-a".into()),
            vec![w(Invalid, "actor_uid", Some("format=uuid"))],
        ),
        (
            "invalid actor_template_name: underscore",
            valid_run(|r| r.actor_template_name = "template_1".into()),
            vec![w(Invalid, "actor_template_name", DNS)],
        ),
        (
            "runsc_path too long",
            valid_run(|r| r.runsc_path = format!("/{}", "x".repeat(4096))),
            vec![w(TooLong, "runsc_path", Some("maxLength"))],
        ),
        (
            "missing spec",
            valid_run(|r| r.spec = None),
            vec![w(Required, "spec", None)],
        ),
        (
            "too many runtime_asset_paths",
            valid_run(|r| {
                r.runtime_asset_paths = (0..17)
                    .map(|i| (format!("asset-{i}"), "/opt/asset".to_string()))
                    .collect()
            }),
            vec![w(TooMany, "runtime_asset_paths", Some("maxProperties"))],
        ),
        (
            "runtime asset name too long",
            valid_run(|r| {
                r.runtime_asset_paths = [("k".repeat(65), "/opt/asset".to_string())].into();
            }),
            vec![w(TooLong, "runtime_asset_paths", Some("maxLength"))],
        ),
        (
            "runtime asset path too long",
            valid_run(|r| {
                r.runtime_asset_paths =
                    [("kata-kernel".to_string(), format!("/{}", "x".repeat(4096)))].into();
            }),
            vec![w(
                TooLong,
                "runtime_asset_paths[kata-kernel]",
                Some("maxLength"),
            )],
        ),
        (
            "negative cpu_millicores",
            valid_run(|r| r.resources = Some(limits(-1, 0))),
            vec![w(
                Invalid,
                "resources.limits.cpu_millicores",
                Some("minimum"),
            )],
        ),
        (
            "cpu_millicores past the keeper's bound",
            valid_run(|r| r.resources = Some(limits(1_000_000, 0))),
            vec![w(
                Invalid,
                "resources.limits.cpu_millicores",
                Some("maximum"),
            )],
        ),
        (
            "negative memory_bytes",
            valid_run(|r| r.resources = Some(limits(0, -1))),
            vec![w(Invalid, "resources.limits.memory_bytes", Some("minimum"))],
        ),
        (
            "empty egress_gateway",
            valid_run(|r| r.egress_gateway = Some(pb::EgressGateway::default())),
            vec![w(Required, "egress_gateway.address", None)],
        ),
        (
            "egress_gateway address without port",
            valid_run(|r| r.egress_gateway.as_mut().unwrap().address = "gateway".into()),
            vec![w(Invalid, "egress_gateway.address", None)],
        ),
        (
            "egress_gateway address too long",
            valid_run(|r| {
                r.egress_gateway.as_mut().unwrap().address = format!("{}:443", "h".repeat(258))
            }),
            vec![
                w(TooLong, "egress_gateway.address", Some("maxLength")),
                w(Invalid, "egress_gateway.address", None),
            ],
        ),
        (
            "missing actor_dirs",
            valid_run(|r| r.actor_dirs = None),
            vec![w(Required, "actor_dirs", None)],
        ),
        (
            "relative oci_bundle_dir",
            valid_run(|r| r.actor_dirs.as_mut().unwrap().oci_bundle_dir = "bundles".into()),
            vec![w(Invalid, "actor_dirs.oci_bundle_dir", None)],
        ),
        (
            "spec without containers",
            valid_run(|r| r.spec = Some(pb::WorkloadSpec::default())),
            vec![w(Required, "spec.containers", None)],
        ),
        (
            "bad container name",
            valid_run(|r| r.spec.as_mut().unwrap().containers[0].name = "Main".into()),
            vec![w(Invalid, "spec.containers[0].name", DNS)],
        ),
        (
            "valid: no wakeup probe",
            valid_run(|r| r.spec.as_mut().unwrap().containers[0].wakeup_probe = None),
            vec![],
        ),
        (
            "empty wakeup probe",
            valid_run(|r| *probe(r) = pb::WakeupProbe::default()),
            vec![
                w(Required, &format!("{PROBE}.http_get"), None),
                w(Required, &format!("{PROBE}.timeout_seconds"), None),
            ],
        ),
        (
            "wakeup probe timeout too long",
            valid_run(|r| probe(r).timeout_seconds = 3601),
            vec![w(
                Invalid,
                &format!("{PROBE}.timeout_seconds"),
                Some("maximum"),
            )],
        ),
        (
            "empty http_get",
            valid_run(|r| probe(r).http_get = Some(pb::HttpGetAction::default())),
            vec![
                w(Required, &format!("{PROBE}.http_get.path"), None),
                w(Required, &format!("{PROBE}.http_get.port"), None),
            ],
        ),
        (
            "http_get path without leading slash",
            valid_run(|r| probe(r).http_get.as_mut().unwrap().path = "healthz".into()),
            vec![w(Invalid, &format!("{PROBE}.http_get.path"), None)],
        ),
        (
            "http_get path with query",
            valid_run(|r| probe(r).http_get.as_mut().unwrap().path = "/healthz?x=1".into()),
            vec![w(Invalid, &format!("{PROBE}.http_get.path"), None)],
        ),
        (
            "http_get path too long",
            valid_run(|r| {
                probe(r).http_get.as_mut().unwrap().path = format!("/{}", "x".repeat(1024))
            }),
            vec![w(
                TooLong,
                &format!("{PROBE}.http_get.path"),
                Some("maxLength"),
            )],
        ),
        (
            "http_get port past the range",
            valid_run(|r| probe(r).http_get.as_mut().unwrap().port = 65536),
            vec![w(
                Invalid,
                &format!("{PROBE}.http_get.port"),
                Some("maximum"),
            )],
        ),
    ];
    for (name, req, want) in cases {
        assert_eq!(got(&run_workload_request(&req)), want, "{name}");
    }
}

#[test]
fn run_workload_request_edge() {
    assert!(into_status(run_workload_request(&valid_run(|_| {}))).is_ok());
    let err = into_status(run_workload_request(&pb::RunWorkloadRequest::default())).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

fn valid_checkpoint(
    mutate: impl FnOnce(&mut pb::CheckpointWorkloadRequest),
) -> pb::CheckpointWorkloadRequest {
    let mut r = pb::CheckpointWorkloadRequest {
        hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: UID.into(),
        actor_template_hive: "team-a".into(),
        actor_template_name: "template-1".into(),
        runsc_path: "/opt/runsc".into(),
        spec: Some(full_spec()),
        fidelity: pb::SnapshotFidelity::Memory as i32,
        actor_dirs: Some(full_actor_dirs()),
    };
    mutate(&mut r);
    r
}

#[test]
fn checkpoint_workload_request_cases() {
    let cases: Vec<(&str, pb::CheckpointWorkloadRequest, Want)> = vec![
        ("valid", valid_checkpoint(|_| {}), vec![]),
        (
            "valid: volumes fidelity",
            valid_checkpoint(|r| r.fidelity = pb::SnapshotFidelity::Volumes as i32),
            vec![],
        ),
        (
            "valid: rootfs fidelity",
            valid_checkpoint(|r| r.fidelity = pb::SnapshotFidelity::Rootfs as i32),
            vec![],
        ),
        (
            "missing actor_uid",
            valid_checkpoint(|r| r.actor_uid.clear()),
            vec![w(Required, "actor_uid", None)],
        ),
        (
            "invalid actor_name: trailing dash",
            valid_checkpoint(|r| r.actor_name = "actor-".into()),
            vec![w(Invalid, "actor_name", DNS)],
        ),
        (
            "runsc_path too long",
            valid_checkpoint(|r| r.runsc_path = format!("/{}", "x".repeat(4096))),
            vec![w(TooLong, "runsc_path", Some("maxLength"))],
        ),
        (
            "missing spec",
            valid_checkpoint(|r| r.spec = None),
            vec![w(Required, "spec", None)],
        ),
        (
            "missing fidelity",
            valid_checkpoint(|r| r.fidelity = 0),
            vec![w(Required, "fidelity", None)],
        ),
        (
            "fidelity past the enum",
            valid_checkpoint(|r| r.fidelity = 4),
            vec![w(Invalid, "fidelity", Some("maximum"))],
        ),
        (
            "missing actor_dirs",
            valid_checkpoint(|r| r.actor_dirs = None),
            vec![w(Required, "actor_dirs", None)],
        ),
        (
            "relative checkpoint_dir",
            valid_checkpoint(|r| {
                r.actor_dirs.as_mut().unwrap().checkpoint_dir = "checkpoint".into()
            }),
            vec![w(Invalid, "actor_dirs.checkpoint_dir", None)],
        ),
    ];
    for (name, req, want) in cases {
        assert_eq!(got(&checkpoint_workload_request(&req)), want, "{name}");
    }
    let err = into_status(checkpoint_workload_request(
        &pb::CheckpointWorkloadRequest::default(),
    ))
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

fn spec_of(names: impl IntoIterator<Item = String>) -> pb::WorkloadSpec {
    pb::WorkloadSpec {
        containers: names
            .into_iter()
            .map(|name| pb::Container {
                name,
                ..Default::default()
            })
            .collect(),
    }
}

#[test]
fn workload_spec_rules() {
    let op = Operation::create();
    let p = Path::root();
    let cases: Vec<(&str, pb::WorkloadSpec, Want)> = vec![
        ("valid", full_spec(), vec![]),
        (
            "valid: at the containers bound",
            spec_of((0..MAX_CONTAINERS).map(|i| format!("c-{i}"))),
            vec![],
        ),
        (
            "no containers: the fallback spec for a template-less Actor",
            pb::WorkloadSpec::default(),
            vec![],
        ),
        (
            "over the containers bound",
            spec_of((0..=MAX_CONTAINERS).map(|i| format!("c-{i}"))),
            vec![w(TooMany, "containers", Some("maxItems"))],
        ),
        (
            "duplicate container name",
            spec_of(["app".to_string(), "app".to_string()]),
            vec![w(Duplicate, "containers[1]", None)],
        ),
        (
            "bad name inside the list",
            spec_of(["app".to_string(), "Sidecar".to_string()]),
            vec![w(Invalid, "containers[1].name", DNS)],
        ),
    ];
    for (name, spec, want) in cases {
        assert_eq!(got(&workload_spec(&op, &p, &spec, false)), want, "{name}");
    }
}

#[test]
fn container_rules() {
    let op = Operation::create();
    let p = Path::root();
    let valid = |f: &dyn Fn(&mut pb::Container)| {
        let mut c = full_container();
        f(&mut c);
        c
    };
    let cases: Vec<(&str, pb::Container, Want)> = vec![
        ("valid", valid(&|_| {}), vec![]),
        (
            "valid: name only",
            pb::Container {
                name: "app".into(),
                ..Default::default()
            },
            vec![],
        ),
        (
            "missing name",
            valid(&|c| c.name.clear()),
            vec![w(Required, "name", None)],
        ),
        (
            "invalid name: the sandbox's own container",
            valid(&|c| c.name = "_sandbox".into()),
            vec![w(Invalid, "name", DNS)],
        ),
        (
            "too many external mounts",
            valid(&|c| {
                c.external_volume_mounts = (0..33).map(|i| mount("v", &format!("/m/{i}"))).collect()
            }),
            vec![w(TooMany, "external_volume_mounts", Some("maxItems"))],
        ),
        (
            "duplicate durable-dir mount path",
            valid(&|c| {
                c.durable_dir_volume_mounts = vec![mount("a", "/data"), mount("b", "/data")]
            }),
            vec![w(Duplicate, "durable_dir_volume_mounts[1]", None)],
        ),
        (
            "valid: no mounts",
            valid(&|c| {
                c.durable_dir_volume_mounts.clear();
                c.external_volume_mounts.clear();
                c.system_info_volume_mounts.clear();
                c.image_volume_mounts.clear();
            }),
            vec![],
        ),
        (
            "empty system-info mount",
            valid(&|c| c.system_info_volume_mounts = vec![pb::VolumeMount::default()]),
            vec![
                w(Required, "system_info_volume_mounts[0].name", None),
                w(Required, "system_info_volume_mounts[0].mount_path", None),
            ],
        ),
        (
            "invalid external mount name: uppercase",
            valid(&|c| c.external_volume_mounts[0].name = "EXT".into()),
            vec![w(Invalid, "external_volume_mounts[0].name", DNS)],
        ),
        (
            "relative durable-dir mount_path",
            valid(&|c| c.durable_dir_volume_mounts[0].mount_path = "data".into()),
            vec![w(Invalid, "durable_dir_volume_mounts[0].mount_path", None)],
        ),
        (
            "image mount_path with dot-dot",
            valid(&|c| c.image_volume_mounts[0].mount_path = "/img/../etc".into()),
            vec![w(Invalid, "image_volume_mounts[0].mount_path", None)],
        ),
        (
            "image mount_path too long",
            valid(&|c| c.image_volume_mounts[0].mount_path = format!("/{}", "x".repeat(4096))),
            vec![w(
                TooLong,
                "image_volume_mounts[0].mount_path",
                Some("maxLength"),
            )],
        ),
        (
            "external mount nested under another in its list",
            valid(&|c| c.external_volume_mounts.push(mount("ext-2", "/ext/inner"))),
            vec![w(Invalid, "external_volume_mounts[1].mount_path", None)],
        ),
        (
            "system-info mount nested under an external mount",
            valid(&|c| {
                c.system_info_volume_mounts
                    .push(mount("sysinfo-2", "/ext/inner"))
            }),
            vec![w(Invalid, "system_info_volume_mounts[1].mount_path", None)],
        ),
        (
            "image mount nested over a system-info mount",
            valid(&|c| c.image_volume_mounts.push(mount("etc", "/etc"))),
            vec![w(Invalid, "image_volume_mounts[1].mount_path", None)],
        ),
        (
            "same mount path in two lists",
            valid(&|c| c.image_volume_mounts.push(mount("data-again", "/data"))),
            vec![w(Duplicate, "image_volume_mounts[1].mount_path", None)],
        ),
    ];
    for (name, c, want) in cases {
        assert_eq!(got(&container(&op, &p, &c)), want, "{name}");
    }
}

#[test]
fn actor_dirs_rules() {
    let op = Operation::create();
    let p = Path::root();
    let valid = |f: &dyn Fn(&mut pb::ActorDirs)| {
        let mut d = full_actor_dirs();
        f(&mut d);
        d
    };
    let cases: Vec<(&str, pb::ActorDirs, Want)> = vec![
        ("valid", valid(&|_| {}), vec![]),
        (
            "missing root_dir",
            valid(&|d| d.root_dir.clear()),
            vec![w(Required, "root_dir", None)],
        ),
        (
            "missing external_volume_mounts_dir",
            valid(&|d| d.external_volume_mounts_dir.clear()),
            vec![w(Required, "external_volume_mounts_dir", None)],
        ),
        (
            "missing sandbox_assets_dir",
            valid(&|d| d.sandbox_assets_dir.clear()),
            vec![w(Required, "sandbox_assets_dir", None)],
        ),
        (
            "relative sandbox_assets_dir",
            valid(&|d| d.sandbox_assets_dir = "sandbox-assets".into()),
            vec![w(Invalid, "sandbox_assets_dir", None)],
        ),
        (
            "relative root_dir",
            valid(&|d| d.root_dir = "node/actors/a".into()),
            vec![w(Invalid, "root_dir", None)],
        ),
        (
            "unclean checkpoint_dir",
            valid(&|d| d.checkpoint_dir = "/node/actors/a/../b/checkpoint-state".into()),
            vec![w(Invalid, "checkpoint_dir", None)],
        ),
        (
            "relative oci_bundle_dir",
            valid(&|d| d.oci_bundle_dir = "bundles".into()),
            vec![w(Invalid, "oci_bundle_dir", None)],
        ),
        (
            "unclean restore_dir",
            valid(&|d| d.restore_dir = "/actors/uid/../other/restore".into()),
            vec![w(Invalid, "restore_dir", None)],
        ),
        (
            "trailing slash",
            valid(&|d| d.durable_dir_volume_mounts_dir = "/actors/uid/durable/".into()),
            vec![w(Invalid, "durable_dir_volume_mounts_dir", None)],
        ),
        (
            "too long",
            valid(&|d| d.system_info_volume_roots_dir = format!("/{}", "x".repeat(4096))),
            vec![w(
                TooLong,
                "system_info_volume_roots_dir",
                Some("maxLength"),
            )],
        ),
        (
            "two bad dirs are both reported",
            valid(&|d| {
                d.root_dir.clear();
                d.checkpoint_dir = "checkpoint".into();
            }),
            vec![
                w(Required, "root_dir", None),
                w(Invalid, "checkpoint_dir", None),
            ],
        ),
    ];
    for (name, d, want) in cases {
        assert_eq!(got(&actor_dirs(&op, &p, &d)), want, "{name}");
    }
}

fn valid_restore(
    mutate: impl FnOnce(&mut pb::RestoreWorkloadRequest),
) -> pb::RestoreWorkloadRequest {
    let mut r = pb::RestoreWorkloadRequest {
        hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: UID.into(),
        actor_template_hive: "team-a".into(),
        actor_template_name: "template-1".into(),
        runsc_path: "/opt/runsc".into(),
        spec: Some(full_spec()),
        runtime_asset_paths: [("kata-kernel".to_string(), "/opt/kernel".to_string())].into(),
        fidelity: pb::SnapshotFidelity::Volumes as i32,
        egress_gateway: Some(pb::EgressGateway {
            address: "gateway:443".into(),
        }),
        resources: Some(limits(1500, 1 << 30)),
        actor_dirs: Some(full_actor_dirs()),
        preserve_restore_dir: false,
    };
    mutate(&mut r);
    r
}

#[test]
fn restore_workload_request_cases() {
    let cases: Vec<(&str, pb::RestoreWorkloadRequest, Want)> = vec![
        ("valid", valid_restore(|_| {}), vec![]),
        (
            "valid: memory fidelity",
            valid_restore(|r| r.fidelity = pb::SnapshotFidelity::Memory as i32),
            vec![],
        ),
        (
            "valid: rootfs fidelity, which runtimes judge for themselves",
            valid_restore(|r| r.fidelity = pb::SnapshotFidelity::Rootfs as i32),
            vec![],
        ),
        (
            "missing actor_uid",
            valid_restore(|r| r.actor_uid.clear()),
            vec![w(Required, "actor_uid", None)],
        ),
        (
            "invalid hive: uppercase",
            valid_restore(|r| r.hive = "Team-A".into()),
            vec![w(Invalid, "hive", DNS)],
        ),
        (
            "missing spec",
            valid_restore(|r| r.spec = None),
            vec![w(Required, "spec", None)],
        ),
        (
            "spec without containers",
            valid_restore(|r| r.spec = Some(pb::WorkloadSpec::default())),
            vec![w(Required, "spec.containers", None)],
        ),
        (
            "missing actor_dirs",
            valid_restore(|r| r.actor_dirs = None),
            vec![w(Required, "actor_dirs", None)],
        ),
        (
            "negative cpu_millicores",
            valid_restore(|r| r.resources = Some(limits(-1, 0))),
            vec![w(
                Invalid,
                "resources.limits.cpu_millicores",
                Some("minimum"),
            )],
        ),
        (
            "cpu_millicores past the keeper's bound",
            valid_restore(|r| r.resources = Some(limits(1_000_000, 0))),
            vec![w(
                Invalid,
                "resources.limits.cpu_millicores",
                Some("maximum"),
            )],
        ),
        (
            "runtime asset path too long",
            valid_restore(|r| {
                r.runtime_asset_paths =
                    [("kata-kernel".to_string(), format!("/{}", "x".repeat(4096)))].into();
            }),
            vec![w(
                TooLong,
                "runtime_asset_paths[kata-kernel]",
                Some("maxLength"),
            )],
        ),
        (
            "egress_gateway address without port",
            valid_restore(|r| r.egress_gateway.as_mut().unwrap().address = "gateway".into()),
            vec![w(Invalid, "egress_gateway.address", None)],
        ),
        (
            "missing fidelity",
            valid_restore(|r| r.fidelity = 0),
            vec![w(Required, "fidelity", None)],
        ),
        (
            "fidelity past the enum",
            valid_restore(|r| r.fidelity = 4),
            vec![w(Invalid, "fidelity", Some("maximum"))],
        ),
    ];
    for (name, req, want) in cases {
        assert_eq!(got(&restore_workload_request(&req)), want, "{name}");
    }
}

#[test]
fn restore_workload_request_edge() {
    assert!(into_status(restore_workload_request(&valid_restore(|_| {}))).is_ok());
    let err = into_status(restore_workload_request(
        &pb::RestoreWorkloadRequest::default(),
    ))
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

fn valid_terminate(
    mutate: impl FnOnce(&mut pb::TerminateWorkloadRequest),
) -> pb::TerminateWorkloadRequest {
    let mut r = pb::TerminateWorkloadRequest {
        hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: UID.into(),
        actor_template_hive: "team-a".into(),
        actor_template_name: "template-1".into(),
        runsc_path: "/opt/runsc".into(),
        spec: Some(full_spec()),
        actor_dirs: Some(full_actor_dirs()),
    };
    mutate(&mut r);
    r
}

#[test]
fn terminate_workload_request_cases() {
    let cases: Vec<(&str, pb::TerminateWorkloadRequest, Want)> = vec![
        ("valid", valid_terminate(|_| {}), vec![]),
        (
            "valid: no template ref, no runsc_path",
            valid_terminate(|r| {
                r.actor_template_hive.clear();
                r.actor_template_name.clear();
                r.runsc_path.clear();
            }),
            vec![],
        ),
        (
            "missing hive",
            valid_terminate(|r| r.hive.clear()),
            vec![w(Required, "hive", None)],
        ),
        (
            "invalid hive: uppercase",
            valid_terminate(|r| r.hive = "Team-A".into()),
            vec![w(Invalid, "hive", DNS)],
        ),
        (
            "missing actor_name",
            valid_terminate(|r| r.actor_name.clear()),
            vec![w(Required, "actor_name", None)],
        ),
        (
            "invalid actor_name: trailing dash",
            valid_terminate(|r| r.actor_name = "actor-".into()),
            vec![w(Invalid, "actor_name", DNS)],
        ),
        (
            "missing actor_uid",
            valid_terminate(|r| r.actor_uid.clear()),
            vec![w(Required, "actor_uid", None)],
        ),
        (
            "invalid actor_uid: not a uuid",
            valid_terminate(|r| r.actor_uid = "uid-a".into()),
            vec![w(Invalid, "actor_uid", Some("format=uuid"))],
        ),
        (
            "invalid actor_template_hive: uppercase",
            valid_terminate(|r| r.actor_template_hive = "Team-A".into()),
            vec![w(Invalid, "actor_template_hive", DNS)],
        ),
        (
            "invalid actor_template_name: underscore",
            valid_terminate(|r| r.actor_template_name = "template_1".into()),
            vec![w(Invalid, "actor_template_name", DNS)],
        ),
        (
            "runsc_path too long",
            valid_terminate(|r| r.runsc_path = format!("/{}", "x".repeat(4096))),
            vec![w(TooLong, "runsc_path", Some("maxLength"))],
        ),
        (
            "missing spec",
            valid_terminate(|r| r.spec = None),
            vec![w(Required, "spec", None)],
        ),
        (
            "valid: spec without containers, the fallback for a template-less Actor",
            valid_terminate(|r| r.spec = Some(pb::WorkloadSpec::default())),
            vec![],
        ),
        (
            "missing actor_dirs",
            valid_terminate(|r| r.actor_dirs = None),
            vec![w(Required, "actor_dirs", None)],
        ),
    ];
    for (name, req, want) in cases {
        assert_eq!(got(&terminate_workload_request(&req)), want, "{name}");
    }
}

#[test]
fn terminate_workload_request_edge() {
    assert!(into_status(terminate_workload_request(&valid_terminate(|_| {}))).is_ok());
    let err = into_status(terminate_workload_request(
        &pb::TerminateWorkloadRequest::default(),
    ))
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[test]
fn checkpoint_workload_request_edge() {
    assert!(into_status(checkpoint_workload_request(&valid_checkpoint(|_| {}))).is_ok());
}

#[test]
fn get_workload_stats_request_rules() {
    let ok = pb::GetWorkloadStatsRequest {
        actor_uid: UID.into(),
    };
    assert!(get_workload_stats_request(&ok).is_empty());
    assert_eq!(
        got(&get_workload_stats_request(
            &pb::GetWorkloadStatsRequest::default()
        )),
        vec![w(Required, "actor_uid", None)]
    );
    let bad = pb::GetWorkloadStatsRequest {
        actor_uid: "not-a-uuid".into(),
    };
    assert_eq!(
        got(&get_workload_stats_request(&bad)),
        vec![w(Invalid, "actor_uid", Some("format=uuid"))]
    );
}

#[test]
fn get_workload_stats_request_edge() {
    let ok = pb::GetWorkloadStatsRequest {
        actor_uid: UID.into(),
    };
    assert!(into_status(get_workload_stats_request(&ok)).is_ok());
    let err = into_status(get_workload_stats_request(
        &pb::GetWorkloadStatsRequest::default(),
    ))
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

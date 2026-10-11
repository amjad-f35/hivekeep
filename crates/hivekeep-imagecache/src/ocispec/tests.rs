use std::fs;

use super::gvisor::{gvisor_cgroup_leaf, shape_gvisor, GvisorOptions, SANDBOX_ID};
use super::microvm::{
    shape_microvm, MicroVmOptions, GUEST_SHARED_DIR, SHARE_CSI, SHARE_DURABLE, SHARE_SYSTEM_INFO,
    SHARE_VOLUMES,
};
use super::sizing::SandboxSize;
use super::*;

const TEST_ACTOR_UID: &str = "actor_uid";

pub(crate) fn sample_actor_dirs() -> pb::ActorDirs {
    pb::ActorDirs {
        root_dir: "/node/actors/a".into(),
        oci_bundle_dir: "/node/actors/a/bundles".into(),
        durable_dir_volume_mounts_dir: "/node/actors/a/durable-dir".into(),
        system_info_volume_roots_dir: "/node/actors/a/system-info".into(),
        external_volume_mounts_dir: "/node/actors/a/volumes".into(),
        ..Default::default()
    }
}

fn vm(name: &str, path: &str) -> pb::VolumeMount {
    pb::VolumeMount {
        name: name.into(),
        mount_path: path.into(),
    }
}

/// One volume of every kind.
fn sample_container() -> pb::Container {
    pb::Container {
        name: "app".into(),
        container_spec: Some(pb::ContainerSpec {
            args: vec!["/app".into()],
            ..Default::default()
        }),
        durable_dir_volume_mounts: vec![vm("data", "/var/data")],
        system_info_volume_mounts: vec![vm("sysinfo", "/run/hivekeep")],
        external_volume_mounts: vec![vm("csi", "/mnt/csi")],
        image_volume_mounts: vec![vm("agent", "/hivekeep")],
        ..Default::default()
    }
}

const SAMPLE_MOUNT_PATHS: [&str; 4] = ["/var/data", "/run/hivekeep", "/mnt/csi", "/hivekeep"];

const SAMPLE_SIZE: SandboxSize = SandboxSize {
    milli_cpu: 500,
    memory_bytes: 512 << 20,
};

fn sample_spec(dirs: &pb::ActorDirs, c: &pb::Container) -> Spec {
    build(Options {
        actor_uid: TEST_ACTOR_UID,
        actor_dirs: Some(dirs),
        container: Some(c),
    })
}

fn mount_for<'a>(spec: &'a Spec, dest: &str) -> &'a Mount {
    spec.mounts
        .iter()
        .flatten()
        .find(|m| m.destination == dest)
        .unwrap_or_else(|| panic!("no mount at {dest}; mounts = {:?}", spec.mounts))
}

fn app_only(spec: pb::ContainerSpec) -> pb::Container {
    pb::Container {
        container_spec: Some(spec),
        ..Default::default()
    }
}

fn args_app() -> pb::ContainerSpec {
    pb::ContainerSpec {
        args: vec!["/app".into()],
        ..Default::default()
    }
}

// ---- behaviour across runtimes ----

#[test]
fn build_is_runtime_neutral() {
    let (d, c) = (sample_actor_dirs(), sample_container());
    let b = serde_json::to_string(&sample_spec(&d, &c)).unwrap();
    for literal in ["runsc", "io.kubernetes.cri.", "dev.gvisor.", "kata"] {
        assert!(!b.contains(literal), "base spec contains {literal:?}: {b}");
    }
}

#[test]
fn shapers_preserve_every_volume_mount() {
    let (d, c) = (sample_actor_dirs(), sample_container());
    for runtime in ["gvisor", "microvm"] {
        let mut spec = sample_spec(&d, &c);
        if runtime == "gvisor" {
            shape_gvisor(
                &mut spec,
                GvisorOptions {
                    actor_uid: TEST_ACTOR_UID,
                    container_name: "app",
                    size: SAMPLE_SIZE,
                    ..Default::default()
                },
            );
        } else {
            shape_microvm(
                &mut spec,
                MicroVmOptions {
                    actor_dirs: Some(&d),
                    container_name: "app",
                },
            )
            .unwrap();
        }
        for dest in SAMPLE_MOUNT_PATHS {
            assert!(
                !mount_for(&spec, dest).source.is_empty(),
                "{runtime}: {dest} has no source"
            );
        }
        assert_eq!(spec.hostname, "actor", "{runtime}");
        assert!(
            spec.linux.as_ref().unwrap().resources.is_some(),
            "{runtime}: no resources"
        );
    }
}

#[test]
fn shape_microvm_translates_sources_into_the_share() {
    let (d, c) = (sample_actor_dirs(), sample_container());
    let mut spec = sample_spec(&d, &c);
    shape_microvm(
        &mut spec,
        MicroVmOptions {
            actor_dirs: Some(&d),
            container_name: "app",
        },
    )
    .unwrap();
    for (dest, want) in [
        (
            "/var/data",
            format!("{GUEST_SHARED_DIR}/{SHARE_DURABLE}/data"),
        ),
        (
            "/run/hivekeep",
            format!("{GUEST_SHARED_DIR}/{SHARE_SYSTEM_INFO}/sysinfo"),
        ),
        ("/mnt/csi", format!("{GUEST_SHARED_DIR}/{SHARE_CSI}/csi")),
        (
            "/hivekeep",
            format!("{GUEST_SHARED_DIR}/c_app/{SHARE_VOLUMES}/agent"),
        ),
    ] {
        assert_eq!(mount_for(&spec, dest).source, want, "{dest}");
    }
    assert_eq!(
        mount_for(&spec, "/dev/shm").source,
        "shm",
        "guest system mounts replace the host's"
    );
}

#[test]
fn shape_microvm_unstaged_source_is_an_error() {
    let c = app_only(args_app());
    let mut spec = build(Options {
        container: Some(&c),
        ..Default::default()
    });
    spec.mounts.as_mut().unwrap().push(Mount {
        destination: "/mnt/new".into(),
        kind: "bind".into(),
        source: "/var/lib/hivekeep/new-kind/x".into(),
        options: vec![],
    });
    let d = sample_actor_dirs();
    assert!(shape_microvm(
        &mut spec,
        MicroVmOptions {
            actor_dirs: Some(&d),
            container_name: "app",
        }
    )
    .is_err());
}

#[test]
fn shape_gvisor_idempotent() {
    let (d, c) = (sample_actor_dirs(), sample_container());
    let mut spec = sample_spec(&d, &c);
    let o = || GvisorOptions {
        actor_uid: TEST_ACTOR_UID,
        actor_dirs: Some(&d),
        container_name: "app",
        size: SAMPLE_SIZE,
        ..Default::default()
    };
    shape_gvisor(&mut spec, o());
    let first = spec.mounts.as_ref().unwrap().len();
    let snapshot = spec.clone();
    shape_gvisor(&mut spec, o());
    assert_eq!(spec.mounts.as_ref().unwrap().len(), first);
    assert_eq!(spec, snapshot);
}

#[test]
fn shape_gvisor_sandbox() {
    let d = sample_actor_dirs();
    let mut spec = build_sandbox(TEST_ACTOR_UID);
    let durable = vec!["data".to_string()];
    shape_gvisor(
        &mut spec,
        GvisorOptions {
            actor_uid: TEST_ACTOR_UID,
            actor_dirs: Some(&d),
            container_name: SANDBOX_ID,
            durable_volumes: &durable,
            size: SAMPLE_SIZE,
        },
    );
    assert!(spec.process.is_none() && spec.root.is_none() && spec.mounts.is_none());
    let a = &spec.annotations;
    assert_eq!(a["io.kubernetes.cri.container-type"], "sandbox");
    assert_eq!(a["dev.gvisor.spec.mount.data.type"], "tmpfs");
    assert_eq!(a["dev.gvisor.spec.mount.data.share"], "container");
    assert_eq!(
        a["dev.gvisor.spec.mount.data.source"],
        "/node/actors/a/durable-dir/data"
    );
}

// The container's cgroup leaf is sized from the actor-level limits; the
// sandbox keeps its limits, cgroup and namespaces.
#[test]
fn shape_gvisor_applies_the_actor_size() {
    let (d, c) = (sample_actor_dirs(), sample_container());
    for name in ["app", SANDBOX_ID] {
        let mut spec = if name == SANDBOX_ID {
            build_sandbox(TEST_ACTOR_UID)
        } else {
            sample_spec(&d, &c)
        };
        shape_gvisor(
            &mut spec,
            GvisorOptions {
                actor_uid: TEST_ACTOR_UID,
                container_name: name,
                size: SAMPLE_SIZE,
                ..Default::default()
            },
        );
        let linux = spec.linux.as_ref().unwrap();
        assert_eq!(
            linux
                .resources
                .as_ref()
                .unwrap()
                .memory
                .as_ref()
                .unwrap()
                .limit,
            Some(SAMPLE_SIZE.memory_bytes),
            "{name}"
        );
        assert_eq!(
            linux.cgroups_path,
            format!("/{}", gvisor_cgroup_leaf(TEST_ACTOR_UID, name))
        );
        assert!(
            linux
                .namespaces
                .iter()
                .any(|n| n.kind == "network" && !n.path.is_empty()),
            "{name}"
        );
        assert!(!spec.hostname.is_empty());
    }
}

// ---- the base spec ----

#[test]
fn build_volume_mounts() {
    let dirs = pb::ActorDirs {
        oci_bundle_dir: "/node/actors/a/bundles".into(),
        durable_dir_volume_mounts_dir: "/node/actors/a/durable-dir".into(),
        external_volume_mounts_dir: "/node/actors/a/volumes".into(),
        system_info_volume_roots_dir: "/node/actors/a/system-info".into(),
        ..Default::default()
    };
    let c = pb::Container {
        name: "app".into(),
        container_spec: Some(args_app()),
        durable_dir_volume_mounts: vec![vm("data", "/var/data"), vm("data", "/home/counter")],
        system_info_volume_mounts: vec![vm("sysinfo", "/run/hivekeep")],
        external_volume_mounts: vec![vm("csi", "/mnt/csi")],
        image_volume_mounts: vec![vm("agent", "/hivekeep")],
        ..Default::default()
    };
    let spec = build(Options {
        actor_dirs: Some(&dirs),
        container: Some(&c),
        ..Default::default()
    });
    let image_vol = image_volume_mount_path(Path::new("/node/actors/a/bundles/app"), "agent")
        .to_string_lossy()
        .into_owned();
    for (dest, src, mode) in [
        (
            "/var/data",
            "/node/actors/a/durable-dir/data".to_string(),
            "rw",
        ),
        (
            "/home/counter",
            "/node/actors/a/durable-dir/data".to_string(),
            "rw",
        ),
        (
            "/run/hivekeep",
            "/node/actors/a/system-info/sysinfo".to_string(),
            "ro",
        ),
        ("/mnt/csi", "/node/actors/a/volumes/csi".to_string(), "rw"),
        ("/hivekeep", image_vol, "ro"),
    ] {
        let m = mount_for(&spec, dest);
        assert_eq!(m.kind, "bind", "{dest}");
        assert_eq!(m.source, src, "{dest}");
        assert_eq!(
            m.options,
            vec!["bind".to_string(), mode.to_string()],
            "{dest}"
        );
    }
}

#[test]
fn build_joins_the_actor_netns() {
    let c = app_only(args_app());
    let spec = build(Options {
        actor_uid: "uid-a",
        container: Some(&c),
        ..Default::default()
    });
    let ns = spec
        .linux
        .unwrap()
        .namespaces
        .into_iter()
        .find(|n| n.kind == "network")
        .expect("a network namespace");
    assert_eq!(ns.path, actor_netns_path("uid-a"));
}

#[test]
fn build_capabilities() {
    let want = vec!["CAP_CHOWN".to_string(), "CAP_KILL".to_string()];
    let c = app_only(pb::ContainerSpec {
        capabilities: want.clone(),
        ..args_app()
    });
    let spec = build(Options {
        container: Some(&c),
        ..Default::default()
    });
    let caps = spec.process.unwrap().capabilities.unwrap();
    assert_eq!(caps.bounding, want);
    assert_eq!(caps.effective, want);
    assert_eq!(caps.permitted, want);
    assert!(caps.inheritable.is_empty() && caps.ambient.is_empty());
}

#[test]
fn build_no_capabilities() {
    let c = app_only(args_app());
    let spec = build(Options {
        container: Some(&c),
        ..Default::default()
    });
    assert_eq!(
        spec.process.unwrap().capabilities.unwrap(),
        Capabilities::default()
    );
}

#[test]
fn save_round_trips() {
    let bundle = tempfile::tempdir().unwrap();
    let c = app_only(args_app());
    let want = build(Options {
        actor_uid: "uid-a",
        container: Some(&c),
        ..Default::default()
    });
    save(bundle.path(), &want).unwrap();
    let got: Spec =
        serde_json::from_slice(&fs::read(bundle.path().join(SPEC_FILE)).unwrap()).unwrap();
    assert_eq!(got, want);
}

fn limits(mem: i64, cpu: i64) -> pb::ResourceRequirements {
    pb::ResourceRequirements {
        limits: Some(pb::Resources {
            memory_bytes: mem,
            cpu_millicores: cpu,
        }),
    }
}

#[test]
fn oci_resources_maps_limits() {
    assert!(oci_resources(None).is_none());
    assert!(oci_resources(Some(&pb::ResourceRequirements {
        limits: Some(pb::Resources::default())
    }))
    .is_none());
    let got = oci_resources(Some(&limits(268_435_456, 200))).unwrap();
    assert_eq!(got.memory.unwrap().limit, Some(268_435_456));
    let cpu = got.cpu.unwrap();
    let (q, p) = (cpu.quota.unwrap(), cpu.period.unwrap());
    // The ratio, not a literal: retuning the period must keep quota/period
    // equal to the declared milli-cores.
    assert_eq!(q * 1000 / p as i64, 200);
    assert_eq!(p as i64, CPU_QUOTA_PERIOD_US);
}

// The kernel rejects a quota below 1ms.
#[test]
fn oci_resources_clamps_quota_to_kernel_minimum() {
    for m in [1, 5, 9] {
        let q = oci_resources(Some(&limits(0, m)))
            .unwrap()
            .cpu
            .unwrap()
            .quota
            .unwrap();
        assert!(q >= CPU_QUOTA_MIN_US, "cpu={m}m quota={q}");
    }
    assert_eq!(
        oci_resources(Some(&limits(0, 10)))
            .unwrap()
            .cpu
            .unwrap()
            .quota,
        Some(CPU_QUOTA_MIN_US)
    );
}

// A negative limit must not produce an empty resources object.
#[test]
fn oci_resources_negative_is_unset() {
    assert!(oci_resources(Some(&limits(-1, -1))).is_none());
}

#[test]
fn build_no_resources_leaves_linux_untouched() {
    let c = app_only(args_app());
    let spec = build(Options {
        container: Some(&c),
        ..Default::default()
    });
    assert!(spec.linux.unwrap().resources.is_none());
}

#[test]
fn build_resources_applied() {
    let c = app_only(pb::ContainerSpec {
        resources: Some(limits(67_108_864, 0)),
        ..args_app()
    });
    let spec = build(Options {
        container: Some(&c),
        ..Default::default()
    });
    assert_eq!(
        spec.linux.unwrap().resources.unwrap().memory.unwrap().limit,
        Some(67_108_864)
    );
}

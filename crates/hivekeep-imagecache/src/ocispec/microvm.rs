//! Shaping a spec for the micro-VM: guest system mounts replace the
//! host's, volume binds point at their paths inside the virtio-fs share,
//! kata's default resources fill the gaps, and the guest's privileged-port
//! limit is lifted (gVisor has none).

use std::path::{Path, PathBuf};

use hivekeep_proto::worker::v1 as pb;

use super::{Cpu, DeviceCgroup, Linux, Mount, Resources, Spec};
use crate::error::{Error, Result};
use crate::spec::image_volume_mount_path;

/// Where the guest agent mounts the share.
pub const GUEST_SHARED_DIR: &str = "/run/kata-containers/shared/containers";
/// Durable-dir volumes, one subdirectory each.
pub const SHARE_DURABLE: &str = "durable";
/// External (CSI) volumes, one subdirectory each.
pub const SHARE_CSI: &str = "csi";
/// System-info volumes, one subdirectory each.
pub const SHARE_SYSTEM_INFO: &str = "system-info";
/// A container's image volumes, at `<containerID>/volumes/<name>`.
pub const SHARE_VOLUMES: &str = "volumes";

const UNPRIVILEGED_PORT_START: &str = "net.ipv4.ip_unprivileged_port_start";

/// The guest container ID for a container name. The guest agent requires
/// at least two characters and a name can be one; `_` is not valid in a
/// name, so the ID cannot collide with another name or a share dir. It
/// depends only on the name, since a restored guest keeps its boot IDs.
pub fn guest_container_id(name: &str) -> String {
    format!("c_{name}")
}

/// The micro-VM context of one actor container.
#[derive(Debug, Clone, Default)]
pub struct MicroVmOptions<'a> {
    /// The actor's directories, whose volume sources are staged into the
    /// share.
    pub actor_dirs: Option<&'a pb::ActorDirs>,
    /// The container.
    pub container_name: &'a str,
}

/// Shapes an unshaped spec for the micro-VM; errors on a bind it cannot
/// place in the guest.
pub fn shape_microvm(spec: &mut Spec, o: MicroVmOptions<'_>) -> Result<()> {
    let default_dirs = pb::ActorDirs::default();
    let dirs = o.actor_dirs.unwrap_or(&default_dirs);
    let mut volumes = Vec::new();
    for m in spec.mounts.iter().flatten() {
        if m.kind != "bind" {
            continue;
        }
        let source = guest_volume_source(&m.source, dirs, o.container_name)
            .map_err(|e| e.context(format!("mount {:?}", m.destination)))?;
        // rbind, so nested mount points come across.
        let options = m
            .options
            .iter()
            .map(|opt| {
                if opt == "bind" {
                    "rbind".into()
                } else {
                    opt.clone()
                }
            })
            .collect();
        volumes.push(Mount {
            source,
            options,
            ..m.clone()
        });
    }
    let mut mounts = guest_system_mounts();
    mounts.extend(volumes);
    spec.mounts = Some(mounts);
    let linux = spec.linux.get_or_insert_with(Linux::default);
    linux
        .sysctl
        .entry(UNPRIVILEGED_PORT_START.to_string())
        .or_insert_with(|| "0".to_string());
    linux.resources = Some(merge_kata_resources(linux.resources.take()));
    Ok(())
}

/// Fills in the kata defaults `from` leaves unset; the caller's values win
/// and anything without a default is carried through, so an upstream
/// addition reaches the guest instead of being dropped.
pub fn merge_kata_resources(from: Option<Resources>) -> Resources {
    let def = guest_resources();
    let Some(mut out) = from else { return def };
    if out.devices.is_empty() {
        out.devices = def.devices;
    }
    match &mut out.cpu {
        None => out.cpu = def.cpu,
        Some(cpu) if cpu.shares.is_none() => cpu.shares = def.cpu.and_then(|c| c.shares),
        Some(_) => {}
    }
    out
}

fn guest_volume_source(host: &str, dirs: &pb::ActorDirs, container: &str) -> Result<String> {
    let bundle = PathBuf::from(&dirs.oci_bundle_dir).join(container);
    let volumes_dir = image_volume_mount_path(&bundle, "");
    let staged: [(String, String); 4] = [
        (
            dirs.durable_dir_volume_mounts_dir.clone(),
            format!("{GUEST_SHARED_DIR}/{SHARE_DURABLE}"),
        ),
        (
            dirs.external_volume_mounts_dir.clone(),
            format!("{GUEST_SHARED_DIR}/{SHARE_CSI}"),
        ),
        (
            dirs.system_info_volume_roots_dir.clone(),
            format!("{GUEST_SHARED_DIR}/{SHARE_SYSTEM_INFO}"),
        ),
        (
            volumes_dir
                .to_string_lossy()
                .trim_end_matches('/')
                .to_string(),
            format!(
                "{GUEST_SHARED_DIR}/{}/{SHARE_VOLUMES}",
                guest_container_id(container)
            ),
        ),
    ];
    for (h, guest) in staged {
        if let Some(rel) = host.strip_prefix(&format!("{h}/")) {
            return Ok(Path::new(&guest).join(rel).to_string_lossy().into_owned());
        }
    }
    Err(Error::msg(format!(
        "host path {host:?} is not staged into the guest share"
    )))
}

fn mount(dest: &str, kind: &str, source: &str, options: &[&str]) -> Mount {
    Mount {
        destination: dest.into(),
        kind: kind.into(),
        source: source.into(),
        options: options.iter().map(|s| s.to_string()).collect(),
    }
}

/// The standard guest mount set.
pub fn guest_system_mounts() -> Vec<Mount> {
    vec![
        mount("/proc", "proc", "proc", &["nosuid", "noexec", "nodev"]),
        mount(
            "/dev",
            "tmpfs",
            "tmpfs",
            &["nosuid", "strictatime", "mode=755", "size=65536k"],
        ),
        mount(
            "/dev/pts",
            "devpts",
            "devpts",
            &[
                "nosuid",
                "noexec",
                "newinstance",
                "ptmxmode=0666",
                "mode=0620",
                "gid=5",
            ],
        ),
        mount(
            "/dev/shm",
            "tmpfs",
            "shm",
            &["nosuid", "noexec", "nodev", "mode=1777", "size=65536k"],
        ),
        mount(
            "/dev/mqueue",
            "mqueue",
            "mqueue",
            &["nosuid", "noexec", "nodev"],
        ),
        mount(
            "/sys",
            "sysfs",
            "sysfs",
            &["nosuid", "noexec", "nodev", "ro"],
        ),
        mount(
            "/run",
            "tmpfs",
            "tmpfs",
            &["nosuid", "strictatime", "mode=755", "size=65536k"],
        ),
    ]
}

/// The standard guest device allowlist and CPU shares.
pub fn guest_resources() -> Resources {
    let dev = |kind: &str, major: i64, minor: i64| DeviceCgroup {
        allow: true,
        kind: kind.into(),
        major: (major != 0).then_some(major),
        minor: (minor >= 0).then_some(minor),
        access: "rwm".into(),
    };
    Resources {
        devices: vec![
            DeviceCgroup {
                allow: false,
                access: "rwm".into(),
                ..Default::default()
            },
            dev("c", 1, 3),    // /dev/null
            dev("c", 1, 8),    // /dev/random
            dev("c", 1, 7),    // /dev/full
            dev("c", 5, 0),    // /dev/tty
            dev("c", 1, 5),    // /dev/zero
            dev("c", 1, 9),    // /dev/urandom
            dev("c", 5, 1),    // /dev/console
            dev("c", 136, -1), // pts
            dev("c", 5, 2),    // /dev/ptmx
        ],
        cpu: Some(Cpu {
            shares: Some(1024),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::sample_actor_dirs;
    use super::super::{build, Memory, Options, Pids};
    use super::*;

    fn kata_id_ok(id: &str) -> bool {
        let b = id.as_bytes();
        b.len() >= 2
            && b[0].is_ascii_alphanumeric()
            && b[1..]
                .iter()
                .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(c))
    }

    #[test]
    fn guest_container_id_is_valid_and_distinct() {
        let shares = [SHARE_DURABLE, SHARE_CSI, SHARE_SYSTEM_INFO];
        let long = "a".repeat(63);
        for name in ["w", "app", long.as_str()].into_iter().chain(shares) {
            let id = guest_container_id(name);
            assert!(kata_id_ok(&id), "{id} is rejected by the guest agent");
            assert!(
                !shares.contains(&id.as_str()),
                "{id} collides with a share dir"
            );
        }
    }

    fn assert_default_device_allowlist(got: &[DeviceCgroup]) {
        assert_eq!(got, guest_resources().devices.as_slice());
    }

    #[test]
    fn merge_kata_resources_keeps_defaults() {
        let limit = 64 << 20;
        let got = merge_kata_resources(Some(Resources {
            memory: Some(Memory { limit: Some(limit) }),
            ..Default::default()
        }));
        assert_default_device_allowlist(&got.devices);
        assert_eq!(got.cpu.unwrap().shares, Some(1024));
        assert_eq!(got.memory.unwrap().limit, Some(limit));
    }

    #[test]
    fn merge_kata_resources_nil_is_defaults() {
        let got = merge_kata_resources(None);
        assert_default_device_allowlist(&got.devices);
        assert!(got.memory.is_none());
    }

    #[test]
    fn merge_kata_resources_carries_unknown_fields() {
        let mut other = serde_json::Map::new();
        other.insert(
            "hugepageLimits".into(),
            serde_json::json!([{"pageSize": "2MB", "limit": 1}]),
        );
        let got = merge_kata_resources(Some(Resources {
            pids: Some(Pids { limit: 128 }),
            devices: vec![DeviceCgroup {
                allow: true,
                kind: "c".into(),
                major: Some(195),
                access: "rwm".into(),
                ..Default::default()
            }],
            other,
            ..Default::default()
        }));
        assert_eq!(got.pids, Some(Pids { limit: 128 }));
        assert_eq!(got.devices.len(), 1);
        assert_eq!(got.devices[0].major, Some(195));
        assert!(got.other.contains_key("hugepageLimits"));
    }

    #[test]
    fn merge_kata_resources_cpu_quota_overlaid() {
        let got = merge_kata_resources(Some(Resources {
            cpu: Some(Cpu {
                quota: Some(20_000),
                period: Some(100_000),
                shares: None,
            }),
            ..Default::default()
        }));
        let cpu = got.cpu.unwrap();
        assert_eq!(
            (cpu.quota, cpu.period, cpu.shares),
            (Some(20_000), Some(100_000), Some(1024))
        );
    }

    fn app(resources: Option<pb::ResourceRequirements>) -> pb::Container {
        pb::Container {
            container_spec: Some(pb::ContainerSpec {
                args: vec!["/app".into()],
                resources,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn shaped(c: &pb::Container) -> Spec {
        let dirs = sample_actor_dirs();
        let mut spec = build(Options {
            container: Some(c),
            ..Default::default()
        });
        shape_microvm(
            &mut spec,
            MicroVmOptions {
                actor_dirs: Some(&dirs),
                container_name: "app",
            },
        )
        .unwrap();
        spec
    }

    #[test]
    fn shape_microvm_keeps_declared_container_limits() {
        let declared = 64 << 20;
        let spec = shaped(&app(Some(pb::ResourceRequirements {
            limits: Some(pb::Resources {
                memory_bytes: declared,
                cpu_millicores: 0,
            }),
        })));
        let r = spec.linux.unwrap().resources.unwrap();
        assert_eq!(r.memory.unwrap().limit, Some(declared));
    }

    #[test]
    fn shape_microvm_leaves_undeclared_container_unlimited() {
        let spec = shaped(&app(None));
        let r = spec.linux.unwrap().resources.unwrap();
        assert!(r.memory.and_then(|m| m.limit).unwrap_or(0) <= 0);
        assert!(r.cpu.and_then(|c| c.quota).unwrap_or(0) <= 0);
    }

    #[test]
    fn shape_microvm_allows_low_ports_in_the_guest() {
        let c = app(None);
        let spec = build(Options {
            container: Some(&c),
            ..Default::default()
        });
        assert!(!spec
            .linux
            .as_ref()
            .unwrap()
            .sysctl
            .contains_key(UNPRIVILEGED_PORT_START));
        assert_eq!(
            shaped(&c).linux.unwrap().sysctl[UNPRIVILEGED_PORT_START],
            "0"
        );

        let dirs = sample_actor_dirs();
        let mut preset = build(Options {
            container: Some(&c),
            ..Default::default()
        });
        preset
            .linux
            .as_mut()
            .unwrap()
            .sysctl
            .insert(UNPRIVILEGED_PORT_START.into(), "1024".into());
        shape_microvm(
            &mut preset,
            MicroVmOptions {
                actor_dirs: Some(&dirs),
                container_name: "app",
            },
        )
        .unwrap();
        assert_eq!(
            preset.linux.unwrap().sysctl[UNPRIVILEGED_PORT_START],
            "1024"
        );
    }
}

//! The OCI runtime spec of a container in the micro-VM, and its shaping for
//! the guest: system mounts the guest provides, volume binds pointed at the
//! shared virtio-fs tree, and the kata default resources.
//!
//! The runtime-neutral container spec comes from
//! [`hivekeep_sandbox_gvisor::oci::build_container`]; this model is the
//! superset of fields the guest agent understands.

use std::collections::BTreeMap;
use std::path::Path;

use hivekeep_proto::worker::v1 as pb;
use hivekeep_sandbox_gvisor::oci;
use serde::{Deserialize, Serialize};

/// Where the guest agent mounts the shared virtio-fs tree.
pub const GUEST_SHARED_DIR: &str = "/run/kata-containers/shared/containers";
/// Share subdirectory holding one directory per durable-dir volume.
pub const SHARE_DURABLE: &str = "durable";
/// Share subdirectory holding one directory per CSI volume.
pub const SHARE_CSI: &str = "csi";
/// Share subdirectory holding one directory per system-info volume.
pub const SHARE_SYSTEM_INFO: &str = "system-info";
/// A container's image volumes sit at `<container id>/volumes/<name>`.
pub const SHARE_VOLUMES: &str = "volumes";

/// The UTS hostname of Actor containers.
pub const HOSTNAME: &str = oci::HOSTNAME;

/// The guest container id for a container name. The guest agent requires
/// ids of at least two characters, and `_` is not valid in a name, so the id
/// collides with neither another name nor a share directory. It depends only
/// on the name: a restored guest keeps the ids it booted with.
pub fn guest_container_id(name: &str) -> String {
    format!("c_{name}")
}

/// A container's OCI runtime spec.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Spec {
    /// Spec version.
    pub oci_version: String,
    /// The container process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<Process>,
    /// The root filesystem.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<Root>,
    /// UTS hostname.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hostname: String,
    /// Mounts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<Mount>,
    /// Linux settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linux: Option<Linux>,
    /// Annotations.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

/// The container process.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Process {
    /// The user it runs as.
    pub user: User,
    /// Command line.
    pub args: Vec<String>,
    /// Environment.
    pub env: Vec<String>,
    /// Working directory.
    pub cwd: String,
    /// Capability sets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Capabilities>,
    /// Resource limits.
    #[serde(default)]
    pub rlimits: Vec<Rlimit>,
    /// Whether `no_new_privs` is set.
    #[serde(default)]
    pub no_new_privileges: bool,
}

/// The process user.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    /// User id.
    pub uid: u32,
    /// Group id.
    pub gid: u32,
    /// Supplementary groups.
    #[serde(default)]
    pub additional_gids: Vec<u32>,
    /// User name.
    #[serde(default)]
    pub username: String,
}

/// Capability sets.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Bounding set.
    pub bounding: Vec<String>,
    /// Effective set.
    pub effective: Vec<String>,
    /// Inheritable set.
    #[serde(default)]
    pub inheritable: Vec<String>,
    /// Permitted set.
    pub permitted: Vec<String>,
    /// Ambient set.
    #[serde(default)]
    pub ambient: Vec<String>,
}

/// One POSIX resource limit.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Rlimit {
    /// The limit, e.g. `RLIMIT_NOFILE`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Hard limit.
    pub hard: u64,
    /// Soft limit.
    pub soft: u64,
}

/// The root filesystem.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Root {
    /// Path to the rootfs.
    pub path: String,
    /// Whether it is read-only.
    #[serde(default)]
    pub readonly: bool,
}

/// One mount.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Mount {
    /// Where it is mounted in the container.
    pub destination: String,
    /// What is mounted.
    pub source: String,
    /// Filesystem type.
    #[serde(rename = "type")]
    pub kind: String,
    /// Mount options.
    #[serde(default)]
    pub options: Vec<String>,
}

/// Linux settings.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Linux {
    /// Namespaces.
    #[serde(default)]
    pub namespaces: Vec<Namespace>,
    /// Resource limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
    /// The cgroup path.
    #[serde(default)]
    pub cgroups_path: String,
    /// Paths masked from the container.
    #[serde(default)]
    pub masked_paths: Vec<String>,
    /// Paths made read-only.
    #[serde(default)]
    pub readonly_paths: Vec<String>,
    /// Sysctls.
    #[serde(default)]
    pub sysctl: BTreeMap<String, String>,
}

/// One namespace.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Namespace {
    /// Kind: `pid`, `network`, `ipc`, `uts`, `mount`, `cgroup`, `time`, `user`.
    #[serde(rename = "type")]
    pub kind: String,
    /// A namespace to join; empty creates one.
    #[serde(default)]
    pub path: String,
}

/// Resource limits.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Resources {
    /// Device cgroup rules.
    #[serde(default)]
    pub devices: Vec<DeviceCgroup>,
    /// Memory limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Memory>,
    /// CPU limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<Cpu>,
}

/// One device cgroup rule.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DeviceCgroup {
    /// Allow or deny.
    pub allow: bool,
    /// `c`, `b` or `a`; empty for all.
    #[serde(rename = "type", default)]
    pub kind: String,
    /// Major number; none for all.
    #[serde(default)]
    pub major: Option<i64>,
    /// Minor number; none for all.
    #[serde(default)]
    pub minor: Option<i64>,
    /// Access: some of `rwm`.
    #[serde(default)]
    pub access: String,
}

/// Memory limits.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Memory {
    /// Limit in bytes.
    #[serde(default)]
    pub limit: Option<i64>,
}

/// CPU limits.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Cpu {
    /// Relative weight.
    #[serde(default)]
    pub shares: Option<u64>,
    /// CFS quota in microseconds.
    #[serde(default)]
    pub quota: Option<i64>,
    /// CFS period in microseconds.
    #[serde(default)]
    pub period: Option<u64>,
}

impl From<&oci::Spec> for Spec {
    fn from(s: &oci::Spec) -> Self {
        Spec {
            oci_version: s.oci_version.clone(),
            process: s.process.as_ref().map(|p| Process {
                user: User {
                    uid: p.user.uid,
                    gid: p.user.gid,
                    ..Default::default()
                },
                args: p.args.clone(),
                env: p.env.clone(),
                cwd: p.cwd.clone(),
                capabilities: p.capabilities.as_ref().map(|c| Capabilities {
                    bounding: c.bounding.clone(),
                    effective: c.effective.clone(),
                    permitted: c.permitted.clone(),
                    ..Default::default()
                }),
                rlimits: p
                    .rlimits
                    .iter()
                    .map(|r| Rlimit {
                        kind: r.kind.clone(),
                        hard: r.hard,
                        soft: r.soft,
                    })
                    .collect(),
                no_new_privileges: false,
            }),
            root: s.root.as_ref().map(|r| Root {
                path: r.path.clone(),
                readonly: r.readonly,
            }),
            hostname: s.hostname.clone(),
            mounts: s
                .mounts
                .iter()
                .flatten()
                .map(|m| Mount {
                    destination: m.destination.clone(),
                    source: m.source.clone(),
                    kind: m.kind.clone(),
                    options: m.options.clone(),
                })
                .collect(),
            linux: s.linux.as_ref().map(|l| Linux {
                namespaces: l
                    .namespaces
                    .iter()
                    .map(|n| Namespace {
                        kind: n.kind.clone(),
                        path: n.path.clone(),
                    })
                    .collect(),
                resources: l.resources.as_ref().map(|r| Resources {
                    devices: vec![],
                    memory: r.memory.as_ref().map(|m| Memory { limit: m.limit }),
                    cpu: r.cpu.as_ref().map(|c| Cpu {
                        shares: None,
                        quota: c.quota,
                        period: c.period,
                    }),
                }),
                cgroups_path: l.cgroups_path.clone(),
                ..Default::default()
            }),
            annotations: s.annotations.clone(),
        }
    }
}

/// The runtime-neutral spec of one Actor container in this model.
pub fn build(actor_uid: &str, actor_dirs: &pb::ActorDirs, c: &pb::Container) -> Spec {
    Spec::from(&oci::build_container(actor_uid, actor_dirs, c))
}

/// Replaces host system mounts with the guest's, points volume binds at the
/// shared tree, fills in kata's default resources and lifts the guest's
/// privileged-port limit. Runs on an unshaped spec; a bind it cannot place in
/// the guest is an error.
pub fn shape_micro_vm(
    spec: &mut Spec,
    actor_dirs: &pb::ActorDirs,
    container: &str,
) -> Result<(), String> {
    let mut volumes = Vec::new();
    for m in &spec.mounts {
        if m.kind != "bind" {
            continue;
        }
        let source = guest_volume_source(&m.source, actor_dirs, container)
            .map_err(|e| format!("mount {:?}: {e}", m.destination))?;
        // rbind, so nested mount points come across.
        let options = m
            .options
            .iter()
            .map(|o| {
                if o == "bind" {
                    "rbind".to_string()
                } else {
                    o.clone()
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
    spec.mounts = mounts;
    let linux = spec.linux.get_or_insert_with(Linux::default);
    linux
        .sysctl
        .entry("net.ipv4.ip_unprivileged_port_start".into())
        .or_insert_with(|| "0".into());
    linux.resources = Some(merge_kata_resources(linux.resources.take()));
    Ok(())
}

/// Fills in the kata defaults the spec leaves unset; the spec's own values
/// win, and anything without a default passes through untouched.
fn merge_kata_resources(from: Option<Resources>) -> Resources {
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

/// Maps a volume's host directory to its path in the guest share.
fn guest_volume_source(host: &str, d: &pb::ActorDirs, container: &str) -> Result<String, String> {
    let bundle = Path::new(&d.oci_bundle_dir).join(container);
    let image_volumes = oci::image_volume_mount_path(&bundle, "");
    let image_volumes = image_volumes.to_string_lossy();
    let staged = [
        (
            d.durable_dir_volume_mounts_dir.as_str(),
            format!("{GUEST_SHARED_DIR}/{SHARE_DURABLE}"),
        ),
        (
            d.external_volume_mounts_dir.as_str(),
            format!("{GUEST_SHARED_DIR}/{SHARE_CSI}"),
        ),
        (
            d.system_info_volume_roots_dir.as_str(),
            format!("{GUEST_SHARED_DIR}/{SHARE_SYSTEM_INFO}"),
        ),
        (
            image_volumes.trim_end_matches('/'),
            format!(
                "{GUEST_SHARED_DIR}/{}/{SHARE_VOLUMES}",
                guest_container_id(container)
            ),
        ),
    ];
    for (host_dir, guest) in staged {
        if let Some(rel) = host.strip_prefix(&format!("{host_dir}/")) {
            return Ok(format!("{guest}/{rel}"));
        }
    }
    Err(format!(
        "host path {host:?} is not staged into the guest share"
    ))
}

fn mount(dest: &str, kind: &str, source: &str, opts: &[&str]) -> Mount {
    Mount {
        destination: dest.into(),
        kind: kind.into(),
        source: source.into(),
        options: opts.iter().map(|s| s.to_string()).collect(),
    }
}

/// The standard kata guest mount set.
fn guest_system_mounts() -> Vec<Mount> {
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

/// The kata device allowlist and CPU shares.
fn guest_resources() -> Resources {
    let dev = |major: i64, minor: i64| DeviceCgroup {
        allow: true,
        kind: "c".into(),
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
            dev(1, 3),    // /dev/null
            dev(1, 8),    // /dev/random
            dev(1, 7),    // /dev/full
            dev(5, 0),    // /dev/tty
            dev(1, 5),    // /dev/zero
            dev(1, 9),    // /dev/urandom
            dev(5, 1),    // /dev/console
            dev(136, -1), // pts
            dev(5, 2),    // /dev/ptmx
        ],
        memory: None,
        cpu: Some(Cpu {
            shares: Some(1024),
            ..Default::default()
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dirs() -> pb::ActorDirs {
        pb::ActorDirs {
            oci_bundle_dir: "/a/bundle".into(),
            durable_dir_volume_mounts_dir: "/a/durable".into(),
            external_volume_mounts_dir: "/a/csi".into(),
            system_info_volume_roots_dir: "/a/sysinfo".into(),
            ..Default::default()
        }
    }

    #[test]
    fn shaping_repoints_volume_binds_into_the_share() {
        let c = pb::Container {
            name: "app".into(),
            durable_dir_volume_mounts: vec![pb::VolumeMount {
                name: "data".into(),
                mount_path: "/data".into(),
            }],
            image_volume_mounts: vec![pb::VolumeMount {
                name: "models".into(),
                mount_path: "/models".into(),
            }],
            ..Default::default()
        };
        let mut spec = build("uid", &dirs(), &c);
        shape_micro_vm(&mut spec, &dirs(), "app").unwrap();
        let binds: Vec<_> = spec.mounts.iter().filter(|m| m.kind == "bind").collect();
        assert_eq!(binds[0].source, format!("{GUEST_SHARED_DIR}/durable/data"));
        assert_eq!(
            binds[1].source,
            format!("{GUEST_SHARED_DIR}/c_app/volumes/models")
        );
        assert!(binds.iter().all(|m| m.options[0] == "rbind"));
        let linux = spec.linux.unwrap();
        assert_eq!(linux.sysctl["net.ipv4.ip_unprivileged_port_start"], "0");
        let r = linux.resources.unwrap();
        assert_eq!(r.devices.len(), 10);
        assert_eq!(r.cpu.unwrap().shares, Some(1024));
    }

    #[test]
    fn shaping_refuses_an_unstaged_bind() {
        let mut spec = Spec {
            mounts: vec![mount("/x", "bind", "/elsewhere/x", &["bind"])],
            ..Default::default()
        };
        assert!(shape_micro_vm(&mut spec, &dirs(), "app").is_err());
    }
}

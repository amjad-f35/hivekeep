//! The OCI runtime specs of an Actor's sandbox and containers, shaped for
//! gVisor.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use hivekeep_proto::worker::v1 as pb;
use serde::{Deserialize, Serialize};

use crate::node_path;
use crate::sizing::{SandboxSize, CPU_QUOTA_PERIOD_US};

/// The runsc id of the sandbox. runsc creates it with `-no-root-container`,
/// so it has no process and no rootfs. The underscore keeps it outside the
/// DNS-label names containers are given, so no container can collide with it.
pub const SANDBOX_ID: &str = "_sandbox";

/// The UTS hostname of Actor containers.
pub const HOSTNAME: &str = "actor";

/// The spec's file name inside a bundle.
pub const SPEC_FILE: &str = "config.json";

/// The smallest CFS quota the kernel accepts (1ms).
const CPU_QUOTA_MIN_US: i64 = 1000;

/// File-descriptor limit of Actor processes: the kernel's default `fs.nr_open`.
const NOFILE_LIMIT: u64 = 1_048_576;

/// The subset of the OCI runtime spec the worker writes.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Spec {
    /// Spec version.
    pub oci_version: String,
    /// The container process; none for the sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<Process>,
    /// The root filesystem; none for the sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<Root>,
    /// UTS hostname.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hostname: String,
    /// Mounts; none for the sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mounts: Option<Vec<Mount>>,
    /// Linux-specific settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linux: Option<Linux>,
    /// Annotations runsc reads.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

/// A container process.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Process {
    /// User the process runs as.
    pub user: User,
    /// argv.
    #[serde(default)]
    pub args: Vec<String>,
    /// KEY=VALUE environment.
    #[serde(default)]
    pub env: Vec<String>,
    /// Working directory.
    pub cwd: String,
    /// Capability sets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Capabilities>,
    /// Resource limits.
    #[serde(default)]
    pub rlimits: Vec<Rlimit>,
}

/// A user and group id.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct User {
    /// User id.
    pub uid: u32,
    /// Group id.
    pub gid: u32,
}

/// Capability sets. Inheritable and ambient stay empty: capabilities pass
/// through the bounding set.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Bounding set.
    #[serde(default)]
    pub bounding: Vec<String>,
    /// Effective set.
    #[serde(default)]
    pub effective: Vec<String>,
    /// Permitted set.
    #[serde(default)]
    pub permitted: Vec<String>,
}

/// One POSIX rlimit.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Rlimit {
    /// e.g. `RLIMIT_NOFILE`.
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
    /// Path relative to the bundle.
    pub path: String,
    /// Read-only root.
    #[serde(default)]
    pub readonly: bool,
}

/// One mount.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Mount {
    /// Where it appears in the container.
    pub destination: String,
    /// Filesystem type.
    #[serde(rename = "type")]
    pub kind: String,
    /// Source.
    pub source: String,
    /// Mount options.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
}

/// Linux settings.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Linux {
    /// Namespaces to create or join.
    #[serde(default)]
    pub namespaces: Vec<Namespace>,
    /// cgroup limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
    /// cgroup path relative to the pod scope.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cgroups_path: String,
}

/// A namespace, joined when `path` is set.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Namespace {
    /// Namespace type.
    #[serde(rename = "type")]
    pub kind: String,
    /// Existing namespace to join.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub path: String,
}

/// cgroup limits.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Resources {
    /// Memory limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Memory>,
    /// CPU limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<Cpu>,
}

/// Memory limits.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Memory {
    /// Hard limit in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// CFS bandwidth.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Cpu {
    /// Quota per period, in microseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota: Option<i64>,
    /// Period, in microseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period: Option<u64>,
}

const OCI_VERSION: &str = "1.0.2";

fn namespaces(actor_uid: &str) -> Vec<Namespace> {
    let ns = |kind: &str, path: String| Namespace {
        kind: kind.into(),
        path,
    };
    vec![
        ns("pid", String::new()),
        ns(
            "network",
            node_path::actor_netns_path(actor_uid).display().to_string(),
        ),
        ns("ipc", String::new()),
        ns("uts", String::new()),
        ns("mount", String::new()),
    ]
}

fn container_resources(r: Option<&pb::ResourceRequirements>) -> Option<Resources> {
    let l = r.and_then(|r| r.limits).unwrap_or_default();
    if l.memory_bytes <= 0 && l.cpu_millicores <= 0 {
        return None;
    }
    Some(Resources {
        memory: (l.memory_bytes > 0).then_some(Memory {
            limit: Some(l.memory_bytes),
        }),
        cpu: (l.cpu_millicores > 0).then(|| Cpu {
            quota: Some((l.cpu_millicores * CPU_QUOTA_PERIOD_US / 1000).max(CPU_QUOTA_MIN_US)),
            period: Some(CPU_QUOTA_PERIOD_US as u64),
        }),
    })
}

/// Where an image volume is mounted inside a container's bundle.
pub fn image_volume_mount_path(bundle: &Path, volume: &str) -> std::path::PathBuf {
    bundle.join("volumes").join(volume)
}

/// The spec of a sandbox with no root container: hostname and namespaces,
/// and no process, rootfs or mounts.
pub fn build_sandbox(actor_uid: &str) -> Spec {
    Spec {
        oci_version: OCI_VERSION.into(),
        hostname: HOSTNAME.into(),
        linux: Some(Linux {
            namespaces: namespaces(actor_uid),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The runtime-neutral spec of one Actor container.
pub fn build_container(actor_uid: &str, actor_dirs: &pb::ActorDirs, c: &pb::Container) -> Spec {
    let cs = c.container_spec.clone().unwrap_or_default();
    let mut mounts = vec![
        Mount {
            destination: "/proc".into(),
            kind: "proc".into(),
            source: "proc".into(),
            options: vec![],
        },
        Mount {
            destination: "/dev".into(),
            kind: "tmpfs".into(),
            source: "tmpfs".into(),
            options: vec![],
        },
        Mount {
            destination: "/sys".into(),
            kind: "sysfs".into(),
            source: "sysfs".into(),
            options: ["nosuid", "noexec", "nodev", "ro"]
                .map(String::from)
                .to_vec(),
        },
    ];
    // Mount order across volume kinds does not matter: validation rejects
    // mounts that nest under one another.
    let mut bind = |dest: &str, src: &Path, mode: &str| {
        mounts.push(Mount {
            destination: dest.into(),
            kind: "bind".into(),
            source: src.display().to_string(),
            options: vec!["bind".into(), mode.into()],
        })
    };
    let dir = |d: &str| Path::new(d).to_path_buf();
    for m in &c.durable_dir_volume_mounts {
        bind(
            &m.mount_path,
            &dir(&actor_dirs.durable_dir_volume_mounts_dir).join(&m.name),
            "rw",
        );
    }
    for m in &c.external_volume_mounts {
        bind(
            &m.mount_path,
            &dir(&actor_dirs.external_volume_mounts_dir).join(&m.name),
            "rw",
        );
    }
    for m in &c.system_info_volume_mounts {
        bind(
            &m.mount_path,
            &dir(&actor_dirs.system_info_volume_roots_dir).join(&m.name),
            "ro",
        );
    }
    let bundle = dir(&actor_dirs.oci_bundle_dir).join(&c.name);
    for m in &c.image_volume_mounts {
        bind(
            &m.mount_path,
            &image_volume_mount_path(&bundle, &m.name),
            "ro",
        );
    }

    Spec {
        oci_version: OCI_VERSION.into(),
        process: Some(Process {
            user: User::default(),
            args: cs.args.clone(),
            env: cs.env.clone(),
            cwd: "/".into(),
            capabilities: Some(Capabilities {
                bounding: cs.capabilities.clone(),
                effective: cs.capabilities.clone(),
                permitted: cs.capabilities.clone(),
            }),
            rlimits: vec![Rlimit {
                kind: "RLIMIT_NOFILE".into(),
                hard: NOFILE_LIMIT,
                soft: NOFILE_LIMIT,
            }],
        }),
        root: Some(Root {
            path: "rootfs".into(),
            readonly: false,
        }),
        hostname: HOSTNAME.into(),
        mounts: Some(mounts),
        linux: Some(Linux {
            namespaces: namespaces(actor_uid),
            resources: container_resources(cs.resources.as_ref()),
            cgroups_path: String::new(),
        }),
        annotations: BTreeMap::new(),
    }
}

/// An Actor container's cgroup leaf, relative to the pod scope.
pub fn gvisor_cgroup_leaf(actor_uid: &str, container: &str) -> String {
    format!("{actor_uid}-{container}")
}

/// What [`shape_gvisor`] needs to know about the container.
#[derive(Debug, Clone, Copy)]
pub struct GvisorOptions<'a> {
    /// The Actor's uid.
    pub actor_uid: &'a str,
    /// The Actor's directories.
    pub actor_dirs: &'a pb::ActorDirs,
    /// The container (or [`SANDBOX_ID`]).
    pub container: &'a str,
    /// Durable-dir volume names, declared on the sandbox only.
    pub durable_volumes: &'a [String],
    /// Sizes the container's cgroup leaf.
    pub size: SandboxSize,
}

/// Adds the CRI annotations runsc reads, durable-dir mount hints and a
/// per-container cgroup to `spec`, and applies the sandbox size. Idempotent.
pub fn shape_gvisor(spec: &mut Spec, o: GvisorOptions<'_>) {
    let a = &mut spec.annotations;
    a.insert(
        "io.kubernetes.cri.container-name".into(),
        o.container.into(),
    );
    if o.container == SANDBOX_ID {
        a.insert("io.kubernetes.cri.container-type".into(), "sandbox".into());
        for v in o.durable_volumes {
            let key = |k: &str| format!("dev.gvisor.spec.mount.{v}.{k}");
            a.insert(key("type"), "tmpfs".into());
            a.insert(key("share"), "container".into());
            a.insert(
                key("source"),
                Path::new(&o.actor_dirs.durable_dir_volume_mounts_dir)
                    .join(v)
                    .display()
                    .to_string(),
            );
        }
    } else {
        a.insert(
            "io.kubernetes.cri.container-type".into(),
            "container".into(),
        );
        a.insert("io.kubernetes.cri.sandbox-id".into(), SANDBOX_ID.into());
    }
    let linux = spec.linux.get_or_insert_with(Linux::default);
    // A colon-free path relative to the pod scope.
    if linux.cgroups_path.is_empty() {
        linux.cgroups_path = format!("/{}", gvisor_cgroup_leaf(o.actor_uid, o.container));
    }
    apply_size(linux, o.size);
}

/// Writes the sandbox's CPU and memory limits into `linux.resources`, so
/// runsc creates the cgroup leaf with them. Unset sizes leave the spec alone.
fn apply_size(linux: &mut Linux, size: SandboxSize) {
    if size.milli_cpu <= 0 && size.memory_bytes <= 0 {
        return;
    }
    let r = linux.resources.get_or_insert_with(Resources::default);
    if let Some(quota) = size.cpu_quota_us() {
        let cpu = r.cpu.get_or_insert_with(Cpu::default);
        cpu.period = Some(CPU_QUOTA_PERIOD_US as u64);
        cpu.quota = Some(quota);
    }
    if size.memory_bytes > 0 {
        r.memory.get_or_insert_with(Memory::default).limit = Some(size.memory_bytes);
    }
}

/// Writes `spec` into the bundle at `bundle`.
pub fn save(bundle: &Path, spec: &Spec) -> io::Result<()> {
    let path = bundle.join(SPEC_FILE);
    let b = serde_json::to_vec_pretty(spec).map_err(io::Error::other)?;
    write_private(&path, &b)
        .map_err(|e| io::Error::new(e.kind(), format!("writing {}: {e}", path.display())))
}

fn write_private(path: &Path, data: &[u8]) -> io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dirs() -> pb::ActorDirs {
        pb::ActorDirs {
            oci_bundle_dir: "/a/bundles".into(),
            durable_dir_volume_mounts_dir: "/a/durable".into(),
            external_volume_mounts_dir: "/a/volumes".into(),
            system_info_volume_roots_dir: "/a/sysinfo".into(),
            ..Default::default()
        }
    }

    #[test]
    fn container_spec_binds_each_volume_kind() {
        let c = pb::Container {
            name: "app".into(),
            durable_dir_volume_mounts: vec![pb::VolumeMount {
                name: "data".into(),
                mount_path: "/data".into(),
            }],
            system_info_volume_mounts: vec![pb::VolumeMount {
                name: "info".into(),
                mount_path: "/info".into(),
            }],
            image_volume_mounts: vec![pb::VolumeMount {
                name: "img".into(),
                mount_path: "/img".into(),
            }],
            container_spec: Some(pb::ContainerSpec {
                args: vec!["/app".into()],
                resources: Some(pb::ResourceRequirements {
                    limits: Some(pb::Resources {
                        cpu_millicores: 5,
                        memory_bytes: 0,
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let spec = build_container("uid-a", &dirs(), &c);
        let mounts = spec.mounts.unwrap();
        let binds: Vec<_> = mounts
            .iter()
            .filter(|m| m.kind == "bind")
            .map(|m| {
                (
                    m.destination.as_str(),
                    m.source.as_str(),
                    m.options[1].as_str(),
                )
            })
            .collect();
        assert_eq!(
            binds,
            vec![
                ("/data", "/a/durable/data", "rw"),
                ("/info", "/a/sysinfo/info", "ro"),
                ("/img", "/a/bundles/app/volumes/img", "ro"),
            ]
        );
        let cpu = spec.linux.unwrap().resources.unwrap().cpu.unwrap();
        // 5 millicores is below the kernel's minimum quota.
        assert_eq!(cpu.quota, Some(CPU_QUOTA_MIN_US));
    }

    #[test]
    fn shaping_annotates_and_sizes() {
        let mut spec = build_sandbox("uid-a");
        let vols = vec!["data".to_string()];
        let o = GvisorOptions {
            actor_uid: "uid-a",
            actor_dirs: &dirs(),
            container: SANDBOX_ID,
            durable_volumes: &vols,
            size: SandboxSize {
                milli_cpu: 1500,
                memory_bytes: 1 << 30,
            },
        };
        shape_gvisor(&mut spec, o);
        shape_gvisor(&mut spec, o);
        assert_eq!(
            spec.annotations["io.kubernetes.cri.container-type"],
            "sandbox"
        );
        assert_eq!(
            spec.annotations["dev.gvisor.spec.mount.data.source"],
            "/a/durable/data"
        );
        let linux = spec.linux.unwrap();
        assert_eq!(linux.cgroups_path, "/uid-a-_sandbox");
        let r = linux.resources.unwrap();
        assert_eq!(r.cpu.unwrap().quota, Some(150_000));
        assert_eq!(r.memory.unwrap().limit, Some(1 << 30));
        assert!(spec.process.is_none() && spec.root.is_none() && spec.mounts.is_none());
    }
}

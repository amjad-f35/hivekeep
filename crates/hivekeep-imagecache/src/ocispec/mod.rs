//! The runtime-neutral OCI runtime spec of an actor's containers, which each
//! worker then shapes for its runtime ([`gvisor`], [`microvm`]).
//!
//! Only the subset of the runtime spec the workers write is modelled;
//! unknown resource fields are carried through untouched, so a field added
//! upstream reaches the runtime rather than being dropped here.

pub mod gvisor;
pub mod microvm;
pub mod sizing;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use hivekeep_proto::worker::v1 as pb;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::spec::image_volume_mount_path;

/// The spec's file name in a bundle.
pub const SPEC_FILE: &str = "config.json";
/// The UTS hostname of actor containers.
pub const HOSTNAME: &str = "actor";
/// The runtime spec version written.
pub const OCI_VERSION: &str = "1.0.2";

/// The CFS period a CPU quota is expressed against (100ms, the kernel
/// default): N milli-cores may run N/1000 of every period.
pub const CPU_QUOTA_PERIOD_US: i64 = 100_000;
/// The smallest quota the kernel accepts (1ms): limits under 10m are
/// raised to it rather than producing a spec the runtime refuses.
pub const CPU_QUOTA_MIN_US: i64 = 1000;
/// File-descriptor limit of actor processes: the kernel's default
/// `fs.nr_open` (proxy-heavy actors exhaust lower ones).
pub const NOFILE_LIMIT: u64 = 1_048_576;

/// The actor's network namespace path, created by the worker.
pub fn actor_netns_path(actor_uid: &str) -> String {
    format!("/run/netns/worker-actor:{actor_uid}")
}

/// An OCI runtime spec.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Spec {
    /// The spec version.
    #[serde(default)]
    pub oci_version: String,
    /// The container process; none for a sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<Process>,
    /// The root filesystem; none for a sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<Root>,
    /// UTS hostname.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hostname: String,
    /// Mounts; none for a sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mounts: Option<Vec<Mount>>,
    /// Linux settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linux: Option<Linux>,
    /// Runtime annotations.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

/// A container process.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Process {
    /// The user it runs as.
    pub user: User,
    /// argv.
    #[serde(default)]
    pub args: Vec<String>,
    /// `KEY=value` environment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<String>,
    /// Working directory.
    pub cwd: String,
    /// Capability sets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Capabilities>,
    /// Resource limits.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rlimits: Vec<Rlimit>,
}

/// A numeric user.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct User {
    /// uid.
    pub uid: u32,
    /// gid.
    pub gid: u32,
}

/// Capability sets.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Bounding set.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bounding: Vec<String>,
    /// Effective set.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effective: Vec<String>,
    /// Inheritable set.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inheritable: Vec<String>,
    /// Permitted set.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub permitted: Vec<String>,
    /// Ambient set.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ambient: Vec<String>,
}

/// A POSIX resource limit.
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
    /// Bundle-relative path.
    pub path: String,
    /// Mounted read-only.
    #[serde(default)]
    pub readonly: bool,
}

/// A mount.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Mount {
    /// Where in the container.
    pub destination: String,
    /// Filesystem type.
    #[serde(rename = "type", default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    /// Source.
    #[serde(default, skip_serializing_if = "String::is_empty")]
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub namespaces: Vec<Namespace>,
    /// cgroup limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
    /// The cgroup path.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cgroups_path: String,
    /// Kernel parameters.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sysctl: BTreeMap<String, String>,
}

/// A namespace.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Namespace {
    /// e.g. `network`.
    #[serde(rename = "type")]
    pub kind: String,
    /// A namespace to join instead of creating one.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub path: String,
}

/// cgroup limits.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Resources {
    /// Device allowlist.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub devices: Vec<DeviceCgroup>,
    /// Memory limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Memory>,
    /// CPU limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<Cpu>,
    /// Process count limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pids: Option<Pids>,
    /// Fields not modelled here, carried through.
    #[serde(flatten)]
    pub other: serde_json::Map<String, serde_json::Value>,
}

/// One device cgroup rule.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceCgroup {
    /// Allow or deny.
    pub allow: bool,
    /// `c`, `b` or `a`; empty means all.
    #[serde(rename = "type", default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    /// Major number; none is the wildcard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub major: Option<i64>,
    /// Minor number; none is the wildcard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minor: Option<i64>,
    /// `rwm` subset.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub access: String,
}

/// Memory limits.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Memory {
    /// Bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// CPU limits.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cpu {
    /// Relative weight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shares: Option<u64>,
    /// Microseconds per period.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota: Option<i64>,
    /// The period in microseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period: Option<u64>,
}

/// Process count limit.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pids {
    /// Maximum processes.
    pub limit: i64,
}

/// One actor container to build a spec for.
#[derive(Debug, Clone, Default)]
pub struct Options<'a> {
    /// Names the actor's network namespace.
    pub actor_uid: &'a str,
    /// The actor's directories bind sources are joined from.
    pub actor_dirs: Option<&'a pb::ActorDirs>,
    /// The process and its volume mounts.
    pub container: Option<&'a pb::Container>,
}

/// The resolved limits as `linux.resources`; none when nothing is set, so a
/// container declaring no limits carries no resources at all.
pub fn oci_resources(r: Option<&pb::ResourceRequirements>) -> Option<Resources> {
    let l = r.and_then(|r| r.limits.as_ref());
    let (mem, cpu) = (
        l.map_or(0, |l| l.memory_bytes),
        l.map_or(0, |l| l.cpu_millicores),
    );
    if mem <= 0 && cpu <= 0 {
        return None;
    }
    let mut out = Resources::default();
    if mem > 0 {
        out.memory = Some(Memory { limit: Some(mem) });
    }
    if cpu > 0 {
        out.cpu = Some(Cpu {
            shares: None,
            quota: Some((cpu * CPU_QUOTA_PERIOD_US / 1000).max(CPU_QUOTA_MIN_US)),
            period: Some(CPU_QUOTA_PERIOD_US as u64),
        });
    }
    Some(out)
}

fn namespaces(actor_uid: &str) -> Vec<Namespace> {
    let ns = |k: &str, path: String| Namespace {
        kind: k.to_string(),
        path,
    };
    vec![
        ns("pid", String::new()),
        ns("network", actor_netns_path(actor_uid)),
        ns("ipc", String::new()),
        ns("uts", String::new()),
        ns("mount", String::new()),
    ]
}

/// The spec of a sandbox with no root container: hostname and namespaces,
/// no process, rootfs or mounts.
pub fn build_sandbox(actor_uid: &str) -> Spec {
    Spec {
        oci_version: OCI_VERSION.to_string(),
        hostname: HOSTNAME.to_string(),
        linux: Some(Linux {
            namespaces: namespaces(actor_uid),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn join(dir: &str, name: &str) -> String {
    Path::new(dir).join(name).to_string_lossy().into_owned()
}

/// The runtime-neutral spec of one actor container.
pub fn build(o: Options<'_>) -> Spec {
    let default_container = pb::Container::default();
    let c = o.container.unwrap_or(&default_container);
    let default_spec = pb::ContainerSpec::default();
    let cs = c.container_spec.as_ref().unwrap_or(&default_spec);
    let caps = cs.capabilities.clone();
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
    // Order across volume kinds does not matter: the API rejects mounts
    // nesting under one another.
    let mut bind = |dest: &str, src: String, mode: &str| {
        mounts.push(Mount {
            destination: dest.to_string(),
            kind: "bind".into(),
            source: src,
            options: vec!["bind".into(), mode.into()],
        })
    };
    let default_dirs = pb::ActorDirs::default();
    let dirs = o.actor_dirs.unwrap_or(&default_dirs);
    for m in &c.durable_dir_volume_mounts {
        bind(
            &m.mount_path,
            join(&dirs.durable_dir_volume_mounts_dir, &m.name),
            "rw",
        );
    }
    for m in &c.external_volume_mounts {
        bind(
            &m.mount_path,
            join(&dirs.external_volume_mounts_dir, &m.name),
            "rw",
        );
    }
    for m in &c.system_info_volume_mounts {
        bind(
            &m.mount_path,
            join(&dirs.system_info_volume_roots_dir, &m.name),
            "ro",
        );
    }
    let bundle = PathBuf::from(&dirs.oci_bundle_dir).join(&c.name);
    for m in &c.image_volume_mounts {
        bind(
            &m.mount_path,
            image_volume_mount_path(&bundle, &m.name)
                .to_string_lossy()
                .into_owned(),
            "ro",
        );
    }
    Spec {
        oci_version: OCI_VERSION.to_string(),
        process: Some(Process {
            user: User { uid: 0, gid: 0 },
            args: cs.args.clone(),
            env: cs.env.clone(),
            cwd: "/".into(),
            // Inheritable and ambient stay empty; capabilities inherit
            // through the bounding set.
            capabilities: Some(Capabilities {
                bounding: caps.clone(),
                effective: caps.clone(),
                permitted: caps,
                ..Default::default()
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
        hostname: HOSTNAME.to_string(),
        mounts: Some(mounts),
        linux: Some(Linux {
            namespaces: namespaces(o.actor_uid),
            resources: oci_resources(cs.resources.as_ref()),
            ..Default::default()
        }),
        annotations: BTreeMap::new(),
    }
}

/// Writes `spec` as the bundle's OCI spec.
pub fn save(bundle: &Path, spec: &Spec) -> Result<()> {
    let path = bundle.join(SPEC_FILE);
    let b = serde_json::to_vec_pretty(spec)
        .map_err(|e| Error::msg(format!("marshaling {path:?}: {e}")))?;
    crate::store::write_private(&path, &b).map_err(|e| Error::io(format!("writing {path:?}"), e))
}

#[cfg(test)]
pub(crate) mod tests;

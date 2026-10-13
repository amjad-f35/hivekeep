//! What each local worker advertises: the runtimes of its pool's sandbox
//! config, with the hardware attributes the control plane matches actors
//! against.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::assets::{AssetEntry, SandboxAssetsRecord};
use crate::runtime_index::HashedVersion;

/// The node architecture attribute.
pub const ATTR_ARCHITECTURE: &str = "architecture";
/// The CPU features a gVisor sandbox exposes.
pub const ATTR_CPU_FEATURES: &str = "cpu_features";
/// The host CPU vendor, for micro-VM snapshots.
pub const ATTR_CPU_VENDOR: &str = "cpu_vendor";
/// The host CPU model, for micro-VM snapshots.
pub const ATTR_CPU_MODEL: &str = "cpu_model";

/// Bounds one probe's asset fetch.
pub const PROBE_FETCH_TIMEOUT: Duration = Duration::from_secs(10 * 60);
/// Bounds one `runsc cpu-features` run.
pub const CPU_FEATURES_TIMEOUT: Duration = Duration::from_secs(30);

/// This node's architecture, as sandbox configs name it.
pub fn node_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

pub use hivekeep_node::sandbox_config::{AssetFile, SandboxConfig, SandboxVersion, VersionState};

/// The plan for one worker: its config, class and versions, default first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxPlan {
    /// The config's name.
    pub config: String,
    /// The config's class.
    pub sandbox_class: String,
    /// The default version first, then the other enabled ones.
    pub versions: Vec<(String, bool, SandboxAssetsRecord)>,
}

/// Why a worker's runtimes could not be planned or probed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SyncError {
    /// Retrying cannot help until the config changes.
    #[error("permanent failure: {0}")]
    Permanent(String),
    /// The pool or its config is not known yet.
    #[error("unresolved sandbox config: {0}")]
    Unresolved(String),
    /// Some non-default versions could not be probed; the file was written
    /// without them.
    #[error("incomplete sandbox runtimes: {0}")]
    Incomplete(String),
    /// Anything else, worth a retry.
    #[error("{0}")]
    Other(String),
}

/// Plans a config's versions for this node's architecture: the enabled
/// default first, then every other enabled version with assets here.
pub fn plan_config(cfg: &SandboxConfig) -> Result<SandboxPlan, SyncError> {
    let def = cfg.default_version_config().ok_or_else(|| {
        SyncError::Permanent(format!(
            "SandboxConfig {:?} has no enabled version {:?} named by its defaultVersion",
            cfg.name, cfg.default_version
        ))
    })?;
    let def_rec = record_for_arch(cfg, def).ok_or_else(|| {
        SyncError::Permanent(format!(
            "SandboxConfig {:?}, default version {:?} has no assets for architecture {:?}",
            cfg.name,
            def.name,
            node_arch()
        ))
    })?;
    let mut versions = vec![(def.name.clone(), true, def_rec)];
    for v in &cfg.versions {
        if v.name == def.name || v.state == VersionState::Disabled {
            continue;
        }
        if let Some(rec) = record_for_arch(cfg, v) {
            versions.push((v.name.clone(), false, rec));
        }
    }
    Ok(SandboxPlan {
        config: cfg.name.clone(),
        sandbox_class: cfg.sandbox_class.clone(),
        versions,
    })
}

fn record_for_arch(cfg: &SandboxConfig, v: &SandboxVersion) -> Option<SandboxAssetsRecord> {
    let files = v.assets.get(node_arch())?;
    if files.is_empty() {
        return None;
    }
    Some(SandboxAssetsRecord {
        sandbox_class: cfg.sandbox_class.clone(),
        assets: files
            .iter()
            .map(|(n, f)| {
                (
                    n.clone(),
                    AssetEntry {
                        url: f.url.clone(),
                        sha256: f.sha256.clone(),
                    },
                )
            })
            .collect(),
        ..Default::default()
    })
}

/// Hashes each planned version.
pub fn hash_versions(plan: &SandboxPlan) -> Vec<HashedVersion> {
    plan.versions
        .iter()
        .map(|(name, is_default, rec)| HashedVersion {
            name: name.clone(),
            is_default: *is_default,
            hash: rec.bundle_hash(),
            rec: rec.clone(),
        })
        .collect()
}

/// A worker's sandbox runtimes file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimesFile {
    /// One entry per sandbox class the worker runs.
    pub sandbox_classes: Vec<RuntimesClass>,
}

/// One class in a runtimes file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimesClass {
    /// The sandbox class.
    pub sandbox_class: String,
    /// The runtime new actors start on.
    pub default_runtime: Runtime,
    /// Other runtimes the worker can restore snapshots of.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub restorable_runtimes: Vec<Runtime>,
}

/// One runtime: its compatibility attributes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Runtime {
    /// The attributes, by key.
    pub version: BTreeMap<String, String>,
}

/// Writes `file` at `path` unless it already holds exactly this content.
/// Reports whether it wrote.
pub fn write_runtimes_file(path: &Path, file: &RuntimesFile) -> Result<bool, String> {
    let data = serde_json::to_vec_pretty(file)
        .map_err(|e| format!("while encoding {}: {e}", path.display()))?;
    if std::fs::read(path).is_ok_and(|cur| cur == data) {
        return Ok(false);
    }
    crate::atomicfile::write(path, &data, 0o644)
        .map_err(|e| format!("while writing {}: {e}", path.display()))?;
    Ok(true)
}

/// Reads a runtimes file, refusing unknown fields.
pub fn read_runtimes_file(path: &Path) -> Result<RuntimesFile, String> {
    let data = std::fs::read(path).map_err(|e| e.to_string())?;
    serde_json::from_slice(&data).map_err(|e| format!("while decoding {}: {e}", path.display()))
}

/// Probes of the host and of runtime binaries.
#[async_trait]
pub trait HostProbe: Send + Sync + 'static {
    /// The host CPU's vendor and model.
    fn cpu_identity(&self) -> Result<(String, String), String>;
    /// The CPU feature names a gVisor sandbox from `runsc` exposes.
    async fn cpu_features(&self, runsc: &Path) -> Result<Vec<String>, SyncError>;
    /// Encodes feature names as the cpu_features attribute value.
    fn encode_cpu_features(&self, arch: &str, names: &[String]) -> Result<String, String>;
}

/// The host probe used in production.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemHostProbe;

#[async_trait]
impl HostProbe for SystemHostProbe {
    fn cpu_identity(&self) -> Result<(String, String), String> {
        let info = std::fs::read_to_string("/proc/cpuinfo")
            .map_err(|e| format!("while reading /proc/cpuinfo: {e}"))?;
        parse_cpu_info(&info)
    }

    async fn cpu_features(&self, runsc: &Path) -> Result<Vec<String>, SyncError> {
        run_cpu_features(runsc, CPU_FEATURES_TIMEOUT).await
    }

    fn encode_cpu_features(&self, arch: &str, names: &[String]) -> Result<String, String> {
        hivekeep_core::cpu_features::encode_cpu_features(arch, names).map_err(|e| e.to_string())
    }
}

/// Runs `runsc cpu-features`, bounded by `timeout`, and returns the feature
/// names it prints. A binary that fails or prints nothing will do so again
/// and is a permanent failure; one that cannot start or hangs may not be.
pub async fn run_cpu_features(runsc: &Path, timeout: Duration) -> Result<Vec<String>, SyncError> {
    let mut cmd = tokio::process::Command::new(runsc);
    cmd.arg("cpu-features").env_clear().kill_on_drop(true);
    let out = match tokio::time::timeout(timeout, cmd.output()).await {
        Err(_) => {
            return Err(SyncError::Other(format!(
                "while running {} cpu-features: timed out",
                runsc.display()
            )))
        }
        Ok(Err(e)) => {
            return Err(SyncError::Other(format!(
                "while starting {} cpu-features: {e}",
                runsc.display()
            )))
        }
        Ok(Ok(o)) => o,
    };
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        return Err(SyncError::Permanent(format!(
            "{} cpu-features: {} ({stderr})",
            runsc.display(),
            out.status
        )));
    }
    let printed = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if printed.is_empty() {
        return Err(SyncError::Permanent(format!(
            "{} cpu-features printed no features",
            runsc.display()
        )));
    }
    Ok(printed.split(',').map(|s| s.to_string()).collect())
}

/// The vendor and model in `/proc/cpuinfo` text.
pub fn parse_cpu_info(info: &str) -> Result<(String, String), String> {
    let mut vendor = None;
    let mut model = None;
    for line in info.lines() {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        let (k, v) = (k.trim(), v.trim());
        match k {
            "vendor_id" | "CPU implementer" if vendor.is_none() => vendor = Some(v.to_string()),
            "model name" | "CPU part" if model.is_none() => model = Some(v.to_string()),
            _ => {}
        }
    }
    match (vendor, model) {
        (Some(v), Some(m)) => Ok((v, m)),
        _ => Err("cpuinfo names no CPU vendor and model".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_index::ATTR_SANDBOX_BUNDLE_HASH;
    use std::collections::HashMap;

    fn cfg() -> SandboxConfig {
        let assets = |d: &str| {
            let mut m = HashMap::new();
            let mut files = BTreeMap::new();
            files.insert(
                "gvisor".to_string(),
                AssetFile {
                    url: format!("gs://gvisor/{d}.tar.zst"),
                    sha256: d.repeat(64),
                },
            );
            m.insert(node_arch().to_string(), files);
            m
        };
        SandboxConfig {
            name: "gvisor-cfg".into(),
            generation: 1,
            sandbox_class: "gvisor".into(),
            default_version: "v2".into(),
            versions: vec![
                SandboxVersion {
                    name: "v1".into(),
                    state: VersionState::Enabled,
                    assets: assets("a"),
                },
                SandboxVersion {
                    name: "v2".into(),
                    state: VersionState::Enabled,
                    assets: assets("b"),
                },
                SandboxVersion {
                    name: "v0".into(),
                    state: VersionState::Disabled,
                    assets: assets("c"),
                },
                SandboxVersion {
                    name: "other-arch".into(),
                    state: VersionState::Enabled,
                    assets: HashMap::new(),
                },
            ],
        }
    }

    #[test]
    fn plans_default_first_and_skips_disabled_or_foreign() {
        let plan = plan_config(&cfg()).unwrap();
        let names: Vec<_> = plan.versions.iter().map(|v| (v.0.as_str(), v.1)).collect();
        assert_eq!(names, vec![("v2", true), ("v1", false)]);
        let hashed = hash_versions(&plan);
        assert_ne!(hashed[0].hash, hashed[1].hash);
    }

    #[test]
    fn a_missing_default_is_permanent() {
        let mut c = cfg();
        c.default_version = "v0".into();
        assert!(matches!(plan_config(&c), Err(SyncError::Permanent(_))));
        let mut c = cfg();
        c.default_version = "other-arch".into();
        assert!(matches!(plan_config(&c), Err(SyncError::Permanent(_))));
    }

    #[test]
    fn runtimes_file_round_trips_and_skips_identical_writes() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("sandbox-runtimes.json");
        let mut attrs = BTreeMap::new();
        attrs.insert(ATTR_SANDBOX_BUNDLE_HASH.to_string(), "sha256:x".to_string());
        let f = RuntimesFile {
            sandbox_classes: vec![RuntimesClass {
                sandbox_class: "gvisor".into(),
                default_runtime: Runtime { version: attrs },
                restorable_runtimes: vec![],
            }],
        };
        assert!(write_runtimes_file(&p, &f).unwrap());
        assert!(!write_runtimes_file(&p, &f).unwrap());
        assert_eq!(read_runtimes_file(&p).unwrap(), f);
        let text = std::fs::read_to_string(&p).unwrap();
        assert!(text.contains("\"sandboxClasses\"") && text.contains("\"defaultRuntime\""));
        std::fs::write(&p, r#"{"sandboxClasses":[],"extra":1}"#).unwrap();
        assert!(read_runtimes_file(&p).is_err());
    }

    #[test]
    fn parses_cpu_identity() {
        let x86 = "processor\t: 0\nvendor_id\t: GenuineIntel\nmodel name\t: Xeon\n";
        assert_eq!(
            parse_cpu_info(x86).unwrap(),
            ("GenuineIntel".into(), "Xeon".into())
        );
        let arm = "CPU implementer\t: 0x41\nCPU part\t: 0xd0c\n";
        assert_eq!(
            parse_cpu_info(arm).unwrap(),
            ("0x41".into(), "0xd0c".into())
        );
        assert!(parse_cpu_info("").is_err());
    }
}

#[cfg(test)]
mod behavior_tests;

//! `SandboxConfig`: the versions of a sandbox runtime's binaries.

use std::collections::BTreeMap;

use kube::{CustomResource, KubeSchema};
use serde::{Deserialize, Serialize};

use super::sandbox_class::SandboxClass;

/// Marks a SandboxConfig as a default for its sandbox class when set to
/// `"true"`. Several configs of a class may carry it; the newest wins.
pub const CLASS_DEFAULT_ANNOTATION: &str = "sandboxconfig.hivekeep.io/is-class-default";

/// Put on a SandboxConfig by the controller before any WorkerPool's status
/// records it, and kept until the config is deleted.
///
/// Deleting a config that carries it takes three steps:
///
/// 1. The delete only sets `deletionTimestamp`; the config stays,
///    Terminating.
/// 2. The controller checks whether any WorkerPool's
///    `status.sandboxClasses` names the config in `configRef` or
///    `pendingConfigRef`. While one does, the finalizer stays (rechecked on
///    every WorkerPool change); once none does, it is removed.
/// 3. With no finalizers left, the API server deletes the config.
///
/// Without the controller, step 2 never happens and the config stays
/// Terminating until the finalizer is cleared by hand. A config no pool ever
/// used carries no finalizer and deletes at once.
///
/// A pool switching to a config names it in `pendingConfigRef` first, then
/// re-reads it from the API server and moves `configRef` only if that read
/// finds it live, so a config is never released while a `configRef` names
/// it.
pub const WORKER_POOL_PROTECTION_FINALIZER: &str =
    "sandboxconfig.hivekeep.io/workerpool-protection";

/// Maximum number of versions per config.
pub const MAX_VERSIONS: usize = 16;

/// The assets every architecture of a gVisor version must define.
pub const GVISOR_REQUIRED_ASSETS: &[&str] = &["gvisor"];
/// The assets every architecture of a micro-VM version must define.
pub const MICROVM_REQUIRED_ASSETS: &[&str] =
    &["cloud-hypervisor", "virtiofsd", "kata-kernel", "kata-image"];

/// One content-addressed file the keeper fetches for a sandbox runtime.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, KubeSchema)]
pub struct AssetFile {
    /// Where to download the asset (e.g. a `gs://` or `s3://` URL). The
    /// keeper fetches it through the node's object-store plugin, which
    /// refuses URLs under a snapshot location; a `gs://` URL the plugin
    /// cannot serve is read anonymously, which covers public buckets.
    #[schemars(length(min = 1, max = 2048))]
    pub url: String,
    /// Lower-case hex SHA-256 of the asset: names the cached file and
    /// verifies the download.
    #[schemars(regex(pattern = r"^[a-f0-9]{64}$"))]
    pub sha256: String,
}

/// Whether a version may be used.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
pub enum SandboxVersionState {
    /// Usable. The default.
    #[default]
    Enabled,
    /// Not usable. The default version cannot be disabled.
    Disabled,
}

/// One version of a SandboxConfig: the asset set a sandbox boots with.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, KubeSchema)]
pub struct SandboxVersionConfig {
    /// Identifies the version within its config; a DNS label.
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
    )]
    #[serde(default)]
    pub name: String,
    /// Enabled (default) or Disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<SandboxVersionState>,
    /// Files the keeper fetches for this runtime, keyed by architecture
    /// (`amd64`, `arm64`) and then by asset name. The names are the sandbox
    /// backend's business: gVisor needs a `gvisor` release tarball; the
    /// micro-VM backend needs [`MICROVM_REQUIRED_ASSETS`]. Per-class
    /// requirements are enforced by an admission policy.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub assets: BTreeMap<String, BTreeMap<String, AssetFile>>,
}

impl SandboxVersionConfig {
    /// Whether the version is Disabled.
    pub fn is_disabled(&self) -> bool {
        self.state == Some(SandboxVersionState::Disabled)
    }
}

/// Cluster-scoped configuration of the sandbox binary versions for one
/// sandbox runtime family. WorkerPools reference it by
/// `spec.sandboxClasses[].configRef`, or resolve it through
/// [`CLASS_DEFAULT_ANNOTATION`], which decouples binary selection from the
/// workload definition.
#[allow(clippy::duplicated_attributes)]
#[derive(CustomResource, Debug, Clone, Default, PartialEq, Serialize, Deserialize, KubeSchema)]
#[kube(
    group = "hivekeep.io",
    version = "v1",
    kind = "SandboxConfig",
    shortname = "sandboxconfig",
    printcolumn(name = "Class", type_ = "string", json_path = ".spec.sandboxClass"),
    printcolumn(name = "Default", type_ = "string", json_path = ".spec.defaultVersion"),
    printcolumn(
        name = "Age",
        type_ = "date",
        json_path = ".metadata.creationTimestamp"
    )
)]
#[x_kube(
    validation = Rule::new("self.versions.exists(v, v.name == self.defaultVersion)").message("defaultVersion must name an entry in versions"),
    validation = Rule::new("!self.versions.exists(v, v.name == self.defaultVersion && has(v.state) && v.state == 'Disabled')").message("the version named by defaultVersion must not be Disabled")
)]
#[serde(rename_all = "camelCase")]
pub struct SandboxConfigSpec {
    /// The sandbox runtime family this config applies to. A WorkerPool only
    /// uses configs whose class matches its `spec.sandboxClasses` entry.
    #[serde(default)]
    pub sandbox_class: SandboxClass,
    /// The version new sandboxes boot with; must name an existing,
    /// non-Disabled entry of `versions`.
    #[schemars(length(min = 1, max = 63))]
    #[serde(default)]
    pub default_version: String,
    /// The versions this config offers, keyed by name.
    #[schemars(length(min = 1, max = 16))]
    #[x_kube(merge_strategy = ListMerge::Map(vec!["name".into()]))]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub versions: Vec<SandboxVersionConfig>,
}

impl SandboxConfigSpec {
    /// The entry of `versions` named by `default_version`. A Disabled entry
    /// is never returned: the schema rejects a Disabled default, so a spec
    /// that reads back that way has no default version.
    pub fn default_version_config(&self) -> Option<&SandboxVersionConfig> {
        self.versions
            .iter()
            .find(|v| v.name == self.default_version)
            .filter(|v| !v.is_disabled())
    }
}

impl SandboxConfig {
    /// Whether the config is annotated as its class's default.
    pub fn is_class_default(&self) -> bool {
        self.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(CLASS_DEFAULT_ANNOTATION))
            .is_some_and(|v| v == "true")
    }

    /// Whether the config carries the WorkerPool protection finalizer.
    pub fn is_protected(&self) -> bool {
        self.metadata
            .finalizers
            .as_ref()
            .is_some_and(|f| f.iter().any(|f| f == WORKER_POOL_PROTECTION_FINALIZER))
    }

    /// Whether the config is being deleted.
    pub fn is_deleting(&self) -> bool {
        self.metadata.deletion_timestamp.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(name: &str, state: Option<SandboxVersionState>) -> SandboxVersionConfig {
        SandboxVersionConfig {
            name: name.into(),
            state,
            ..Default::default()
        }
    }

    #[test]
    fn default_version_config() {
        use SandboxVersionState::*;
        let cases: Vec<(&str, SandboxConfigSpec, Option<&str>)> = vec![
            (
                "default with nil state is returned",
                SandboxConfigSpec {
                    default_version: "v1".into(),
                    versions: vec![version("v1", None)],
                    ..Default::default()
                },
                Some("v1"),
            ),
            (
                "default explicitly Enabled is returned",
                SandboxConfigSpec {
                    default_version: "v1".into(),
                    versions: vec![version("v1", Some(Enabled))],
                    ..Default::default()
                },
                Some("v1"),
            ),
            (
                "default picked out of several versions",
                SandboxConfigSpec {
                    default_version: "v2".into(),
                    versions: vec![
                        version("v1", Some(Disabled)),
                        version("v2", None),
                        version("v3", None),
                    ],
                    ..Default::default()
                },
                Some("v2"),
            ),
            (
                "Disabled default is treated as missing",
                SandboxConfigSpec {
                    default_version: "v1".into(),
                    versions: vec![version("v1", Some(Disabled)), version("v2", None)],
                    ..Default::default()
                },
                None,
            ),
            (
                "defaultVersion names no entry",
                SandboxConfigSpec {
                    default_version: "v9".into(),
                    versions: vec![version("v1", None)],
                    ..Default::default()
                },
                None,
            ),
            (
                "no versions",
                SandboxConfigSpec {
                    default_version: "v1".into(),
                    ..Default::default()
                },
                None,
            ),
        ];
        for (name, spec, want) in cases {
            let got = spec.default_version_config();
            match want {
                None => assert!(got.is_none(), "{name}: got {got:?}"),
                Some(want) => {
                    let got = got.unwrap_or_else(|| panic!("{name}: no default"));
                    assert_eq!(got.name, want, "{name}");
                    // The reference aliases the spec's entry, not a copy.
                    let idx = spec.versions.iter().position(|v| v.name == want).unwrap();
                    assert!(
                        std::ptr::eq(got, &spec.versions[idx]),
                        "{name}: returned a copy"
                    );
                }
            }
        }
    }
}

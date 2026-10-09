//! Cluster-scoped sandbox configs as the keeper reads them: the runtime
//! versions a pool's workers run for one sandbox class, and their assets.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

/// A sandbox version's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum VersionState {
    /// Offered to workers.
    #[default]
    Enabled,
    /// Withdrawn.
    Disabled,
}

/// One asset of a version, for one architecture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AssetFile {
    /// Where to fetch it.
    pub url: String,
    /// Its hex sha256.
    pub sha256: String,
}

/// One version of a sandbox config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SandboxVersion {
    /// The version's name.
    pub name: String,
    /// Whether it is offered.
    #[serde(default)]
    pub state: VersionState,
    /// Assets by architecture, then by asset name.
    #[serde(default)]
    pub assets: HashMap<String, BTreeMap<String, AssetFile>>,
}

/// A cluster-scoped sandbox config: the runtime versions a pool's workers
/// run for one sandbox class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SandboxConfig {
    /// The config's name.
    pub name: String,
    /// Bumped on every spec change.
    #[serde(default)]
    pub generation: i64,
    /// The sandbox class.
    pub sandbox_class: String,
    /// The version new actors run.
    pub default_version: String,
    /// Every version.
    #[serde(default)]
    pub versions: Vec<SandboxVersion>,
}

impl SandboxConfig {
    /// The enabled default version.
    pub fn default_version_config(&self) -> Option<&SandboxVersion> {
        self.versions
            .iter()
            .find(|v| v.name == self.default_version && v.state == VersionState::Enabled)
    }
}

//! The sandbox runtimes file the keeper writes for each worker pod: per
//! sandbox class, the default runtime and the runtimes it can restore from,
//! each as its compatibility attributes. Its format is the keeper's; this is
//! the reader's side of it.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// The file.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimesFile {
    /// One entry per sandbox class.
    #[serde(default)]
    pub sandbox_classes: Vec<RuntimesClass>,
}

/// One class.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimesClass {
    /// The class.
    pub sandbox_class: String,
    /// The runtime fresh Actors start on.
    #[serde(default)]
    pub default_runtime: Runtime,
    /// Runtimes whose snapshots this worker can restore.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub restorable_runtimes: Vec<Runtime>,
}

/// One runtime.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Runtime {
    /// The compatibility attributes, by key.
    #[serde(default)]
    pub version: BTreeMap<String, String>,
}

/// Reads the file. A missing file is `NotFound`; a malformed one is
/// `InvalidData`.
pub fn read(path: &Path) -> io::Result<RuntimesFile> {
    let data = std::fs::read(path)?;
    serde_json::from_slice(&data).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("while decoding {}: {e}", path.display()),
        )
    })
}

/// Writes the file atomically (for tests and tools).
pub fn write(path: &Path, f: &RuntimesFile) -> io::Result<()> {
    let data = serde_json::to_vec_pretty(f).map_err(io::Error::other)?;
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data)?;
    std::fs::rename(tmp, path)
}

impl RuntimesFile {
    /// The entry for `class`.
    pub fn class(&self, class: &str) -> Option<&RuntimesClass> {
        self.sandbox_classes
            .iter()
            .find(|c| c.sandbox_class == class)
    }
}

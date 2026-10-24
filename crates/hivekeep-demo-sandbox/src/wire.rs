//! JSON bodies of `POST /process`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// What to run.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessRequest {
    /// argv; the first element is resolved on `PATH`.
    #[serde(default)]
    pub command: Vec<String>,
    /// Added to the workload's own environment.
    #[serde(
        default,
        rename = "envvars",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub env_vars: BTreeMap<String, String>,
    /// Working directory; the workload's own when empty.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cwd: String,
    /// A duration such as `30s` or `1m30s`; no limit when empty.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub timeout: String,
}

/// What happened.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessResponse {
    pub stdout: String,
    pub stderr: String,
    /// The exit status; -1 when the command did not exit normally or never
    /// started.
    #[serde(rename = "exitCode", default)]
    pub exit_code: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

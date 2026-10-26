//! The built-in script variants and loading a script from a file.

use std::path::Path;

use super::script::Script;
use super::script_file::{decode, ScriptError};

/// The variant a run uses when no script is named: the 20-step coding
/// session.
pub const DEFAULT_SCRIPT: &str = "coding-session";

/// The built-in variants, selectable by name with `agentsession_script`.
const BUILT_IN: &[(&str, &str)] = &[
    (
        "coding-session",
        include_str!("scripts/coding-session.yaml"),
    ),
    (
        "personal-assistant",
        include_str!("scripts/personal-assistant.yaml"),
    ),
    (
        "personal-assistant-light",
        include_str!("scripts/personal-assistant-light.yaml"),
    ),
];

/// The built-in variant names, sorted.
pub fn names() -> Vec<&'static str> {
    let mut n: Vec<_> = BUILT_IN.iter().map(|(n, _)| *n).collect();
    n.sort_unstable();
    n
}

/// The raw YAML of a script source: a file on the worker when `from_file`,
/// else a built-in variant name.
pub(super) fn read_script(source: &str, from_file: bool) -> Result<Vec<u8>, ScriptError> {
    if from_file {
        return std::fs::read(source)
            .map_err(|e| ScriptError(format!("read agent-session script: {e}")));
    }
    BUILT_IN
        .iter()
        .find(|(n, _)| *n == source)
        .map(|(_, data)| data.as_bytes().to_vec())
        .ok_or_else(|| {
            ScriptError(format!(
                "no built-in agent-session script {source:?} (have {})",
                names().join(", ")
            ))
        })
}

/// Decodes YAML read from `source`. A built-in variant's name must match
/// its file name, so a knob value always matches what stats and logs
/// report; a file's name is unconstrained.
pub(super) fn decode_script(
    data: &[u8],
    source: &str,
    from_file: bool,
) -> Result<Script, ScriptError> {
    let s = decode(data).map_err(|e| ScriptError(format!("agent-session script {source}: {e}")))?;
    if !from_file && s.name != source {
        return Err(ScriptError(format!(
            "built-in script file {source:?} names itself {:?}",
            s.name
        )));
    }
    Ok(s)
}

/// Reads a script from a YAML file on the worker, typically a mounted
/// ConfigMap.
pub fn load_file(path: &Path) -> Result<Script, ScriptError> {
    let source = path.to_string_lossy();
    let data = read_script(&source, true)?;
    decode_script(&data, &source, true)
}

/// The built-in variant called `name`.
pub fn load(name: &str) -> Result<Script, ScriptError> {
    let data = read_script(name, false)?;
    decode_script(&data, name, false)
}

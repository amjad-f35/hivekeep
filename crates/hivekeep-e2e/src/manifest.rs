//! Rendering the fixture manifest templates.
//!
//! Templates carry two kinds of `${...}` placeholder:
//!
//! - inline, substituted wherever they appear (an empty value just
//!   disappears);
//! - block, which must be the entire content of its line. A block expands
//!   to a YAML fragment that brings its own indentation, and an empty value
//!   takes the whole line with it. Requiring the placeholder to be the whole
//!   line is what lets a comment mention one without being deleted.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::harness::T;

/// The placeholder values one rendering uses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Substitutions {
    /// Inline placeholders and their values.
    pub inline: BTreeMap<String, String>,
    /// Whole-line block placeholders and their fragments.
    pub blocks: BTreeMap<String, String>,
}

impl Substitutions {
    /// Sets the inline placeholder `key` (e.g. `"${NAME}"`).
    pub fn inline(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.inline.insert(key.into(), value.into());
        self
    }

    /// Sets the block placeholder `key`.
    pub fn block(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.blocks.insert(key.into(), value.into());
        self
    }

    /// Adds every entry of `other`, which wins on conflicts.
    pub fn extend(&mut self, other: &Substitutions) -> &mut Self {
        self.inline.extend(other.inline.clone());
        self.blocks.extend(other.blocks.clone());
        self
    }
}

/// Substitutes `subs` into the template text `raw`.
pub fn render_str(raw: &str, subs: &Substitutions) -> String {
    let mut out = Vec::new();
    for line in raw.split('\n') {
        if let Some(value) = subs.blocks.get(line.trim()) {
            if !value.is_empty() {
                out.push(value.clone());
            }
            continue;
        }
        let mut line = line.to_string();
        for (placeholder, value) in &subs.inline {
            line = line.replace(placeholder.as_str(), value);
        }
        out.push(line);
    }
    out.join("\n")
}

/// The absolute path of `rel_path`, relative to this crate (fixtures live
/// under `fixtures/`).
pub fn fixture_path(rel_path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(rel_path)
}

/// Renders the template at `rel_path` (relative to this crate) into a file
/// in a temp dir of `t` and returns its path, so an apply and a later delete
/// consume the same file with no shell involved. A `.tmpl` suffix is
/// dropped from the file name.
pub fn render_manifest(t: &T, rel_path: &str, subs: &Substitutions) -> PathBuf {
    let path = fixture_path(rel_path);
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| t.fatal(format!("reading manifest template {rel_path}: {e}")));
    let base = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let rendered = t.temp_dir().join(base.trim_end_matches(".tmpl"));
    std::fs::write(&rendered, render_str(&raw, subs)).unwrap_or_else(|e| {
        t.fatal(format!(
            "writing rendered manifest {}: {e}",
            rendered.display()
        ))
    });
    rendered
}

/// Renders `items` as `key:` followed by their YAML, every line indented
/// by `indent` spaces, for a block placeholder. An empty list renders to
/// `""`, which drops the placeholder's whole line, key included: a bare
/// `volumes:` with nothing under it is not what the caller meant.
///
/// Serializing real API types rather than taking YAML text keeps a fragment
/// honest: a misspelled field does not compile.
pub fn yaml_list_block<S: Serialize>(
    key: &str,
    items: &[S],
    indent: usize,
) -> Result<String, String> {
    if items.is_empty() {
        return Ok(String::new());
    }
    let raw = serde_yaml_ng::to_string(items)
        .map_err(|e| format!("marshaling {key} for the manifest: {e}"))?;
    let pad = " ".repeat(indent);
    let mut out = vec![format!("{pad}{key}:")];
    for line in raw.trim_end_matches('\n').split('\n') {
        out.push(format!("{pad}{line}"));
    }
    Ok(out.join("\n"))
}

/// Applies the manifest at `path` to the cluster under test.
pub fn kubectl_apply(t: &T, path: &Path) {
    crate::cmd::kubectl(t, &["apply", "-f", &path.to_string_lossy()]);
}

/// Deletes what the manifest at `path` declares, tolerating absence.
pub fn kubectl_delete(t: &T, path: &Path) {
    crate::cmd::kubectl(
        t,
        &[
            "delete",
            "--ignore-not-found",
            "-f",
            &path.to_string_lossy(),
        ],
    );
}

/// Splits a rendered multi-document YAML stream into its documents.
pub fn split_documents(rendered: &str) -> Vec<&str> {
    rendered
        .split("\n---")
        .map(|d| d.trim_start_matches('\n'))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_and_block_placeholders() {
        let mut s = Substitutions::default();
        s.inline("${A}", "x")
            .inline("${EMPTY}", "")
            .block("${BLOCK}", "  b: 1")
            .block("${GONE}", "");
        let raw = "a: ${A}${EMPTY}\n${BLOCK}\n  ${GONE}\n# mentions ${GONE} inline\nend";
        assert_eq!(
            render_str(raw, &s),
            "a: x\n  b: 1\n# mentions ${GONE} inline\nend"
        );
    }

    #[test]
    fn yaml_list_blocks() {
        #[derive(Serialize)]
        struct Item {
            name: &'static str,
        }
        assert_eq!(yaml_list_block::<Item>("volumes", &[], 2).unwrap(), "");
        assert_eq!(
            yaml_list_block("volumes", &[Item { name: "a" }, Item { name: "b" }], 2).unwrap(),
            "  volumes:\n  - name: a\n  - name: b"
        );
    }
}

/// Decodes one YAML document as the Kubernetes type `K`, failing on any
/// field `K` does not model. The Kubernetes types ignore unknown fields, so
/// strictness is a round trip: every field of the document must survive
/// decoding and re-encoding with its value. That is what catches a block
/// fragment indented to the wrong parent.
pub fn strict_k8s<K: serde::de::DeserializeOwned + Serialize>(doc: &str) -> Result<K, String> {
    let original: serde_json::Value =
        serde_yaml_ng::from_str(doc).map_err(|e| format!("invalid YAML: {e}"))?;
    let typed: K = serde_json::from_value(original.clone()).map_err(|e| e.to_string())?;
    let back = serde_json::to_value(&typed).map_err(|e| e.to_string())?;
    subset(&original, &back, "").map(|()| typed)
}

fn subset(orig: &serde_json::Value, back: &serde_json::Value, path: &str) -> Result<(), String> {
    use serde_json::Value;
    match (orig, back) {
        (Value::Object(o), Value::Object(b)) => {
            for (k, v) in o {
                let p = format!("{path}.{k}");
                match b.get(k) {
                    Some(bv) => subset(v, bv, &p)?,
                    None if v.is_null() => {}
                    None => return Err(format!("unknown field {p}")),
                }
            }
            Ok(())
        }
        (Value::Array(o), Value::Array(b)) if o.len() == b.len() => o
            .iter()
            .zip(b)
            .enumerate()
            .try_for_each(|(i, (x, y))| subset(x, y, &format!("{path}[{i}]"))),
        (Value::Number(x), Value::Number(y)) if x.as_f64() == y.as_f64() => Ok(()),
        (x, y) if x == y => Ok(()),
        (x, y) => Err(format!(
            "field {path} = {x} does not decode as written (got {y})"
        )),
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    use k8s_openapi::api::core::v1::Pod;

    #[test]
    fn strict_decoding_catches_misplaced_fields() {
        let good = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: p\nspec:\n  containers:\n  - name: c\n    image: i\n";
        assert!(strict_k8s::<Pod>(good).is_ok());
        let bad = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: p\nspec:\n  containers:\n  - name: c\n    image: i\n  mountPath: /x\n";
        assert!(strict_k8s::<Pod>(bad)
            .unwrap_err()
            .contains("spec.mountPath"));
    }
}

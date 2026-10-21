//! Editing individual documents of a YAML stream while leaving the rest of
//! it byte for byte as written.
//!
//! The install steps patch a few objects of a rendered stream (a container's
//! args, say) and splice text markers that live in comments elsewhere in it.
//! Re-encoding the whole stream would drop those comments, so only the
//! documents an edit actually changes are re-encoded.

use super::{Result, StepError};
use serde_json::Value;

/// Whether `line` separates two documents.
fn is_separator(line: &str) -> bool {
    let rest = match line.strip_prefix("---") {
        Some(r) => r,
        None => return false,
    };
    rest.is_empty() || rest.starts_with(' ') || rest.starts_with('\t') || rest == "\r"
}

/// The stream as alternating pieces: document text, then the separator line
/// that ends it (empty for the last).
fn pieces(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut doc = String::new();
    for line in text.split_inclusive('\n') {
        let bare = line.strip_suffix('\n').unwrap_or(line);
        if is_separator(bare) {
            out.push((std::mem::take(&mut doc), line.to_string()));
        } else {
            doc.push_str(line);
        }
    }
    out.push((doc, String::new()));
    out
}

/// The documents of a stream, as text.
pub fn split_documents(text: &str) -> Vec<String> {
    pieces(text).into_iter().map(|(d, _)| d).collect()
}

/// Decodes one document; `None` for an empty one (comments only, or blank).
pub fn decode_document(text: &str) -> Result<Option<Value>> {
    let yaml: serde_yaml::Value = serde_yaml::from_str(text)
        .map_err(|e| StepError::msg(format!("while parsing manifest: {e}")))?;
    let json: Value = serde_json::to_value(&yaml)
        .map_err(|e| StepError::msg(format!("while parsing manifest: {e}")))?;
    let mut json = json;
    normalize_octal_modes(&mut json);
    Ok(match json {
        Value::Null => None,
        Value::Object(ref m) if m.is_empty() => None,
        other => Some(other),
    })
}

pub use crate::kube::normalize_octal_modes;

/// Every non-empty document of a stream, decoded.
pub fn decode_all(manifest: &[u8]) -> Result<Vec<Value>> {
    let text = std::str::from_utf8(manifest)
        .map_err(|e| StepError::msg(format!("manifest is not UTF-8: {e}")))?;
    let mut out = Vec::new();
    for doc in split_documents(text) {
        if let Some(v) = decode_document(&doc)? {
            out.push(v);
        }
    }
    Ok(out)
}

/// Calls `edit` on every non-empty document. A document `edit` reports as
/// changed is re-encoded; every other byte of the stream is kept.
pub fn edit_documents(
    manifest: &[u8],
    mut edit: impl FnMut(&mut Value) -> Result<bool>,
) -> Result<Vec<u8>> {
    let text = std::str::from_utf8(manifest)
        .map_err(|e| StepError::msg(format!("manifest is not UTF-8: {e}")))?;
    let mut out = String::with_capacity(text.len());
    for (doc, sep) in pieces(text) {
        let mut replaced = false;
        if let Some(mut value) = decode_document(&doc)? {
            if edit(&mut value)? {
                let encoded = serde_yaml::to_string(&value)
                    .map_err(|e| StepError::msg(format!("while writing manifest: {e}")))?;
                out.push_str(&encoded);
                if !encoded.ends_with('\n') {
                    out.push('\n');
                }
                replaced = true;
            }
        }
        if !replaced {
            out.push_str(&doc);
        }
        out.push_str(&sep);
    }
    Ok(out.into_bytes())
}

/// Where a workload keeps its container lists.
pub const CONTAINER_LIST_PATHS: &[&[&str]] = &[
    &["spec", "containers"],
    &["spec", "initContainers"],
    &["spec", "template", "spec", "containers"],
    &["spec", "template", "spec", "initContainers"],
    &[
        "spec",
        "jobTemplate",
        "spec",
        "template",
        "spec",
        "containers",
    ],
    &[
        "spec",
        "jobTemplate",
        "spec",
        "template",
        "spec",
        "initContainers",
    ],
];

/// Calls `f` on every container of a decoded object; true when any call
/// changed one.
pub fn for_each_container_mut(obj: &mut Value, mut f: impl FnMut(&mut Value) -> bool) -> bool {
    let mut changed = false;
    for path in CONTAINER_LIST_PATHS {
        if let Some(Value::Array(list)) = obj.pointer_mut(&format!("/{}", path.join("/"))) {
            for c in list.iter_mut() {
                changed |= f(c);
            }
        }
    }
    changed
}

/// Every container of a decoded object.
pub fn containers(obj: &Value) -> Vec<&Value> {
    CONTAINER_LIST_PATHS
        .iter()
        .filter_map(|p| {
            obj.pointer(&format!("/{}", p.join("/")))
                .and_then(Value::as_array)
        })
        .flatten()
        .collect()
}

/// A container's args as strings.
pub fn args_of(container: &Value) -> Vec<String> {
    container
        .get("args")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| v.to_string())
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn octal_modes_become_integers() {
        let v = decode_document("volumes:\n- secret:\n    defaultMode: 0600\n    items:\n    - mode: 0o644\nmode: internal\n").unwrap().unwrap();
        assert_eq!(
            v.pointer("/volumes/0/secret/defaultMode"),
            Some(&Value::from(384))
        );
        assert_eq!(
            v.pointer("/volumes/0/secret/items/0/mode"),
            Some(&Value::from(420))
        );
        assert_eq!(v["mode"], "internal");
    }

    #[test]
    fn untouched_documents_keep_their_bytes() {
        let text = "# lead\na: 1\n---\nb: [2]  # keep\n---\n";
        let out = edit_documents(text.as_bytes(), |_| Ok(false)).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), text);
    }

    #[test]
    fn edited_document_is_reencoded() {
        let text = "a: 1\n---\nb: 2 # gone\n";
        let out = edit_documents(text.as_bytes(), |v| {
            if v.get("b").is_some() {
                v["b"] = Value::from(3);
                return Ok(true);
            }
            Ok(false)
        })
        .unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "a: 1\n---\nb: 3\n");
    }
}

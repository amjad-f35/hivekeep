//! Reading manifests from disk and decoding multi-document YAML or JSON.
//!
//! A directory is read non-recursively, in lexical file order, taking only
//! `.yaml`, `.yml` and `.json` files. [`read_path`] and [`load_path`] cover
//! the same documents in the same order: the pre-built image resolver
//! rewrites what `read_path` returns and that is applied in place of what
//! `load_path` would have applied.

use super::{KubeError, Object, ObjectExt};
use serde::Deserialize;
use std::path::{Path, PathBuf};

const MANIFEST_EXTENSIONS: [&str; 3] = ["yaml", "yml", "json"];

/// Decodes every document in a YAML (or JSON) stream. Empty documents are
/// skipped; a document without a kind is an error.
pub fn decode_manifest(text: &str) -> Result<Vec<Object>, KubeError> {
    let mut objs = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(text) {
        let value = serde_yaml::Value::deserialize(doc)
            .map_err(|e| KubeError::Decode(format!("while decoding a manifest document: {e}")))?;
        let json: serde_json::Value = serde_json::to_value(&value)
            .map_err(|e| KubeError::Decode(format!("while decoding a manifest document: {e}")))?;
        match &json {
            serde_json::Value::Null => continue,
            serde_json::Value::Object(m) if m.is_empty() => continue,
            serde_json::Value::Object(_) => {}
            _ => {
                return Err(KubeError::Decode(
                    "while decoding a manifest document: not a mapping".into(),
                ))
            }
        }
        let mut json = json;
        normalize_octal_modes(&mut json);
        if json.kind().is_empty() {
            return Err(KubeError::Decode("manifest document has no kind".into()));
        }
        objs.push(json);
    }
    Ok(objs)
}

/// Turns file modes written as YAML 1.1 octal (`defaultMode: 0600`) into
/// the integers Kubernetes reads them as. The YAML 1.2 parser keeps such a
/// scalar as the string "0600", which the API server would reject.
pub fn normalize_octal_modes(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(m) => {
            for (k, item) in m.iter_mut() {
                if k == "defaultMode" || k == "mode" {
                    if let Some(n) = item.as_str().and_then(parse_octal) {
                        *item = serde_json::Value::from(n);
                        continue;
                    }
                }
                normalize_octal_modes(item);
            }
        }
        serde_json::Value::Array(a) => a.iter_mut().for_each(normalize_octal_modes),
        _ => {}
    }
}

fn parse_octal(s: &str) -> Option<i64> {
    let digits = s.strip_prefix("0o").or_else(|| s.strip_prefix('0'))?;
    if digits.is_empty() || !digits.chars().all(|c| ('0'..='7').contains(&c)) {
        return None;
    }
    i64::from_str_radix(digits, 8).ok()
}

/// [`decode_manifest`] over raw bytes.
pub fn decode_manifest_bytes(data: &[u8]) -> Result<Vec<Object>, KubeError> {
    let text = std::str::from_utf8(data)
        .map_err(|e| KubeError::Decode(format!("manifest is not UTF-8: {e}")))?;
    decode_manifest(text)
}

/// Encodes objects as a multi-document YAML stream.
pub fn encode_manifest(objs: &[Object]) -> Result<String, KubeError> {
    let mut out = String::new();
    for obj in objs {
        out.push_str("---\n");
        let text = serde_yaml::to_string(obj).map_err(|e| {
            KubeError::Decode(format!("while encoding {}: {e}", super::describe(obj)))
        })?;
        out.push_str(&text);
    }
    Ok(out)
}

/// Decodes every object in a file, or in the manifests of a directory.
pub fn load_path(path: &Path) -> Result<Vec<Object>, KubeError> {
    let mut objs = Vec::new();
    for file in manifest_files(path)? {
        let text = std::fs::read_to_string(&file)
            .map_err(|e| KubeError::io(&e, format!("while opening {}", file.display())))?;
        let file_objs =
            decode_manifest(&text).map_err(|e| e.context(format!("in {}", file.display())))?;
        objs.extend(file_objs);
    }
    Ok(objs)
}

/// The raw bytes of a file, or of a directory's manifests concatenated with a
/// document separator before each.
pub fn read_path(path: &Path) -> Result<Vec<u8>, KubeError> {
    let mut out = Vec::new();
    for file in manifest_files(path)? {
        let data = std::fs::read(&file)
            .map_err(|e| KubeError::io(&e, format!("while reading {}", file.display())))?;
        out.extend_from_slice(b"---\n");
        out.extend_from_slice(&data);
        if !data.ends_with(b"\n") {
            out.push(b'\n');
        }
    }
    Ok(out)
}

fn manifest_files(path: &Path) -> Result<Vec<PathBuf>, KubeError> {
    let meta = std::fs::metadata(path)
        .map_err(|e| KubeError::io(&e, format!("while reading {}", path.display())))?;
    if !meta.is_dir() {
        return Ok(vec![path.to_path_buf()]);
    }
    let entries = std::fs::read_dir(path)
        .map_err(|e| KubeError::io(&e, format!("while listing {}", path.display())))?;
    let mut names = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|e| KubeError::io(&e, format!("while listing {}", path.display())))?;
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let p = entry.path();
        let ext = p
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase());
        if ext.is_some_and(|e| MANIFEST_EXTENSIONS.contains(&e.as_str())) {
            names.push(p);
        }
    }
    names.sort();
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn write_files(files: &BTreeMap<&str, String>) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (name, body) in files {
            let path = dir.path().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, body).unwrap();
        }
        dir
    }

    fn manifest(name: &str) -> String {
        format!("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {name}\n")
    }

    fn names(data: &[u8]) -> Vec<String> {
        decode_manifest_bytes(data)
            .unwrap()
            .iter()
            .map(|o| o.name().to_string())
            .collect()
    }

    #[test]
    fn read_path_matches_load_path() {
        type Case<'a> = (&'a str, Vec<(&'a str, String)>, Vec<&'a str>);
        let cases: Vec<Case> = vec![
            (
                "single file",
                vec![("a.yaml", manifest("only"))],
                vec!["only"],
            ),
            (
                "directory is lexical, not filesystem order",
                vec![
                    ("20-second.yaml", manifest("second")),
                    ("10-first.yaml", manifest("first")),
                    ("30-third.yaml", manifest("third")),
                ],
                vec!["first", "second", "third"],
            ),
            (
                "non-manifest extensions are skipped",
                vec![
                    ("keep.yaml", manifest("keep")),
                    ("keep2.yml", manifest("keep2")),
                    (
                        "keep3.json",
                        r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"keep3"}}"#
                            .into(),
                    ),
                    ("skip.tmpl", manifest("skip")),
                    ("skip.md", "# not a manifest\n".into()),
                    ("skip.yaml.bak", manifest("skip")),
                ],
                vec!["keep", "keep2", "keep3"],
            ),
            (
                "nested directories are not recursed into",
                vec![
                    ("top.yaml", manifest("top")),
                    ("nested/deep.yaml", manifest("deep")),
                ],
                vec!["top"],
            ),
            (
                "a file with no trailing newline does not merge into the next",
                vec![
                    (
                        "a.yaml",
                        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a".into(),
                    ),
                    ("b.yaml", manifest("b")),
                ],
                vec!["a", "b"],
            ),
            (
                "an empty file contributes no document",
                vec![("a.yaml", manifest("a")), ("empty.yaml", String::new())],
                vec!["a"],
            ),
            (
                "multi-document files keep their internal order",
                vec![(
                    "a.yaml",
                    format!("{}---\n{}", manifest("one"), manifest("two")),
                )],
                vec!["one", "two"],
            ),
        ];
        for (name, files, want) in cases {
            let map: BTreeMap<&str, String> = files.into_iter().collect();
            let dir = write_files(&map);
            let path = if map.len() == 1 {
                dir.path().join(map.keys().next().unwrap())
            } else {
                dir.path().to_path_buf()
            };
            let data = read_path(&path).unwrap();
            let got = names(&data);
            assert_eq!(got, want, "{name}: read_path");
            let loaded: Vec<String> = load_path(&path)
                .unwrap()
                .iter()
                .map(|o| o.name().to_string())
                .collect();
            assert_eq!(loaded, got, "{name}: load_path vs read_path");
        }
    }

    #[test]
    fn read_path_missing_is_not_exist() {
        let dir = tempfile::tempdir().unwrap();
        let err = read_path(&dir.path().join("absent.yaml")).unwrap_err();
        assert!(err.is_not_exist(), "{err}");
    }

    #[test]
    fn document_without_kind_is_rejected() {
        assert!(decode_manifest("apiVersion: v1\nmetadata:\n  name: x\n").is_err());
    }
}

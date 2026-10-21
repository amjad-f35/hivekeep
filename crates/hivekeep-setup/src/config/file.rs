//! The configuration document (`--config`): settings held flattened by
//! their dotted keys.

use super::document::DOCUMENT_METADATA_KEY;
use super::errors::ConfigError;
use super::registry::lookup;
use super::yaml_text::{self, Node};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

/// The header every configuration document carries. Versioning it from the
/// start means a later schema change is detected rather than mis-parsed.
pub const FILE_API_VERSION: &str = "install.hivekeep.io/v1alpha1";
pub const FILE_KIND: &str = "HivekeepInstall";

/// Top-level keys that are not settings: the header and the metadata the
/// installer writes into its own records, so a record can be handed straight
/// back to `--config`.
const RESERVED: [&str; 3] = ["apiVersion", "kind", DOCUMENT_METADATA_KEY];

/// A parsed configuration document.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct File {
    /// Where the document was read from, for error messages.
    pub path: String,
    values: BTreeMap<String, String>,
}

impl File {
    /// A document holding exactly these flattened values.
    pub fn from_values(path: &str, values: BTreeMap<String, String>) -> Self {
        Self {
            path: path.to_string(),
            values,
        }
    }

    /// The raw value for a canonical key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    /// Every key the document set, sorted.
    pub fn keys(&self) -> Vec<String> {
        self.values.keys().cloned().collect()
    }
}

/// Reads a configuration document.
///
/// Unknown keys are rejected rather than ignored: a typo in a setting name
/// would otherwise leave the install silently running on the default.
pub fn parse_file(path: &Path) -> Result<File, ConfigError> {
    let shown = path.display().to_string();
    let raw = std::fs::read_to_string(path).map_err(|e| {
        // The absolute path: a relative one means different files from
        // different working directories.
        let abs = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
        ConfigError::Other(format!("reading config {}: {e}", abs.display()))
    })?;
    let doc =
        yaml_text::parse(&raw).map_err(|e| ConfigError::Other(format!("parsing {shown}: {e}")))?;
    let entries = match doc {
        Node::Null => Vec::new(),
        Node::Map(entries) => entries,
        _ => {
            return Err(ConfigError::Other(format!(
                "{shown}: the document must be a mapping"
            )))
        }
    };

    let header = |name: &str| {
        entries
            .iter()
            .find_map(|(k, v)| match v {
                Node::Scalar(s) if k == name => Some(s.clone()),
                _ => None,
            })
            .unwrap_or_default()
    };
    let api_version = header("apiVersion");
    if api_version != FILE_API_VERSION {
        return Err(ConfigError::Other(format!(
            "{shown}: apiVersion must be {FILE_API_VERSION}, got {api_version:?}"
        )));
    }
    let kind = header("kind");
    if kind != FILE_KIND {
        return Err(ConfigError::Other(format!(
            "{shown}: kind must be {FILE_KIND}, got {kind:?}"
        )));
    }

    let mut flat = BTreeMap::new();
    flatten("", &entries, &mut flat).map_err(|e| ConfigError::Other(format!("{shown}: {e}")))?;

    let unknown: Vec<String> = flat
        .keys()
        .filter(|k| lookup(k).is_none())
        .map(|k| format!("{k:?}"))
        .collect();
    if !unknown.is_empty() {
        let plural = if unknown.len() == 1 { "" } else { "s" };
        return Err(ConfigError::Other(format!(
            "{shown}: unknown setting{plural} {}",
            unknown.join(", ")
        )));
    }
    Ok(File {
        path: shown,
        values: flat,
    })
}

/// Walks the document into dotted keys, carrying each scalar's text through
/// unchanged. A key given twice is an error -- repeated inside one mapping,
/// or written dotted beside its nested form -- rather than a silent win for
/// one spelling.
fn flatten(
    prefix: &str,
    entries: &[(String, Node)],
    out: &mut BTreeMap<String, String>,
) -> Result<(), String> {
    let mut seen = HashSet::new();
    for (name, value) in entries {
        if prefix.is_empty() && RESERVED.contains(&name.as_str()) {
            continue;
        }
        let key = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        if !seen.insert(name.as_str()) {
            return Err(format!("setting {key:?} is set twice"));
        }
        let text = match value {
            Node::Map(children) => {
                flatten(&key, children, out)?;
                continue;
            }
            Node::Seq => {
                return Err(format!(
                    "setting {key:?} is a list; no setting takes a list"
                ))
            }
            Node::Other => return Err(format!("setting {key:?} has an unsupported value")),
            Node::Null => String::new(),
            Node::Scalar(s) => s.clone(),
        };
        if out.contains_key(&key) {
            return Err(format!("setting {key:?} is set twice"));
        }
        out.insert(key, text);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::registry::registry;
    use crate::config::testutil::{scratch_setting, write_config, TestEnv, VALID_HEADER};

    #[test]
    fn parse_file_nesting() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let (_d, path) = write_config(&format!(
            "{VALID_HEADER}\nfixture:\n  name: a-name\n  mode: beta\n  enabled: true\n  count: 6\n  nested:\n    deep:\n      key: buried\n"
        ));
        let f = parse_file(&path).unwrap();
        for (key, want) in [
            ("fixture.name", "a-name"),
            ("fixture.mode", "beta"),
            ("fixture.enabled", "true"),
            ("fixture.nested.deep.key", "buried"),
            ("fixture.count", "6"),
        ] {
            assert_eq!(f.get(key), Some(want), "{key}");
        }
    }

    #[test]
    fn parse_file_numbers_are_not_float_formatted() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let (_d, path) = write_config(&format!("{VALID_HEADER}fixture:\n  count: 1000000\n"));
        assert_eq!(
            parse_file(&path).unwrap().get("fixture.count"),
            Some("1000000")
        );
    }

    #[test]
    fn parse_file_keeps_the_text_of_a_string_setting() {
        let mut t = TestEnv::new();
        t.use_settings(vec![scratch_setting()]);
        for (name, written, want) in [
            ("trailing zero", "0.10", "0.10"),
            ("leading zero", "0123", "0123"),
            ("yes", "yes", "yes"),
            (
                "leading zero on a long number",
                "012345678901",
                "012345678901",
            ),
            (
                "beyond float precision",
                "12345678901234567890",
                "12345678901234567890",
            ),
            ("quoted", "\"0.10\"", "0.10"),
        ] {
            let (_d, path) = write_config(&format!("{VALID_HEADER}scratch:\n  str: {written}\n"));
            assert_eq!(
                parse_file(&path).unwrap().get("scratch.str"),
                Some(want),
                "{name}"
            );
        }
    }

    #[test]
    fn parse_file_rejects_duplicate_keys() {
        let mut t = TestEnv::new();
        t.use_settings(vec![scratch_setting()]);
        for (name, body) in [
            ("repeated in one map", "scratch:\n  str: a\n  str: b\n"),
            (
                "dotted beside nested",
                "scratch.str: a\nscratch:\n  str: b\n",
            ),
        ] {
            let (_d, path) = write_config(&format!("{VALID_HEADER}{body}"));
            let err = parse_file(&path).expect_err(name).to_string();
            assert!(err.contains("scratch.str"), "{name}: {err}");
        }
    }

    #[test]
    fn parse_file_rejects_unknown_keys() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        for (name, body, want_in) in [
            (
                "top level",
                "nosuchsetting: x\n",
                vec!["unknown setting", "nosuchsetting"],
            ),
            (
                "nested near miss",
                "fixture:\n  mod: beta\n",
                vec!["unknown setting", "fixture.mod"],
            ),
            (
                "several are all named",
                "aaa: 1\nzzz: 2\n",
                vec!["unknown settings", "aaa", "zzz"],
            ),
        ] {
            let (_d, path) = write_config(&format!("{VALID_HEADER}{body}"));
            let err = parse_file(&path).expect_err(name).to_string();
            for w in want_in {
                assert!(err.contains(w), "{name}: {err} lacks {w}");
            }
        }
    }

    #[test]
    fn parse_file_header() {
        for (name, body, want_in) in [
            (
                "wrong apiVersion",
                format!("apiVersion: v1\nkind: {FILE_KIND}\n"),
                "apiVersion",
            ),
            (
                "missing apiVersion",
                format!("kind: {FILE_KIND}\n"),
                "apiVersion",
            ),
            (
                "wrong kind",
                format!("apiVersion: {FILE_API_VERSION}\nkind: Other\n"),
                "kind",
            ),
            (
                "missing kind",
                format!("apiVersion: {FILE_API_VERSION}\n"),
                "kind",
            ),
        ] {
            let (_d, path) = write_config(&body);
            let err = parse_file(&path).expect_err(name).to_string();
            assert!(err.contains(want_in), "{name}: {err}");
        }
    }

    #[test]
    fn parse_file_rejects_lists() {
        let (_d, path) = write_config(&format!("{VALID_HEADER}namespace:\n  - a\n  - b\n"));
        assert!(parse_file(&path).unwrap_err().to_string().contains("list"));
    }

    #[test]
    fn parse_file_missing() {
        let d = tempfile::tempdir().unwrap();
        assert!(parse_file(&d.path().join("absent.yaml")).is_err());
    }

    #[test]
    fn parse_file_header_only() {
        let (_d, path) = write_config(VALID_HEADER);
        assert!(parse_file(&path).unwrap().keys().is_empty());
    }

    #[test]
    fn parse_file_null_is_empty_not_absent() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let (_d, path) = write_config(&format!("{VALID_HEADER}fixture:\n  name:\n"));
        assert_eq!(parse_file(&path).unwrap().get("fixture.name"), Some(""));
    }

    #[test]
    fn every_registry_key_is_accepted_by_the_parser() {
        let mut body = VALID_HEADER.to_string();
        for s in registry() {
            body.push_str(&format!("\"{}\": \"\"\n", s.key));
        }
        let (_d, path) = write_config(&body);
        let f = parse_file(&path).unwrap();
        for s in registry() {
            assert!(f.get(&s.key).is_some(), "{}", s.key);
        }
    }
}

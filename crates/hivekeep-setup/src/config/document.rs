//! A resolved configuration as a file: the record of a run, which is also
//! valid `--config` input for the next one.

use super::errors::ConfigError;
use super::file::{FILE_API_VERSION, FILE_KIND};
use super::resolve::{Resolved, Value};
use super::setting::ValueKind;
use serde::Serialize;
use serde_yaml::{Mapping, Value as Yaml};
use std::collections::BTreeMap;

/// The document's one reserved top-level key, skipped when the document is
/// read back as configuration.
pub const DOCUMENT_METADATA_KEY: &str = "cluster";

/// How the run that produced a document ended.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    #[default]
    Succeeded,
    Failed,
}

/// Which run produced a document. Written by the installer, ignored when the
/// document is read back.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentMetadata {
    /// The cluster the run targeted; it also names the file.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub context: String,
    pub outcome: Outcome,
    /// The command that failed, for a failed run.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub failed_at: String,
    /// The Hivekeep release the run installed.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub hivekeep_version: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub written_at: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub written_by: String,
}

/// A resolved configuration ready to write.
#[derive(Clone, Debug)]
pub struct Document {
    pub metadata: DocumentMetadata,
    values: BTreeMap<String, Value>,
}

impl Document {
    /// Records the settings a channel supplied.
    ///
    /// Settings on their declared default are omitted, so they keep tracking
    /// it when a later release changes it. Secrets are omitted whatever
    /// supplied them, and their keys are returned so the caller can say which
    /// a retry must supply again.
    pub fn new(r: &Resolved, mut metadata: DocumentMetadata) -> (Self, Vec<String>) {
        metadata.written_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        metadata.written_by = format!("hivekeep-setup v{}", env!("CARGO_PKG_VERSION"));
        let mut values = BTreeMap::new();
        let mut omitted = Vec::new();
        for v in r.origins() {
            if v.setting.secret {
                omitted.push(v.setting.key.clone());
                continue;
            }
            values.insert(v.setting.key.clone(), v);
        }
        omitted.sort();
        (Self { metadata, values }, omitted)
    }

    /// The settings recorded, sorted.
    pub fn keys(&self) -> Vec<String> {
        self.values.keys().cloned().collect()
    }

    /// Renders the document with the dotted keys nested, so the result is
    /// ordinary `--config` input. Scalars are written in their declared kind:
    /// a count reads as 7, not "7".
    pub fn marshal(&self) -> Result<String, ConfigError> {
        let meta = serde_yaml::to_value(&self.metadata)
            .map_err(|e| ConfigError::Other(format!("while encoding the record: {e}")))?;
        let mut tree = Tree::default();
        for (key, v) in &self.values {
            tree.insert(&key.split('.').collect::<Vec<_>>(), scalar(v));
        }
        let mut doc = Mapping::new();
        doc.insert("apiVersion".into(), FILE_API_VERSION.into());
        doc.insert("kind".into(), FILE_KIND.into());
        doc.insert(DOCUMENT_METADATA_KEY.into(), meta);
        if let Yaml::Mapping(settings) = tree.into_yaml() {
            for (k, v) in settings {
                doc.insert(k, v);
            }
        }
        serde_yaml::to_string(&Yaml::Mapping(doc))
            .map_err(|e| ConfigError::Other(format!("while encoding the record: {e}")))
    }
}

/// A value in its declared kind, falling back to the raw text when it does
/// not parse (only possible for a document assembled by hand).
fn scalar(v: &Value) -> Yaml {
    match v.setting.kind {
        ValueKind::Bool => match v.raw.as_str() {
            "true" | "1" => Yaml::Bool(true),
            "false" | "0" => Yaml::Bool(false),
            _ => Yaml::String(v.raw.clone()),
        },
        ValueKind::Int => match v.raw.parse::<i64>() {
            Ok(n) => Yaml::Number(n.into()),
            Err(_) => Yaml::String(v.raw.clone()),
        },
        _ => Yaml::String(v.raw.clone()),
    }
}

/// Nested keys, sorted. A key cannot both hold a value and have children
/// (the registry has no such pair), so on a collision the leaf wins.
#[derive(Default)]
struct Tree {
    leaf: Option<Yaml>,
    children: BTreeMap<String, Tree>,
}

impl Tree {
    fn insert(&mut self, path: &[&str], value: Yaml) {
        match path.split_first() {
            None => self.leaf = Some(value),
            Some((first, rest)) => self
                .children
                .entry(first.to_string())
                .or_default()
                .insert(rest, value),
        }
    }

    fn into_yaml(self) -> Yaml {
        if let Some(leaf) = self.leaf {
            return leaf;
        }
        let mut m = Mapping::new();
        for (k, child) in self.children {
            m.insert(Yaml::String(k), child.into_yaml());
        }
        Yaml::Mapping(m)
    }
}

//! Which hive may resolve Secrets in which Kubernetes namespace.
//!
//! The policy is a YAML file of grants, each naming one hive, the
//! namespaces it may read and optionally a label selector narrowing the
//! grant to matching Secrets. Enforcement is default-deny: a hive absent
//! from the file resolves nothing.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use hivekeep_core::names::{check_label_key, check_label_value};
use serde::Deserialize;

/// Why a policy could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    /// The file could not be read.
    #[error("reading namespace policy file {path:?}: {source}")]
    Read {
        /// The file.
        path: String,
        /// The I/O error.
        source: std::io::Error,
    },
    /// The file is not a valid policy document.
    #[error("parsing namespace policy file {path:?}: {source}")]
    Parse {
        /// The file.
        path: String,
        /// The YAML error.
        source: serde_yaml::Error,
    },
    /// A grant is semantically invalid.
    #[error("namespace policy {index}: {reason}")]
    Invalid {
        /// Position of the grant in the file.
        index: usize,
        /// What is wrong with it.
        reason: String,
    },
}

/// The policy file: a list of grants.
///
/// Unknown fields are rejected so that a typo such as `matchLabel`, or an
/// unsupported `matchExpressions`, fails loading instead of parsing as an
/// empty selector that would grant every Secret in the namespace.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyFile {
    /// The grants.
    #[serde(default)]
    pub policies: Vec<HivePolicy>,
}

/// One grant: the namespaces one hive may read from.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HivePolicy {
    /// The hive granted access.
    #[serde(default)]
    pub hive: String,
    /// Namespaces whose Secrets it may resolve.
    #[serde(default)]
    pub allowed_namespaces: Vec<String>,
    /// Narrows the grant to Secrets carrying all of these labels. Absent or
    /// empty means every Secret in the allowed namespaces, like an empty
    /// Kubernetes label selector. There is no grant that admits nothing;
    /// leave the policy out instead.
    #[serde(default)]
    pub secret_selector: Option<SecretSelector>,
}

/// Selects Secrets by exact label equality, the shape of the selectors
/// used elsewhere in Hivekeep. Mirrored rather than shared: this is a config
/// file, not the API.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SecretSelector {
    /// Labels a Secret must carry.
    #[serde(default)]
    pub match_labels: BTreeMap<String, String>,
}

/// What one hive may read in one namespace: Secrets carrying all of
/// `labels` (every Secret when empty).
#[derive(Debug, Clone, Default)]
struct Grant {
    labels: BTreeMap<String, String>,
}

impl Grant {
    fn admits(&self, labels: Option<&BTreeMap<String, String>>) -> bool {
        self.labels
            .iter()
            .all(|(k, want)| labels.and_then(|l| l.get(k)) == Some(want))
    }
}

/// Decides whether a hive may resolve Secrets in a namespace.
#[derive(Debug, Clone, Default)]
pub struct NamespaceAuthorizer {
    /// hive → namespace → the grants applying there. Grants are OR-ed (a
    /// Secret is admitted if any admits it); the labels within one grant are
    /// AND-ed.
    allowed: HashMap<String, HashMap<String, Vec<Grant>>>,
}

impl NamespaceAuthorizer {
    /// Reads and validates the policy at `path`, so a malformed file fails
    /// startup rather than the first request.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, PolicyError> {
        let path = path.as_ref();
        let data = std::fs::read(path).map_err(|source| PolicyError::Read {
            path: path.display().to_string(),
            source,
        })?;
        let file: PolicyFile =
            serde_yaml::from_slice(&data).map_err(|source| PolicyError::Parse {
                path: path.display().to_string(),
                source,
            })?;
        Self::new(file)
    }

    /// Builds an authorizer over a parsed policy, validating each grant.
    pub fn new(file: PolicyFile) -> Result<Self, PolicyError> {
        let mut allowed: HashMap<String, HashMap<String, Vec<Grant>>> = HashMap::new();
        for (index, p) in file.policies.into_iter().enumerate() {
            let invalid = |reason: String| PolicyError::Invalid { index, reason };
            if p.hive.is_empty() {
                return Err(invalid("hive is required".into()));
            }
            let labels = p
                .secret_selector
                .map(|s| s.match_labels)
                .unwrap_or_default();
            // A malformed label can never match, so it would narrow the grant
            // to nothing and look like the policy was ignored.
            for (k, v) in &labels {
                if !check_label_key(k).is_empty() {
                    return Err(invalid(format!("invalid label key {k:?}")));
                }
                if !check_label_value(v).is_empty() {
                    return Err(invalid(format!("invalid label value {v:?} for key {k:?}")));
                }
            }
            // Each policy keeps its own grant: merging would AND the
            // selectors of separate policies, and let a narrow policy take
            // away a namespace a broad one granted.
            let grant = Grant { labels };
            let set = allowed.entry(p.hive).or_default();
            for ns in p.allowed_namespaces {
                set.entry(ns).or_default().push(grant.clone());
            }
        }
        Ok(NamespaceAuthorizer { allowed })
    }

    /// The loaded policy as hive → sorted namespaces.
    pub fn grants(&self) -> BTreeMap<String, Vec<String>> {
        self.allowed
            .iter()
            .map(|(hive, namespaces)| {
                let mut list: Vec<String> = namespaces.keys().cloned().collect();
                list.sort();
                (hive.clone(), list)
            })
            .collect()
    }

    /// Whether `hive` may resolve any Secret in `namespace`.
    pub fn allowed(&self, hive: &str, namespace: &str) -> bool {
        self.allowed
            .get(hive)
            .and_then(|set| set.get(namespace))
            .is_some_and(|grants| !grants.is_empty())
    }

    /// Whether `hive` may resolve a Secret carrying `labels` in
    /// `namespace`. A grant without a selector admits every Secret, even one
    /// that does not exist: pass `None` to ask before reading anything.
    pub fn allowed_secret(
        &self,
        hive: &str,
        namespace: &str,
        labels: Option<&BTreeMap<String, String>>,
    ) -> bool {
        self.allowed
            .get(hive)
            .and_then(|set| set.get(namespace))
            .is_some_and(|grants| grants.iter().any(|g| g.admits(labels)))
    }
}

#[cfg(test)]
#[path = "nsauthz_tests.rs"]
mod tests;

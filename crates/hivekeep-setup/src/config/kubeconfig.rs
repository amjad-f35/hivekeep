//! Just enough kubeconfig reading to name the cluster a run targets: the
//! current context and the contexts defined, merged across files the way
//! kubectl merges them (the first file to set a value wins).

use std::collections::BTreeSet;
use std::path::PathBuf;

/// What the merged kubeconfig says about contexts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KubeconfigSummary {
    pub current_context: String,
    pub contexts: BTreeSet<String>,
}

/// Why the kubeconfig could not be read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KubeconfigError {
    /// An explicitly named file does not exist.
    Missing(String),
    /// A file exists but is not a kubeconfig.
    Invalid(String),
}

fn default_paths() -> Vec<PathBuf> {
    if let Ok(list) = std::env::var("KUBECONFIG") {
        let paths: Vec<PathBuf> = std::env::split_paths(&list)
            .filter(|p| !p.as_os_str().is_empty())
            .collect();
        if !paths.is_empty() {
            return paths;
        }
    }
    match std::env::var_os("HOME") {
        Some(home) => vec![PathBuf::from(home).join(".kube").join("config")],
        None => vec![],
    }
}

/// Loads the kubeconfig `kubeconfig` names: one file is an explicit path
/// that must exist; a PATH-style list is a precedence chain in which missing
/// files are skipped; empty uses `$KUBECONFIG`, then `~/.kube/config`.
pub fn load(kubeconfig: &str) -> Result<KubeconfigSummary, KubeconfigError> {
    let (paths, explicit) = if kubeconfig.is_empty() {
        (default_paths(), false)
    } else {
        let paths: Vec<PathBuf> = std::env::split_paths(kubeconfig).collect();
        let explicit = paths.len() == 1;
        (paths, explicit)
    };
    let mut out = KubeconfigSummary::default();
    let mut found = false;
    for path in paths {
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(_) if explicit => return Err(KubeconfigError::Missing(path.display().to_string())),
            Err(_) => continue,
        };
        found = true;
        let doc: serde_yaml::Value = serde_yaml::from_str(&text)
            .map_err(|e| KubeconfigError::Invalid(format!("{}: {e}", path.display())))?;
        if out.current_context.is_empty() {
            if let Some(c) = doc.get("current-context").and_then(|c| c.as_str()) {
                out.current_context = c.to_string();
            }
        }
        if let Some(list) = doc.get("contexts").and_then(|c| c.as_sequence()) {
            for c in list {
                if let Some(name) = c.get("name").and_then(|n| n.as_str()) {
                    out.contexts.insert(name.to_string());
                }
            }
        }
    }
    if !found {
        return Err(KubeconfigError::Missing(kubeconfig.to_string()));
    }
    Ok(out)
}

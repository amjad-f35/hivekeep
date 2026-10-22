//! Runs the lint rules against the Hivekeep API and compares the findings to the
//! checked-in exemptions file. Shared by the `validate` command and its
//! presubmit test.

use std::path::{Path, PathBuf};

use super::exemption::Exemption;
use super::lint::{self, Finding, LintError, Rule};
use super::model::{self, Api, ModelError};

/// The public API definition, embedded at build time.
pub const API_SOURCE: &str =
    include_str!("../../../hivekeep-proto/proto/hivekeep/api/v1/api.proto");

/// Display name of the validated file, for output.
pub const API_FILE_NAME: &str = "api.proto";

/// The RPC service validated: the public, documented API surface.
pub const SERVICE_NAME: &str = "Control";

/// Where the exemptions file lives, relative to the workspace root.
pub const EXEMPTIONS_RELATIVE_PATH: &str = "crates/hivekeep-devtools/exemptions.json";

/// Builds the API model from `source`, scoped to [`SERVICE_NAME`].
pub fn api_from_source(source: &str) -> Result<Api, ModelError> {
    let api = model::build(source).map_err(|e| match e {
        ModelError::Compile(m) => ModelError::Compile(format!("while building API model: {m}")),
        other => other,
    })?;
    Ok(model::scope_to_service(&api, SERVICE_NAME))
}

/// Builds the embedded API model, scoped to [`SERVICE_NAME`].
pub fn api() -> Result<Api, ModelError> {
    api_from_source(API_SOURCE)
}

/// One rule with the findings it raised, in [`lint::ALL`] order.
#[derive(Debug)]
pub struct RuleFindings {
    pub rule: Rule,
    pub findings: Vec<Finding>,
}

/// Errors evaluating the rules.
#[derive(Debug, thiserror::Error)]
#[error("rule {rule}: {source}")]
pub struct RuleError {
    pub rule: &'static str,
    #[source]
    pub source: LintError,
}

/// Runs every rule in [`lint::ALL`] against `api`.
pub fn all(api: &Api) -> Result<Vec<RuleFindings>, RuleError> {
    lint::ALL
        .iter()
        .map(|rule| {
            (rule.check)(api)
                .map(|findings| RuleFindings {
                    rule: *rule,
                    findings,
                })
                .map_err(|source| RuleError {
                    rule: rule.name,
                    source,
                })
        })
        .collect()
}

/// Flattens results into the exemption identity of each finding.
pub fn as_exemptions(results: &[RuleFindings]) -> Vec<Exemption> {
    results
        .iter()
        .flat_map(|r| {
            r.findings
                .iter()
                .map(|f| Exemption::new(r.rule.name, &f.subject, &f.message))
        })
        .collect()
}

/// The workspace root could not be located.
#[derive(Debug, thiserror::Error)]
pub enum RootError {
    #[error("while getting working directory: {0}")]
    Cwd(std::io::Error),
    #[error("could not find a Cargo.toml declaring a [workspace] above {0}")]
    NotFound(String),
}

/// The exemptions file's default location, resolved from the workspace root so
/// it is found regardless of the working directory.
pub fn default_exemptions_path() -> Result<PathBuf, RootError> {
    Ok(repo_root()?.join(EXEMPTIONS_RELATIVE_PATH))
}

/// Walks up from the working directory to the workspace root.
pub fn repo_root() -> Result<PathBuf, RootError> {
    let start = std::env::current_dir().map_err(RootError::Cwd)?;
    repo_root_from(&start)
}

/// Walks up from `start` to the first directory whose `Cargo.toml` declares a
/// `[workspace]`.
pub fn repo_root_from(start: &Path) -> Result<PathBuf, RootError> {
    let mut dir = Some(start);
    while let Some(d) = dir {
        if let Ok(manifest) = std::fs::read_to_string(d.join("Cargo.toml")) {
            if manifest.lines().any(|l| l.trim() == "[workspace]") {
                return Ok(d.to_path_buf());
            }
        }
        dir = d.parent();
    }
    Err(RootError::NotFound(start.display().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apitool::exemption;

    /// The workspace root, computed from this crate's known location rather
    /// than from the working directory.
    fn want_repo_root() -> PathBuf {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap();
        assert!(
            root.join("Cargo.toml").exists(),
            "{} has no Cargo.toml",
            root.display()
        );
        root
    }

    #[test]
    fn find_repo_root() {
        let want = want_repo_root();
        // From the workspace root itself.
        assert_eq!(repo_root_from(&want).unwrap(), want);
        // From this crate's directory, the usual invocation point.
        let crate_dir = want.join("crates/hivekeep-devtools");
        assert_eq!(repo_root_from(&crate_dir).unwrap(), want);
        // Outside the workspace entirely.
        let outside = tempfile::tempdir().unwrap();
        assert!(repo_root_from(outside.path()).is_err());
    }

    /// Presubmit gate: the exemptions file matches every current finding exactly.
    #[test]
    fn exemptions() {
        let api = api().expect("building the API model");
        let results = all(&api).expect("running the rules");
        let current = as_exemptions(&results);

        let path = want_repo_root().join(EXEMPTIONS_RELATIVE_PATH);
        let want = exemption::load(&path).expect("loading exemptions");
        exemption::check_sorted(&want).unwrap_or_else(|e| {
            panic!(
                "{}: {e}; run `hivekeep-apitool validate --update` to restore canonical order",
                path.display()
            )
        });

        let (missing, stale) = exemption::diff(&current, &want);
        let mut problems = Vec::new();
        for f in &missing {
            problems.push(format!(
                "finding has no exemption: [{}] {}: {}",
                f.rule, f.subject, f.message
            ));
        }
        for e in &stale {
            problems.push(format!(
                "exemption no longer matches any finding: [{}] {}: {}",
                e.rule, e.subject, e.message
            ));
        }
        assert!(
            problems.is_empty(),
            "{} out of date (run `hivekeep-apitool validate --update` to regenerate):\n{}",
            path.display(),
            problems.join("\n")
        );
    }

    #[test]
    fn api_is_scoped_to_control() {
        let api = api().unwrap();
        assert_eq!(api.services.len(), 1);
        assert_eq!(api.services[0].name, SERVICE_NAME);
        assert!(api
            .messages
            .iter()
            .all(|m| !m.full_name.ends_with("WorkerEpochRequest")));
    }
}

//! Writing what a run used, so the next one can reproduce it by pointing
//! `--config` at the file.

use super::annotations::records_run;
use crate::config::{record_dir, record_failure, record_success, Resolved};
use crate::log;
use std::path::PathBuf;

/// Records a run of the command at `path` (without the program name).
///
/// Best effort: an install that worked must not be reported as failed
/// because a directory was read-only. Nothing is recorded before the
/// configuration was resolved, or for a command that does not record.
/// Returns the file written.
pub fn record_run(
    path: &str,
    resolved: Option<&Resolved>,
    version: &str,
    failed: bool,
) -> Option<PathBuf> {
    let r = resolved?;
    if !records_run(path) {
        return None;
    }
    let dir = match record_dir(&r.string("record.dir")) {
        Ok(d) => d,
        Err(e) => {
            log::warn(&format!("not recording this run: {e}"));
            return None;
        }
    };
    let context = r.cluster_key();
    let command = format!("hivekeep-setup {path}");
    let written = if failed {
        record_failure(&dir, &context, version, r, &command)
    } else {
        record_success(&dir, &context, version, r)
    };
    let (file, omitted) = match written {
        Ok(v) => v,
        Err(e) => {
            log::warn(&format!("not recording this run: {e}"));
            return None;
        }
    };
    if !failed {
        log::info(&format!(
            "recorded this configuration in {}",
            file.display()
        ));
        return Some(file);
    }
    log::info(&format!(
        "the settings this run used were written to {}",
        file.display()
    ));
    log::info(&format!(
        "retry with: {command} --config {}",
        file.display()
    ));
    if !omitted.is_empty() {
        log::info(&format!(
            "{} secret setting(s) were not written and must be supplied again: {}",
            omitted.len(),
            omitted.join(", ")
        ));
    }
    Some(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::testutil::TestEnv;
    use crate::config::{parse_file, resolve, Config, ResolveOptions};
    use crate::steps::Env;
    use std::collections::BTreeMap;
    use std::path::Path;

    fn resolved(vars: &[(&str, &str)]) -> Resolved {
        super::super::register_settings();
        let env: BTreeMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        resolve(
            None,
            ResolveOptions {
                file: None,
                env: Some(&env),
            },
        )
        .unwrap()
    }

    fn installs(dir: &Path) -> Vec<PathBuf> {
        let mut out: Vec<PathBuf> = std::fs::read_dir(dir.join("installs"))
            .map(|rd| rd.filter_map(|e| e.ok()).map(|e| e.path()).collect())
            .unwrap_or_default();
        out.sort();
        out
    }

    /// A record of `deploy gate` against `dir`, as the run hook writes it.
    fn record_for(dir: &Path, vars: &[(&str, &str)]) -> PathBuf {
        let mut all = vec![("HK_RECORD_DIR", dir.to_str().unwrap())];
        all.extend_from_slice(vars);
        let r = resolved(&all);
        record_run("deploy gate", Some(&r), "", false).expect("a record")
    }

    #[test]
    fn no_record_without_resolved_configuration() {
        assert!(record_run("deploy gate", None, "", false).is_none());
    }

    #[test]
    fn record_run_writes_a_parseable_file() {
        let dir = tempfile::tempdir().unwrap();
        let r = resolved(&[
            ("HK_RECORD_DIR", dir.path().to_str().unwrap()),
            ("KUBECTL_CONTEXT", "prod"),
            ("HK_GATE_DATAPLANE", "envoy"),
        ]);
        record_run("deploy gate", Some(&r), "", false);
        let path = dir.path().join("installs/prod.yaml");
        assert!(path.exists(), "no record at {}", path.display());
        let f = parse_file(&path).expect("the record is valid --config input");
        assert_eq!(f.get("context"), Some("prod"));
        assert_eq!(f.get("gate.dataplane"), Some("envoy"));
    }

    #[test]
    fn record_run_writes_a_failure_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let r = resolved(&[
            ("HK_RECORD_DIR", dir.path().to_str().unwrap()),
            ("KUBECTL_CONTEXT", "prod"),
        ]);
        record_run("deploy gate", Some(&r), "", true);
        let failed: Vec<PathBuf> = std::fs::read_dir(dir.path().join("failed"))
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("prod-")
            })
            .collect();
        assert_eq!(failed.len(), 1, "{failed:?}");
        parse_file(&failed[0]).expect("the failure artifact is valid --config input");
        assert!(
            installs(dir.path()).is_empty(),
            "a failed run wrote an install record"
        );
    }

    #[test]
    fn record_names_the_installed_version() {
        let mut t = TestEnv::new();
        t.set_env("HK_VERSION", "v0.9.1");
        let env = Env::local(Config::default());
        let version = env.hivekeep_version();
        let dir = tempfile::tempdir().unwrap();
        let r = resolved(&[("HK_RECORD_DIR", dir.path().to_str().unwrap())]);
        let path = record_run("deploy gate", Some(&r), &version, false).unwrap();
        let doc: serde_yaml::Value =
            serde_yaml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(doc["cluster"]["hivekeepVersion"].as_str(), Some("v0.9.1"));
    }

    /// A Kind install resolves to the kind-kind context without anyone
    /// passing --context, so its record must not be the unkeyed one.
    #[test]
    fn record_is_named_for_the_cluster_not_the_flag() {
        let dir = tempfile::tempdir().unwrap();
        let path = record_for(dir.path(), &[("HK_INSTALL_KIND", "true")]);
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        assert!(!name.starts_with("unkeyed"), "a Kind install wrote {name}");
    }

    #[test]
    fn two_clusters_do_not_share_a_record() {
        let dir = tempfile::tempdir().unwrap();
        let first = record_for(dir.path(), &[("HK_INSTALL_KIND", "true")]);
        let second = record_for(dir.path(), &[("KUBECONFIG", "/nonexistent/other")]);
        assert_ne!(first, second);
        assert_eq!(installs(dir.path()).len(), 2, "{:?}", installs(dir.path()));
    }
}

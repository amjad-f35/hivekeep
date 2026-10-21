//! Install records: what each run used, written so the next run can
//! reproduce it by pointing `--config` at the file.
//!
//! A success replaces the cluster's record, since it is that cluster's
//! current configuration; a failure is one attempt among several, so failures
//! accumulate under their own names.

use super::document::{Document, DocumentMetadata, Outcome};
use super::errors::ConfigError;
use super::kubeconfig;
use super::registry::KIND_ENABLED_KEY;
use super::resolve::Resolved;
use std::io::Write;
use std::path::{Path, PathBuf};

const INSTALLS_DIR: &str = "installs";
const FAILED_DIR: &str = "failed";
/// Names the record of a run whose cluster could not be determined.
pub const UNKEYED_CONTEXT: &str = "unkeyed";

/// The user cache directory: `$XDG_CACHE_HOME`, else `~/Library/Caches` on
/// macOS and `~/.cache` elsewhere.
pub fn user_cache_dir() -> Result<PathBuf, ConfigError> {
    if let Some(xdg) = std::env::var_os("XDG_CACHE_HOME").filter(|v| !v.is_empty()) {
        let p = PathBuf::from(xdg);
        if p.is_absolute() {
            return Ok(p);
        }
    }
    let home = std::env::var_os("HOME")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| ConfigError::Other("neither $XDG_CACHE_HOME nor $HOME is defined".into()))?;
    let home = PathBuf::from(home);
    Ok(if cfg!(target_os = "macos") {
        home.join("Library/Caches")
    } else {
        home.join(".cache")
    })
}

/// Where records go: the configured directory, else a directory of the
/// installer's own under the user cache directory.
pub fn record_dir(configured: &str) -> Result<PathBuf, ConfigError> {
    if !configured.is_empty() {
        return Ok(PathBuf::from(configured));
    }
    let base = user_cache_dir().map_err(|e| {
        ConfigError::Other(format!(
            "while resolving the cache directory for install records: {e}"
        ))
    })?;
    Ok(base.join("hivekeep-setup"))
}

/// Writes the record of a completed run, replacing the cluster's previous
/// one. Returns its path and the secret settings left out.
pub fn record_success(
    dir: &Path,
    context: &str,
    hivekeep_version: &str,
    r: &Resolved,
) -> Result<(PathBuf, Vec<String>), ConfigError> {
    let (d, omitted) = Document::new(
        r,
        DocumentMetadata {
            context: context.into(),
            outcome: Outcome::Succeeded,
            hivekeep_version: hivekeep_version.into(),
            ..Default::default()
        },
    );
    let path = dir
        .join(INSTALLS_DIR)
        .join(format!("{}.yaml", record_name(context)));
    write_replacing(&path, &d)?;
    Ok((path, omitted))
}

/// Writes the record of a failed run as a new file, so earlier attempts
/// survive.
pub fn record_failure(
    dir: &Path,
    context: &str,
    hivekeep_version: &str,
    r: &Resolved,
    failed_at: &str,
) -> Result<(PathBuf, Vec<String>), ConfigError> {
    let (d, omitted) = Document::new(
        r,
        DocumentMetadata {
            context: context.into(),
            outcome: Outcome::Failed,
            failed_at: failed_at.into(),
            hivekeep_version: hivekeep_version.into(),
            ..Default::default()
        },
    );
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    let base = dir
        .join(FAILED_DIR)
        .join(format!("{}-{stamp}", record_name(context)));
    let path = write_new(&base, &d)?;
    Ok((path, omitted))
}

impl Resolved {
    /// The cluster this run targets, which names its record: the context
    /// setting, the Kind cluster's context, or the kubeconfig's current
    /// context. Empty when none can be determined.
    pub fn cluster_key(&self) -> String {
        let ctx = self.string("context");
        if !ctx.is_empty() {
            return ctx;
        }
        if self.bool(KIND_ENABLED_KEY) {
            return format!("kind-{}", self.string("kindCluster.name"));
        }
        kubeconfig::load(&self.string("kubeconfig"))
            .map(|k| k.current_context)
            .unwrap_or_default()
    }
}

/// A context name made safe as a file name.
pub fn record_name(context: &str) -> String {
    if context.is_empty() {
        return UNKEYED_CONTEXT.into();
    }
    context
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn io_err(e: std::io::Error, what: &Path) -> ConfigError {
    ConfigError::Other(format!("while writing {}: {e}", what.display()))
}

#[cfg(unix)]
fn private_options() -> std::fs::OpenOptions {
    use std::os::unix::fs::OpenOptionsExt;
    let mut o = std::fs::OpenOptions::new();
    o.write(true).mode(0o600);
    o
}

#[cfg(not(unix))]
fn private_options() -> std::fs::OpenOptions {
    let mut o = std::fs::OpenOptions::new();
    o.write(true);
    o
}

/// Creates `<base>.yaml`, or `<base>-N.yaml` for the first N free.
fn write_new(base: &Path, d: &Document) -> Result<PathBuf, ConfigError> {
    let raw = d.marshal()?;
    let parent = base.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent).map_err(|e| io_err(e, parent))?;
    for n in 1.. {
        let path = if n == 1 {
            PathBuf::from(format!("{}.yaml", base.display()))
        } else {
            PathBuf::from(format!("{}-{n}.yaml", base.display()))
        };
        match private_options().create_new(true).open(&path) {
            Ok(mut f) => {
                f.write_all(raw.as_bytes()).map_err(|e| io_err(e, &path))?;
                return Ok(path);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(io_err(e, &path)),
        }
    }
    unreachable!("the loop only ends by returning")
}

/// Replaces `path` atomically through a private temporary file in the same
/// directory.
fn write_replacing(path: &Path, d: &Document) -> Result<(), ConfigError> {
    let raw = d.marshal()?;
    let parent = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent).map_err(|e| io_err(e, parent))?;
    let mut tmp = tempfile::Builder::new()
        .prefix(".record-")
        .tempfile_in(parent)
        .map_err(|e| io_err(e, parent))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o600))
            .map_err(|e| io_err(e, path))?;
    }
    tmp.write_all(raw.as_bytes()).map_err(|e| io_err(e, path))?;
    tmp.persist(path).map_err(|e| io_err(e.error, path))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::all;
    use crate::config::file::parse_file;
    use crate::config::resolve::{resolve, Flags, ResolveOptions};
    use crate::config::setting::Setting;
    use crate::config::testutil::{env_map, TestEnv};
    use std::collections::BTreeMap;

    fn resolved_with(t: &mut TestEnv, env: &[(&str, &str)]) -> Resolved {
        t.use_fixtures();
        let env = env_map(env);
        resolve(
            None,
            ResolveOptions {
                env: Some(&env),
                file: None,
            },
        )
        .unwrap()
    }

    fn scratch_with_default() -> Setting {
        Setting::new("scratch.str", "SCRATCH_STR", "scratch-str")
            .default_value("alpha")
            .usage("a string")
    }

    fn count_files(dir: &Path) -> usize {
        std::fs::read_dir(dir).unwrap().count()
    }

    #[test]
    fn record_round_trips_through_config() {
        let mut t = TestEnv::new();
        let r = resolved_with(
            &mut t,
            &[
                ("FIXTURE_MODE", "beta"),
                ("FIXTURE_NAME", "a-name"),
                ("FIXTURE_COUNT", "6"),
                ("FIXTURE_NESTED_DEEP_KEY", "buried"),
            ],
        );
        let dir = tempfile::tempdir().unwrap();
        let (path, _) = record_success(dir.path(), "prod", "", &r).unwrap();
        let f = parse_file(&path).unwrap();
        for (key, want) in [
            ("fixture.mode", "beta"),
            ("fixture.name", "a-name"),
            ("fixture.count", "6"),
            ("fixture.nested.deep.key", "buried"),
        ] {
            assert_eq!(f.get(key), Some(want), "{key}");
        }
    }

    #[test]
    fn record_round_trips_an_empty_flag() {
        let mut t = TestEnv::new();
        t.use_settings(vec![scratch_with_default()]);
        let mut flags = Flags::default();
        flags.set("scratch-str", "");
        let r = resolve(Some(&flags), ResolveOptions::default()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (path, _) = record_success(dir.path(), "prod", "", &r).unwrap();
        let f = parse_file(&path).unwrap();
        let back = resolve(
            None,
            ResolveOptions {
                file: Some(&f),
                env: None,
            },
        )
        .unwrap();
        assert_eq!(back.string("scratch.str"), r.string("scratch.str"));
        assert_eq!(back.supplied("scratch.str"), r.supplied("scratch.str"));
    }

    #[test]
    fn record_can_clear_a_string_that_has_a_default() {
        let mut t = TestEnv::new();
        t.use_settings(vec![scratch_with_default()]);
        let env = env_map(&[("SCRATCH_STR", "")]);
        let r = resolve(
            None,
            ResolveOptions {
                env: Some(&env),
                file: None,
            },
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (path, _) = record_success(dir.path(), "prod", "", &r).unwrap();
        let f = parse_file(&path).unwrap();
        assert!(
            f.get("scratch.str").is_some(),
            "the record does not name the cleared setting"
        );
        let back = resolve(
            None,
            ResolveOptions {
                file: Some(&f),
                env: None,
            },
        )
        .unwrap();
        assert_eq!(back.string("scratch.str"), "");
    }

    #[test]
    fn record_omits_settings_nobody_supplied() {
        let mut t = TestEnv::new();
        let r = resolved_with(&mut t, &[("FIXTURE_NAME", "a-name")]);
        let dir = tempfile::tempdir().unwrap();
        let (path, _) = record_success(dir.path(), "prod", "", &r).unwrap();
        let f = parse_file(&path).unwrap();
        assert_eq!(f.keys(), vec!["fixture.name".to_string()]);
    }

    #[test]
    fn record_never_writes_a_secret() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let mut env = BTreeMap::new();
        let mut secrets = Vec::new();
        for s in all() {
            if s.secret {
                env.insert(s.env.clone(), format!("SENSITIVE-{}", s.key));
                secrets.push(s.key.clone());
            }
        }
        assert!(!secrets.is_empty());
        let r = resolve(
            None,
            ResolveOptions {
                env: Some(&env),
                file: None,
            },
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (path, omitted) = record_success(dir.path(), "prod", "", &r).unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains("SENSITIVE"), "{raw}");
        assert_eq!(omitted.len(), secrets.len(), "{omitted:?}");
    }

    #[test]
    fn success_replaces_and_failure_accumulates() {
        let mut t = TestEnv::new();
        let r = resolved_with(&mut t, &[]);
        let dir = tempfile::tempdir().unwrap();
        for _ in 0..2 {
            record_success(dir.path(), "prod", "", &r).unwrap();
        }
        assert_eq!(count_files(&dir.path().join(INSTALLS_DIR)), 1);
        let (first, _) =
            record_failure(dir.path(), "prod", "", &r, "hivekeep-setup deploy gate").unwrap();
        let (second, _) =
            record_failure(dir.path(), "prod", "", &r, "hivekeep-setup deploy gate").unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn record_name_is_path_safe() {
        for (context, want) in [
            ("kind-kind", "kind-kind"),
            (
                "gke_prod_us-central1-a_hivekeep",
                "gke_prod_us-central1-a_hivekeep",
            ),
            (
                "arn:aws:eks:us-east-1:111122223333:cluster/prod",
                "arn-aws-eks-us-east-1-111122223333-cluster-prod",
            ),
            ("", UNKEYED_CONTEXT),
        ] {
            assert_eq!(record_name(context), want);
            assert!(!want.contains(['/', '\\']));
        }
    }

    #[cfg(unix)]
    #[test]
    fn record_file_is_not_world_readable() {
        use std::os::unix::fs::PermissionsExt;
        let mut t = TestEnv::new();
        let r = resolved_with(&mut t, &[("FIXTURE_NAME", "x")]);
        let dir = tempfile::tempdir().unwrap();
        let (path, _) = record_success(dir.path(), "prod", "", &r).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let (failed, _) = record_failure(dir.path(), "prod", "", &r, "x").unwrap();
        assert_eq!(
            std::fs::metadata(&failed).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn record_leaves_no_temporary_files() {
        let mut t = TestEnv::new();
        let r = resolved_with(&mut t, &[("FIXTURE_NAME", "x")]);
        let dir = tempfile::tempdir().unwrap();
        record_success(dir.path(), "prod", "", &r).unwrap();
        assert_eq!(count_files(&dir.path().join(INSTALLS_DIR)), 1);
    }

    #[test]
    fn record_dir_cases() {
        let _t = TestEnv::new();
        assert_eq!(
            record_dir("/tmp/somewhere").unwrap(),
            PathBuf::from("/tmp/somewhere")
        );
        let got = record_dir("").unwrap();
        assert!(got.ends_with("hivekeep-setup"), "{}", got.display());
        assert!(got.is_absolute(), "{}", got.display());
    }

    const KUBECONFIG_FIXTURE: &str = "apiVersion: v1
kind: Config
current-context: prod-cluster
clusters:
- {name: c, cluster: {server: \"https://127.0.0.1:1\"}}
contexts:
- {name: prod-cluster, context: {cluster: c, user: u}}
users:
- {name: u, user: {}}
";

    fn write_kubeconfig(dir: &Path, name: &str) -> PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, KUBECONFIG_FIXTURE).unwrap();
        p
    }

    #[test]
    fn cluster_key_uses_the_default_kubeconfig_rules() {
        let mut t = TestEnv::new();
        let dir = tempfile::tempdir().unwrap();
        t.set_env(
            "KUBECONFIG",
            write_kubeconfig(dir.path(), "config").to_str().unwrap(),
        );
        // Nothing supplies the kubeconfig setting; the process environment is
        // what the default rules read.
        let r = resolved_with(&mut t, &[]);
        assert_eq!(r.string("kubeconfig"), "");
        assert_eq!(r.cluster_key(), "prod-cluster");
    }

    #[test]
    fn cluster_key_reads_a_kubeconfig_list() {
        let mut t = TestEnv::new();
        let dir = tempfile::tempdir().unwrap();
        let list = std::env::join_paths([
            dir.path().join("missing"),
            write_kubeconfig(dir.path(), "second"),
        ])
        .unwrap()
        .into_string()
        .unwrap();
        let r = resolved_with(&mut t, &[("KUBECONFIG", &list)]);
        assert_eq!(r.cluster_key(), "prod-cluster");
    }

    #[test]
    fn cluster_key_is_empty_without_a_readable_kubeconfig() {
        let mut t = TestEnv::new();
        let r = resolved_with(&mut t, &[("KUBECONFIG", "/nonexistent/kubeconfig")]);
        assert_eq!(r.cluster_key(), "");
    }
}

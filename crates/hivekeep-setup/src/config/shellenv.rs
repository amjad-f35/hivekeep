//! Sourcing the developer environment file through bash, and finding the
//! repository root it lives in.

use super::errors::ConfigError;
use super::MANIFESTS_DIR_ENV;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Sources the file and prints every exported variable NUL-separated on fd 3.
/// The file's own output goes to stderr, so a dev env that echoes cannot glue
/// text onto the first variable printed.
const SOURCE_ENV_SCRIPT: &str = r#"
set -o errexit
exec 3>&1 1>&2
source "$1"
for __hk_name in $(compgen -e); do
  printf '%s=%s\0' "${__hk_name}" "${!__hk_name}" >&3
done
"#;

/// Runs the developer env file in bash from `workdir` and returns what it
/// exports, so shell interpolation keeps working in existing files.
pub fn source_shell_env(
    path: &Path,
    workdir: &Path,
) -> Result<BTreeMap<String, String>, ConfigError> {
    let out = Command::new("bash")
        .arg("-c")
        .arg(SOURCE_ENV_SCRIPT)
        .arg("bash")
        .arg(path)
        .current_dir(workdir)
        .output()
        .map_err(|e| ConfigError::Other(format!("while running bash: {e}")))?;
    if !out.status.success() {
        return Err(ConfigError::Other(format!(
            "{}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    let mut env = BTreeMap::new();
    for entry in String::from_utf8_lossy(&out.stdout).split('\0') {
        if let Some((name, value)) = entry.split_once('=') {
            if !name.is_empty() {
                env.insert(name.to_string(), value.to_string());
            }
        }
    }
    Ok(env)
}

/// The repository root: the nearest directory above the working directory
/// whose Cargo.toml declares a workspace. Outside a checkout, an installer
/// pointed at its manifests by `HK_MANIFESTS_DIR` runs from the working
/// directory.
pub fn repo_root() -> Result<PathBuf, ConfigError> {
    let cwd = std::env::current_dir()
        .map_err(|e| ConfigError::Other(format!("while resolving the working directory: {e}")))?;
    for dir in cwd.ancestors() {
        if let Ok(manifest) = std::fs::read_to_string(dir.join("Cargo.toml")) {
            if manifest.lines().any(|l| l.trim() == "[workspace]") {
                return Ok(dir.to_path_buf());
            }
        }
    }
    if std::env::var_os(MANIFESTS_DIR_ENV).is_some_and(|v| !v.is_empty()) {
        return Ok(cwd);
    }
    Err(ConfigError::Other(format!(
        "no Cargo workspace found above {}: run hivekeep-setup from inside the repository, or set {MANIFESTS_DIR_ENV}",
        cwd.display()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_shell_env_interpolates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("env.sh");
        std::fs::write(
            &path,
            "export PROJECT_ID=demo-project\nexport BUCKET_NAME=snapshots-${PROJECT_ID}\n",
        )
        .unwrap();
        let env = source_shell_env(&path, dir.path()).unwrap();
        assert_eq!(env["PROJECT_ID"], "demo-project");
        assert_eq!(env["BUCKET_NAME"], "snapshots-demo-project");
    }

    #[test]
    fn source_shell_env_ignores_script_output() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("env.sh");
        std::fs::write(
            &path,
            "echo 'Using project demo-project'\nprintf 'no trailing newline either'\nexport AAA_FIRST=one\nexport PROJECT_ID=demo-project\n",
        )
        .unwrap();
        let env = source_shell_env(&path, dir.path()).unwrap();
        assert_eq!(env["AAA_FIRST"], "one");
        assert_eq!(env["PROJECT_ID"], "demo-project");
        assert!(!env.keys().any(|k| k.contains("Using project")));
    }

    #[test]
    fn source_shell_env_reports_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("env.sh");
        std::fs::write(&path, "exit 3\n").unwrap();
        assert!(source_shell_env(&path, dir.path()).is_err());
    }
}

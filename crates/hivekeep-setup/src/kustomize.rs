//! Rendering kustomizations, and composing a manifest with kustomize
//! components.
//!
//! Rendering shells out to `kubectl kustomize` (or a standalone `kustomize`
//! binary named by `HK_KUSTOMIZE`), with load restrictions off so overlays may
//! reference files outside their own directory.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Environment variable naming a `kustomize` binary to use instead of
/// `kubectl kustomize`.
pub const KUSTOMIZE_ENV: &str = "HK_KUSTOMIZE";

#[derive(Debug, thiserror::Error)]
pub enum KustomizeError {
    #[error("while building the kustomization at {dir}: {message}")]
    Build { dir: String, message: String },
    #[error("{0}")]
    Io(String),
    #[error("while composing {resource} with {components}: {source}")]
    Compose {
        resource: String,
        components: String,
        source: Box<KustomizeError>,
    },
}

fn command() -> Command {
    match std::env::var(KUSTOMIZE_ENV) {
        Ok(bin) if !bin.is_empty() => {
            let mut c = Command::new(bin);
            c.arg("build");
            c
        }
        _ => {
            let mut c = Command::new("kubectl");
            c.arg("kustomize");
            c
        }
    }
}

/// Whether a kustomize renderer is available on this host.
pub fn available() -> bool {
    let mut c = command();
    c.arg("--help");
    c.stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    c.status().map(|s| s.success()).unwrap_or(false)
}

/// Renders the kustomization in `dir`.
pub fn build(dir: &Path) -> Result<Vec<u8>, KustomizeError> {
    let out = command()
        .arg("--load-restrictor")
        .arg("LoadRestrictionsNone")
        .arg(dir)
        .output()
        .map_err(|e| KustomizeError::Build {
            dir: dir.display().to_string(),
            message: e.to_string(),
        })?;
    if !out.status.success() {
        return Err(KustomizeError::Build {
            dir: dir.display().to_string(),
            message: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    Ok(out.stdout)
}

/// Renders `path` (a manifest file or a kustomization directory) with
/// `components` layered over it.
pub fn compose(path: &Path, components: &[PathBuf]) -> Result<Vec<u8>, KustomizeError> {
    let dir = scratch()?;
    compose_in(dir.path(), path, components)
}

/// Renders `manifest` with `components` layered over it. A stream holding
/// none of the objects a component targets passes through unchanged.
pub fn compose_bytes(manifest: &[u8], components: &[PathBuf]) -> Result<Vec<u8>, KustomizeError> {
    let dir = scratch()?;
    let resource = dir.path().join("manifest.yaml");
    std::fs::write(&resource, manifest)
        .map_err(|e| KustomizeError::Io(format!("while staging a manifest for kustomize: {e}")))?;
    compose_in(dir.path(), &resource, components)
}

fn scratch() -> Result<tempfile::TempDir, KustomizeError> {
    tempfile::Builder::new()
        .prefix("hivekeep-setup-kustomize-")
        .tempdir()
        .map_err(|e| KustomizeError::Io(format!("while creating a scratch kustomization: {e}")))
}

fn compose_in(
    dir: &Path,
    resource: &Path,
    components: &[PathBuf],
) -> Result<Vec<u8>, KustomizeError> {
    let mut k = String::from(
        "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n",
    );
    k.push_str(&format!("  - {}\n", relative_to(dir, resource)?));
    k.push_str("components:\n");
    for c in components {
        k.push_str(&format!("  - {}\n", relative_to(dir, c)?));
    }
    std::fs::write(dir.join("kustomization.yaml"), k)
        .map_err(|e| KustomizeError::Io(format!("while writing a scratch kustomization: {e}")))?;
    build(dir).map_err(|e| KustomizeError::Compose {
        resource: resource.display().to_string(),
        components: components
            .iter()
            .map(|c| c.display().to_string())
            .collect::<Vec<_>>()
            .join(", "),
        source: Box::new(e),
    })
}

/// `target` relative to `dir`, both with symlinks resolved, so the scratch
/// directory's real location (macOS `/var` → `/private/var`) cannot skew it.
fn relative_to(dir: &Path, target: &Path) -> Result<String, KustomizeError> {
    let real_dir = std::fs::canonicalize(dir)
        .map_err(|e| KustomizeError::Io(format!("while resolving {}: {e}", dir.display())))?;
    let real_target = std::fs::canonicalize(target)
        .map_err(|e| KustomizeError::Io(format!("while resolving {}: {e}", target.display())))?;
    let a: Vec<_> = real_dir.components().collect();
    let b: Vec<_> = real_target.components().collect();
    let common = a.iter().zip(&b).take_while(|(x, y)| x == y).count();
    let mut parts: Vec<String> = std::iter::repeat_n("..".to_string(), a.len() - common).collect();
    parts.extend(
        b[common..]
            .iter()
            .map(|c| c.as_os_str().to_string_lossy().into_owned()),
    );
    Ok(parts.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_DEPLOYMENT: &str = "apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: demo
spec:
  template:
    spec:
      containers:
      - name: web
        image: example.com/web
";

    const TEST_CONFIG_MAP: &str = "apiVersion: v1
kind: ConfigMap
metadata:
  name: settings
  namespace: demo
data:
  key: value
";

    /// Labels every Deployment and nothing else, so a composed render is
    /// distinguishable from a plain one.
    const TEST_COMPONENT: &str = "apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
patches:
  - target:
      kind: Deployment
    patch: |-
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: overridden-by-target
        labels:
          composed: \"true\"
";

    struct Fixture {
        _root: tempfile::TempDir,
        component: PathBuf,
        deployment: PathBuf,
        config_map: PathBuf,
    }

    /// The component and manifests live in separate directories, so the
    /// relative paths composed have to cross directory boundaries.
    fn fixture() -> Fixture {
        let root = tempfile::tempdir().unwrap();
        let component = root.path().join("components/label");
        let manifests = root.path().join("manifests");
        std::fs::create_dir_all(&component).unwrap();
        std::fs::create_dir_all(&manifests).unwrap();
        std::fs::write(component.join("kustomization.yaml"), TEST_COMPONENT).unwrap();
        std::fs::write(manifests.join("deployment.yaml"), TEST_DEPLOYMENT).unwrap();
        std::fs::write(manifests.join("configmap.yaml"), TEST_CONFIG_MAP).unwrap();
        Fixture {
            component,
            deployment: manifests.join("deployment.yaml"),
            config_map: manifests.join("configmap.yaml"),
            _root: root,
        }
    }

    macro_rules! require_kustomize {
        () => {
            if !available() {
                eprintln!("skipping: no kustomize renderer (kubectl) on PATH");
                return;
            }
        };
    }

    #[test]
    fn compose_applies_component_to_file() {
        require_kustomize!();
        let f = fixture();
        let out =
            String::from_utf8(compose(&f.deployment, std::slice::from_ref(&f.component)).unwrap())
                .unwrap();
        assert!(out.contains("composed: \"true\""), "{out}");
        assert!(out.contains("name: web"), "{out}");
    }

    #[test]
    fn compose_leaves_unmatched_stream_alone() {
        require_kustomize!();
        let f = fixture();
        let out =
            String::from_utf8(compose(&f.config_map, std::slice::from_ref(&f.component)).unwrap())
                .unwrap();
        assert!(!out.contains("composed"), "{out}");
        assert!(out.contains("key: value"), "{out}");
    }

    #[test]
    fn compose_applies_component_to_directory() {
        require_kustomize!();
        let f = fixture();
        let dir = f.deployment.parent().unwrap();
        std::fs::write(
            dir.join("kustomization.yaml"),
            "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - deployment.yaml\n  - configmap.yaml\n",
        )
        .unwrap();
        let out =
            String::from_utf8(compose(dir, std::slice::from_ref(&f.component)).unwrap()).unwrap();
        assert!(out.contains("composed: \"true\""), "{out}");
        assert!(out.contains("name: settings"), "{out}");
    }

    #[test]
    fn compose_bytes_applies_component() {
        require_kustomize!();
        let f = fixture();
        let out =
            String::from_utf8(compose_bytes(TEST_DEPLOYMENT.as_bytes(), &[f.component]).unwrap())
                .unwrap();
        assert!(out.contains("composed: \"true\""), "{out}");
    }

    #[test]
    fn compose_reports_missing_component() {
        let f = fixture();
        let absent = tempfile::tempdir().unwrap().path().join("absent");
        assert!(compose(&f.deployment, &[absent]).is_err());
    }

    #[test]
    fn relative_paths_cross_directories() {
        let f = fixture();
        let dir = f.deployment.parent().unwrap();
        assert_eq!(
            relative_to(dir, &f.component).unwrap(),
            "../components/label"
        );
    }
}

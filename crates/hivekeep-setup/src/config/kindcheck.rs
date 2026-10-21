//! Checking a Kind target is reachable before a deploy starts, so a missing
//! Docker daemon or kubeconfig context fails with directions rather than a
//! connection error halfway through.

use super::errors::ConfigError;
use super::kubeconfig::{self, KubeconfigError};
use super::Config;
use std::time::Duration;

impl Config {
    /// Fails unless Docker answers and the kubeconfig has the target context.
    pub async fn check_kind_cluster(&self) -> Result<(), ConfigError> {
        let docker = tokio::process::Command::new("docker").arg("info").output();
        match tokio::time::timeout(Duration::from_secs(10), docker).await {
            Err(_) => {
                return Err(ConfigError::Other(
                    "cannot access Docker; start Docker and check `docker info` before deploying to Kind: timed out"
                        .into(),
                ))
            }
            Ok(Err(e)) => {
                return Err(ConfigError::Other(format!(
                    "cannot access Docker; start Docker and check `docker info` before deploying to Kind: {e}"
                )))
            }
            Ok(Ok(out)) if !out.status.success() => {
                let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
                text.push_str(&String::from_utf8_lossy(&out.stderr));
                return Err(ConfigError::Other(format!(
                    "cannot access Docker; start Docker and check `docker info` before deploying to Kind: {}\n{}",
                    out.status,
                    text.trim()
                )));
            }
            Ok(Ok(_)) => {}
        }
        self.check_kind_context()
    }

    /// The kubeconfig half of [`Self::check_kind_cluster`].
    pub fn check_kind_context(&self) -> Result<(), ConfigError> {
        let mut name = self.context.clone();
        match kubeconfig::load(&self.kubeconfig) {
            Ok(k) => {
                if name.is_empty() {
                    name = k.current_context.clone();
                }
                if k.contexts.contains(&name) {
                    return Ok(());
                }
            }
            Err(KubeconfigError::Missing(_)) => {}
            Err(KubeconfigError::Invalid(e)) => {
                return Err(ConfigError::Other(format!("while reading kubeconfig: {e}")))
            }
        }
        let cluster = self.resolved().string("kindCluster.name");
        Err(ConfigError::Other(format!(
            "Kubernetes context {name:?} was not found. If Kind cluster {cluster:?} already exists, restore its \
             kubeconfig with:\n  kind export kubeconfig --name={cluster:?}\nOtherwise create the cluster (and a \
             local registry at localhost:5001) with:\n  kind create cluster --name={cluster:?}\nFor a custom \
             context, check --context and --kubeconfig instead"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KUBECONFIG: &str = "apiVersion: v1\nkind: Config\ncurrent-context: kind-kind\ncontexts:\n- {name: kind-kind, context: {cluster: c, user: u}}\n";

    #[test]
    fn context_present_passes_and_absent_names_the_cluster() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, KUBECONFIG).unwrap();
        let cfg = Config {
            kubeconfig: path.display().to_string(),
            context: "kind-kind".into(),
            ..Default::default()
        };
        cfg.check_kind_context().unwrap();
        let cfg = Config {
            kubeconfig: path.display().to_string(),
            ..Default::default()
        };
        cfg.check_kind_context().unwrap();
        let cfg = Config {
            kubeconfig: path.display().to_string(),
            context: "kind-other".into(),
            ..Default::default()
        };
        let err = cfg.check_kind_context().unwrap_err().to_string();
        assert!(
            err.contains("kind-other") && err.contains("kind create cluster"),
            "{err}"
        );
        let cfg = Config {
            kubeconfig: dir.path().join("absent").display().to_string(),
            context: "kind-kind".into(),
            ..Default::default()
        };
        assert!(cfg.check_kind_context().is_err());
    }
}

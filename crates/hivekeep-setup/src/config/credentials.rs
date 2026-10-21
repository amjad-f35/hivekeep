//! Fetching GKE credentials into the kubeconfig before any client is built,
//! for an install whose dev env names a cluster rather than a context.

use super::errors::ConfigError;
use super::{Config, DEV_ENV_FILE};
use crate::log;

impl Config {
    /// Runs `gcloud container clusters get-credentials` when the
    /// configuration names a GKE cluster and no context. Kind installs and
    /// installs with an explicit context are left alone.
    pub async fn ensure_cluster_credentials(&self) -> Result<(), ConfigError> {
        if self.kind || !self.context.is_empty() || self.project_id.is_empty() {
            return Ok(());
        }
        if self.cluster_name.is_empty() || self.cluster_location.is_empty() {
            return Err(ConfigError::Other(format!(
                "PROJECT_ID is set but CLUSTER_NAME and CLUSTER_LOCATION are not; set them in {DEV_ENV_FILE} \
                 or pass --context to use an already-configured cluster"
            )));
        }
        log::step(&format!(
            "gcloud container clusters get-credentials {}",
            self.cluster_name
        ));
        let _t = log::Timer::new("gcloud get-credentials");
        let mut cmd = tokio::process::Command::new("gcloud");
        cmd.args([
            "container",
            "clusters",
            "get-credentials",
            &self.cluster_name,
        ])
        .args([
            "--location",
            &self.cluster_location,
            "--project",
            &self.project_id,
        ])
        .env_clear();
        for kv in self.script_env() {
            if let Some((k, v)) = kv.split_once('=') {
                cmd.env(k, v);
            }
        }
        let status = cmd.status().await.map_err(|e| {
            ConfigError::Other(format!(
                "while fetching credentials for cluster {}: {e}",
                self.cluster_name
            ))
        })?;
        if !status.success() {
            return Err(ConfigError::Other(format!(
                "while fetching credentials for cluster {}: gcloud {status}",
                self.cluster_name
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn nothing_to_fetch_without_gke_coordinates() {
        let cfg = Config {
            kind: true,
            project_id: "p".into(),
            ..Default::default()
        };
        cfg.ensure_cluster_credentials().await.unwrap();
        let cfg = Config {
            context: "ctx".into(),
            project_id: "p".into(),
            ..Default::default()
        };
        cfg.ensure_cluster_credentials().await.unwrap();
        Config::default()
            .ensure_cluster_credentials()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_project_without_a_cluster_is_an_error() {
        let cfg = Config {
            project_id: "p".into(),
            ..Default::default()
        };
        let err = cfg
            .ensure_cluster_credentials()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("CLUSTER_NAME") && err.contains(DEV_ENV_FILE),
            "{err}"
        );
    }
}

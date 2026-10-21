//! Refusing workload installs before the control plane exists.

use super::{Env, Result, StepError};
use k8s_openapi::api::apps::v1::Deployment;
use std::time::Duration;

/// The controller Deployment that reconciles workloads into Deployments.
const CONTROLLER: &str = "hivekeep-controller";

impl Env {
    /// Refuses demos and benchmarks before they create resources only the
    /// controller can reconcile.
    pub async fn require_control_plane(&self) -> Result<()> {
        let ns = self.namespace();
        let k = self.kube()?;
        let found = tokio::time::timeout(
            Duration::from_secs(10),
            k.get_typed::<Deployment>(ns, CONTROLLER),
        )
        .await
        .map_err(|_| {
            StepError::msg(format!(
                "while checking deployment/{CONTROLLER} in namespace {ns}: timed out"
            ))
        })?
        .map_err(|e| {
            StepError::msg(format!(
                "while checking deployment/{CONTROLLER} in namespace {ns}: {e}"
            ))
        })?;
        if found.is_some() {
            return Ok(());
        }
        let mut flags = String::new();
        if self.cfg.kind {
            flags.push_str(" --kind");
        }
        if !self.cfg.context.is_empty() {
            flags.push_str(&format!(" --context={:?}", self.cfg.context));
        }
        if !self.cfg.kubeconfig.is_empty() {
            flags.push_str(&format!(" --kubeconfig={:?}", self.cfg.kubeconfig));
        }
        Err(StepError::msg(format!(
            "demos and benchmarks require deployment/{CONTROLLER} in namespace {ns}: not found\n\
             Deploy the control plane first, using the same installation settings:\n  \
             hivekeep-setup{flags} deploy hivekeep-system --credential-provider='{{\"name\":\"k8s.io\"}}'"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::steps::cluster_fixture::fake_env;

    #[tokio::test]
    async fn require_control_plane_cases() {
        let (e, _f) = fake_env(
            Config {
                kind: true,
                context: "kind-kind".into(),
                ..Default::default()
            },
            vec![],
        );
        let err = e.require_control_plane().await.unwrap_err().to_string();
        assert!(err.contains("deployment/hivekeep-controller"), "{err}");
        assert!(
            err.contains("hivekeep-setup --kind --context=\"kind-kind\" deploy hivekeep-system"),
            "{err}"
        );

        let dep = crate::kube::stub(
            &crate::kube::Gvk::new("apps", "v1", "Deployment"),
            "hivekeep-system",
            CONTROLLER,
        );
        let (e, _f) = fake_env(Config::default(), vec![dep]);
        e.require_control_plane().await.unwrap();
    }
}

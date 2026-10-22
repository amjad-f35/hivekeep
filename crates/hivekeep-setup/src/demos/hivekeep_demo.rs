//! The common demo shape: one manifest holding the namespace and worker
//! pool, plus ActorTemplates created through the control plane API.

use super::render::{render, EXTERNAL_VOLUME_PLACEHOLDERS};
use super::Demo;
use crate::kube::WorkloadKind;
use crate::log;
use crate::steps::actors::TemplateRef;
use crate::steps::env::DEMO_TIMEOUT;
use crate::steps::hivekeep::{
    actor_template_from_manifest, create_actor_template, ensure_hive, wait_actor_template_golden,
};
use crate::steps::{Env, Result};
use async_trait::async_trait;
use hivekeep_core::resource::ActorTemplateRef;
use std::collections::BTreeMap;
use std::time::Duration;

/// The golden-snapshot budget of the micro-VM demos: a cloud-hypervisor cold
/// boot plus a checkpoint, on nested KVM in CI, does not fit the default.
pub const MICROVM_GOLDEN_TIMEOUT: Duration = Duration::from_secs(600);

/// One protojson ActorTemplate manifest and the template it creates.
#[derive(Clone, Debug)]
pub struct DemoTemplate {
    /// The `*.yaml.tmpl` path, relative to the manifests directory.
    pub manifest: String,
    pub reference: ActorTemplateRef,
}

impl DemoTemplate {
    pub fn new(manifest: &str, hive: &str, name: &str) -> Self {
        Self {
            manifest: manifest.into(),
            reference: ActorTemplateRef::new(hive, name),
        }
    }
}

/// A demo in the worker-pool-plus-templates shape. Most demos are exactly
/// this; one needing more wraps it and supplies its own deploy and delete.
#[derive(Clone, Debug)]
pub struct HivekeepDemo {
    pub demo_name: String,
    pub short: String,
    /// The `*.yaml.tmpl` holding the namespace and WorkerPool, relative to
    /// the manifests directory.
    pub worker_pool_manifest: String,
    /// Pool Deployments to wait for, in order. The WorkerPool controller
    /// names each Deployment after its WorkerPool, in the hive's namespace.
    pub deployments: Vec<TemplateRef>,
    /// The ActorTemplates, created in order.
    pub templates: Vec<DemoTemplate>,
    /// Budget for each golden snapshot; zero means the default demo timeout.
    pub golden_timeout: Duration,
}

impl HivekeepDemo {
    pub fn new(name: &str, short: &str, worker_pool_manifest: &str) -> Self {
        Self {
            demo_name: name.into(),
            short: short.into(),
            worker_pool_manifest: worker_pool_manifest.into(),
            deployments: Vec::new(),
            templates: Vec::new(),
            golden_timeout: Duration::ZERO,
        }
    }

    pub fn deployment(mut self, hive: &str, name: &str) -> Self {
        self.deployments.push(TemplateRef {
            hive: hive.into(),
            name: name.into(),
        });
        self
    }

    pub fn template(mut self, manifest: &str, hive: &str, name: &str) -> Self {
        self.templates.push(DemoTemplate::new(manifest, hive, name));
        self
    }

    pub fn golden_timeout(mut self, t: Duration) -> Self {
        self.golden_timeout = t;
        self
    }

    /// The distinct hives of the templates, in first-use order.
    pub fn hives(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for t in &self.templates {
            if !out.contains(&t.reference.hive) {
                out.push(t.reference.hive.clone());
            }
        }
        out
    }

    /// Deploys with extra placeholder values. Optional placeholders the
    /// values do not supply are dropped, so a demo opts lines in by
    /// providing values for them.
    pub async fn deploy_with_values(
        &self,
        env: &Env,
        values: &BTreeMap<String, String>,
    ) -> Result<()> {
        log::step(&format!("{}_deploy", self.demo_name));
        env.ensure_crds().await?;
        let drop: Vec<&str> = EXTERNAL_VOLUME_PLACEHOLDERS
            .iter()
            .copied()
            .filter(|n| !values.contains_key(*n))
            .collect();

        let manifest = render(env, &self.worker_pool_manifest, values, &drop)?;
        env.resolve_and_apply_bytes(&manifest).await?;
        for d in &self.deployments {
            env.kube()?
                .rollout_status(WorkloadKind::Deployment, &d.hive, &d.name, DEMO_TIMEOUT)
                .await?;
        }

        let client = env.api_client().await?;
        for hive in self.hives() {
            ensure_hive(client.as_ref(), &hive).await?;
        }
        for t in &self.templates {
            let manifest = render(env, &t.manifest, values, &drop)?;
            // Image references are pinned before the manifest is parsed,
            // which is what the ActorTemplate schema requires.
            let resolved = env.resolve_manifest_bytes(&manifest).await?;
            let template = actor_template_from_manifest(&resolved)?;
            create_actor_template(client.as_ref(), template).await?;
        }

        // Block until the golden snapshots exist, so callers run against an
        // already-warm node rather than racing first-start costs.
        let golden = if self.golden_timeout.is_zero() {
            DEMO_TIMEOUT
        } else {
            self.golden_timeout
        };
        for t in &self.templates {
            log::step(&format!(
                "Waiting for the {} golden snapshot...",
                t.reference
            ));
            wait_actor_template_golden(client.as_ref(), &t.reference, golden).await?;
        }
        Ok(())
    }

    pub async fn delete_demo(&self, env: &Env) -> Result<()> {
        log::step(&format!("{}_delete", self.demo_name));
        let refs: Vec<ActorTemplateRef> =
            self.templates.iter().map(|t| t.reference.clone()).collect();
        env.delete_hivekeep_demo(&refs, &self.hives()).await?;
        let manifest = render(
            env,
            &self.worker_pool_manifest,
            &BTreeMap::new(),
            &EXTERNAL_VOLUME_PLACEHOLDERS,
        )?;
        Ok(env.kube()?.delete_bytes(&manifest).await?)
    }
}

#[async_trait]
impl Demo for HivekeepDemo {
    fn name(&self) -> &str {
        &self.demo_name
    }
    fn description(&self) -> &str {
        &self.short
    }
    fn hivekeep_demo(&self) -> Option<&HivekeepDemo> {
        Some(self)
    }
    async fn deploy(&self, env: &Env) -> Result<()> {
        self.deploy_with_values(env, &BTreeMap::new()).await
    }
    async fn delete(&self, env: &Env) -> Result<()> {
        self.delete_demo(env).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hives_are_distinct_in_first_use_order() {
        let d = HivekeepDemo::new("demo-x", "x", "demos/x.yaml.tmpl")
            .template("a", "h1", "t1")
            .template("b", "h2", "t2")
            .template("c", "h1", "t3");
        assert_eq!(d.hives(), vec!["h1".to_string(), "h2".to_string()]);
    }
}

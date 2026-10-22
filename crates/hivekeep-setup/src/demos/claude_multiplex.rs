//! Several Claude Code agents multiplexed onto one WorkerPool. Its workload
//! image is built from a Dockerfile at deploy time.

use super::{Demo, HivekeepDemo};
use crate::images;
use crate::steps::{Env, Result, StepError};
use async_trait::async_trait;
use std::collections::BTreeMap;

const POOL_MANIFEST: &str = "demos/claude-code-multiplex/claude-code-multiplex.yaml.tmpl";
/// Build context of the workload image, relative to the repository root.
const WORKLOAD: &str = "demos/claude-code-multiplex/workload";
const HIVE: &str = "claude-multiplex-demo";
const IMAGE_NAME: &str = "claude-multiplex-demo-workload";
const AGENTS: [&str; 3] = ["agent-luna", "agent-mars", "agent-orion"];

pub(super) struct ClaudeMultiplex {
    base: HivekeepDemo,
}

pub(super) fn demo() -> ClaudeMultiplex {
    let mut base = HivekeepDemo::new(
        "demo-claude-code-multiplex",
        "Several Claude Code agents multiplexed onto one WorkerPool (requires ANTHROPIC_API_KEY, BUCKET_NAME, \
         HK_BUILD_REPO)",
        POOL_MANIFEST,
    )
    .deployment(HIVE, "claude-workerpool");
    for agent in AGENTS {
        base = base.template(
            &format!("demos/claude-code-multiplex/{agent}-template.yaml.tmpl"),
            HIVE,
            agent,
        );
    }
    ClaudeMultiplex { base }
}

impl ClaudeMultiplex {
    async fn render_values(&self, env: &Env) -> Result<BTreeMap<String, String>> {
        let image = images::build_dockerfile_image(
            &env.cfg.root,
            &env.cfg.build_repo,
            IMAGE_NAME,
            &env.cfg.path(&[WORKLOAD]),
            &env.cfg.build_platforms,
            &env.cfg.docker_build_flags,
        )
        .await?;
        Ok(BTreeMap::from([
            (
                "ANTHROPIC_API_KEY".to_string(),
                env.cfg.anthropic_api_key.clone(),
            ),
            ("WORKLOAD_IMAGE".to_string(), image),
        ]))
    }
}

#[async_trait]
impl Demo for ClaudeMultiplex {
    fn name(&self) -> &str {
        self.base.name()
    }
    fn description(&self) -> &str {
        self.base.description()
    }
    // Not exposed as the shared shape: its templates need values only a
    // deploy can supply, so it is covered by its own test.
    async fn deploy(&self, env: &Env) -> Result<()> {
        if env.cfg.anthropic_api_key.is_empty() {
            return Err(StepError::msg("ANTHROPIC_API_KEY must be set"));
        }
        if env.cfg.bucket_name.is_empty() {
            return Err(StepError::msg("BUCKET_NAME must be set"));
        }
        if env.cfg.build_repo.is_empty() {
            return Err(StepError::msg("HK_BUILD_REPO must be set"));
        }
        let values = self.render_values(env).await?;
        self.base.deploy_with_values(env, &values).await
    }
    async fn delete(&self, env: &Env) -> Result<()> {
        self.base.delete_demo(env).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::demos::render;
    use crate::demos::testutil::{assert_rendered, assert_rendered_actor_template, env};

    #[test]
    fn render_manifests() {
        let e = env();
        let pool = render(&e, POOL_MANIFEST, &BTreeMap::new(), &[]).unwrap();
        assert_rendered(&pool);

        let values = BTreeMap::from([
            ("ANTHROPIC_API_KEY".to_string(), "test-api-key".to_string()),
            (
                "WORKLOAD_IMAGE".to_string(),
                format!(
                    "registry.example.dev/{IMAGE_NAME}@sha256:{}",
                    "1".repeat(64)
                ),
            ),
        ]);
        let d = demo();
        for t in &d.base.templates {
            let manifest = render(&e, &t.manifest, &values, &[]).unwrap();
            assert_rendered_actor_template(&manifest, &t.reference);
        }
    }
}

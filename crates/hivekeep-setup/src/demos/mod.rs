//! The bundled demos: the contract every demo implements, the registry the
//! command tree is built from, and the shared behaviour of a demo that is one
//! worker pool plus a set of ActorTemplates.

mod claude_multiplex;
mod counter;
mod counter_microvm;
mod egress;
mod egress_microvm;
mod hivekeep_demo;
mod jupyter;
mod multi_template;
mod parking;
mod render;
mod sandbox;
#[cfg(test)]
pub(crate) mod testutil;

pub use hivekeep_demo::{DemoTemplate, HivekeepDemo, MICROVM_GOLDEN_TIMEOUT};
pub use render::{render, BUCKET_NAME_PLACEHOLDER, EXTERNAL_VOLUME_PLACEHOLDERS};

use crate::config::{register_command, Setting};
use crate::steps::{Deleter, Env, Result};
use async_trait::async_trait;

/// One installable example workload.
#[async_trait]
pub trait Demo: Send + Sync {
    /// Registry name, e.g. `demo-counter`.
    fn name(&self) -> &str;
    /// One-line summary shown in help.
    fn description(&self) -> &str;
    /// The demo's own configuration. Registered globally and bound as flags
    /// only on this demo's deploy command.
    fn settings(&self) -> Vec<Setting> {
        Vec::new()
    }
    /// The worker-pool-plus-templates shape, when the demo has it. Tests use
    /// it to reach every demo's manifests uniformly.
    fn hivekeep_demo(&self) -> Option<&HivekeepDemo> {
        None
    }
    async fn deploy(&self, env: &Env) -> Result<()>;
    /// Removes the demo. Must succeed where the demo was never installed,
    /// because `delete all` runs every demo's delete.
    async fn delete(&self, env: &Env) -> Result<()>;
}

/// Every bundled demo, sorted by name.
pub fn all() -> Vec<Box<dyn Demo>> {
    let mut list: Vec<Box<dyn Demo>> = vec![
        Box::new(claude_multiplex::demo()),
        Box::new(counter::demo()),
        Box::new(counter_microvm::demo()),
        Box::new(egress::demo()),
        Box::new(egress_microvm::demo()),
        Box::new(jupyter::demo()),
        Box::new(multi_template::demo()),
        Box::new(parking::demo()),
        Box::new(sandbox::demo()),
    ];
    list.sort_by(|a, b| a.name().cmp(b.name()));
    list
}

/// The command-line argument naming the demo: its name without `demo-`.
pub fn demo_arg(d: &dyn Demo) -> &str {
    d.name().strip_prefix("demo-").unwrap_or(d.name())
}

/// The command a demo is deployed by, as typed: `deploy demo counter`. Demo
/// settings bind their flags to it.
pub fn deploy_command_path(d: &dyn Demo) -> String {
    format!("deploy demo {}", demo_arg(d))
}

/// Registers every demo's settings, scoped to its deploy command, so a
/// configuration document can name them whichever command runs.
pub fn register_settings() {
    for d in all() {
        let path = deploy_command_path(d.as_ref());
        register_command(d.settings().into_iter().map(|mut s| {
            s.commands = vec![path.clone()];
            s
        }));
    }
}

/// Adapts a demo to the narrower interface `delete all` takes.
pub struct DemoDeleter<'a>(pub &'a dyn Demo);

#[async_trait]
impl Deleter for DemoDeleter<'_> {
    async fn delete(&self, env: &Env) -> Result<()> {
        self.0.delete(env).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every demo module produces a registration: a demo file nothing lists
    /// would compile and then quietly have no subcommand.
    #[test]
    fn every_demo_module_is_registered() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/demos");
        let support = ["mod.rs", "hivekeep_demo.rs", "render.rs", "testutil.rs"];
        let files = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".rs") && !support.contains(&n.as_str()))
            .count();
        let names: Vec<String> = all().iter().map(|d| d.name().to_string()).collect();
        assert_eq!(
            files,
            names.len(),
            "{files} demo modules but registered {names:?}"
        );
    }

    /// Guards against drift between the templates and the placeholder sets:
    /// a new `${PLACEHOLDER}` nothing substitutes or drops would otherwise
    /// surface only as an apply-time YAML error.
    #[test]
    fn demo_templates_render() {
        let e = testutil::env();
        let drop = EXTERNAL_VOLUME_PLACEHOLDERS;
        let none = std::collections::BTreeMap::new();
        let mut covered = 0;
        for d in all() {
            // The claude-code multiplex demo has its own placeholders and its
            // own test; it is the one demo this skips.
            let Some(s) = d.hivekeep_demo() else { continue };
            covered += 1;
            let pool = render(&e, &s.worker_pool_manifest, &none, &drop).unwrap();
            testutil::assert_rendered(&pool);
            for t in &s.templates {
                let m = render(&e, &t.manifest, &none, &drop).unwrap();
                testutil::assert_rendered_actor_template(&m, &t.reference);
            }
        }
        assert_eq!(covered, all().len() - 1);
    }

    #[test]
    fn names_are_unique_and_prefixed() {
        let mut names: Vec<String> = all().iter().map(|d| d.name().to_string()).collect();
        assert!(names.iter().all(|n| n.starts_with("demo-")), "{names:?}");
        let n = names.len();
        names.dedup();
        assert_eq!(n, names.len());
    }
}

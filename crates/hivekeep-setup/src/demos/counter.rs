//! A counter actor exercising snapshot, resume and gate ingress, optionally
//! with an external volume.

use super::{Demo, HivekeepDemo};
use crate::config::{Setting, ValueKind};
use crate::steps::{Env, Result};
use async_trait::async_trait;
use std::collections::BTreeMap;

const HIVE: &str = "hivekeep-demo-counter";
const TEMPLATE: &str = "demos/counter/counter-template.yaml.tmpl";
const DEFAULT_STORAGE_CLASS: &str = "standard";

pub(super) struct Counter {
    base: HivekeepDemo,
}

pub(super) fn demo() -> Counter {
    Counter {
        base: HivekeepDemo::new(
            "demo-counter",
            "A counter actor exercising snapshot, resume, and gate ingress",
            "demos/counter/counter.yaml.tmpl",
        )
        .deployment(HIVE, "counter")
        .template(TEMPLATE, HIVE, "counter"),
    }
}

impl Counter {
    /// The placeholder values that attach the external volume.
    fn external_volume_values(&self, env: &Env) -> BTreeMap<String, String> {
        let sc = env.cfg.resolved().string("demo.counter.storageClass");
        BTreeMap::from([
            (
                "VALIDATE_EXISTING_FILE_PATH_ARG".to_string(),
                "  - --validate-existing-file-path=/external-data/test.txt".to_string(),
            ),
            ("EXTERNAL_VOLUME_MOUNTS".to_string(), "  - name: external-data\n    mountPath: /external-data".to_string()),
            (
                "EXTERNAL_VOLUMES".to_string(),
                format!(
                    "- name: external-data\n  external:\n    template:\n      capacityBytes: 1073741824 # 1Gi\n      storageClassName: {sc}"
                ),
            ),
        ])
    }
}

#[async_trait]
impl Demo for Counter {
    fn name(&self) -> &str {
        self.base.name()
    }
    fn description(&self) -> &str {
        self.base.description()
    }
    fn settings(&self) -> Vec<Setting> {
        vec![
            Setting::new("demo.counter.withExternalVolume", "HK_DEMO_COUNTER_WITH_EXTERNAL_VOLUME", "with-external-volume")
                .kind(ValueKind::Bool)
                .default_value("false")
                .usage("Attach an external volume and validate a pre-seeded file on it (run \"setup csi\" first)"),
            Setting::new("demo.counter.storageClass", "STORAGE_CLASS", "storage-class")
                .default_value(DEFAULT_STORAGE_CLASS)
                .usage(
                    "StorageClass backing the external volume, e.g. csi-nfs-sc or csi-hostpath-sc. Must be used \
                     --with-external-volume=true.",
                ),
        ]
    }
    fn hivekeep_demo(&self) -> Option<&HivekeepDemo> {
        Some(&self.base)
    }
    async fn deploy(&self, env: &Env) -> Result<()> {
        let values = if env.cfg.resolved().bool("demo.counter.withExternalVolume") {
            self.external_volume_values(env)
        } else {
            BTreeMap::new()
        };
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
    use crate::demos::testutil::{assert_rendered_actor_template, env_with};
    use hivekeep_core::resource::ActorTemplateRef;

    #[test]
    fn external_volume_renders() {
        for (name, storage_class, want_sc) in [
            ("unset", None, "storageClassName: standard"),
            (
                "explicit",
                Some("csi-nfs-sc"),
                "storageClassName: csi-nfs-sc",
            ),
        ] {
            let mut vars = BTreeMap::new();
            if let Some(sc) = storage_class {
                vars.insert("STORAGE_CLASS".to_string(), sc.to_string());
            }
            let e = env_with(&vars);
            let d = demo();
            let manifest = render(&e, TEMPLATE, &d.external_volume_values(&e), &[]).unwrap();
            assert_rendered_actor_template(&manifest, &ActorTemplateRef::new(HIVE, "counter"));
            let text = String::from_utf8(manifest).unwrap();
            for want in [
                "--validate-existing-file-path=/external-data/test.txt",
                "mountPath: /external-data",
                "external:",
                "template:",
                want_sc,
            ] {
                assert!(
                    text.contains(want),
                    "{name}: rendered manifest is missing {want:?}"
                );
            }
        }
    }
}

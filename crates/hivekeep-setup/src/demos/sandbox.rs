//! An on-demand sandbox actor driven by the sandbox client.

use super::HivekeepDemo;

const HIVE: &str = "hivekeep-demo-sandbox";

pub(super) fn demo() -> HivekeepDemo {
    HivekeepDemo::new(
        "demo-sandbox",
        "An on-demand sandbox actor driven by the sandbox client",
        "demos/sandbox/sandbox.yaml.tmpl",
    )
    .deployment(HIVE, "sandbox-workerpool")
    .template(
        "demos/sandbox/sandbox-template.yaml.tmpl",
        HIVE,
        "sandbox-template",
    )
}

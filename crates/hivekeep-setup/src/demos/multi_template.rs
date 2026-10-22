//! Two ActorTemplates, in two hives, sharing one WorkerPool.

use super::HivekeepDemo;

pub(super) fn demo() -> HivekeepDemo {
    HivekeepDemo::new(
        "demo-multi-template",
        "Two ActorTemplates sharing one WorkerPool",
        "demos/multi-template/multi-template.yaml.tmpl",
    )
    .deployment("hivekeep-demo-multi-template-pool", "shared-pool")
    .template(
        "demos/multi-template/counter-template.yaml.tmpl",
        "hivekeep-demo-multi-template-counter",
        "counter",
    )
    .template(
        "demos/multi-template/fspersist-template.yaml.tmpl",
        "hivekeep-demo-multi-template-fspersist",
        "fspersist",
    )
}

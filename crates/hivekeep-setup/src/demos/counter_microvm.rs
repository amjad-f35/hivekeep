//! The counter demo on micro-VM workers.

use super::{HivekeepDemo, MICROVM_GOLDEN_TIMEOUT};

const HIVE: &str = "hivekeep-demo-counter-microvm";

pub(super) fn demo() -> HivekeepDemo {
    HivekeepDemo::new(
        "demo-counter-microvm",
        "The counter demo on micro-VM workers (needs scripts/install-microvm-deps.sh --install)",
        "demos/counter/counter-microvm.yaml.tmpl",
    )
    .deployment(HIVE, "counter-microvm")
    .template(
        "demos/counter/counter-microvm-template.yaml.tmpl",
        HIVE,
        "counter-microvm",
    )
    .golden_timeout(MICROVM_GOLDEN_TIMEOUT)
}

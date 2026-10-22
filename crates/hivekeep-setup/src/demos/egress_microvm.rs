//! Egress policy enforcement on micro-VM workers.

use super::{HivekeepDemo, MICROVM_GOLDEN_TIMEOUT};

const HIVE: &str = "hivekeep-demo-egress-microvm";

pub(super) fn demo() -> HivekeepDemo {
    HivekeepDemo::new(
        "demo-egress-microvm",
        "Egress policy enforcement on micro-VM workers (needs scripts/install-microvm-deps.sh --install)",
        "demos/egress/egress-microvm.yaml.tmpl",
    )
    .deployment(HIVE, "egress-microvm")
    .template("demos/egress/egress-microvm-template.yaml.tmpl", HIVE, "egress-microvm")
    .golden_timeout(MICROVM_GOLDEN_TIMEOUT)
}

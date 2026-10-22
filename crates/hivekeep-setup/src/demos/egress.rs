//! Egress policy enforcement through the gate.

use super::HivekeepDemo;

const HIVE: &str = "hivekeep-demo-egress";

pub(super) fn demo() -> HivekeepDemo {
    HivekeepDemo::new(
        "demo-egress",
        "Egress policy enforcement through the gate",
        "demos/egress/egress.yaml.tmpl",
    )
    .deployment(HIVE, "egress")
    .template("demos/egress/egress-template.yaml.tmpl", HIVE, "egress")
}

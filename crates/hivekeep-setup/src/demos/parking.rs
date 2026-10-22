//! Actor parking and unparking on a small WorkerPool.

use super::HivekeepDemo;

const HIVE: &str = "hivekeep-demo-parking";

pub(super) fn demo() -> HivekeepDemo {
    HivekeepDemo::new(
        "demo-parking",
        "Actor parking and unparking on a small WorkerPool",
        "demos/parking/parking.yaml.tmpl",
    )
    .deployment(HIVE, "parking")
    .template("demos/parking/parking-template.yaml.tmpl", HIVE, "parking")
}

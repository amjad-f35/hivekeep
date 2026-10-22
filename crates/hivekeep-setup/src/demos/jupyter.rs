//! An unmodified Jupyter notebook image as a suspending actor.

use super::HivekeepDemo;

const HIVE: &str = "hivekeep-demo-jupyter";

pub(super) fn demo() -> HivekeepDemo {
    HivekeepDemo::new(
        "demo-jupyter",
        "An unmodified Jupyter notebook image as a suspending actor",
        "demos/jupyter/jupyter.yaml.tmpl",
    )
    .deployment(HIVE, "jupyter")
    .template("demos/jupyter/jupyter-template.yaml.tmpl", HIVE, "jupyter")
}

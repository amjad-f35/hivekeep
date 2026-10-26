//! The glutton-family user classes, each driving glutton Actors through
//! hivekeep-api and the gate router:
//!
//! - `glutton` (GluttonUser): resume / working set / ping / suspend cycles.
//! - `spawn` (SpawnUser): one batch of Actors, timed to first ping.
//! - `durdir` (DurdirUser): a file in the durable directory checked across
//!   suspend and resume.

use std::sync::Arc;

use crate::loadgen::userclass::{Entry, UserTask};

pub mod calls;
pub mod knobs;
pub mod wait_group;

pub mod actor;
pub mod durdir;
pub mod lifecycle;
pub mod spawn;

#[cfg(test)]
pub(crate) mod test_support;

/// The GluttonUser locust class.
pub const GLUTTON_USER_CLASS: &str = "GluttonUser";
/// The SpawnUser locust class.
pub const SPAWN_USER_CLASS: &str = "SpawnUser";
/// The DurdirUser locust class.
pub const DURDIR_USER_CLASS: &str = "DurdirUser";
/// The ActorTemplate GluttonUser and SpawnUser create Actors from.
pub const GLUTTON_TEMPLATE: &str = "glutton";
/// The Hive the benchmark templates live in.
pub const TEMPLATE_HIVE: &str = "benchmark-workloads";

/// The glutton, spawn and durdir classes.
pub fn entries() -> Vec<Entry> {
    vec![
        Entry {
            name: "glutton",
            locust_file: "glutton.py",
            user_class: GLUTTON_USER_CLASS,
            config: Some(knobs::glutton_codec),
            init: |cfg| Arc::new(lifecycle::TaskRuntime::new(cfg)) as Arc<dyn UserTask>,
        },
        Entry {
            name: "spawn",
            locust_file: "spawn.py",
            user_class: SPAWN_USER_CLASS,
            config: Some(knobs::spawn_codec),
            init: |cfg| Arc::new(spawn::SpawnRuntime::new(cfg)) as Arc<dyn UserTask>,
        },
        Entry {
            name: "durdir",
            locust_file: "durdir.py",
            user_class: DURDIR_USER_CLASS,
            config: Some(knobs::durdir_codec),
            init: |cfg| Arc::new(durdir::DurDirRuntime::new(cfg)) as Arc<dyn UserTask>,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_name_each_class_once() {
        let names: Vec<_> = entries().iter().map(|e| (e.name, e.user_class)).collect();
        assert_eq!(
            names,
            vec![
                ("glutton", "GluttonUser"),
                ("spawn", "SpawnUser"),
                ("durdir", "DurdirUser")
            ]
        );
        for e in entries() {
            let _ = e.holder();
        }
    }
}

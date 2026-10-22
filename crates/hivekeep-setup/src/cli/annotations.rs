//! Per-command behaviour that the nearest annotated command decides: whether
//! a run is recorded, and whether it contacts a cluster.
//!
//! A command inherits from its closest annotated ancestor, so the deploy
//! subtree opts in to recording once and a component added later cannot
//! quietly miss it.

/// Commands whose runs are recorded. Demos and benchmarks deploy on top of
/// an installed system with only their own settings, so recording them would
/// replace the system's record with one that no longer reproduces it.
const RECORD: &[(&str, bool)] = &[
    ("deploy", true),
    ("deploy demo", false),
    ("deploy benchmarks", false),
];

/// Commands that never contact a cluster: they get an environment without a
/// Kubernetes client, so they run on a host with no kubeconfig.
const CLUSTER: &[(&str, bool)] = &[("publish", false), ("gcp", false)];

/// The value of the nearest annotated prefix of `path`.
fn nearest(path: &str, table: &[(&str, bool)]) -> Option<bool> {
    let words: Vec<&str> = path.split_whitespace().collect();
    (1..=words.len()).rev().find_map(|n| {
        let prefix = words[..n].join(" ");
        table.iter().find(|(p, _)| *p == prefix).map(|(_, v)| *v)
    })
}

/// Whether a run of the command at `path` (without the program name) is
/// recorded.
pub fn records_run(path: &str) -> bool {
    nearest(path, RECORD).unwrap_or(false)
}

/// Whether the command at `path` contacts a cluster. Commands do unless
/// marked otherwise.
pub fn needs_cluster(path: &str) -> bool {
    nearest(path, CLUSTER).unwrap_or(true)
}

/// Whether the command is in the deploy subtree, which checks a Kind target
/// before it connects.
pub fn is_deploy(path: &str) -> bool {
    path == "deploy" || path.starts_with("deploy ")
}

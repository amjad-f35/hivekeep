//! Label values shared by metrics, logs and on-disk records. The values are
//! the workspace-wide ones of [`hivekeep_core::attr`]; this module names the
//! subsets the keeper records.

use hivekeep_core::attr;

/// Snapshot fidelity values, as recorded in a snapshot manifest.
pub use hivekeep_core::attr::snapshot_fidelity as fidelity;
/// Phases of a checkpoint or restore. They overlap (a restore downloads
/// while it unpacks images), so they are independent observations.
pub use hivekeep_core::attr::snapshot_phase as phase;

/// The label of a wire fidelity value.
pub fn fidelity_label(f: i32) -> &'static str {
    attr::snapshot_fidelity_value(f)
}

/// Where a snapshot an operation used came from. A keeper never boots, so
/// only the restore kinds appear.
pub mod kind {
    pub use hivekeep_core::attr::snapshot_kind::{LATEST, LOCAL};
}

/// The bounded label of a sandbox class: known classes as themselves,
/// anything else as `unknown`, so a bad value cannot grow cardinality. An
/// empty class stays empty: records omit the label while the class is not
/// yet known.
pub fn sandbox_class_label(class: &str) -> &str {
    if class.is_empty() {
        ""
    } else {
        attr::normalize_sandbox_class(class)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_proto::keeper::v1 as pb;

    #[test]
    fn fidelity_labels() {
        assert_eq!(
            fidelity_label(pb::SnapshotFidelity::Memory as i32),
            "memory"
        );
        assert_eq!(fidelity_label(0), "unknown");
        assert_eq!(fidelity_label(42), "unknown");
    }

    #[test]
    fn sandbox_classes_are_bounded() {
        assert_eq!(sandbox_class_label("gvisor"), "gvisor");
        assert_eq!(sandbox_class_label("bogus"), "unknown");
        assert_eq!(sandbox_class_label(""), "");
    }
}

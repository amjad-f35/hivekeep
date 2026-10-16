//! Sandbox runtime compatibility between workers and snapshots.
//!
//! A snapshot that holds process memory can only be restored by a runtime
//! compatible with the one it was taken under. Workers report, per sandbox
//! class, a default runtime and the other runtimes they can restore from;
//! each runtime is a schema version plus attributes. A snapshot's runtime is
//! satisfied by a worker runtime with the same schema version on which every
//! snapshot attribute is present and equal, except `cpu_features`, a CPU
//! feature bitmask that must be a subset of the worker's.
//!
//! The matching rules live in [`hivekeep_core::hardware`] so the control
//! plane, keeper and workers share one implementation.

pub use hivekeep_core::cpu_features::FeatureMask;
pub use hivekeep_core::hardware::{
    cold_boot_runtime, matches, matches_compat, matching_runtime, runtime_for, sandbox_class,
    worker_matches, Requirement, ATTR_CPU_FEATURES,
};

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_proto::api::v1 as pb;

    fn compat(schema: &str, attrs: &[(&str, &str)]) -> pb::VersionedSandboxCompat {
        pb::VersionedSandboxCompat {
            schema_version: schema.into(),
            attributes: attrs
                .iter()
                .map(|(k, v)| pb::AttributeEntry {
                    key: k.to_string(),
                    value: v.to_string(),
                })
                .collect(),
        }
    }

    fn rt(class: &str, c: pb::VersionedSandboxCompat) -> pb::ActorSandboxRuntime {
        pb::ActorSandboxRuntime {
            sandbox_class: class.into(),
            version: Some(c),
        }
    }

    fn status(
        class: &str,
        default: pb::VersionedSandboxCompat,
        restorable: Vec<pb::VersionedSandboxCompat>,
    ) -> pb::WorkerStatus {
        pb::WorkerStatus {
            sandbox_classes: vec![pb::WorkerSandboxClass {
                sandbox_class: class.into(),
                default_runtime: Some(pb::WorkerSandboxRuntime {
                    version: Some(default),
                    actor_overhead: None,
                }),
                restorable_runtimes: restorable
                    .into_iter()
                    .map(|v| pb::WorkerSandboxRuntime {
                        version: Some(v),
                        actor_overhead: None,
                    })
                    .collect(),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn feature_masks() {
        assert!(FeatureMask::parse("").is_none());
        assert!(FeatureMask::parse("0f").is_none());
        assert!(FeatureMask::parse("F").is_none());
        let a = FeatureMask::parse("5").unwrap();
        let b = FeatureMask::parse("7").unwrap();
        assert!(a.subset_of(&b));
        assert!(!b.subset_of(&a));
        let wide = FeatureMask::parse("10000000000000000").unwrap();
        assert!(!wide.subset_of(&b));
        assert!(FeatureMask::parse("0").unwrap().subset_of(&a));
    }

    #[test]
    fn requirement_matching() {
        let none = Requirement::new(None);
        assert!(none.matches(None));
        assert!(none.worker_matches(None));

        let snap = rt(
            "gvisor",
            compat("v1", &[("architecture", "amd64"), ("cpu_features", "5")]),
        );
        let req = Requirement::new(Some(&snap));
        assert!(req.matches(Some(&rt(
            "gvisor",
            compat(
                "v1",
                &[
                    ("architecture", "amd64"),
                    ("cpu_features", "7"),
                    ("extra", "x")
                ]
            )
        ))));
        assert!(!req.matches(Some(&rt(
            "gvisor",
            compat("v1", &[("architecture", "amd64"), ("cpu_features", "2")])
        ))));
        assert!(!req.matches(Some(&rt(
            "microvm",
            compat("v1", &[("architecture", "amd64"), ("cpu_features", "7")])
        ))));
        assert!(!req.matches(Some(&rt(
            "gvisor",
            compat("v2", &[("architecture", "amd64"), ("cpu_features", "7")])
        ))));
        assert!(!req.matches(Some(&rt("gvisor", compat("v1", &[("cpu_features", "7")])))));
        assert!(!req.matches(None));

        let bad = Requirement::new(Some(&rt(
            "gvisor",
            compat("v1", &[("cpu_features", "0x5")]),
        )));
        assert!(!bad.matches(Some(&rt("gvisor", compat("v1", &[("cpu_features", "7")])))));
    }

    #[test]
    fn runtime_selection() {
        let old = compat("v1", &[("sandbox_bundle_hash", "old")]);
        let new = compat("v1", &[("sandbox_bundle_hash", "new")]);
        let st = status("gvisor", new.clone(), vec![old.clone()]);

        let req = Requirement::new(Some(&rt("gvisor", old.clone())));
        assert!(req.worker_matches(Some(&st)));
        assert_eq!(req.runtime_for(Some(&st)), Some(rt("gvisor", old.clone())));

        let other = Requirement::new(Some(&rt(
            "gvisor",
            compat("v1", &[("sandbox_bundle_hash", "gone")]),
        )));
        assert!(!other.worker_matches(Some(&st)));
        // Falls back to the cold-boot runtime.
        assert_eq!(
            other.runtime_for(Some(&st)),
            Some(rt("gvisor", new.clone()))
        );
        assert_eq!(
            Requirement::default().runtime_for(Some(&st)),
            Some(rt("gvisor", new))
        );
        assert_eq!(sandbox_class(Some(&st)), "gvisor");
        assert_eq!(sandbox_class(None), "");
        assert!(cold_boot_runtime(Some(&pb::WorkerStatus::default())).is_none());
    }
}

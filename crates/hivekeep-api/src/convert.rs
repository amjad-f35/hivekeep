//! Conversions between the public API and the keeper API, which mirror each
//! other field for field.

use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::keeper::v1 as kp;

/// The keeper fidelity for a public one. Anything unknown, UNSPECIFIED
/// included, goes out as UNSPECIFIED, which the keeper rejects.
pub fn fidelity_to_keeper(f: i32) -> kp::SnapshotFidelity {
    match pb::SnapshotFidelity::try_from(f) {
        Ok(pb::SnapshotFidelity::Volumes) => kp::SnapshotFidelity::Volumes,
        Ok(pb::SnapshotFidelity::Rootfs) => kp::SnapshotFidelity::Rootfs,
        Ok(pb::SnapshotFidelity::Memory) => kp::SnapshotFidelity::Memory,
        _ => kp::SnapshotFidelity::Unspecified,
    }
}

/// The public fidelity for a keeper one.
pub fn fidelity_from_keeper(f: i32) -> pb::SnapshotFidelity {
    match kp::SnapshotFidelity::try_from(f) {
        Ok(kp::SnapshotFidelity::Volumes) => pb::SnapshotFidelity::Volumes,
        Ok(kp::SnapshotFidelity::Rootfs) => pb::SnapshotFidelity::Rootfs,
        Ok(kp::SnapshotFidelity::Memory) => pb::SnapshotFidelity::Memory,
        _ => pb::SnapshotFidelity::Unspecified,
    }
}

/// The label value of a keeper fidelity.
pub fn fidelity_label(f: kp::SnapshotFidelity) -> &'static str {
    match f {
        kp::SnapshotFidelity::Volumes => "volumes",
        kp::SnapshotFidelity::Rootfs => "rootfs",
        kp::SnapshotFidelity::Memory => "memory",
        kp::SnapshotFidelity::Unspecified => "unspecified",
    }
}

/// A keeper snapshot report in the inline public form; `None` when it names
/// no snapshot.
pub fn snapshot_report_from_keeper(r: Option<&kp::SnapshotReport>) -> Option<pb::SnapshotReport> {
    let r = r?;
    let snap = r.snapshot.as_ref()?;
    Some(pb::SnapshotReport {
        uuid: snap.snapshot_uuid.clone(),
        generation: snap.generation,
        fidelity: fidelity_from_keeper(snap.fidelity) as i32,
        object: snap.object.as_ref().map(|o| pb::ObjectSnapshot {
            snapshot_uri: o.snapshot_uri.clone(),
        }),
        local_commit_time: r.local_commit_time,
        durable_complete_time: r.durable_complete_time,
    })
}

/// The keeper form of a sandbox runtime.
pub fn runtime_to_keeper(r: Option<&pb::ActorSandboxRuntime>) -> Option<kp::ActorSandboxRuntime> {
    let r = r?;
    Some(kp::ActorSandboxRuntime {
        sandbox_class: r.sandbox_class.clone(),
        version: r.version.as_ref().map(|v| kp::VersionedSandboxCompat {
            schema_version: v.schema_version.clone(),
            attributes: v
                .attributes
                .iter()
                .map(|a| kp::AttributeEntry {
                    key: a.key.clone(),
                    value: a.value.clone(),
                })
                .collect(),
        }),
    })
}

/// The keeper form of declared limits, `None` when nothing is declared.
pub fn resources_to_keeper(
    r: Option<&pb::ResourceRequirements>,
) -> Option<kp::ResourceRequirements> {
    let l = r.and_then(|r| r.limits)?;
    if l.cpu_millicores == 0 && l.memory_bytes == 0 {
        return None;
    }
    Some(kp::ResourceRequirements {
        limits: Some(kp::Resources {
            cpu_millicores: l.cpu_millicores,
            memory_bytes: l.memory_bytes,
        }),
    })
}

/// The keeper form of an identity field selector.
pub fn metadata_field_to_keeper(f: i32) -> kp::ActorMetadataField {
    match pb::ActorMetadataField::try_from(f) {
        Ok(pb::ActorMetadataField::Name) => kp::ActorMetadataField::Name,
        Ok(pb::ActorMetadataField::Hive) => kp::ActorMetadataField::Hive,
        Ok(pb::ActorMetadataField::Uid) => kp::ActorMetadataField::Uid,
        _ => kp::ActorMetadataField::Unspecified,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fidelity_round_trips() {
        for f in [
            pb::SnapshotFidelity::Volumes,
            pb::SnapshotFidelity::Rootfs,
            pb::SnapshotFidelity::Memory,
        ] {
            assert_eq!(fidelity_from_keeper(fidelity_to_keeper(f as i32) as i32), f);
        }
        assert_eq!(fidelity_to_keeper(0), kp::SnapshotFidelity::Unspecified);
        assert_eq!(fidelity_to_keeper(99), kp::SnapshotFidelity::Unspecified);
    }

    #[test]
    fn reports_and_resources() {
        assert!(snapshot_report_from_keeper(Some(&kp::SnapshotReport::default())).is_none());
        let r = snapshot_report_from_keeper(Some(&kp::SnapshotReport {
            snapshot: Some(kp::Snapshot {
                snapshot_uuid: "u".into(),
                generation: 3,
                fidelity: kp::SnapshotFidelity::Rootfs as i32,
                object: Some(kp::ObjectStorage {
                    snapshot_uri: "gs://b/x".into(),
                    sharing: 0,
                }),
            }),
            ..Default::default()
        }))
        .unwrap();
        assert_eq!(
            (r.generation, r.fidelity),
            (3, pb::SnapshotFidelity::Rootfs as i32)
        );
        assert!(resources_to_keeper(Some(&pb::ResourceRequirements::default())).is_none());
    }
}

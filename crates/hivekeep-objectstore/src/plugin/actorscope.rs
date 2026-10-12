//! The object-storage prefixes an actor's transfers may touch, derived from
//! the actor record, with the access each needs. No storage provider is
//! known here: a provider's credential exchange turns the grants into
//! whatever scoped credential its storage supports.

use std::fmt;

use hivekeep_core::snapshot::{SnapshotOwner, SnapshotUri, StoragePrefix};
use hivekeep_proto::api::v1 as pb;

/// What a grant permits under its prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Access {
    /// The tag or golden snapshot the actor was created from.
    ReadOnly,
    /// The actor's own snapshots.
    ReadWrite,
}

impl fmt::Display for Access {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Access::ReadOnly => "ro",
            Access::ReadWrite => "rw",
        })
    }
}

/// One prefix and the access allowed under it, as the control plane built
/// the prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Grant {
    /// The prefix.
    pub prefix: StoragePrefix,
    /// The access.
    pub access: Access,
}

/// Derives the grants from the actor's snapshot generations.
///
/// - Every durable URI names the storage location it sits under; for each
///   location the actor gets ReadWrite on its own prefix there, so the
///   grant exists from the first generation on and a repointed actor keeps
///   both locations.
/// - Each tag-owned generation with a completed durable URI grants ReadOnly
///   on the tag's prefix: the golden or tag the actor was created from.
///
/// An owner this code does not know is an error, so a new kind is never
/// silently dropped. Local-only generations contribute nothing. ReadWrite
/// grants come first, then by prefix, so equal inputs give equal output.
pub fn from_actor(actor: &pb::Actor) -> Result<Vec<Grant>, String> {
    let meta = actor.metadata.clone().unwrap_or_default();
    let me = SnapshotOwner::actor(meta.hive.clone(), meta.uid.clone());
    let mut grants: Vec<Grant> = Vec::new();
    let mut add = |g: Grant| {
        if !grants.contains(&g) {
            grants.push(g);
        }
    };
    let status = actor.status.clone().unwrap_or_default();
    for generation in &status.snapshots {
        let Some(durable) = &generation.durable_snapshot else {
            continue;
        };
        let uri = durable
            .object
            .as_ref()
            .map(|o| o.snapshot_uri.as_str())
            .unwrap_or("");
        if uri.is_empty() {
            continue;
        }
        let what = |e: String| {
            format!(
                "snapshot generation {} of actor {}/{}: {e}",
                generation.generation, meta.hive, meta.name
            )
        };
        let parsed = SnapshotUri::parse(uri).map_err(|e| what(e.to_string()))?;
        let own = me
            .prefix(parsed.location())
            .map_err(|e| what(e.to_string()))?;
        add(Grant {
            prefix: own,
            access: Access::ReadWrite,
        });
        match pb::SnapshotOwner::try_from(generation.owner) {
            Ok(pb::SnapshotOwner::Actor) => {}
            Ok(pb::SnapshotOwner::Tag) => {
                if durable.state != pb::SnapshotStorageState::Completed as i32 {
                    continue;
                }
                add(Grant {
                    prefix: parsed.owner_prefix(),
                    access: Access::ReadOnly,
                });
            }
            _ => {
                return Err(format!(
                    "snapshot generation {} of actor {}/{} has unknown owner {}",
                    generation.generation, meta.hive, meta.name, generation.owner
                ))
            }
        }
    }
    grants.sort_by(|a, b| {
        b.access
            .cmp(&a.access)
            .then_with(|| a.prefix.as_str().cmp(b.prefix.as_str()))
    });
    Ok(grants)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub const LOC: &str = "gs://snaps/hivekeep-demo";
    pub const HIVE: &str = "team-a";
    pub const UID: &str = "9f1e0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a";
    pub const TAG_UID: &str = "7a3c0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a";

    pub fn actor_uri(location: &str, name: &str) -> String {
        format!("{location}/hives/{HIVE}/actors/{UID}/snapshots/{name}")
    }

    pub fn tag_uri() -> String {
        format!("{LOC}/hives/hivekeep-golden/tags/{TAG_UID}")
    }

    pub fn generation(
        n: i32,
        owner: pb::SnapshotOwner,
        state: pb::SnapshotStorageState,
        uri: &str,
    ) -> pb::Snapshot {
        let mut s = pb::Snapshot {
            generation: n,
            owner: owner as i32,
            ..Default::default()
        };
        if uri.is_empty() {
            s.locality = "node-1".into();
        } else {
            s.durable_snapshot = Some(pb::SnapshotStorage {
                state: state as i32,
                object: Some(pb::ObjectSnapshot {
                    snapshot_uri: uri.into(),
                }),
            });
        }
        s
    }

    pub fn actor_with(snaps: Vec<pb::Snapshot>) -> pb::Actor {
        pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: HIVE.into(),
                name: "bot".into(),
                uid: UID.into(),
                ..Default::default()
            }),
            status: Some(pb::ActorStatus {
                snapshots: snaps,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn prefix(uri: &str) -> StoragePrefix {
        StoragePrefix::parse(uri).unwrap()
    }

    fn actor_grant() -> Grant {
        Grant {
            prefix: prefix(&format!("{LOC}/hives/{HIVE}/actors/{UID}")),
            access: Access::ReadWrite,
        }
    }

    fn tag_grant() -> Grant {
        Grant {
            prefix: prefix(&tag_uri()),
            access: Access::ReadOnly,
        }
    }

    use pb::SnapshotOwner::{Actor as OwnerActor, Tag as OwnerTag};
    use pb::SnapshotStorageState::{Completed, Pending};

    #[test]
    fn from_actor_cases() {
        let cases: Vec<(&str, pb::Actor, Vec<Grant>)> = vec![
            ("no generations", actor_with(vec![]), vec![]),
            (
                "tag only, before the first suspend",
                actor_with(vec![generation(1, OwnerTag, Completed, &tag_uri())]),
                vec![actor_grant(), tag_grant()],
            ),
            (
                "tag plus pending actor generation, during the first suspend",
                actor_with(vec![
                    generation(1, OwnerTag, Completed, &tag_uri()),
                    generation(2, OwnerActor, Pending, &actor_uri(LOC, "c0de")),
                ]),
                vec![actor_grant(), tag_grant()],
            ),
            (
                "actor only, after the first suspend",
                actor_with(vec![generation(
                    2,
                    OwnerActor,
                    Completed,
                    &actor_uri(LOC, "c0de"),
                )]),
                vec![actor_grant()],
            ),
            (
                "local-only generation adds nothing",
                actor_with(vec![
                    generation(2, OwnerActor, Completed, &actor_uri(LOC, "c0de")),
                    generation(3, OwnerActor, Completed, ""),
                ]),
                vec![actor_grant()],
            ),
            (
                "two actor generations under one prefix give one grant",
                actor_with(vec![
                    generation(2, OwnerActor, Completed, &actor_uri(LOC, "c0de")),
                    generation(3, OwnerActor, Pending, &actor_uri(LOC, "d00d")),
                ]),
                vec![actor_grant()],
            ),
            (
                "a repointed actor keeps both locations",
                actor_with(vec![
                    generation(2, OwnerActor, Completed, &actor_uri(LOC, "c0de")),
                    generation(
                        3,
                        OwnerActor,
                        Pending,
                        &actor_uri("gs://other-bucket/root", "d00d"),
                    ),
                ]),
                vec![
                    Grant {
                        prefix: prefix(&format!(
                            "gs://other-bucket/root/hives/{HIVE}/actors/{UID}"
                        )),
                        access: Access::ReadWrite,
                    },
                    actor_grant(),
                ],
            ),
            (
                "a pending tag copy is not yet readable, but names the location",
                actor_with(vec![generation(1, OwnerTag, Pending, &tag_uri())]),
                vec![actor_grant()],
            ),
        ];
        for (name, actor, want) in cases {
            assert_eq!(from_actor(&actor).unwrap(), want, "{name}");
        }
    }

    #[test]
    fn from_actor_refuses_unknown_owner() {
        let a = actor_with(vec![generation(
            1,
            pb::SnapshotOwner::Unspecified,
            Completed,
            &tag_uri(),
        )]);
        assert!(from_actor(&a).is_err());
    }

    #[test]
    fn from_actor_refuses_malformed_uri() {
        let a = actor_with(vec![generation(
            2,
            OwnerActor,
            Completed,
            "gs://snaps/not/a/snapshot",
        )]);
        assert!(from_actor(&a).is_err());
    }
}

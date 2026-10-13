//! One snapshot of an actor, as the tracker records it.

use chrono::{DateTime, Utc};
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use serde::{Deserialize, Serialize};

/// A snapshot of an actor the keeper holds or has uploaded.
#[derive(Clone, PartialEq)]
pub struct SnapshotRef {
    /// The snapshot as the control plane named it.
    pub snapshot: pb::Snapshot,
    /// When the checkpoint that produced it committed on this node; `None`
    /// when this keeper did not take it.
    pub commit_time: Option<DateTime<Utc>>,
    /// When this keeper committed its manifest to object storage; only
    /// meaningful for a durable snapshot.
    pub upload_time: Option<DateTime<Utc>>,
    /// The actor it belongs to, so a background upload acts for the actor
    /// as an RPC for it would.
    pub actor: ActorRef,
}

impl std::fmt::Debug for SnapshotRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotRef")
            .field("uuid", &self.uuid())
            .field("generation", &self.generation())
            .field("commit_time", &self.commit_time)
            .field("upload_time", &self.upload_time)
            .field("actor", &self.actor.to_string())
            .finish()
    }
}

impl SnapshotRef {
    /// A reference to `snapshot` of `actor`.
    pub fn new(snapshot: pb::Snapshot, actor: ActorRef) -> Self {
        SnapshotRef {
            snapshot,
            commit_time: None,
            upload_time: None,
            actor,
        }
    }

    /// The snapshot's UUID.
    pub fn uuid(&self) -> &str {
        &self.snapshot.snapshot_uuid
    }

    /// The snapshot's object storage URI.
    pub fn uri(&self) -> &str {
        self.snapshot
            .object
            .as_ref()
            .map(|o| o.snapshot_uri.as_str())
            .unwrap_or("")
    }

    /// The snapshot's generation.
    pub fn generation(&self) -> i32 {
        self.snapshot.generation
    }

    /// The snapshot's fidelity.
    pub fn fidelity(&self) -> i32 {
        self.snapshot.fidelity
    }

    /// The snapshot as a field of `ActorSnapshotState`.
    pub fn report(&self) -> pb::SnapshotReport {
        pb::SnapshotReport {
            snapshot: Some(self.snapshot.clone()),
            local_commit_time: self.commit_time.map(timestamp),
            durable_complete_time: self.upload_time.map(timestamp),
        }
    }
}

/// Whether `a` and `b` are both set and name the same UUID.
pub fn same_uuid(a: Option<&SnapshotRef>, b: Option<&SnapshotRef>) -> bool {
    matches!((a, b), (Some(a), Some(b)) if a.uuid() == b.uuid())
}

/// A wall-clock time as a protobuf timestamp.
pub fn timestamp(t: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: t.timestamp(),
        nanos: t.timestamp_subsec_nanos() as i32,
    }
}

/// The JSON form of a snapshot in the state file, keeping every field of
/// the wire message under its proto JSON name.
#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct SnapshotJson {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    snapshot_uuid: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    generation: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    fidelity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    object: Option<ObjectJson>,
}

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ObjectJson {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    snapshot_uri: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    sharing: String,
}

fn is_zero(v: &i32) -> bool {
    *v == 0
}

/// The JSON form of a [`SnapshotRef`] in the state file.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RefJson {
    snapshot: SnapshotJson,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    commit_time: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    upload_time: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    hive: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    actor_name: String,
}

fn fidelity_name(v: i32) -> String {
    match pb::SnapshotFidelity::try_from(v) {
        Ok(pb::SnapshotFidelity::Unspecified) => String::new(),
        Ok(f) => f.as_str_name().to_string(),
        Err(_) => v.to_string(),
    }
}

fn fidelity_value(s: &str) -> Result<i32, String> {
    if s.is_empty() {
        return Ok(0);
    }
    if let Some(f) = pb::SnapshotFidelity::from_str_name(s) {
        return Ok(f as i32);
    }
    s.parse().map_err(|_| format!("unknown fidelity {s:?}"))
}

fn sharing_name(v: i32) -> String {
    match pb::SnapshotSharing::try_from(v) {
        Ok(pb::SnapshotSharing::Unspecified) => String::new(),
        Ok(f) => f.as_str_name().to_string(),
        Err(_) => v.to_string(),
    }
}

fn sharing_value(s: &str) -> Result<i32, String> {
    if s.is_empty() {
        return Ok(0);
    }
    if let Some(f) = pb::SnapshotSharing::from_str_name(s) {
        return Ok(f as i32);
    }
    s.parse().map_err(|_| format!("unknown sharing {s:?}"))
}

impl Serialize for SnapshotRef {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let snap = &self.snapshot;
        RefJson {
            snapshot: SnapshotJson {
                snapshot_uuid: snap.snapshot_uuid.clone(),
                generation: snap.generation,
                fidelity: fidelity_name(snap.fidelity),
                object: snap.object.as_ref().map(|o| ObjectJson {
                    snapshot_uri: o.snapshot_uri.clone(),
                    sharing: sharing_name(o.sharing),
                }),
            },
            commit_time: self.commit_time,
            upload_time: self.upload_time,
            hive: self.actor.hive.clone(),
            actor_name: self.actor.name.clone(),
        }
        .serialize(s)
    }
}

impl<'de> Deserialize<'de> for SnapshotRef {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let r = RefJson::deserialize(d)?;
        let object = match r.snapshot.object {
            Some(o) => Some(pb::ObjectStorage {
                snapshot_uri: o.snapshot_uri,
                sharing: sharing_value(&o.sharing).map_err(serde::de::Error::custom)?,
            }),
            None => None,
        };
        Ok(SnapshotRef {
            snapshot: pb::Snapshot {
                snapshot_uuid: r.snapshot.snapshot_uuid,
                generation: r.snapshot.generation,
                fidelity: fidelity_value(&r.snapshot.fidelity).map_err(serde::de::Error::custom)?,
                object,
            },
            commit_time: r.commit_time,
            upload_time: r.upload_time,
            actor: ActorRef::new(r.hive, r.actor_name),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_round_trip_keeps_every_field() {
        let r = SnapshotRef {
            snapshot: pb::Snapshot {
                snapshot_uuid: "s1".into(),
                generation: 3,
                fidelity: pb::SnapshotFidelity::Memory as i32,
                object: Some(pb::ObjectStorage {
                    snapshot_uri: "gs://b/hives/h/actors/u/snapshots/s1".into(),
                    sharing: pb::SnapshotSharing::Shared as i32,
                }),
            },
            commit_time: Some(DateTime::from_timestamp(1_700_000_000, 5).unwrap()),
            upload_time: None,
            actor: ActorRef::new("h", "a"),
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("SNAPSHOT_FIDELITY_MEMORY"), "{json}");
        assert!(!json.contains("uploadTime"));
        let back: SnapshotRef = serde_json::from_str(&json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn report_sets_times_only_when_known() {
        let r = SnapshotRef::new(pb::Snapshot::default(), ActorRef::new("h", "a"));
        let rep = r.report();
        assert!(rep.local_commit_time.is_none() && rep.durable_complete_time.is_none());
    }
}

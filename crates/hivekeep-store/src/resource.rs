//! The stored resource kinds and the rules every backend applies when
//! writing them: server-owned metadata, immutable fields and snapshot URI
//! checks.

use std::time::SystemTime;

use hivekeep_core::defaults::Defaults;
use hivekeep_core::snapshot::SnapshotUri;
use hivekeep_proto::api::v1 as pb;
use prost_types::Timestamp;

use crate::error::{Result, StoreError};

/// A read-modify-write closure: edits the stored object in place, or refuses
/// the write by returning an error.
pub type Mutate<'a, T> = &'a mut (dyn FnMut(&mut T) -> Result<()> + Send);

/// A message the store persists, addressed by its [`pb::ResourceMetadata`].
pub trait Resource: prost::Message + Clone + Default + PartialEq + Send + Sync + 'static {
    /// Kind name for messages, e.g. `"actor"`.
    const KIND: &'static str;

    /// The resource's metadata, if set.
    fn metadata(&self) -> Option<&pb::ResourceMetadata>;

    /// The resource's metadata, created empty if unset.
    fn metadata_mut(&mut self) -> &mut pb::ResourceMetadata;

    /// Fills defaults into a record read back from storage, so records
    /// written before a default existed read the same as new ones.
    fn backfill(&mut self) {}

    /// Kind-specific checks on a write. `before` is the stored object for
    /// an update and `None` for a create.
    fn check_write(&self, _before: Option<&Self>) -> Result<()> {
        Ok(())
    }

    /// The stored uid, or empty.
    fn uid(&self) -> &str {
        self.metadata().map(|m| m.uid.as_str()).unwrap_or("")
    }

    /// The stored version, or zero.
    fn version(&self) -> i64 {
        self.metadata().map(|m| m.version).unwrap_or(0)
    }

    /// The name, or empty.
    fn name(&self) -> &str {
        self.metadata().map(|m| m.name.as_str()).unwrap_or("")
    }

    /// The hive, or empty.
    fn hive(&self) -> &str {
        self.metadata().map(|m| m.hive.as_str()).unwrap_or("")
    }
}

macro_rules! resource {
    ($t:ident, $kind:literal { $($body:tt)* }) => {
        impl Resource for pb::$t {
            const KIND: &'static str = $kind;
            fn metadata(&self) -> Option<&pb::ResourceMetadata> {
                self.metadata.as_ref()
            }
            fn metadata_mut(&mut self) -> &mut pb::ResourceMetadata {
                self.metadata.get_or_insert_with(Default::default)
            }
            $($body)*
        }
    };
    ($t:ident, $kind:literal) => {
        resource!($t, $kind {
            fn backfill(&mut self) {
                Defaults::apply_defaults(self)
            }
        });
    };
}

resource!(Hive, "hive");
resource!(EgressPolicy, "egress policy");
resource!(AccessPolicy, "access policy");
resource!(Worker, "worker");
resource!(ActorAssignment, "actor assignment" {});

resource!(Actor, "actor" {
    fn backfill(&mut self) {
        Defaults::apply_defaults(self)
    }
    fn check_write(&self, _before: Option<&Self>) -> Result<()> {
        let snaps = self.status.as_ref().map(|s| s.snapshots.as_slice()).unwrap_or(&[]);
        check_snapshot_uris(Self::KIND, self.name(), snaps.iter())
    }
});

resource!(Tag, "tag" {
    fn backfill(&mut self) {
        Defaults::apply_defaults(self)
    }
    fn check_write(&self, before: Option<&Self>) -> Result<()> {
        if let Some(before) = before {
            check_tag_mutation(before, self)?;
        }
        let snap = self.status.as_ref().and_then(|s| s.snapshot.as_ref());
        check_snapshot_uris(Self::KIND, self.name(), snap.into_iter())
    }
});

resource!(ActorTemplate, "actor template" {
    fn backfill(&mut self) {
        Defaults::apply_defaults(self)
    }
    fn check_write(&self, before: Option<&Self>) -> Result<()> {
        match before {
            Some(before) => check_identity_unchanged(before, self),
            None => Ok(()),
        }
    }
});

/// The current time as a protobuf timestamp.
pub fn now() -> Timestamp {
    Timestamp::from(SystemTime::now())
}

/// Stamps the server-owned metadata of a new object: a fresh uid, version 1,
/// create and update time now, and no delete time.
pub fn stamp_create<T: Resource>(msg: &mut T) {
    let md = msg.metadata_mut();
    md.uid = uuid::Uuid::new_v4().to_string();
    md.version = 1;
    let t = now();
    md.create_time = Some(t);
    md.update_time = Some(t);
    md.delete_time = None;
}

/// Restores the server-owned metadata of an updated object from `old`,
/// discarding whatever the mutation did to it, and advances the revision.
/// With `mark_for_deletion`, an unset delete time becomes the write's
/// update time; an existing one is kept.
pub fn stamp_update<T: Resource>(msg: &mut T, old: &pb::ResourceMetadata, mark_for_deletion: bool) {
    let md = msg.metadata_mut();
    md.hive = old.hive.clone();
    md.name = old.name.clone();
    md.uid = old.uid.clone();
    md.version = old.version + 1;
    md.create_time = old.create_time;
    md.update_time = Some(now());
    md.delete_time = old.delete_time;
    if mark_for_deletion && md.delete_time.is_none() {
        md.delete_time = md.update_time;
    }
}

/// Builds the new stored object for a create: a copy of `input` with
/// server-owned metadata, checked by the kind's write rules.
pub fn prepare_create<T: Resource>(input: &T) -> Result<T> {
    let mut msg = input.clone();
    stamp_create(&mut msg);
    msg.check_write(None)?;
    Ok(msg)
}

/// Runs a read-modify-write against `stored`: checks `guard`, applies
/// `mutate`, checks the kind's write rules and stamps the next revision.
/// Returns the object to write, or the first error, in which case nothing
/// may be written.
pub fn prepare_update<T: Resource>(
    stored: &T,
    guard: &crate::DeletePreconditions,
    mutate: Option<Mutate<'_, T>>,
    mark_for_deletion: bool,
) -> Result<T> {
    guard.check(stored.metadata())?;
    let old = stored.metadata().cloned().unwrap_or_default();
    let mut msg = stored.clone();
    if let Some(mutate) = mutate {
        mutate(&mut msg)?;
    }
    msg.check_write(Some(stored))?;
    stamp_update(&mut msg, &old, mark_for_deletion);
    Ok(msg)
}

/// Rejects snapshots whose durable copy names a URI outside the storage
/// layout.
pub fn check_snapshot_uris<'a>(
    kind: &str,
    name: &str,
    snapshots: impl Iterator<Item = &'a pb::Snapshot>,
) -> Result<()> {
    for snap in snapshots {
        let Some(durable) = &snap.durable_snapshot else {
            continue;
        };
        let uri = durable
            .object
            .as_ref()
            .map(|o| o.snapshot_uri.as_str())
            .unwrap_or("");
        if let Err(e) = SnapshotUri::parse(uri) {
            return Err(StoreError::InvalidArgument(format!(
                "{kind} {name} snapshot {}: {e}",
                snap.uuid
            )));
        }
    }
    Ok(())
}

fn check_identity_unchanged<T: Resource>(stored: &T, mutated: &T) -> Result<()> {
    if stored.hive() != mutated.hive() {
        return Err(StoreError::ImmutableField(format!(
            "metadata.hive changed from {:?} to {:?}",
            stored.hive(),
            mutated.hive()
        )));
    }
    if stored.name() != mutated.name() {
        return Err(StoreError::ImmutableField(format!(
            "metadata.name changed from {:?} to {:?}",
            stored.name(),
            mutated.name()
        )));
    }
    Ok(())
}

/// The one change a Tag's snapshot allows after it is recorded: its durable
/// copy going from PENDING to COMPLETED with nothing else touched.
fn tag_snapshot_transition_allowed(
    stored: Option<&pb::Snapshot>,
    mutated: Option<&pb::Snapshot>,
) -> bool {
    let Some(stored) = stored else {
        return true;
    };
    let Some(mutated) = mutated else {
        return false;
    };
    if stored == mutated {
        return true;
    }
    let (Some(sd), Some(md)) = (&stored.durable_snapshot, &mutated.durable_snapshot) else {
        return false;
    };
    if sd.state != pb::SnapshotStorageState::Pending as i32
        || md.state != pb::SnapshotStorageState::Completed as i32
    {
        return false;
    }
    let mut expected = stored.clone();
    if let Some(d) = &mut expected.durable_snapshot {
        d.state = pb::SnapshotStorageState::Completed as i32;
    }
    &expected == mutated
}

fn check_tag_mutation(stored: &pb::Tag, mutated: &pb::Tag) -> Result<()> {
    check_identity_unchanged(stored, mutated)?;
    let snap = |t: &pb::Tag| t.status.as_ref().and_then(|s| s.snapshot.clone());
    let (s, m) = (snap(stored), snap(mutated));
    if !tag_snapshot_transition_allowed(s.as_ref(), m.as_ref()) {
        return Err(StoreError::ImmutableField(
            "status.snapshot is immutable once set".into(),
        ));
    }
    let tmpl = |t: &pb::Tag| {
        t.status
            .as_ref()
            .map(|s| s.actor_template_uid.clone())
            .unwrap_or_default()
    };
    if tmpl(stored) != tmpl(mutated) {
        return Err(StoreError::ImmutableField(format!(
            "status.actor_template_uid changed from {:?} to {:?}",
            tmpl(stored),
            tmpl(mutated)
        )));
    }
    Ok(())
}

/// Checks that a record's key columns agree with the metadata it carries.
pub fn check_projection<T: Resource>(msg: &T, uid: &str, version: i64) -> Result<()> {
    if msg.uid() != uid || msg.version() != version {
        return Err(StoreError::Corrupt(format!(
            "{} {}: columns (uid {uid:?}, version {version}) disagree with the record (uid {:?}, version {})",
            T::KIND,
            msg.name(),
            msg.uid(),
            msg.version()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending() -> pb::Snapshot {
        pb::Snapshot {
            generation: 1,
            durable_snapshot: Some(pb::SnapshotStorage {
                state: pb::SnapshotStorageState::Pending as i32,
                object: Some(pb::ObjectSnapshot {
                    snapshot_uri: "gs://b/hives/h/tags/t".into(),
                }),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn tag_snapshot_transitions() {
        let p = pending();
        let mut done = p.clone();
        done.durable_snapshot.as_mut().unwrap().state = pb::SnapshotStorageState::Completed as i32;
        assert!(tag_snapshot_transition_allowed(None, Some(&p)));
        assert!(tag_snapshot_transition_allowed(Some(&p), Some(&p)));
        assert!(tag_snapshot_transition_allowed(Some(&p), Some(&done)));
        assert!(!tag_snapshot_transition_allowed(Some(&done), Some(&p)));
        assert!(!tag_snapshot_transition_allowed(Some(&p), None));
        let mut moved = done.clone();
        moved.generation = 2;
        assert!(!tag_snapshot_transition_allowed(Some(&p), Some(&moved)));
    }

    #[test]
    fn update_discards_metadata_edits() {
        let mut actor = pb::Actor::default();
        actor.metadata_mut().name = "a".into();
        stamp_create(&mut actor);
        let before = actor.clone();
        let mut edit = |a: &mut pb::Actor| {
            let md = a.metadata_mut();
            md.uid = "client".into();
            md.version = 99;
            md.name = "renamed".into();
            md.delete_time = Some(now());
            Ok(())
        };
        let out = prepare_update(&before, &Default::default(), Some(&mut edit), false).unwrap();
        let md = out.metadata().unwrap();
        assert_eq!(md.uid, before.uid());
        assert_eq!(md.version, 2);
        assert_eq!(md.name, "a");
        assert_eq!(md.delete_time, None);
        assert_eq!(md.create_time, before.metadata().unwrap().create_time);
    }
}

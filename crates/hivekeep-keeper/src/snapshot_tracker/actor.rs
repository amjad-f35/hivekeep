//! What the tracker knows of one actor's snapshots, and the rules over it.

use std::time::Duration;

use chrono::{DateTime, Utc};
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use serde::{Deserialize, Serialize};
use tonic::Status;

use super::op::{ActorOp, OpKind};
use super::snapshot_ref::{same_uuid, SnapshotRef};
use crate::clock;

/// The part of an actor's entry kept in its snapshot state file.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorRecord {
    /// The newest snapshot stored in the actor's local checkpoint dir.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_local: Option<SnapshotRef>,
    /// The snapshot a background upload has begun making durable. It stays
    /// set across checkpoints, retries and restarts until the upload
    /// succeeds, a newer local snapshot replaces it after a failure, or a
    /// newer snapshot becomes durable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uploading: Option<SnapshotRef>,
    /// The newest snapshot confirmed in object storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_durable: Option<SnapshotRef>,
    /// What the upload delay counts from: the commit time of the oldest
    /// local checkpoint since the last durable snapshot, or for one taken
    /// while an upload ran, when that upload succeeded. Counting from here
    /// rather than the latest commit keeps an actor that suspends and
    /// resumes often from postponing durability forever.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub undurable_since: Option<DateTime<Utc>>,
    /// When the keeper next checks whether to detach the actor; `None`
    /// means no detach is scheduled. A commit schedules one, a failed
    /// detach reschedules it, and a Run or Restore, a detach, a newer
    /// durable snapshot, or a Terminate clears it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detach_at: Option<DateTime<Utc>>,
    /// Set when a Run or Restore begins, cleared when a Checkpoint commits.
    /// While unset, the actor's directories still belong to that
    /// Checkpoint's cleanup.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub started: bool,
    /// When the latest change the keeper owes the control plane a report of
    /// finished; `None` once none is due.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report_due_since: Option<DateTime<Utc>>,
}

/// The RPC in progress on an actor. RPCs are serialized per actor, so at
/// most one holds an actor at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Hold {
    /// None.
    #[default]
    None,
    /// Run or Restore.
    Start,
    /// Checkpoint under deferred upload.
    Checkpoint,
    /// Checkpoint under immediate upload, or PromoteSnapshot.
    Durable,
    /// Terminate.
    Terminate,
}

impl Hold {
    /// Whether no background upload starts while this RPC holds the actor:
    /// one that makes a snapshot durable would only cancel it, and a
    /// Terminate forgets the actor.
    pub fn blocks_uploads(self) -> bool {
        matches!(self, Hold::Durable | Hold::Terminate)
    }
}

/// One actor's entry: its saved record plus what lives only in memory.
#[derive(Debug, Clone)]
pub struct ActorSnapshots {
    /// Tells entries apart, so an RPC ending can see Forget dropped its.
    pub(crate) id: u64,
    /// The actor's UID.
    pub actor_uid: String,
    /// The saved part.
    pub rec: ActorRecord,
    /// The actor's upload or detach in progress, if any.
    pub(crate) op: Option<ActorOp>,
    /// The RPC in progress.
    pub(crate) hold: Hold,
    /// A report held back by `hold`, queued once it ends.
    pub(crate) report_deferred: bool,
    /// Holds the next background work back after a failure.
    pub(crate) retry_at: Option<DateTime<Utc>>,
    /// Set after a background upload of `uploading` failed and it was kept
    /// to retry.
    pub(crate) uploading_failed: bool,
    /// Set while the state file lacks a change because writing it failed.
    pub(crate) unsaved: bool,
}

impl ActorSnapshots {
    pub(crate) fn new(id: u64, actor_uid: &str, rec: ActorRecord) -> Self {
        ActorSnapshots {
            id,
            actor_uid: actor_uid.to_string(),
            rec,
            op: None,
            hold: Hold::None,
            report_deferred: false,
            retry_at: None,
            uploading_failed: false,
            unsaved: false,
        }
    }

    /// Whether a Checkpoint of `snap` committed it already, as LatestLocal
    /// or LatestDurable. Fails with FailedPrecondition when one of them has
    /// `snap`'s UUID but differs otherwise: the control plane named one
    /// snapshot two ways.
    pub fn checkpoint_committed(&self, snap: &pb::Snapshot) -> Result<bool, Status> {
        for r in [
            self.rec.latest_local.as_ref(),
            self.rec.latest_durable.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if r.uuid() != snap.snapshot_uuid {
                continue;
            }
            let uri = snap
                .object
                .as_ref()
                .map(|o| o.snapshot_uri.as_str())
                .unwrap_or("");
            if r.generation() != snap.generation || r.fidelity() != snap.fidelity || r.uri() != uri
            {
                return Err(Status::failed_precondition(format!(
                    "snapshot {} is committed as generation {} with fidelity {} at {:?}, but requested as generation {} with fidelity {} at {:?}",
                    r.uuid(),
                    r.generation(),
                    fidelity_name(r.fidelity()),
                    r.uri(),
                    snap.generation,
                    fidelity_name(snap.fidelity),
                    uri
                )));
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// Whether CommitLocal should record `snap`: LatestLocal holds neither
    /// it nor a newer snapshot, and no other committed snapshot is newer or
    /// has its generation under another UUID.
    pub fn local_needed(&self, snap: &SnapshotRef) -> bool {
        if let Some(l) = &self.rec.latest_local {
            if l.generation() >= snap.generation() {
                return false;
            }
        }
        for r in [
            self.rec.uploading.as_ref(),
            self.rec.latest_durable.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if r.generation() > snap.generation() {
                return false;
            }
            if r.generation() == snap.generation() && !same_uuid(Some(r), Some(snap)) {
                return false;
            }
        }
        true
    }

    /// Whether a report of the actor's state is owed.
    pub fn report_due(&self) -> bool {
        self.rec.report_due_since.is_some()
    }

    /// The actor LatestDurable belongs to.
    pub fn latest_durable_actor(&self) -> ActorRef {
        match &self.rec.latest_durable {
            Some(d) => d.actor.clone(),
            None => ActorRef::new("", ""),
        }
    }

    /// Whether `r` still needs uploading: neither it nor a newer snapshot
    /// is durable.
    pub fn upload_needed(&self, r: &SnapshotRef) -> bool {
        match &self.rec.latest_durable {
            None => true,
            Some(d) => d.generation() < r.generation(),
        }
    }

    /// Whether `r` is set, still needs uploading, and names a location to
    /// upload it to.
    pub fn can_upload(&self, r: Option<&SnapshotRef>) -> bool {
        let Some(r) = r else { return false };
        if !self.upload_needed(r) || r.uri().is_empty() {
            return false;
        }
        self.is_latest_durable(r).is_ok()
    }

    /// Whether `r` is no longer kept as LatestLocal.
    pub fn local_superseded(&self, r: &SnapshotRef) -> bool {
        if same_uuid(self.rec.latest_local.as_ref(), Some(r)) {
            return false;
        }
        match &self.rec.latest_local {
            None => true,
            Some(l) => l.generation() > r.generation(),
        }
    }

    /// Whether Uploading is still in use: an upload runs it, or it can
    /// still be uploaded and has not failed with a newer uploadable local
    /// snapshot ready to replace it.
    pub fn keep_uploading(&self) -> bool {
        let Some(up) = &self.rec.uploading else {
            return false;
        };
        if self.running_upload().is_some() {
            return true;
        }
        if !self.can_upload(Some(up)) {
            return false;
        }
        !(self.uploading_failed
            && self.local_superseded(up)
            && self.can_upload(self.rec.latest_local.as_ref()))
    }

    /// Clears Uploading once it is no longer kept. Returns the local copy
    /// to delete, if any, and whether it cleared it.
    pub fn clear_superseded_uploading(&mut self) -> (Option<SnapshotRef>, bool) {
        if self.rec.uploading.is_none() || self.keep_uploading() {
            return (None, false);
        }
        let old = self.rec.uploading.take().expect("checked above");
        self.uploading_failed = false;
        if !self.local_superseded(&old) {
            return (None, true);
        }
        (Some(old), true)
    }

    /// The snapshot the next background upload makes durable: Uploading
    /// when no upload runs it and it is still kept, else LatestLocal when
    /// it can be uploaded and is not being uploaded already.
    pub fn next_upload_target(&self) -> Option<&SnapshotRef> {
        let running = self.running_upload();
        if self.keep_uploading() && running.is_none() {
            return self.rec.uploading.as_ref();
        }
        if !same_uuid(running, self.rec.latest_local.as_ref())
            && self.can_upload(self.rec.latest_local.as_ref())
        {
            return self.rec.latest_local.as_ref();
        }
        None
    }

    /// Whether `r` is LatestDurable. Fails with FailedPrecondition when
    /// LatestDurable has `r`'s UUID but differs otherwise.
    pub fn is_latest_durable(&self, r: &SnapshotRef) -> Result<bool, Status> {
        let Some(d) = &self.rec.latest_durable else {
            return Ok(false);
        };
        if d.uuid() != r.uuid() {
            return Ok(false);
        }
        if d.generation() != r.generation() || d.uri() != r.uri() {
            return Err(Status::failed_precondition(format!(
                "snapshot {} is durable as generation {} at {:?}, but requested as generation {} at {:?}",
                r.uuid(),
                d.generation(),
                d.uri(),
                r.generation(),
                r.uri()
            )));
        }
        if d.fidelity() != r.fidelity() {
            return Err(Status::failed_precondition(format!(
                "snapshot {} is durable with fidelity {}, but requested with fidelity {}",
                r.uuid(),
                fidelity_name(d.fidelity()),
                fidelity_name(r.fidelity())
            )));
        }
        Ok(true)
    }

    /// Records `d` as durable unless it or a newer snapshot already is.
    pub fn record_durable(&mut self, d: SnapshotRef) -> bool {
        if !self.upload_needed(&d) {
            return false;
        }
        self.rec.latest_durable = Some(d);
        true
    }

    /// The snapshot an upload is running on now.
    pub fn running_upload(&self) -> Option<&SnapshotRef> {
        match &self.op {
            Some(op) if op.kind == OpKind::Upload => Some(&op.target),
            _ => None,
        }
    }

    /// The upload in progress, when a snapshot newer than its target is
    /// durable.
    pub fn superseded_upload(&self) -> Option<ActorOp> {
        let up = self.running_upload()?;
        let d = self.rec.latest_durable.as_ref()?;
        if up.generation() >= d.generation() {
            return None;
        }
        self.op.clone()
    }

    /// Whether a Run or Restore in progress may use a superseded local
    /// snapshot: a Restore may read any local snapshot in place.
    pub fn local_in_use(&self) -> bool {
        self.hold == Hold::Start
    }

    /// The snapshot a detach is running on now.
    pub fn running_detach(&self) -> Option<&SnapshotRef> {
        match &self.op {
            Some(op) if op.kind == OpKind::Detach => Some(&op.target),
            _ => None,
        }
    }

    /// The actor's snapshot with the highest generation.
    pub fn newest(&self) -> Option<&SnapshotRef> {
        let mut newest: Option<&SnapshotRef> = None;
        for r in [
            self.rec.latest_local.as_ref(),
            self.rec.uploading.as_ref(),
            self.rec.latest_durable.as_ref(),
            self.op.as_ref().map(|o| &o.target),
        ]
        .into_iter()
        .flatten()
        {
            if newest.is_none_or(|n| r.generation() > n.generation()) {
                newest = Some(r);
            }
        }
        newest
    }

    /// Fails with Aborted when `snap`'s generation is stale: below the
    /// newest snapshot's, or equal to it under another UUID.
    pub fn check_generation(&self, snap: &pb::Snapshot) -> Result<(), Status> {
        let Some(n) = self.newest() else {
            return Ok(());
        };
        if snap.generation < n.generation() {
            return Err(Status::aborted(format!(
                "snapshot {:?} has generation {}, below the actor's newest snapshot {:?} at generation {}",
                snap.snapshot_uuid,
                snap.generation,
                n.uuid(),
                n.generation()
            )));
        }
        if snap.generation == n.generation() && snap.snapshot_uuid != n.uuid() {
            return Err(Status::aborted(format!(
                "snapshot {:?} has generation {}, which the actor's newest snapshot {:?} already holds",
                snap.snapshot_uuid,
                snap.generation,
                n.uuid()
            )));
        }
        Ok(())
    }

    /// When the next background upload is due: the upload delay after
    /// UndurableSince, or the retry time if later. `None` when nothing
    /// waits to upload.
    pub fn upload_due(&self, upload_delay: Duration) -> Option<DateTime<Utc>> {
        let target = self.next_upload_target()?;
        let since = self
            .rec
            .undurable_since
            .or(target.commit_time)
            .unwrap_or(DateTime::UNIX_EPOCH);
        Some(later(clock::add(since, upload_delay), self.retry_at))
    }

    /// Records an upload that succeeded at `now`. UndurableSince becomes
    /// unset, or `now` if a newer checkpoint still needs an upload.
    pub fn upload_succeeded(&mut self, now: DateTime<Utc>) {
        self.rec.undurable_since = None;
        if let Some(l) = &self.rec.latest_local {
            if self.upload_needed(l) {
                self.rec.undurable_since = Some(now);
            }
        }
    }

    /// Records `snap` as LatestLocal and schedules its detach at
    /// `detach_at`. LatestLocal never carries an upload time.
    pub fn set_latest_local(&mut self, snap: &SnapshotRef, detach_at: Option<DateTime<Utc>>) {
        self.rec.latest_local = Some(SnapshotRef {
            snapshot: snap.snapshot.clone(),
            commit_time: snap.commit_time,
            upload_time: None,
            actor: snap.actor.clone(),
        });
        self.rec.detach_at = detach_at;
    }

    /// Records that no local snapshot is held, which unschedules the detach.
    pub fn clear_latest_local(&mut self) {
        self.rec.latest_local = None;
        self.rec.detach_at = None;
    }

    /// Records that a Run or Restore begins: sets Started and unschedules
    /// the detach. Reports whether either changed.
    pub fn mark_started(&mut self) -> bool {
        if self.rec.started && self.rec.detach_at.is_none() {
            return false;
        }
        self.rec.started = true;
        self.rec.detach_at = None;
        true
    }

    /// Whether a detach is scheduled: a local snapshot is held and no Run or
    /// Restore has begun since it was committed.
    pub fn detach_scheduled(&self) -> bool {
        self.rec.latest_local.is_some() && self.rec.detach_at.is_some()
    }

    /// LatestLocal, which a detach makes durable, and whether it already
    /// is. Fails with FailedPrecondition when no detach can make it durable
    /// until a commit replaces it.
    pub fn detach_target(&self) -> Result<(SnapshotRef, bool), Status> {
        let target = self
            .rec
            .latest_local
            .clone()
            .ok_or_else(|| Status::failed_precondition("no local snapshot to detach"))?;
        let durable = self.is_latest_durable(&target)?;
        if !durable && target.uri().is_empty() {
            return Err(Status::failed_precondition(format!(
                "local snapshot {} names no object storage location to upload it to",
                target.uuid()
            )));
        }
        Ok((target, durable))
    }

    /// When the keeper next checks whether to detach: DetachAt, or the
    /// retry time if later. `None` when there is nothing a detach can do.
    pub fn detach_due(&self) -> Option<DateTime<Utc>> {
        if !self.detach_scheduled() || self.detach_target().is_err() {
            return None;
        }
        Some(later(self.rec.detach_at.expect("scheduled"), self.retry_at))
    }
}

/// The later of `t` and `u`.
pub fn later(t: DateTime<Utc>, u: Option<DateTime<Utc>>) -> DateTime<Utc> {
    match u {
        Some(u) if u > t => u,
        _ => t,
    }
}

fn fidelity_name(v: i32) -> &'static str {
    pb::SnapshotFidelity::try_from(v)
        .map(|f| f.as_str_name())
        .unwrap_or("SNAPSHOT_FIDELITY_UNKNOWN")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(uuid: &str, generation: i32) -> SnapshotRef {
        SnapshotRef {
            snapshot: pb::Snapshot {
                snapshot_uuid: uuid.into(),
                generation,
                fidelity: pb::SnapshotFidelity::Memory as i32,
                object: Some(pb::ObjectStorage {
                    snapshot_uri: format!("gs://b/hives/h/actors/u/snapshots/{uuid}"),
                    sharing: 0,
                }),
            },
            commit_time: Some(DateTime::from_timestamp(1000, 0).unwrap()),
            upload_time: None,
            actor: ActorRef::new("h", "a"),
        }
    }

    fn actor() -> ActorSnapshots {
        ActorSnapshots::new(1, "u", ActorRecord::default())
    }

    #[test]
    fn generation_checks() {
        let mut a = actor();
        a.rec.latest_local = Some(snap("s2", 2));
        assert!(a.check_generation(&snap("s3", 3).snapshot).is_ok());
        assert_eq!(
            a.check_generation(&snap("s1", 1).snapshot)
                .unwrap_err()
                .code(),
            tonic::Code::Aborted
        );
        assert_eq!(
            a.check_generation(&snap("x", 2).snapshot)
                .unwrap_err()
                .code(),
            tonic::Code::Aborted
        );
        assert!(a.check_generation(&snap("s2", 2).snapshot).is_ok());
    }

    #[test]
    fn committed_detects_conflicting_names() {
        let mut a = actor();
        a.rec.latest_local = Some(snap("s1", 1));
        assert!(a.checkpoint_committed(&snap("s1", 1).snapshot).unwrap());
        assert!(!a.checkpoint_committed(&snap("s2", 2).snapshot).unwrap());
        let mut other = snap("s1", 4).snapshot;
        other.generation = 4;
        assert_eq!(
            a.checkpoint_committed(&other).unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[test]
    fn local_needed_rules() {
        let mut a = actor();
        assert!(a.local_needed(&snap("s1", 1)));
        a.rec.latest_local = Some(snap("s1", 1));
        assert!(!a.local_needed(&snap("s1", 1)));
        assert!(a.local_needed(&snap("s2", 2)));
        a.rec.latest_durable = Some(snap("s3", 3));
        assert!(!a.local_needed(&snap("s2", 2)));
        assert!(a.local_needed(&snap("s3", 3)));
        assert!(!a.local_needed(&snap("other", 3)));
    }

    #[test]
    fn upload_due_counts_from_undurable_since() {
        let mut a = actor();
        a.rec.latest_local = Some(snap("s1", 1));
        let since = DateTime::from_timestamp(500, 0).unwrap();
        a.rec.undurable_since = Some(since);
        assert_eq!(
            a.upload_due(Duration::from_secs(60)),
            Some(DateTime::from_timestamp(560, 0).unwrap())
        );
        a.retry_at = Some(DateTime::from_timestamp(9999, 0).unwrap());
        assert_eq!(a.upload_due(Duration::from_secs(60)), a.retry_at);
        a.rec.latest_durable = Some(snap("s1", 1));
        assert_eq!(a.upload_due(Duration::from_secs(60)), None);
    }

    #[test]
    fn failed_uploading_gives_way_to_newer_local() {
        let mut a = actor();
        a.rec.uploading = Some(snap("s1", 1));
        a.rec.latest_local = Some(snap("s2", 2));
        assert!(a.keep_uploading());
        assert_eq!(a.next_upload_target().unwrap().uuid(), "s1");
        a.uploading_failed = true;
        assert!(!a.keep_uploading());
        assert_eq!(a.next_upload_target().unwrap().uuid(), "s2");
        let (removed, cleared) = a.clear_superseded_uploading();
        assert!(cleared);
        assert_eq!(removed.unwrap().uuid(), "s1");
    }

    #[test]
    fn detach_target_needs_a_location() {
        let mut a = actor();
        let mut s = snap("s1", 1);
        s.snapshot.object = None;
        a.rec.latest_local = Some(s);
        a.rec.detach_at = Some(DateTime::from_timestamp(10, 0).unwrap());
        assert!(a.detach_target().is_err());
        assert!(a.detach_due().is_none());
        a.rec.latest_local = Some(snap("s1", 1));
        assert!(!a.detach_target().unwrap().1);
        assert!(a.detach_due().is_some());
        assert!(a.mark_started());
        assert!(!a.mark_started());
        assert!(a.detach_due().is_none());
    }

    #[test]
    fn record_round_trips() {
        let mut a = actor();
        a.rec.latest_local = Some(snap("s1", 1));
        a.rec.started = true;
        let json = serde_json::to_string(&a.rec).unwrap();
        assert!(json.contains("latestLocal") && json.contains("\"started\":true"));
        let back: ActorRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back, a.rec);
        assert_eq!(
            serde_json::to_string(&ActorRecord::default()).unwrap(),
            "{}"
        );
    }
}

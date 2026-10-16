//! An Actor's snapshot records and how keeper reports change them.
//!
//! An Actor holds at most a few snapshot generations: the newest durable
//! one (COMPLETED in object storage), possibly a newer node-local one whose
//! upload is PENDING, and the one a suspend is taking. Keepers report uploads
//! that finished and detaches (node copies deleted); [`apply_latest_durable`]
//! and [`apply_detach`] fold those into the record.

use hivekeep_core::preview::{Gate, PreviewGates};
use hivekeep_proto::api::v1 as pb;

use crate::error::{status, Result};

const COMPLETED: i32 = pb::SnapshotStorageState::Completed as i32;
const PENDING: i32 = pb::SnapshotStorageState::Pending as i32;

/// The durable state of `s`, or UNSPECIFIED.
pub fn durable_state(s: &pb::Snapshot) -> i32 {
    s.durable_snapshot.as_ref().map(|d| d.state).unwrap_or(0)
}

/// The upload URI recorded on `s`, or empty.
pub fn durable_uri(s: &pb::Snapshot) -> &str {
    s.durable_snapshot
        .as_ref()
        .and_then(|d| d.object.as_ref())
        .map(|o| o.snapshot_uri.as_str())
        .unwrap_or("")
}

/// Whether `s` has a COMPLETED durable copy.
pub fn is_durable(s: &pb::Snapshot) -> bool {
    durable_state(s) == COMPLETED
}

fn snaps(status: Option<&pb::ActorStatus>) -> &[pb::Snapshot] {
    status.map(|s| s.snapshots.as_slice()).unwrap_or(&[])
}

/// The highest-generation snapshot matching `pred`.
pub fn latest(
    status: Option<&pb::ActorStatus>,
    pred: impl Fn(&pb::Snapshot) -> bool,
) -> Option<&pb::Snapshot> {
    let mut best: Option<&pb::Snapshot> = None;
    for s in snaps(status) {
        if pred(s) && best.is_none_or(|b| s.generation >= b.generation) {
            best = Some(s);
        }
    }
    best
}

/// The snapshot at `gen`.
pub fn by_generation(status: Option<&pb::ActorStatus>, gen: i32) -> Option<&pb::Snapshot> {
    snaps(status).iter().find(|s| s.generation == gen)
}

/// The snapshot at `gen`, mutable.
pub fn by_generation_mut(status: &mut pb::ActorStatus, gen: i32) -> Option<&mut pb::Snapshot> {
    status.snapshots.iter_mut().find(|s| s.generation == gen)
}

/// The snapshot at `last_assigned_generation`.
pub fn at_latest_generation(status: Option<&pb::ActorStatus>) -> Option<&pb::Snapshot> {
    by_generation(status, status?.last_assigned_generation)
}

/// The newest snapshot whose durable copy is in `state`.
pub fn latest_durable(
    status: Option<&pb::ActorStatus>,
    state: pb::SnapshotStorageState,
) -> Option<&pb::Snapshot> {
    latest(status, |s| durable_state(s) == state as i32)
}

/// The newest COMPLETED snapshot.
pub fn latest_completed(status: Option<&pb::ActorStatus>) -> Option<&pb::Snapshot> {
    latest_durable(status, pb::SnapshotStorageState::Completed)
}

/// The newest snapshot with a node-local copy.
pub fn latest_local(status: Option<&pb::ActorStatus>) -> Option<&pb::Snapshot> {
    latest(status, |s| !s.locality.is_empty())
}

/// Whether the latest generation has a node-local copy.
pub fn latest_is_local(status: Option<&pb::ActorStatus>) -> bool {
    at_latest_generation(status).is_some_and(|s| !s.locality.is_empty())
}

/// The snapshot a resume restores from: the newest one that is local or
/// durable (a borrowed Tag snapshot included). `None` means a cold boot.
pub fn to_resume(status: Option<&pb::ActorStatus>) -> Option<&pb::Snapshot> {
    latest(status, |s| !s.locality.is_empty() || is_durable(s))
}

/// Whether `snap` is recorded local on a node other than `node`.
pub fn local_elsewhere(snap: Option<&pb::Snapshot>, node: &str) -> bool {
    snap.is_some_and(|s| !s.locality.is_empty() && s.locality != node)
}

/// Whether the newest local snapshot lacks a durable copy.
pub fn latest_local_not_durable(status: Option<&pb::ActorStatus>) -> bool {
    latest_local(status).is_some_and(|s| !is_durable(s))
}

/// Clears every locality, keeping PENDING records so a delete can still
/// collect an in-flight upload.
pub fn clear_local(status: &mut pb::ActorStatus) {
    for s in &mut status.snapshots {
        s.locality.clear();
    }
}

/// Keeps only the snapshot at `local_gen` (whose local copy the Actor keeps)
/// and the newest COMPLETED one, clearing locality on everything else. Runs
/// after a keeper call removed the other local checkpoints.
/// `last_assigned_generation` never decreases.
pub fn prune(status: &mut pb::ActorStatus, local_gen: Option<i32>) {
    let durable_gen = latest_completed(Some(status)).map(|s| s.generation);
    status.snapshots.retain_mut(|s| {
        let local = Some(s.generation) == local_gen;
        if !local {
            s.locality.clear();
        }
        local || Some(s.generation) == durable_gen
    });
}

/// A snapshot record with a durable copy at `uri`.
#[allow(clippy::too_many_arguments)]
pub fn new_durable(
    generation: i32,
    owner: pb::SnapshotOwner,
    fidelity: i32,
    template_uid: &str,
    uuid: &str,
    uri: &str,
    state: pb::SnapshotStorageState,
) -> pb::Snapshot {
    pb::Snapshot {
        uuid: uuid.into(),
        generation,
        owner: owner as i32,
        fidelity,
        actor_template_uid: template_uid.into(),
        durable_snapshot: Some(pb::SnapshotStorage {
            state: state as i32,
            object: Some(pb::ObjectSnapshot {
                snapshot_uri: uri.into(),
            }),
        }),
        ..Default::default()
    }
}

/// The Tag's snapshot URI when its durable copy is in `state` (any state
/// for `None`), or empty.
pub fn tag_durable_uri(tag: &pb::Tag, state: Option<pb::SnapshotStorageState>) -> &str {
    let Some(snap) = tag.status.as_ref().and_then(|s| s.snapshot.as_ref()) else {
        return "";
    };
    match state {
        Some(st) if durable_state(snap) != st as i32 => "",
        _ => durable_uri(snap),
    }
}

/// The base location the template's snapshots are stored under: its own
/// `storage_location` when the PerTemplateSnapshotLocation gate is enabled
/// on the server and the field is set, else the cluster-wide location.
pub fn resolve_location(
    cluster: &str,
    gates: &PreviewGates,
    template: &pb::ActorTemplate,
) -> Result<String> {
    if gates.is_enabled(Gate::PerTemplateSnapshotLocation) {
        if let Some(loc) = template
            .snapshot_config
            .as_ref()
            .map(|c| c.storage_location.as_str())
        {
            if !loc.is_empty() {
                return Ok(loc.to_string());
            }
        }
    }
    if !cluster.is_empty() {
        return Ok(cluster.to_string());
    }
    let md = template.metadata.clone().unwrap_or_default();
    Err(status::failed_precondition(format!(
        "actor template {}/{} has no snapshot storage location: set snapshotConfig.storageLocation, or set hivekeep-api's --snapshot-location",
        md.hive, md.name
    )))
}

/// Log messages of reports that change nothing.
pub const MSG_DURABLE_OLDER: &str =
    "Ignoring a durable snapshot older than the latest completed one";
/// See [`MSG_DURABLE_OLDER`].
pub const MSG_DURABLE_NOT_RECORDED: &str =
    "Ignoring a durable snapshot at a generation the actor no longer records";
/// See [`MSG_DURABLE_OLDER`].
pub const MSG_DURABLE_CRASHED_SUSPEND: &str =
    "Ignoring the upload of a snapshot whose suspend crashed";

/// Marks the reported durable snapshot COMPLETED and records its fidelity.
/// Its uuid and upload URI must match the record, and it must report a
/// fidelity. A late report (older than the newest COMPLETED, or a
/// generation no longer recorded) changes nothing, as does a CRASHED
/// Actor's report of a snapshot whose suspend crashed.
pub fn apply_latest_durable(
    status: &mut pb::ActorStatus,
    reported: Option<&pb::SnapshotReport>,
) -> Result<()> {
    let Some(reported) = reported else {
        return Ok(());
    };
    if let Some(latest) = latest_completed(Some(status)) {
        if latest.generation >= reported.generation {
            if latest.generation > reported.generation {
                tracing::info!(generation = reported.generation, uuid = %reported.uuid, latest = latest.generation, "{MSG_DURABLE_OLDER}");
            }
            return Ok(());
        }
    }
    let crashed = status.state == pb::ActorState::Crashed as i32;
    let last = status.last_assigned_generation;
    let Some(snap) = by_generation_mut(status, reported.generation) else {
        if reported.generation <= last {
            tracing::info!(generation = reported.generation, uuid = %reported.uuid, "{MSG_DURABLE_NOT_RECORDED}");
            return Ok(());
        }
        return Err(status::failed_precondition(format!(
            "snapshot generation {} ({}) is not recorded for this actor",
            reported.generation, reported.uuid
        )));
    };
    if snap.uuid != reported.uuid {
        return Err(status::failed_precondition(format!(
            "snapshot generation {} is recorded as {}, not {}",
            reported.generation, snap.uuid, reported.uuid
        )));
    }
    if snap.durable_snapshot.is_none() {
        return Err(status::failed_precondition(format!(
            "snapshot generation {} records no upload",
            reported.generation
        )));
    }
    let want = durable_uri(snap).to_string();
    let got = reported
        .object
        .as_ref()
        .map(|o| o.snapshot_uri.as_str())
        .unwrap_or("");
    if got != want {
        return Err(status::invalid_argument(format!(
            "snapshot generation {} was uploaded to {got:?}, want {want:?}",
            reported.generation
        )));
    }
    if reported.fidelity == pb::SnapshotFidelity::Unspecified as i32 {
        return Err(status::invalid_argument(format!(
            "snapshot generation {} reports no fidelity",
            reported.generation
        )));
    }
    if crashed && snap.locality.is_empty() {
        tracing::info!(generation = reported.generation, uuid = %reported.uuid, "{MSG_DURABLE_CRASHED_SUSPEND}");
        return Ok(());
    }
    snap.fidelity = reported.fidelity;
    if let Some(d) = snap.durable_snapshot.as_mut() {
        d.state = COMPLETED;
    }
    Ok(())
}

/// Records that the keeper on `node` deleted its copies of generations up
/// to `up_to`. Refuses when the newest deleted one is not durable (unless
/// the Actor is CRASHED: its revert drops them anyway). A SUSPENDED Actor is
/// released from `node` once nothing of it remains there.
pub fn apply_detach(status: &mut pb::ActorStatus, node: &str, up_to: i32) -> Result<()> {
    let deleted = |s: &pb::Snapshot| s.locality == node && s.generation <= up_to;
    if status.state != pb::ActorState::Crashed as i32 {
        if let Some(l) = latest(Some(status), deleted) {
            if !is_durable(l) {
                return Err(status::failed_precondition(format!(
                    "snapshot generation {} on node {node} is not durable",
                    l.generation
                )));
            }
        }
    }
    for s in &mut status.snapshots {
        if deleted(s) {
            s.locality.clear();
        }
    }
    let on_node = latest(Some(status), |s| s.locality == node).is_some();
    if status.state == pb::ActorState::Suspended as i32 && status.assigned_node == node && !on_node
    {
        status.assigned_node.clear();
    }
    Ok(())
}

/// Applies a keeper report: an upload finished, and (when it names neither
/// a local nor an uploading snapshot) the keeper detached the Actor.
pub fn apply_actor_snapshot_state(
    status: &mut pb::ActorStatus,
    node: &str,
    state: &pb::ActorSnapshotState,
) -> Result<()> {
    apply_latest_durable(status, state.latest_durable.as_ref())?;
    if state.latest_local.is_none() && state.uploading.is_none() {
        let up_to = state
            .latest_durable
            .as_ref()
            .map(|d| d.generation)
            .unwrap_or(0);
        return apply_detach(status, node, up_to);
    }
    Ok(())
}

/// Every PENDING upload URI recorded on the Actor.
pub fn pending_uris(status: Option<&pb::ActorStatus>) -> Vec<String> {
    snaps(status)
        .iter()
        .filter(|s| durable_state(s) == PENDING && !durable_uri(s).is_empty())
        .map(|s| durable_uri(s).to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pb::SnapshotStorageState as St;

    fn snap(gen: i32, state: St, locality: &str) -> pb::Snapshot {
        let mut s = new_durable(
            gen,
            pb::SnapshotOwner::Actor,
            pb::SnapshotFidelity::Memory as i32,
            "t",
            &format!("u{gen}"),
            &format!("gs://b/x/u{gen}"),
            state,
        );
        s.locality = locality.into();
        s
    }

    fn status(state: pb::ActorState, node: &str, snaps: Vec<pb::Snapshot>) -> pb::ActorStatus {
        pb::ActorStatus {
            state: state as i32,
            assigned_node: node.into(),
            last_assigned_generation: snaps.iter().map(|s| s.generation).max().unwrap_or(0),
            snapshots: snaps,
            ..Default::default()
        }
    }

    fn report(gen: i32) -> pb::SnapshotReport {
        pb::SnapshotReport {
            uuid: format!("u{gen}"),
            generation: gen,
            fidelity: pb::SnapshotFidelity::Memory as i32,
            object: Some(pb::ObjectSnapshot {
                snapshot_uri: format!("gs://b/x/u{gen}"),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn lookups() {
        let st = status(
            pb::ActorState::Suspended,
            "n",
            vec![snap(1, St::Completed, ""), snap(2, St::Pending, "n")],
        );
        assert_eq!(to_resume(Some(&st)).unwrap().generation, 2);
        assert_eq!(latest_completed(Some(&st)).unwrap().generation, 1);
        assert!(latest_is_local(Some(&st)));
        assert!(latest_local_not_durable(Some(&st)));
        assert!(local_elsewhere(to_resume(Some(&st)), "m"));
        assert_eq!(pending_uris(Some(&st)), vec!["gs://b/x/u2"]);
        assert!(to_resume(None).is_none());
    }

    #[test]
    fn prune_keeps_local_and_latest_durable() {
        let mut st = status(
            pb::ActorState::Suspending,
            "n",
            vec![
                snap(1, St::Completed, "n"),
                snap(2, St::Completed, ""),
                snap(3, St::Pending, "n"),
            ],
        );
        prune(&mut st, Some(3));
        let gens: Vec<_> = st
            .snapshots
            .iter()
            .map(|s| (s.generation, s.locality.clone()))
            .collect();
        assert_eq!(gens, vec![(2, String::new()), (3, "n".to_string())]);
        prune(&mut st, None);
        assert_eq!(st.snapshots.len(), 1);
        assert_eq!(st.last_assigned_generation, 3);
    }

    #[test]
    fn durable_reports() {
        let mut st = status(
            pb::ActorState::Suspended,
            "n",
            vec![snap(1, St::Completed, ""), snap(2, St::Pending, "n")],
        );
        apply_latest_durable(&mut st, Some(&report(2))).unwrap();
        assert!(is_durable(by_generation(Some(&st), 2).unwrap()));
        // Older and repeated reports change nothing.
        apply_latest_durable(&mut st, Some(&report(1))).unwrap();
        apply_latest_durable(&mut st, Some(&report(2))).unwrap();
        // Unknown future generation.
        assert!(apply_latest_durable(&mut st, Some(&report(9))).is_err());

        let mut st = status(
            pb::ActorState::Suspended,
            "n",
            vec![snap(2, St::Pending, "n")],
        );
        let mut bad = report(2);
        bad.uuid = "other".into();
        assert_eq!(
            apply_latest_durable(&mut st, Some(&bad))
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        let mut bad = report(2);
        bad.object = None;
        assert_eq!(
            apply_latest_durable(&mut st, Some(&bad))
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let mut bad = report(2);
        bad.fidelity = 0;
        assert_eq!(
            apply_latest_durable(&mut st, Some(&bad))
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );

        // A crashed suspend's upload is not recorded.
        let mut st = status(pb::ActorState::Crashed, "n", vec![snap(2, St::Pending, "")]);
        apply_latest_durable(&mut st, Some(&report(2))).unwrap();
        assert!(!is_durable(&st.snapshots[0]));
    }

    #[test]
    fn detach_reports() {
        let mut st = status(
            pb::ActorState::Suspended,
            "n",
            vec![snap(2, St::Pending, "n")],
        );
        assert!(apply_detach(&mut st, "n", 2).is_err());
        let state = pb::ActorSnapshotState {
            latest_durable: Some(report(2)),
            ..Default::default()
        };
        apply_actor_snapshot_state(&mut st, "n", &state).unwrap();
        assert!(st.snapshots[0].locality.is_empty());
        assert!(st.assigned_node.is_empty());

        // A newer snapshot on the node keeps the Actor there.
        let mut st = status(
            pb::ActorState::Suspended,
            "n",
            vec![snap(1, St::Completed, "n"), snap(2, St::Pending, "n")],
        );
        apply_detach(&mut st, "n", 1).unwrap();
        assert_eq!(st.assigned_node, "n");
        assert_eq!(st.snapshots[1].locality, "n");

        // A crashed Actor is never refused.
        let mut st = status(
            pb::ActorState::Crashed,
            "n",
            vec![snap(2, St::Pending, "n")],
        );
        apply_detach(&mut st, "n", 2).unwrap();
    }

    #[test]
    fn locations() {
        let mut t = pb::ActorTemplate {
            snapshot_config: Some(pb::SnapshotConfig {
                storage_location: "gs://own/".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            resolve_location("gs://c/", &PreviewGates::none(), &t).unwrap(),
            "gs://c/"
        );
        let gates = PreviewGates::none().with(Gate::PerTemplateSnapshotLocation);
        assert_eq!(
            resolve_location("gs://c/", &gates, &t).unwrap(),
            "gs://own/"
        );
        t.snapshot_config = None;
        assert!(resolve_location("", &gates, &t).is_err());
    }
}

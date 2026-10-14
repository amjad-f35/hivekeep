//! The tracker's reports, sent to the control plane's WorkerService.

use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as api;
use hivekeep_proto::keeper::v1 as pb;
use tonic::Status;

use crate::context::Ctx;
use crate::control::WorkerServiceApi;
use crate::snapshot_tracker::Reporter;

/// Reports through WorkerService.ReportActorSnapshotState, returning the
/// control plane's error unchanged so the tracker can tell a refusal from a
/// failure to reach it.
pub struct ControlPlaneReporter {
    workers: Arc<dyn WorkerServiceApi>,
}

impl ControlPlaneReporter {
    /// A reporter over `workers`.
    pub fn new(workers: Arc<dyn WorkerServiceApi>) -> Self {
        ControlPlaneReporter { workers }
    }
}

#[async_trait]
impl Reporter for ControlPlaneReporter {
    async fn report_actor_snapshot_state(
        &self,
        ctx: &Ctx,
        actor_uid: &str,
        actor: &ActorRef,
        state: &pb::ActorSnapshotState,
    ) -> Result<(), Status> {
        self.workers
            .report_actor_snapshot_state(
                ctx,
                api::ReportActorSnapshotStateRequest {
                    actor: Some(actor.to_object_ref()),
                    actor_uid: actor_uid.to_string(),
                    state: Some(to_api_state(state)),
                },
            )
            .await
    }
}

/// The control plane's form of a snapshot state, each snapshot's fields
/// inline. A snapshot not held stays unset: the control plane reads a state
/// with neither a local nor an uploading snapshot as a detach.
pub fn to_api_state(s: &pb::ActorSnapshotState) -> api::ActorSnapshotState {
    api::ActorSnapshotState {
        latest_local: s.latest_local.as_ref().and_then(to_api_report),
        uploading: s.uploading.as_ref().and_then(to_api_report),
        latest_durable: s.latest_durable.as_ref().and_then(to_api_report),
        upload_due_time: s.upload_due_time,
    }
}

fn to_api_report(r: &pb::SnapshotReport) -> Option<api::SnapshotReport> {
    let snap = r.snapshot.as_ref()?;
    Some(api::SnapshotReport {
        uuid: snap.snapshot_uuid.clone(),
        generation: snap.generation,
        fidelity: to_api_fidelity(snap.fidelity),
        object: snap.object.as_ref().map(|o| api::ObjectSnapshot {
            snapshot_uri: o.snapshot_uri.clone(),
        }),
        local_commit_time: r.local_commit_time,
        durable_complete_time: r.durable_complete_time,
    })
}

/// Maps the keeper's fidelity onto the identical public one, level by
/// level; anything else goes out unspecified for the control plane to
/// reject.
pub fn to_api_fidelity(f: i32) -> i32 {
    match pb::SnapshotFidelity::try_from(f) {
        Ok(pb::SnapshotFidelity::Volumes) => api::SnapshotFidelity::Volumes as i32,
        Ok(pb::SnapshotFidelity::Rootfs) => api::SnapshotFidelity::Rootfs as i32,
        Ok(pb::SnapshotFidelity::Memory) => api::SnapshotFidelity::Memory as i32,
        _ => api::SnapshotFidelity::Unspecified as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_held_snapshots_only() {
        let s = pb::ActorSnapshotState {
            latest_local: Some(pb::SnapshotReport {
                snapshot: Some(pb::Snapshot {
                    snapshot_uuid: "s1".into(),
                    generation: 2,
                    fidelity: pb::SnapshotFidelity::Rootfs as i32,
                    object: Some(pb::ObjectStorage {
                        snapshot_uri: "gs://b/x".into(),
                        sharing: 0,
                    }),
                }),
                local_commit_time: Some(prost_types::Timestamp {
                    seconds: 5,
                    nanos: 0,
                }),
                durable_complete_time: None,
            }),
            uploading: Some(pb::SnapshotReport::default()),
            latest_durable: None,
            upload_due_time: None,
        };
        let out = to_api_state(&s);
        let l = out.latest_local.unwrap();
        assert_eq!((l.uuid.as_str(), l.generation), ("s1", 2));
        assert_eq!(l.fidelity, api::SnapshotFidelity::Rootfs as i32);
        assert_eq!(l.object.unwrap().snapshot_uri, "gs://b/x");
        assert!(
            out.uploading.is_none(),
            "a report naming no snapshot converts to none"
        );
        assert!(out.latest_durable.is_none());
        assert_eq!(
            to_api_fidelity(99),
            api::SnapshotFidelity::Unspecified as i32
        );
    }
}

#[cfg(test)]
mod behavior_tests;

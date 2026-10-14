//! Reports of snapshot state to the control plane.

use super::*;
use crate::sandbox_test_support::RecordingWorkers;

const UID: &str = "3d7f1b62-8a04-4c19-b5e6-2f9c7a1d0e38";

fn ts(seconds: i64) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp { seconds, nanos: 0 })
}

fn uri(name: &str) -> String {
    format!("gs://test-bucket/hives/hk-demo/actors/{UID}/snapshots/{name}")
}

fn local(name: &str, generation: i32) -> pb::SnapshotReport {
    pb::SnapshotReport {
        snapshot: Some(pb::Snapshot {
            snapshot_uuid: name.into(),
            generation,
            fidelity: pb::SnapshotFidelity::Memory as i32,
            object: Some(pb::ObjectStorage {
                snapshot_uri: uri(name),
                sharing: 0,
            }),
        }),
        local_commit_time: ts(1_700_000_000),
        durable_complete_time: None,
    }
}

fn reported(name: &str, generation: i32) -> api::SnapshotReport {
    api::SnapshotReport {
        uuid: name.into(),
        generation,
        fidelity: api::SnapshotFidelity::Memory as i32,
        object: Some(api::ObjectSnapshot {
            snapshot_uri: uri(name),
        }),
        local_commit_time: ts(1_700_000_000),
        durable_complete_time: None,
    }
}

fn actor() -> ActorRef {
    ActorRef::new("hk-demo", "counter")
}

#[tokio::test]
async fn sends_the_state() {
    let mut durable = local("s1", 1);
    durable.durable_complete_time = ts(1_700_000_060);
    let mut reported_durable = reported("s1", 1);
    reported_durable.durable_complete_time = ts(1_700_000_060);
    for (name, state, want) in [
        (
            "every snapshot",
            pb::ActorSnapshotState {
                latest_local: Some(local("s3", 3)),
                uploading: Some(local("s2", 2)),
                latest_durable: Some(durable.clone()),
                upload_due_time: ts(1_700_000_300),
            },
            api::ActorSnapshotState {
                latest_local: Some(reported("s3", 3)),
                uploading: Some(reported("s2", 2)),
                latest_durable: Some(reported_durable.clone()),
                upload_due_time: ts(1_700_000_300),
            },
        ),
        (
            "detached",
            pb::ActorSnapshotState {
                latest_durable: Some(durable.clone()),
                ..Default::default()
            },
            api::ActorSnapshotState {
                latest_durable: Some(reported_durable.clone()),
                ..Default::default()
            },
        ),
    ] {
        let workers = Arc::new(RecordingWorkers::default());
        ControlPlaneReporter::new(workers.clone())
            .report_actor_snapshot_state(&Ctx::background(), UID, &actor(), &state)
            .await
            .unwrap();
        let want = api::ReportActorSnapshotStateRequest {
            actor: Some(api::ObjectRef {
                hive: "hk-demo".into(),
                name: "counter".into(),
            }),
            actor_uid: UID.into(),
            state: Some(want),
        };
        assert_eq!(*workers.reports.lock(), vec![want], "{name}");
    }
}

#[tokio::test]
async fn returns_the_refusal() {
    let workers = Arc::new(RecordingWorkers::default());
    *workers.err.lock() = Some(Status::not_found("actor not found"));
    let state = pb::ActorSnapshotState {
        latest_durable: Some(local("s1", 1)),
        ..Default::default()
    };
    let err = ControlPlaneReporter::new(workers)
        .report_actor_snapshot_state(&Ctx::background(), UID, &actor(), &state)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

/// Each keeper fidelity maps to the public one of the same name.
#[test]
fn fidelity_maps_by_name() {
    for f in [
        pb::SnapshotFidelity::Unspecified,
        pb::SnapshotFidelity::Volumes,
        pb::SnapshotFidelity::Rootfs,
        pb::SnapshotFidelity::Memory,
    ] {
        let got = api::SnapshotFidelity::try_from(to_api_fidelity(f as i32)).unwrap();
        assert_eq!(got.as_str_name(), f.as_str_name(), "{f:?}");
    }
    assert_eq!(
        to_api_fidelity(99),
        api::SnapshotFidelity::Unspecified as i32
    );
}

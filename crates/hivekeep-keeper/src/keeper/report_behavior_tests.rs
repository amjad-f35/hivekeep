//! Background uploads and detaches reach the control plane through the
//! tracker's reporter.

use std::sync::Arc;
use std::time::Duration;

use hivekeep_proto::api::v1 as api;
use hivekeep_proto::keeper::v1 as pb;

use super::lifecycle_harness::*;
use crate::sandbox_test_support::RecordingWorkers;
use crate::snapshot_report::ControlPlaneReporter;
use crate::snapshot_tracker::Options;

fn uuid(r: Option<&api::SnapshotReport>) -> &str {
    r.map(|r| r.uuid.as_str()).unwrap_or("")
}

/// Waits for a report whose state satisfies `want`, checking each
/// report names the actor.
async fn report_where(
    workers: &RecordingWorkers,
    what: &str,
    want: impl Fn(&api::ActorSnapshotState) -> bool,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        for r in workers.reports.lock().iter() {
            let actor = r.actor.as_ref().unwrap();
            assert_eq!(
                (
                    actor.hive.as_str(),
                    actor.name.as_str(),
                    r.actor_uid.as_str()
                ),
                (HIVE, ACTOR, UID)
            );
            if want(r.state.as_ref().unwrap()) {
                return;
            }
        }
        let wait = workers.reported.notified();
        assert!(
            tokio::time::timeout_at(deadline, wait).await.is_ok(),
            "no {what} report: {:?}",
            workers.reports.lock()
        );
    }
}

#[tokio::test]
async fn background_upload_and_detach_are_reported() {
    let workers = Arc::new(RecordingWorkers::default());
    let a = run_plugin_actor(NodeOpts {
        store: StoreKind::Transfer,
        tracker: Options {
            upload_delay: Duration::from_millis(300),
            detach_delay: Duration::from_millis(1500),
            reporter: Some(Arc::new(ControlPlaneReporter::new(workers.clone()))),
            ..Options::default()
        },
        start_tracker: true,
        ..Default::default()
    })
    .await;
    a.checkpoint(&a.snapshot("pause-snap-1", 1), pb::UploadMode::Deferred)
        .await
        .expect("Checkpoint");
    report_where(&workers, "upload", |s| {
        uuid(s.latest_durable.as_ref()) == "pause-snap-1"
            && uuid(s.latest_local.as_ref()) == "pause-snap-1"
    })
    .await;
    report_where(&workers, "detach", |s| {
        s.latest_local.is_none()
            && s.uploading.is_none()
            && uuid(s.latest_durable.as_ref()) == "pause-snap-1"
    })
    .await;
    assert!(
        !a.paths().local_snapshot_dir(UID, "pause-snap-1").exists(),
        "pause-snap-1 is still on the node after the reported detach"
    );
    a.node.stop_tracker().await;
}

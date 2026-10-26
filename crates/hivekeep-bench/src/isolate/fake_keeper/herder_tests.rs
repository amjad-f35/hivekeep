use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hivekeep_proto::api::v1 as api;
use hivekeep_proto::keeper::v1 as kpb;
use parking_lot::Mutex;
use tonic::Status;

use super::*;
use crate::isolate::fake_keeper::config::Delays;
use crate::isolate::fake_keeper::epochs::WorkerEpochs;
use crate::isolate::fake_keeper::storage::ObjectWriter;

const TEST_SNAPSHOT_URI: &str =
    "gs://bench-bucket/benchmark-workloads/sleep/hives/team-a/actors/0f9c1d2e-3b4a-4c5d-8e6f-7a8b9c0d1e2f/snapshots/snap-1";

/// Records every object written.
#[derive(Default)]
struct RecordingStorage {
    written: Mutex<Vec<String>>,
    err: Option<String>,
    delay: Duration,
}

#[async_trait]
impl ObjectWriter for RecordingStorage {
    async fn put_object(&self, bucket: &str, object: &str, _data: Vec<u8>) -> Result<(), String> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        self.written.lock().push(format!("{bucket}/{object}"));
        Ok(())
    }
}

fn storage() -> Arc<RecordingStorage> {
    Arc::new(RecordingStorage::default())
}

fn herder(delays: Delays, storage: Arc<RecordingStorage>) -> Herder {
    Herder::new(delays, storage)
}

fn external_checkpoint(uri: &str) -> kpb::CheckpointRequest {
    checkpoint_with_mode(uri, kpb::UploadMode::Immediate)
}

fn checkpoint_with_mode(uri: &str, mode: kpb::UploadMode) -> kpb::CheckpointRequest {
    kpb::CheckpointRequest {
        snapshot: Some(snapshot(uri)),
        upload_config: Some(kpb::UploadConfig { mode: mode as i32 }),
        ..Default::default()
    }
}

fn snapshot(uri: &str) -> kpb::Snapshot {
    kpb::Snapshot {
        object: Some(kpb::ObjectStorage {
            snapshot_uri: uri.into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

type Call<'a> = Pin<Box<dyn Future<Output = Result<(), Status>> + Send + 'a>>;

/// Every keeper call the herder implements. Checkpoint and PromoteSnapshot
/// upload a snapshot, so each writes a placeholder.
fn herder_calls(h: &Herder) -> Vec<(&'static str, Call<'_>)> {
    vec![
        (
            "Run",
            Box::pin(async { h.run(Default::default()).await.map(|_| ()) }),
        ),
        (
            "Restore",
            Box::pin(async { h.restore(Default::default()).await.map(|_| ()) }),
        ),
        (
            "Checkpoint",
            Box::pin(async {
                h.checkpoint(external_checkpoint(TEST_SNAPSHOT_URI))
                    .await
                    .map(|_| ())
            }),
        ),
        (
            "PromoteSnapshot",
            Box::pin(async {
                h.promote_snapshot(kpb::PromoteSnapshotRequest {
                    snapshot: Some(snapshot(TEST_SNAPSHOT_URI)),
                    ..Default::default()
                })
                .await
                .map(|_| ())
            }),
        ),
        (
            "Terminate",
            Box::pin(async { h.terminate(Default::default()).await.map(|_| ()) }),
        ),
    ]
}

#[tokio::test]
async fn succeeds_after_delay() {
    const D: Duration = Duration::from_millis(20);
    let h = herder(Delays::uniform(D), storage());
    for (name, call) in herder_calls(&h) {
        let start = Instant::now();
        call.await.unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(
            start.elapsed() >= D,
            "{name} returned after {:?}",
            start.elapsed()
        );
    }
}

// Run and Restore report the epoch the Worker registered with through the
// relay, as its worker process would, and 0 for a Worker that never did.
#[tokio::test]
async fn reports_the_registered_epoch() {
    let epochs = Arc::new(WorkerEpochs::new());
    epochs.set("pod-a", 4);
    let mut h = herder(Delays::default(), storage());
    h.epochs = epochs;
    for (pod, want) in [("pod-a", 4), ("pod-unregistered", 0)] {
        let run = h
            .run(kpb::RunRequest {
                worker_pod_uid: pod.into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(run.worker_epoch, want, "Run({pod})");
        let restore = h
            .restore(kpb::RestoreRequest {
                worker_pod_uid: pod.into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(restore.worker_epoch, want, "Restore({pod})");
    }
}

#[tokio::test]
async fn zero_delay() {
    let h = herder(Delays::default(), storage());
    for (name, call) in herder_calls(&h) {
        call.await.unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}

#[tokio::test]
async fn stops_when_caller_gives_up() {
    let h = herder(Delays::uniform(Duration::from_secs(3600)), storage());
    for (name, call) in herder_calls(&h) {
        let r = tokio::time::timeout(Duration::from_millis(10), call).await;
        assert!(r.is_err(), "{name} finished despite an hour's delay");
    }
}

#[tokio::test]
async fn uses_each_calls_own_delay() {
    let h = herder(
        Delays {
            run: Duration::from_secs(3600),
            ..Default::default()
        },
        storage(),
    );
    h.restore(Default::default())
        .await
        .expect("Restore with no delay of its own");
    let r = tokio::time::timeout(Duration::from_millis(10), h.run(Default::default())).await;
    assert!(r.is_err(), "Run with an hour's delay finished");
}

// hivekeep-api copies an Actor's snapshot when it creates a Tag and refuses
// to copy an empty one, so calls that upload before they return must leave
// an object under its URI.
#[tokio::test]
async fn writes_placeholder_under_snapshot_uri() {
    let want = format!(
        "bench-bucket/benchmark-workloads/sleep/hives/team-a/actors/0f9c1d2e-3b4a-4c5d-8e6f-7a8b9c0d1e2f/snapshots/snap-1/{PLACEHOLDER_OBJECT}"
    );
    for which in ["Checkpoint", "PromoteSnapshot"] {
        let s = storage();
        let h = herder(Delays::default(), s.clone());
        let call = herder_calls(&h)
            .into_iter()
            .find(|(n, _)| *n == which)
            .unwrap()
            .1;
        call.await.unwrap();
        assert_eq!(*s.written.lock(), vec![want.clone()], "{which}");
    }
}

// A deferred upload happens after Checkpoint returns, which the fake does
// not model, so only an immediate one writes a placeholder.
#[tokio::test]
async fn writes_nothing_for_local_checkpoint() {
    let s = storage();
    let h = herder(Delays::default(), s.clone());
    h.checkpoint(Default::default()).await.unwrap();
    h.checkpoint(checkpoint_with_mode(
        TEST_SNAPSHOT_URI,
        kpb::UploadMode::Deferred,
    ))
    .await
    .unwrap();
    for (name, call) in herder_calls(&h) {
        if name == "Checkpoint" || name == "PromoteSnapshot" {
            continue;
        }
        call.await.unwrap();
    }
    assert!(s.written.lock().is_empty(), "wrote {:?}", s.written.lock());
}

// hivekeep-api checks that PromoteSnapshot reports the snapshot it asked
// for durable, and fails the Tag otherwise.
#[tokio::test]
async fn promote_snapshot_reports_snapshot_durable() {
    let snap = kpb::Snapshot {
        snapshot_uuid: "snap-1".into(),
        generation: 2,
        fidelity: kpb::SnapshotFidelity::Memory as i32,
        object: Some(kpb::ObjectStorage {
            snapshot_uri: TEST_SNAPSHOT_URI.into(),
            ..Default::default()
        }),
    };
    let h = herder(Delays::default(), storage());
    let resp = h
        .promote_snapshot(kpb::PromoteSnapshotRequest {
            snapshot: Some(snap.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    let report = kpb::SnapshotReport {
        snapshot: Some(snap),
        ..Default::default()
    };
    assert_eq!(
        resp.state,
        Some(kpb::ActorSnapshotState {
            latest_local: Some(report.clone()),
            latest_durable: Some(report),
            ..Default::default()
        })
    );
}

#[tokio::test]
async fn fails_when_placeholder_cannot_be_written() {
    let s = Arc::new(RecordingStorage {
        err: Some("bucket unavailable".into()),
        ..Default::default()
    });
    let h = herder(Delays::default(), s);
    assert!(h
        .checkpoint(external_checkpoint(TEST_SNAPSHOT_URI))
        .await
        .is_err());
    assert!(h
        .checkpoint(external_checkpoint("not a snapshot uri"))
        .await
        .is_err());
    let promote = herder_calls(&h)
        .into_iter()
        .find(|(n, _)| *n == "PromoteSnapshot")
        .unwrap()
        .1;
    assert!(promote.await.is_err());
}

// The write runs inside the delay, so it does not add to it.
#[tokio::test]
async fn placeholder_write_runs_inside_delay() {
    const D: Duration = Duration::from_millis(100);
    let s = Arc::new(RecordingStorage {
        delay: Duration::from_millis(60),
        ..Default::default()
    });
    let h = herder(Delays::uniform(D), s);
    let start = Instant::now();
    h.checkpoint(external_checkpoint(TEST_SNAPSHOT_URI))
        .await
        .unwrap();
    let got = start.elapsed();
    assert!(
        got >= D && got <= D + Duration::from_millis(50),
        "Checkpoint took {got:?}"
    );
}

#[derive(Clone, Default)]
struct LogBuf(Arc<Mutex<Vec<u8>>>);

impl Write for LogBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// A write slower than a nonzero delay is worth a warning; with no delay
// every write is slower, and warning on each would flood the log.
#[tokio::test]
async fn warns_only_when_write_outlasts_a_nonzero_delay() {
    for (name, delay, want_warn) in [
        ("no delay", Duration::ZERO, false),
        ("delay outlasted", Duration::from_millis(1), true),
        ("delay not reached", Duration::from_secs(3600), false),
    ] {
        let logs = LogBuf::default();
        let sink = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || sink.clone())
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let s = Arc::new(RecordingStorage {
            delay: Duration::from_millis(20),
            ..Default::default()
        });
        let h = herder(Delays::default(), s);
        h.write_placeholder(Instant::now(), delay, TEST_SNAPSHOT_URI)
            .await
            .unwrap();
        let out = String::from_utf8(logs.0.lock().clone()).unwrap();
        assert_eq!(
            out.contains("outlasted the configured delay"),
            want_warn,
            "{name}: {out}"
        );
    }
}

/// Records every mint, as hivekeep-api's WorkerService.
#[derive(Default)]
struct RecordingMinter {
    reqs: Mutex<Vec<api::MintWorkerActorCertificateRequest>>,
    err: Option<String>,
    delay: Duration,
}

#[async_trait]
impl ActorCertMinter for RecordingMinter {
    async fn mint_worker_actor_certificate(
        &self,
        req: api::MintWorkerActorCertificateRequest,
    ) -> Result<api::MintWorkerActorCertificateResponse, Status> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.reqs.lock().push(req);
        if let Some(e) = &self.err {
            return Err(Status::not_found(e.clone()));
        }
        Ok(api::MintWorkerActorCertificateResponse {
            actor_certificates: vec![b"leaf".to_vec()],
        })
    }
}

const TEST_ACTOR_UID: &str = "0f9c1d2e-3b4a-4c5d-8e6f-7a8b9c0d1e2f";
const TEST_WORKER_POD_UID: &str = "5d4c3b2a-1f0e-4d9c-8b7a-6f5e4d3c2b1a";

fn test_gateway() -> kpb::EgressGateway {
    kpb::EgressGateway {
        address: "gate-egress.hivekeep-system.svc:443".into(),
    }
}

/// Run and Restore for one Actor, naming `gateway`.
fn activations(h: &Herder, gateway: Option<kpb::EgressGateway>) -> Vec<(&'static str, Call<'_>)> {
    let g = gateway.clone();
    vec![
        (
            "Run",
            Box::pin(async move {
                h.run(kpb::RunRequest {
                    worker_pod_uid: TEST_WORKER_POD_UID.into(),
                    hive: "team-a".into(),
                    actor_name: "sleep-1".into(),
                    actor_uid: TEST_ACTOR_UID.into(),
                    egress_gateway: g,
                    ..Default::default()
                })
                .await
                .map(|_| ())
            }),
        ),
        (
            "Restore",
            Box::pin(async move {
                h.restore(kpb::RestoreRequest {
                    worker_pod_uid: TEST_WORKER_POD_UID.into(),
                    hive: "team-a".into(),
                    actor_name: "sleep-1".into(),
                    actor_uid: TEST_ACTOR_UID.into(),
                    egress_gateway: gateway,
                    ..Default::default()
                })
                .await
                .map(|_| ())
            }),
        ),
    ]
}

fn minting_herder(m: Arc<RecordingMinter>, d: Delays) -> Herder {
    let mut h = herder(d, storage());
    h.csr = new_actor_csr().unwrap();
    h.minter = Some(m);
    h
}

// The worker mints the Actor's certificate before starting any workload with
// tunnelled egress, so each Run and Restore that names a gateway costs
// hivekeep-api one mint, for that Actor, with a CSR it accepts.
#[tokio::test]
async fn mints_actor_certificate_on_activation() {
    for which in ["Run", "Restore"] {
        let m = Arc::new(RecordingMinter::default());
        let h = minting_herder(m.clone(), Delays::default());
        let call = activations(&h, Some(test_gateway()))
            .into_iter()
            .find(|(n, _)| *n == which)
            .unwrap()
            .1;
        call.await.unwrap();
        let got = m.reqs.lock().clone();
        assert_eq!(got.len(), 1, "{which} minted {} times", got.len());
        assert_eq!(
            got[0].actor,
            Some(api::ObjectRef {
                hive: "team-a".into(),
                name: "sleep-1".into()
            })
        );
        assert_eq!(got[0].actor_uid, TEST_ACTOR_UID);
        // The control plane mints only for the worker hosting the Actor.
        assert_eq!(
            got[0].worker,
            Some(api::ObjectRef {
                hive: String::new(),
                name: TEST_WORKER_POD_UID.into()
            })
        );
        // hivekeep-api parses the CSR and checks its signature.
        hivekeep_identity::csr::verified_public_key(&got[0].certificate_signing_request)
            .expect("CSR verifies");
    }
}

// The worker mints only for tunnelled egress, which hivekeep-api asks for by
// naming a gateway; and --mint-actor-certificate=false leaves the mint out.
#[tokio::test]
async fn skips_mint() {
    let m = Arc::new(RecordingMinter::default());
    let h = minting_herder(m.clone(), Delays::default());
    for (name, call) in activations(&h, None) {
        call.await
            .unwrap_or_else(|e| panic!("{name} without a gateway: {e}"));
    }
    assert!(m.reqs.lock().is_empty());
    let off = herder(Delays::default(), storage());
    for (name, call) in activations(&off, Some(test_gateway())) {
        call.await
            .unwrap_or_else(|e| panic!("{name} with minting off: {e}"));
    }
}

// A failed mint fails the activation, as tunnelled egress fails closed.
#[tokio::test]
async fn fails_when_mint_fails() {
    let m = Arc::new(RecordingMinter {
        err: Some("actor not found".into()),
        ..Default::default()
    });
    let h = minting_herder(m, Delays::default());
    for (name, call) in activations(&h, Some(test_gateway())) {
        assert!(
            call.await.is_err(),
            "{name} succeeded with the mint refused"
        );
    }
}

// The mint is hivekeep-api's time, so it adds to the data plane's delay.
#[tokio::test]
async fn mint_adds_to_delay() {
    const D: Duration = Duration::from_millis(100);
    const MINT: Duration = Duration::from_millis(60);
    let m = Arc::new(RecordingMinter {
        delay: MINT,
        ..Default::default()
    });
    let h = minting_herder(m, Delays::uniform(D));
    for (name, call) in activations(&h, Some(test_gateway())) {
        let start = Instant::now();
        call.await.unwrap();
        assert!(
            start.elapsed() >= D + MINT,
            "{name} took {:?}",
            start.elapsed()
        );
    }
}

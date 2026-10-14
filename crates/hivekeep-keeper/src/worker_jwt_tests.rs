use std::path::Path;

use chrono::TimeZone;
use hivekeep_core::snapshot::SnapshotUri;

use super::*;
use crate::clock::MockClock;
use crate::objectstore::ObjectStore;
use crate::paths::NodePaths;
use crate::service_stubs::{Minter, RecordingStore};
use crate::snapshot_transfer::SnapshotTransfer;

const PAUSED_URI: &str =
    "gs://bucket/root/hives/demo/actors/3d7f1b62-8a04-4c19-b5e6-2f9c7a1d0e38/snapshots/snap-1";

fn test_actor() -> TransferActor {
    TransferActor {
        hive: "demo".into(),
        name: "bot".into(),
        uid: "9f1e0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a".into(),
    }
}

#[tokio::test(start_paused = true)]
async fn mints_once_per_actor_until_near_expiry() {
    let now = Utc.with_ymd_and_hms(2026, 10, 16, 10, 0, 0).unwrap();
    let minter = Arc::new(Minter::new("jwt-1", now + chrono::Duration::hours(1)));
    let j = WorkerActorJwts::new(minter.clone(), Arc::new(MockClock::new(now)));
    let ctx = Ctx::background();
    for _ in 0..2 {
        assert_eq!(j.get(&ctx, &test_actor()).await.unwrap(), "jwt-1");
    }
    assert_eq!(minter.got.lock().len(), 1, "minted more than once");
    {
        let got = minter.got.lock();
        let req = &got[0];
        let a = req.actor.as_ref().unwrap();
        assert_eq!(
            (a.hive.as_str(), a.name.as_str(), req.actor_uid.as_str()),
            ("demo", "bot", test_actor().uid.as_str())
        );
        assert_eq!(req.audiences, vec![ACTOR_JWT_AUDIENCE.to_string()]);
    }

    let mut other = test_actor();
    other.uid = "another-uid".into();
    j.get(&ctx, &other).await.unwrap();
    assert_eq!(minter.got.lock().len(), 2, "a different actor should mint");

    // Inside the refresh margin the cached JWT is not used.
    tokio::time::advance(Duration::from_secs(3600) - REFRESH_MARGIN + Duration::from_secs(1)).await;
    *minter.jwt.lock() = "jwt-2".into();
    assert_eq!(j.get(&ctx, &test_actor()).await.unwrap(), "jwt-2");
}

#[tokio::test]
async fn mint_errors() {
    let later = Utc::now() + chrono::Duration::hours(1);
    for (name, minter, want) in [
        // NotFound and Aborted must not reach callers as NotFound, which a
        // transfer reads as a missing snapshot object.
        (
            "not this node's actor",
            Minter::failing(Status::not_found("actor not found")),
            Code::FailedPrecondition,
        ),
        (
            "recreated actor",
            Minter::failing(Status::aborted("uid changed")),
            Code::FailedPrecondition,
        ),
        (
            "control plane down",
            Minter::failing(Status::unavailable("connection refused")),
            Code::Unavailable,
        ),
        (
            "control plane refuses the keeper",
            Minter::failing(Status::unauthenticated("bad cert")),
            Code::Internal,
        ),
        ("empty token", Minter::new("", later), Code::Internal),
    ] {
        let j = WorkerActorJwts::new(Arc::new(minter), Arc::new(crate::clock::SystemClock));
        let err = j.get(&Ctx::background(), &test_actor()).await.unwrap_err();
        assert_eq!(err.code(), want, "{name}: {err:?}");
    }
}

fn transfer(d: &Path, store: Arc<RecordingStore>, minter: Minter) -> SnapshotTransfer {
    let jwts = Arc::new(WorkerActorJwts::new(
        Arc::new(minter),
        Arc::new(crate::clock::SystemClock),
    ));
    SnapshotTransfer::new(
        store,
        jwts,
        Arc::new(NodePaths::new(d.join("node"))),
        "boot-1".into(),
    )
}

#[tokio::test]
async fn transfers_carry_the_actor_jwt() {
    let d = tempfile::tempdir().unwrap();
    let store = Arc::new(RecordingStore::new(&d.path().join("objects")));
    let t = transfer(
        d.path(),
        store.clone(),
        Minter::new("test-actor-jwt", Utc::now() + chrono::Duration::hours(1)),
    );
    let ctx = Ctx::background();
    let src = d.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("a"), b"a").unwrap();
    t.upload_snapshot_files(&ctx, &test_actor(), PAUSED_URI, &src, &["a".into()])
        .await
        .unwrap();
    let dst = d.path().join("dst");
    std::fs::create_dir_all(&dst).unwrap();
    t.fetch_snapshot_files(&ctx, &test_actor(), PAUSED_URI, &dst, &["a".into()])
        .await
        .unwrap();
    let fetches = store.fetches.lock();
    let uploads = store.uploads.lock();
    assert_eq!(fetches.len(), 1);
    assert_eq!(fetches[0].actor_jwt, "test-actor-jwt");
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].actor_jwt, "test-actor-jwt");
    let _ = SnapshotUri::parse(PAUSED_URI).unwrap();
    let _: &dyn ObjectStore = &*store;
}

#[tokio::test]
async fn transfers_stop_when_the_mint_fails() {
    let d = tempfile::tempdir().unwrap();
    let store = Arc::new(RecordingStore::new(&d.path().join("objects")));
    let t = transfer(
        d.path(),
        store.clone(),
        Minter::failing(Status::not_found("actor not found")),
    );
    let err = t
        .fetch_snapshot_files(
            &Ctx::background(),
            &test_actor(),
            PAUSED_URI,
            d.path(),
            &["a".into()],
        )
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Code::FailedPrecondition,
        "want FailedPrecondition for an actor this node cannot transfer for: {err:?}"
    );
    assert!(
        store.fetches.lock().is_empty(),
        "the store was called without a JWT"
    );
}

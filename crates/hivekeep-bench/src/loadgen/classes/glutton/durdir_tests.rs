use sha2::{Digest, Sha256};

use super::*;
use crate::glutton::fake;
use crate::loadgen::classes::glutton::test_support::config;
use crate::loadgen::dynconfig::{Holder, Lifecycle, LIFECYCLE_MODE_ASYNC, RESUME_MODE_EXPLICIT};
use crate::loadgen::fake_control::FakeControl;

fn knobs(resume: &str, lifecycle: &str) -> DurDirKnobs {
    DurDirKnobs {
        lifecycle: Lifecycle {
            resume_mode: resume.into(),
            lifecycle_mode: lifecycle.into(),
        },
        ..Default::default()
    }
}

async fn user(
    srv: &fake::Server,
    api: Arc<FakeControl>,
    k: DurDirKnobs,
    data_len: usize,
) -> DurDirUser {
    let cfg = config(api, &srv.url(), Holder::fixed(k));
    DurDirUser {
        cfg: Arc::new(cfg),
        actor_name: "duractor".into(),
        template_name: DEFAULT_DUR_TEMPLATE.into(),
        user_class: DURDIR_USER_CLASS.into(),
        expected_digest: String::new(),
        expected_size: data_len as i64,
    }
}

fn data(bytes: &[u8]) -> fake::Config {
    fake::Config {
        data: bytes.to_vec(),
        ..Default::default()
    }
}

#[tokio::test]
async fn loop_sequence() {
    let http = vec![READ_DISK_ROUTE, READ_DISK_ROUTE, WRITE_DISK_ROUTE];
    for (name, resume, lifecycle, grpc, mode) in [
        (
            "explicit resume mode",
            RESUME_MODE_EXPLICIT,
            "",
            vec!["SuspendActor(async)", "ResumeActor"],
            api::DurabilityMode::Asynchronous,
        ),
        // No ResumeActor call: router traffic wakes the Actor.
        (
            "implicit resume mode",
            RESUME_MODE_IMPLICIT,
            "",
            vec!["SuspendActor(async)"],
            api::DurabilityMode::Asynchronous,
        ),
        (
            "sync lifecycle mode",
            RESUME_MODE_EXPLICIT,
            LIFECYCLE_MODE_SYNC,
            vec!["SuspendActor", "ResumeActor"],
            api::DurabilityMode::Synchronous,
        ),
        (
            "async lifecycle mode",
            RESUME_MODE_EXPLICIT,
            LIFECYCLE_MODE_ASYNC,
            vec!["SuspendActor(async)", "ResumeActor"],
            api::DurabilityMode::Asynchronous,
        ),
    ] {
        let srv = fake::Server::start(data(b"seq content")).await;
        let api = FakeControl::new();
        let k = knobs(resume, lifecycle);
        let mut du = user(&srv, api.clone(), k.clone(), 11).await;
        du.expected_digest = srv.hex_digest();
        du.step(&k).await;
        assert_eq!(api.calls(), grpc, "{name}: gRPC calls");
        assert_eq!(srv.recorded_paths(), http, "{name}: HTTP calls");
        for req in api.with(|s| s.suspend_reqs.clone()) {
            assert_eq!(req.options.unwrap().mode(), mode, "{name}");
        }
    }
}

#[tokio::test]
async fn uses_configured_file_size() {
    let size = 1 << 20;
    let srv = fake::Server::start(data(&vec![0; size])).await;
    let mut du = user(&srv, FakeControl::new(), DurDirKnobs::default(), size).await;
    du.write_disk("TestConfiguredSize", size as i64, pb::WriteMode::Truncate)
        .await
        .expect("write_disk");
    assert_eq!(srv.recorded().write_sizes, vec![size as i32]);
}

#[test]
fn test_file_is_a_valid_glutton_key() {
    let re = regex::Regex::new("^[a-zA-Z0-9_-]+$").unwrap();
    assert!(
        re.is_match(DURDIR_TEST_FILE),
        "{DURDIR_TEST_FILE} would be refused by the glutton"
    );
}

fn empty_payload(digest: Option<Vec<u8>>) -> fake::Config {
    fake::Config {
        data: vec![0; 1024],
        empty_payload: true,
        digest,
        ..Default::default()
    }
}

#[tokio::test]
async fn digest_only_accepts_empty_payload() {
    let srv = fake::Server::start(empty_payload(None)).await;
    let mut du = user(&srv, FakeControl::new(), DurDirKnobs::default(), 1024).await;
    du.expected_digest = srv.hex_digest();
    du.read_disk("DigestOnlyEmpty", pb::ReadMode::DigestOnly)
        .await
        .expect("digest-only read");
}

#[tokio::test]
async fn data_mode_rejects_empty_payload() {
    let srv = fake::Server::start(empty_payload(None)).await;
    let mut du = user(&srv, FakeControl::new(), DurDirKnobs::default(), 1024).await;
    du.expected_digest = srv.hex_digest();
    assert!(du
        .read_disk("DataModeEmpty", pb::ReadMode::Data)
        .await
        .is_err());
}

#[tokio::test]
async fn digest_only_still_rejects_wrong_digest() {
    let wrong = Sha256::digest(b"wrong data").to_vec();
    let srv = fake::Server::start(empty_payload(Some(wrong))).await;
    let mut du = user(&srv, FakeControl::new(), DurDirKnobs::default(), 1024).await;
    du.expected_digest = hex::encode(Sha256::digest([0u8; 1024]));
    assert!(du
        .read_disk("DigestOnlyWrong", pb::ReadMode::DigestOnly)
        .await
        .is_err());
}

#[tokio::test]
async fn read_mode_sent_on_wire() {
    let srv = fake::Server::start(Default::default()).await;
    let mut du = user(&srv, FakeControl::new(), DurDirKnobs::default(), 0).await;
    du.expected_digest = srv.hex_digest();
    du.read_disk("ReadModeOnWire", pb::ReadMode::DigestOnly)
        .await
        .expect("read");
    assert_eq!(srv.recorded().read_modes, vec![pb::ReadMode::DigestOnly]);
}

#[tokio::test]
async fn bootstrap_uses_configured_resume_mode() {
    for (name, resume, want_resume) in [
        ("explicit resume mode", RESUME_MODE_EXPLICIT, true),
        ("implicit resume mode", RESUME_MODE_IMPLICIT, false),
    ] {
        let srv = fake::Server::start(data(b"data")).await;
        let api = FakeControl::new();
        let k = DurDirKnobs {
            file_size: 4,
            ..knobs(resume, "")
        };
        let cfg = config(api.clone(), &srv.url(), Holder::fixed(k.clone()));
        let rt = DurDirRuntime::new(Arc::new(cfg));
        rt.start_user(&k)
            .await
            .unwrap_or_else(|e| panic!("{name}: start_user: {e}"));
        let calls = api.calls();
        assert_eq!(
            calls.contains(&"ResumeActor".to_string()),
            want_resume,
            "{name}: {calls:?}"
        );
    }
}

#[tokio::test]
async fn bootstrap_failure_suspends_before_delete() {
    let srv = fake::Server::start(fake::Config {
        status: Some(500),
        ..Default::default()
    })
    .await;
    let api = FakeControl::new();
    let k = DurDirKnobs {
        file_size: 1024,
        ..knobs(RESUME_MODE_EXPLICIT, "")
    };
    let cfg = config(api.clone(), &srv.url(), Holder::fixed(k.clone()));
    let rt = DurDirRuntime::new(Arc::new(cfg));
    assert!(rt.start_user(&k).await.is_err());
    let calls = api.calls();
    assert!(
        calls.len() >= 2 && calls[calls.len() - 2..] == ["SuspendActor(async)", "DeleteActor"],
        "{calls:?}"
    );
    let suspends = api.with(|s| s.suspend_reqs.clone());
    assert_eq!(
        suspends.last().unwrap().options.unwrap().mode(),
        api::DurabilityMode::Asynchronous
    );
}

#[tokio::test]
async fn shutdown_hibernates_before_delete() {
    for (name, lifecycle, want, mode) in [
        (
            "default lifecycle mode",
            "",
            ["SuspendActor(async)", "DeleteActor"],
            api::DurabilityMode::Asynchronous,
        ),
        (
            "sync lifecycle mode",
            LIFECYCLE_MODE_SYNC,
            ["SuspendActor", "DeleteActor"],
            api::DurabilityMode::Synchronous,
        ),
        (
            "async lifecycle mode",
            LIFECYCLE_MODE_ASYNC,
            ["SuspendActor(async)", "DeleteActor"],
            api::DurabilityMode::Asynchronous,
        ),
    ] {
        let srv = fake::Server::start(Default::default()).await;
        let api = FakeControl::new();
        let du = user(&srv, api.clone(), knobs("", lifecycle), 0).await;
        let rt = DurDirRuntime::new(du.cfg.clone());
        rt.users
            .lock()
            .insert(0, Arc::new(tokio::sync::Mutex::new(du)));
        rt.shutdown().await;
        assert_eq!(api.calls(), want, "{name}");
        let suspends = api.with(|s| s.suspend_reqs.clone());
        assert_eq!(suspends.len(), 1, "{name}");
        assert_eq!(suspends[0].options.unwrap().mode(), mode, "{name}");
    }
}

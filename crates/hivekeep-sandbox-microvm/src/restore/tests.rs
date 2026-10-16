use super::*;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;

use hivekeep_sandbox_gvisor::node_path;

use crate::kata::agent::AgentError;
use crate::service::ServiceConfig;
use crate::stats::tests::test_actor;
use crate::ttrpc::TtrpcError;
use crate::vm_boot::{ASSET_CH, ASSET_IMAGE, ASSET_KERNEL};

fn test_dirs() -> pb::ActorDirs {
    pb::ActorDirs {
        root_dir: "/node/actors/actor-a".into(),
        oci_bundle_dir: "/node/actors/actor-a/bundle".into(),
        checkpoint_dir: "/node/actors/actor-a/checkpoint-state".into(),
        restore_dir: "/node/actors/actor-a/restore-state".into(),
        durable_dir_volume_mounts_dir: "/node/actors/actor-a/durable-dirs".into(),
        system_info_volume_roots_dir: "/node/actors/actor-a/system-info".into(),
        external_volume_mounts_dir: "/node/actors/actor-a/volumes".into(),
        sandbox_assets_dir: node_path::actor_sandbox_assets_dir(
            &node_path::actors_dir(),
            &test_actor().uid,
        )
        .to_string_lossy()
        .into(),
    }
}

fn test_spec() -> pb::WorkloadSpec {
    pb::WorkloadSpec {
        containers: vec![pb::Container {
            name: "app".into(),
            ..Default::default()
        }],
    }
}

fn valid_run() -> pb::RunWorkloadRequest {
    let a = test_actor();
    pb::RunWorkloadRequest {
        hive: a.actor.hive.clone(),
        actor_name: a.actor.name.clone(),
        actor_uid: a.uid.clone(),
        spec: Some(test_spec()),
        actor_dirs: Some(test_dirs()),
        ..Default::default()
    }
}

fn valid_restore() -> pb::RestoreWorkloadRequest {
    let a = test_actor();
    pb::RestoreWorkloadRequest {
        hive: a.actor.hive.clone(),
        actor_name: a.actor.name.clone(),
        actor_uid: a.uid.clone(),
        spec: Some(test_spec()),
        actor_dirs: Some(test_dirs()),
        fidelity: pb::SnapshotFidelity::Memory as i32,
        ..Default::default()
    }
}

#[tokio::test]
async fn rpcs_reject_empty_requests() {
    let s = WorkerService::new(ServiceConfig::default());
    let c = CancellationToken::new;
    let codes = [
        (
            "RunWorkload",
            s.run_workload(Default::default(), c())
                .await
                .err()
                .map(|e| e.code()),
        ),
        (
            "RestoreWorkload",
            s.restore_workload(Default::default(), c())
                .await
                .err()
                .map(|e| e.code()),
        ),
        (
            "CheckpointWorkload",
            s.checkpoint_workload(Default::default(), c())
                .await
                .err()
                .map(|e| e.code()),
        ),
        (
            "TerminateWorkload",
            s.terminate_workload(Default::default())
                .await
                .err()
                .map(|e| e.code()),
        ),
        (
            "GetWorkloadStats",
            s.get_workload_stats(Default::default())
                .await
                .err()
                .map(|e| e.code()),
        ),
    ];
    for (name, code) in codes {
        assert_eq!(code, Some(tonic::Code::InvalidArgument), "{name}");
    }
}

#[tokio::test]
async fn startups_wait_for_registration() {
    let s = WorkerService::new(ServiceConfig::default());
    let run = s
        .run_workload(valid_run(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(run.code(), tonic::Code::Unavailable, "{run}");
    let restore = s
        .restore_workload(valid_restore(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(restore.code(), tonic::Code::Unavailable, "{restore}");
}

#[tokio::test]
async fn rpcs_reject_untrusted_runtime_assets() {
    let s = WorkerService::new(ServiceConfig::default());
    s.epoch.store(1);
    let assets = HashMap::from([(ASSET_CH.to_string(), "/bin/sh".to_string())]);
    let mut run = valid_run();
    run.runtime_asset_paths = assets.clone();
    let mut restore = valid_restore();
    restore.runtime_asset_paths = assets;
    for (name, err) in [
        (
            "RunWorkload",
            s.run_workload(run, CancellationToken::new())
                .await
                .unwrap_err(),
        ),
        (
            "RestoreWorkload",
            s.restore_workload(restore, CancellationToken::new())
                .await
                .unwrap_err(),
        ),
    ] {
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{name}: {err}");
        assert!(
            err.message().contains("runtime_asset_paths"),
            "{name}: {}",
            err.message()
        );
    }
}

#[tokio::test]
async fn restore_needs_a_restore_dir() {
    let s = WorkerService::new(ServiceConfig::default());
    let mut no_dirs = valid_restore();
    no_dirs.actor_dirs = None;
    let mut no_restore_dir = valid_restore();
    no_restore_dir
        .actor_dirs
        .as_mut()
        .unwrap()
        .restore_dir
        .clear();
    for (name, req, want) in [
        ("nil actor_dirs", no_dirs, "actor_dirs"),
        ("empty restore_dir", no_restore_dir, "restore_dir"),
    ] {
        let err = s
            .restore_workload(req, CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{name}");
        assert!(err.message().contains(want), "{name}: {}", err.message());
    }
}

#[tokio::test]
async fn memory_restore_requires_kernel_and_image() {
    let s = WorkerService::new(ServiceConfig::default());
    let p = |kv: &[(&str, &str)]| {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>()
    };
    for (name, paths) in [
        ("no assets", p(&[])),
        (
            "no kernel",
            p(&[(
                ASSET_IMAGE,
                "/node/actors/actor-a/sandbox-assets/kata-image",
            )]),
        ),
        (
            "no image",
            p(&[(
                ASSET_KERNEL,
                "/node/actors/actor-a/sandbox-assets/kata-kernel",
            )]),
        ),
        (
            "empty path",
            p(&[
                (ASSET_KERNEL, ""),
                (
                    ASSET_IMAGE,
                    "/node/actors/actor-a/sandbox-assets/kata-image",
                ),
            ]),
        ),
    ] {
        let bp = BootParams {
            attribution: hivekeep_sandbox_gvisor::attribution::uid_only("actor-a"),
            actor_dirs: Default::default(),
            containers: vec![],
            asset_paths: paths,
            egress_gateway: None,
            size: Default::default(),
            rootfs_upper_snapshot_dir: None,
        };
        let d = tempfile::tempdir().unwrap();
        let err = s
            .restore_memory_fidelity(
                &bp,
                pb::SnapshotFidelity::Memory as i32,
                d.path(),
                false,
                Instant::now(),
                &CancellationToken::new(),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(ASSET_KERNEL) && err.contains(ASSET_IMAGE),
            "{name}: {err}"
        );
    }
}

#[test]
fn nonces_differ() {
    let (a, b) = (new_reseed_nonce().unwrap(), new_reseed_nonce().unwrap());
    assert_eq!(a.len(), 32);
    assert_ne!(a, b, "two nonces are identical; clones would not diverge");
}

struct FakeReseeder {
    calls: AtomicUsize,
    got: parking_lot::Mutex<Vec<u8>>,
    fail: bool,
}

#[async_trait::async_trait]
impl RandomDevReseeder for FakeReseeder {
    async fn reseed_random_dev(&self, data: &[u8]) -> std::result::Result<(), AgentError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *self.got.lock() = data.to_vec();
        if self.fail {
            return Err(AgentError {
                method: "ReseedRandomDev",
                source: TtrpcError::Codec("agent busy".into()),
            });
        }
        Ok(())
    }
}

#[tokio::test]
async fn reseeds_once_with_a_fresh_nonce() {
    let f = FakeReseeder {
        calls: AtomicUsize::new(0),
        got: Default::default(),
        fail: false,
    };
    reseed_guest_crng(&f).await.unwrap();
    assert_eq!(f.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(f.got.lock().len(), 32);
}

#[tokio::test]
async fn reseed_returns_the_agents_error_without_retrying() {
    let f = FakeReseeder {
        calls: AtomicUsize::new(0),
        got: Default::default(),
        fail: true,
    };
    let err = reseed_guest_crng(&f).await.unwrap_err();
    assert!(err.to_string().contains("agent busy"), "{err}");
    assert_eq!(f.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn the_reseed_is_bounded() {
    struct Hang;
    #[async_trait::async_trait]
    impl RandomDevReseeder for Hang {
        async fn reseed_random_dev(&self, _: &[u8]) -> std::result::Result<(), AgentError> {
            futures::future::pending().await
        }
    }
    let start = tokio::time::Instant::now();
    let err = reseed_guest_crng(&Hang).await.unwrap_err();
    assert!(err.is_context_error(), "{err}");
    assert_eq!(start.elapsed(), RESEED_TIMEOUT);
}

#[test]
fn classifies_guest_ready_errors() {
    let cause = || Error::Context {
        context: "agent ReseedRandomDev".into(),
        source: Box::new(Error::DeadlineExceeded),
    };
    let live = CancellationToken::new();
    let e = restore_guest_ready_error(&live, "reseeding guest CRNG", cause());
    assert_eq!(e.code(), tonic::Code::Unavailable, "{e}");
    assert!(e.to_string().contains("agent ReseedRandomDev"));
    let gone = CancellationToken::new();
    gone.cancel();
    let e = restore_guest_ready_error(&gone, "dialing kata-agent after resume", cause());
    assert_eq!(e.code(), tonic::Code::Cancelled, "{e}");
    let stopped = Error::Context {
        context: format!(
            "{} (cloud-hypervisor removed /run/vsock.sock)",
            crate::boot::GUEST_STOPPED
        ),
        source: Box::new(Error::io(
            "dial",
            std::io::Error::from(std::io::ErrorKind::NotFound),
        )),
    };
    let e = restore_guest_ready_error(&live, "dialing kata-agent after resume", stopped);
    assert_eq!(e.code(), tonic::Code::Internal, "{e}");
    assert!(is_guest_stopped(&e));
}

#[test]
fn memory_restore_mode_follows_the_vmm() {
    let v = |s: &str| VmmInfo {
        version: s.into(),
        ..Default::default()
    };
    assert_eq!(restore_mem_mode(&v("52.0.0")), MEM_RESTORE_ON_DEMAND);
    assert_eq!(restore_mem_mode(&v("53.0.0")), MEM_RESTORE_EAGER);
    assert_eq!(restore_mem_mode(&VmmInfo::default()), MEM_RESTORE_EAGER);
}

#[test]
fn boot_params_attribution_matches_the_request() {
    let req = pb::RunWorkloadRequest {
        hive: "hive-a".into(),
        actor_name: "actor-b".into(),
        actor_uid: "cccccccc-0000-4000-8000-000000000003".into(),
        actor_template_hive: "template-ns-d".into(),
        actor_template_name: "template-name-e".into(),
        ..Default::default()
    };
    let a = req.attribution();
    assert_eq!(a.actor.hive, "hive-a");
    assert_eq!(a.actor.name, "actor-b");
    assert_eq!(a.uid, "cccccccc-0000-4000-8000-000000000003");
    assert_eq!(a.template_hive, "template-ns-d");
    assert_eq!(a.template_name, "template-name-e");
    let empty = pb::RunWorkloadRequest::default().attribution();
    assert!(empty.uid.is_empty() && empty.actor.hive.is_empty() && empty.template_name.is_empty());
}

#[tokio::test]
async fn a_failed_restores_rootfs_is_cleaned_up() {
    let root = tempfile::tempdir().unwrap();
    let bundles = tempfile::tempdir().unwrap();
    let dirs = pb::ActorDirs {
        root_dir: root.path().to_string_lossy().into(),
        oci_bundle_dir: bundles.path().to_string_lossy().into(),
        ..Default::default()
    };
    let upper = rootfs_upper_dir(&dirs);
    std::fs::create_dir_all(upper.join("app/data")).unwrap();
    std::fs::write(upper.join("app/data/state"), "restored").unwrap();
    let s = WorkerService::new(ServiceConfig {
        cgroup_root: root.path().join("no-cgroups"),
        ..Default::default()
    });
    let uid = uuid::Uuid::new_v4().to_string();
    s.cleanup_failed_restore_rootfs(&uid, &dirs).await;
    assert!(!upper.exists(), "the rootfs upper dir survived");
}

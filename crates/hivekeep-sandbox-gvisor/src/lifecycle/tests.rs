//! The handlers against a fake runsc, a fake prober and fixture directories.

use super::*;
use crate::node_path;
use crate::ports::Dialer;
use crate::runsc::RuntimeFactory;
use crate::service::ServiceConfig;
use crate::wakeup_probe::WakeupProber;
use async_trait::async_trait;

const UID: &str = "aaaaaaaa-0000-4000-8000-000000000001";

/// Records every runsc operation. `fail` makes the named `op container`
/// fail. A checkpoint writes the files runsc would.
#[derive(Default)]
struct FakeRunsc {
    calls: Mutex<Vec<String>>,
    fail: Mutex<Option<String>>,
}

impl FakeRunsc {
    fn record(&self, call: String) -> Result<()> {
        let failing = self.fail.lock().as_deref() == Some(call.as_str());
        self.calls.lock().push(call.clone());
        if failing {
            Err(Error::msg(format!("{call}: exit status 1")))
        } else {
            Ok(())
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }
}

#[async_trait]
impl ContainerRuntime for FakeRunsc {
    async fn create(
        &self,
        _: &CancellationToken,
        _: &Output,
        c: &str,
        extra: &[String],
    ) -> Result<()> {
        self.record(format!(
            "create {c}{}",
            if extra.is_empty() {
                String::new()
            } else {
                format!(" {}", extra.join(" "))
            }
        ))
    }
    async fn start(&self, _: &CancellationToken, _: &Output, c: &str) -> Result<()> {
        self.record(format!("start {c}"))
    }
    async fn checkpoint(
        &self,
        _: &CancellationToken,
        c: &str,
        image: &str,
        _: &[String],
    ) -> Result<()> {
        self.record(format!("checkpoint {c}"))?;
        for f in ["checkpoint.img", "pages.img"] {
            std::fs::write(Path::new(image).join(f), b"x").unwrap();
        }
        std::fs::create_dir_all(Path::new(image).join("rootfs")).unwrap();
        std::fs::write(Path::new(image).join("rootfs/fscheckpoint.pb"), b"x").unwrap();
        Ok(())
    }
    async fn fs_checkpoint(
        &self,
        _: &CancellationToken,
        c: &str,
        image: &str,
        paths: &[String],
    ) -> Result<()> {
        self.record(format!("fscheckpoint {c} {}", paths.join(" ")))?;
        std::fs::create_dir_all(Path::new(image).join("fs")).unwrap();
        std::fs::write(Path::new(image).join("fs/fscheckpoint.pb"), b"x").unwrap();
        Ok(())
    }
    async fn restore(&self, _: &CancellationToken, _: &Output, c: &str, _: &str) -> Result<()> {
        self.record(format!("restore {c}"))
    }
    async fn delete(&self, _: &CancellationToken, c: &str) -> Result<()> {
        self.record(format!("delete {c}"))
    }
    async fn state(&self, _: &CancellationToken, c: &str) -> Result<()> {
        self.record(format!("state {c}"))
    }
    async fn list(&self, _: &CancellationToken) -> Result<Vec<String>> {
        self.record("list".into()).map(|()| Vec::new())
    }
    async fn kill(&self, _: &CancellationToken, c: &str, sig: &str) -> Result<()> {
        self.record(format!("kill {c} {sig}"))
    }
    async fn wait(&self, _: &CancellationToken, c: &str) -> Result<()> {
        self.record(format!("wait {c}"))
    }
    async fn wait_restore(&self, _: &CancellationToken, c: &str) -> Result<()> {
        self.record(format!("wait-restore {c}"))
    }
    async fn wait_fs_restore(&self, _: &CancellationToken, c: &str) -> Result<()> {
        self.record(format!("wait-fsrestore {c}"))
    }
}

struct Factory(Arc<FakeRunsc>);

impl RuntimeFactory for Factory {
    fn runtime(&self, _: RunscCommand) -> Arc<dyn ContainerRuntime> {
        self.0.clone()
    }
}

struct NoProbes;

#[async_trait]
impl WakeupProber for NoProbes {
    async fn wait_all(&self, _: &[pb::Container], _: Dialer, _: &CancellationToken) -> Result<()> {
        Ok(())
    }
}

struct Fixture {
    _tmp: tempfile::TempDir,
    svc: Arc<WorkerService>,
    runsc: Arc<FakeRunsc>,
    dirs: pb::ActorDirs,
    runsc_path: String,
}

fn fixture() -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    // Canonical, so the runsc path's symlink check passes on hosts whose
    // temp dir sits behind a symlink.
    let base = std::fs::canonicalize(tmp.path()).unwrap();
    let actors = base.join("actors");
    let assets = node_path::actor_sandbox_assets_dir(&actors, UID);
    std::fs::create_dir_all(&assets).unwrap();
    let runsc_path = assets.join("runsc");
    std::fs::write(&runsc_path, b"").unwrap();
    let root = actors.join(UID);
    let d = |n: &str| root.join(n).display().to_string();
    let dirs = pb::ActorDirs {
        root_dir: d(""),
        oci_bundle_dir: d("bundles"),
        checkpoint_dir: d("checkpoint"),
        restore_dir: d("restore"),
        durable_dir_volume_mounts_dir: d("durable"),
        system_info_volume_roots_dir: d("sysinfo"),
        external_volume_mounts_dir: d("volumes"),
        sandbox_assets_dir: assets.display().to_string(),
    };
    let dirs = pb::ActorDirs {
        root_dir: root.display().to_string(),
        ..dirs
    };
    std::fs::create_dir_all(Path::new(&dirs.oci_bundle_dir).join("app")).unwrap();
    let cg = base.join("cgroup");
    std::fs::create_dir_all(&cg).unwrap();
    let runsc = Arc::new(FakeRunsc::default());
    let svc = WorkerService::new(ServiceConfig {
        cgroup_root: cg,
        actors_dir: actors,
        runtimes: Arc::new(Factory(runsc.clone())),
        prober: Arc::new(NoProbes),
        ..Default::default()
    });
    Fixture {
        _tmp: tmp,
        svc,
        runsc,
        dirs,
        runsc_path: runsc_path.display().to_string(),
    }
}

fn spec() -> pb::WorkloadSpec {
    pb::WorkloadSpec {
        containers: vec![pb::Container {
            name: "app".into(),
            durable_dir_volume_mounts: vec![pb::VolumeMount {
                name: "data".into(),
                mount_path: "/data".into(),
            }],
            ..Default::default()
        }],
    }
}

impl Fixture {
    fn run_req(&self) -> pb::RunWorkloadRequest {
        pb::RunWorkloadRequest {
            hive: "space-a".into(),
            actor_name: "actor-a".into(),
            actor_uid: UID.into(),
            runsc_path: self.runsc_path.clone(),
            spec: Some(spec()),
            actor_dirs: Some(self.dirs.clone()),
            ..Default::default()
        }
    }

    fn checkpoint_req(&self, fidelity: pb::SnapshotFidelity) -> pb::CheckpointWorkloadRequest {
        pb::CheckpointWorkloadRequest {
            hive: "space-a".into(),
            actor_name: "actor-a".into(),
            actor_uid: UID.into(),
            runsc_path: self.runsc_path.clone(),
            spec: Some(spec()),
            fidelity: fidelity as i32,
            actor_dirs: Some(self.dirs.clone()),
            ..Default::default()
        }
    }

    fn restore_req(&self, fidelity: pb::SnapshotFidelity) -> pb::RestoreWorkloadRequest {
        pb::RestoreWorkloadRequest {
            hive: "space-a".into(),
            actor_name: "actor-a".into(),
            actor_uid: UID.into(),
            runsc_path: self.runsc_path.clone(),
            spec: Some(spec()),
            fidelity: fidelity as i32,
            actor_dirs: Some(self.dirs.clone()),
            ..Default::default()
        }
    }

    fn terminate_req(&self) -> pb::TerminateWorkloadRequest {
        pb::TerminateWorkloadRequest {
            hive: "space-a".into(),
            actor_name: "actor-a".into(),
            actor_uid: UID.into(),
            runsc_path: self.runsc_path.clone(),
            spec: Some(spec()),
            actor_dirs: Some(self.dirs.clone()),
            ..Default::default()
        }
    }
}

fn tok() -> CancellationToken {
    CancellationToken::new()
}

#[tokio::test]
async fn empty_requests_are_invalid() {
    let f = fixture();
    let s = &f.svc;
    let codes = [
        s.run_workload(Default::default(), tok())
            .await
            .unwrap_err()
            .code(),
        s.restore_workload(Default::default(), tok())
            .await
            .unwrap_err()
            .code(),
        s.checkpoint_workload(Default::default(), tok())
            .await
            .unwrap_err()
            .code(),
        s.terminate_workload_rpc(Default::default(), tok())
            .await
            .unwrap_err()
            .code(),
        s.get_workload_stats(Default::default())
            .await
            .unwrap_err()
            .code(),
    ];
    assert!(
        codes.iter().all(|c| *c == tonic::Code::InvalidArgument),
        "{codes:?}"
    );
}

#[tokio::test]
async fn startups_wait_for_registration() {
    let f = fixture();
    let err = f.svc.run_workload(f.run_req(), tok()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable);
    let err = f
        .svc
        .restore_workload(f.restore_req(pb::SnapshotFidelity::Memory), tok())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert!(f.runsc.calls().is_empty());
}

#[tokio::test]
async fn untrusted_runsc_paths_are_refused() {
    let f = fixture();
    f.svc.epoch.store(1);
    let mut run = f.run_req();
    run.runsc_path = "/bin/sh".into();
    let mut restore = f.restore_req(pb::SnapshotFidelity::Memory);
    restore.runsc_path = "/bin/sh".into();
    let mut ckpt = f.checkpoint_req(pb::SnapshotFidelity::Memory);
    ckpt.runsc_path = "/bin/sh".into();
    let mut term = f.terminate_req();
    term.runsc_path = "/bin/sh".into();
    let errs = [
        f.svc.run_workload(run, tok()).await.unwrap_err(),
        f.svc.restore_workload(restore, tok()).await.unwrap_err(),
        f.svc.checkpoint_workload(ckpt, tok()).await.unwrap_err(),
        f.svc.terminate_workload_rpc(term, tok()).await.unwrap_err(),
    ];
    for e in errs {
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
        assert!(e.message().contains("runsc_path"), "{}", e.message());
    }
    assert!(f.runsc.calls().is_empty());
}

#[tokio::test]
async fn bad_fidelity_is_refused_before_the_sandbox() {
    let f = fixture();
    f.svc.epoch.store(1);
    for fidelity in [0, 99] {
        let mut r = f.restore_req(pb::SnapshotFidelity::Memory);
        r.fidelity = fidelity;
        let e = f.svc.restore_workload(r, tok()).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
        assert!(e.message().contains("fidelity"), "{}", e.message());
        let mut c = f.checkpoint_req(pb::SnapshotFidelity::Memory);
        c.fidelity = fidelity;
        let e = f.svc.checkpoint_workload(c, tok()).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
    }
    assert!(f.runsc.calls().is_empty());
}

#[tokio::test]
async fn run_boots_sandbox_then_containers() {
    let f = fixture();
    f.svc.epoch.store(7);
    let resp = f.svc.run_workload(f.run_req(), tok()).await.unwrap();
    assert_eq!(resp.worker_epoch, 7);
    assert_eq!(
        f.runsc.calls(),
        [
            "create _sandbox",
            "start _sandbox",
            "create app",
            "start app"
        ]
    );
    let h = f.svc.lookup_actor(UID).expect("hosted");
    assert_eq!(h.attribution.actor.name, "actor-a");
    assert_eq!(h.session().unwrap().containers, ["app"]);
    assert!(Path::new(&f.dirs.oci_bundle_dir)
        .join("app/rootfs/etc/hosts")
        .exists());
    assert!(f.svc.in_flight.names().is_empty());
    assert!(!f.svc.locks.busy(UID));
}

#[tokio::test]
async fn failed_run_cleans_up_what_it_created() {
    let f = fixture();
    f.svc.epoch.store(1);
    *f.runsc.fail.lock() = Some("start app".into());
    let err = f.svc.run_workload(f.run_req(), tok()).await.unwrap_err();
    assert!(
        err.message()
            .contains("while starting \"app\" application container"),
        "{}",
        err.message()
    );
    assert_eq!(
        f.runsc.calls(),
        [
            "create _sandbox",
            "start _sandbox",
            "create app",
            "start app",
            "delete app",
            "delete _sandbox"
        ]
    );
    assert!(f.svc.lookup_actor(UID).is_none());
    assert_eq!(f.svc.table.read().draining, 0);
}

#[tokio::test]
async fn startups_are_refused_while_draining() {
    let f = fixture();
    f.svc.epoch.store(1);
    f.svc.shutting_down.store(true, Ordering::SeqCst);
    let err = f.svc.run_workload(f.run_req(), tok()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert!(f.runsc.calls().is_empty());
}

#[tokio::test]
async fn memory_checkpoint_saves_and_tears_down() {
    let f = fixture();
    f.svc.epoch.store(1);
    f.svc.run_workload(f.run_req(), tok()).await.unwrap();
    f.runsc.calls.lock().clear();
    let resp = f
        .svc
        .checkpoint_workload(f.checkpoint_req(pb::SnapshotFidelity::Memory), tok())
        .await
        .unwrap();
    assert_eq!(
        resp.snapshot_files,
        ["checkpoint.img", "pages.img", "rootfs/fscheckpoint.pb"]
    );
    assert!(resp.data_snapshot_files.is_empty());
    assert_eq!(
        f.runsc.calls(),
        [
            "checkpoint _sandbox",
            "kill app SIGKILL",
            "wait app",
            "state app",
            "state _sandbox",
            "delete app",
            "delete _sandbox"
        ]
    );
    assert!(
        f.svc.lookup_actor(UID).is_none(),
        "the checkpoint unhosts the Actor"
    );

    // A retry after the caller gave up reuses the finished checkpoint.
    f.runsc.calls.lock().clear();
    let again = f
        .svc
        .checkpoint_workload(f.checkpoint_req(pb::SnapshotFidelity::Memory), tok())
        .await
        .unwrap();
    assert_eq!(again, resp);
    assert!(f.runsc.calls().is_empty());
}

#[tokio::test]
async fn volumes_checkpoint_saves_durable_dirs() {
    let f = fixture();
    f.svc.epoch.store(1);
    f.svc.run_workload(f.run_req(), tok()).await.unwrap();
    f.runsc.calls.lock().clear();
    let resp = f
        .svc
        .checkpoint_workload(f.checkpoint_req(pb::SnapshotFidelity::Volumes), tok())
        .await
        .unwrap();
    assert_eq!(f.runsc.calls()[0], "fscheckpoint _sandbox fs/=app:/data");
    assert_eq!(resp.snapshot_files, ["fs/fscheckpoint.pb"]);
    assert_eq!(resp.data_snapshot_files, ["fs/fscheckpoint.pb"]);
}

#[tokio::test]
async fn failed_checkpoint_keeps_the_actor() {
    let f = fixture();
    f.svc.epoch.store(1);
    f.svc.run_workload(f.run_req(), tok()).await.unwrap();
    *f.runsc.fail.lock() = Some("checkpoint _sandbox".into());
    let err = f
        .svc
        .checkpoint_workload(f.checkpoint_req(pb::SnapshotFidelity::Memory), tok())
        .await
        .unwrap_err();
    assert!(
        err.message().contains("while checkpointing the sandbox"),
        "{}",
        err.message()
    );
    let h = f.svc.lookup_actor(UID).expect("still hosted");
    assert!(
        !h.stopping.load(Ordering::SeqCst),
        "a later sandbox exit is a crash again"
    );
}

#[tokio::test]
async fn memory_restore_restores_every_container() {
    let f = fixture();
    f.svc.epoch.store(1);
    let restore = Path::new(&f.dirs.restore_dir);
    std::fs::create_dir_all(restore.join("rootfs")).unwrap();
    std::fs::write(restore.join("rootfs/fscheckpoint.pb"), b"x").unwrap();
    std::fs::write(restore.join("checkpoint.img"), b"x").unwrap();
    f.svc
        .restore_workload(f.restore_req(pb::SnapshotFidelity::Memory), tok())
        .await
        .unwrap();
    let rootfs = restore.join("rootfs").display().to_string();
    let calls = f.runsc.calls();
    assert_eq!(
        calls[..4],
        [
            format!("create _sandbox --fs-restore-image-path {rootfs}"),
            "restore _sandbox".to_string(),
            "create app".to_string(),
            "restore app".to_string(),
        ]
    );
    // Unpreserved and unshared: the image is dropped at once.
    assert!(!restore.join("checkpoint.img").exists());
    assert!(f.svc.lookup_actor(UID).is_some());
}

#[tokio::test]
async fn rootfs_restore_needs_the_rootfs_bundle() {
    let f = fixture();
    f.svc.epoch.store(1);
    let err = f
        .svc
        .restore_workload(f.restore_req(pb::SnapshotFidelity::Rootfs), tok())
        .await
        .unwrap_err();
    assert!(err.message().contains("rootfs"), "{}", err.message());
    assert!(f.svc.lookup_actor(UID).is_none());
}

#[tokio::test]
async fn terminate_tears_everything_down() {
    let f = fixture();
    f.svc.epoch.store(1);
    f.svc.run_workload(f.run_req(), tok()).await.unwrap();
    f.runsc.calls.lock().clear();
    f.svc
        .terminate_workload_rpc(f.terminate_req(), tok())
        .await
        .unwrap();
    assert_eq!(
        f.runsc.calls(),
        [
            "kill app SIGKILL",
            "wait app",
            "state app",
            "state _sandbox",
            "delete app",
            "delete _sandbox"
        ]
    );
    assert!(f.svc.lookup_actor(UID).is_none());
    // Terminating again is fine.
    f.svc
        .terminate_workload_rpc(f.terminate_req(), tok())
        .await
        .unwrap();
}

#[tokio::test]
async fn graceful_shutdown_signals_hosted_containers() {
    let f = fixture();
    f.svc.epoch.store(1);
    f.svc.run_workload(f.run_req(), tok()).await.unwrap();
    f.runsc.calls.lock().clear();
    f.svc.graceful_shutdown().await;
    assert_eq!(f.runsc.calls(), ["kill app SIGTERM", "wait app"]);
}

/// A restore without its restore dir is refused up front, naming what is
/// missing, before any lock or runsc work.
#[tokio::test]
async fn restore_without_a_restore_dir_is_invalid() {
    let f = fixture();
    f.svc.epoch.store(1);
    let mut no_dirs = f.restore_req(pb::SnapshotFidelity::Memory);
    no_dirs.actor_dirs = None;
    let mut no_restore_dir = f.restore_req(pb::SnapshotFidelity::Memory);
    no_restore_dir
        .actor_dirs
        .as_mut()
        .unwrap()
        .restore_dir
        .clear();
    for (name, req, want) in [
        ("no actor_dirs", no_dirs, "actor_dirs"),
        ("empty restore_dir", no_restore_dir, "restore_dir"),
    ] {
        let e = f.svc.restore_workload(req, tok()).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{name}");
        assert!(e.message().contains(want), "{name}: {}", e.message());
    }
    assert!(f.runsc.calls().is_empty());
}

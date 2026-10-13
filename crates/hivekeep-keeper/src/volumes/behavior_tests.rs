//! External volumes: publishing, unpublishing, and their mount points'
//! lifecycle with the actor's directories.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use parking_lot::Mutex;

use super::*;
use crate::actordirs::reset_actor_dirs;

#[derive(Debug, Clone, PartialEq, Eq)]
struct MountCall {
    volume_id: String,
    target_path: String,
    attributes: BTreeMap<String, String>,
}

#[derive(Default)]
struct Plugin {
    mount_err: Option<Status>,
    mount_errs: HashMap<String, Status>,
    unmount_err: Option<Status>,
    mounts: Mutex<Vec<MountCall>>,
    unmounted: Mutex<Vec<String>>,
}

#[async_trait]
impl VolumePlugin for Plugin {
    async fn mount_volume(&self, _: &Ctx, req: MountVolumeRequest) -> Result<(), Status> {
        self.mounts.lock().push(MountCall {
            volume_id: req.volume_id.clone(),
            target_path: req.target_path,
            attributes: req.volume_context,
        });
        if let Some(e) = self.mount_errs.get(&req.volume_id) {
            return Err(e.clone());
        }
        match &self.mount_err {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
    async fn unmount_volume(&self, _: &Ctx, id: &str, _: &str) -> Result<(), Status> {
        self.unmounted.lock().push(id.into());
        match &self.unmount_err {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
}

fn ext(name: &str, id: &str, driver: &str, ctx: &[(&str, &str)]) -> pb::Volume {
    pb::Volume {
        name: name.into(),
        external: Some(pb::ExternalVolumeSource {
            storage_volume_id: id.into(),
            volume_type: driver.into(),
            volume_context: ctx
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn vol1() -> pb::Volume {
    ext("vol-1", "mock-vol-1", "mock-driver", &[("key", "val1")])
}

fn vol2() -> pb::Volume {
    ext("vol-2", "mock-vol-2", "mock-driver", &[("key", "val2")])
}

fn durable() -> pb::Volume {
    pb::Volume {
        name: "durable-1".into(),
        durable_dir: Some(pb::DurableDirVolumeSource {}),
        ..Default::default()
    }
}

struct Env {
    _d: tempfile::TempDir,
    paths: Arc<NodePaths>,
    plugin: Arc<Plugin>,
    m: VolumeManager,
}

fn env(plugin: Plugin) -> Env {
    let d = tempfile::tempdir().unwrap();
    let paths = Arc::new(NodePaths::new(d.path()));
    let plugin = Arc::new(plugin);
    let mut plugins: HashMap<String, Arc<dyn VolumePlugin>> = HashMap::new();
    plugins.insert("mock-driver".into(), plugin.clone());
    let m = VolumeManager::new(paths.clone(), plugins, Arc::new(NoVolumeDrivers));
    Env {
        _d: d,
        paths,
        plugin,
        m,
    }
}

const UID: &str = "test-actor-123";

fn bg() -> Ctx {
    Ctx::background()
}

#[tokio::test]
async fn unmount_success() {
    let e = env(Plugin::default());
    e.m.unmount_external_volumes(&bg(), UID, &[vol1(), durable(), vol2()])
        .await
        .unwrap();
    assert_eq!(*e.plugin.unmounted.lock(), vec!["mock-vol-1", "mock-vol-2"]);
}

#[tokio::test]
async fn unmount_failure_is_blocking() {
    let e = env(Plugin {
        unmount_err: Some(Status::internal("device or resource busy")),
        ..Default::default()
    });
    let err =
        e.m.unmount_external_volumes(&bg(), UID, &[vol1()])
            .await
            .unwrap_err();
    assert!(err.message().contains("device or resource busy"), "{err:?}");
}

#[tokio::test]
async fn unmount_attempts_every_volume() {
    let e = env(Plugin {
        unmount_err: Some(Status::internal("unmount failed")),
        ..Default::default()
    });
    assert!(e
        .m
        .unmount_external_volumes(&bg(), UID, &[vol1(), vol2()])
        .await
        .is_err());
    assert_eq!(e.plugin.unmounted.lock().len(), 2);
}

#[tokio::test]
async fn mount_creates_mount_points() {
    let e = env(Plugin::default());
    e.m.mount_external_volumes(&bg(), UID, &[vol1(), durable(), vol2()])
        .await
        .unwrap();
    let (p1, p2) = (
        e.paths.external_volume_host_path(UID, "vol-1"),
        e.paths.external_volume_host_path(UID, "vol-2"),
    );
    for p in [&p1, &p2] {
        let m = std::fs::metadata(p).unwrap();
        assert!(m.is_dir(), "{}", p.display());
        assert_eq!(m.permissions().mode() & 0o777, 0o750, "{}", p.display());
    }
    let call = |id: &str, p: &Path, v: &str| MountCall {
        volume_id: id.into(),
        target_path: p.to_string_lossy().into_owned(),
        attributes: BTreeMap::from([("key".to_string(), v.to_string())]),
    };
    assert_eq!(
        *e.plugin.mounts.lock(),
        vec![
            call("mock-vol-1", &p1, "val1"),
            call("mock-vol-2", &p2, "val2")
        ]
    );
}

#[tokio::test]
async fn mount_into_an_existing_mount_point() {
    let e = env(Plugin::default());
    let p = e.paths.external_volume_host_path(UID, "vol-1");
    std::fs::create_dir_all(&p).unwrap();
    e.m.mount_external_volumes(&bg(), UID, &[vol1()])
        .await
        .unwrap();
    let mounts = e.plugin.mounts.lock();
    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0].target_path, p.to_string_lossy());
}

#[tokio::test]
async fn mount_with_an_unknown_driver_fails() {
    let e = env(Plugin::default());
    let err =
        e.m.mount_external_volumes(
            &bg(),
            UID,
            &[ext(
                "vol-unknown",
                "mock-vol-unknown",
                "unknown-driver",
                &[],
            )],
        )
        .await
        .unwrap_err();
    assert!(err.message().contains("unknown-driver"), "{err:?}");
}

#[tokio::test]
async fn mount_failure_is_returned() {
    let e = env(Plugin {
        mount_err: Some(Status::internal(
            "mount operation failed: device or resource busy",
        )),
        ..Default::default()
    });
    let err =
        e.m.mount_external_volumes(&bg(), UID, &[vol1()])
            .await
            .unwrap_err();
    assert!(err.message().contains("device or resource busy"), "{err:?}");
}

#[tokio::test]
async fn mount_stops_at_the_first_failure() {
    let e = env(Plugin {
        mount_err: Some(Status::internal("cannot mount volume")),
        ..Default::default()
    });
    assert!(e
        .m
        .mount_external_volumes(&bg(), UID, &[vol1(), vol2()])
        .await
        .is_err());
    assert_eq!(
        e.plugin.mounts.lock().len(),
        1,
        "mounted past the first failure"
    );
}

/// A later failure leaves earlier volumes mounted, for the caller's
/// teardown to unmount.
#[tokio::test]
async fn mount_failure_does_not_roll_back() {
    let e = env(Plugin {
        mount_errs: HashMap::from([(
            "mock-vol-2".to_string(),
            Status::internal("simulated mount error on vol-2"),
        )]),
        ..Default::default()
    });
    let err =
        e.m.mount_external_volumes(&bg(), UID, &[vol1(), vol2()])
            .await
            .unwrap_err();
    assert!(
        err.message().contains("simulated mount error on vol-2"),
        "{err:?}"
    );
    let ids: Vec<String> = e
        .plugin
        .mounts
        .lock()
        .iter()
        .map(|c| c.volume_id.clone())
        .collect();
    assert_eq!(ids, vec!["mock-vol-1", "mock-vol-2"]);
    assert!(
        e.plugin.unmounted.lock().is_empty(),
        "vol-1 was rolled back"
    );
    for v in ["vol-1", "vol-2"] {
        assert!(e.paths.external_volume_host_path(UID, v).exists(), "{v}");
    }
}

const CLEANUP_UID: &str = "test-actor-cleanup";

#[tokio::test]
async fn unmount_keeps_mount_points() {
    let e = env(Plugin::default());
    let vols = [vol1(), vol2()];
    e.m.mount_external_volumes(&bg(), CLEANUP_UID, &vols)
        .await
        .unwrap();
    e.m.unmount_external_volumes(&bg(), CLEANUP_UID, &vols)
        .await
        .unwrap();
    for v in ["vol-1", "vol-2"] {
        assert!(
            e.paths.external_volume_host_path(CLEANUP_UID, v).exists(),
            "{v} removed by unmount"
        );
    }
}

#[tokio::test]
async fn reset_removes_empty_mount_points() {
    let e = env(Plugin::default());
    let vols = [vol1(), vol2()];
    e.m.mount_external_volumes(&bg(), CLEANUP_UID, &vols)
        .await
        .unwrap();
    e.m.unmount_external_volumes(&bg(), CLEANUP_UID, &vols)
        .await
        .unwrap();
    reset_actor_dirs(&e.paths, CLEANUP_UID).unwrap();
    for v in ["vol-1", "vol-2"] {
        assert!(
            !e.paths.external_volume_host_path(CLEANUP_UID, v).exists(),
            "{v} kept by reset"
        );
    }
    let dir = e.paths.external_volume_mounts_dir(CLEANUP_UID);
    assert_eq!(
        std::fs::read_dir(&dir).unwrap().count(),
        0,
        "volumes dir not empty"
    );
}

/// A mount point still holding data is a volume that did not unmount:
/// reset refuses to delete through it.
#[tokio::test]
async fn reset_keeps_a_populated_mount_point() {
    let e = env(Plugin::default());
    e.m.mount_external_volumes(&bg(), CLEANUP_UID, &[vol1()])
        .await
        .unwrap();
    let file = e
        .paths
        .external_volume_host_path(CLEANUP_UID, "vol-1")
        .join("lingering.txt");
    std::fs::write(&file, b"volume data").unwrap();
    let err = reset_actor_dirs(&e.paths, CLEANUP_UID).unwrap_err();
    assert!(
        err.to_string().contains("while removing volume dir"),
        "{err}"
    );
    assert!(file.exists(), "volume data was deleted");
}

#[tokio::test]
async fn reset_without_a_volumes_dir() {
    let e = env(Plugin::default());
    reset_actor_dirs(&e.paths, CLEANUP_UID).unwrap();
    assert!(
        e.paths.external_volume_mounts_dir(CLEANUP_UID).is_dir(),
        "volumes dir not created"
    );
}

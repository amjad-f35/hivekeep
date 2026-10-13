//! External volumes end to end through the fake CSI node driver.

use std::collections::HashMap;
use std::sync::Arc;

use hivekeep_node::csi::fake::{FakeCsiDriver, FakeCsiServer, FAKE_DRIVER_NAME};
use hivekeep_node::csi::pb as csipb;
use hivekeep_node::csi::{CsiDriverConfig, StaticCsiDriverConfigs};
use hivekeep_proto::keeper::v1 as pb;
use parking_lot::Mutex;
use tonic::{Code, Status};

use super::*;
use crate::paths::NodePaths;
use crate::volumes::VolumeManager;

struct Harness {
    driver: Arc<FakeCsiDriver>,
    _server: FakeCsiServer,
    paths: Arc<NodePaths>,
    manager: VolumeManager,
    _dir: tempfile::TempDir,
}

async fn start() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let paths = Arc::new(NodePaths::new(dir.path().join("base")));
    let driver = FakeCsiDriver::new();
    let server = driver
        .serve_unix(&dir.path().join("csi.sock"))
        .await
        .unwrap();
    let configs = StaticCsiDriverConfigs(vec![CsiDriverConfig {
        driver_name: FAKE_DRIVER_NAME.into(),
        controller_endpoint: "tcp://unused:1".into(),
        node_socket_override: server.endpoint.clone(),
        tls: None,
    }]);
    let factory = CsiVolumeDrivers::new(Arc::new(configs), paths.csi_staging_dir());
    let manager = VolumeManager::new(paths.clone(), HashMap::new(), Arc::new(factory));
    Harness {
        driver,
        _server: server,
        paths,
        manager,
        _dir: dir,
    }
}

fn ext(name: &str, driver: &str) -> pb::Volume {
    pb::Volume {
        name: name.into(),
        external: Some(pb::ExternalVolumeSource {
            storage_volume_id: format!("vol-{name}"),
            volume_type: driver.into(),
            volume_context: [("share".to_string(), name.to_string())].into(),
            publish_context: [("devicePath".to_string(), "/dev/xvdba".to_string())].into(),
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn mounts_stage_then_publish_at_the_actor_mount_point() {
    let h = start().await;
    let published = Arc::new(Mutex::new(Vec::new()));
    let p = published.clone();
    h.driver.node_publish_volume.set(move |r| {
        p.lock().push(r);
        Ok(csipb::NodePublishVolumeResponse {})
    });
    h.manager
        .mount_external_volumes(
            &Ctx::background(),
            "actor-1",
            &[ext("a", FAKE_DRIVER_NAME), ext("b", FAKE_DRIVER_NAME)],
        )
        .await
        .unwrap();
    let got = published.lock().clone();
    assert_eq!(got.len(), 2);
    let a = &got[0];
    assert_eq!(a.volume_id, "vol-a");
    assert_eq!(
        a.target_path,
        h.paths
            .external_volume_host_path("actor-1", "a")
            .to_string_lossy()
    );
    assert_eq!(
        a.staging_target_path,
        h.paths.csi_staging_dir().join("vol-a").to_string_lossy()
    );
    assert_eq!(a.volume_context["share"], "a");
    assert_eq!(a.publish_context["devicePath"], "/dev/xvdba");
    assert!(h.paths.external_volume_host_path("actor-1", "a").is_dir());
    // The driver was opened (and probed) once for both volumes.
    assert_eq!(h.driver.count("GetPluginInfo"), 1);
    assert_eq!(h.driver.count("NodeGetCapabilities"), 1);
    assert_eq!(h.driver.count("NodeStageVolume"), 2);
}

#[tokio::test]
async fn unmounts_unpublish_then_unstage() {
    let h = start().await;
    let vols = [ext("a", FAKE_DRIVER_NAME)];
    h.manager
        .mount_external_volumes(&Ctx::background(), "u", &vols)
        .await
        .unwrap();
    h.manager
        .unmount_external_volumes(&Ctx::background(), "u", &vols)
        .await
        .unwrap();
    let calls = h.driver.calls();
    let tail: Vec<&str> = calls
        .iter()
        .map(String::as_str)
        .skip_while(|c| *c != "NodeUnpublishVolume")
        .collect();
    assert_eq!(tail, ["NodeUnpublishVolume", "NodeUnstageVolume"]);
    assert!(!h.paths.csi_staging_dir().join("vol-a").exists());
}

#[tokio::test]
async fn a_volume_the_driver_no_longer_has_counts_as_unmounted() {
    let h = start().await;
    h.driver
        .node_unpublish_volume
        .set(|_| Err(Status::not_found("no such volume")));
    h.manager
        .unmount_external_volumes(&Ctx::background(), "u", &[ext("a", FAKE_DRIVER_NAME)])
        .await
        .unwrap();
    h.driver
        .node_unpublish_volume
        .set(|_| Err(Status::internal("device busy")));
    let err = h
        .manager
        .unmount_external_volumes(&Ctx::background(), "u", &[ext("a", FAKE_DRIVER_NAME)])
        .await
        .unwrap_err();
    assert!(err.message().contains("device busy"), "{err}");
}

#[tokio::test]
async fn publish_failures_and_unknown_drivers_fail_the_mount() {
    let h = start().await;
    h.driver
        .node_publish_volume
        .set(|_| Err(Status::resource_exhausted("no attach slots")));
    let err = h
        .manager
        .mount_external_volumes(&Ctx::background(), "u", &[ext("a", FAKE_DRIVER_NAME)])
        .await
        .unwrap_err();
    assert!(err.message().contains("no attach slots"), "{err}");

    let err = h
        .manager
        .mount_external_volumes(&Ctx::background(), "u", &[ext("b", "unknown.csi.k8s.io")])
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("CSIDriverConfig"), "{err}");
}

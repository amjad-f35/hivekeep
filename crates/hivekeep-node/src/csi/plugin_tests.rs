//! The volume plugin against the fake driver: each operation, and how
//! capabilities decide which optional RPCs are made.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tonic::Status;

use super::client::CsiClient;
use super::driver_config::{CsiDriverConfig, StaticCsiDriverConfigs};
use super::fake::{controller_caps, node_caps, FakeCsiDriver, FakeCsiServer, FAKE_DRIVER_NAME};
use super::open::{open_csi_plugin_with, OpenOptions, Role};
use super::pb;
use super::plugin::{
    AttachVolumeRequest, ControllerRpc, CreateVolumeRequest, CsiPlugin, MountVolumeRequest, NodeRpc,
};

struct Harness {
    driver: Arc<FakeCsiDriver>,
    _server: FakeCsiServer,
    plugin: CsiPlugin,
    dir: tempfile::TempDir,
}

async fn start(driver: Arc<FakeCsiDriver>) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let server = driver
        .serve_unix(&dir.path().join("csi.sock"))
        .await
        .unwrap();
    let plugin = CsiPlugin::new(CsiClient::new(&server.endpoint, None).unwrap())
        .with_staging_dir(dir.path().join("staging"));
    Harness {
        driver,
        _server: server,
        plugin,
        dir,
    }
}

fn mount(volume_id: &str, target: &std::path::Path) -> MountVolumeRequest {
    MountVolumeRequest {
        volume_id: volume_id.into(),
        target_path: target.to_string_lossy().into_owned(),
        ..Default::default()
    }
}

fn attach() -> AttachVolumeRequest {
    AttachVolumeRequest {
        volume_id: "test-vol".into(),
        node: "node-1".into(),
    }
}

#[tokio::test]
async fn plugin_create_volume() {
    let h = start(FakeCsiDriver::new()).await;
    let resp = h
        .plugin
        .create_volume(CreateVolumeRequest {
            name: "test-vol".into(),
            capacity_bytes: 1 << 30,
            parameters: HashMap::new(),
        })
        .await
        .unwrap();
    assert_eq!(resp.volume_id, "test-vol");
}

#[tokio::test]
async fn plugin_create_volume_requests_one_node_writer_mount() {
    let h = start(FakeCsiDriver::new()).await;
    h.driver.create_volume.set(|r| {
        assert_eq!(r.capacity_range.unwrap().required_bytes, 42);
        assert_eq!(r.parameters["type"], "ssd");
        let cap = &r.volume_capabilities[0];
        assert_eq!(
            cap.access_mode.unwrap().mode,
            pb::volume_capability::access_mode::Mode::SingleNodeWriter as i32
        );
        assert!(matches!(
            cap.access_type,
            Some(pb::volume_capability::AccessType::Mount(_))
        ));
        Ok(pb::CreateVolumeResponse {
            volume: Some(pb::Volume {
                volume_id: "vol-1".into(),
                volume_context: [("k".to_string(), "v".to_string())].into(),
                ..Default::default()
            }),
        })
    });
    let resp = h
        .plugin
        .create_volume(CreateVolumeRequest {
            name: "n".into(),
            capacity_bytes: 42,
            parameters: [("type".to_string(), "ssd".to_string())].into(),
        })
        .await
        .unwrap();
    assert_eq!(resp.volume_id, "vol-1");
    assert_eq!(resp.volume_context["k"], "v");

    h.driver
        .create_volume
        .set(|_| Ok(pb::CreateVolumeResponse { volume: None }));
    let err = h
        .plugin
        .create_volume(CreateVolumeRequest::default())
        .await
        .unwrap_err();
    assert!(err.message().contains("nil volume"), "{err}");
    h.driver
        .create_volume
        .set(|_| Err(Status::resource_exhausted("full")));
    let err = h
        .plugin
        .create_volume(CreateVolumeRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert!(
        err.message().starts_with("CSI CreateVolume failed"),
        "{err}"
    );
}

#[tokio::test]
async fn plugin_delete_volume() {
    let h = start(FakeCsiDriver::new()).await;
    h.plugin.delete_volume("test-vol").await.unwrap();
    h.driver
        .delete_volume
        .set(|_| Err(Status::not_found("gone")));
    // The code survives wrapping, so callers can treat NotFound as done.
    assert_eq!(
        h.plugin.delete_volume("test-vol").await.unwrap_err().code(),
        tonic::Code::NotFound
    );
}

#[tokio::test]
async fn plugin_attach_volume() {
    let h = start(FakeCsiDriver::new()).await;
    h.plugin.init_controller_capabilities().await;
    let resp = h.plugin.attach_volume(attach()).await.unwrap();
    assert!(
        resp.publish_context.is_empty(),
        "expected no publish context, got {:?}",
        resp.publish_context
    );

    h.driver.controller_publish_volume.set(|r| {
        assert_eq!(r.node_id, "node-1");
        assert_eq!(r.volume_id, "test-vol");
        Ok(pb::ControllerPublishVolumeResponse {
            publish_context: [("devicePath".to_string(), "/dev/xvdba".to_string())].into(),
        })
    });
    let resp = h.plugin.attach_volume(attach()).await.unwrap();
    assert_eq!(
        resp.publish_context,
        HashMap::from([("devicePath".to_string(), "/dev/xvdba".to_string())])
    );

    // Unimplemented is not an error.
    h.driver.controller_publish_volume.set(|r| {
        assert_eq!(r.node_id, "node-1");
        Err(Status::unimplemented("unimplemented"))
    });
    let resp = h.plugin.attach_volume(attach()).await.unwrap();
    assert!(resp.publish_context.is_empty());
}

#[tokio::test]
async fn plugin_detach_volume() {
    let h = start(FakeCsiDriver::new()).await;
    h.plugin.init_controller_capabilities().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    h.driver.controller_unpublish_volume.set(move |r| {
        c.fetch_add(1, Ordering::SeqCst);
        assert_eq!(r.node_id, "node-1");
        assert_eq!(r.volume_id, "test-vol");
        Ok(pb::ControllerUnpublishVolumeResponse {})
    });
    h.plugin.detach_volume("test-vol", "node-1").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let c = calls.clone();
    h.driver.controller_unpublish_volume.set(move |r| {
        c.fetch_add(1, Ordering::SeqCst);
        assert_eq!(r.node_id, "node-1");
        Err(Status::unimplemented("unimplemented"))
    });
    h.plugin.detach_volume("test-vol", "node-1").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn plugin_mount_volume() {
    let h = start(FakeCsiDriver::new()).await;
    let target = h.dir.path().join("target");
    h.plugin.init_node_capabilities().await;
    let stage_ctx = Arc::new(parking_lot::Mutex::new(None));
    let s = stage_ctx.clone();
    h.driver.node_stage_volume.set(move |r| {
        *s.lock() = Some((r.staging_target_path.clone(), r.publish_context.clone()));
        Ok(pb::NodeStageVolumeResponse {})
    });
    let publish_staging = Arc::new(parking_lot::Mutex::new(String::new()));
    let p = publish_staging.clone();
    h.driver.node_publish_volume.set(move |r| {
        *p.lock() = r.staging_target_path.clone();
        Ok(pb::NodePublishVolumeResponse {})
    });
    h.plugin
        .mount_volume(MountVolumeRequest {
            publish_context: BTreeMap::from([("devicePath".to_string(), "/dev/xvdba".to_string())]),
            ..mount("test-vol", &target)
        })
        .await
        .unwrap();
    let staging = h.plugin.staging_path("test-vol");
    assert!(
        staging.is_dir(),
        "staging directory {} was not created",
        staging.display()
    );
    let (stage_path, stage_publish) = stage_ctx.lock().clone().unwrap();
    assert_eq!(stage_path, staging.to_string_lossy());
    assert_eq!(stage_publish["devicePath"], "/dev/xvdba");
    assert_eq!(*publish_staging.lock(), staging.to_string_lossy());

    // NodeStageVolume Unimplemented: the mount still succeeds.
    h.driver
        .node_stage_volume
        .set(|_| Err(Status::unimplemented("unimplemented")));
    std::fs::remove_dir_all(h.plugin.staging_dir()).unwrap();
    std::fs::create_dir_all(h.plugin.staging_dir()).unwrap();
    h.plugin
        .mount_volume(mount("test-vol-2", &target))
        .await
        .unwrap();
}

#[tokio::test]
async fn plugin_unmount_volume() {
    let h = start(FakeCsiDriver::new()).await;
    let target = h.dir.path().join("target");
    let staging = h.plugin.staging_path("test-vol");
    std::fs::create_dir_all(&staging).unwrap();
    h.plugin.init_node_capabilities().await;
    h.plugin
        .unmount_volume("test-vol", &target.to_string_lossy())
        .await
        .unwrap();
    assert!(
        !staging.exists(),
        "staging directory {} was not deleted",
        staging.display()
    );

    // NodeUnstageVolume Unimplemented: the unmount still succeeds.
    h.driver
        .node_unstage_volume
        .set(|_| Err(Status::unimplemented("unimplemented")));
    std::fs::create_dir_all(&staging).unwrap();
    h.plugin
        .unmount_volume("test-vol", &target.to_string_lossy())
        .await
        .unwrap();

    // Unpublish failures are returned with their code.
    h.driver
        .node_unpublish_volume
        .set(|_| Err(Status::not_found("not mounted")));
    let err = h
        .plugin
        .unmount_volume("test-vol", &target.to_string_lossy())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn capabilities_skip_attach_detach_when_unsupported() {
    let driver = FakeCsiDriver::new();
    // Only CREATE_DELETE_VOLUME, like the hostpath driver.
    driver
        .controller_get_capabilities
        .set(|_| Ok(controller_caps(&[ControllerRpc::CreateDeleteVolume])));
    let h = start(driver).await;
    h.plugin.init_controller_capabilities().await;
    assert!(!h
        .plugin
        .supports_controller_capability(ControllerRpc::PublishUnpublishVolume));
    h.plugin.attach_volume(attach()).await.unwrap();
    assert_eq!(h.driver.count("ControllerPublishVolume"), 0);
    h.plugin.detach_volume("test-vol", "node-1").await.unwrap();
    assert_eq!(h.driver.count("ControllerUnpublishVolume"), 0);
}

#[tokio::test]
async fn capabilities_skip_stage_unstage_when_unsupported() {
    let driver = FakeCsiDriver::new();
    driver
        .node_get_capabilities
        .set(|_| Ok(node_caps(&[NodeRpc::GetVolumeStats])));
    let h = start(driver).await;
    h.plugin.init_node_capabilities().await;
    assert!(!h
        .plugin
        .supports_node_capability(NodeRpc::StageUnstageVolume));
    let target = h.dir.path().join("target");
    std::fs::create_dir_all(&target).unwrap();
    h.plugin
        .mount_volume(mount("test-vol", &target))
        .await
        .unwrap();
    assert_eq!(h.driver.count("NodeStageVolume"), 0);
    h.plugin
        .unmount_volume("test-vol", &target.to_string_lossy())
        .await
        .unwrap();
    assert_eq!(h.driver.count("NodeUnstageVolume"), 0);
    assert_eq!(h.driver.count("NodePublishVolume"), 1);
    assert_eq!(h.driver.count("NodeUnpublishVolume"), 1);
}

#[tokio::test]
async fn capabilities_all_enabled_when_query_fails_or_empty() {
    let cases: [(&str, Option<Status>); 3] = [
        ("Unimplemented", Some(Status::unimplemented("failed"))),
        ("Unavailable", Some(Status::unavailable("failed"))),
        ("Empty", None),
    ];
    for (name, err) in cases {
        let driver = FakeCsiDriver::new();
        let e = err.clone();
        driver.controller_get_capabilities.set(move |_| match &e {
            Some(s) => Err(s.clone()),
            None => Ok(pb::ControllerGetCapabilitiesResponse::default()),
        });
        let e = err.clone();
        driver.node_get_capabilities.set(move |_| match &e {
            Some(s) => Err(s.clone()),
            None => Ok(pb::NodeGetCapabilitiesResponse::default()),
        });
        let h = start(driver).await;
        h.plugin.init_controller_capabilities().await;
        h.plugin.init_node_capabilities().await;
        assert!(
            h.plugin
                .supports_controller_capability(ControllerRpc::PublishUnpublishVolume),
            "{name}"
        );
        assert!(
            h.plugin
                .supports_node_capability(NodeRpc::StageUnstageVolume),
            "{name}"
        );
    }
}

#[tokio::test]
async fn capabilities_disable_attach_detach_after_unimplemented() {
    let driver = FakeCsiDriver::new();
    driver
        .controller_publish_volume
        .set(|_| Err(Status::unimplemented("unimplemented")));
    let h = start(driver).await;
    h.plugin.init_controller_capabilities().await;
    for _ in 0..2 {
        h.plugin.attach_volume(attach()).await.unwrap();
    }
    assert_eq!(h.driver.count("ControllerPublishVolume"), 1);
    assert!(!h
        .plugin
        .supports_controller_capability(ControllerRpc::PublishUnpublishVolume));
    h.plugin.detach_volume("test-vol", "node-1").await.unwrap();
    assert_eq!(h.driver.count("ControllerUnpublishVolume"), 0);
}

#[tokio::test]
async fn capabilities_disable_stage_unstage_after_unimplemented() {
    let driver = FakeCsiDriver::new();
    driver
        .node_stage_volume
        .set(|_| Err(Status::unimplemented("unimplemented")));
    driver.node_publish_volume.set(|r| {
        assert_eq!(
            r.staging_target_path, "",
            "expected empty StagingTargetPath"
        );
        Ok(pb::NodePublishVolumeResponse {})
    });
    let h = start(driver).await;
    let target = h.dir.path().join("target");
    h.plugin.init_node_capabilities().await;
    for _ in 0..2 {
        h.plugin
            .mount_volume(mount("test-vol", &target))
            .await
            .unwrap();
    }
    assert_eq!(h.driver.count("NodeStageVolume"), 1);
    assert!(!h
        .plugin
        .supports_node_capability(NodeRpc::StageUnstageVolume));
    let staging = h.plugin.staging_path("test-vol");
    assert!(
        !staging.exists(),
        "expected staging directory {} to be removed",
        staging.display()
    );
    h.plugin
        .unmount_volume("test-vol", &target.to_string_lossy())
        .await
        .unwrap();
    assert_eq!(h.driver.count("NodeUnstageVolume"), 0);
}

fn config_for(endpoint: &str) -> StaticCsiDriverConfigs {
    StaticCsiDriverConfigs(vec![CsiDriverConfig {
        driver_name: FAKE_DRIVER_NAME.into(),
        controller_endpoint: endpoint.into(),
        node_socket_override: endpoint.into(),
        tls: None,
    }])
}

#[tokio::test]
async fn new_csi_plugin_capability_query_failure_is_not_fatal() {
    for role in [Role::Controller, Role::Node] {
        let driver = FakeCsiDriver::new();
        driver
            .controller_get_capabilities
            .set(|_| Err(Status::unavailable("unavailable")));
        driver
            .node_get_capabilities
            .set(|_| Err(Status::unavailable("unavailable")));
        let dir = tempfile::tempdir().unwrap();
        let server = driver
            .serve_unix(&dir.path().join("csi.sock"))
            .await
            .unwrap();
        let plugin = open_csi_plugin_with(
            &config_for(&server.endpoint),
            FAKE_DRIVER_NAME,
            role,
            OpenOptions::default(),
        )
        .await
        .unwrap_or_else(|e| panic!("{role:?}: {e}"));
        match role {
            Role::Controller => {
                assert!(plugin.supports_controller_capability(ControllerRpc::PublishUnpublishVolume))
            }
            Role::Node => assert!(plugin.supports_node_capability(NodeRpc::StageUnstageVolume)),
        }
    }
}

#[tokio::test]
async fn open_checks_config_reachability_and_name() {
    let dir = tempfile::tempdir().unwrap();
    let driver = FakeCsiDriver::named("other-driver");
    let server = driver
        .serve_unix(&dir.path().join("csi.sock"))
        .await
        .unwrap();
    let getter = config_for(&server.endpoint);

    let err = open_csi_plugin_with(&getter, "missing", Role::Node, OpenOptions::default())
        .await
        .unwrap_err();
    assert!(
        err.message().contains("failed to retrieve CSIDriverConfig"),
        "{err}"
    );

    let err = open_csi_plugin_with(
        &getter,
        FAKE_DRIVER_NAME,
        Role::Node,
        OpenOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(
        err.message().contains("does not match requested name"),
        "{err}"
    );

    let unreachable = config_for(&format!(
        "unix://{}",
        dir.path().join("nothing.sock").display()
    ));
    let err = open_csi_plugin_with(
        &unreachable,
        FAKE_DRIVER_NAME,
        Role::Node,
        OpenOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(err.message().contains("failed to get driver name"), "{err}");

    // The node role stages where it is told to.
    *driver.name.lock() = FAKE_DRIVER_NAME.into();
    let staging = dir.path().join("stage-here");
    let plugin = open_csi_plugin_with(
        &getter,
        FAKE_DRIVER_NAME,
        Role::Node,
        OpenOptions {
            staging_dir: Some(staging.clone()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(plugin.staging_dir(), staging);
}

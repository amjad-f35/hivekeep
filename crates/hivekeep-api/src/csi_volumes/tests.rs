//! The CSI provider against the fake driver and a fake API server.

use std::collections::HashMap;

use hivekeep_k8s::fake::FakeApiServer;
use hivekeep_node::csi::fake::{FakeCsiDriver, FakeCsiServer, FAKE_DRIVER_NAME};
use hivekeep_node::csi::pb as csipb;
use hivekeep_node::csi::{CsiDriverConfig, StaticCsiDriverConfigs};
use hivekeep_proto::api::v1 as pb;
use serde_json::json;

use super::*;
use crate::volumes::{
    actor_volumes, attach_actor_volumes, create_actor_volumes, delete_actor_volumes,
    detach_actor_volumes, external_volumes_status, initial_external_volumes,
};

async fn driver() -> (
    std::sync::Arc<FakeCsiDriver>,
    FakeCsiServer,
    StaticCsiDriverConfigs,
) {
    let d = FakeCsiDriver::new();
    let server = d.serve_tcp().await.unwrap();
    let configs = StaticCsiDriverConfigs(vec![CsiDriverConfig {
        driver_name: FAKE_DRIVER_NAME.into(),
        controller_endpoint: server.endpoint.clone(),
        ..Default::default()
    }]);
    (d, server, configs)
}

fn template(names: &[&str], class: &str) -> pb::ActorTemplate {
    pb::ActorTemplate {
        volumes: names
            .iter()
            .map(|n| pb::Volume {
                name: n.to_string(),
                external: Some(pb::ExternalVolumeSource {
                    template: Some(pb::ExternalVolumeTemplate {
                        capacity_bytes: 1 << 30,
                        storage_class_name: class.into(),
                    }),
                }),
                ..Default::default()
            })
            .collect(),
        containers: vec![pb::Container {
            volume_mounts: names
                .iter()
                .map(|n| pb::VolumeMount {
                    name: n.to_string(),
                    mount_path: format!("/{n}"),
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn file_classes() -> StaticVolumes {
    let fixed = StaticVolumes::new();
    fixed
        .load_storage_classes(
            json!({"fast": {"provisioner": FAKE_DRIVER_NAME, "parameters": {"tier": "ssd"}}})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    fixed
}

#[tokio::test]
async fn provisions_attaches_detaches_and_deletes_through_the_driver() {
    let (d, _server, configs) = driver().await;
    d.create_volume.set(|r| {
        assert_eq!(r.parameters["tier"], "ssd");
        assert_eq!(r.capacity_range.unwrap().required_bytes, 1 << 30);
        Ok(csipb::CreateVolumeResponse {
            volume: Some(csipb::Volume {
                volume_id: format!("pv-{}", r.name),
                volume_context: [("share".to_string(), "s".to_string())].into(),
                ..Default::default()
            }),
        })
    });
    d.controller_publish_volume.set(|r| {
        assert_eq!(r.node_id, "node-1");
        Ok(csipb::ControllerPublishVolumeResponse {
            publish_context: [("devicePath".to_string(), format!("/dev/{}", r.volume_id))].into(),
        })
    });
    let p = CsiVolumes::new(file_classes(), std::sync::Arc::new(configs), None);
    let t = template(&["data"], "fast");

    let init = initial_external_volumes(&p, &t).await.unwrap();
    assert_eq!(init[0].volume_type, FAKE_DRIVER_NAME);
    let (vols, err) = create_actor_volumes(&p, "uid-1", &t, &init).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(vols[0].storage_volume_id, "pv-hivekeep-uid-1-data");
    assert_eq!(vols[0].volume_context["share"], "s");

    let actor = pb::Actor {
        status: Some(pb::ActorStatus {
            external_volumes: external_volumes_status(vols),
            assigned_node: "node-1".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let ctx = attach_actor_volumes(&p, &actor, &t, "node-1")
        .await
        .unwrap();
    assert_eq!(ctx["data"]["devicePath"], "/dev/pv-hivekeep-uid-1-data");
    detach_actor_volumes(&p, &actor, Some(&t)).await.unwrap();
    // A volume the driver no longer has counts as deleted.
    d.delete_volume
        .set(|_| Err(tonic::Status::not_found("gone")));
    delete_actor_volumes(&p, "uid-1", actor_volumes(&actor))
        .await
        .unwrap();

    // The driver was opened and probed once for all of it.
    assert_eq!(d.count("GetPluginInfo"), 1);
    assert_eq!(d.count("ControllerGetCapabilities"), 1);
    assert_eq!(d.count("ControllerUnpublishVolume"), 1);
    assert_eq!(d.count("DeleteVolume"), 1);
}

#[tokio::test]
async fn driver_failures_keep_their_meaning() {
    let (d, _server, configs) = driver().await;
    let p = CsiVolumes::new(file_classes(), std::sync::Arc::new(configs), None);
    let t = template(&["data"], "fast");
    let init = initial_external_volumes(&p, &t).await.unwrap();

    d.create_volume
        .set(|_| Err(tonic::Status::resource_exhausted("quota")));
    let (vols, err) = create_actor_volumes(&p, "u", &t, &init).await;
    assert!(vols[0].storage_volume_id.is_empty());
    assert!(format!("{:?}", err.unwrap()).contains("quota"));

    d.delete_volume
        .set(|_| Err(tonic::Status::internal("backend down")));
    let err = delete_actor_volumes(&p, "u", &init).await.unwrap_err();
    assert!(err.contains("backend down"), "{err}");
}

#[tokio::test]
async fn unknown_drivers_and_classes_are_refused() {
    let (_d, _server, configs) = driver().await;
    let fixed = StaticVolumes::new();
    fixed
        .load_storage_classes(
            json!({"other": {"provisioner": "unknown.csi.k8s.io"}})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    let p = CsiVolumes::new(fixed, std::sync::Arc::new(configs), None);
    assert!(initial_external_volumes(&p, &template(&["v"], "missing"))
        .await
        .is_err());

    let t = template(&["v"], "other");
    let init = initial_external_volumes(&p, &t).await.unwrap();
    let (_, err) = create_actor_volumes(&p, "u", &t, &init).await;
    let msg = format!("{:?}", err.unwrap());
    assert!(msg.contains("CSIDriverConfig"), "{msg}");
    assert!(p.plugin("unknown.csi.k8s.io").await.is_err());
}

#[tokio::test]
async fn storage_classes_come_from_the_file_then_the_cluster() {
    let (_d, _server, configs) = driver().await;
    let fake = FakeApiServer::new();
    let sc: KubeStorageClass = serde_json::from_value(json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
        "metadata": {"name": "csi-hostpath-sc"},
        "provisioner": FAKE_DRIVER_NAME,
        "parameters": {"kind": "hostpath"}
    }))
    .unwrap();
    fake.insert(&sc);
    let shadow: KubeStorageClass = serde_json::from_value(json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
        "metadata": {"name": "fast"},
        "provisioner": "cluster.csi.k8s.io"
    }))
    .unwrap();
    fake.insert(&shadow);
    let p = CsiVolumes::new(
        file_classes(),
        std::sync::Arc::new(configs),
        Some(fake.client()),
    );

    let c = p.storage_class("csi-hostpath-sc").await.unwrap().unwrap();
    assert_eq!(c.provisioner, FAKE_DRIVER_NAME);
    assert_eq!(
        c.parameters,
        HashMap::from([("kind".to_string(), "hostpath".to_string())])
    );
    // The file wins over the cluster.
    assert_eq!(
        p.storage_class("fast").await.unwrap().unwrap().parameters["tier"],
        "ssd"
    );
    assert!(p.storage_class("nope").await.unwrap().is_none());
}

#[tokio::test]
async fn a_failed_open_is_retried() {
    let (d, _server, configs) = driver().await;
    *d.name.lock() = "someone-else".into();
    let p = CsiVolumes::new(StaticVolumes::new(), std::sync::Arc::new(configs), None);
    let err = p.plugin(FAKE_DRIVER_NAME).await.err().unwrap();
    assert!(err.contains("does not match"), "{err}");
    *d.name.lock() = FAKE_DRIVER_NAME.into();
    assert!(p.plugin(FAKE_DRIVER_NAME).await.is_ok());
}

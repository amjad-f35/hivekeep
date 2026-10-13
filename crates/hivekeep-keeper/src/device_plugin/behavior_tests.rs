//! The device list, sockets and allocations served for a host device.

use std::collections::HashSet;
use std::path::Path;

use super::*;
use crate::context::Ctx;

fn kvm() -> DevicePlugin {
    DevicePlugin::new(HostDevice {
        resource_name: "hivekeep.io/kvm".into(),
        path: "/dev/kvm".into(),
    })
}

#[test]
fn socket_paths_are_distinct_and_in_kubelet_dir() {
    let other = DevicePlugin::new(HostDevice {
        resource_name: "hivekeep.io/other".into(),
        path: "/dev/other".into(),
    });
    let kvm = kvm();
    assert_ne!(kvm.socket(), other.socket(), "sockets collide");
    for p in [&kvm, &other] {
        assert_eq!(
            format!("{}/", p.socket().parent().unwrap().display()),
            KUBELET_DEVICE_PLUGIN_PATH
        );
        assert_eq!(p.socket().extension().unwrap(), "sock");
    }
}

#[test]
fn device_count_exceeds_max_pods_per_node() {
    // The largest supported max-pods settings are in the low hundreds.
    const HIGHEST_REALISTIC_MAX_PODS: usize = 256;
    const { assert!(DEVICE_COUNT > HIGHEST_REALISTIC_MAX_PODS) };
    let p = kvm();
    assert_eq!(p.devices().len(), DEVICE_COUNT);
    let mut seen = HashSet::new();
    for d in p.devices() {
        assert!(seen.insert(d.id.clone()), "duplicate device ID {:?}", d.id);
        assert!(d.healthy, "{} unhealthy", d.id);
    }
}

#[test]
fn allocate_returns_only_the_requested_device() {
    let resp = kvm().allocate(&[vec!["kvm-0"], vec!["kvm-1"]]);
    assert_eq!(resp.len(), 2);
    for (i, devs) in resp.iter().enumerate() {
        assert_eq!(devs.len(), 1, "container {i}");
        let d = &devs[0];
        assert_eq!(
            (d.host_path.as_str(), d.container_path.as_str()),
            ("/dev/kvm", "/dev/kvm")
        );
        assert_eq!(d.permissions, "rw", "container {i}: no mknod");
    }
}

#[cfg(unix)]
#[test]
fn available_filters_to_present_devices() {
    let devs = [
        HostDevice {
            resource_name: "hivekeep.io/null".into(),
            path: "/dev/null".into(),
        },
        HostDevice {
            resource_name: "hivekeep.io/absent".into(),
            path: "/dev/definitely-not-a-device".into(),
        },
    ];
    let got = available(&devs, Path::new("/dev"));
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].resource_name, "hivekeep.io/null");
}

#[test]
fn present_rejects_non_device() {
    let d = tempfile::tempdir().unwrap();
    std::fs::write(d.path().join("kvm"), b"").unwrap();
    let dev = HostDevice {
        resource_name: "hivekeep.io/kvm".into(),
        path: "/dev/kvm".into(),
    };
    assert!(!dev.present(d.path()));
}

#[tokio::test]
async fn list_and_watch_sends_devices_then_blocks() {
    let p = std::sync::Arc::new(kvm());
    let ctx = Ctx::background();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let task = {
        let (p, ctx) = (p.clone(), ctx.clone());
        tokio::spawn(async move {
            p.list_and_watch(&ctx, |d| tx.send(d.len()).map_err(|e| e.to_string()))
                .await
        })
    };
    assert_eq!(rx.recv().await, Some(DEVICE_COUNT));
    tokio::task::yield_now().await;
    assert!(!task.is_finished(), "returned while the stream was open");
    ctx.cancel();
    assert_eq!(task.await.unwrap(), Ok(()));
}

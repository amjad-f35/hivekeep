//! Advertising the sandbox host devices present on this node, such as
//! `/dev/kvm`, as extended resources, so a worker can be granted one
//! without running privileged.
//!
//! The keeper decides which devices exist and keeps one registration
//! running per device behind [`DeviceRegistrar`]; [`kubelet`] implements it
//! with the kubelet's v1beta1 device plugin API ([`pb`]). Failures are
//! logged, never fatal: a node that cannot advertise devices still runs
//! every sandbox class needing none.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use crate::context::Ctx;

/// The generated kubelet device plugin API (v1beta1).
#[allow(missing_docs, clippy::all, clippy::pedantic)]
pub mod pb {
    tonic::include_proto!("v1beta1");
}

pub mod kubelet;

/// The extended resource granting `/dev/kvm`.
pub const RESOURCE_KVM: &str = "hivekeep.io/kvm";
/// Where the node's `/dev` is mounted for inspection.
pub const HOST_DEV_ROOT: &str = "/host/dev";
/// The kubelet's device plugin directory.
pub const KUBELET_DEVICE_PLUGIN_PATH: &str = "/var/lib/kubelet/device-plugins/";

/// A device node advertised under a resource name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostDevice {
    /// The extended resource pods request.
    pub resource_name: String,
    /// The device node, exposed to the container at the same path.
    pub path: String,
}

/// The host devices a sandbox runtime needs a grant for: only those the
/// container runtime denies by default.
pub fn sandbox_devices() -> Vec<HostDevice> {
    vec![HostDevice {
        resource_name: RESOURCE_KVM.into(),
        path: "/dev/kvm".into(),
    }]
}

impl HostDevice {
    /// Whether the device exists as a character device under `dev_root`,
    /// where the node's `/dev` is mounted.
    pub fn present(&self, dev_root: &Path) -> bool {
        let rel = self.path.trim_start_matches("/dev/");
        let p: PathBuf = dev_root.join(rel);
        match std::fs::metadata(&p) {
            #[cfg(unix)]
            Ok(m) => {
                use std::os::unix::fs::FileTypeExt;
                m.file_type().is_char_device()
            }
            #[cfg(not(unix))]
            Ok(_) => false,
            Err(_) => false,
        }
    }
}

/// The devices of `devs` present under `dev_root`.
pub fn available(devs: &[HostDevice], dev_root: &Path) -> Vec<HostDevice> {
    devs.iter()
        .filter(|d| d.present(dev_root))
        .cloned()
        .collect()
}

/// How many interchangeable units of each device are advertised. Every
/// worker pod takes one, so this stays far above any max-pods-per-node
/// setting; otherwise the kubelet would report the resource exhausted.
pub const DEVICE_COUNT: usize = 4096;

/// One advertised unit of a device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvertisedDevice {
    /// Unique among the device's units.
    pub id: String,
    /// Whether the kubelet may hand it out.
    pub healthy: bool,
}

/// A device node handed to one container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceSpec {
    /// The node on the host.
    pub host_path: String,
    /// Where the container sees it.
    pub container_path: String,
    /// cgroup device permissions.
    pub permissions: String,
}

/// What a registrar serves to the kubelet for one device: its socket, the
/// unit list, and allocations.
#[derive(Debug, Clone)]
pub struct DevicePlugin {
    dev: HostDevice,
    socket: PathBuf,
    devices: Vec<AdvertisedDevice>,
}

impl DevicePlugin {
    /// The plugin of `dev`, with its socket in the kubelet's plugin
    /// directory.
    pub fn new(dev: HostDevice) -> Self {
        Self::in_dir(dev, Path::new(KUBELET_DEVICE_PLUGIN_PATH))
    }

    /// The plugin of `dev`, with its socket in `plugin_dir`.
    pub fn in_dir(dev: HostDevice, plugin_dir: &Path) -> Self {
        let base = Path::new(&dev.path)
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let devices = (0..DEVICE_COUNT)
            .map(|i| AdvertisedDevice {
                id: format!("{base}-{i}"),
                healthy: true,
            })
            .collect();
        let socket = plugin_dir.join(socket_name(&dev.resource_name));
        DevicePlugin {
            dev,
            socket,
            devices,
        }
    }

    /// The socket the plugin serves on.
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// The device advertised.
    pub fn device(&self) -> &HostDevice {
        &self.dev
    }

    /// The advertised units.
    pub fn devices(&self) -> &[AdvertisedDevice] {
        &self.devices
    }

    /// One response per container request: exactly the device node,
    /// read/write but without mknod, so the container gets this node and
    /// not the ability to make others.
    pub fn allocate<S: AsRef<str>>(&self, container_requests: &[Vec<S>]) -> Vec<Vec<DeviceSpec>> {
        container_requests
            .iter()
            .map(|_| {
                vec![DeviceSpec {
                    host_path: self.dev.path.clone(),
                    container_path: self.dev.path.clone(),
                    permissions: "rw".into(),
                }]
            })
            .collect()
    }

    /// Sends the full unit list once, then holds the stream open until
    /// `ctx` ends; the units never change.
    pub async fn list_and_watch<F>(&self, ctx: &Ctx, mut send: F) -> Result<(), String>
    where
        F: FnMut(&[AdvertisedDevice]) -> Result<(), String> + Send,
    {
        send(&self.devices).map_err(|e| format!("while sending device list: {e}"))?;
        ctx.done().await;
        Ok(())
    }
}

/// The socket file of a resource: its domain and name, so two resources
/// never share one.
fn socket_name(resource_name: &str) -> String {
    let p = Path::new(resource_name);
    let name = p
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let domain = p
        .parent()
        .and_then(|d| d.file_name())
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| ".".into());
    format!("{domain}-{name}.sock")
}

/// Registers one device with the kubelet and serves its allocations until
/// `ctx` ends.
#[async_trait]
pub trait DeviceRegistrar: Send + Sync + 'static {
    /// Runs the registration for `dev`.
    async fn run(&self, ctx: &Ctx, dev: &HostDevice) -> Result<(), String>;
}

/// Starts a registration for every sandbox device present on the node.
/// Without the kubelet plugin directory there is nobody to register with.
pub fn start_device_plugins(
    ctx: &Ctx,
    registrar: Arc<dyn DeviceRegistrar>,
    plugin_dir: &Path,
    dev_root: &Path,
) -> usize {
    if !plugin_dir.exists() {
        tracing::info!(path = %plugin_dir.display(), "Kubelet device plugin directory unavailable; not advertising host devices");
        return 0;
    }
    let devs = available(&sandbox_devices(), dev_root);
    if devs.is_empty() {
        tracing::info!(dev_root = %dev_root.display(), "No sandbox host devices present on this node; not advertising any");
        return 0;
    }
    for dev in devs.iter().cloned() {
        let (ctx, r) = (ctx.clone(), registrar.clone());
        tokio::spawn(async move {
            if let Err(err) = r.run(&ctx, &dev).await {
                if !ctx.is_done() {
                    tracing::error!(resource = %dev.resource_name, %err, "Device plugin stopped");
                }
            }
        });
    }
    devs.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regular_files_are_not_devices() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("kvm"), b"").unwrap();
        assert!(available(&sandbox_devices(), d.path()).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn character_devices_are_found() {
        // /dev/null is a character device everywhere.
        let dev = HostDevice {
            resource_name: "x".into(),
            path: "/dev/null".into(),
        };
        assert!(dev.present(Path::new("/dev")));
    }

    #[tokio::test]
    async fn nothing_starts_without_the_plugin_dir() {
        let d = tempfile::tempdir().unwrap();
        let n = start_device_plugins(
            &Ctx::background(),
            Arc::new(kubelet::KubeletRegistrar::new(d.path().join("missing"))),
            &d.path().join("missing"),
            Path::new("/dev"),
        );
        assert_eq!(n, 0);
    }
}

#[cfg(test)]
mod behavior_tests;

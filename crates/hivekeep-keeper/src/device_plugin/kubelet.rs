//! Registering host devices with the kubelet's device manager.
//!
//! For each device the keeper serves the `DevicePlugin` service on a socket
//! in the kubelet's device-plugins directory, then calls the kubelet's
//! `Registration.Register` on `kubelet.sock` in the same directory, naming
//! that socket and the extended resource. The kubelet then lists the units
//! (`ListAndWatch`) and asks for an allocation (`Allocate`) whenever a pod
//! requesting the resource starts.
//!
//! A restarting kubelet forgets every plugin and recreates its socket, so
//! the registrar watches `kubelet.sock` and registers again when the file
//! is replaced; otherwise the resource would vanish from the node.

use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hyper_util::rt::TokioIo;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use super::pb;
use super::pb::device_plugin_server::{DevicePlugin as DevicePluginApi, DevicePluginServer};
use super::pb::registration_client::RegistrationClient;
use super::{DevicePlugin, DeviceRegistrar, HostDevice};
use crate::context::Ctx;

/// The kubelet's registration socket, in its device-plugins directory.
pub const KUBELET_SOCKET_NAME: &str = "kubelet.sock";
/// The device plugin API version this registrar speaks.
pub const API_VERSION: &str = "v1beta1";
/// What the kubelet calls a usable unit.
pub const HEALTHY: &str = "Healthy";
/// What the kubelet calls an unusable unit.
pub const UNHEALTHY: &str = "Unhealthy";

/// Registers devices with the kubelet whose device-plugins directory is
/// `plugin_dir`, serving each device's API until the context ends.
#[derive(Debug, Clone)]
pub struct KubeletRegistrar {
    plugin_dir: PathBuf,
    register_timeout: Duration,
    retry_interval: Duration,
    watch_interval: Duration,
}

impl KubeletRegistrar {
    /// A registrar for the kubelet whose device-plugins directory is
    /// `plugin_dir`.
    pub fn new(plugin_dir: impl Into<PathBuf>) -> Self {
        KubeletRegistrar {
            plugin_dir: plugin_dir.into(),
            register_timeout: Duration::from_secs(30),
            retry_interval: Duration::from_secs(5),
            watch_interval: Duration::from_secs(1),
        }
    }

    /// Overrides how long to wait before retrying a failed registration.
    pub fn with_retry_interval(mut self, d: Duration) -> Self {
        self.retry_interval = d;
        self
    }

    /// Overrides how often the kubelet socket is checked for a restart.
    pub fn with_watch_interval(mut self, d: Duration) -> Self {
        self.watch_interval = d;
        self
    }

    /// The kubelet's registration socket.
    pub fn kubelet_socket(&self) -> PathBuf {
        self.plugin_dir.join(KUBELET_SOCKET_NAME)
    }

    /// Starts serving `plugin` on its socket and registers it. The returned
    /// token stops the server.
    async fn serve_and_register(
        &self,
        ctx: &Ctx,
        plugin: &Arc<DevicePlugin>,
    ) -> Result<CancellationToken, String> {
        let socket = plugin.socket().to_path_buf();
        // A socket left by an earlier run would make bind fail.
        match std::fs::remove_file(&socket) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(format!(
                    "while removing stale socket {}: {e}",
                    socket.display()
                ))
            }
        }
        let listener = tokio::net::UnixListener::bind(&socket)
            .map_err(|e| format!("while listening on {}: {e}", socket.display()))?;
        let stop = ctx.token().child_token();
        let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
        let svc = DevicePluginService {
            plugin: plugin.clone(),
            stop: stop.clone(),
        };
        let (resource, stopped) = (plugin.device().resource_name.clone(), stop.clone());
        tokio::spawn(async move {
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(DevicePluginServer::new(svc))
                .serve_with_incoming_shutdown(incoming, stopped.cancelled_owned())
                .await
            {
                tracing::error!(%resource, err = %e, "Device plugin server stopped");
            }
        });
        if let Err(e) = self.register(plugin).await {
            stop.cancel();
            return Err(e);
        }
        Ok(stop)
    }

    /// Tells the kubelet which resource the plugin's socket serves.
    async fn register(&self, plugin: &DevicePlugin) -> Result<(), String> {
        let kubelet = self.kubelet_socket();
        let channel = unix_channel(&kubelet);
        let endpoint = plugin
            .socket()
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let resource = plugin.device().resource_name.clone();
        let mut client = RegistrationClient::new(channel);
        let call = client.register(pb::RegisterRequest {
            version: API_VERSION.into(),
            endpoint,
            resource_name: resource.clone(),
            options: None,
        });
        match tokio::time::timeout(self.register_timeout, call).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(format!(
                "while registering {resource:?} with kubelet: {}",
                e.message()
            )),
            Err(_) => Err(format!(
                "while registering {resource:?} with kubelet: timed out after {:?}",
                self.register_timeout
            )),
        }
    }

    /// Waits until the kubelet socket is replaced (the kubelet restarted
    /// and dropped the registration) or `ctx` ends.
    async fn wait_for_kubelet_restart(&self, ctx: &Ctx, registered_with: Option<FileId>) {
        let socket = self.kubelet_socket();
        while ctx.sleep(self.watch_interval).await.is_ok() {
            let now = file_id(&socket);
            if now.is_some() && now != registered_with {
                tracing::info!(socket = %socket.display(), "Kubelet restarted; re-registering device plugin");
                return;
            }
        }
    }
}

#[async_trait]
impl DeviceRegistrar for KubeletRegistrar {
    async fn run(&self, ctx: &Ctx, dev: &HostDevice) -> Result<(), String> {
        let plugin = Arc::new(DevicePlugin::in_dir(dev.clone(), &self.plugin_dir));
        while !ctx.is_done() {
            // Identify the kubelet socket before registering, so a restart
            // racing the registration still shows up as a change.
            let kubelet = file_id(&self.kubelet_socket());
            let stop = match self.serve_and_register(ctx, &plugin).await {
                Ok(stop) => stop,
                Err(err) => {
                    tracing::error!(resource = %dev.resource_name, %err, "Device plugin registration failed; retrying");
                    if ctx.sleep(self.retry_interval).await.is_err() {
                        break;
                    }
                    continue;
                }
            };
            tracing::info!(resource = %dev.resource_name, device = %dev.path, "Device plugin registered");
            self.wait_for_kubelet_restart(ctx, kubelet).await;
            stop.cancel();
        }
        let _ = std::fs::remove_file(plugin.socket());
        Ok(())
    }
}

/// A file's identity: device, inode and change time, which a recreated
/// socket never shares with its predecessor.
type FileId = (u64, u64, i64, i64);

#[cfg(unix)]
fn file_id(path: &Path) -> Option<FileId> {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::symlink_metadata(path).ok()?;
    Some((m.dev(), m.ino(), m.ctime(), m.ctime_nsec()))
}

#[cfg(not(unix))]
fn file_id(_: &Path) -> Option<FileId> {
    None
}

/// A lazily connected gRPC channel over the unix socket at `path`.
pub(crate) fn unix_channel(path: &Path) -> tonic::transport::Channel {
    let path = path.to_path_buf();
    tonic::transport::Endpoint::from_static("http://kubelet.local").connect_with_connector_lazy(
        tower::service_fn(move |_: http::Uri| {
            let path = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(&path).await?;
                Ok::<_, io::Error>(TokioIo::new(stream))
            }
        }),
    )
}

/// The `DevicePlugin` service of one device.
#[derive(Clone)]
struct DevicePluginService {
    plugin: Arc<DevicePlugin>,
    stop: CancellationToken,
}

fn to_pb_devices(plugin: &DevicePlugin) -> Vec<pb::Device> {
    plugin
        .devices()
        .iter()
        .map(|d| pb::Device {
            id: d.id.clone(),
            health: if d.healthy { HEALTHY } else { UNHEALTHY }.into(),
            topology: None,
        })
        .collect()
}

type DeviceListStream =
    Pin<Box<dyn Stream<Item = Result<pb::ListAndWatchResponse, Status>> + Send>>;

#[tonic::async_trait]
impl DevicePluginApi for DevicePluginService {
    /// The defaults are right: no pre-start hook, no preferred allocation.
    async fn get_device_plugin_options(
        &self,
        _: Request<pb::Empty>,
    ) -> Result<Response<pb::DevicePluginOptions>, Status> {
        Ok(Response::new(pb::DevicePluginOptions::default()))
    }

    /// Sends the unit list once and holds the stream open until the
    /// kubelet hangs up or the plugin stops; the units never change.
    async fn list_and_watch(
        &self,
        _: Request<pb::Empty>,
    ) -> Result<Response<DeviceListStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let plugin = self.plugin.clone();
        let ctx = Ctx::from_token(self.stop.child_token());
        tokio::spawn(async move {
            let (sender, listed) = (tx.clone(), plugin.clone());
            let watch = plugin.list_and_watch(&ctx, move |_| {
                sender
                    .try_send(Ok(pb::ListAndWatchResponse {
                        devices: to_pb_devices(&listed),
                    }))
                    .map_err(|e| e.to_string())
            });
            tokio::select! {
                r = watch => {
                    if let Err(err) = r {
                        tracing::error!(%err, "Device plugin ListAndWatch failed");
                    }
                }
                _ = tx.closed() => ctx.cancel(),
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    /// One device node per container request.
    async fn allocate(
        &self,
        req: Request<pb::AllocateRequest>,
    ) -> Result<Response<pb::AllocateResponse>, Status> {
        let requests: Vec<Vec<String>> = req
            .into_inner()
            .container_requests
            .into_iter()
            .map(|c| c.devices_ids)
            .collect();
        let container_responses = self
            .plugin
            .allocate(&requests)
            .into_iter()
            .map(|devs| pb::ContainerAllocateResponse {
                devices: devs
                    .into_iter()
                    .map(|d| pb::DeviceSpec {
                        container_path: d.container_path,
                        host_path: d.host_path,
                        permissions: d.permissions,
                    })
                    .collect(),
                ..Default::default()
            })
            .collect();
        Ok(Response::new(pb::AllocateResponse {
            container_responses,
        }))
    }

    /// Nothing to prepare; only called if the options asked for it.
    async fn pre_start_container(
        &self,
        _: Request<pb::PreStartContainerRequest>,
    ) -> Result<Response<pb::PreStartContainerResponse>, Status> {
        Ok(Response::new(pb::PreStartContainerResponse {}))
    }
}

#[cfg(test)]
mod tests;

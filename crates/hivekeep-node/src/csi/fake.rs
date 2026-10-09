//! An in-process CSI driver for tests: the Identity, Controller and Node
//! services over a unix socket, plain TCP or mutual TLS, with every RPC's
//! answer replaceable by a test hook.
//!
//! Without hooks the driver behaves like a minimal well-behaved one named
//! [`FAKE_DRIVER_NAME`]: it creates volumes named as requested, advertises
//! attach/detach and staging, and succeeds at everything else it
//! implements. RPCs it does not implement answer `Unimplemented`.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::sync::CancellationToken;
use tonic::transport::server::Connected;
use tonic::{Request, Response, Status};

use super::pb;
use super::pb::controller_server::{Controller, ControllerServer};
use super::pb::identity_server::{Identity, IdentityServer};
use super::pb::node_server::{Node, NodeServer};

/// The name the fake driver reports unless told otherwise.
pub const FAKE_DRIVER_NAME: &str = "mock-driver";

type HookFn<Req, Resp> = Arc<dyn Fn(Req) -> Result<Resp, Status> + Send + Sync>;

/// A replaceable answer to one RPC.
pub struct Hook<Req, Resp>(Mutex<Option<HookFn<Req, Resp>>>);

impl<Req, Resp> Default for Hook<Req, Resp> {
    fn default() -> Self {
        Hook(Mutex::new(None))
    }
}

impl<Req, Resp> Hook<Req, Resp> {
    /// Answers the RPC with `f` from now on.
    pub fn set(&self, f: impl Fn(Req) -> Result<Resp, Status> + Send + Sync + 'static) {
        *self.0.lock() = Some(Arc::new(f));
    }

    /// Restores the default answer.
    pub fn clear(&self) {
        *self.0.lock() = None;
    }

    fn call(
        &self,
        req: Req,
        default: impl FnOnce(Req) -> Result<Resp, Status>,
    ) -> Result<Response<Resp>, Status> {
        let hook = self.0.lock().clone();
        match hook {
            Some(f) => f(req),
            None => default(req),
        }
        .map(Response::new)
    }
}

/// The fake driver's state and hooks.
pub struct FakeCsiDriver {
    /// The name `GetPluginInfo` reports.
    pub name: Mutex<String>,
    /// Every RPC received, by method name, in order.
    calls: Mutex<Vec<String>>,
    /// `GetPluginCapabilities`.
    pub get_plugin_capabilities:
        Hook<pb::GetPluginCapabilitiesRequest, pb::GetPluginCapabilitiesResponse>,
    /// `Probe`.
    pub probe: Hook<pb::ProbeRequest, pb::ProbeResponse>,
    /// `CreateVolume`.
    pub create_volume: Hook<pb::CreateVolumeRequest, pb::CreateVolumeResponse>,
    /// `DeleteVolume`.
    pub delete_volume: Hook<pb::DeleteVolumeRequest, pb::DeleteVolumeResponse>,
    /// `ControllerPublishVolume`.
    pub controller_publish_volume:
        Hook<pb::ControllerPublishVolumeRequest, pb::ControllerPublishVolumeResponse>,
    /// `ControllerUnpublishVolume`.
    pub controller_unpublish_volume:
        Hook<pb::ControllerUnpublishVolumeRequest, pb::ControllerUnpublishVolumeResponse>,
    /// `ControllerGetCapabilities`.
    pub controller_get_capabilities:
        Hook<pb::ControllerGetCapabilitiesRequest, pb::ControllerGetCapabilitiesResponse>,
    /// `NodeStageVolume`.
    pub node_stage_volume: Hook<pb::NodeStageVolumeRequest, pb::NodeStageVolumeResponse>,
    /// `NodeUnstageVolume`.
    pub node_unstage_volume: Hook<pb::NodeUnstageVolumeRequest, pb::NodeUnstageVolumeResponse>,
    /// `NodePublishVolume`.
    pub node_publish_volume: Hook<pb::NodePublishVolumeRequest, pb::NodePublishVolumeResponse>,
    /// `NodeUnpublishVolume`.
    pub node_unpublish_volume:
        Hook<pb::NodeUnpublishVolumeRequest, pb::NodeUnpublishVolumeResponse>,
    /// `NodeGetCapabilities`.
    pub node_get_capabilities:
        Hook<pb::NodeGetCapabilitiesRequest, pb::NodeGetCapabilitiesResponse>,
}

impl Default for FakeCsiDriver {
    fn default() -> Self {
        FakeCsiDriver {
            name: Mutex::new(FAKE_DRIVER_NAME.into()),
            calls: Mutex::default(),
            get_plugin_capabilities: Hook::default(),
            probe: Hook::default(),
            create_volume: Hook::default(),
            delete_volume: Hook::default(),
            controller_publish_volume: Hook::default(),
            controller_unpublish_volume: Hook::default(),
            controller_get_capabilities: Hook::default(),
            node_stage_volume: Hook::default(),
            node_unstage_volume: Hook::default(),
            node_publish_volume: Hook::default(),
            node_unpublish_volume: Hook::default(),
            node_get_capabilities: Hook::default(),
        }
    }
}

impl FakeCsiDriver {
    /// A driver with default answers.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// A driver reporting `name`.
    pub fn named(name: &str) -> Arc<Self> {
        let d = Self::default();
        *d.name.lock() = name.into();
        Arc::new(d)
    }

    /// The RPCs received so far, by method name.
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }

    /// How many times `method` was called.
    pub fn count(&self, method: &str) -> usize {
        self.calls.lock().iter().filter(|c| *c == method).count()
    }

    fn record(&self, method: &str) {
        self.calls.lock().push(method.to_string());
    }

    /// Serves all three services on a unix socket at `path`.
    pub async fn serve_unix(self: &Arc<Self>, path: &Path) -> std::io::Result<FakeCsiServer> {
        let _ = std::fs::remove_file(path);
        let listener = tokio::net::UnixListener::bind(path)?;
        let stop = CancellationToken::new();
        let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
        self.spawn(incoming, stop.clone());
        Ok(FakeCsiServer {
            endpoint: format!("unix://{}", path.display()),
            stop,
        })
    }

    /// Serves all three services over plaintext TCP on the loopback.
    pub async fn serve_tcp(self: &Arc<Self>) -> std::io::Result<FakeCsiServer> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let stop = CancellationToken::new();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        self.spawn(incoming, stop.clone());
        Ok(FakeCsiServer {
            endpoint: format!("tcp://{addr}"),
            stop,
        })
    }

    /// Serves all three services over TLS on the loopback with `config`
    /// (which decides whether client certificates are required).
    pub async fn serve_tls(
        self: &Arc<Self>,
        config: Arc<rustls::ServerConfig>,
    ) -> std::io::Result<FakeCsiServer> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let stop = CancellationToken::new();
        let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<TlsConn>>(16);
        let acceptor = tokio_rustls::TlsAcceptor::from(config);
        let accept_stop = stop.clone();
        tokio::spawn(async move {
            loop {
                let (tcp, _) = tokio::select! {
                    _ = accept_stop.cancelled() => return,
                    r = listener.accept() => match r {
                        Ok(c) => c,
                        Err(_) => continue,
                    },
                };
                let (acceptor, tx) = (acceptor.clone(), tx.clone());
                tokio::spawn(async move {
                    // A failed handshake only closes that connection.
                    if let Ok(tls) = acceptor.accept(tcp).await {
                        let _ = tx.send(Ok(TlsConn(tls))).await;
                    }
                });
            }
        });
        self.spawn(
            tokio_stream::wrappers::ReceiverStream::new(rx),
            stop.clone(),
        );
        Ok(FakeCsiServer {
            endpoint: format!("tcp://{addr}"),
            stop,
        })
    }

    fn spawn<S, IO>(self: &Arc<Self>, incoming: S, stop: CancellationToken)
    where
        S: tokio_stream::Stream<Item = std::io::Result<IO>> + Send + 'static,
        IO: AsyncRead + AsyncWrite + Connected + Send + Unpin + 'static,
    {
        let svc = Svc(self.clone());
        let router = tonic::transport::Server::builder()
            .add_service(IdentityServer::new(svc.clone()))
            .add_service(ControllerServer::new(svc.clone()))
            .add_service(NodeServer::new(svc));
        tokio::spawn(async move {
            let _ = router
                .serve_with_incoming_shutdown(incoming, stop.cancelled_owned())
                .await;
        });
    }
}

/// A running fake driver; dropping it stops the server.
#[derive(Debug)]
pub struct FakeCsiServer {
    /// The endpoint to configure clients with.
    pub endpoint: String,
    stop: CancellationToken,
}

impl FakeCsiServer {
    /// Stops serving.
    pub fn stop(&self) {
        self.stop.cancel();
    }
}

impl Drop for FakeCsiServer {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// A server-side TLS connection the gRPC server can accept.
pub struct TlsConn(tokio_rustls::server::TlsStream<tokio::net::TcpStream>);

impl Connected for TlsConn {
    type ConnectInfo = ();
    fn connect_info(&self) -> Self::ConnectInfo {}
}

impl AsyncRead for TlsConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// A controller capability list with one RPC.
pub fn controller_caps(
    rpcs: &[pb::controller_service_capability::rpc::Type],
) -> pb::ControllerGetCapabilitiesResponse {
    pb::ControllerGetCapabilitiesResponse {
        capabilities: rpcs
            .iter()
            .map(|t| pb::ControllerServiceCapability {
                r#type: Some(pb::controller_service_capability::Type::Rpc(
                    pb::controller_service_capability::Rpc { r#type: *t as i32 },
                )),
            })
            .collect(),
    }
}

/// A node capability list.
pub fn node_caps(
    rpcs: &[pb::node_service_capability::rpc::Type],
) -> pb::NodeGetCapabilitiesResponse {
    pb::NodeGetCapabilitiesResponse {
        capabilities: rpcs
            .iter()
            .map(|t| pb::NodeServiceCapability {
                r#type: Some(pb::node_service_capability::Type::Rpc(
                    pb::node_service_capability::Rpc { r#type: *t as i32 },
                )),
            })
            .collect(),
    }
}

#[derive(Clone)]
struct Svc(Arc<FakeCsiDriver>);

#[tonic::async_trait]
impl Identity for Svc {
    async fn get_plugin_info(
        &self,
        _: Request<pb::GetPluginInfoRequest>,
    ) -> Result<Response<pb::GetPluginInfoResponse>, Status> {
        self.0.record("GetPluginInfo");
        Ok(Response::new(pb::GetPluginInfoResponse {
            name: self.0.name.lock().clone(),
            vendor_version: "v1.0.0".into(),
            manifest: Default::default(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        req: Request<pb::GetPluginCapabilitiesRequest>,
    ) -> Result<Response<pb::GetPluginCapabilitiesResponse>, Status> {
        self.0.record("GetPluginCapabilities");
        self.0.get_plugin_capabilities.call(req.into_inner(), |_| {
            Ok(pb::GetPluginCapabilitiesResponse {
                capabilities: vec![pb::PluginCapability {
                    r#type: Some(pb::plugin_capability::Type::Service(
                        pb::plugin_capability::Service {
                            r#type: pb::plugin_capability::service::Type::ControllerService as i32,
                        },
                    )),
                }],
            })
        })
    }

    async fn probe(
        &self,
        req: Request<pb::ProbeRequest>,
    ) -> Result<Response<pb::ProbeResponse>, Status> {
        self.0.record("Probe");
        self.0
            .probe
            .call(req.into_inner(), |_| Ok(pb::ProbeResponse::default()))
    }
}

#[tonic::async_trait]
impl Controller for Svc {
    async fn create_volume(
        &self,
        req: Request<pb::CreateVolumeRequest>,
    ) -> Result<Response<pb::CreateVolumeResponse>, Status> {
        self.0.record("CreateVolume");
        self.0.create_volume.call(req.into_inner(), |r| {
            Ok(pb::CreateVolumeResponse {
                volume: Some(pb::Volume {
                    volume_id: r.name.clone(),
                    capacity_bytes: r
                        .capacity_range
                        .map(|c| c.required_bytes)
                        .unwrap_or_default(),
                    ..Default::default()
                }),
            })
        })
    }

    async fn delete_volume(
        &self,
        req: Request<pb::DeleteVolumeRequest>,
    ) -> Result<Response<pb::DeleteVolumeResponse>, Status> {
        self.0.record("DeleteVolume");
        self.0
            .delete_volume
            .call(req.into_inner(), |_| Ok(pb::DeleteVolumeResponse {}))
    }

    async fn controller_publish_volume(
        &self,
        req: Request<pb::ControllerPublishVolumeRequest>,
    ) -> Result<Response<pb::ControllerPublishVolumeResponse>, Status> {
        self.0.record("ControllerPublishVolume");
        self.0
            .controller_publish_volume
            .call(req.into_inner(), |_| {
                Ok(pb::ControllerPublishVolumeResponse::default())
            })
    }

    async fn controller_unpublish_volume(
        &self,
        req: Request<pb::ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<pb::ControllerUnpublishVolumeResponse>, Status> {
        self.0.record("ControllerUnpublishVolume");
        self.0
            .controller_unpublish_volume
            .call(req.into_inner(), |_| {
                Ok(pb::ControllerUnpublishVolumeResponse {})
            })
    }

    async fn controller_get_capabilities(
        &self,
        req: Request<pb::ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<pb::ControllerGetCapabilitiesResponse>, Status> {
        self.0.record("ControllerGetCapabilities");
        self.0
            .controller_get_capabilities
            .call(req.into_inner(), |_| {
                Ok(controller_caps(&[
                    pb::controller_service_capability::rpc::Type::PublishUnpublishVolume,
                ]))
            })
    }
}

#[tonic::async_trait]
impl Node for Svc {
    async fn node_stage_volume(
        &self,
        req: Request<pb::NodeStageVolumeRequest>,
    ) -> Result<Response<pb::NodeStageVolumeResponse>, Status> {
        self.0.record("NodeStageVolume");
        self.0
            .node_stage_volume
            .call(req.into_inner(), |_| Ok(pb::NodeStageVolumeResponse {}))
    }

    async fn node_unstage_volume(
        &self,
        req: Request<pb::NodeUnstageVolumeRequest>,
    ) -> Result<Response<pb::NodeUnstageVolumeResponse>, Status> {
        self.0.record("NodeUnstageVolume");
        self.0
            .node_unstage_volume
            .call(req.into_inner(), |_| Ok(pb::NodeUnstageVolumeResponse {}))
    }

    async fn node_publish_volume(
        &self,
        req: Request<pb::NodePublishVolumeRequest>,
    ) -> Result<Response<pb::NodePublishVolumeResponse>, Status> {
        self.0.record("NodePublishVolume");
        self.0
            .node_publish_volume
            .call(req.into_inner(), |_| Ok(pb::NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        req: Request<pb::NodeUnpublishVolumeRequest>,
    ) -> Result<Response<pb::NodeUnpublishVolumeResponse>, Status> {
        self.0.record("NodeUnpublishVolume");
        self.0
            .node_unpublish_volume
            .call(req.into_inner(), |_| Ok(pb::NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_capabilities(
        &self,
        req: Request<pb::NodeGetCapabilitiesRequest>,
    ) -> Result<Response<pb::NodeGetCapabilitiesResponse>, Status> {
        self.0.record("NodeGetCapabilities");
        self.0.node_get_capabilities.call(req.into_inner(), |_| {
            Ok(node_caps(&[
                pb::node_service_capability::rpc::Type::StageUnstageVolume,
            ]))
        })
    }
}

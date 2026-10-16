//! The client of the kata guest agent: ttrpc over the VMM's hybrid vsock.
//!
//! The worker boots the VMM itself (no kata shim), so it also does what a
//! shim would after boot: create the agent sandbox, configure guest
//! networking, then create and start each container on the rootfs the host
//! assembled.

use std::io;
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::agentpb::{grpc as apb, types as tpb};
use crate::guest_spec::Spec;
use crate::kata::paths::{guest_shared_rootfs, FS_TAG};
use crate::kata::specconv::spec_to_agent_pb;
use crate::ttrpc::{self, TtrpcError};

/// The guest port the agent's ttrpc server listens on.
pub const AGENT_VSOCK_PORT: u32 = 1024;
/// The guest port of the debug console (a raw shell), when enabled.
pub const DEBUG_CONSOLE_VSOCK_PORT: u32 = 1026;
/// The agent's service name.
pub const SERVICE: &str = "grpc.AgentService";

/// Connects to guest `port` through the hybrid-vsock socket at `path`: a
/// plain-text `CONNECT <port>` the VMM answers with `OK <port>`.
pub async fn hybrid_vsock_connect(
    path: &Path,
    port: u32,
    timeout: Duration,
) -> io::Result<UnixStream> {
    let mut s = UnixStream::connect(path).await?;
    let handshake = async {
        s.write_all(format!("CONNECT {port}\n").as_bytes()).await?;
        // Byte by byte, so nothing past the reply line is consumed.
        let mut line = Vec::new();
        let mut b = [0u8; 1];
        loop {
            if s.read(&mut b).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "hybrid vsock CONNECT response: EOF",
                ));
            }
            if b[0] == b'\n' {
                break;
            }
            line.push(b[0]);
            if line.len() > 256 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "hybrid vsock CONNECT response too long",
                ));
            }
        }
        let line = String::from_utf8_lossy(&line).to_string();
        if !line.starts_with("OK ") {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("hybrid vsock CONNECT refused: {:?}", line.trim()),
            ));
        }
        Ok(())
    };
    match tokio::time::timeout(timeout, handshake).await {
        Ok(Ok(())) => Ok(s),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "hybrid vsock CONNECT timed out",
        )),
    }
}

/// An agent call that failed.
#[derive(Debug, Clone, thiserror::Error)]
#[error("agent {method}: {source}")]
pub struct AgentError {
    /// The method called.
    pub method: &'static str,
    /// What went wrong.
    #[source]
    pub source: TtrpcError,
}

impl From<AgentError> for hivekeep_sandbox_gvisor::error::Error {
    fn from(e: AgentError) -> Self {
        hivekeep_sandbox_gvisor::error::Error::msg(e.to_string())
    }
}

/// A connected agent client. Calls multiplex over one connection, so the log
/// forwarders, stats reads and lifecycle calls share it.
#[derive(Debug, Clone)]
pub struct AgentClient {
    client: ttrpc::Client,
}

impl AgentClient {
    /// Dials the agent through the hybrid vsock at `vsock_path`, bounding the
    /// connect and handshake by `timeout`.
    pub async fn dial(vsock_path: &Path, timeout: Duration) -> io::Result<AgentClient> {
        let s = tokio::time::timeout(
            timeout,
            hybrid_vsock_connect(vsock_path, AGENT_VSOCK_PORT, timeout),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "dialing the agent timed out"))?
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("dialing hybrid vsock {}: {e}", vsock_path.display()),
            )
        })?;
        Ok(AgentClient::from_stream(s))
    }

    /// A client over an already-connected stream.
    pub fn from_stream<S>(s: S) -> AgentClient
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static,
    {
        AgentClient {
            client: ttrpc::Client::new(s),
        }
    }

    /// Closes the connection; calls in flight fail, which ends the log
    /// forwarders reading over it.
    pub fn close(&self) {
        self.client.close();
    }

    async fn call<Req: prost::Message, Resp: prost::Message + Default>(
        &self,
        method: &'static str,
        req: &Req,
    ) -> Result<Resp, AgentError> {
        self.client
            .call(SERVICE, method, req, None)
            .await
            .map_err(|source| AgentError { method, source })
    }

    /// Creates a container: the agent mounts its storages and builds the
    /// rootfs, then forks the parked init.
    pub async fn create_container(
        &self,
        req: &apb::CreateContainerRequest,
    ) -> Result<(), AgentError> {
        self.call::<_, ()>("CreateContainer", req).await.map(drop)
    }

    /// Starts a created container's init.
    pub async fn start_container(&self, container_id: &str) -> Result<(), AgentError> {
        let req = apb::StartContainerRequest {
            container_id: container_id.into(),
        };
        self.call::<_, ()>("StartContainer", &req).await.map(drop)
    }

    /// Establishes the agent's sandbox before any container is created.
    pub async fn create_sandbox(&self, req: &apb::CreateSandboxRequest) -> Result<(), AgentError> {
        self.call::<_, ()>("CreateSandbox", req).await.map(drop)
    }

    /// Configures a guest interface, matched by hardware address.
    pub async fn update_interface(&self, iface: tpb::Interface) -> Result<(), AgentError> {
        let req = apb::UpdateInterfaceRequest {
            interface: Some(iface),
        };
        self.call::<_, tpb::Interface>("UpdateInterface", &req)
            .await
            .map(drop)
    }

    /// Replaces the guest's routes.
    pub async fn update_routes(&self, routes: Vec<tpb::Route>) -> Result<(), AgentError> {
        let req = apb::UpdateRoutesRequest {
            routes: Some(apb::Routes { routes }),
        };
        self.call::<_, apb::Routes>("UpdateRoutes", &req)
            .await
            .map(drop)
    }

    /// Installs static neighbor entries.
    pub async fn add_arp_neighbors(
        &self,
        neighbors: Vec<tpb::ArpNeighbor>,
    ) -> Result<(), AgentError> {
        let req = apb::AddArpNeighborsRequest {
            neighbors: Some(apb::ArpNeighbors {
                arp_neighbors: neighbors,
            }),
        };
        self.call::<_, ()>("AddARPNeighbors", &req).await.map(drop)
    }

    /// Reads up to `max` bytes of a container's stdout or stderr. A unary
    /// call: it returns what the agent has buffered and fails once the
    /// stream ends.
    pub async fn read_stdio(
        &self,
        container_id: &str,
        exec_id: &str,
        stderr: bool,
        max: u32,
    ) -> Result<Vec<u8>, AgentError> {
        let req = apb::ReadStreamRequest {
            container_id: container_id.into(),
            exec_id: exec_id.into(),
            len: max,
        };
        let method = if stderr { "ReadStderr" } else { "ReadStdout" };
        self.call::<_, apb::ReadStreamResponse>(method, &req)
            .await
            .map(|r| r.data)
    }

    /// Creates the guest sandbox with the shared virtio-fs tree mounted: the
    /// merged rootfs trees and every volume share.
    pub async fn create_sandbox_for_actor(
        &self,
        sandbox_id: &str,
        hostname: &str,
    ) -> Result<(), AgentError> {
        self.create_sandbox(&apb::CreateSandboxRequest {
            hostname: hostname.into(),
            sandbox_id: sandbox_id.into(),
            storages: vec![apb::Storage {
                driver: "virtio-fs".into(),
                source: FS_TAG.into(),
                fstype: "virtiofs".into(),
                mount_point: format!("{}/", crate::guest_spec::GUEST_SHARED_DIR),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
    }

    /// Creates and starts one container on its merged rootfs in the shared
    /// tree, in a cgroup under the shared `/actor` parent so the guest
    /// accounts the Actor's containers hierarchically.
    pub async fn start_rootfs_container(&self, cid: &str, spec: &Spec) -> Result<(), AgentError> {
        let mut pb = spec_to_agent_pb(spec);
        pb.root = Some(apb::Root {
            path: guest_shared_rootfs(cid),
            readonly: false,
        });
        if let Some(l) = pb.linux.as_mut() {
            l.cgroups_path = format!("/actor/{cid}");
        }
        self.create_container(&apb::CreateContainerRequest {
            container_id: cid.into(),
            exec_id: cid.into(),
            oci: Some(pb),
            ..Default::default()
        })
        .await?;
        self.start_container(cid).await
    }
}

/// The agent calls graceful shutdown makes: signal a guest process and wait
/// for it to exit.
#[async_trait]
pub trait GuestProcesses: Send + Sync {
    /// Sends `signal` to the process; an exec id equal to the container id is
    /// the container's init.
    async fn signal_process(
        &self,
        container_id: &str,
        exec_id: &str,
        signal: u32,
    ) -> Result<(), AgentError>;
    /// Waits for the process to exit and returns its status.
    async fn wait_process(&self, container_id: &str, exec_id: &str) -> Result<i32, AgentError>;
}

/// The agent call the stats reads make.
#[async_trait]
pub trait ContainerStatsReader: Send + Sync {
    /// The guest cgroup accounting of one container; `None` when the agent
    /// has none for it (normal for one that exited).
    async fn stats_container(
        &self,
        container_id: &str,
    ) -> Result<Option<apb::CgroupStats>, AgentError>;
}

/// The agent call a restore makes to diverge clones of one snapshot.
#[async_trait]
pub trait RandomDevReseeder: Send + Sync {
    /// Mixes `data` into the guest's random device and reseeds its CRNG.
    async fn reseed_random_dev(&self, data: &[u8]) -> Result<(), AgentError>;
}

#[async_trait]
impl GuestProcesses for AgentClient {
    async fn signal_process(
        &self,
        container_id: &str,
        exec_id: &str,
        signal: u32,
    ) -> Result<(), AgentError> {
        let req = apb::SignalProcessRequest {
            container_id: container_id.into(),
            exec_id: exec_id.into(),
            signal,
        };
        self.call::<_, ()>("SignalProcess", &req).await.map(drop)
    }

    async fn wait_process(&self, container_id: &str, exec_id: &str) -> Result<i32, AgentError> {
        let req = apb::WaitProcessRequest {
            container_id: container_id.into(),
            exec_id: exec_id.into(),
        };
        self.call::<_, apb::WaitProcessResponse>("WaitProcess", &req)
            .await
            .map(|r| r.status)
    }
}

#[async_trait]
impl ContainerStatsReader for AgentClient {
    async fn stats_container(
        &self,
        container_id: &str,
    ) -> Result<Option<apb::CgroupStats>, AgentError> {
        let req = apb::StatsContainerRequest {
            container_id: container_id.into(),
        };
        self.call::<_, apb::StatsContainerResponse>("StatsContainer", &req)
            .await
            .map(|r| r.cgroup_stats)
    }
}

#[async_trait]
impl RandomDevReseeder for AgentClient {
    async fn reseed_random_dev(&self, data: &[u8]) -> Result<(), AgentError> {
        let req = apb::ReseedRandomDevRequest {
            data: data.to_vec(),
        };
        self.call::<_, ()>("ReseedRandomDev", &req).await.map(drop)
    }
}

/// Runs `cmd` on the guest's debug console (enabled by the debug kernel
/// parameters) and returns its combined output. Diagnostic and best effort:
/// a failure comes back as text.
pub async fn debug_console_dump(vsock_path: &Path, cmd: &str) -> String {
    let budget = Duration::from_secs(8);
    let work = async {
        let mut s = hybrid_vsock_connect(vsock_path, DEBUG_CONSOLE_VSOCK_PORT, budget)
            .await
            .map_err(|e| format!("debug-console dial: {e}"))?;
        // The console is an interactive shell that echoes its input. The
        // sentinel is written split by '' (which the shell strips), so only
        // the shell's output of it matches.
        s.write_all(format!("{{ {cmd} ; }} 2>&1; echo __HK''_END__\n").as_bytes())
            .await
            .map_err(|e| format!("debug-console write: {e}"))?;
        const SENTINEL: &str = "__HK_END__";
        let mut out = String::new();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = s.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            while let Some(i) = buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buf.drain(..=i).collect();
                let line = String::from_utf8_lossy(&line);
                if line.contains(SENTINEL) {
                    return Ok(out);
                }
                out.push_str(&line);
            }
        }
        Ok::<_, String>(out)
    };
    match tokio::time::timeout(budget, work).await {
        Ok(Ok(s)) | Ok(Err(s)) => s,
        Err(_) => "debug-console: timed out".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::net::UnixListener;

    /// A fake agent behind a hybrid-vsock style socket, answering
    /// ReseedRandomDev through `handle`.
    async fn fake_agent(
        dir: &Path,
        handle: impl Fn(Vec<u8>) -> Result<(), String> + Send + Sync + 'static,
    ) -> std::path::PathBuf {
        let sock = dir.join("agent.sock");
        let l = UnixListener::bind(&sock).unwrap();
        let handle = Arc::new(handle);
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                let handle = handle.clone();
                tokio::spawn(async move {
                    let mut line = Vec::new();
                    let mut b = [0u8; 1];
                    while s.read(&mut b).await.unwrap_or(0) == 1 && b[0] != b'\n' {
                        line.push(b[0]);
                    }
                    if line != format!("CONNECT {AGENT_VSOCK_PORT}").into_bytes() {
                        return;
                    }
                    s.write_all(b"OK 1024\n").await.unwrap();
                    let h: ttrpc::Handler = Arc::new(move |service, method, payload| {
                        let handle = handle.clone();
                        Box::pin(async move {
                            if service != SERVICE || method != "ReseedRandomDev" {
                                return Err(ttrpc::RpcStatus {
                                    code: 12,
                                    message: format!("{service}/{method} not implemented"),
                                    details: vec![],
                                });
                            }
                            let req = <apb::ReseedRandomDevRequest as prost::Message>::decode(
                                payload.as_slice(),
                            )
                            .unwrap();
                            handle(req.data).map_err(|m| ttrpc::RpcStatus {
                                code: 2,
                                message: m,
                                details: vec![],
                            })?;
                            Ok(prost::Message::encode_to_vec(&()))
                        })
                    });
                    ttrpc::serve(s, h).await;
                });
            }
        });
        sock
    }

    #[tokio::test]
    async fn reseed_sends_the_data() {
        let d = tempfile::tempdir().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sock = fake_agent(d.path(), move |data| {
            tx.send(data).unwrap();
            Ok(())
        })
        .await;
        let ac = AgentClient::dial(&sock, Duration::from_secs(5))
            .await
            .unwrap();
        ac.reseed_random_dev(b"per-restore nonce").await.unwrap();
        assert_eq!(rx.try_recv().unwrap(), b"per-restore nonce");
    }

    #[tokio::test]
    async fn reseed_returns_the_agent_error() {
        let d = tempfile::tempdir().unwrap();
        let sock = fake_agent(d.path(), |_| Err("reseed refused".into())).await;
        let ac = AgentClient::dial(&sock, Duration::from_secs(5))
            .await
            .unwrap();
        let err = ac.reseed_random_dev(b"nonce").await.unwrap_err();
        assert!(err.to_string().contains("reseed refused"), "{err}");
    }
}

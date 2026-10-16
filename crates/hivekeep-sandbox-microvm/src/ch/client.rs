//! The API client of one VMM, and launching a bare VMM.

use std::io;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::actor_cgroup::ActorLeaf;
use crate::ch::http;
use crate::ch::vm_config::VmConfig;
use crate::ch::vmm_info::VmmInfo;
use crate::child::Process;

/// How long one API request may take.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a launched VMM may take to answer.
pub const LAUNCH_TIMEOUT: Duration = Duration::from_secs(15);

/// The `file://` URL the VMM wants for a snapshot directory.
pub fn snapshot_url(dir: &Path) -> String {
    format!("file://{}", dir.display())
}

/// Talks to one VMM over its API socket.
#[derive(Debug)]
pub struct Client {
    socket: PathBuf,
    info: Mutex<VmmInfo>,
}

fn err(op: &str, r: &http::Response) -> io::Error {
    io::Error::other(format!("{op}: status {}: {}", r.status, r.text()))
}

impl Client {
    /// A client of the VMM at `socket`, which need not exist yet.
    pub fn new(socket: impl Into<PathBuf>) -> Client {
        Client {
            socket: socket.into(),
            info: Mutex::default(),
        }
    }

    /// The API socket.
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// What the VMM last reported about itself; empty before a ping.
    pub fn info(&self) -> VmmInfo {
        self.info.lock().clone()
    }

    async fn get(&self, path: &str) -> io::Result<Vec<u8>> {
        let r = http::request(&self.socket, "GET", path, None, &[], REQUEST_TIMEOUT).await?;
        if !r.ok() {
            return Err(err(&format!("GET {path}"), &r));
        }
        Ok(r.body)
    }

    async fn put(&self, path: &str, body: Option<Vec<u8>>, fds: &[RawFd]) -> io::Result<()> {
        let r = http::request(
            &self.socket,
            "PUT",
            path,
            body.as_deref(),
            fds,
            REQUEST_TIMEOUT,
        )
        .await?;
        if !r.ok() {
            return Err(err(&format!("PUT {path}"), &r));
        }
        Ok(())
    }

    fn json<T: Serialize>(v: &T) -> io::Result<Option<Vec<u8>>> {
        serde_json::to_vec(v).map(Some).map_err(io::Error::other)
    }

    /// Asks the VMM about itself and remembers the answer.
    pub async fn ping(&self) -> io::Result<VmmInfo> {
        let b = self.get("/api/v1/vmm.ping").await?;
        let info: VmmInfo = serde_json::from_slice(&b).map_err(io::Error::other)?;
        *self.info.lock() = info.clone();
        Ok(info)
    }

    /// Waits until the VMM answers a ping, or `deadline` passes.
    pub async fn wait_ready(
        &self,
        deadline: Duration,
        cancel: &CancellationToken,
    ) -> io::Result<VmmInfo> {
        let end = tokio::time::Instant::now() + deadline;
        loop {
            if let Ok(info) = self.ping().await {
                return Ok(info);
            }
            if tokio::time::Instant::now() >= end {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "cloud-hypervisor api socket {} not ready after {deadline:?}",
                        self.socket.display()
                    ),
                ));
            }
            tokio::select! {
                _ = cancel.cancelled() => return Err(io::Error::new(io::ErrorKind::Interrupted, "context canceled")),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    }

    /// The VM's state (`Running`, `Paused`, ...).
    pub async fn state(&self) -> io::Result<String> {
        #[derive(Deserialize)]
        struct Info {
            #[serde(default)]
            state: String,
        }
        let b = self.get("/api/v1/vm.info").await?;
        Ok(serde_json::from_slice::<Info>(&b)
            .map_err(io::Error::other)?
            .state)
    }

    /// Pauses the guest. Pausing a paused guest succeeds (the VMM itself
    /// refuses it, which would wedge a retried checkpoint).
    pub async fn pause(&self) -> io::Result<()> {
        if self.state().await.is_ok_and(|s| s == "Paused") {
            return Ok(());
        }
        self.put("/api/v1/vm.pause", None, &[]).await
    }

    /// Resumes a paused guest.
    pub async fn resume(&self) -> io::Result<()> {
        self.put("/api/v1/vm.resume", None, &[]).await
    }

    /// Writes the paused guest to `dest` (config, state, memory ranges).
    pub async fn snapshot(&self, dest: &Path) -> io::Result<()> {
        std::fs::create_dir_all(dest)?;
        #[derive(Serialize)]
        struct Body {
            destination_url: String,
        }
        self.put(
            "/api/v1/vm.snapshot",
            Self::json(&Body {
                destination_url: snapshot_url(dest),
            })?,
            &[],
        )
        .await
    }

    /// Creates (does not boot) the VM.
    pub async fn create_vm(&self, cfg: &VmConfig) -> io::Result<()> {
        self.put("/api/v1/vm.create", Self::json(cfg)?, &[])
            .await
            .map_err(|e| io::Error::new(e.kind(), format!("vm.create: {e}")))
    }

    /// Boots the created VM.
    pub async fn boot_vm(&self) -> io::Result<()> {
        self.put("/api/v1/vm.boot", None, &[])
            .await
            .map_err(|e| io::Error::new(e.kind(), format!("vm.boot: {e}")))
    }

    /// Restores the snapshot in `source`, backing its fd-backed net devices
    /// with fresh tap fds. `mem_mode` picks how guest RAM comes back; with
    /// on-demand the VMM pages from `source` for the VM's lifetime. The VM
    /// comes back paused.
    pub async fn restore_with_net_fds(
        &self,
        source: &Path,
        nets: &[RestoredNet],
        mem_mode: &str,
    ) -> io::Result<()> {
        #[derive(Serialize)]
        struct NetFds<'a> {
            id: &'a str,
            num_fds: usize,
        }
        #[derive(Serialize)]
        struct Body<'a> {
            source_url: String,
            #[serde(skip_serializing_if = "str::is_empty")]
            memory_restore_mode: &'a str,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            net_fds: Vec<NetFds<'a>>,
        }
        let body = Body {
            source_url: snapshot_url(source),
            memory_restore_mode: mem_mode,
            net_fds: nets
                .iter()
                .map(|n| NetFds {
                    id: &n.id,
                    num_fds: n.fds.len(),
                })
                .collect(),
        };
        let fds: Vec<RawFd> = nets.iter().flat_map(|n| n.fds.iter().copied()).collect();
        self.put("/api/v1/vm.restore", Self::json(&body)?, &fds)
            .await
            .map_err(|e| io::Error::new(e.kind(), format!("vm.restore failed: {e}")))
    }

    /// Adds a virtio-net device backed by tap `fds` (one per queue pair) to a
    /// created VM. `num_queues` is twice the queue pairs.
    pub async fn add_net_with_fds(
        &self,
        mac: &str,
        num_queues: usize,
        fds: &[RawFd],
    ) -> io::Result<()> {
        #[derive(Serialize)]
        struct Body<'a> {
            #[serde(skip_serializing_if = "str::is_empty")]
            mac: &'a str,
            num_queues: usize,
            num_fds: usize,
        }
        self.put(
            "/api/v1/vm.add-net",
            Self::json(&Body {
                mac,
                num_queues,
                num_fds: fds.len(),
            })?,
            fds,
        )
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("vm.add-net failed: {e}")))
    }
}

/// A snapshot's fd-backed net device and the tap fds to back it with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredNet {
    /// The device id in the snapshot config (`_net1`).
    pub id: String,
    /// Tap fds, one per queue pair.
    pub fds: Vec<RawFd>,
}

/// A net device found in a snapshot's config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotNetDevice {
    /// Device id.
    pub id: String,
    /// Tap fds it needs.
    pub queue_pairs: usize,
    /// Guest MAC.
    pub mac: String,
}

/// The net devices in a snapshot's `config.json`, in order.
pub fn snapshot_net_devices(dir: &Path) -> io::Result<Vec<SnapshotNetDevice>> {
    #[derive(Deserialize)]
    struct Net {
        #[serde(default)]
        id: String,
        #[serde(default)]
        num_queues: usize,
        #[serde(default)]
        mac: String,
    }
    #[derive(Deserialize)]
    struct Cfg {
        #[serde(default)]
        net: Vec<Net>,
    }
    let b = std::fs::read(dir.join("config.json"))?;
    let cfg: Cfg = serde_json::from_slice(&b)
        .map_err(|e| io::Error::other(format!("parsing snapshot config.json: {e}")))?;
    Ok(cfg
        .net
        .into_iter()
        .map(|n| SnapshotNetDevice {
            id: n.id,
            queue_pairs: (n.num_queues / 2).max(1),
            mac: n.mac,
        })
        .collect())
}

/// What [`launch_vmm`] starts.
pub struct LaunchOptions<'a> {
    /// The executable; `cloud-hypervisor` from PATH when empty.
    pub binary: &'a str,
    /// The API socket.
    pub api_socket: PathBuf,
    /// The cgroup it starts in.
    pub leaf: Option<&'a ActorLeaf>,
}

/// Starts a VMM with only an API socket (no VM yet) and waits until it
/// answers. The process outlives the request that launched it; the caller
/// owns it. Its output goes to the worker's log.
pub async fn launch_vmm(
    o: LaunchOptions<'_>,
    cancel: &CancellationToken,
) -> io::Result<(Arc<Process>, Client)> {
    let bin = if o.binary.is_empty() {
        "cloud-hypervisor"
    } else {
        o.binary
    };
    let _ = std::fs::remove_file(&o.api_socket);
    let mut cmd = tokio::process::Command::new(bin);
    cmd.arg("--api-socket").arg(&o.api_socket);
    cmd.stdin(std::process::Stdio::null());
    if let Some(leaf) = o.leaf {
        leaf.configure(&mut cmd)?;
    }
    let proc = spawn_logged(cmd, "cloud-hypervisor")?;
    let client = Client::new(&o.api_socket);
    if let Err(e) = client.wait_ready(LAUNCH_TIMEOUT, cancel).await {
        proc.stop().await;
        return Err(io::Error::new(
            e.kind(),
            format!("while waiting for VMM api-socket: {e}"),
        ));
    }
    Ok((proc, client))
}

/// Spawns `cmd` with its piped output forwarded to the log under `name`.
fn spawn_logged(mut cmd: tokio::process::Command, name: &'static str) -> io::Result<Arc<Process>> {
    // Process owns the child, so the output goes through a pipe of our own
    // that a blocking task drains.
    let (r_out, w_out) = std::io::pipe()?;
    let w_err = w_out.try_clone()?;
    cmd.stdout(w_out).stderr(w_err);
    let p = Process::start(cmd)
        .map_err(|e| io::Error::new(e.kind(), format!("while starting {name}: {e}")))?;
    tokio::task::spawn_blocking(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(r_out).lines() {
            let Ok(line) = line else { break };
            tracing::info!(out = %line, "{name}");
        }
    });
    Ok(p)
}

#[cfg(test)]
mod tests;

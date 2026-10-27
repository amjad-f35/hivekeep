//! The subcommands that host the tunnel with the worker's code, which uses
//! Linux network namespaces, nftables and cgroups.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use hivekeep_bench_tunnel::admin::{self, Extra};
use hivekeep_bench_tunnel::supervisor::{self, Supervisor};
use hivekeep_bench_tunnel::workerhost::Host;
use hivekeep_gate::install;
use hivekeep_gate::tunnel::TunnelConfig;
use tokio_util::sync::CancellationToken;

/// Process B: the tunnel started and hosting Actors with the worker's own
/// code, and nothing else, so this process's CPU and memory are the
/// tunnel's. Takes the worker's tunnel flags and serves its counters on
/// /stats; needs root for the Actors' network namespaces.
#[derive(clap::Args)]
pub struct TunnelArgs {
    #[command(flatten)]
    tunnel: TunnelConfig,
    /// Actors to host.
    #[arg(long, default_value_t = 10)]
    actors: usize,
    /// Egress gateway every Actor tunnels to, host:port.
    #[arg(long, default_value = "hk-tunnel-sink:9443")]
    gateway: String,
    /// Address for /stats and /healthz.
    #[arg(long, default_value = ":9090")]
    admin: String,
    /// Log level: debug, info, warn, or error.
    #[arg(long, default_value = "error")]
    log_level: String,
}

pub async fn tunnel(a: TunnelArgs, cancel: CancellationToken) -> anyhow::Result<()> {
    // The tunnel logs a warning per failed dial, which floods the output
    // under a deliberately failing load.
    let mut opts =
        hivekeep_telemetry::InitOptions::cli("hivekeep-tunnel-bench", &Default::default());
    opts.log_level = a.log_level.clone();
    let _telemetry = hivekeep_telemetry::init(opts).context("--log-level")?;
    let (listen, connect) = (
        a.tunnel.listen_address.clone(),
        a.tunnel.connect_listen_address.clone(),
    );
    let host = Arc::new(Host::start(a.tunnel, a.actors, &a.gateway, cancel.clone()).await?);
    let h = host.clone();
    let extra: Extra = Arc::new(move || {
        let mut m = serde_json::Map::new();
        m.insert(
            "tunnel".into(),
            serde_json::to_value(h.info()).unwrap_or_default(),
        );
        m
    });
    let lis = admin::listen_tcp(&a.admin)
        .await
        .context("admin listener")?;
    // Not through tracing, which is quiet by default: the supervisor waits
    // for /healthz, a person reading the log for this.
    eprintln!(
        "tunnel serving: ingress {listen}, connect {connect}, {} Actors, admin {}",
        a.actors, a.admin
    );
    let served = admin::serve_tcp(lis, admin::stats_handler(Some(extra)), cancel.clone()).await;
    let _ = tokio::time::timeout(Duration::from_secs(30), host.close()).await;
    served?;
    Ok(())
}

/// Plays the rest of a worker pod around the tunnel. Arguments after `--`
/// go to `tunnel`.
#[derive(clap::Args)]
pub struct WorkerArgs {
    /// Directory written by `pki`.
    #[arg(long, default_value = "/pki")]
    pki_dir: PathBuf,
    /// CPU limit for the tunnel, in cores; 0 for none.
    #[arg(long, default_value_t = 1.0)]
    tunnel_cpus: f64,
    /// Memory limit for the tunnel, with no swap, e.g. 512m or 1g; empty for none.
    #[arg(long, default_value = "1g")]
    tunnel_memory: String,
    /// Directory shared with the load generator: the actors' sockets and the death log.
    #[arg(long, default_value = "/shared")]
    shared_dir: PathBuf,
    /// The tunnel's admin base URL.
    #[arg(long, default_value = "http://127.0.0.1:9090")]
    tunnel_admin: String,
    /// Response body size of the ingress actors.
    #[arg(long, default_value_t = 16)]
    body_bytes: usize,
    /// Flags for `tunnel`.
    #[arg(last = true)]
    tunnel_args: Vec<String>,
}

pub async fn worker(a: WorkerArgs, cancel: CancellationToken) -> anyhow::Result<()> {
    let mem_bytes = supervisor::parse_bytes(&a.tunnel_memory).context("--tunnel-memory")?;
    let s = Supervisor {
        program: std::env::current_exe()?,
        tunnel_args: a.tunnel_args,
        admin: a.tunnel_admin,
        shared: a.shared_dir,
        body_bytes: a.body_bytes,
        cpus: a.tunnel_cpus,
        mem_bytes,
        cgroup_root: PathBuf::from(supervisor::CGROUP_ROOT),
        keeper_socket: PathBuf::from(install::WORKER_SUPPORT_SOCKET),
    };
    s.run(&a.pki_dir, cancel).await?;
    Ok(())
}

#[derive(clap::Args)]
pub struct NetnsExecArgs {
    /// Path of the network namespace.
    netns: PathBuf,
    /// The command and its arguments.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true, num_args = 1..)]
    command: Vec<String>,
}

/// Enters a network namespace and replaces this process with the command.
/// setns applies to the calling thread only, and exec keeps just that
/// thread, so the whole new program runs in the namespace. Returns only on
/// failure.
#[cfg(target_os = "linux")]
pub fn netns_exec(a: &NetnsExecArgs) -> anyhow::Error {
    use std::os::unix::process::CommandExt;
    let file = match std::fs::File::open(&a.netns) {
        Ok(f) => f,
        Err(e) => return anyhow::anyhow!("opening {}: {e}", a.netns.display()),
    };
    if let Err(e) = nix::sched::setns(&file, nix::sched::CloneFlags::CLONE_NEWNET) {
        return anyhow::anyhow!("entering {}: {e}", a.netns.display());
    }
    let err = std::process::Command::new(&a.command[0])
        .args(&a.command[1..])
        .exec();
    anyhow::anyhow!("exec {}: {err}", a.command[0])
}

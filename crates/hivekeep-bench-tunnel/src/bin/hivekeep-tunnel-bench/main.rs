//! hivekeep-tunnel-bench measures what the worker tunnel costs in CPU and
//! memory as load rises. Every process of the benchmark is a subcommand of
//! this one program; `run-docker.sh` shows how they fit together.

mod load_cmd;
mod rig_cmds;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod worker_cmds;

use clap::{Parser, Subcommand};
use hivekeep_bench_tunnel::procstats::alloc::CountingAllocator;
use tokio_util::sync::CancellationToken;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

/// Measure the worker tunnel's CPU and memory on its own.
#[derive(Parser)]
#[command(name = "hivekeep-tunnel-bench")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Write the throwaway certificates the other subcommands share.
    Pki(rig_cmds::PkiArgs),
    /// Run the fake egress gateway the tunnel's egress connects to.
    Sink(rig_cmds::SinkArgs),
    /// Drive the scenarios through the tunnel and report its CPU and memory per rate.
    Load(load_cmd::LoadArgs),
    /// Stand in for an Actor's program receiving ingress: an HTTP server, with its counters on a Unix socket.
    IngressActor(rig_cmds::IngressActorArgs),
    /// Stand in for an Actor's program making outgoing connections, running egress loads for `load`.
    EgressActor(rig_cmds::EgressActorArgs),
    /// Run the tunnel, hosting Actors with the worker's own code.
    #[cfg(target_os = "linux")]
    Tunnel(worker_cmds::TunnelArgs),
    /// Run the worker pod around the tunnel: fake keeper, the tunnel in its own cgroup, and the Actors' stand-ins.
    #[cfg(target_os = "linux")]
    Worker(worker_cmds::WorkerArgs),
    /// Run COMMAND in the network namespace at path NETNS.
    #[cfg(target_os = "linux")]
    #[command(hide = true)]
    NetnsExec(worker_cmds::NetnsExecArgs),
}

fn main() {
    hivekeep_bench_tunnel::procstats::mark_start();
    let cli = Cli::parse();
    #[cfg(target_os = "linux")]
    if let Cmd::NetnsExec(args) = &cli.command {
        // Before any runtime threads exist: the namespace is entered by
        // this thread, which exec then keeps.
        let err = worker_cmds::netns_exec(args);
        eprintln!("hivekeep-tunnel-bench: {err}");
        std::process::exit(1);
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("hivekeep-tunnel-bench: {e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = runtime.block_on(run(cli.command)) {
        eprintln!("hivekeep-tunnel-bench: {e:#}");
        std::process::exit(1);
    }
}

/// A token cancelled on SIGINT or SIGTERM.
fn shutdown_signal() -> CancellationToken {
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    tokio::spawn(async move {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = async { match term.as_mut() { Some(t) => { t.recv().await; } None => std::future::pending().await } } => {}
        }
        c.cancel();
    });
    cancel
}

async fn run(cmd: Cmd) -> anyhow::Result<()> {
    let cancel = shutdown_signal();
    match cmd {
        Cmd::Pki(a) => rig_cmds::pki(a),
        Cmd::Sink(a) => rig_cmds::sink(a, cancel).await,
        Cmd::Load(a) => load_cmd::load(a, cancel).await,
        Cmd::IngressActor(a) => rig_cmds::ingress_actor(a, cancel).await,
        Cmd::EgressActor(a) => rig_cmds::egress_actor(a, cancel).await,
        #[cfg(target_os = "linux")]
        Cmd::Tunnel(a) => worker_cmds::tunnel(a, cancel).await,
        #[cfg(target_os = "linux")]
        Cmd::Worker(a) => worker_cmds::worker(a, cancel).await,
        #[cfg(target_os = "linux")]
        Cmd::NetnsExec(_) => unreachable!("handled before the runtime starts"),
    }
}

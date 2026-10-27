//! The `load` subcommand: process A, the load generator.

use std::path::PathBuf;
use std::time::Duration;

use hivekeep_bench_tunnel::load::{self, Runner, Targets};
use hivekeep_bench_tunnel::{pki, report};
use tokio_util::sync::CancellationToken;

/// Drives traffic through the tunnel at rising rates, ingress from here and
/// egress through the egress actors in the Actors' sandboxes, and reports,
/// per rate, the tunnel's own CPU and memory as read from its admin
/// endpoint.
#[derive(clap::Args)]
pub struct LoadArgs {
    /// Directory written by `pki`.
    #[arg(long)]
    pki_dir: PathBuf,
    /// Host the tunnel runs on.
    #[arg(long, default_value = "hk-tunnel-worker")]
    tunnel_host: String,
    /// The tunnel's ingress HTTPS port (`--tunnel-listen-address`).
    #[arg(long, default_value_t = 443)]
    ingress_port: u16,
    /// The tunnel's ingress CONNECT port (`--tunnel-connect-listen-address`).
    #[arg(long, default_value_t = 8443)]
    connect_port: u16,
    /// The tunnel's admin base URL.
    #[arg(long, default_value = "http://hk-tunnel-worker:9090")]
    tunnel_admin: String,
    /// The sink's admin base URL.
    #[arg(long, default_value = "http://hk-tunnel-sink:9091")]
    sink_admin: String,
    /// Directory shared with the worker container, holding the egress and ingress actors' sockets.
    #[arg(long, default_value = "/shared")]
    shared_dir: PathBuf,
    /// Comma-separated scenarios to run, or all.
    #[arg(long, default_value = "all", long_help = format!("Comma-separated scenarios to run, or all: {}", load::names()))]
    scenarios: String,
    /// Offered rates per second, in order; connection counts for EgressIdleConns.
    #[arg(long, value_delimiter = ',', default_value = "1000,2000,4000,8000")]
    rungs: Vec<f64>,
    /// Length of each rung.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "10s")]
    step: Duration,
    /// Leading part of each rung left out of the measurement.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "2s")]
    warmup: Duration,
    /// Units in flight at most.
    #[arg(long, default_value_t = 256)]
    workers: usize,
    /// Write every scenario's results to this path as JSON.
    #[arg(long)]
    results_json: Option<PathBuf>,
}

pub async fn load(a: LoadArgs, cancel: CancellationToken) -> anyhow::Result<()> {
    let selected = load::select(&a.scenarios)?;
    if a.rungs.is_empty() {
        anyhow::bail!("--rungs is empty");
    }
    if let Some(r) = a.rungs.iter().find(|r| **r <= 0.0) {
        anyhow::bail!("--rungs: {r} is not a positive number");
    }
    if a.warmup >= a.step {
        anyhow::bail!("--warmup must be shorter than --step");
    }
    let at = |port: u16| format!("{}:{port}", a.tunnel_host);
    let targets = Targets {
        ingress: at(a.ingress_port),
        connect: at(a.connect_port),
        server_name: a.tunnel_host.clone(),
        router_bundle: a.pki_dir.join(pki::ROUTER_BUNDLE),
        ca: a.pki_dir.join(pki::CA_FILE),
        ..Default::default()
    };
    let mut runner = Runner::new(
        targets,
        a.tunnel_admin,
        a.sink_admin,
        a.step,
        a.warmup,
        a.workers,
        Some(a.shared_dir),
    );
    runner.prepare(&cancel).await?;
    let mut reports = Vec::new();
    for s in &selected {
        eprintln!("running {}...", s.name);
        let (rep, result) = runner.run(s, &a.rungs, &cancel).await;
        if let Some(rep) = rep {
            println!("{rep}");
            reports.push(rep);
        }
        if let Some(path) = &a.results_json {
            // Rewritten after every scenario, so an interrupted run keeps
            // what it measured.
            report::write(path, &reports)?;
        }
        result?;
    }
    Ok(())
}

//! glutton: a benchmark workload, run inside an Actor, that consumes RAM,
//! disk, CPU and file descriptors and gossips with its peers on request.

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use hivekeep_bench::boot;
use hivekeep_bench::glutton::{self, Options, Service};

#[derive(Debug, Parser)]
#[command(
    name = "glutton",
    about = "Benchmark workload consuming RAM, disk, CPU and file descriptors on request."
)]
struct Args {
    /// Address and port the server listens on; serves whatever --mode picks.
    #[arg(long, default_value = ":8080")]
    grpc_listen_addr: String,
    /// Address and port of the Prometheus metrics listener.
    #[arg(long, default_value = ":9090")]
    metrics_listen_addr: String,
    /// Directory under which disk files are stored. Required.
    #[arg(long, default_value = "")]
    data_dir: String,
    /// Wire protocol of the main listener: grpc or http.
    #[arg(long, default_value = glutton::MODE_GRPC)]
    mode: String,
    /// Fsync each WriteDisk file before answering, so the write includes the
    /// flush to the backing volume.
    #[arg(long)]
    sync_writes: bool,
    /// Fail /readyz until --data-dir is writable, so a resume waits for an
    /// external volume mount.
    #[arg(long = "readyz-require-writable-data-dir")]
    writable_readyz: bool,
    /// Print the version and exit.
    #[arg(long)]
    version: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.version {
        println!("{}", boot::version());
        return Ok(());
    }
    if args.data_dir.is_empty() {
        eprintln!("--data-dir is required");
        std::process::exit(2);
    }
    let _telemetry = boot::init_logging("glutton");
    boot::install_crypto_provider();

    std::fs::create_dir_all(&args.data_dir)
        .with_context(|| format!("creating data directory {}", args.data_dir))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&args.data_dir, std::fs::Permissions::from_mode(0o700));
    }
    let svc = Arc::new(
        Service::new(
            &args.data_dir,
            Options {
                sync_writes: args.sync_writes,
                writable_readyz: args.writable_readyz,
            },
        )
        .context("constructing glutton service")?,
    );
    let handler = glutton::handler(&args.mode, svc.clone()).context("invalid --mode")?;
    let listener = tokio::net::TcpListener::bind(boot::normalize_addr(&args.grpc_listen_addr))
        .await
        .with_context(|| format!("listening on {}", args.grpc_listen_addr))?;
    let metrics = boot::serve_metrics(&args.metrics_listen_addr, svc.registry().clone())
        .await
        .with_context(|| format!("listening on {}", args.metrics_listen_addr))?;

    tracing::info!(
        listen_addr = %args.grpc_listen_addr,
        metrics_listen_addr = %args.metrics_listen_addr,
        data_dir = %args.data_dir,
        mode = %args.mode,
        "glutton starting"
    );
    let result = tokio::select! {
        r = glutton::serve(listener, handler) => r.context("serving"),
        _ = boot::shutdown_signal() => Ok(()),
    };
    metrics.abort();
    svc.close().await;
    result
}

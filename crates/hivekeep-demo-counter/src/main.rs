//! `counter`: the counter demo workload binary.

use anyhow::Context;
use clap::Parser;
use hivekeep_demo_counter::{app, random_file, state};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "counter",
    about = "Counts requests in memory, on a volume and on the root filesystem"
)]
struct Args {
    #[command(flatten)]
    telemetry: hivekeep_telemetry::TelemetryArgs,
    /// Directory holding the durable-volume counter file.
    #[arg(long, default_value = "/home/counter")]
    file_counter_directory: PathBuf,
    /// Directory for a second volume counter; empty disables it.
    #[arg(long, default_value = "")]
    second_file_counter_directory: String,
    /// Root-filesystem counter file: kept by ROOTFS and MEMORY fidelity
    /// snapshots, reset by VOLUMES.
    #[arg(long, default_value = "/rootfs-counter.txt")]
    rootfs_counter_file: PathBuf,
    /// A file whose content is appended to every response; empty disables it.
    #[arg(long, default_value = "")]
    validate_existing_file_path: String,
    /// Additional HTTP port for arbitrary-port ingress checks; 0 disables it.
    #[arg(long, default_value_t = 0)]
    extra_port: u16,
    /// Plain TCP echo port for tunnelled ingress checks; 0 disables it.
    #[arg(long, default_value_t = 0)]
    tcp_port: u16,
}

fn non_empty(s: String) -> Option<PathBuf> {
    (!s.is_empty()).then(|| PathBuf::from(s))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _telemetry = hivekeep_telemetry::init(hivekeep_telemetry::InitOptions::from_args(
        "hivekeep-demo-counter",
        &args.telemetry,
    ))
    .map_err(|e| anyhow::anyhow!("telemetry: {e}"))?;
    let st = Arc::new(state::State::new(state::Paths {
        file_counter_dir: args.file_counter_directory,
        second_file_counter_dir: non_empty(args.second_file_counter_directory),
        rootfs_counter_file: args.rootfs_counter_file,
        validate_existing_file: non_empty(args.validate_existing_file_path),
    }));

    spawn_sigterm_linger(st.clone());

    let main_listener = tokio::net::TcpListener::bind("0.0.0.0:80")
        .await
        .context("binding :80")?;
    tracing::info!("Starting counter server on port 80");
    let router = app::router(st.clone());
    tokio::spawn(async move {
        if let Err(e) = axum::serve(main_listener, router).await {
            tracing::error!(err = %e, "Error serving");
            std::process::exit(1);
        }
    });

    if args.extra_port > 0 {
        let l = tokio::net::TcpListener::bind(("0.0.0.0", args.extra_port))
            .await
            .context("binding extra port")?;
        tracing::info!(port = args.extra_port, "Starting extra-port server");
        let r = app::extra_port_router(args.extra_port);
        tokio::spawn(async move {
            if let Err(e) = axum::serve(l, r).await {
                tracing::error!(err = %e, "Error serving extra port");
                std::process::exit(1);
            }
        });
    }
    if args.tcp_port > 0 {
        let l = tokio::net::TcpListener::bind(("0.0.0.0", args.tcp_port))
            .await
            .context("binding tcp port")?;
        tracing::info!(port = args.tcp_port, "Starting TCP echo server");
        tokio::spawn(app::tcp_echo(l));
    }

    let rf = Path::new(random_file::RANDOM_FILE);
    match random_file::write(rf) {
        Ok(()) => tracing::info!(fshash = %hash_or_err(rf), "Wrote content to random file"),
        Err(e) => tracing::info!(err = %e, "Error writing random file"),
    }
    st.mark_ready();
    tracing::info!("Readyz now reports OK");

    let mut tick = tokio::time::interval(Duration::from_secs(10));
    let mut count: u64 = 0;
    loop {
        tick.tick().await;
        tracing::info!(count, fshash = %hash_or_err(rf), "Count");
        count += 1;
    }
}

fn hash_or_err(p: &Path) -> String {
    random_file::hash(p).unwrap_or_else(|e| format!("unreadable: {e}"))
}

/// On SIGTERM, lingers for the configured number of seconds before exiting
/// cleanly, so graceful-termination paths can be exercised.
fn spawn_sigterm_linger(st: Arc<state::State>) {
    #[cfg(unix)]
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        let Ok(mut term) = signal(SignalKind::terminate()) else {
            return;
        };
        term.recv().await;
        let secs = st.sigterm_sleep_secs();
        tracing::info!(
            signal = "terminated",
            sleep_secs = secs,
            "Received signal, waiting before exiting"
        );
        tokio::time::sleep(Duration::from_secs(secs.max(0) as u64)).await;
        tracing::info!("Exiting now");
        std::process::exit(0);
    });
    #[cfg(not(unix))]
    let _ = st;
}

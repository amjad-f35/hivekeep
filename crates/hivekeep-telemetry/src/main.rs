//! `hivekeep-otlp-relay`: serves the node OTLP relay socket, forwarding
//! sandbox workers' telemetry to the collector named by the standard
//! `OTEL_EXPORTER_OTLP_*` variables.

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use hivekeep_telemetry::env::ProcessEnv;
use hivekeep_telemetry::otlprelay::{Server, DEFAULT_SOCKET_PATH};

#[derive(Debug, Parser)]
#[command(name = "hivekeep-otlp-relay", version = hivekeep_telemetry::version::VERSION, about)]
struct Args {
    /// Unix socket to serve the relay on. Empty disables the relay.
    #[arg(long, env = "HK_OTLP_RELAY_SOCKET", default_value = DEFAULT_SOCKET_PATH)]
    socket: PathBuf,
    #[command(flatten)]
    telemetry: hivekeep_telemetry::TelemetryArgs,
}

async fn terminated() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let telemetry = hivekeep_telemetry::init(
        hivekeep_telemetry::InitOptions::from_args("hivekeep-otlp-relay", &args.telemetry)
            .metrics(hivekeep_telemetry::MetricsMode::PushOnly),
    )
    .context("telemetry")?;
    tracing::info!(version = %hivekeep_telemetry::version::string(), "Starting hivekeep-otlp-relay");

    let Some(server) = Server::new(&ProcessEnv, &args.socket)? else {
        // Nothing to forward to: stay up without a socket, so workers find
        // none and export directly, rather than restarting in a loop.
        tracing::info!("OTLP relay disabled; idling until terminated");
        terminated().await;
        return Ok(());
    };
    let server = std::sync::Arc::new(server);
    let serving = {
        let server = server.clone();
        tokio::spawn(async move { server.serve().await })
    };
    tokio::select! {
        res = serving => {
            res.context("relay task")?.context("serve the OTLP relay")?;
        }
        _ = terminated() => {
            tracing::info!("Shutting down the OTLP relay");
            server.stop().await;
        }
    }
    let _ = tokio::task::spawn_blocking(move || telemetry.shutdown()).await;
    Ok(())
}

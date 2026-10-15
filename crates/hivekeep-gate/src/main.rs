//! `hivekeep-gate`: the gate's router and SDS certificate minter.

use clap::{Parser, Subcommand};
use hivekeep_gate::router::config::{RouterArgs, RouterConfig};
use hivekeep_gate::{router, sds_mint};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(name = "hivekeep-gate", about = "Hivekeep network gate")]
struct Cli {
    #[command(subcommand)]
    command: Command,
    /// Preview gates to enable (repeatable or comma-separated). The
    /// installer passes it to every component; the gate has no preview
    /// behaviour yet, so it only rejects unknown gate names.
    #[arg(long, global = true, value_delimiter = ',')]
    preview: Vec<String>,
    #[command(flatten)]
    telemetry: hivekeep_telemetry::TelemetryArgs,
}

#[derive(Subcommand)]
// Parsed once at startup, so the size difference is irrelevant.
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Run the ingress or egress router (ext_proc plus Envoy control plane).
    Router(RouterArgs),
    /// Serve short-lived SPIFFE certificates to Envoy over SDS.
    #[command(name = "sdsmint", alias = "sds-mint")]
    SdsMint(SdsMintArgs),
}

#[derive(clap::Args)]
struct SdsMintArgs {
    #[command(flatten)]
    inner: sds_mint::Args,
}

/// Starts process telemetry for one gate role.
fn init_telemetry(
    service: &str,
    args: &hivekeep_telemetry::TelemetryArgs,
    metrics: hivekeep_telemetry::MetricsMode,
) -> anyhow::Result<hivekeep_telemetry::Telemetry> {
    hivekeep_telemetry::init(
        hivekeep_telemetry::InitOptions::from_args(service, args).metrics(metrics),
    )
    .map_err(|e| anyhow::anyhow!("telemetry: {e}"))
}

async fn shutdown_telemetry(t: hivekeep_telemetry::Telemetry) {
    let _ = tokio::task::spawn_blocking(move || t.shutdown()).await;
}

/// Cancels the returned token on SIGINT or SIGTERM.
fn shutdown_signal() -> CancellationToken {
    let token = CancellationToken::new();
    let t = token.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutdown signal received");
        t.cancel();
    });
    token
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("a rustls crypto provider was already installed"))?;
    let args_line = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
    let cli = Cli::parse();
    hivekeep_core::preview::PreviewGates::from_flags(&cli.preview)
        .map_err(|e| anyhow::anyhow!("invalid --preview: {e}"))?;
    let log_level = cli
        .telemetry
        .log_level
        .clone()
        .unwrap_or_else(|| "info".into());
    match cli.command {
        Command::Router(args) => {
            let registry = prometheus::Registry::new();
            let telemetry = init_telemetry(
                "hivekeep-gate-router",
                &cli.telemetry,
                hivekeep_telemetry::MetricsMode::Bridged(registry.clone()),
            )?;
            let mut cfg: RouterConfig = args.into();
            cfg.log_level = log_level;
            let res =
                router::server::run_with_registry(cfg, args_line, shutdown_signal(), registry)
                    .await;
            shutdown_telemetry(telemetry).await;
            res
        }
        Command::SdsMint(args) => {
            let telemetry = init_telemetry(
                "hivekeep-gate-sdsmint",
                &cli.telemetry,
                hivekeep_telemetry::MetricsMode::PushOnly,
            )?;
            let token = shutdown_signal();
            let res = sds_mint::run(args.inner, async move { token.cancelled().await }).await;
            shutdown_telemetry(telemetry).await;
            res
        }
    }
}

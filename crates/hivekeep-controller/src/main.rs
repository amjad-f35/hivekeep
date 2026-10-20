use clap::Parser;

use hivekeep_controller::cli::Args;
use hivekeep_telemetry::{InitOptions, MetricsMode};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    // hivekeep-identity links aws-lc-rs and kube links ring; pick one.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let registry = prometheus::Registry::new();
    let telemetry = hivekeep_telemetry::init(
        InitOptions::from_args("hivekeep-controller", &args.telemetry)
            .metrics(MetricsMode::Bridged(registry.clone())),
    )
    .map_err(|e| anyhow::anyhow!("telemetry: {e}"))?;
    let result = hivekeep_controller::app::run_with_registry(args, registry).await;
    let _ = tokio::task::spawn_blocking(move || telemetry.shutdown()).await;
    result
}

use clap::Parser;
use hivekeep_core::preview::PreviewGates;
use hivekeep_podcert_controller::{app, cli::Args};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Both ring and aws-lc-rs are linked; pick one for every TLS client.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args = Args::parse();

    let mut opts =
        hivekeep_telemetry::InitOptions::from_args("hivekeep-podcert-controller", &args.telemetry);
    if opts.log_format.is_none() && !args.log_json {
        opts.log_format = Some(hivekeep_telemetry::LogFormat::Text);
    }
    let telemetry =
        hivekeep_telemetry::init(opts).map_err(|e| anyhow::anyhow!("telemetry: {e}"))?;
    PreviewGates::from_flags(&args.preview)
        .map_err(|e| anyhow::anyhow!("invalid --preview: {e}"))?;
    args.validate().map_err(anyhow::Error::msg)?;
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        "hivekeep-podcert-controller starting"
    );

    let client = app::kube_client(&args).await?;
    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::info!("Shutting down");
        signal.cancel();
    });
    let result = app::run(args, client, shutdown).await;
    let _ = tokio::task::spawn_blocking(move || telemetry.shutdown()).await;
    result
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

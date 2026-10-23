//! The e2e probe that runs inside an Actor's sandbox and reports what it can
//! see and reach.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // One TLS provider for every client and server in the process.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let _telemetry = hivekeep_telemetry::init(hivekeep_telemetry::InitOptions {
        service_name: "hivekeep-e2e-probe".into(),
        log_target: hivekeep_telemetry::LogTarget::Stderr,
        metrics: hivekeep_telemetry::MetricsMode::PushOnly,
        ..Default::default()
    })
    .map_err(|e| anyhow::anyhow!("telemetry: {e}"))?;
    hivekeep_e2e::fixtures::probe::main().await
}

//! The e2e origin server: HTTP, gRPC and WebSocket echo, egress probing and
//! sandbox killing, one subcommand each.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // One TLS provider for every client and server in the process.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let _telemetry = hivekeep_telemetry::init(hivekeep_telemetry::InitOptions {
        service_name: "hivekeep-e2e-testserver".into(),
        log_target: hivekeep_telemetry::LogTarget::Stderr,
        metrics: hivekeep_telemetry::MetricsMode::PushOnly,
        ..Default::default()
    })
    .map_err(|e| anyhow::anyhow!("telemetry: {e}"))?;
    hivekeep_e2e::fixtures::testserver::main().await
}

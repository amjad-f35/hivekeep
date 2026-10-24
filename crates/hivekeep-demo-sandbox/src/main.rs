//! `sandbox`: the command-execution workload.

use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _telemetry = hivekeep_telemetry::init(hivekeep_telemetry::InitOptions {
        service_name: "hivekeep-demo-sandbox".into(),
        ..Default::default()
    })
    .map_err(|e| anyhow::anyhow!("telemetry: {e}"))?;
    let port = std::env::var("PORT")
        .ok()
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| "80".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .context("binding")?;
    tracing::info!(port = %port, path = hivekeep_demo_sandbox::server::PROCESS_PATH, "Sandbox serving");
    axum::serve(listener, hivekeep_demo_sandbox::server::router())
        .await
        .context("serving")
}

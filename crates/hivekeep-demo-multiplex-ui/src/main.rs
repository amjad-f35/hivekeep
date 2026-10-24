//! `multiplex-ui`: serves the multiplex demo dashboard.
//!
//! Environment:
//! - `PORT` (default `8080`): listen port on all interfaces.
//! - `DEMO_NAMESPACE` (default `claude-multiplex-demo`): the demo hive, and
//!   the namespace worker pod logs are read from.
//! - `HK_API_ENDPOINT` (required): hivekeep-api, e.g. `http://localhost:8443`
//!   behind `kubectl port-forward svc/hivekeep-api 8443:443 -n hivekeep-system`.
//! - `HK_API_TOKEN` (optional): bearer token sent with every call.

use anyhow::Context;
use hivekeep_demo_multiplex_ui::inventory::{
    GrpcInventory, Inventory, API_ENDPOINT_ENV, API_TOKEN_ENV,
};
use hivekeep_demo_multiplex_ui::logs::{KubeLogs, LogSource};
use hivekeep_demo_multiplex_ui::server::{router, AppState};
use hivekeep_demo_multiplex_ui::tasks::TaskBoard;
use std::sync::Arc;

const DEFAULT_PORT: &str = "8080";
const DEFAULT_HIVE: &str = "claude-multiplex-demo";

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let _telemetry = hivekeep_telemetry::init(hivekeep_telemetry::InitOptions {
        service_name: "hivekeep-demo-multiplex-ui".into(),
        ..Default::default()
    })
    .map_err(|e| anyhow::anyhow!("telemetry: {e}"))?;

    let port = env_or("PORT", DEFAULT_PORT);
    let hive = env_or("DEMO_NAMESPACE", DEFAULT_HIVE);
    let endpoint = std::env::var(API_ENDPOINT_ENV)
        .ok()
        .filter(|v| !v.is_empty())
        .with_context(|| {
            format!("{API_ENDPOINT_ENV} must name hivekeep-api, e.g. http://localhost:8443")
        })?;
    let token = std::env::var(API_TOKEN_ENV).ok().filter(|v| !v.is_empty());

    tracing::info!(%endpoint, "connecting to hivekeep-api");
    let inventory: Arc<dyn Inventory> =
        Arc::new(GrpcInventory::connect(&endpoint, token.as_deref()).await?);
    let logs = KubeLogs::connect(&hive)
        .await
        .map(|l| Arc::new(l) as Arc<dyn LogSource>);

    let addr = format!("0.0.0.0:{port}");
    tracing::info!(%addr, %hive, logs = logs.is_some(), "serving dashboard");
    let state = Arc::new(AppState {
        hive,
        api_endpoint: endpoint,
        inventory: Some(inventory),
        logs,
        board: TaskBoard::default(),
    });
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

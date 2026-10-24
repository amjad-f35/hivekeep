//! `egress`: the egress demo server, listening on port 80.

use std::sync::Arc;

use anyhow::Context;
use hivekeep_demo_egress::limits::LISTEN_ADDRESS;
use hivekeep_demo_egress::{router, tls, AppState, HttpFetcher};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _telemetry = hivekeep_telemetry::init(hivekeep_telemetry::InitOptions {
        service_name: "hivekeep-demo-egress".into(),
        ..Default::default()
    })
    .map_err(|e| anyhow::anyhow!("telemetry: {e}"))?;
    tls::install_default_provider();

    let fetcher = HttpFetcher::system()
        .map_err(anyhow::Error::msg)
        .context("building the HTTP client")?;
    let app = router(AppState::new(Arc::new(fetcher)));
    let listener = tokio::net::TcpListener::bind(LISTEN_ADDRESS)
        .await
        .with_context(|| format!("listening on {LISTEN_ADDRESS}"))?;
    tracing::info!(address = LISTEN_ADDRESS, "starting egress demo");
    axum::serve(listener, app)
        .await
        .context("egress demo stopped")?;
    Ok(())
}

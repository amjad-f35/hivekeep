//! `fspersist`: the filesystem-persistence demo workload binary.

use anyhow::Context;
use hivekeep_demo_fspersist::{app, history};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _telemetry = hivekeep_telemetry::init(hivekeep_telemetry::InitOptions {
        service_name: "hivekeep-demo-fspersist".into(),
        ..Default::default()
    })
    .map_err(|e| anyhow::anyhow!("telemetry: {e}"))?;
    let listener = tokio::net::TcpListener::bind("0.0.0.0:80")
        .await
        .context("binding :80")?;
    tracing::info!("Starting fspersist server on port 80");
    axum::serve(listener, app::router(history::HISTORY_PATH.into()))
        .await
        .context("serving")
}

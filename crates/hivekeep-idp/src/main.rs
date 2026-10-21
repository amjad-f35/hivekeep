//! `hivekeep-idp`: serves the OpenID Connect discovery document and JWK set
//! that relying parties use to verify actor JWTs. It publishes the public
//! keys of the authority pool in `--actor-id-jwt-pool`, re-read at most once
//! a minute.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use clap::Parser;
use hivekeep_core::preview::PreviewGates;
use hivekeep_identity::jwt::authority::RefreshingJwtPool;
use hivekeep_idp::flags::missing_flags;
use hivekeep_idp::serve::serve;
use hivekeep_idp::server::IdpServer;
use hivekeep_idp::tls::server_config;
use hivekeep_k8s::install_defaults::{actor_jwt_issuer, namespace_from_pod_env};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

#[derive(Debug, Parser)]
#[command(
    name = "hivekeep-idp",
    version,
    about = "Serves the actor JWT issuer's OpenID discovery document and JWK set."
)]
struct Flags {
    /// Actor JWT issuer URL, https://<host>; must match hivekeep-api's
    /// --actor-jwt-issuer. Defaults to the in-cluster idp Service of this
    /// pod's namespace.
    #[arg(long, default_value_t = actor_jwt_issuer(&namespace_from_pod_env()))]
    issuer: String,
    /// File holding the actor JWT authority pool hivekeep-api signs with.
    #[arg(long, default_value = "")]
    actor_id_jwt_pool: String,
    /// HTTPS listen address.
    #[arg(long, default_value = ":8443")]
    listen_address: String,
    /// Credential bundle (PEM key and chain) presented for serving TLS.
    #[arg(long, default_value = "")]
    server_cred_bundle: String,
    #[command(flatten)]
    telemetry: hivekeep_telemetry::TelemetryArgs,
    /// How long to wait for in-flight requests on shutdown.
    #[arg(long, default_value = "5s", value_parser = humantime::parse_duration)]
    drain_grace: Duration,
    /// Preview gates to enable.
    #[arg(long, value_delimiter = ',')]
    preview: Vec<String>,
}

/// Turns a `:port` listen address (all interfaces) into one the socket API
/// accepts.
fn listen_addr(addr: &str) -> String {
    match addr.strip_prefix(':') {
        Some(port) => format!("0.0.0.0:{port}"),
        None => addr.to_string(),
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let f = Flags::parse();
    let telemetry = hivekeep_telemetry::init(hivekeep_telemetry::InitOptions::from_args(
        "hivekeep-idp",
        &f.telemetry,
    ))
    .context("telemetry")?;
    let gates = PreviewGates::from_flags(&f.preview).context("invalid --preview")?;
    tracing::info!(version = env!("CARGO_PKG_VERSION"), preview = %gates, "Starting hivekeep-idp");

    let missing = missing_flags([
        ("--issuer", f.issuer.as_str()),
        ("--actor-id-jwt-pool", f.actor_id_jwt_pool.as_str()),
        ("--server-cred-bundle", f.server_cred_bundle.as_str()),
    ]);
    if !missing.is_empty() {
        bail!("required flags not set: {}", missing.join(", "));
    }
    let pool =
        RefreshingJwtPool::open(&f.actor_id_jwt_pool).context("loading --actor-id-jwt-pool")?;
    let server = Arc::new(IdpServer::new(&f.issuer, pool).context("cannot publish the keys")?);
    tracing::info!(issuer = %f.issuer, "Serving issuer");

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls = TlsAcceptor::from(Arc::new(server_config(&f.server_cred_bundle, provider)?));
    let addr = listen_addr(&f.listen_address);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("serving on {addr}"))?;
    serve(listener, tls, server, shutdown_signal(), f.drain_grace).await;
    telemetry.shutdown_async().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let f = Flags::parse_from(["hivekeep-idp"]);
        assert_eq!(f.issuer, actor_jwt_issuer(&namespace_from_pod_env()));
        assert_eq!(f.drain_grace, Duration::from_secs(5));
        assert_eq!(listen_addr(&f.listen_address), "0.0.0.0:8443");
    }
}

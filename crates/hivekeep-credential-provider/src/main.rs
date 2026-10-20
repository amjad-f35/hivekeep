//! `hivekeep-credential-provider`: the Kubernetes Secrets credential
//! provider. Serves `hivekeep.credprovider.v1.CredentialProvider` over
//! mutual TLS to the egress gateway's credential injector.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use clap::{CommandFactory, FromArgMatches, Parser};
use hivekeep_core::preview::PreviewGates;
use hivekeep_credential_provider::health::Readiness;
use hivekeep_credential_provider::http::{listen_addr, serve, shutdown_signal};
use hivekeep_credential_provider::metrics::{Metered, ProviderMetrics};
use hivekeep_credential_provider::nsauthz::NamespaceAuthorizer;
use hivekeep_credential_provider::pb::v1::credential_provider_server::CredentialProviderServer;
use hivekeep_credential_provider::pb::FILE_DESCRIPTOR_SET;
use hivekeep_credential_provider::provider::KubeSecretProvider;
use hivekeep_credential_provider::statusz::StatusPage;
use hivekeep_credential_provider::tls::MutualTls;
use hivekeep_k8s::client_rate_limit::rate_limited_client;
use hivekeep_k8s::install_defaults::{egress_spiffe_id, SYSTEM_NAMESPACE};
use tokio::net::TcpListener;
use tokio::sync::watch;

#[derive(Debug, Parser)]
#[command(
    name = "hivekeep-credential-provider",
    version,
    about = "Resolves hk-secret://k8s.io URIs to Kubernetes Secret values."
)]
struct Flags {
    /// gRPC listen address.
    #[arg(long, default_value = ":50051")]
    listen_address: String,
    /// Prometheus /metrics listen address; empty disables it.
    #[arg(long, default_value = ":9090")]
    metrics_address: String,
    /// /readyz and /healthz HTTP listen address.
    #[arg(long, default_value = ":9091")]
    health_address: String,
    /// /statusz HTTP listen address; loopback by default for port-forward
    /// access; empty disables the page.
    #[arg(long, default_value = "127.0.0.1:4040")]
    status_address: String,
    /// Credential bundle (PEM key and chain) presented for serving TLS.
    #[arg(long, default_value = "")]
    server_cred_bundle: String,
    /// CA bundle caller (injector) client certificates must chain to.
    #[arg(long, default_value = "")]
    client_ca_file: String,
    /// SPIFFE ID of the credential injector, the only caller allowed to
    /// fetch secrets. A relocated or renamed install must set it.
    #[arg(long, default_value_t = egress_spiffe_id(SYSTEM_NAMESPACE))]
    injector_identity: String,
    /// The hive → namespace authorization policy (YAML).
    #[arg(long, default_value = "")]
    namespace_policy_file: String,
    /// One of debug, info, warn, error.
    #[arg(long, default_value = "info")]
    log_level: String,
    /// How long to wait for in-flight RPCs on shutdown before a hard stop.
    #[arg(long, default_value = "5s", value_parser = humantime::parse_duration)]
    drain_grace: Duration,
    /// Sustained queries per second allowed against the Kubernetes API.
    #[arg(long, default_value_t = 50.0)]
    kube_api_qps: f32,
    /// Burst of queries allowed against the Kubernetes API.
    #[arg(long, default_value_t = 100)]
    kube_api_burst: i32,
    /// Preview gates to enable.
    #[arg(long, value_delimiter = ',')]
    preview: Vec<String>,
}

/// Process telemetry: JSON logs at `level`, plus OTLP export when an
/// endpoint is configured. The providers live for the whole process.
fn init_logging(level: &str) -> anyhow::Result<()> {
    let telemetry = hivekeep_telemetry::init(hivekeep_telemetry::InitOptions {
        service_name: "hivekeep-credential-provider".into(),
        log_level: level.to_string(),
        metrics: hivekeep_telemetry::MetricsMode::PushOnly,
        ..Default::default()
    })
    .context("invalid --log-level")?;
    std::mem::forget(telemetry);
    Ok(())
}

/// Every flag with its effective value, for the status page.
fn flag_values(matches: &clap::ArgMatches) -> BTreeMap<String, String> {
    Flags::command()
        .get_arguments()
        .filter_map(|a| {
            let id = a.get_id().as_str();
            let long = a.get_long()?;
            let values: Vec<String> = matches
                .get_raw(id)
                .map(|v| v.map(|s| s.to_string_lossy().into_owned()).collect())
                .unwrap_or_default();
            Some((long.to_string(), values.join(",")))
        })
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let matches = Flags::command().get_matches();
    let f = Flags::from_arg_matches(&matches)?;
    init_logging(&f.log_level)?;
    let gates = PreviewGates::from_flags(&f.preview).context("invalid --preview")?;
    tracing::info!(version = env!("CARGO_PKG_VERSION"), preview = %gates, "Starting hivekeep-credential-provider");
    run(f, flag_values(&matches)).await
}

async fn run(f: Flags, flags: BTreeMap<String, String>) -> anyhow::Result<()> {
    if f.kube_api_qps <= 0.0 || f.kube_api_burst <= 0 {
        bail!("--kube-api-qps and --kube-api-burst must be positive");
    }
    let config = kube::Config::incluster().context("in-cluster Kubernetes config")?;
    let burst = u32::try_from(f.kube_api_burst).context("--kube-api-burst")?;
    let client = rate_limited_client(config, f.kube_api_qps, burst)
        .map_err(anyhow::Error::msg)
        .context("Kubernetes client")?;
    tracing::info!(
        qps = f.kube_api_qps,
        burst,
        "Limiting Kubernetes API requests"
    );

    if f.namespace_policy_file.is_empty() {
        bail!("--namespace-policy-file is required");
    }
    let ns_auth =
        NamespaceAuthorizer::load(&f.namespace_policy_file).context("namespace policy")?;
    tracing::info!(file = %f.namespace_policy_file, "Loaded namespace authorization policy");

    if f.server_cred_bundle.is_empty() {
        bail!("--server-cred-bundle is required");
    }
    if f.client_ca_file.is_empty() {
        bail!("--client-ca-file is required");
    }
    let tls = Arc::new(
        MutualTls::new(
            &f.server_cred_bundle,
            &f.client_ca_file,
            &f.injector_identity,
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .context("server credentials")?,
    );
    tracing::info!(ca = %f.client_ca_file, required_san = %f.injector_identity, "Verifying caller client certificates");

    let provider = KubeSecretProvider::new(client, Some(ns_auth));
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()
        .context("gRPC reflection")?;

    let addr = listen_addr(&f.listen_address);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("listen on {addr}"))?;
    let local = listener.local_addr()?;

    let (stop_tx, stop_rx) = watch::channel(false);
    let stopped = |mut rx: watch::Receiver<bool>| async move {
        let _ = rx.wait_for(|s| *s).await;
    };

    // Health starts after the gRPC listener opens so /readyz cannot report
    // ready before the pod accepts calls.
    let readiness = Readiness::default();
    let health_addr = listen_addr(&f.health_address);
    let health = TcpListener::bind(&health_addr)
        .await
        .with_context(|| format!("listen on {health_addr}"))?;
    let r = readiness.clone();
    tokio::spawn(serve(
        health,
        move |req| r.handle(req),
        stopped(stop_rx.clone()),
    ));
    readiness.mark_ready();

    let metrics = Arc::new(ProviderMetrics::new().context("metrics")?);
    if !f.metrics_address.is_empty() {
        let metrics_addr = listen_addr(&f.metrics_address);
        let l = TcpListener::bind(&metrics_addr)
            .await
            .with_context(|| format!("listen on {metrics_addr}"))?;
        tracing::info!(address = %metrics_addr, "metrics listening");
        let m = metrics.clone();
        tokio::spawn(serve(l, move |req| m.handle(req), stopped(stop_rx.clone())));
    }

    if !f.status_address.is_empty() {
        let page = StatusPage::new(
            provider.clone(),
            std::env::args().collect::<Vec<_>>().join(" "),
            flags,
            f.injector_identity.clone(),
        );
        let status_addr = listen_addr(&f.status_address);
        match TcpListener::bind(&status_addr).await {
            Ok(l) => {
                tracing::info!(address = %status_addr, "statusz listening");
                tokio::spawn(serve(
                    l,
                    move |req| page.handle(req),
                    stopped(stop_rx.clone()),
                ));
            }
            // Best effort: the page is a debugging aid.
            Err(e) => {
                tracing::error!(address = %status_addr, err = %e, "statusz server could not listen")
            }
        }
    }

    tracing::info!(address = %local, "credprovider listening");
    let mut server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(reflection)
            .add_service(CredentialProviderServer::new(Metered::new(
                provider, metrics,
            )))
            .serve_with_incoming_shutdown(tls.incoming(listener), stopped(stop_rx.clone())),
    );

    tokio::select! {
        r = &mut server => return r?.context("serving"),
        _ = shutdown_signal() => {}
    }
    tracing::info!("Shutting down");
    readiness.mark_not_ready();
    let _ = stop_tx.send(true);
    match tokio::time::timeout(f.drain_grace, &mut server).await {
        Ok(r) => r?.context("serving"),
        Err(_) => {
            tracing::warn!(grace = ?f.drain_grace, "Graceful shutdown timed out; forcing stop");
            server.abort();
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_flag_values() {
        let m =
            Flags::command().get_matches_from(["hivekeep-credential-provider", "--preview", "a,b"]);
        let f = Flags::from_arg_matches(&m).unwrap();
        assert_eq!(
            f.injector_identity,
            "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-egress"
        );
        assert_eq!(f.drain_grace, Duration::from_secs(5));
        let values = flag_values(&m);
        assert_eq!(values["listen-address"], ":50051");
        assert_eq!(values["injector-identity"], f.injector_identity);
        assert_eq!(values["preview"], "a,b");
    }
}

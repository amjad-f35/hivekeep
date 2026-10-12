//! The object-store plugin sidecar. It serves the plugin API on GCS or S3
//! over a Unix socket: `node` next to the keeper (snapshot transfers and
//! sandbox assets), `control` next to hivekeep-api (cleanup, copy, and on
//! GCS the credential exchange). `healthcheck` reports whether a running
//! plugin is serving, for the sidecar's startup probe: the image has no
//! shell, so the binary checks itself. `HK_STORAGE_BACKEND` picks the
//! backend (`s3`, or GCS by default) with the ambient credentials.

mod exchange;
mod quantity;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use hivekeep_objectstore::plugin::actorlookup::Config as LookupConfig;
use hivekeep_objectstore::plugin::exchange::DEFAULT_PORT;
use hivekeep_objectstore::plugin::exchangeclient::{Config as ExchangeClientConfig, Source};
use hivekeep_objectstore::plugin::{AssetPlugin, ControlPlugin, NodePlugin};
use hivekeep_objectstore::pluginsocket::{
    dial, incoming, listen, serving_health, wait_ready, READY_WAIT,
};
use hivekeep_objectstore::proto::v1::asset_provider_server::AssetProviderServer;
use hivekeep_objectstore::proto::v1::control_provider_server::ControlProviderServer;
use hivekeep_objectstore::proto::v1::node_provider_server::NodeProviderServer;
use hivekeep_objectstore::storage::env::{new_from_env, uses_s3};
use hivekeep_objectstore::store::Store;
use tokio_util::sync::CancellationToken;

use exchange::{serve_exchange, ExchangeConfig};
use quantity::parse_capacity;

/// The namespace hivekeep runs in when the pod does not say.
const DEFAULT_NAMESPACE: &str = "hivekeep-system";
/// Where the pod's service DNS certificate is mounted.
const SERVICE_DNS_DIR: &str = "/run/servicedns.podcert.hivekeep.io";
/// Where the pod identity certificate is mounted.
const POD_IDENTITY_DIR: &str = "/run/podidentity.podcert.hivekeep.io";

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Mode {
    /// Snapshot transfers and sandbox assets, next to the keeper.
    Node,
    /// Snapshot cleanup and copy, and the credential exchange.
    Control,
    /// Checks that a running plugin is serving.
    Healthcheck,
}

/// The API Service host in `namespace`.
fn api_service_host(namespace: &str) -> String {
    format!("api.{namespace}.svc")
}

fn namespace() -> String {
    std::env::var("POD_NAMESPACE")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_NAMESPACE.into())
}

#[derive(Debug, Parser)]
#[command(
    name = "hivekeep-objectstore-plugin",
    about = "Serves the object-store plugin API on GCS or S3"
)]
struct Args {
    #[command(flatten)]
    telemetry: hivekeep_telemetry::TelemetryArgs,
    /// Which plugin to run.
    #[arg(value_enum)]
    mode: Mode,
    /// Unix socket to serve on, or to check in healthcheck mode.
    #[arg(long)]
    socket: PathBuf,
    /// Node mode: the only directory tree local snapshot and asset files may
    /// be read from or written to.
    #[arg(long, default_value = hivekeep_node::paths::DEFAULT_BASE_DIR)]
    root: PathBuf,
    /// Node mode: shared reservations for snapshot compression and upload
    /// buffers, as a Kubernetes quantity; uploads wait for capacity.
    #[arg(long, default_value = "512Mi")]
    upload_memory_budget: String,
    /// Node mode: where assets are downloaded and verified before they are
    /// copied below --root; must not be below --root or readable by the
    /// caller.
    #[arg(
        long,
        default_value = "/var/lib/hivekeep-objectstore-plugin/asset-staging"
    )]
    asset_staging_dir: PathBuf,
    /// Node mode: the most the asset staging directory may hold at once, as
    /// a Kubernetes quantity; downloads wait for space, and a larger asset is
    /// refused.
    #[arg(long, default_value = "8Gi")]
    asset_staging_capacity: String,
    /// Healthcheck mode: how long to wait for the plugin to report serving.
    #[arg(long, default_value = "5s", value_parser = humantime::parse_duration)]
    timeout: Duration,
    /// Node mode on GCS: the credential exchange the control sidecar serves,
    /// where each transfer's actor JWT is traded for a token scoped to the
    /// actor. Defaults to the API Service in the pod's namespace.
    #[arg(long)]
    exchange_address: Option<String>,
    /// Node mode on GCS: the CA bundle verifying the exchange.
    #[arg(long, default_value_t = format!("{SERVICE_DNS_DIR}/trust-bundle.pem"))]
    exchange_ca_file: String,
    /// Node mode on GCS: the name expected on the exchange's certificate.
    #[arg(long)]
    exchange_server_name: Option<String>,
    /// Control mode: TCP address to serve the credential exchange on over
    /// TLS, e.g. `:8445`. Empty leaves it unserved.
    #[arg(long, default_value = "")]
    exchange_listen: String,
    /// Control mode: the credential bundle the exchange serves with.
    #[arg(long, default_value_t = format!("{SERVICE_DNS_DIR}/credential-bundle.pem"))]
    exchange_cred_bundle: String,
    /// Control mode: the issuer of the actor JWTs the exchange accepts.
    #[arg(long, default_value = "")]
    actor_jwt_issuer: String,
    /// Control mode: the JWK set actor JWTs are verified with.
    #[arg(long, default_value = "/run/actor-id-jwt-jwks/jwks.json")]
    actor_jwt_jwks_file: PathBuf,
    /// Control mode on GCS: the service account snapshot transfers run as;
    /// the exchange impersonates it and downscopes its token per actor.
    #[arg(long, default_value = "")]
    transfer_service_account: String,
    /// Control mode: hivekeep-api's gRPC address, the loopback in the pod.
    #[arg(long, default_value = "127.0.0.1:443")]
    api_address: String,
    /// Control mode: the CA bundle verifying hivekeep-api.
    #[arg(long, default_value_t = format!("{SERVICE_DNS_DIR}/trust-bundle.pem"))]
    api_ca_file: String,
    /// Control mode: the name expected on hivekeep-api's certificate.
    #[arg(long)]
    api_server_name: Option<String>,
    /// Control mode: the credential bundle presented to hivekeep-api.
    #[arg(long, default_value_t = format!("{POD_IDENTITY_DIR}/credential-bundle.pem"))]
    api_client_cred_bundle: String,
    /// Preview gates to enable.
    #[arg(long, value_delimiter = ',')]
    preview: Vec<String>,
}

fn new_store() -> Result<Arc<dyn Store>> {
    Ok(hivekeep_objectstore::store::env::new_store_from_env()?)
}

/// The asset URI scheme the node plugin serves: its backend's.
fn asset_scheme() -> &'static str {
    if uses_s3() {
        "s3"
    } else {
        "gs"
    }
}

async fn healthcheck(socket: &std::path::Path, timeout: Duration) -> Result<()> {
    let ch = dial(socket, READY_WAIT)?;
    tokio::time::timeout(timeout, wait_ready(&ch))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "plugin at {} did not report serving within {timeout:?}",
                socket.display()
            )
        })??;
    Ok(())
}

async fn run(args: Args) -> Result<()> {
    hivekeep_core::preview::PreviewGates::from_flags(&args.preview)
        .map_err(|e| anyhow::anyhow!("invalid --preview: {e}"))?;
    if args.mode == Mode::Healthcheck {
        return healthcheck(&args.socket, args.timeout)
            .await
            .context("object-store plugin is not serving");
    }
    let stop = CancellationToken::new();
    let health = serving_health().await;
    let builder = tonic::transport::Server::builder().add_service(health);
    let mut exchange_task = None;
    let router = match args.mode {
        Mode::Node => {
            let objects = new_from_env().context("setting up the object storage backend")?;
            let budget = parse_capacity(&args.upload_memory_budget)
                .map_err(|e| anyhow::anyhow!("invalid --upload-memory-budget: {e}"))?;
            let mut node = NodePlugin::new(objects.clone(), &args.root, budget)
                .context("invalid node upload configuration")?;
            // On GCS every transfer runs on a token scoped to its actor; S3
            // has no per-actor credential and keeps the pod's own.
            if !uses_s3() {
                let host = api_service_host(&namespace());
                let creds = Source::new(ExchangeClientConfig {
                    address: args
                        .exchange_address
                        .clone()
                        .unwrap_or_else(|| format!("{host}:{DEFAULT_PORT}")),
                    ca_file: args.exchange_ca_file.clone().into(),
                    server_name: args.exchange_server_name.clone().unwrap_or(host),
                })
                .context("setting up the credential exchange client")?;
                node = node.with_credentials(Arc::new(creds));
            }
            let capacity = parse_capacity(&args.asset_staging_capacity)
                .map_err(|e| anyhow::anyhow!("invalid --asset-staging-capacity: {e}"))?;
            {
                use std::os::unix::fs::DirBuilderExt;
                std::fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(&args.asset_staging_dir)
                    .context("creating --asset-staging-dir")?;
            }
            let assets = AssetPlugin::new(
                objects,
                asset_scheme(),
                &args.root,
                &args.asset_staging_dir,
                capacity,
            )
            .map_err(|e| anyhow::anyhow!("invalid --asset-staging-dir: {e}"))?;
            builder
                .add_service(NodeProviderServer::new(node))
                .add_service(AssetProviderServer::new(assets))
        }
        Mode::Control => {
            let store = new_store().context("setting up the object storage backend")?;
            if !args.exchange_listen.is_empty() {
                let host = api_service_host(&namespace());
                let cfg = ExchangeConfig {
                    listen: args.exchange_listen.clone(),
                    cred_bundle: args.exchange_cred_bundle.clone().into(),
                    issuer: args.actor_jwt_issuer.clone(),
                    jwks_file: args.actor_jwt_jwks_file.clone(),
                    transfer_account: args.transfer_service_account.clone(),
                    api: LookupConfig {
                        address: args.api_address.clone(),
                        ca_file: args.api_ca_file.clone().into(),
                        server_name: args.api_server_name.clone().unwrap_or(host),
                        client_cred_bundle: args.api_client_cred_bundle.clone().into(),
                    },
                };
                exchange_task = Some(
                    serve_exchange(&cfg, stop.clone())
                        .await
                        .context("serving the credential exchange")?,
                );
            }
            builder.add_service(ControlProviderServer::new(ControlPlugin::new(store)))
        }
        Mode::Healthcheck => unreachable!("handled above"),
    };
    let lis = listen(&args.socket).context("listening")?;
    tracing::info!(mode = ?args.mode, socket = %args.socket.display(), "Serving object-store plugin");
    // Finish in-flight transfers on SIGTERM; the pod's grace period bounds
    // how long. The exchange drains at the same time as the socket.
    let s = stop.clone();
    router
        .serve_with_incoming_shutdown(incoming(lis), async move {
            shutdown_signal().await;
            tracing::info!("Draining object-store plugin");
            s.cancel();
        })
        .await
        .context("serve failed")?;
    if let Some(t) = exchange_task {
        let _ = t.await;
    }
    Ok(())
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

fn main() -> Result<()> {
    hivekeep_objectstore::tls::install_default_provider();
    let args = Args::parse();
    let rt = tokio::runtime::Runtime::new()?;
    let telemetry = rt
        .block_on(async {
            hivekeep_telemetry::init(hivekeep_telemetry::InitOptions::from_args(
                "hivekeep-objectstore-plugin",
                &args.telemetry,
            ))
        })
        .context("telemetry")?;
    let result = rt.block_on(run(args));
    telemetry.shutdown();
    if let Err(e) = result {
        tracing::error!(err = format!("{e:#}"), "object-store plugin failed");
        bail!(e);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_modes() {
        let a = Args::try_parse_from(["p", "node", "--socket", "/run/p.sock"]).unwrap();
        assert_eq!(a.mode, Mode::Node);
        assert_eq!(
            a.root,
            PathBuf::from(hivekeep_node::paths::DEFAULT_BASE_DIR)
        );
        assert_eq!(parse_capacity(&a.upload_memory_budget), Ok(512 << 20));
        assert!(Args::try_parse_from(["p", "other", "--socket", "x"]).is_err());
        assert!(
            Args::try_parse_from(["p", "node"]).is_err(),
            "--socket is required"
        );
        assert_eq!(api_service_host("ns"), "api.ns.svc");
    }

    #[tokio::test]
    async fn healthcheck_fails_against_a_missing_socket() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            healthcheck(&dir.path().join("absent.sock"), Duration::from_millis(200))
                .await
                .is_err()
        );
    }
}

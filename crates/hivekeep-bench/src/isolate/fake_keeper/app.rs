//! Wiring of the fake keeper process.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use hivekeep_core::preview::PreviewGates;
use hivekeep_proto::api::v1::worker_service_client::WorkerServiceClient;
use hivekeep_proto::api::v1::worker_service_server::WorkerServiceServer;
use hivekeep_proto::keeper::v1::keeper_server::KeeperServer;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::config::Args;
use super::epochs::WorkerEpochs;
use super::herder::{new_actor_csr, ActorCertMinter, Herder};
use super::relay::{relay_tls, Relay};
use super::storage::{Backend, ObjectStoreWriter};
use crate::isolate::{health, host_port, init_logging};
use crate::tls::client::{client_config, lazy_channel, PeerCheck};
use crate::tls::server::ServerTls;

/// Runs the fake keeper until SIGINT or SIGTERM.
pub async fn run(args: Args) -> anyhow::Result<()> {
    let _telemetry = init_logging("fake-keeper", &args.log_level)
        .map_err(|e| anyhow!("invalid --log-level: {e}"))?;
    PreviewGates::from_flags(&args.preview).map_err(|e| anyhow!("invalid --preview: {e}"))?;
    let delays = args.delays().map_err(|e| anyhow!("invalid --delay: {e}"))?;

    let storage = Arc::new(ObjectStoreWriter::new(Backend::from_env()));

    // No Kubernetes client: the fake keeper never calls the Kubernetes API,
    // so hivekeep-api is reached through DNS.
    let api_tls = client_config(
        &args.grpc_server_cred_bundle,
        &args.api_ca_file,
        PeerCheck::ServerName(args.api_server_name.clone()),
    )
    .context("building hivekeep-api client credentials")?;
    let channel = lazy_channel(host_port(&args.api_address), api_tls)
        .context("creating hivekeep-api client")?;
    let workers = WorkerServiceClient::new(channel);
    let epochs = Arc::new(WorkerEpochs::new());

    let mut herder = Herder::new(delays, storage);
    herder.epochs = epochs.clone();
    if args.mint_actor_certificate {
        herder.csr = new_actor_csr().map_err(|e| anyhow!("creating the actor CSR: {e}"))?;
        herder.minter = Some(Arc::new(workers.clone()) as Arc<dyn ActorCertMinter>);
    }

    let herder_tls = Arc::new(
        ServerTls::new(&args.grpc_server_cred_bundle, &args.client_ca_certs, None)
            .context("building server TLS config")?,
    );
    let listener = TcpListener::bind(("0.0.0.0", args.port))
        .await
        .context("listening for the keeper API")?;
    let relay_tls = Arc::new(
        relay_tls(
            &args.grpc_server_cred_bundle,
            &args.client_ca_certs,
            &args.relay_allowed_spiffe_id,
        )
        .context("building relay TLS config")?,
    );
    let relay_listener = TcpListener::bind(("0.0.0.0", args.relay_port))
        .await
        .context("listening for the capacity relay")?;
    let health_task = health::serve(&args.health_listen_addr, Arc::new(AtomicBool::new(true)))
        .await
        .context("serving health endpoints")?;

    tracing::warn!(
        delays = ?delays,
        mint_actor_certificate = args.mint_actor_certificate,
        relay_allowed_id = %args.relay_allowed_spiffe_id,
        "Serving the fake keeper: actor lifecycle calls succeed without running any workload"
    );

    // Each port has its own server, so each admits only its own caller:
    // hivekeep-api on the keeper port, fake-workersync on the relay's.
    let stop = CancellationToken::new();
    let keeper_srv = tonic::transport::Server::builder()
        .add_service(KeeperServer::new(herder))
        .serve_with_incoming_shutdown(
            herder_tls.incoming(listener),
            stop.clone().cancelled_owned(),
        );
    let relay_srv = tonic::transport::Server::builder()
        .add_service(WorkerServiceServer::new(Relay::new(
            Arc::new(workers),
            epochs,
        )))
        .serve_with_incoming_shutdown(
            relay_tls.incoming(relay_listener),
            stop.clone().cancelled_owned(),
        );
    let keeper_task = tokio::spawn(keeper_srv);
    let relay_task = tokio::spawn(relay_srv);

    crate::boot::shutdown_signal().await;
    tracing::info!("Shutting down");
    stop.cancel();
    let (k, r) = tokio::join!(keeper_task, relay_task);
    health_task.abort();
    k?.context("serving the keeper API")?;
    r?.context("serving the capacity relay")?;
    Ok(())
}

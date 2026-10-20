//! Wires the controller process: Kubernetes and hivekeep-api clients, the
//! worker registry sync, the reconcilers, and the HTTP listeners.

use std::sync::Arc;

use anyhow::Context;
use hivekeep_core::preview::PreviewGates;
use hivekeep_k8s::install_defaults;
use tokio_util::sync::CancellationToken;

use crate::api_client::{self, DialConfig};
use crate::cli::{listen_addr, Args};
use crate::controllers;
use crate::http_server;
use crate::workersync::{informers, WorkerPoolSyncer};

/// Runs until SIGINT/SIGTERM.
pub async fn run(args: Args) -> anyhow::Result<()> {
    run_with_registry(args, prometheus::Registry::new()).await
}

/// [`run`] recording into `registry`, which the caller may also push over
/// OTLP.
pub async fn run_with_registry(args: Args, registry: prometheus::Registry) -> anyhow::Result<()> {
    args.validate().map_err(anyhow::Error::msg)?;
    let gates = PreviewGates::from_flags(&args.preview)
        .map_err(|e| anyhow::anyhow!("invalid --preview: {e}"))?;
    controllers::init_preview_gates(gates);

    let client = kube::Client::try_default()
        .await
        .context("creating the Kubernetes client")?;
    let system_namespace = install_defaults::namespace_from_pod_env();
    let api = api_client::connect(
        client.clone(),
        &DialConfig {
            target: args.api_conn_spec.clone(),
            ca_file: args.api_ca_file.clone(),
            server_name: args.api_server_name.clone(),
            client_cred_bundle: args.api_client_cert.clone(),
        },
    )
    .context("dialing hivekeep-api")?;

    let shutdown = CancellationToken::new();
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            wait_for_signal().await;
            tracing::info!("Shutting down");
            shutdown.cancel();
        });
    }

    if let Some(addr) = listen_addr(&args.metrics_addr) {
        spawn_listener(addr.to_string(), Some(registry.clone()), shutdown.clone());
    }
    if let Some(addr) = listen_addr(&args.health_addr) {
        spawn_listener(addr.to_string(), None, shutdown.clone());
    }

    // The registry sync runs on informers of its own: it needs a resync to
    // sweep for records that drifted without a pod event, which the
    // reconcilers' caches do not.
    let pods = informers::worker_pod_informer(client.clone());
    let pools = informers::worker_pool_informer(client.clone());
    let syncer = WorkerPoolSyncer::new(
        Arc::new(api.client.clone()),
        client.clone(),
        pods.clone(),
        pools.clone(),
        args.worker_sync_concurrency,
    );
    // Handlers before the informers run, so the initial list enqueues every
    // existing pod.
    let sync_task = syncer.start(shutdown.clone());
    let pod_task = pods.spawn();
    let pool_task = pools.spawn();

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        "hivekeep-controller starting"
    );
    let result = controllers::run(
        client,
        args.controller_config(system_namespace),
        registry,
        shutdown.clone(),
    )
    .await;
    shutdown.cancel();
    pod_task.abort();
    pool_task.abort();
    let _ = sync_task.await;
    drop(api);
    result
}

fn spawn_listener(
    addr: String,
    registry: Option<prometheus::Registry>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        if let Err(err) = http_server::serve(&addr, registry, shutdown).await {
            tracing::error!(%addr, %err, "HTTP listener failed");
        }
    });
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hivekeep_k8s::fake::FakeApiServer;

    use crate::controllers::worker_env::DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL;
    use crate::controllers::{ControllerConfig, WorkerOtelSettings};

    /// The reconcilers cache only the egress CA pool Secret, in the system
    /// namespace, never every Secret in the cluster.
    #[tokio::test]
    async fn secret_cache_is_scoped_to_the_pool_secret() {
        let fake = FakeApiServer::new();
        fake.serve(
            "certificates.k8s.io/v1",
            &[("clustertrustbundles", "ClusterTrustBundle", false)],
        );
        let stop = tokio_util::sync::CancellationToken::new();
        let cfg = ControllerConfig {
            system_namespace: "hivekeep-system".into(),
            keeper_service_account: hivekeep_k8s::install_defaults::KEEPER_SERVICE_ACCOUNT.into(),
            router_service_account: hivekeep_k8s::install_defaults::ROUTER_SERVICE_ACCOUNT.into(),
            dns_egress_policy: DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL.into(),
            otel: WorkerOtelSettings::default(),
        };
        let task = tokio::spawn(crate::controllers::run(
            fake.client(),
            cfg,
            prometheus::Registry::new(),
            stop.clone(),
        ));
        let (ns, name) = crate::controllers::egress_mitm_ca_pool_ref("hivekeep-system");
        let mut seen = Vec::new();
        for _ in 0..200 {
            seen = fake
                .actions()
                .into_iter()
                .filter(|a| a.resource == "secrets" && (a.verb == "list" || a.verb == "watch"))
                .collect();
            if !seen.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!seen.is_empty(), "no Secret list/watch");
        for a in &seen {
            assert_eq!(a.namespace.as_deref(), Some(ns.as_str()), "{a:?}");
            assert_eq!(
                a.query.get("fieldSelector").map(String::as_str),
                Some(format!("metadata.name={name}").as_str()),
                "{a:?}"
            );
        }
        stop.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
}

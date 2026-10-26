//! Command line and wiring of the fake-workersync process.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use clap::Parser;
use hivekeep_core::preview::PreviewGates;
use hivekeep_k8s::crd::WorkerPool;
use hivekeep_k8s::informer::Informer;
use hivekeep_k8s::install_defaults::{keeper_spiffe_id, SYSTEM_NAMESPACE};
use kube::api::Api;
use kube::runtime::watcher;

use super::controller::Controller;
use super::kube_cluster::KubeCluster;
use super::relay_client::GrpcRelay;
use crate::api_client::{connect, DialConfig};
use crate::isolate::fakeworker::{validate_run, DEFAULT_RELAY_PORT};
use crate::isolate::{health, init_logging};

fn default_keeper_id() -> String {
    keeper_spiffe_id(SYSTEM_NAMESPACE)
}

/// fake-workersync's flags.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "fake-workersync",
    version,
    about = "Stands in for the WorkerPool controller and worker registry sync in control-plane benchmarks."
)]
pub struct Args {
    /// hivekeep-api gRPC dial target.
    #[arg(long, default_value = "k8s:///hivekeep-api.hivekeep-system.svc:443")]
    pub api_endpoint: String,
    /// CA bundle hivekeep-api's certificate is verified against.
    #[arg(
        long,
        default_value = "/run/servicedns.podcert.hivekeep.io/trust-bundle.pem"
    )]
    pub api_ca_file: String,
    /// DNS name expected on hivekeep-api's certificate.
    #[arg(long, default_value = "hivekeep-api.hivekeep-system.svc")]
    pub api_server_name: String,
    /// Run prefix of the fake Worker pod names.
    #[arg(long, default_value = "bench")]
    pub run: String,
    /// Namespace of the fake-keeper pods.
    #[arg(long, default_value = SYSTEM_NAMESPACE)]
    pub keeper_namespace: String,
    /// Label selector for the fake-keeper pods.
    #[arg(long, default_value = "hivekeep.io/fake-keeper=true")]
    pub fake_keeper_label: String,
    /// Port the fake keeper serves capacity reports on.
    #[arg(long, default_value_t = DEFAULT_RELAY_PORT)]
    pub relay_port: u16,
    /// SPIFFE ID the fake-keeper relay must present.
    #[arg(long, default_value_t = default_keeper_id())]
    pub keeper_spiffe_id: String,
    /// Credential bundle presented to the relays and to hivekeep-api.
    #[arg(
        long,
        default_value = "/run/podidentity.podcert.hivekeep.io/credential-bundle.pem"
    )]
    pub client_cred_bundle: String,
    /// Pod-identity trust bundle the relay's certificate must chain to.
    #[arg(
        long,
        default_value = "/run/podidentity.podcert.hivekeep.io/trust-bundle.pem"
    )]
    pub trust_bundle: String,
    /// Reconcile at least this often, besides on every WorkerPool change.
    #[arg(long, default_value = "10s", value_parser = humantime::parse_duration)]
    pub resync: Duration,
    /// Worker calls in flight.
    #[arg(long, default_value_t = 32)]
    pub concurrency: usize,
    /// Delete this run's registered fake Workers, then exit. Stop the
    /// serving fake-workersync first: it would go on counting the deleted
    /// Workers until it restarts.
    #[arg(long)]
    pub cleanup: bool,
    /// How long a node keeps its fake Workers with no fake-keeper pod, as
    /// while its fake keeper is replaced.
    #[arg(long, default_value = "5m", value_parser = humantime::parse_duration)]
    pub node_grace: Duration,
    /// Address to serve /healthz and /readyz on.
    #[arg(long, default_value = ":9091")]
    pub health_listen_addr: String,
    /// How long a draining fake Worker waits for its Actors to leave before
    /// it is deleted, releasing them.
    #[arg(long, default_value = "1m", value_parser = humantime::parse_duration)]
    pub drain_grace: Duration,
    /// Minimum log level: debug, info, warn or error.
    #[arg(long, default_value = "info")]
    pub log_level: String,
    /// Preview gates to enable.
    #[arg(long, value_delimiter = ',')]
    pub preview: Vec<String>,
}

/// Checks the flags that clap cannot.
pub fn validate(args: &Args) -> anyhow::Result<()> {
    validate_run(&args.run).context("invalid --run")?;
    if args.concurrency < 1 {
        bail!(
            "invalid --concurrency: must be at least 1, got {}",
            args.concurrency
        );
    }
    Ok(())
}

/// Runs fake-workersync until SIGINT or SIGTERM (or, with `--cleanup`,
/// until its Workers are deleted).
pub async fn run(args: Args) -> anyhow::Result<()> {
    let _telemetry = init_logging("fake-workersync", &args.log_level)
        .map_err(|e| anyhow!("invalid --log-level: {e}"))?;
    PreviewGates::from_flags(&args.preview).map_err(|e| anyhow!("invalid --preview: {e}"))?;
    validate(&args)?;

    let kube = kube::Client::try_default()
        .await
        .context("creating the Kubernetes client")?;
    let conn = connect(
        kube.clone(),
        &DialConfig {
            target: args.api_endpoint.clone(),
            ca_file: args.api_ca_file.clone(),
            server_name: args.api_server_name.clone(),
            client_cred_bundle: args.client_cred_bundle.clone(),
        },
    )
    .context("dialing hivekeep-api")?;
    let relay = Arc::new(
        GrpcRelay::new(
            &args.client_cred_bundle,
            &args.trust_bundle,
            &args.keeper_spiffe_id,
        )
        .context("building the relay client")?,
    );

    let pools = Informer::new(
        Api::<WorkerPool>::all(kube.clone()),
        watcher::Config::default(),
    );
    let changed = Arc::new(tokio::sync::Notify::new());
    {
        let changed = changed.clone();
        pools.add_handler(move |_| changed.notify_one());
    }
    let cluster = Arc::new(KubeCluster::new(
        kube,
        Some(pools.clone()),
        &args.keeper_namespace,
        &args.fake_keeper_label,
        args.relay_port,
        args.node_grace,
    ));
    let c = Controller::new(
        Arc::new(conn.client.clone()),
        relay.clone(),
        cluster.clone(),
        &args.run,
        args.concurrency,
        args.drain_grace,
    );

    // A cleanup run is a one-off that nothing probes, so it serves no
    // health endpoints.
    let ready = Arc::new(AtomicBool::new(false));
    let health_task = if args.cleanup {
        None
    } else {
        Some(
            health::serve(&args.health_listen_addr, ready.clone())
                .await
                .context("serving health endpoints")?,
        )
    };

    let shutdown = crate::boot::shutdown_signal();
    tokio::pin!(shutdown);
    tokio::select! {
        r = c.adopt_until_served(Duration::from_secs(1), Duration::from_secs(30)) => {
            r.map_err(|e| anyhow!("loading registered fake Workers: {e}"))?;
        }
        _ = &mut shutdown => return Ok(()),
    }
    ready.store(true, Ordering::Relaxed);
    // A node whose fake keeper is being replaced has no pod right now; its
    // adopted Workers keep it in the set.
    cluster.remember(&c.nodes());
    if args.cleanup {
        c.delete_all()
            .await
            .map_err(|e| anyhow!("deleting fake Workers: {e}"))?;
        return Ok(());
    }

    let informer_task = pools.spawn();
    while !pools.has_synced() {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            _ = &mut shutdown => return Ok(()),
        }
    }
    let mut ticker = tokio::time::interval(args.resync);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if let Err(err) = c.reconcile().await {
            tracing::warn!(%err, "Reconcile incomplete; retrying");
        }
        tokio::select! {
            _ = &mut shutdown => break,
            _ = changed.notified() => {}
            _ = ticker.tick() => {}
        }
    }
    informer_task.abort();
    relay.close();
    if let Some(t) = health_task {
        t.abort();
    }
    drop(conn);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_validate() {
        let ok = Args::try_parse_from(["fake-workersync"]).unwrap();
        validate(&ok).unwrap();
        assert_eq!(
            ok.keeper_spiffe_id,
            "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-keeper"
        );
        assert_eq!(ok.node_grace, Duration::from_secs(300));
        let bad_run = Args::try_parse_from(["fake-workersync", "--run=Bad_Run"]).unwrap();
        assert!(validate(&bad_run).is_err());
        let bad_conc = Args::try_parse_from(["fake-workersync", "--concurrency=0"]).unwrap();
        assert!(validate(&bad_conc).is_err());
    }
}

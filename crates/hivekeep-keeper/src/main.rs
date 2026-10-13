//! The keeper binary: wires the node agent's parts together and serves the
//! Keeper and WorkerSupport services.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context as _, Result};
use clap::Parser;
use hivekeep_core::preview::PreviewGates;
use hivekeep_core::validation::rules::Operation;
use prometheus::Registry;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server};

use hivekeep_keeper::boot_id::{local_snapshot_boot_check, read_boot_id, BOOT_ID_PATH};
use hivekeep_keeper::clock::SystemClock;
use hivekeep_keeper::config::{Args, ObjectStoreKind};
use hivekeep_keeper::context::Ctx;
use hivekeep_keeper::control::GrpcControlPlane;
use hivekeep_keeper::csi_volumes::CsiVolumeDrivers;
use hivekeep_keeper::device_plugin::kubelet::KubeletRegistrar;
use hivekeep_keeper::device_plugin::{
    start_device_plugins, HOST_DEV_ROOT, KUBELET_DEVICE_PLUGIN_PATH,
};
use hivekeep_keeper::drain::{drain_on_shutdown, force_closable};
use hivekeep_keeper::filecache;
use hivekeep_keeper::filecachegc::{self, CACHE_SANDBOX_ASSET, CACHE_SNAPSHOT};
use hivekeep_keeper::http_endpoints::{self, Readiness};
use hivekeep_keeper::image_gc::ImageCacheGc;
use hivekeep_keeper::keeper::{Keeper, KeeperDeps, KeeperService};
use hivekeep_keeper::keeper_metrics::KeeperInstruments;
use hivekeep_keeper::objectstore::{LocalObjectStore, ObjectStore};
use hivekeep_keeper::oci::ImageCache;
use hivekeep_keeper::peer_identity::{
    bundle_pod_identity, require_same_node, require_spiffe_id, server_tls_config,
    CertPeerAuthenticator,
};
use hivekeep_keeper::prewarm::RuntimePrewarmer;
use hivekeep_keeper::runtime_index::RuntimeIndex;
use hivekeep_keeper::sandbox_assets::{AnonymousFetcher, AssetStager, PublicGcsFetcher};
use hivekeep_keeper::sandbox_syncer::{ClusterView, StaticClusterView, WorkerSandboxSyncer};
use hivekeep_keeper::sandbox_versions::SystemHostProbe;
use hivekeep_keeper::snapshot_cache::SnapshotDownloader;
use hivekeep_keeper::snapshot_report::ControlPlaneReporter;
use hivekeep_keeper::snapshot_tracker::{self, Tracker};
use hivekeep_keeper::snapshot_transfer::SnapshotTransfer;
use hivekeep_keeper::stats_poller::{clamp_poll_interval, StatsInstruments, StatsPoller};
use hivekeep_keeper::system_info::SystemInfoRefresher;
use hivekeep_keeper::trustbundle::{self, Source};
use hivekeep_keeper::volumes::{NoVolumeDrivers, VolumeManager, VolumePluginFactory};
use hivekeep_keeper::worker_dialer::UnixWorkerDialer;
use hivekeep_keeper::worker_jwt::WorkerActorJwts;
use hivekeep_keeper::worker_support::WorkerSupportService;

use hivekeep_imagecache::keychain::Keychain as ImageKeychain;
use hivekeep_imagecache::{Store as ImageStore, StoreOptions as ImageStoreOptions};
use hivekeep_k8s::cluster_view::KubeClusterView;
use hivekeep_k8s::csi_driver_configs::KubeCsiDriverConfigs;
use hivekeep_objectstore::keeper_store::{DirectObjectStore, PluginObjectStore};
use hivekeep_objectstore::plugin::{AssetPlugin, NodePlugin};
use hivekeep_objectstore::storage::env::{new_from_env, uses_s3};

#[tokio::main]
async fn main() -> Result<()> {
    // tonic links ring and reqwest links aws-lc-rs; with two providers
    // present rustls picks neither on its own.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args = Args::parse();
    let registry = Registry::new();
    let telemetry = hivekeep_telemetry::init(hivekeep_telemetry::InitOptions {
        service_name: "keeper".into(),
        log_level: args.log_level.clone(),
        metrics: hivekeep_telemetry::MetricsMode::Bridged(registry.clone()),
        ..Default::default()
    })
    .context("invalid --log-level")?;
    let gates = PreviewGates::from_flags(&args.preview)
        .map_err(|e| anyhow::anyhow!("invalid --preview: {e}"))?;
    args.validate().map_err(anyhow::Error::msg)?;
    tracing::info!(version = env!("CARGO_PKG_VERSION"), "keeper starting");
    let result = run(args, gates, registry).await;
    telemetry.shutdown_async().await;
    result
}

async fn run(args: Args, gates: PreviewGates, registry: Registry) -> Result<()> {
    let root = Ctx::background();
    let readiness = Arc::new(Readiness::default());
    let paths = Arc::new(args.paths());

    tokio::spawn(http_endpoints::serve(
        root.clone(),
        args.metrics_listen_addr.clone(),
        Some(registry.clone()),
        None,
    ));
    tokio::spawn(http_endpoints::serve(
        root.clone(),
        args.health_listen_addr.clone(),
        None,
        Some(readiness.clone()),
    ));

    start_device_plugins(
        &root,
        Arc::new(KubeletRegistrar::new(KUBELET_DEVICE_PLUGIN_PATH)),
        KUBELET_DEVICE_PLUGIN_PATH.as_ref(),
        HOST_DEV_ROOT.as_ref(),
    );

    // Control plane.
    let (cred_bundle, client_ca) = if args.insecure_dev {
        (Vec::new(), Vec::new())
    } else {
        (
            std::fs::read(&args.grpc_server_cred_bundle)
                .context("reading --grpc-server-cred-bundle")?,
            std::fs::read(&args.client_ca_certs).context("reading --client-ca-certs")?,
        )
    };
    let mut ep =
        Endpoint::from_shared(args.api_address.clone()).context("invalid --api-address")?;
    if !args.insecure_dev {
        let ca = std::fs::read(&args.api_ca_file).context("reading --api-ca-file")?;
        ep = ep
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(ca))
                    .domain_name(args.api_server_name.clone())
                    .identity(Identity::from_pem(&cred_bundle, &cred_bundle)),
            )
            .context("control plane TLS")?;
    }
    let control = Arc::new(GrpcControlPlane::new(ep.connect_lazy()));

    // Object storage, snapshots and assets.
    let objects = object_store(&root, &args).await?;
    let boot_id = read_boot_id(BOOT_ID_PATH.as_ref()).map_err(anyhow::Error::msg)?;
    let jwts = Arc::new(WorkerActorJwts::new(control.clone(), Arc::new(SystemClock)));
    let transfer = Arc::new(SnapshotTransfer::new(
        objects.clone(),
        jwts,
        paths.clone(),
        boot_id.clone(),
    ));

    let gc_inst = filecachegc::Instruments::new(&registry)?;
    let asset_cache_dir = args
        .sandbox_asset_cache_dir
        .clone()
        .unwrap_or_else(|| paths.sandbox_asset_cache_dir());
    let asset_cache = hivekeep_keeper::sandbox_assets::open_sandbox_asset_cache(
        &asset_cache_dir,
        filecache::Options {
            min_age: args.sandbox_asset_cache_min_age,
            fetch_timeout: args.sandbox_asset_fetch_timeout,
        },
        &paths.sandbox_asset_scratch_dir(),
        &[&paths.actors_dir(), &paths.workers_dir()],
    )
    .await
    .map_err(anyhow::Error::msg)?;
    filecachegc::start(
        &root,
        CACHE_SANDBOX_ASSET,
        Arc::new(asset_cache.clone()),
        args.sandbox_asset_cache_gc(),
        "sandbox-asset-cache",
        Some(gc_inst.clone()),
    )
    .map_err(anyhow::Error::msg)?;
    let snapshot_cache = match args.snapshot_cache_dir() {
        None => None,
        Some(dir) => {
            let c = hivekeep_keeper::snapshot_cache::open_snapshot_cache(
                &dir,
                filecache::Options {
                    min_age: args.snapshot_cache_min_age,
                    fetch_timeout: args.snapshot_cache_fetch_timeout,
                },
            )
            .await
            .map_err(anyhow::Error::msg)?;
            filecachegc::start(
                &root,
                CACHE_SNAPSHOT,
                Arc::new(c.clone()),
                args.snapshot_cache_gc(),
                "snapshot-cache",
                Some(gc_inst.clone()),
            )
            .map_err(anyhow::Error::msg)?;
            Some(c)
        }
    };
    let anonymous: Option<Arc<dyn AnonymousFetcher>> = if args.anonymous_gcs_assets {
        Some(Arc::new(
            PublicGcsFetcher::new().map_err(anyhow::Error::msg)?,
        ))
    } else {
        None
    };
    let assets = Arc::new(AssetStager::new(
        asset_cache,
        Some(objects),
        anonymous,
        "local",
    ));
    let downloader = Arc::new(SnapshotDownloader::new(transfer.clone(), snapshot_cache));

    // The tracker's workers run until the gRPC servers drain.
    let tracker_ctx = Ctx::background();
    let mut topts = args.tracker_options();
    topts.boot_check = Some(local_snapshot_boot_check(paths.clone(), boot_id));
    topts.instruments = Some(snapshot_tracker::Instruments::new(&registry)?);
    topts.reporter = Some(Arc::new(ControlPlaneReporter::new(control.clone())));
    let tracker = Tracker::new(
        tracker_ctx.clone(),
        paths.clone(),
        Some(transfer.clone()),
        Arc::new(SystemClock),
        topts,
    );

    let runtimes = Arc::new(
        RuntimeIndex::open(paths.sandbox_runtime_index_file()).map_err(anyhow::Error::msg)?,
    );
    let workers = Arc::new(UnixWorkerDialer::new(paths.clone(), 256));

    // Trust bundles for system-info volumes.
    let system_roots = std::fs::read(&args.system_roots_file).unwrap_or_default();
    let egress_file = args.egress_ca_bundle_file.clone();
    let source = Source::new(
        Arc::new(move |name: &str| {
            if name != trustbundle::EGRESS_CTB {
                return Ok(None);
            }
            match &egress_file {
                Some(p) => std::fs::read_to_string(p)
                    .map(Some)
                    .map_err(|e| e.to_string()),
                None => Ok(None),
            }
        }),
        system_roots,
    );
    let system_info = Arc::new(SystemInfoRefresher::new(source));
    {
        let si = system_info.clone();
        let ctx = root.clone();
        tokio::spawn(si.clone().run(ctx.clone()));
        tokio::spawn(async move {
            // The egress CA file has no change notifications; poll it, and
            // let the refresher skip files that already hold its contents.
            while ctx.sleep(Duration::from_secs(60)).await.is_ok() {
                si.on_trust_bundle_object(trustbundle::EGRESS_CTB);
            }
        });
    }

    let images = image_cache(&root, &args, &registry)?;
    let volume_drivers = volume_drivers(&paths).await;
    let keeper = Arc::new(Keeper::new(KeeperDeps {
        paths: paths.clone(),
        workers: workers.clone(),
        images,
        assets: assets.clone(),
        transfer,
        downloader,
        runtimes: runtimes.clone(),
        instruments: Some(KeeperInstruments::new(&registry)?),
        volumes: Arc::new(VolumeManager::new(
            paths.clone(),
            HashMap::new(),
            volume_drivers,
        )),
        system_info,
        snapshots: tracker.clone(),
        validation: Operation::create().with_gates(gates),
    }));
    tracker.start().await;

    // Worker sandbox runtimes.
    let cluster = cluster_view(&args, &cred_bundle).await?;
    let prewarmer = Arc::new(RuntimePrewarmer::new(runtimes.clone(), assets.clone()));
    tokio::spawn(prewarmer.clone().run(root.clone()));
    if let Some(c) = &cluster {
        let syncer = Arc::new(WorkerSandboxSyncer::new(
            c.clone(),
            paths.clone(),
            runtimes,
            Some(prewarmer),
            assets,
            Arc::new(SystemHostProbe),
        ));
        tokio::spawn(syncer.run(root.clone(), args.sandbox_sync_period));
    }
    let interval = clamp_poll_interval(args.actor_stats_poll_interval);
    if !interval.is_zero() {
        let inst = StatsInstruments::new(&registry).ok();
        let poller = Arc::new(StatsPoller::new(
            paths.workers_dir(),
            workers,
            cluster,
            inst,
        ));
        tokio::spawn(poller.run(root.clone(), interval));
    }

    // WorkerSupport, on the node-local socket.
    let oid = args.pod_identity_oid.clone();
    let support = WorkerSupportService::new(
        control.clone(),
        control,
        Arc::new(CertPeerAuthenticator::new(oid.clone())),
    );
    let sock = paths.worker_support_socket();
    match std::fs::remove_file(&sock) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).context("removing a stale worker support socket"),
    }
    if let Some(parent) = sock.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let uds = tokio::net::UnixListener::bind(&sock).context("listening for workers")?;
    hivekeep_keeper::atomicfile::chmod(&sock, 0o600)?;
    let support_server = if args.insecure_dev {
        Server::builder()
            .add_service(support.into_server())
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(uds),
                root.clone().done_owned(),
            )
    } else {
        let node = bundle_pod_identity(&cred_bundle, &oid).map_err(anyhow::Error::msg)?;
        let svc = hivekeep_proto::keeper::v1::worker_support_server::WorkerSupportServer::with_interceptor(
            support,
            require_same_node(node, oid),
        );
        Server::builder()
            .tls_config(server_tls_config(&cred_bundle, &client_ca).map_err(anyhow::Error::msg)?)?
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(uds),
                root.clone().done_owned(),
            )
    };
    let support_task = tokio::spawn(support_server);

    // The Keeper service.
    let addr: std::net::SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("listening for the Keeper service")?;
    let force_close = tokio_util::sync::CancellationToken::new();
    let incoming = force_closable(
        tokio_stream::wrappers::TcpListenerStream::new(listener),
        force_close.clone(),
    );
    let drain = Ctx::background();
    let keeper_svc = KeeperService::new(keeper);
    readiness.mark_serving();
    let shutdown = {
        let drain = drain.clone();
        async move {
            drain.done().await;
        }
    };
    let keeper_server = if args.insecure_dev {
        tokio::spawn(
            Server::builder()
                .layer(hivekeep_telemetry::interceptors::GrpcLogLayer::internal())
                .add_service(keeper_svc.into_server())
                .serve_with_incoming_shutdown(incoming, shutdown),
        )
    } else {
        let svc = hivekeep_proto::keeper::v1::keeper_server::KeeperServer::with_interceptor(
            keeper_svc,
            require_spiffe_id(args.api_allowed_spiffe_id.clone()),
        );
        tokio::spawn(
            Server::builder()
                .tls_config(
                    server_tls_config(&cred_bundle, &client_ca).map_err(anyhow::Error::msg)?,
                )?
                .layer(hivekeep_telemetry::interceptors::GrpcLogLayer::internal())
                .add_service(svc)
                .serve_with_incoming_shutdown(incoming, shutdown),
        )
    };
    tracing::info!(%addr, "Keeper listening");

    // Drain on SIGTERM: not ready, the drain delay, then a graceful stop
    // bounded by the drain timeout, then a forced one.
    wait_for_signal().await;
    tracing::info!("Shutdown signal received; draining");
    drain_on_shutdown(
        &readiness,
        args.drain_delay,
        args.drain_timeout,
        &drain,
        &force_close,
        keeper_server,
    )
    .await;
    tracker_ctx.cancel();
    root.cancel();
    let _ = support_task.await;
    if tracker
        .wait(&Ctx::background().with_timeout(Duration::from_secs(30)))
        .await
        .is_err()
    {
        tracing::warn!("Background snapshot work did not stop in time; exiting anyway");
    }
    tracing::info!("Shutdown complete");
    Ok(())
}

/// The object store the flags select. A plugin that never serves would fail
/// every transfer, so startup waits for it (bounded) instead.
async fn object_store(root: &Ctx, args: &Args) -> Result<Arc<dyn ObjectStore>> {
    Ok(match args.object_store_kind() {
        ObjectStoreKind::Plugin => {
            let s = PluginObjectStore::connect(&args.objectstore_plugin_socket)
                .map_err(anyhow::Error::msg)
                .context("setting up the object-store plugin client")?;
            s.wait_ready(&root.with_timeout(args.objectstore_plugin_ready_timeout))
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "object-store plugin at {} is not serving: {}",
                        args.objectstore_plugin_socket.display(),
                        e.message()
                    )
                })?;
            tracing::info!(socket = %args.objectstore_plugin_socket.display(), "Using the object-store plugin");
            Arc::new(s)
        }
        ObjectStoreKind::Direct => {
            let client = new_from_env().context("setting up the object storage backend")?;
            let node = NodePlugin::new(
                client.clone(),
                &args.base_dir,
                args.objectstore_upload_memory_bytes,
            )
            .context("invalid in-process object store upload configuration")?;
            let staging = args.objectstore_asset_staging_dir();
            {
                use std::os::unix::fs::DirBuilderExt;
                std::fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(&staging)
                    .context("creating --objectstore-asset-staging-dir")?;
            }
            let assets = AssetPlugin::new(
                client,
                if uses_s3() { "s3" } else { "gs" },
                &args.base_dir,
                &staging,
                args.objectstore_asset_staging_bytes,
            )
            .map_err(anyhow::Error::msg)
            .context("invalid --objectstore-asset-staging-dir")?;
            tracing::info!("Using the in-process object store");
            Arc::new(DirectObjectStore::new(node, Some(assets)))
        }
        ObjectStoreKind::Local => {
            let Some(dir) = args.object_store_dir.clone() else {
                bail!("--object-store=local requires --object-store-dir");
            };
            tracing::warn!(dir = %dir.display(), "Using a local directory as object storage; development only");
            Arc::new(LocalObjectStore::new(dir))
        }
    })
}

/// The node's image layer cache, with its eviction loop started.
fn image_cache(root: &Ctx, args: &Args, registry: &Registry) -> Result<Arc<dyn ImageCache>> {
    let dir = args.image_cache_dir();
    let mut opts = ImageStoreOptions::default()
        .with_actors_dir(args.paths().actors_dir())
        .with_min_age(args.image_cache_min_age)
        .with_localhost_registry_replacement(args.localhost_registry_replacement.clone())
        .with_metrics(
            hivekeep_imagecache::metrics::RequestMetrics::register(registry)
                .map_err(|e| anyhow::anyhow!("image cache metrics: {e}"))?,
        );
    // Without a credential provider config, pulls are anonymous.
    if let (Some(config), Some(bin_dir)) = (
        &args.image_credential_provider_config,
        &args.image_credential_provider_bin_dir,
    ) {
        let kc = hivekeep_keeper::credential_provider::Keychain::new(config, bin_dir)
            .map_err(anyhow::Error::msg)
            .context("configuring image pull credentials")?;
        opts = opts.with_keychain(Arc::new(kc) as Arc<dyn ImageKeychain>);
    }
    let store = ImageStore::open(&dir, opts).context("opening the image cache")?;
    let gc = args.image_cache_gc();
    if !gc.period.is_zero() {
        tokio::spawn(ImageCacheGc::new(Arc::new(store.clone()), dir, gc).run(root.clone()));
    }
    Ok(Arc::new(store))
}

/// The CSI node drivers external volumes are mounted with, found through
/// their `CSIDriverConfig`s. Without a Kubernetes API (development) no
/// driver is known, and actors with external volumes fail to start here.
async fn volume_drivers(paths: &hivekeep_keeper::paths::NodePaths) -> Arc<dyn VolumePluginFactory> {
    match kube::Client::try_default().await {
        Ok(client) => Arc::new(CsiVolumeDrivers::new(
            Arc::new(KubeCsiDriverConfigs::new(client)),
            paths.csi_staging_dir(),
        )),
        Err(e) => {
            tracing::warn!(err = %e, "No Kubernetes API for CSIDriverConfigs; external volumes cannot be mounted on this node");
            Arc::new(NoVolumeDrivers)
        }
    }
}

/// The cluster view the sandbox syncer and stats poller read: the
/// Kubernetes API by default, or a JSON file in development.
async fn cluster_view(args: &Args, cred_bundle: &[u8]) -> Result<Option<Arc<dyn ClusterView>>> {
    if let Some(p) = &args.cluster_view_file {
        let v: StaticClusterView =
            serde_json::from_slice(&std::fs::read(p).context("reading --cluster-view-file")?)?;
        tracing::warn!(file = %p.display(), "Using a static cluster view; development only");
        return Ok(Some(Arc::new(v)));
    }
    let node = match &args.node_name {
        Some(n) if !n.is_empty() => n.clone(),
        _ if args.insecure_dev => {
            bail!("--node-name is required with --insecure-dev unless --cluster-view-file is set")
        }
        _ => {
            bundle_pod_identity(cred_bundle, &args.pod_identity_oid)
                .map_err(anyhow::Error::msg)
                .context("reading the node name from the keeper's pod identity")?
                .node_name
        }
    };
    let client = kube::Client::try_default()
        .await
        .context("connecting to the Kubernetes API for the cluster view")?;
    let view = KubeClusterView::new(client, &node);
    view.spawn();
    tracing::info!(node = %node, "Watching worker pods, WorkerPools and SandboxConfigs");
    Ok(Some(Arc::new(view)))
}

async fn wait_for_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

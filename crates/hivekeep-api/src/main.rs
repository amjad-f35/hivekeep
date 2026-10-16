//! hivekeep-api: the Hivekeep control plane.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use clap::Parser;
use hivekeep_api::authn::{self, Authenticator, HttpJwksSource, JwtProvider};
use hivekeep_api::authz::{AllowAll, Authorizer, PolicyAuthorizer, SystemPrincipals};
use hivekeep_api::control::{ControlService, Identity};
use hivekeep_api::csi_volumes::CsiVolumes;
use hivekeep_api::keeper_dialer::{KeeperDialer, StaticKeeperDialer};
use hivekeep_api::keeper_tls::ReloadingClientCert;
use hivekeep_api::kube_dialer::{
    KeeperTls, KubeKeeperDialer, DEFAULT_KEEPER_PORT, DEFAULT_KEEPER_SELECTOR,
};
use hivekeep_api::metrics::Metrics;
use hivekeep_api::object_store::{
    CloudObjectStore, ControlPluginStore, LocalObjectStore, ObjectStore,
};
use hivekeep_api::scheduling::{Scheduler, WorkerSource};
use hivekeep_api::server::{self, Readiness};
use hivekeep_api::volumes::{StaticVolumes, VolumeProvider};
use hivekeep_api::worker_cache::WorkerCache;
use hivekeep_api::worker_service::WorkerServiceImpl;
use hivekeep_api::worker_workflow::WorkerWorkflow;
use hivekeep_api::workflow::{ActorWorkflow, Settings};
use hivekeep_api::{
    assignment_reconciler::AssignmentReconciler, config, template_reconciler::TemplateReconciler,
};
use hivekeep_core::preview::{Gate, PreviewGates};
use hivekeep_identity::jwt::authority::RefreshingJwtPool;
use hivekeep_identity::jwt::verifier::Verifier;
use hivekeep_identity::pod_cert::service_account_spiffe_id;
use hivekeep_identity::state_file::RefreshingCaPool;
use hivekeep_store::{MemoryStore, PgStore, Store, StoreError};
use tokio_util::sync::CancellationToken;

/// Namespace assumed when `POD_NAMESPACE` is unset.
const DEFAULT_NAMESPACE: &str = "hivekeep-system";
const MIN_RESYNC_INTERVAL: Duration = Duration::from_millis(250);

fn namespace() -> String {
    std::env::var("POD_NAMESPACE")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_NAMESPACE.into())
}

fn default_id(sa: &str) -> String {
    service_account_spiffe_id(&namespace(), sa)
}

#[derive(Parser, Debug)]
#[command(name = "hivekeep-api", version, about = "The Hivekeep control plane")]
struct Flags {
    /// Configuration file (.pb, .textproto or .textpb).
    #[arg(long, env = "HK_API_CONFIG_FILE")]
    config_file: String,
    /// gRPC listen address.
    #[arg(long, default_value = "0.0.0.0:443")]
    grpc_listen_addr: SocketAddr,
    /// Prometheus metrics listen address.
    #[arg(long, default_value = "0.0.0.0:9090")]
    metrics_listen_addr: SocketAddr,
    /// /healthz and /readyz listen address.
    #[arg(long, default_value = "0.0.0.0:9091")]
    health_listen_addr: SocketAddr,
    /// Server TLS credential bundle (PRIVATE KEY then CERTIFICATE blocks).
    /// Empty serves plaintext, for local development only.
    #[arg(long, default_value = "")]
    grpc_server_cred_bundle: String,
    /// YAML or JSON file of trusted JWT providers.
    #[arg(long, env = "HK_API_AUTHENTICATION_CONFIG")]
    authentication_config: String,
    /// PostgreSQL read/write connection URL. Empty with --memory-store.
    #[arg(
        long,
        env = "HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING",
        default_value = ""
    )]
    postgres_read_write_connection_string: String,
    /// PostgreSQL owner connection URL (migrations); defaults to read/write.
    #[arg(
        long,
        env = "HK_API_POSTGRES_OWNER_CONNECTION_STRING",
        default_value = ""
    )]
    postgres_owner_connection_string: String,
    /// Role assumed by read/write connections.
    #[arg(long, env = "HK_API_POSTGRES_READ_WRITE_ROLE", default_value = "")]
    postgres_read_write_role: String,
    /// Role assumed by owner connections.
    #[arg(long, env = "HK_API_POSTGRES_OWNER_ROLE", default_value = "")]
    postgres_owner_role: String,
    /// Schema of the Hivekeep tables.
    #[arg(long, env = "HK_API_POSTGRES_SCHEMA", default_value = "hivekeep")]
    postgres_schema: String,
    /// Cap on read/write connections; 0 uses the driver default.
    #[arg(long, env = "HK_API_POSTGRES_POOL_MAX_CONNS", default_value_t = 0)]
    postgres_pool_max_conns: u32,
    /// Keep all state in memory (single replica, development only).
    #[arg(long)]
    memory_store: bool,
    /// Principals that are always global owners.
    #[arg(long, value_delimiter = ',')]
    authz_bootstrap_owners: Vec<String>,
    /// Allow every authenticated request (development only).
    #[arg(long)]
    authz_allow_all: bool,
    /// mTLS SPIFFE ID of the worker controller.
    #[arg(long, default_value_t = default_id("hivekeep-controller"))]
    controller_spiffe_id: String,
    /// mTLS SPIFFE ID of the egress gateway.
    #[arg(long, default_value_t = default_id("hivekeep-gate-egress"))]
    egress_gateway_spiffe_id: String,
    /// mTLS SPIFFE ID of the ingress router.
    #[arg(long, default_value_t = default_id("hivekeep-gate-router"))]
    ingress_router_spiffe_id: String,
    /// mTLS SPIFFE ID of the API server pod's sidecars.
    #[arg(long, default_value_t = default_id("hivekeep-api"))]
    api_server_sidecar_spiffe_id: String,
    /// JWT authority pool file signing Actor JWTs.
    #[arg(long)]
    actor_id_jwt_pool: String,
    /// `iss` of Actor JWTs (https, no query or fragment).
    #[arg(long, env = "HK_API_ACTOR_JWT_ISSUER")]
    actor_jwt_issuer: String,
    /// host:port of the egress gateway sent on every Run and Restore.
    #[arg(long, default_value = "")]
    default_egress_gateway_address: String,
    /// CA pool file signing Actor certificates.
    #[arg(long)]
    actor_id_ca_pool: String,
    /// Pod-identity CA bundle: verifies client and keeper certificates.
    #[arg(long, default_value = "")]
    pod_identity_ca_certs: String,
    /// Credential bundle presented when dialing keepers.
    #[arg(long, default_value = "")]
    keeper_client_cred_bundle: String,
    /// Service account keepers run as (forms their SPIFFE ID).
    #[arg(long, default_value = "hivekeep-keeper")]
    keeper_service_account: String,
    /// Label selector of keeper pods.
    #[arg(long, default_value = DEFAULT_KEEPER_SELECTOR)]
    keeper_selector: String,
    /// Port keepers serve on.
    #[arg(long, default_value_t = DEFAULT_KEEPER_PORT)]
    keeper_port: u16,
    /// Static node=endpoint keeper routes (development); replaces the
    /// Kubernetes lookup.
    #[arg(long, value_delimiter = ',')]
    keeper_endpoints: Vec<String>,
    /// How long to keep serving after SIGTERM before draining.
    #[arg(long, default_value = "13s", value_parser = humantime::parse_duration)]
    drain_delay: Duration,
    /// Deadline of the graceful drain.
    #[arg(long, default_value = "15s", value_parser = humantime::parse_duration)]
    drain_timeout: Duration,
    /// Which object store external snapshots are copied and collected in:
    /// `plugin` (the control plugin's socket, the deployed shape), `direct`
    /// (GCS or S3 in process, as HK_STORAGE_BACKEND selects), `local`
    /// (--objectstore-local-root, development) or `none`. Defaults to
    /// `local` when --objectstore-local-root is set, else `plugin` when
    /// --objectstore-plugin-socket is, else `none`.
    #[arg(long, default_value = "")]
    objectstore: String,
    /// Unix socket of the object-store control plugin.
    #[arg(long, default_value = "")]
    objectstore_plugin_socket: String,
    /// Root directory standing in for object storage (development).
    #[arg(long, default_value = "")]
    objectstore_local_root: String,
    /// Base URI snapshots are stored under.
    #[arg(long, env = "HK_API_SNAPSHOT_LOCATION", default_value = "")]
    snapshot_location: String,
    /// JSON file of storage classes for external volumes
    /// (`{"name": {"provisioner": "...", "parameters": {...}}}`); classes
    /// not in it are read from the cluster's StorageClasses.
    #[arg(long, default_value = "")]
    storage_classes_file: String,
    /// Credential bundle presented to CSI controller drivers whose
    /// CSIDriverConfig enables TLS.
    #[arg(long, default_value = hivekeep_node::csi::tls::DEFAULT_CLIENT_CERT_PATH)]
    csi_client_cred_bundle: String,
    /// Trust bundle CSI controller drivers' serving certificates must
    /// chain to.
    #[arg(long, default_value = hivekeep_node::csi::tls::DEFAULT_CA_CERT_PATH)]
    csi_ca_file: String,
    /// Interval between ActorTemplate resyncs.
    #[arg(long, default_value = "20s", value_parser = humantime::parse_duration)]
    template_resync_interval: Duration,
    /// Interval between full worker relists.
    #[arg(long, default_value = "5m", value_parser = humantime::parse_duration)]
    worker_relist_interval: Duration,
    /// Preview gates to enable (or ALL).
    #[arg(long, value_delimiter = ',')]
    preview: Vec<String>,
}

/// Process telemetry: JSON logs at the configured level, and the metrics
/// registry served on `/metrics` bridged onto the OTLP push when an
/// endpoint is configured.
fn init_logging(
    level: &str,
    registry: prometheus::Registry,
) -> anyhow::Result<hivekeep_telemetry::Telemetry> {
    hivekeep_telemetry::init(hivekeep_telemetry::InitOptions {
        service_name: "hivekeep-api".into(),
        log_level: level.to_string(),
        metrics: hivekeep_telemetry::MetricsMode::Bridged(registry),
        ..Default::default()
    })
    .context("telemetry")
}

async fn connect_store(f: &Flags, stop: &CancellationToken) -> anyhow::Result<Arc<dyn Store>> {
    if f.memory_store {
        tracing::warn!("using the in-memory store; state is lost on restart");
        return Ok(Arc::new(MemoryStore::new()));
    }
    let cfg = server::pg_config(
        &f.postgres_read_write_connection_string,
        &f.postgres_owner_connection_string,
        &f.postgres_read_write_role,
        &f.postgres_owner_role,
        &f.postgres_schema,
        f.postgres_pool_max_conns,
    )
    .map_err(anyhow::Error::msg)?;
    let mut last = None;
    for attempt in 1..=30 {
        match PgStore::connect(&cfg).await {
            Ok(s) => return Ok(Arc::new(s)),
            Err(e @ StoreError::Unavailable(_)) => {
                tracing::warn!(attempt, error = %e, "failed to connect to PostgreSQL, retrying");
                last = Some(e);
            }
            Err(e) => return Err(e.into()),
        }
        tokio::select! {
            _ = stop.cancelled() => bail!("interrupted"),
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
    }
    Err(anyhow::anyhow!(
        "connect to PostgreSQL after 30 attempts: {}",
        last.map(|e| e.to_string()).unwrap_or_default()
    ))
}

fn build_providers(
    cfg: &authn::AuthenticationConfig,
    stop: &CancellationToken,
) -> anyhow::Result<Vec<JwtProvider>> {
    let mut out = Vec::new();
    for p in &cfg.jwt_providers {
        let source = HttpJwksSource::new(
            &p.issuer,
            &p.certificate_authority_file,
            &p.discovery_token_file,
        )
        .with_context(|| format!("initialize JWT provider {:?}", p.name))?;
        source.spawn_refresh(stop.clone());
        out.push(JwtProvider {
            name: p.name.clone(),
            issuer: p.issuer.clone(),
            verifier: Arc::new(Verifier::new(&p.issuer, p.audiences.clone(), source)),
        });
        tracing::info!(name = %p.name, issuer = %p.issuer, "configured JWT provider");
    }
    authn::validate_providers(&out)?;
    Ok(out)
}

async fn keeper_dialer(f: &Flags, keeper_id: &str) -> anyhow::Result<Arc<dyn KeeperDialer>> {
    if !f.keeper_endpoints.is_empty() {
        return Ok(Arc::new(
            StaticKeeperDialer::from_pairs(&f.keeper_endpoints).map_err(anyhow::Error::msg)?,
        ));
    }
    if f.pod_identity_ca_certs.is_empty() || f.keeper_client_cred_bundle.is_empty() {
        bail!("--pod-identity-ca-certs and --keeper-client-cred-bundle are required to dial keepers (or use --keeper-endpoints)");
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client_cert = Arc::new(ReloadingClientCert::new(
        &f.keeper_client_cred_bundle,
        provider.clone(),
    ));
    client_cert.load().map_err(anyhow::Error::msg)?;
    let tls = KeeperTls {
        roots: hivekeep_identity::cred_bundle::load_trust_anchors(&f.pod_identity_ca_certs)?,
        keeper_spiffe_id: keeper_id.to_string(),
        client_cert,
        provider,
    };
    let client = kube::Client::try_default()
        .await
        .context("create Kubernetes client")?;
    Ok(Arc::new(KubeKeeperDialer::new(
        client,
        &namespace(),
        &f.keeper_selector,
        f.keeper_port,
        tls,
    )))
}

/// Where external volumes are provisioned: CSI drivers opened from their
/// CSIDriverConfigs when the ExternalVolumes gate is on, otherwise only the
/// storage classes file (with no drivers, so such templates are refused).
async fn volume_provider(
    f: &Flags,
    gates: &PreviewGates,
) -> anyhow::Result<Arc<dyn VolumeProvider>> {
    let fixed = StaticVolumes::new();
    if !f.storage_classes_file.is_empty() {
        fixed
            .load_storage_classes(
                &std::fs::read(&f.storage_classes_file)
                    .context("reading --storage-classes-file")?,
            )
            .map_err(anyhow::Error::msg)
            .context("parsing --storage-classes-file")?;
    }
    if !gates.is_enabled(Gate::ExternalVolumes) {
        return Ok(Arc::new(fixed));
    }
    let client = kube::Client::try_default()
        .await
        .context("create Kubernetes client for CSIDriverConfigs and StorageClasses")?;
    let open = hivekeep_node::csi::OpenOptions {
        tls_paths: Some(hivekeep_node::csi::TlsPaths {
            client_cert: f.csi_client_cred_bundle.clone().into(),
            ca_cert: f.csi_ca_file.clone().into(),
        }),
        staging_dir: None,
    };
    let configs = Arc::new(hivekeep_k8s::csi_driver_configs::KubeCsiDriverConfigs::new(
        client.clone(),
    ));
    tracing::info!("External volumes: opening CSI controller drivers from CSIDriverConfigs");
    Ok(Arc::new(
        CsiVolumes::new(fixed, configs, Some(client)).with_open_options(open),
    ))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let f = Flags::parse();
    let cfg = config::load(&f.config_file).context("loading config file")?;
    let registry = prometheus::Registry::new();
    let _telemetry = init_logging(
        &cfg.logging
            .as_ref()
            .map(|l| l.level.clone())
            .unwrap_or_default(),
        registry.clone(),
    )?;
    let gates = PreviewGates::from_flags(&f.preview).context("invalid --preview")?;
    tracing::info!(version = env!("CARGO_PKG_VERSION"), preview = %gates, "hivekeep-api starting");

    if f.template_resync_interval < MIN_RESYNC_INTERVAL {
        bail!("--template-resync-interval must be at least {MIN_RESYNC_INTERVAL:?}");
    }
    hivekeep_identity::jwt::claims::validate_issuer(&f.actor_jwt_issuer)
        .context("invalid --actor-jwt-issuer")?;
    if !f.default_egress_gateway_address.is_empty() {
        let errs = hivekeep_core::validation::checks::host_port(
            &"default-egress-gateway-address".into(),
            &f.default_egress_gateway_address,
        );
        if !errs.is_empty() {
            bail!(
                "invalid --default-egress-gateway-address: {}",
                errs.message()
            );
        }
    }
    if !f.snapshot_location.is_empty() {
        hivekeep_core::snapshot::validate_snapshot_location(&f.snapshot_location)
            .context("invalid --snapshot-location")?;
    } else if !gates.is_enabled(Gate::PerTemplateSnapshotLocation) {
        bail!("--snapshot-location is required unless the PerTemplateSnapshotLocation preview gate is enabled");
    }
    if f.authz_bootstrap_owners.is_empty() && !f.authz_allow_all {
        bail!("--authz-bootstrap-owners must list at least one principal");
    }
    if f.keeper_service_account.is_empty() {
        bail!("--keeper-service-account must not be empty");
    }
    let keeper_id = service_account_spiffe_id(&namespace(), &f.keeper_service_account);

    let stop = CancellationToken::new();
    let authn_cfg = authn::load_config(&f.authentication_config)?;
    let providers = build_providers(&authn_cfg, &stop)?;
    let store = connect_store(&f, &stop).await?;

    let metrics = Arc::new(Metrics::with_registry(registry));
    let cache = WorkerCache::new(store.clone(), f.worker_relist_interval);
    cache
        .start(stop.clone())
        .await
        .map_err(anyhow::Error::msg)
        .context("seeding worker cache")?;

    let dialer = keeper_dialer(&f, &keeper_id).await?;
    let volumes = volume_provider(&f, &gates).await?;
    let object_store = object_store(&f).await?;
    let settings = Settings {
        egress_gateway_address: f.default_egress_gateway_address.clone(),
        snapshot_location: f.snapshot_location.clone(),
        gates: gates.clone(),
    };
    let workflow = Arc::new(ActorWorkflow::new(
        store.clone(),
        cache.clone(),
        Scheduler::new(cache.clone() as Arc<dyn WorkerSource>),
        dialer.clone(),
        volumes,
        object_store,
        metrics.clone(),
        settings,
    ));
    let external = gates.is_enabled(Gate::ExternalVolumes);
    let worker_workflow = Arc::new(WorkerWorkflow::new(
        store.clone(),
        Some(dialer),
        metrics.clone(),
        external,
    ));
    let authz: Arc<dyn Authorizer> = if f.authz_allow_all {
        tracing::warn!("authorization disabled: every authenticated request is allowed");
        Arc::new(AllowAll)
    } else {
        let system = SystemPrincipals {
            controller: f.controller_spiffe_id.clone(),
            egress_gateway: f.egress_gateway_spiffe_id.clone(),
            ingress_router: f.ingress_router_spiffe_id.clone(),
            keeper: keeper_id.clone(),
            api_server_sidecar: f.api_server_sidecar_spiffe_id.clone(),
        };
        system
            .validate()
            .map_err(anyhow::Error::msg)
            .context("invalid system SPIFFE IDs")?;
        Arc::new(PolicyAuthorizer::new(
            store.clone(),
            f.authz_bootstrap_owners.clone(),
            system,
        ))
    };
    let identity = Identity {
        jwt_issuer: f.actor_jwt_issuer.clone(),
        jwt_signer: Arc::new(
            RefreshingJwtPool::open(&f.actor_id_jwt_pool)
                .context("loading the Actor JWT authority pool")?,
        ),
        ca: Arc::new(
            RefreshingCaPool::open(&f.actor_id_ca_pool).context("loading the Actor CA pool")?,
        ),
    };
    let control = ControlService::new(
        store.clone(),
        workflow,
        worker_workflow.clone(),
        authz,
        identity.clone(),
    );
    let worker_service = WorkerServiceImpl::new(control.clone(), &keeper_id, identity);

    TemplateReconciler::new(store.clone(), control.clone(), f.template_resync_interval)
        .start(stop.clone());
    AssignmentReconciler::new(store.clone(), worker_workflow).start(&cache, stop.clone());

    let readiness = Readiness::new(store.clone());
    tokio::spawn(server::serve_health(
        f.health_listen_addr,
        readiness.clone(),
        stop.clone(),
    ));
    tokio::spawn(server::serve_metrics(
        f.metrics_listen_addr,
        metrics,
        cache,
        stop.clone(),
    ));

    let tls = if f.grpc_server_cred_bundle.is_empty() {
        tracing::warn!("serving gRPC without TLS");
        None
    } else {
        Some(
            server::server_tls(&f.grpc_server_cred_bundle, &f.pod_identity_ca_certs)
                .map_err(anyhow::Error::msg)?,
        )
    };

    let grpc_stop = CancellationToken::new();
    let (gs, delay, ready, all) = (
        grpc_stop.clone(),
        f.drain_delay,
        readiness.clone(),
        stop.clone(),
    );
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown signal received; draining");
        ready.mark_not_ready();
        tokio::time::sleep(delay).await;
        gs.cancel();
        all.cancel();
    });
    server::serve_grpc(
        f.grpc_listen_addr,
        tls,
        Authenticator::new(providers),
        control,
        worker_service,
        grpc_stop,
        f.drain_timeout,
    )
    .await
    .map_err(anyhow::Error::msg)?;
    tracing::info!("shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

/// The object store the flags select; see `--objectstore`.
async fn object_store(f: &Flags) -> anyhow::Result<Option<Arc<dyn ObjectStore>>> {
    let kind = match f.objectstore.as_str() {
        "" if !f.objectstore_local_root.is_empty() => "local",
        "" if !f.objectstore_plugin_socket.is_empty() => "plugin",
        "" => "none",
        k => k,
    };
    Ok(match kind {
        "plugin" => {
            if f.objectstore_plugin_socket.is_empty() {
                bail!("--objectstore=plugin requires --objectstore-plugin-socket");
            }
            let s =
                ControlPluginStore::connect(std::path::Path::new(&f.objectstore_plugin_socket))?;
            // Fail at startup rather than on the first Tag, giving the
            // sidecar time to come up.
            tokio::time::timeout(Duration::from_secs(60), s.wait_ready())
                .await
                .context("waiting for the object-store control plugin")??;
            tracing::info!(socket = %f.objectstore_plugin_socket, "using the object-store control plugin");
            Some(Arc::new(s))
        }
        "direct" => {
            tracing::info!("using the object store in process");
            Some(Arc::new(CloudObjectStore::from_env()?))
        }
        "local" => {
            if f.objectstore_local_root.is_empty() {
                bail!("--objectstore=local requires --objectstore-local-root");
            }
            tracing::warn!(root = %f.objectstore_local_root, "using a local directory as object storage; development only");
            Some(Arc::new(LocalObjectStore::new(&f.objectstore_local_root)))
        }
        "none" => {
            tracing::warn!(
                "no object store configured: external snapshots are neither copied nor collected"
            );
            None
        }
        k => bail!("unknown --objectstore {k:?}: want plugin, direct, local or none"),
    })
}

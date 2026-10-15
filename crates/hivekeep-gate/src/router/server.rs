//! The router daemon: wires the external processor, the xDS server, the
//! health, status and metrics endpoints and the shutdown drain together.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::api_dial::dial_control_plane;
use super::config::RouterConfig;
use super::dataplane::{xds_listen_address, Dataplane};
use super::drain::{
    drain_on_shutdown, remove_stale_drain_marker, write_drain_marker, DrainParams, ServeHandle,
};
use super::egress::{
    dial_provider, provider_name, Handler as EgressHandler, ProviderDialConfig, RootsLoader,
};
use super::envoy_drain::EnvoyDrainer;
use super::extproc::{
    new_route_duration_histogram, Direction, Handler as ExtprocHandler, Handlers,
    Server as ExtprocServer,
};
use super::health::{serve_probes, HealthConfig, Readiness, RouterHealth};
use super::ingress::{Handler as IngressHandler, ParkingMetrics};
use super::status::{kube_router_ip, listen_status, serve_status, StatusSource};
use super::xds::{XdsServer, DRAIN_ROUTE_BUDGET};
use crate::router::config::DRAIN_TIMEOUT_MARGIN;

fn listen_addr(a: &str) -> String {
    if a.starts_with(':') {
        format!("0.0.0.0{a}")
    } else {
        a.to_string()
    }
}

/// Serves the prometheus registry at `/metrics`.
pub async fn serve_metrics(
    listener: TcpListener,
    registry: prometheus::Registry,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    loop {
        let (s, _) = tokio::select! {
            r = listener.accept() => r?,
            _ = cancel.cancelled() => return Ok(()),
        };
        let registry = registry.clone();
        tokio::spawn(async move {
            let svc =
                hyper::service::service_fn(move |_req: http::Request<hyper::body::Incoming>| {
                    let body = prometheus::TextEncoder::new()
                        .encode_to_string(&registry.gather())
                        .unwrap_or_default();
                    async move {
                        Ok::<_, std::convert::Infallible>(
                            http::Response::builder()
                                .header(http::header::CONTENT_TYPE, "text/plain; version=0.0.4")
                                .body(Full::new(Bytes::from(body)))
                                .unwrap(),
                        )
                    }
                });
            let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(s), svc)
                .await;
        });
    }
}

/// Loads the Actor identity CAs from a PEM file on every call.
fn roots_loader(path: String) -> RootsLoader {
    Arc::new(move || crate::tunnel::ingress::tls::load_roots(std::path::Path::new(&path)))
}

async fn kube_client(kubeconfig: &str) -> anyhow::Result<kube::Client> {
    if kubeconfig.is_empty() {
        return Ok(kube::Client::try_default().await?);
    }
    let kc = kube::config::Kubeconfig::read_from(kubeconfig)
        .map_err(|e| anyhow::anyhow!("failed to read config from path {kubeconfig}: {e}"))?;
    let cfg = kube::Config::from_custom_kubeconfig(kc, &Default::default()).await?;
    Ok(kube::Client::try_from(cfg)?)
}

async fn bind(what: &str, addr: &str) -> anyhow::Result<TcpListener> {
    TcpListener::bind(listen_addr(addr))
        .await
        .map_err(|e| anyhow::anyhow!("failed to listen on {what} address {addr:?}: {e}"))
}

/// Runs the router until `shutdown` fires (SIGTERM/SIGINT), then drains.
pub async fn run(
    cfg: RouterConfig,
    args_line: String,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    run_with_registry(cfg, args_line, shutdown, prometheus::Registry::new()).await
}

/// [`run`] recording into `registry`, which the caller may also push over
/// OTLP.
pub async fn run_with_registry(
    cfg: RouterConfig,
    args_line: String,
    shutdown: CancellationToken,
    registry: prometheus::Registry,
) -> anyhow::Result<()> {
    cfg.validate()
        .map_err(|e| anyhow::anyhow!("invalid router configuration: {e}"))?;
    let dataplane = cfg.dataplane().map_err(anyhow::Error::msg)?;
    let park = cfg.parked_request.normalized();
    remove_stale_drain_marker(&cfg.drain_complete_file);
    let work = CancellationToken::new();

    let readiness = Arc::new(Readiness::default());
    let metrics_lis = bind("metrics", &cfg.metrics_addr).await?;
    tokio::spawn(serve_metrics(metrics_lis, registry.clone(), work.clone()));
    let health_lis = bind("health", &cfg.health_addr).await?;
    tokio::spawn(serve_probes(health_lis, readiness.clone(), work.clone()));

    // Only ingress talks to Kubernetes (EndpointSlices for the hivekeep-api
    // channel, health, status); an egress-only router runs without cluster
    // access and dials the control plane by name.
    let kube =
        if cfg.serves_ingress() {
            Some(kube_client(&cfg.kubeconfig).await.map_err(|e| {
                anyhow::anyhow!("unable to establish Kubernetes configuration: {e}")
            })?)
        } else {
            None
        };
    let api = dial_control_plane(
        &cfg.hivekeep_api_addr,
        &cfg.auth.ca_file,
        &cfg.auth.client_cert_path,
        &cfg.auth.server_name,
        kube.as_ref(),
    )
    .map_err(|e| anyhow::anyhow!("building hivekeep-api dial options: {e}"))?;

    let mut handlers: Handlers = Handlers::new();
    let mut ingress_handler: Option<Arc<IngressHandler>> = None;
    if cfg.serves_ingress() {
        let metrics = ParkingMetrics::new(&registry)?;
        let h = Arc::new(IngressHandler::new(
            Arc::new(api.clone()),
            park,
            Some(metrics),
        ));
        handlers.insert(Direction::Ingress, h.clone() as Arc<dyn ExtprocHandler>);
        ingress_handler = Some(h);
    }
    if cfg.serves_egress() {
        let roots = if cfg.actor_identity_ca_file.is_empty() {
            None
        } else {
            let load = roots_loader(cfg.actor_identity_ca_file.clone());
            load().map_err(|e| anyhow::anyhow!("loading --actor-identity-ca-file: {e}"))?;
            Some(load)
        };
        let (provider, name): (Option<Arc<dyn super::egress::CredentialProvider>>, String) = if cfg
            .credential_provider
            .address
            .is_empty()
        {
            (None, String::new())
        } else {
            let name = provider_name(&cfg.credential_provider.name)
                .map_err(|e| anyhow::anyhow!("--credential-provider-name: {e}"))?;
            let cp = &cfg.credential_provider;
            let client = dial_provider(&ProviderDialConfig {
                address: cp.address.clone(),
                ca_file: (!cp.ca_file.is_empty()).then(|| cp.ca_file.clone().into()),
                client_cert: (!cp.client_cert.is_empty()).then(|| cp.client_cert.clone().into()),
                server_name: (!cp.server_name.is_empty()).then(|| cp.server_name.clone()),
                insecure: cp.insecure,
            })
            .map_err(|e| anyhow::anyhow!("dial credential provider: {e}"))?;
            tracing::info!(provider = %cp.address, provider_name = %cp.name, "egress credential injection enabled");
            (Some(Arc::new(client)), name)
        };
        let h = EgressHandler::new(
            Arc::new(api.clone()),
            roots,
            cfg.egress_policy_cache_ttl,
            provider,
            name,
        );
        handlers.insert(Direction::Egress, Arc::new(h) as Arc<dyn ExtprocHandler>);
    }
    let route_duration = new_route_duration_histogram(&registry)?;
    let extproc = Arc::new(ExtprocServer::new(Some(route_duration), handlers));

    let health = Arc::new(RouterHealth::new(
        HealthConfig {
            serves_ingress: cfg.serves_ingress(),
            dataplane,
            interval: cfg.health_interval,
        },
        kube.clone()
            .map(|k| Arc::new(k) as Arc<dyn super::health::K8sProbe>),
        Some(Arc::new(api.clone())),
    ));
    {
        let (h, c) = (health.clone(), work.clone());
        tokio::spawn(async move { h.run(c).await });
    }

    if cfg.status_port > 0 {
        let lis = listen_status(&cfg.status_addr, cfg.status_port as u16)
            .await
            .map_err(anyhow::Error::msg)?;
        let parking = ingress_handler
            .clone()
            .map(|h| Arc::new(move || h.parking_status()) as Arc<dyn Fn() -> _ + Send + Sync>);
        let source = Arc::new(StatusSource {
            router_ip: kube
                .clone()
                .map(|k| kube_router_ip(k, cfg.namespace.clone(), cfg.router_service_name.clone())),
            namespace: cfg.namespace.clone(),
            mode: cfg.mode.clone(),
            https_port: cfg.https_port,
            xds_port: cfg.xds_port,
            extproc_port: cfg.extproc_port,
            status_port: cfg.status_port as u16,
            args: args_line,
            queries: Some(extproc.recorder()),
            health: Some(health.clone()),
            parking,
            ..Default::default()
        });
        tokio::spawn(serve_status(lis, source, work.clone()));
    }

    if cfg.serves_ingress() && dataplane == Dataplane::Envoy {
        let xds = XdsServer::new(cfg.xds_port);
        xds.set_config(cfg.extproc_port, &cfg.extproc_addr);
        xds.set_connect_tls_port(cfg.connect_tls_port);
        xds.set_otlp_collector_or_disable(&cfg.otlp_collector_address);
        xds.set_route_timeout(cfg.route_timeout);
        xds.set_ext_proc_max_requests(cfg.ext_proc_max_requests());
        if park.enabled() {
            xds.set_ext_proc_message_timeout(park.budget + Duration::from_secs(5));
        }
        xds.set_tls_config(cfg.https_port, &cfg.envoy_cert_path);
        crate::tunnel::ingress::tls::load_roots(std::path::Path::new(
            &cfg.ingress_auth.client_ca_file,
        ))
        .map_err(|e| anyhow::anyhow!("loading --ingress-client-ca-file: {e}"))?;
        xds.set_downstream_client_auth(
            &cfg.ingress_auth.client_ca_file,
            &cfg.ingress_auth.allowed_spiffe_ids,
        );
        if cfg.ingress_auth.allowed_spiffe_ids.is_empty() {
            tracing::info!("no --ingress-allowed-spiffe-ids, so the router serves no traffic");
        }
        xds.set_upstream_tls(
            &cfg.upstream_credential_bundle_path,
            &cfg.upstream_trust_bundle_path,
        );
        xds.set_upstream_connect(cfg.upstream_connect);
        xds.update_snapshot()
            .map_err(|e| anyhow::anyhow!("building the xDS snapshot: {e}"))?;
        let addr = xds_listen_address(cfg.xds_port);
        let lis = bind("xDS", &addr).await?;
        tracing::info!(address = %addr, "starting Envoy xDS server");
        let c = work.clone();
        tokio::spawn(async move {
            if let Err(err) = xds.serve(lis, c).await {
                tracing::error!(%err, "xDS server failed");
            }
        });
    }

    let ext_addr = if cfg.extproc_addr.is_empty() {
        format!("0.0.0.0:{}", cfg.extproc_port)
    } else if cfg.extproc_addr.contains(':') {
        format!("[{}]:{}", cfg.extproc_addr, cfg.extproc_port)
    } else {
        format!("{}:{}", cfg.extproc_addr, cfg.extproc_port)
    };
    let ext_lis = bind("ext_proc", &ext_addr).await?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(ext_lis);
    let svc = extproc.clone().into_service();
    let handle = Arc::new(ServeHandle::spawn(move |stop| {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(incoming, stop)
    }));
    readiness.mark_ready();
    tracing::info!(mode = %cfg.mode, dataplane = dataplane.as_str(), "router serving");

    let drainer = (dataplane == Dataplane::Envoy).then(|| {
        Arc::new(EnvoyDrainer::new(cfg.envoy_admin_addr.clone()))
            as Arc<dyn super::drain::DataplaneDrainer>
    });
    let work2 = work.clone();
    let marker = cfg.drain_complete_file.clone();
    let drained = drain_on_shutdown(
        shutdown,
        DrainParams {
            readiness,
            delay: cfg.drain_delay,
            dataplane: drainer,
            dataplane_window: DRAIN_ROUTE_BUDGET + DRAIN_TIMEOUT_MARGIN,
            extproc: handle,
            timeout: cfg.drain_timeout(&park),
            stop_rest: Box::new(move || {
                write_drain_marker(&marker);
                work2.cancel();
            }),
        },
    );
    let _ = drained.await;
    Ok(())
}

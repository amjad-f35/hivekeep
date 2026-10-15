//! State-of-the-world discovery services serving the snapshot cache: ADS,
//! plus the per-type CDS, EDS, LDS, RDS and SDS services.
//!
//! Each stream tracks, per type URL, the resource names it asked for and the
//! version it was last sent. A request that subscribes to a type or changes
//! its names is answered from the current snapshot; an ACK or NACK of the
//! version already sent is not. A new snapshot is pushed to every type a
//! stream subscribed to. Named requests (RDS, SDS) are answered only once
//! the snapshot holds every name asked for.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use envoy_types::pb::envoy::service::cluster::v3::cluster_discovery_service_server::{
    ClusterDiscoveryService, ClusterDiscoveryServiceServer,
};
use envoy_types::pb::envoy::service::discovery::v3::aggregated_discovery_service_server::{
    AggregatedDiscoveryService, AggregatedDiscoveryServiceServer,
};
use envoy_types::pb::envoy::service::discovery::v3::{
    DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
};
use envoy_types::pb::envoy::service::endpoint::v3::endpoint_discovery_service_server::{
    EndpointDiscoveryService, EndpointDiscoveryServiceServer,
};
use envoy_types::pb::envoy::service::listener::v3::listener_discovery_service_server::{
    ListenerDiscoveryService, ListenerDiscoveryServiceServer,
};
use envoy_types::pb::envoy::service::route::v3::route_discovery_service_server::{
    RouteDiscoveryService, RouteDiscoveryServiceServer,
};
use envoy_types::pb::envoy::service::secret::v3::secret_discovery_service_server::{
    SecretDiscoveryService, SecretDiscoveryServiceServer,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use super::snapshot::{
    Snapshot, SnapshotCache, CLUSTER_TYPE, ENDPOINT_TYPE, LISTENER_TYPE, ROUTE_TYPE, SECRET_TYPE,
};

/// Depth of each stream's send queue.
const SEND_DEPTH: usize = 16;

type Out<T> = ReceiverStream<Result<T, Status>>;

/// The discovery services over one snapshot cache, serving `node_id`.
#[derive(Debug, Clone)]
pub struct DiscoveryServer {
    cache: SnapshotCache,
    node_id: Arc<str>,
    nonce: Arc<AtomicU64>,
}

#[derive(Debug, Default)]
struct Subscription {
    names: Vec<String>,
    sent_version: Option<String>,
    dirty: bool,
}

/// The response to a subscription of `type_url` to `names` (all when
/// empty), or none while a named resource is missing.
fn response(
    snapshot: &Snapshot,
    type_url: &str,
    names: &[String],
    nonce: u64,
) -> Option<DiscoveryResponse> {
    let empty = Default::default();
    let all = snapshot.resources(type_url).unwrap_or(&empty);
    let resources = if names.is_empty() {
        all.values().cloned().collect()
    } else {
        let mut picked = Vec::with_capacity(names.len());
        for n in names {
            picked.push(all.get(n)?.clone());
        }
        picked
    };
    Some(DiscoveryResponse {
        version_info: snapshot.version().to_string(),
        resources,
        type_url: type_url.to_string(),
        nonce: nonce.to_string(),
        ..Default::default()
    })
}

impl DiscoveryServer {
    /// Services over `cache`, serving the snapshot published to `node_id`.
    pub fn new(cache: SnapshotCache, node_id: &str) -> Self {
        DiscoveryServer {
            cache,
            node_id: node_id.into(),
            nonce: Arc::default(),
        }
    }

    /// The ADS service.
    pub fn ads_service(&self) -> AggregatedDiscoveryServiceServer<Self> {
        AggregatedDiscoveryServiceServer::new(self.clone())
    }

    /// The CDS service.
    pub fn cds_service(&self) -> ClusterDiscoveryServiceServer<Self> {
        ClusterDiscoveryServiceServer::new(self.clone())
    }

    /// The EDS service.
    pub fn eds_service(&self) -> EndpointDiscoveryServiceServer<Self> {
        EndpointDiscoveryServiceServer::new(self.clone())
    }

    /// The LDS service.
    pub fn lds_service(&self) -> ListenerDiscoveryServiceServer<Self> {
        ListenerDiscoveryServiceServer::new(self.clone())
    }

    /// The RDS service.
    pub fn rds_service(&self) -> RouteDiscoveryServiceServer<Self> {
        RouteDiscoveryServiceServer::new(self.clone())
    }

    /// The SDS service.
    pub fn sds_service(&self) -> SecretDiscoveryServiceServer<Self> {
        SecretDiscoveryServiceServer::new(self.clone())
    }

    fn next_nonce(&self) -> u64 {
        self.nonce.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Starts serving one stream; `fixed_type` pins the type of a per-type
    /// service.
    fn open(
        &self,
        incoming: Streaming<DiscoveryRequest>,
        fixed_type: Option<&'static str>,
    ) -> Out<DiscoveryResponse> {
        let (tx, rx) = mpsc::channel(SEND_DEPTH);
        let server = self.clone();
        tokio::spawn(async move {
            if let Err(status) = server.serve_stream(incoming, &tx, fixed_type).await {
                let _ = tx.send(Err(status)).await;
            }
        });
        ReceiverStream::new(rx)
    }

    async fn serve_stream(
        &self,
        mut incoming: Streaming<DiscoveryRequest>,
        tx: &mpsc::Sender<Result<DiscoveryResponse, Status>>,
        fixed_type: Option<&'static str>,
    ) -> Result<(), Status> {
        let mut snapshots = self.cache.subscribe();
        let mut node_matches: Option<bool> = None;
        let mut subs: HashMap<String, Subscription> = HashMap::new();
        loop {
            tokio::select! {
                req = incoming.message() => {
                    let Some(req) = req? else { return Ok(()) };
                    if node_matches.is_none() {
                        if let Some(node) = &req.node {
                            node_matches = Some(node.id == *self.node_id);
                        }
                    }
                    let type_url = match (fixed_type, req.type_url.as_str()) {
                        (Some(t), "") => t.to_string(),
                        (Some(t), got) if got != t => {
                            return Err(Status::invalid_argument(format!("type {got:?} requested on the {t} stream")));
                        }
                        (_, "") => return Err(Status::invalid_argument("discovery request names no type URL")),
                        (_, got) => got.to_string(),
                    };
                    if let Some(err) = &req.error_detail {
                        tracing::warn!(type_url, version = req.version_info, error = err.message, "Envoy rejected an xDS update");
                    }
                    let sub = subs.entry(type_url).or_insert_with(|| Subscription { dirty: true, ..Default::default() });
                    if sub.names != req.resource_names {
                        sub.names = req.resource_names;
                        sub.dirty = true;
                    }
                }
                changed = snapshots.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }
                }
            }
            if node_matches != Some(true) {
                continue;
            }
            let current = snapshots.borrow_and_update().clone();
            let Some((node, snapshot)) = current else {
                continue;
            };
            if *node != *self.node_id {
                continue;
            }
            for (type_url, sub) in subs.iter_mut() {
                if !sub.dirty && sub.sent_version.as_deref() == Some(snapshot.version()) {
                    continue;
                }
                let Some(resp) = response(&snapshot, type_url, &sub.names, self.next_nonce())
                else {
                    continue;
                };
                if tx.send(Ok(resp)).await.is_err() {
                    return Ok(());
                }
                sub.sent_version = Some(snapshot.version().to_string());
                sub.dirty = false;
            }
        }
    }

    fn fetch(&self, req: DiscoveryRequest, type_url: &str) -> Result<DiscoveryResponse, Status> {
        let node = req.node.as_ref().map(|n| n.id.as_str()).unwrap_or_default();
        let snapshot = self
            .cache
            .snapshot(node)
            .ok_or_else(|| Status::not_found(format!("no snapshot for node {node:?}")))?;
        if snapshot.version() == req.version_info {
            return Err(Status::unavailable("already at the latest version"));
        }
        response(&snapshot, type_url, &req.resource_names, self.next_nonce())
            .ok_or_else(|| Status::not_found("a requested resource is missing"))
    }
}

fn no_delta<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented(
        "incremental xDS is not served; use the state-of-the-world protocol",
    ))
}

#[async_trait]
impl AggregatedDiscoveryService for DiscoveryServer {
    type StreamAggregatedResourcesStream = Out<DiscoveryResponse>;
    type DeltaAggregatedResourcesStream = Out<DeltaDiscoveryResponse>;

    async fn stream_aggregated_resources(
        &self,
        request: Request<Streaming<DiscoveryRequest>>,
    ) -> Result<Response<Self::StreamAggregatedResourcesStream>, Status> {
        Ok(Response::new(self.open(request.into_inner(), None)))
    }

    async fn delta_aggregated_resources(
        &self,
        _: Request<Streaming<DeltaDiscoveryRequest>>,
    ) -> Result<Response<Self::DeltaAggregatedResourcesStream>, Status> {
        no_delta()
    }
}

/// Implements one per-type discovery service over [`DiscoveryServer`].
macro_rules! per_type_service {
    ($trait:ident, $type_url:expr, $stream_ty:ident, $stream:ident, $delta_ty:ident, $delta:ident, $fetch:ident) => {
        #[async_trait]
        impl $trait for DiscoveryServer {
            type $stream_ty = Out<DiscoveryResponse>;
            type $delta_ty = Out<DeltaDiscoveryResponse>;

            async fn $stream(
                &self,
                request: Request<Streaming<DiscoveryRequest>>,
            ) -> Result<Response<Self::$stream_ty>, Status> {
                Ok(Response::new(
                    self.open(request.into_inner(), Some($type_url)),
                ))
            }

            async fn $delta(
                &self,
                _: Request<Streaming<DeltaDiscoveryRequest>>,
            ) -> Result<Response<Self::$delta_ty>, Status> {
                no_delta()
            }

            async fn $fetch(
                &self,
                request: Request<DiscoveryRequest>,
            ) -> Result<Response<DiscoveryResponse>, Status> {
                self.fetch(request.into_inner(), $type_url)
                    .map(Response::new)
            }
        }
    };
}

per_type_service!(
    ClusterDiscoveryService,
    CLUSTER_TYPE,
    StreamClustersStream,
    stream_clusters,
    DeltaClustersStream,
    delta_clusters,
    fetch_clusters
);
per_type_service!(
    EndpointDiscoveryService,
    ENDPOINT_TYPE,
    StreamEndpointsStream,
    stream_endpoints,
    DeltaEndpointsStream,
    delta_endpoints,
    fetch_endpoints
);
per_type_service!(
    ListenerDiscoveryService,
    LISTENER_TYPE,
    StreamListenersStream,
    stream_listeners,
    DeltaListenersStream,
    delta_listeners,
    fetch_listeners
);
per_type_service!(
    RouteDiscoveryService,
    ROUTE_TYPE,
    StreamRoutesStream,
    stream_routes,
    DeltaRoutesStream,
    delta_routes,
    fetch_routes
);
per_type_service!(
    SecretDiscoveryService,
    SECRET_TYPE,
    StreamSecretsStream,
    stream_secrets,
    DeltaSecretsStream,
    delta_secrets,
    fetch_secrets
);

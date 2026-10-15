//! Versioned resource snapshots and the cache streams watch for them.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use envoy_types::pb::envoy::config::cluster::v3::{
    cluster::ClusterDiscoveryType, cluster::DiscoveryType, Cluster,
};
use envoy_types::pb::envoy::config::listener::v3::{filter, Listener};
use envoy_types::pb::envoy::config::route::v3::RouteConfiguration;
use envoy_types::pb::envoy::extensions::filters::network::http_connection_manager::v3::{
    http_connection_manager::RouteSpecifier, HttpConnectionManager,
};
use envoy_types::pb::envoy::extensions::transport_sockets::tls::v3::Secret;
use envoy_types::pb::google::protobuf::Any;
use prost::{Message, Name};
use tokio::sync::watch;

use super::consts::HCM_FILTER_NAME;
use super::pb;

/// Type URL of clusters (CDS).
pub const CLUSTER_TYPE: &str = "type.googleapis.com/envoy.config.cluster.v3.Cluster";
/// Type URL of endpoint assignments (EDS).
pub const ENDPOINT_TYPE: &str =
    "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";
/// Type URL of listeners (LDS).
pub const LISTENER_TYPE: &str = "type.googleapis.com/envoy.config.listener.v3.Listener";
/// Type URL of route configurations (RDS).
pub const ROUTE_TYPE: &str = "type.googleapis.com/envoy.config.route.v3.RouteConfiguration";
/// Type URL of secrets (SDS).
pub const SECRET_TYPE: &str =
    "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret";

/// Every type a snapshot can hold.
pub const RESOURCE_TYPES: [&str; 5] = [
    CLUSTER_TYPE,
    ENDPOINT_TYPE,
    LISTENER_TYPE,
    ROUTE_TYPE,
    SECRET_TYPE,
];

/// An inconsistent snapshot.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SnapshotError(String);

/// One version of every resource the router serves, packed and keyed by
/// type URL and resource name.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    version: String,
    resources: BTreeMap<&'static str, BTreeMap<String, Any>>,
}

fn named<T: Message + Name>(
    items: Vec<T>,
    name: impl Fn(&T) -> String,
) -> Result<BTreeMap<String, Any>, SnapshotError> {
    let mut out = BTreeMap::new();
    for item in items {
        let n = name(&item);
        if out.insert(n.clone(), pb::any(item)).is_some() {
            return Err(SnapshotError(format!(
                "duplicate {} resource {n:?}",
                T::full_name()
            )));
        }
    }
    Ok(out)
}

impl Snapshot {
    /// A snapshot at `version` holding the given resources.
    pub fn new(
        version: impl Into<String>,
        clusters: Vec<Cluster>,
        routes: Vec<RouteConfiguration>,
        listeners: Vec<Listener>,
        secrets: Vec<Secret>,
    ) -> Result<Snapshot, SnapshotError> {
        let mut resources = BTreeMap::new();
        resources.insert(CLUSTER_TYPE, named(clusters, |c| c.name.clone())?);
        resources.insert(ENDPOINT_TYPE, BTreeMap::new());
        resources.insert(LISTENER_TYPE, named(listeners, |l| l.name.clone())?);
        resources.insert(ROUTE_TYPE, named(routes, |r| r.name.clone())?);
        resources.insert(SECRET_TYPE, named(secrets, |s| s.name.clone())?);
        Ok(Snapshot {
            version: version.into(),
            resources,
        })
    }

    /// The version every resource type of this snapshot is served at.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// The packed resources of `type_url`, by name.
    pub fn resources(&self, type_url: &str) -> Option<&BTreeMap<String, Any>> {
        self.resources.get(type_url)
    }

    /// The resources of `T`'s type, decoded, by name.
    pub fn decoded<T: Message + Name + Default>(&self) -> BTreeMap<String, T> {
        let type_url = T::type_url();
        self.resources(&type_url)
            .map(|m| {
                m.iter()
                    .filter_map(|(name, any)| pb::unpack::<T>(any).ok().map(|v| (name.clone(), v)))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Checks that the routes listeners fetch over RDS are exactly the
    /// routes held, and that EDS clusters have their endpoints.
    pub fn consistent(&self) -> Result<(), SnapshotError> {
        let mut referenced = BTreeSet::new();
        for (name, l) in self.decoded::<Listener>() {
            for chain in &l.filter_chains {
                for f in &chain.filters {
                    let Some(filter::ConfigType::TypedConfig(any)) = &f.config_type else {
                        continue;
                    };
                    if f.name != HCM_FILTER_NAME
                        && any.type_url != HttpConnectionManager::type_url()
                    {
                        continue;
                    }
                    let hcm = pb::unpack::<HttpConnectionManager>(any)
                        .map_err(|e| SnapshotError(format!("listener {name:?}: {e}")))?;
                    if let Some(RouteSpecifier::Rds(rds)) = hcm.route_specifier {
                        referenced.insert(rds.route_config_name);
                    }
                }
            }
        }
        let routes: BTreeSet<String> = self
            .resources(ROUTE_TYPE)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        if referenced != routes {
            return Err(SnapshotError(format!("mismatched route references: listeners use {referenced:?}, snapshot holds {routes:?}")));
        }
        let endpoints = self.resources(ENDPOINT_TYPE).cloned().unwrap_or_default();
        for (name, c) in self.decoded::<Cluster>() {
            if c.cluster_discovery_type
                != Some(ClusterDiscoveryType::Type(DiscoveryType::Eds as i32))
            {
                continue;
            }
            let wanted = c
                .eds_cluster_config
                .as_ref()
                .map(|e| e.service_name.clone())
                .filter(|s| !s.is_empty())
                .unwrap_or(name);
            if !endpoints.contains_key(&wanted) {
                return Err(SnapshotError(format!(
                    "cluster endpoints {wanted:?} are missing"
                )));
            }
        }
        Ok(())
    }
}

/// The node a snapshot is published to, and the snapshot.
pub(crate) type Published = Option<(String, Arc<Snapshot>)>;

/// The current snapshot per node; discovery streams watch it for changes.
#[derive(Debug, Clone)]
pub struct SnapshotCache {
    tx: Arc<watch::Sender<Published>>,
}

impl Default for SnapshotCache {
    fn default() -> Self {
        SnapshotCache {
            tx: Arc::new(watch::channel(None).0),
        }
    }
}

impl SnapshotCache {
    /// Publishes `snapshot` to `node_id`, replacing what it was served.
    pub fn set_snapshot(&self, node_id: &str, snapshot: Snapshot) {
        self.tx
            .send_replace(Some((node_id.to_string(), Arc::new(snapshot))));
    }

    /// The snapshot published to `node_id`, if any.
    pub fn snapshot(&self, node_id: &str) -> Option<Arc<Snapshot>> {
        self.tx
            .borrow()
            .as_ref()
            .filter(|(n, _)| n == node_id)
            .map(|(_, s)| s.clone())
    }

    /// A receiver seeing every published snapshot.
    pub(crate) fn subscribe(&self) -> watch::Receiver<Published> {
        self.tx.subscribe()
    }
}

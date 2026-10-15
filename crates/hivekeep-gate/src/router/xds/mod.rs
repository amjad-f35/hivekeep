//! Envoy's control plane: builds the listeners, clusters, routes and
//! secrets of the router's Envoy and serves them over ADS.

pub mod ads;
mod clusters;
pub mod consts;
mod filters;
mod hcm;
mod listeners;
pub mod otlp;
mod pb;
mod routes;
mod secrets;
pub mod snapshot;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

pub use ads::DiscoveryServer;
pub use consts::*;
pub use otlp::{normalize_otlp_collector, OtlpAddressError};
pub use snapshot::{Snapshot, SnapshotCache, SnapshotError};

/// Why a snapshot could not be published.
#[derive(Debug, thiserror::Error)]
pub enum XdsError {
    /// TLS listeners were enabled without a client CA.
    #[error("TLS ingress listeners need a client CA: call set_downstream_client_auth")]
    MissingClientCa,
    /// The built resources do not form a snapshot.
    #[error("failed to build xDS snapshot: {0}")]
    Build(SnapshotError),
    /// The snapshot is not self-consistent.
    #[error("snapshot evaluation failed integrity check: {0}")]
    Inconsistent(SnapshotError),
}

/// What the resources are built from; set through [`XdsServer`]'s setters.
#[derive(Debug, Clone, PartialEq)]
pub struct XdsSettings {
    pub(crate) extproc_port: u16,
    pub(crate) extproc_addr: String,
    pub(crate) https_port: u16,
    pub(crate) connect_tls_port: u16,
    pub(crate) cert_path: String,
    pub(crate) upstream_credential_bundle_path: String,
    pub(crate) upstream_trust_bundle_path: String,
    pub(crate) upstream_connect: bool,
    pub(crate) downstream_client_ca_path: String,
    pub(crate) downstream_allowed_spiffe_ids: Vec<String>,
    pub(crate) otlp_host: String,
    pub(crate) otlp_port: u32,
    pub(crate) trace_root_sampling_percent: f64,
    pub(crate) ext_proc_message_timeout: Duration,
    pub(crate) ext_proc_max_requests: u32,
    pub(crate) route_timeout: Duration,
}

impl Default for XdsSettings {
    fn default() -> Self {
        XdsSettings {
            extproc_port: 50051,
            extproc_addr: "127.0.0.1".to_string(),
            https_port: 0,
            connect_tls_port: 0,
            cert_path: String::new(),
            upstream_credential_bundle_path: String::new(),
            upstream_trust_bundle_path: String::new(),
            upstream_connect: false,
            downstream_client_ca_path: String::new(),
            downstream_allowed_spiffe_ids: Vec::new(),
            otlp_host: String::new(),
            otlp_port: 0,
            trace_root_sampling_percent: 0.0,
            ext_proc_message_timeout: DEFAULT_EXT_PROC_MESSAGE_TIMEOUT,
            ext_proc_max_requests: DEFAULT_EXT_PROC_MAX_REQUESTS,
            route_timeout: DEFAULT_ROUTE_TIMEOUT,
        }
    }
}

impl XdsSettings {
    /// Whether workers' certificates are verified, which lets the router pin
    /// the worker identity it expects.
    pub(crate) fn upstream_validated(&self) -> bool {
        !self.upstream_credential_bundle_path.is_empty()
            && !self.upstream_trust_bundle_path.is_empty()
    }

    /// Which TLS listeners to build, as (HTTPS, CONNECT). Each needs its
    /// port, the serving certificate and at least one allowed client.
    fn tls_listeners(&self) -> Result<(bool, bool), XdsError> {
        let clients_allowed = !self.downstream_allowed_spiffe_ids.is_empty();
        let https_enabled = self.https_port > 0 && !self.cert_path.is_empty() && clients_allowed;
        let connect_enabled =
            self.connect_tls_port > 0 && !self.cert_path.is_empty() && clients_allowed;
        if (https_enabled || connect_enabled) && self.downstream_client_ca_path.is_empty() {
            return Err(XdsError::MissingClientCa);
        }
        Ok((https_enabled, connect_enabled))
    }

    /// The snapshot these settings describe, at `version`.
    pub fn build_snapshot(&self, version: &str) -> Result<Snapshot, XdsError> {
        let (https_enabled, connect_enabled) = self.tls_listeners()?;

        let mut clusters = vec![self.build_cluster()];
        if self.upstream_connect {
            clusters.push(clusters::build_actor_tunnel_cluster(
                ACTOR_TUNNEL_HTTP1_CLUSTER_NAME,
                false,
            ));
            clusters.push(clusters::build_actor_tunnel_cluster(
                ACTOR_TUNNEL_HTTP2_CLUSTER_NAME,
                true,
            ));
            clusters.push(self.build_actor_connect_cluster());
        } else {
            clusters.push(self.build_original_dst_cluster());
        }
        if connect_enabled {
            clusters.push(self.build_main_internal_cluster());
        }
        if !self.otlp_host.is_empty() {
            clusters.push(self.build_otlp_collector_cluster());
        }

        let mut routes = Vec::new();
        if https_enabled || connect_enabled {
            routes.push(self.build_routes());
        }

        let mut listeners = Vec::new();
        if self.upstream_connect {
            listeners.push(listeners::build_actor_connect_listener());
        }
        if connect_enabled {
            listeners.push(self.build_main_internal_listener());
        }
        if https_enabled {
            listeners.push(self.build_https_listener());
        }
        if connect_enabled {
            listeners.push(self.build_connect_terminate_tls_listener());
        }

        let mut secrets = Vec::new();
        if https_enabled || connect_enabled {
            secrets.push(self.build_tls_secret());
            secrets.push(self.build_ingress_client_trust_secret());
        }
        if !self.upstream_credential_bundle_path.is_empty() {
            secrets.push(self.build_upstream_cert_secret());
            if !self.upstream_trust_bundle_path.is_empty() {
                secrets.push(self.build_upstream_trust_secret());
            }
        }

        let snapshot = Snapshot::new(version, clusters, routes, listeners, secrets)
            .map_err(XdsError::Build)?;
        snapshot.consistent().map_err(XdsError::Inconsistent)?;
        Ok(snapshot)
    }
}

#[derive(Debug)]
struct State {
    settings: XdsSettings,
    version_count: u64,
}

/// The xDS server of the router's Envoy.
///
/// Configure it with the setters, then call [`XdsServer::update_snapshot`]
/// to publish; [`XdsServer::serve`] answers Envoy's discovery streams.
#[derive(Debug)]
pub struct XdsServer {
    xds_port: u16,
    state: Mutex<State>,
    cache: SnapshotCache,
    version_epoch: String,
}

impl XdsServer {
    /// A server that will be reached on `xds_port`.
    ///
    /// Snapshot versions start with a per-process epoch, so a restarted
    /// router never reuses a version Envoy already acknowledged.
    pub fn new(xds_port: u16) -> Arc<XdsServer> {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        Arc::new(XdsServer {
            xds_port,
            state: Mutex::new(State {
                settings: XdsSettings::default(),
                version_count: 0,
            }),
            cache: SnapshotCache::default(),
            version_epoch: format!("{secs}-{}", &nonce[..8]),
        })
    }

    /// The port Envoy reaches this server on.
    pub fn xds_port(&self) -> u16 {
        self.xds_port
    }

    /// The cache the discovery services serve from.
    pub fn cache(&self) -> &SnapshotCache {
        &self.cache
    }

    /// A copy of the current settings.
    pub fn settings(&self) -> XdsSettings {
        self.state.lock().settings.clone()
    }

    fn update(&self, f: impl FnOnce(&mut XdsSettings)) {
        f(&mut self.state.lock().settings);
    }

    /// Where Envoy dials the ext_proc server.
    pub fn set_config(&self, extproc_port: u16, extproc_addr: &str) {
        self.update(|s| {
            s.extproc_port = extproc_port;
            s.extproc_addr = extproc_addr.to_string();
        });
    }

    /// The port of the CONNECT-terminating TLS listener; 0 disables it.
    pub fn set_connect_tls_port(&self, port: u16) {
        self.update(|s| s.connect_tls_port = port);
    }

    /// The per-message ext_proc timeout; zero keeps the current one.
    pub fn set_ext_proc_message_timeout(&self, d: Duration) {
        if !d.is_zero() {
            self.update(|s| s.ext_proc_message_timeout = d);
        }
    }

    /// The ext_proc cluster's `max_requests` circuit breaker; zero keeps the
    /// current one.
    pub fn set_ext_proc_max_requests(&self, n: usize) {
        if n > 0 {
            self.update(|s| s.ext_proc_max_requests = u32::try_from(n).unwrap_or(u32::MAX));
        }
    }

    /// The workload route's end-to-end timeout; zero keeps the current one.
    pub fn set_route_timeout(&self, d: Duration) {
        if !d.is_zero() {
            self.update(|s| s.route_timeout = d);
        }
    }

    /// The HTTPS listener's port and serving certificate bundle.
    pub fn set_tls_config(&self, https_port: u16, cert_path: &str) {
        if https_port > 0 && cert_path.is_empty() {
            tracing::warn!(
                https_port,
                "An HTTPS port without a serving certificate: the HTTPS listener stays disabled"
            );
        }
        self.update(|s| {
            s.https_port = https_port;
            s.cert_path = cert_path.to_string();
        });
    }

    /// The CA bundle signing ingress client certificates, and the SPIFFE IDs
    /// allowed to connect. Without any ID no TLS listener is built.
    pub fn set_downstream_client_auth(&self, ca_path: &str, allowed_spiffe_ids: &[String]) {
        self.update(|s| {
            s.downstream_client_ca_path = ca_path.to_string();
            s.downstream_allowed_spiffe_ids = allowed_spiffe_ids.to_vec();
        });
    }

    /// The client certificate presented to workers and the trust bundle
    /// validating them; an empty credential path disables upstream mTLS.
    pub fn set_upstream_tls(&self, credential_bundle_path: &str, trust_bundle_path: &str) {
        self.update(|s| {
            s.upstream_credential_bundle_path = credential_bundle_path.to_string();
            s.upstream_trust_bundle_path = trust_bundle_path.to_string();
        });
    }

    /// Whether workers are reached through CONNECT tunnels.
    pub fn set_upstream_connect(&self, enabled: bool) {
        self.update(|s| s.upstream_connect = enabled);
    }

    /// Points Envoy's tracer at the collector `addr`; empty disables tracing.
    /// An unusable address is an error and leaves the setting unchanged.
    pub fn set_otlp_collector(&self, addr: &str) -> Result<(), OtlpAddressError> {
        if addr.is_empty() {
            self.disable_otlp_collector();
            return Ok(());
        }
        let (host, port) = normalize_otlp_collector(addr)?;
        self.update(|s| {
            s.otlp_host = host;
            s.otlp_port = port;
        });
        Ok(())
    }

    /// Like [`XdsServer::set_otlp_collector`], but an unusable address
    /// disables Envoy-side tracing with a warning instead of failing.
    pub fn set_otlp_collector_or_disable(&self, addr: &str) {
        if let Err(err) = self.set_otlp_collector(addr) {
            tracing::warn!(
                address = addr,
                %err,
                "Envoy-side tracing disabled: the OTLP collector address is not one Envoy can use. The router's own spans are unaffected; set --otlp-collector-address to point Envoy at a plaintext collector"
            );
            self.disable_otlp_collector();
        }
    }

    /// Turns Envoy-side tracing off.
    pub fn disable_otlp_collector(&self) {
        self.update(|s| {
            s.otlp_host.clear();
            s.otlp_port = 0;
        });
    }

    /// The percentage of root spans Envoy samples.
    pub fn set_trace_root_sampling_percent(&self, percent: f64) {
        self.update(|s| s.trace_root_sampling_percent = percent);
    }

    /// Builds the resources from the current settings and publishes them at a
    /// new version.
    pub fn update_snapshot(&self) -> Result<(), XdsError> {
        let mut state = self.state.lock();
        state.settings.tls_listeners()?;
        state.version_count += 1;
        let version = format!("{}-{}", self.version_epoch, state.version_count);
        let snapshot = state.settings.build_snapshot(&version)?;
        self.cache.set_snapshot(NODE_ID, snapshot);
        Ok(())
    }

    /// The snapshot currently published to Envoy.
    pub fn snapshot(&self) -> Option<Arc<Snapshot>> {
        self.cache.snapshot(NODE_ID)
    }

    /// Publishes the initial snapshot and serves ADS (and the per-type
    /// discovery services) on `listener` until `cancel` fires.
    pub async fn serve(
        &self,
        listener: TcpListener,
        cancel: CancellationToken,
    ) -> Result<(), tonic::transport::Error> {
        if let Err(err) = self.update_snapshot() {
            tracing::error!(%err, "Failed to publish the initial xDS snapshot");
        }
        let discovery = DiscoveryServer::new(self.cache.clone(), NODE_ID);
        tonic::transport::Server::builder()
            .add_service(discovery.ads_service())
            .add_service(discovery.cds_service())
            .add_service(discovery.eds_service())
            .add_service(discovery.lds_service())
            .add_service(discovery.rds_service())
            .add_service(discovery.sds_service())
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), cancel.cancelled())
            .await
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(test)]
#[path = "upstream_connect_tests.rs"]
pub(crate) mod upstream_connect_tests;

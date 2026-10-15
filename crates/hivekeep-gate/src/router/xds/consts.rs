//! Names and limits shared by the xDS resource builders.

use std::time::Duration;

/// Node ID the router's Envoy announces; the snapshot is served to it.
pub const NODE_ID: &str = "hivekeep-envoy-node";
/// The ingress HTTPS listener.
pub const INGRESS_HTTPS_LISTENER: &str = "ingress_https_listener";
/// The CONNECT-terminating TLS listener.
pub const CONNECT_TERMINATE_TLS_LISTENER: &str = "connect_terminate_tls";
/// The route configuration served over RDS.
pub const ROUTE_NAME: &str = "hivekeep_routes";
/// The ext_proc cluster.
pub const CLUSTER_NAME: &str = "hk-cluster";
/// The OTLP collector cluster Envoy reports spans to.
pub const OTLP_CLUSTER_NAME: &str = "otel_collector_cluster";
/// SDS secret holding the ingress serving certificate.
pub const HTTPS_CERT_SECRET_NAME: &str = "https_serving_cert";
/// SDS secret holding the client certificate presented to sandbox tunnels.
pub const UPSTREAM_CERT_SECRET_NAME: &str = "upstream_client_cert";
/// SDS secret holding the trust bundle for sandbox tunnel certificates.
pub const UPSTREAM_TRUST_SECRET_NAME: &str = "upstream_trust_bundle";
/// SDS secret holding the CAs allowed to sign ingress client certificates.
pub const INGRESS_CLIENT_TRUST_SECRET_NAME: &str = "ingress_client_trust_bundle";
/// Key of a cluster's HTTP protocol options.
pub const HTTP_PROTOCOL_OPTIONS_NAME: &str =
    "envoy.extensions.upstreams.http.v3.HttpProtocolOptions";
/// Cluster dialing the worker address picked by ext_proc.
pub const ORIGINAL_DST_CLUSTER_NAME: &str = "actor_original_dst";
/// Tunnel cluster for HTTP/1 requests.
pub const ACTOR_TUNNEL_HTTP1_CLUSTER_NAME: &str = "actor_tunnel_http1";
/// Tunnel cluster for HTTP/2 (gRPC) requests.
pub const ACTOR_TUNNEL_HTTP2_CLUSTER_NAME: &str = "actor_tunnel_http2";
/// The internal listener (and cluster) opening CONNECT tunnels to workers.
pub const ACTOR_CONNECT_NAME: &str = "actor_connect";
/// Address the TLS listeners bind; an IPv6 twin is added alongside.
pub const WILDCARD_IP: &str = "0.0.0.0";
/// Upgrade type of CONNECT requests.
pub const CONNECT_UPGRADE_TYPE: &str = "CONNECT";
/// Internal listener (and cluster) behind the CONNECT terminator.
pub const MAIN_INTERNAL_NAME: &str = "main_internal";
/// The HTTP ext_proc filter.
pub const HTTP_EXT_PROC_FILTER_NAME: &str = "envoy.filters.http.ext_proc";
/// The HTTP set_filter_state filter.
pub const HTTP_SET_FILTER_STATE_NAME: &str = "envoy.filters.http.set_filter_state";
/// The HTTP router filter.
pub const HTTP_ROUTER_FILTER_NAME: &str = "envoy.filters.http.router";
/// The HTTP connection manager network filter.
pub const HCM_FILTER_NAME: &str = "envoy.filters.network.http_connection_manager";
/// The TLS transport socket.
pub const TLS_TRANSPORT_SOCKET_NAME: &str = "envoy.transport_sockets.tls";

/// Default per-message ext_proc timeout.
pub const DEFAULT_EXT_PROC_MESSAGE_TIMEOUT: Duration = Duration::from_secs(5);
/// Default circuit-breaker `max_requests` on the ext_proc cluster.
pub const DEFAULT_EXT_PROC_MAX_REQUESTS: u32 = 2048;
/// Default end-to-end timeout of the workload route.
pub const DEFAULT_ROUTE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Time a drain leaves for in-flight routed requests beyond parking.
pub const DRAIN_ROUTE_BUDGET: Duration = Duration::from_secs(10);
/// Envoy's default stream idle timeout.
pub const ENVOY_DEFAULT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// How far the route idle timeout stays beyond the route timeout.
pub const ROUTE_IDLE_TIMEOUT_MARGIN: Duration = Duration::from_secs(30);
/// Circuit-breaker limits of the tunnel clusters: effectively unlimited, since
/// each request rides its own stream.
pub const TUNNEL_CIRCUIT_BREAKER_LIMIT: u32 = 1 << 20;
/// Circuit-breaker limits of the cluster dialing workers' CONNECT port.
pub const ACTOR_CONNECT_CIRCUIT_BREAKER_LIMIT: u32 = 1 << 20;
/// Idle timeout of pooled tunnel connections.
pub const TUNNEL_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// OTLP gRPC port used when the collector address names none.
pub const OTLP_DEFAULT_PORT: u32 = 4317;

/// Filter state keying the tunnel pool by worker, actor and port.
pub const TUNNEL_KEY_FILTER_STATE_KEY: &str = "io.hivekeep.tunnel.key";
/// Filter state carrying the worker address of the tunnel.
pub const TUNNEL_WORKER_FILTER_STATE_KEY: &str = "io.hivekeep.tunnel.worker";
/// Filter state carrying the target actor of the tunnel.
pub const TUNNEL_ACTOR_FILTER_STATE_KEY: &str = "io.hivekeep.tunnel.actor";
/// Filter state carrying the actor port of the tunnel.
pub const TUNNEL_PORT_FILTER_STATE_KEY: &str = "io.hivekeep.tunnel.port";
/// Filter state keying the CONNECT terminator's upstream pool.
pub const CONNECT_POOL_FILTER_STATE_KEY: &str = "io.hivekeep.connect.pool";
/// Envoy's filter state overriding the upstream SANs to verify.
pub const UPSTREAM_SANS_FILTER_STATE_KEY: &str = "envoy.network.upstream_subject_alt_names";
/// URI SAN prefix of every workload identity in the trust domain.
pub const TRUST_DOMAIN_SPIFFE_PREFIX: &str = "spiffe://cluster.local/";
/// Service name Envoy's spans are reported under.
pub const TRACING_SERVICE_NAME: &str = "gate-router-envoy";

/// Access log line of HTTP requests; the path is logged without its query.
pub const ACCESS_LOG_FORMAT: &str = "[%START_TIME%] \"%REQ(:METHOD)% %PATH(NQ:ORIG_OR_PATH)% %PROTOCOL%\" %RESPONSE_CODE% %RESPONSE_FLAGS% %BYTES_RECEIVED% %BYTES_SENT% %DURATION% %RESP(X-ENVOY-UPSTREAM-SERVICE-TIME)% \"%REQ(X-FORWARDED-FOR)%\" \"%REQ(USER-AGENT)%\" \"%REQ(X-REQUEST-ID)%\" \"%REQ(:AUTHORITY)%\" \"%UPSTREAM_HOST%\"\n";

/// Access log line of a CONNECT tunnel to a worker.
pub fn tunnel_access_log_format() -> String {
    format!(
        "[%START_TIME%] tunnel actor=%FILTER_STATE({TUNNEL_ACTOR_FILTER_STATE_KEY}:PLAIN)% port=%FILTER_STATE({TUNNEL_PORT_FILTER_STATE_KEY}:PLAIN)% \"%UPSTREAM_HOST%\" %RESPONSE_FLAGS% connect_response=%FILTER_STATE(envoy.tcp_proxy.propagate_response_headers:TYPED)% %BYTES_RECEIVED% %BYTES_SENT% %DURATION%\n"
    )
}

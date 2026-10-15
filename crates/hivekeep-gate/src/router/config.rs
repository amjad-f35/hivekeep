//! The router's configuration: the flags, the settings they produce, and the
//! cross-flag validation run before anything starts.

use std::fmt;
use std::time::Duration;

use crate::router::dataplane::Dataplane;
use crate::router::ingress::{
    ParkedRequestConfig, DEFAULT_PARKED_REQUEST_MAX, DEFAULT_PARKED_REQUEST_RETRY_FACTOR,
    DEFAULT_PARKED_REQUEST_RETRY_JITTER,
};
use crate::router::xds::DRAIN_ROUTE_BUDGET;

/// The traffic direction one router instance serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Ingress: ext_proc for the ingress listeners, plus Envoy's xDS server.
    Ingress,
    /// Egress: ext_proc only.
    Egress,
}

impl Mode {
    /// Parses a `--mode` value.
    pub fn parse(s: &str) -> Option<Mode> {
        match s {
            "ingress" => Some(Mode::Ingress),
            "egress" => Some(Mode::Egress),
            _ => None,
        }
    }

    /// The flag spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Ingress => "ingress",
            Mode::Egress => "egress",
        }
    }

    /// Whether this mode serves ingress.
    pub fn serves_ingress(self) -> bool {
        self == Mode::Ingress
    }

    /// Whether this mode serves egress.
    pub fn serves_egress(self) -> bool {
        self == Mode::Egress
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Floor of the derived ext_proc circuit breaker: Envoy's own default.
pub const EXT_PROC_MAX_REQUESTS_FLOOR: usize = 1024;
/// Slack the derived drain timeout leaves beyond parking and routing.
pub const DRAIN_TIMEOUT_MARGIN: Duration = Duration::from_secs(5);
/// Marker file the drain creates for the dataplane's preStop hook.
pub use crate::router::drain::DEFAULT_DRAIN_COMPLETE_FILE;
/// Service name of the router in its namespace.
pub const DEFAULT_ROUTER_SERVICE_NAME: &str = crate::install::ROUTER_SERVICE_NAME;
/// Dial target of the control plane.
pub const DEFAULT_HIVEKEEP_API_ADDRESS: &str = "k8s:///hivekeep-api.hivekeep-system.svc:443";
/// Client certificate presented to workers.
pub const DEFAULT_UPSTREAM_CREDENTIAL_BUNDLE: &str =
    "/run/podidentity.podcert.hivekeep.io/credential-bundle.pem";
/// Trust bundle validating workers.
pub const DEFAULT_UPSTREAM_TRUST_BUNDLE: &str =
    "/run/podidentity.podcert.hivekeep.io/trust-bundle.pem";

/// A configuration the router refuses to start with.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(String);

macro_rules! bail {
    ($($arg:tt)*) => { return Err(ConfigError(format!($($arg)*))) };
}

/// Ingress client authentication.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngressAuthConfig {
    /// PEM bundle of the CAs signing ingress client certificates.
    pub client_ca_file: String,
    /// SPIFFE IDs allowed through the ingress dataplane.
    pub allowed_spiffe_ids: Vec<String>,
}

/// How the router authenticates to the control plane.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApiAuthConfig {
    /// CAs trusted to verify the control plane's certificate.
    pub ca_file: String,
    /// Credential bundle presented as the client certificate.
    pub client_cert_path: String,
    /// SNI expected on the control plane's certificate.
    pub server_name: String,
}

/// The credential provider egress injects secrets from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CredentialProviderConfig {
    /// Provider name, the host of its secret URIs.
    pub name: String,
    /// gRPC dial target; empty disables injection.
    pub address: String,
    /// CA its serving certificate chains to.
    pub ca_file: String,
    /// Credential bundle presented as the client certificate.
    pub client_cert: String,
    /// SNI expected on its serving certificate.
    pub server_name: String,
    /// Dial without TLS (development only).
    pub insecure: bool,
}

/// Everything the router runs with.
#[derive(Debug, Clone, PartialEq)]
pub struct RouterConfig {
    /// `--mode`, as given; see [`RouterConfig::mode`].
    pub mode: String,
    /// `--gate-dataplane`, as given; see [`RouterConfig::dataplane`].
    pub dataplane: String,
    pub namespace: String,
    pub router_service_name: String,
    pub kubeconfig: String,
    pub hivekeep_api_addr: String,
    pub xds_port: u16,
    pub extproc_port: u16,
    pub extproc_addr: String,
    /// `<= 0` disables the status page.
    pub status_port: i32,
    pub status_addr: String,
    pub health_interval: Duration,
    pub https_port: u16,
    pub connect_tls_port: u16,
    pub envoy_cert_path: String,
    pub upstream_credential_bundle_path: String,
    pub upstream_trust_bundle_path: String,
    pub upstream_connect: bool,
    pub actor_identity_ca_file: String,
    pub egress_policy_cache_ttl: Duration,
    pub credential_provider: CredentialProviderConfig,
    pub log_level: String,
    pub metrics_addr: String,
    pub health_addr: String,
    pub otlp_collector_address: String,
    pub auth: ApiAuthConfig,
    pub ingress_auth: IngressAuthConfig,
    pub route_timeout: Duration,
    pub parked_request: ParkedRequestConfig,
    /// 0 derives it; see [`RouterConfig::ext_proc_max_requests`].
    pub ext_proc_max_requests: i64,
    pub drain_delay: Duration,
    /// Zero derives it; see [`RouterConfig::drain_timeout`].
    pub drain_timeout: Duration,
    pub envoy_admin_addr: String,
    pub drain_complete_file: String,
}

impl Default for RouterConfig {
    /// Every field unset, as an empty flag set would leave it.
    fn default() -> Self {
        RouterConfig {
            mode: String::new(),
            dataplane: String::new(),
            namespace: String::new(),
            router_service_name: String::new(),
            kubeconfig: String::new(),
            hivekeep_api_addr: String::new(),
            xds_port: 0,
            extproc_port: 0,
            extproc_addr: String::new(),
            status_port: 0,
            status_addr: String::new(),
            health_interval: Duration::ZERO,
            https_port: 0,
            connect_tls_port: 0,
            envoy_cert_path: String::new(),
            upstream_credential_bundle_path: String::new(),
            upstream_trust_bundle_path: String::new(),
            upstream_connect: false,
            actor_identity_ca_file: String::new(),
            egress_policy_cache_ttl: Duration::ZERO,
            credential_provider: CredentialProviderConfig::default(),
            log_level: String::new(),
            metrics_addr: String::new(),
            health_addr: String::new(),
            otlp_collector_address: String::new(),
            auth: ApiAuthConfig::default(),
            ingress_auth: IngressAuthConfig::default(),
            route_timeout: Duration::ZERO,
            parked_request: ParkedRequestConfig::disabled(),
            ext_proc_max_requests: 0,
            drain_delay: Duration::ZERO,
            drain_timeout: Duration::ZERO,
            envoy_admin_addr: String::new(),
            drain_complete_file: String::new(),
        }
    }
}

impl RouterConfig {
    /// The mode, if `--mode` names one.
    pub fn mode(&self) -> Option<Mode> {
        Mode::parse(&self.mode)
    }

    /// Whether this instance serves ingress.
    pub fn serves_ingress(&self) -> bool {
        self.mode().is_some_and(Mode::serves_ingress)
    }

    /// Whether this instance serves egress.
    pub fn serves_egress(&self) -> bool {
        self.mode().is_some_and(Mode::serves_egress)
    }

    /// The dataplane; empty means Envoy.
    pub fn dataplane(&self) -> Result<Dataplane, String> {
        Dataplane::parse(&self.dataplane)
    }

    fn is_envoy(&self) -> bool {
        self.dataplane() == Ok(Dataplane::Envoy)
    }

    /// The ext_proc circuit breaker: the explicit value, or twice the parking
    /// lot (so parked requests leave headroom for the fast path), floored at
    /// Envoy's default.
    pub fn ext_proc_max_requests(&self) -> usize {
        if self.ext_proc_max_requests > 0 {
            return self.ext_proc_max_requests as usize;
        }
        (2 * self.parked_request.max).max(EXT_PROC_MAX_REQUESTS_FLOOR)
    }

    /// The ext_proc drain deadline: the explicit value, or long enough for a
    /// parked request to use its whole budget and still be routed.
    pub fn drain_timeout(&self, park: &ParkedRequestConfig) -> Duration {
        if !self.drain_timeout.is_zero() {
            return self.drain_timeout;
        }
        park.budget + DRAIN_ROUTE_BUDGET + DRAIN_TIMEOUT_MARGIN
    }

    /// Checks the flags against each other.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.dataplane().map_err(ConfigError)?;
        if self.mode().is_none() {
            bail!(
                "--mode must be \"ingress\" or \"egress\", got {:?}",
                self.mode
            );
        }
        self.parked_request.validate().map_err(ConfigError)?;
        self.validate_ingress_auth()?;
        if self.ext_proc_max_requests < 0 {
            bail!(
                "--extproc-max-requests must not be negative, got {} (0 derives it from --parked-request-max)",
                self.ext_proc_max_requests
            );
        }
        let max = self.parked_request.max;
        if self.ext_proc_max_requests > 0 && max > 0 && (self.ext_proc_max_requests as usize) < max
        {
            bail!(
                "--extproc-max-requests ({}) must be >= --parked-request-max ({max}): a circuit breaker below the parking lot silently truncates it with Envoy-generated 503s",
                self.ext_proc_max_requests
            );
        }
        if self.upstream_connect
            && self.upstream_credential_bundle_path.is_empty()
            && self.serves_ingress()
            && self.is_envoy()
        {
            bail!("--upstream-connect (on by default) requires --upstream-credential-bundle: the sandbox tunnel's CONNECT listener accepts only mTLS. Set --upstream-connect=false to dial the tunnel without it");
        }
        let budget = self.parked_request.normalized().budget;
        if !self.drain_timeout.is_zero()
            && self.parked_request.enabled()
            && self.drain_timeout < budget
        {
            bail!(
                "--drain-timeout ({}) must be >= --parked-request-budget ({}): a drain shorter than the parking budget resets parked requests on shutdown instead of letting them finish",
                humantime::format_duration(self.drain_timeout),
                humantime::format_duration(budget)
            );
        }
        Ok(())
    }

    fn validate_ingress_auth(&self) -> Result<(), ConfigError> {
        if !self.serves_ingress() || !self.is_envoy() {
            return Ok(());
        }
        if self.envoy_cert_path.is_empty() {
            bail!("--envoy-cert-path is required: without a serving certificate there is no TLS listener");
        }
        if self.https_port == 0 && self.connect_tls_port == 0 {
            bail!("--port-https or --port-connect-tls is required: there is no TLS listener");
        }
        if self.ingress_auth.client_ca_file.is_empty() {
            bail!("--ingress-client-ca-file is required");
        }
        for id in &self.ingress_auth.allowed_spiffe_ids {
            if let Err(e) = hivekeep_core::spiffe::validate_spiffe_id(id) {
                bail!("--ingress-allowed-spiffe-ids: {id:?} is not a valid SPIFFE ID: {e}");
            }
        }
        Ok(())
    }
}

fn duration(s: &str) -> Result<Duration, humantime::DurationError> {
    humantime::parse_duration(s)
}

/// The router's flags.
#[derive(Debug, Clone, clap::Args)]
pub struct RouterArgs {
    /// Required. Traffic direction this instance serves: "ingress" (also runs
    /// the xDS server for an Envoy dataplane) or "egress" (ext_proc only, no
    /// Kubernetes access). The ext_proc mux refuses the other direction.
    #[arg(long = "mode", default_value = "")]
    pub mode: String,
    /// Address the Prometheus metrics server listens on.
    #[arg(long = "metrics-listen-addr", default_value = ":9090")]
    pub metrics_listen_addr: String,
    /// Address the /readyz and /healthz probe server listens on.
    #[arg(long = "health-listen-addr", default_value = ":9091")]
    pub health_listen_addr: String,
    /// Ingress and egress dataplane: envoy.
    #[arg(long = "gate-dataplane", default_value = "envoy")]
    pub gate_dataplane: String,
    /// Target operations namespace.
    #[arg(long = "namespace", default_value = "default")]
    pub namespace: String,
    /// Service name of this router in the operations namespace.
    #[arg(long = "router-service-name", default_value = DEFAULT_ROUTER_SERVICE_NAME)]
    pub router_service_name: String,
    /// Absolute path to a kubeconfig file.
    #[arg(long = "kubeconfig", default_value = "")]
    pub kubeconfig: String,
    /// gRPC dial target of the control plane.
    #[arg(long = "hivekeep-api-address", alias = "api-address", default_value = DEFAULT_HIVEKEEP_API_ADDRESS)]
    pub hivekeep_api_address: String,
    /// TCP port for CONNECT-tunneled traffic entering over TLS. It differs
    /// from --port-https since both listeners commonly run at once.
    #[arg(long = "port-connect-tls", default_value_t = 8444)]
    pub port_connect_tls: u16,
    /// TCP port of the xDS server Envoy connects to.
    #[arg(long = "port-xds", default_value_t = 18000)]
    pub port_xds: u16,
    /// Port of the ext_proc server the dataplane calls.
    #[arg(long = "port-extproc", default_value_t = 50051)]
    pub port_extproc: u16,
    /// Address the ext_proc server binds and the co-located dataplane dials.
    /// Loopback keeps it unreachable from other pods; empty binds every
    /// interface.
    #[arg(long = "extproc-address", default_value = "127.0.0.1")]
    pub extproc_address: String,
    /// Port of the /statusz page (<= 0 disables it).
    #[arg(
        long = "status-port",
        default_value_t = 4040,
        allow_negative_numbers = true
    )]
    pub status_port: i32,
    /// Address the unauthenticated /statusz page binds; empty binds every
    /// interface.
    #[arg(long = "status-address", default_value = "127.0.0.1")]
    pub status_address: String,
    /// Interval of dependency health checks.
    #[arg(long = "health-interval", default_value = "1s", value_parser = duration)]
    pub health_interval: Duration,
    /// TCP port for HTTPS workload traffic.
    #[arg(long = "port-https", default_value_t = 8443)]
    pub port_https: u16,
    /// Path to Envoy's serving certificate bundle.
    #[arg(long = "envoy-cert-path", default_value = "")]
    pub envoy_cert_path: String,
    /// PEM bundle of the CAs signing ingress client certificates; Envoy reads
    /// it, so it must be mounted in the Envoy container too.
    #[arg(long = "ingress-client-ca-file", default_value = "")]
    pub ingress_client_ca_file: String,
    /// SPIFFE IDs (exact match) allowed through the ingress dataplane;
    /// comma-separated or repeated. Without any, no traffic is served.
    #[arg(long = "ingress-allowed-spiffe-ids", value_delimiter = ',')]
    pub ingress_allowed_spiffe_ids: Vec<String>,
    /// Credential bundle presented to sandbox tunnels; empty disables
    /// upstream mTLS and needs --upstream-connect=false.
    #[arg(long = "upstream-credential-bundle", default_value = DEFAULT_UPSTREAM_CREDENTIAL_BUNDLE)]
    pub upstream_credential_bundle: String,
    /// Trust bundle validating sandbox tunnel certificates.
    #[arg(long = "upstream-trust-bundle", default_value = DEFAULT_UPSTREAM_TRUST_BUNDLE)]
    pub upstream_trust_bundle: String,
    /// Reach sandbox tunnels through mTLS CONNECT tunnels; false uses HTTPS
    /// instead (Envoy dataplane only).
    #[arg(long = "upstream-connect", default_value_t = true, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub upstream_connect: bool,
    /// Trust bundle of the actor-identity CA verifying egress CONNECT client
    /// certificates; empty denies every egress CONNECT.
    #[arg(long = "actor-identity-ca-file", default_value = "")]
    pub actor_identity_ca_file: String,
    /// How long egress acts on a cached EgressPolicy; 0 disables the cache.
    #[arg(long = "egress-policy-cache-ttl", default_value = "10s", value_parser = duration)]
    pub egress_policy_cache_ttl: Duration,
    /// Credential provider this egress gateway serves; empty disables the
    /// check (development only).
    #[arg(long = "credential-provider-name", default_value = "")]
    pub credential_provider_name: String,
    /// gRPC dial target of the credential provider; empty disables egress
    /// credential injection.
    #[arg(long = "credential-provider-address", default_value = "")]
    pub credential_provider_address: String,
    /// CA the credential provider's certificate must chain to.
    #[arg(long = "credential-provider-ca-file", default_value = "")]
    pub credential_provider_ca_file: String,
    /// Credential bundle presented to the credential provider.
    #[arg(long = "credential-provider-client-cert", default_value = "")]
    pub credential_provider_client_cert: String,
    /// SNI expected on the credential provider's certificate.
    #[arg(long = "credential-provider-server-name", default_value = "")]
    pub credential_provider_server_name: String,
    /// Dial the credential provider without TLS (development only).
    #[arg(long = "credential-provider-insecure", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub credential_provider_insecure: bool,
    /// OTLP gRPC collector Envoy reports spans to, as host:port or an http://
    /// URL. An unusable address disables Envoy-side tracing with a warning;
    /// empty disables it.
    #[arg(
        long = "otlp-collector-address",
        env = "OTEL_EXPORTER_OTLP_ENDPOINT",
        default_value = ""
    )]
    pub otlp_collector_address: String,
    /// CAs trusted to verify the control plane's certificate.
    #[arg(
        long = "hivekeep-api-ca-file",
        alias = "api-ca-file",
        default_value = ""
    )]
    pub hivekeep_api_ca_file: String,
    /// Credential bundle presented to the control plane.
    #[arg(
        long = "hivekeep-api-client-cert",
        alias = "api-client-cert",
        default_value = ""
    )]
    pub hivekeep_api_client_cert: String,
    /// SNI expected on the control plane's certificate.
    #[arg(
        long = "hivekeep-api-server-name",
        alias = "api-server-name",
        default_value = ""
    )]
    pub hivekeep_api_server_name: String,
    /// Envoy's end-to-end timeout on the workload route. It does not cover
    /// the resume before the request, nor size the shutdown drain.
    #[arg(long = "route-timeout", default_value = "5m", value_parser = duration)]
    pub route_timeout: Duration,
    /// Longest a resume flight keeps a request parked.
    #[arg(long = "parked-request-budget", default_value = "5s", value_parser = duration)]
    pub parked_request_budget: Duration,
    /// Most requests parked at once; 0 disables parking.
    #[arg(long = "parked-request-max", default_value_t = DEFAULT_PARKED_REQUEST_MAX)]
    pub parked_request_max: usize,
    /// Delay before a parked request's first retry.
    #[arg(long = "parked-request-retry-interval", default_value = "100ms", value_parser = duration)]
    pub parked_request_retry_interval: Duration,
    /// Retry delay multiplier; must be >= 1.
    #[arg(long = "parked-request-retry-factor", default_value_t = DEFAULT_PARKED_REQUEST_RETRY_FACTOR)]
    pub parked_request_retry_factor: f64,
    /// Random fraction in [0, 1) added to each retry delay.
    #[arg(long = "parked-request-retry-jitter", default_value_t = DEFAULT_PARKED_REQUEST_RETRY_JITTER)]
    pub parked_request_retry_jitter: f64,
    /// Envoy's ext_proc circuit breaker; 0 derives twice
    /// --parked-request-max (at least 1024).
    #[arg(
        long = "extproc-max-requests",
        default_value_t = 0,
        allow_negative_numbers = true
    )]
    pub extproc_max_requests: i64,
    /// How long to keep serving after SIGTERM before draining.
    #[arg(long = "drain-delay", default_value = "13s", value_parser = duration)]
    pub drain_delay: Duration,
    /// Deadline of the ext_proc drain; 0 derives it from
    /// --parked-request-budget.
    #[arg(long = "drain-timeout", default_value = "0s", value_parser = duration)]
    pub drain_timeout: Duration,
    /// Envoy admin interface the shutdown sequence drains through.
    #[arg(long = "envoy-admin-address", default_value = "localhost:9901")]
    pub envoy_admin_address: String,
    /// Marker created once the drain completes, for the dataplane's preStop
    /// hook; empty disables the handshake.
    #[arg(long = "drain-complete-file", default_value = DEFAULT_DRAIN_COMPLETE_FILE)]
    pub drain_complete_file: String,
}

impl From<RouterArgs> for RouterConfig {
    fn from(a: RouterArgs) -> Self {
        RouterConfig {
            mode: a.mode,
            dataplane: a.gate_dataplane,
            namespace: a.namespace,
            router_service_name: a.router_service_name,
            kubeconfig: a.kubeconfig,
            hivekeep_api_addr: a.hivekeep_api_address,
            xds_port: a.port_xds,
            extproc_port: a.port_extproc,
            extproc_addr: a.extproc_address,
            status_port: a.status_port,
            status_addr: a.status_address,
            health_interval: a.health_interval,
            https_port: a.port_https,
            connect_tls_port: a.port_connect_tls,
            envoy_cert_path: a.envoy_cert_path,
            upstream_credential_bundle_path: a.upstream_credential_bundle,
            upstream_trust_bundle_path: a.upstream_trust_bundle,
            upstream_connect: a.upstream_connect,
            actor_identity_ca_file: a.actor_identity_ca_file,
            egress_policy_cache_ttl: a.egress_policy_cache_ttl,
            credential_provider: CredentialProviderConfig {
                name: a.credential_provider_name,
                address: a.credential_provider_address,
                ca_file: a.credential_provider_ca_file,
                client_cert: a.credential_provider_client_cert,
                server_name: a.credential_provider_server_name,
                insecure: a.credential_provider_insecure,
            },
            log_level: "info".into(),
            metrics_addr: a.metrics_listen_addr,
            health_addr: a.health_listen_addr,
            otlp_collector_address: a.otlp_collector_address,
            auth: ApiAuthConfig {
                ca_file: a.hivekeep_api_ca_file,
                client_cert_path: a.hivekeep_api_client_cert,
                server_name: a.hivekeep_api_server_name,
            },
            ingress_auth: IngressAuthConfig {
                client_ca_file: a.ingress_client_ca_file,
                allowed_spiffe_ids: a.ingress_allowed_spiffe_ids,
            },
            route_timeout: a.route_timeout,
            parked_request: ParkedRequestConfig {
                budget: a.parked_request_budget,
                max: a.parked_request_max,
                retry_interval: a.parked_request_retry_interval,
                retry_factor: a.parked_request_retry_factor,
                retry_jitter: a.parked_request_retry_jitter,
            },
            ext_proc_max_requests: a.extproc_max_requests,
            drain_delay: a.drain_delay,
            drain_timeout: a.drain_timeout,
            envoy_admin_addr: a.envoy_admin_address,
            drain_complete_file: a.drain_complete_file,
        }
    }
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;

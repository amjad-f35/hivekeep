//! Command-line flags.

use clap::Parser;
use hivekeep_k8s::install_defaults;

use crate::controllers::{ControllerConfig, WorkerOtelSettings};

/// The default control-plane target: the hivekeep-api Service, resolved
/// through its EndpointSlices.
pub const DEFAULT_API_TARGET: &str = "k8s:///hivekeep-api.hivekeep-system.svc:443";
/// The default CA bundle the hivekeep-api serving certificate chains to.
pub const DEFAULT_API_CA: &str = "/run/servicedns.podcert.hivekeep.io/trust-bundle.pem";
/// The default metrics listener; `0` turns it off.
pub const DEFAULT_METRICS_ADDR: &str = "0.0.0.0:8080";
/// The default health listener.
pub const DEFAULT_HEALTH_ADDR: &str = "0.0.0.0:8081";

/// Hivekeep cluster controller.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "hivekeep-controller",
    version,
    about = "Hivekeep cluster controller"
)]
pub struct Args {
    /// hivekeep-api target: `k8s:///<svc>.<ns>.svc:<port>` (EndpointSlice
    /// load balancing) or an `https://host:port` URL.
    #[arg(long, default_value = DEFAULT_API_TARGET)]
    pub api_conn_spec: String,
    /// Logging and OTLP export flags (`--log-level`, `--log-format`,
    /// `--otlp-endpoint`).
    #[command(flatten)]
    pub telemetry: hivekeep_telemetry::TelemetryArgs,
    /// Log as JSON. JSON is the default; kept so existing manifests parse.
    #[arg(long, hide = true)]
    pub log_json: bool,
    /// Preview gates to enable (repeatable or comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub preview: Vec<String>,

    /// OTLP endpoint set on worker pods.
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT", default_value = "")]
    pub otel_exporter_otlp_endpoint: String,
    /// Metric export interval (ms) set on worker pods; empty keeps the SDK default.
    #[arg(long, env = "OTEL_METRIC_EXPORT_INTERVAL", default_value = "")]
    pub otel_metric_export_interval: String,
    /// Per-export timeout (ms) set on worker pods; empty keeps the SDK default.
    #[arg(long, env = "OTEL_METRIC_EXPORT_TIMEOUT", default_value = "")]
    pub otel_metric_export_timeout: String,
    /// Trace sampler set on worker pods.
    #[arg(long, env = "OTEL_TRACES_SAMPLER", default_value = "")]
    pub otel_traces_sampler: String,
    /// Trace sampler argument set on worker pods.
    #[arg(long, env = "OTEL_TRACES_SAMPLER_ARG", default_value = "")]
    pub otel_traces_sampler_arg: String,
    /// Logs exporter set on worker pods.
    #[arg(long, env = "HK_WORKER_OTEL_LOGS_EXPORTER", default_value = "")]
    pub otel_logs_exporter: String,
    /// Traces exporter set on worker pods.
    #[arg(long, env = "HK_WORKER_OTEL_TRACES_EXPORTER", default_value = "")]
    pub otel_traces_exporter: String,
    /// Metrics exporter set on worker pods.
    #[arg(long, env = "HK_WORKER_OTEL_METRICS_EXPORTER", default_value = "")]
    pub otel_metrics_exporter: String,

    /// ServiceAccount the keeper runs as: the service-account segment of
    /// the SPIFFE ID each worker's tunnel expects on the credential broker.
    #[arg(long, default_value = install_defaults::KEEPER_SERVICE_ACCOUNT)]
    pub keeper_service_account: String,
    /// ServiceAccount the gate router runs as: the service-account segment
    /// of the SPIFFE ID each worker's tunnel accepts on actor ingress.
    #[arg(long, default_value = install_defaults::ROUTER_SERVICE_ACCOUNT)]
    pub router_service_account: String,

    /// PEM file of CAs trusted for the hivekeep-api serving certificate.
    #[arg(long, default_value = DEFAULT_API_CA)]
    pub api_ca_file: String,
    /// Server name expected on the hivekeep-api certificate; defaults to
    /// the target's Service host.
    #[arg(long, default_value = "")]
    pub api_server_name: String,
    /// Credential bundle (key + chain PEM) presented as the client
    /// certificate to hivekeep-api. Required.
    #[arg(long, default_value = "")]
    pub api_client_cert: String,

    /// Concurrent workers reconciling worker pods into the registry.
    #[arg(long, default_value_t = 2)]
    pub worker_sync_concurrency: usize,
    /// DNS egress policy mode propagated to worker pods.
    #[arg(long, default_value = "")]
    pub dns_egress_policy: String,

    /// Metrics listener; `0` disables it.
    #[arg(long, default_value = DEFAULT_METRICS_ADDR)]
    pub metrics_addr: String,
    /// Health listener (`/healthz`, `/readyz`); `0` disables it.
    #[arg(long, default_value = DEFAULT_HEALTH_ADDR)]
    pub health_addr: String,
}

impl Args {
    /// Checks flag values that clap cannot.
    pub fn validate(&self) -> Result<(), String> {
        if self.worker_sync_concurrency == 0 {
            return Err("--worker-sync-concurrency must be positive".into());
        }
        crate::controllers::validate_dns_egress_policy(&self.dns_egress_policy)
            .map_err(|e| format!("--dns-egress-policy: {e}"))?;
        // An empty ServiceAccount would still yield identities that parse
        // but match nothing, so every worker would reject both peers.
        for (flag, v) in [
            ("--keeper-service-account", &self.keeper_service_account),
            ("--router-service-account", &self.router_service_account),
        ] {
            if v.is_empty() {
                return Err(format!("{flag} must not be empty"));
            }
        }
        if self.api_client_cert.is_empty() {
            return Err("--api-client-cert is required".into());
        }
        Ok(())
    }

    /// The controllers' configuration.
    pub fn controller_config(&self, system_namespace: String) -> ControllerConfig {
        ControllerConfig {
            system_namespace,
            keeper_service_account: self.keeper_service_account.clone(),
            router_service_account: self.router_service_account.clone(),
            dns_egress_policy: self.dns_egress_policy.clone(),
            otel: WorkerOtelSettings {
                endpoint: self.otel_exporter_otlp_endpoint.clone(),
                metric_export_interval: self.otel_metric_export_interval.clone(),
                metric_export_timeout: self.otel_metric_export_timeout.clone(),
                traces_sampler: self.otel_traces_sampler.clone(),
                traces_sampler_arg: self.otel_traces_sampler_arg.clone(),
                logs_exporter: self.otel_logs_exporter.clone(),
                traces_exporter: self.otel_traces_exporter.clone(),
                metrics_exporter: self.otel_metrics_exporter.clone(),
            },
        }
    }
}

/// A listen address flag: `None` when it is `0` or empty.
pub fn listen_addr(flag: &str) -> Option<&str> {
    match flag {
        "" | "0" => None,
        a => Some(a),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::controllers::worker_env::DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL as ENFORCE;

    fn parse(extra: &[&str]) -> Args {
        let mut argv = vec!["hivekeep-controller", "--api-client-cert", "/x"];
        if !extra.contains(&"--dns-egress-policy") {
            argv.extend_from_slice(&["--dns-egress-policy", ENFORCE]);
        }
        argv.extend_from_slice(extra);
        Args::try_parse_from(argv).unwrap()
    }

    #[test]
    fn dns_egress_policy_default_is_empty() {
        let a = Args::try_parse_from(["hivekeep-controller"]).unwrap();
        assert_eq!(a.dns_egress_policy, "");
        // Empty is no policy, which the controller refuses to start with.
        assert!(parse(&["--dns-egress-policy", ""]).validate().is_err());
        parse(&[]).validate().unwrap();
    }

    #[test]
    fn metrics_listener() {
        assert_eq!(
            listen_addr(&parse(&[]).metrics_addr),
            Some(DEFAULT_METRICS_ADDR)
        );
        assert_eq!(
            listen_addr(&parse(&["--metrics-addr", "0"]).metrics_addr),
            None
        );
    }

    #[test]
    fn validation() {
        assert!(parse(&["--worker-sync-concurrency", "0"])
            .validate()
            .is_err());
        assert!(parse(&["--keeper-service-account", ""]).validate().is_err());
        assert!(parse(&["--router-service-account", ""]).validate().is_err());
        assert!(parse(&["--dns-egress-policy", "bogus"]).validate().is_err());
        let a =
            Args::try_parse_from(["hivekeep-controller", "--dns-egress-policy", ENFORCE]).unwrap();
        assert!(a.validate().unwrap_err().contains("--api-client-cert"));
    }

    #[test]
    fn controller_config_carries_flags() {
        let c = parse(&[
            "--keeper-service-account",
            "k",
            "--otel-traces-exporter",
            "none",
        ])
        .controller_config("ns".into());
        assert_eq!(c.system_namespace, "ns");
        assert_eq!(c.keeper_service_account, "k");
        assert_eq!(c.otel.traces_exporter, "none");
    }
}

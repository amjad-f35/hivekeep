//! Command-line flags.

use std::path::PathBuf;

use clap::Parser;
use hivekeep_k8s::install_defaults::SYSTEM_NAMESPACE;

/// Pod certificate controller: answers PodCertificateRequests for the
/// service DNS, pod identity and PostgreSQL login signers.
#[derive(Debug, Clone, Parser)]
#[command(name = "hivekeep-podcert-controller", version)]
pub struct Args {
    /// Path to the kubeconfig file (default: `$HOME/.kube/config`).
    #[arg(long)]
    pub kubeconfig: Option<PathBuf>,

    /// Use the in-cluster service account configuration.
    #[arg(long)]
    pub in_cluster: bool,

    /// (Work sharding) Namespace the controller runs in; replica Leases
    /// live here.
    #[arg(long, default_value = "")]
    pub sharding_pod_namespace: String,

    /// (Work sharding) Pod name of this replica.
    #[arg(long, default_value = "")]
    pub sharding_pod_name: String,

    /// (Work sharding) Pod UID of this replica.
    #[arg(long, default_value = "")]
    pub sharding_pod_uid: String,

    /// (Work sharding) Application name that tells this controller's
    /// Leases apart.
    #[arg(long, default_value = "")]
    pub sharding_application_name: String,

    /// CA pool state file of the service DNS signer.
    #[arg(long)]
    pub service_dns_ca_pool: PathBuf,

    /// CA pool state file of the pod identity signer.
    #[arg(long)]
    pub pod_identity_ca_pool: PathBuf,

    /// CA pool state file of the PostgreSQL login signer.
    #[arg(long)]
    pub postgres_ca_pool: PathBuf,

    /// Namespace whose control plane service account may request
    /// PostgreSQL login certificates.
    #[arg(long, default_value = SYSTEM_NAMESPACE)]
    pub postgres_client_namespace: String,

    /// Concurrent workers per signer.
    #[arg(long, default_value_t = 1)]
    pub workers_per_signer: usize,

    /// Sustained Kubernetes API queries per second, enforced client side;
    /// 0 disables the limit.
    #[arg(long, default_value_t = 0.0)]
    pub kube_api_qps: f32,

    /// Kubernetes API query burst above the sustained rate; 0 allows one
    /// second's worth of queries.
    #[arg(long, default_value_t = 0)]
    pub kube_api_burst: i64,

    /// Preview gates to enable.
    #[arg(long, value_delimiter = ',')]
    pub preview: Vec<String>,

    /// Logging and OTLP export flags (`--log-level`, which also takes a
    /// filter such as `hivekeep_podcert_controller=debug`, `--log-format`,
    /// `--otlp-endpoint`).
    #[command(flatten)]
    pub telemetry: hivekeep_telemetry::TelemetryArgs,

    /// Log as JSON lines; `false` selects text when `--log-format` is
    /// unset. Kept so existing manifests parse.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set, hide = true)]
    pub log_json: bool,
}

impl Args {
    /// Rejects flag combinations that cannot work.
    pub fn validate(&self) -> Result<(), String> {
        if self.kube_api_qps < 0.0 || self.kube_api_burst < 0 {
            return Err(
                "invalid rate limits: --kube-api-qps and --kube-api-burst must not be negative"
                    .into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(extra: &[&str]) -> Result<Args, clap::Error> {
        let mut argv = vec![
            "hivekeep-podcert-controller",
            "--service-dns-ca-pool=/a",
            "--pod-identity-ca-pool=/b",
            "--postgres-ca-pool=/c",
        ];
        argv.extend_from_slice(extra);
        Args::try_parse_from(argv)
    }

    #[test]
    fn defaults() {
        let a = parse(&[]).unwrap();
        assert_eq!(a.postgres_client_namespace, "hivekeep-system");
        assert_eq!(a.workers_per_signer, 1);
        assert!(!a.in_cluster);
        assert!(a.validate().is_ok());
    }

    #[test]
    fn negative_rate_limits_are_rejected() {
        assert!(parse(&["--kube-api-qps=-1"]).unwrap().validate().is_err());
        assert!(parse(&["--kube-api-burst=-1"]).unwrap().validate().is_err());
        assert!(parse(&["--kube-api-qps=5", "--kube-api-burst=10"])
            .unwrap()
            .validate()
            .is_ok());
    }

    #[test]
    fn ca_pools_are_required() {
        assert!(Args::try_parse_from(["hivekeep-podcert-controller"]).is_err());
    }
}

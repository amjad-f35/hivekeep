//! The worker's command line.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

/// The floor of `--usage-sample-interval`, matching the keeper's poll floor,
/// so one sweep's budget fits inside an interval.
pub const MIN_USAGE_SAMPLE_INTERVAL: Duration = Duration::from_secs(50);

/// The micro-VM sandbox worker.
#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Config {
    /// The UID of this worker pod.
    #[arg(long, env = "HK_POD_UID", default_value = "")]
    pub pod_uid: String,
    /// The comma-separated IPs of this worker pod.
    #[arg(long, env = "HK_POD_IPS", default_value = "")]
    pub pod_ips: String,
    /// The VMM binary when a request brings none.
    #[arg(long, default_value = "cloud-hypervisor")]
    pub cloud_hypervisor_binary: String,
    /// Boot guests with the agent's debug log, debug console and early UART.
    #[arg(long)]
    pub guest_debug: bool,
    /// Minimum log level: debug, info, warn or error.
    #[arg(long, default_value = "info")]
    pub log_level: String,
    /// The address of the HTTP /readyz and /healthz probes.
    #[arg(long, default_value = "0.0.0.0:8080")]
    pub readiness_listen_address: String,
    /// How many Actors this worker hosts at once.
    #[arg(long, default_value_t = 1000)]
    pub max_actors: i64,
    /// How often hosted Actors' usage is sampled.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "1m")]
    pub usage_sample_interval: Duration,
    /// The worker pod's cgroup scope.
    #[arg(long, default_value = crate::actor_cgroup::DEFAULT_CGROUP_ROOT)]
    pub cgroup_root: PathBuf,
    /// Serve with this epoch instead of registering with the keeper. For
    /// development without a keeper.
    #[arg(long)]
    pub standalone_epoch: Option<i64>,
    /// The worker pool's namespace, for usage records.
    #[arg(long, env = "POD_NAMESPACE", default_value = "")]
    pub pool_namespace: String,
    /// The worker pool's name, for usage records.
    #[arg(long, env = "WORKER_POOL_NAME", default_value = "")]
    pub pool_name: String,
    /// Preview gates to enable.
    #[arg(long, value_delimiter = ',')]
    pub preview: Vec<String>,
    /// The tunnel, keeper connection and pod capacity.
    #[command(flatten)]
    pub node: hivekeep_sandbox_gvisor::node_flags::NodeFlags,
}

impl Config {
    /// Checks the flags against each other.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_actors < 0 {
            return Err(format!(
                "--max-actors must not be negative, got {}",
                self.max_actors
            ));
        }
        if self.usage_sample_interval < MIN_USAGE_SAMPLE_INTERVAL {
            return Err(format!(
                "--usage-sample-interval must be at least {MIN_USAGE_SAMPLE_INTERVAL:?}, got {:?}",
                self.usage_sample_interval
            ));
        }
        hivekeep_core::names::check_dns_label(&self.pod_uid)
            .first()
            .map_or(Ok(()), |m| {
                Err(format!("--pod-uid {:?}: {m}", self.pod_uid))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Config {
        Config::parse_from(std::iter::once("w").chain(args.iter().copied()))
    }

    #[test]
    fn validates_flags() {
        let ok = parse(&["--pod-uid", "6f1d6c7e-2f7e-4b1e-9c1d-1d2f3a4b5c6d"]);
        assert!(ok.validate().is_ok(), "{:?}", ok.validate());
        assert!(parse(&["--pod-uid", "6f1d6c7e", "--max-actors=-1"])
            .validate()
            .is_err());
        assert!(
            parse(&["--pod-uid", "6f1d6c7e", "--usage-sample-interval", "10s"])
                .validate()
                .is_err()
        );
        assert!(parse(&["--pod-uid", "Bad/UID"]).validate().is_err());
    }
}

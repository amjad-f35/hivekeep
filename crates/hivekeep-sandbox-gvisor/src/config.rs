//! Command-line configuration of the worker binary.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

/// The floor of `--usage-sample-interval`, matching the keeper's poll floor.
pub const MIN_USAGE_SAMPLE_INTERVAL: Duration = Duration::from_secs(50);

/// Flags of `hivekeep-sandbox-gvisor`.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "hivekeep-sandbox-gvisor",
    version,
    about = "Runs Hivekeep Actors in gVisor sandboxes."
)]
pub struct Config {
    /// The UID of the worker pod.
    #[arg(long, env = "HK_POD_UID", default_value = "")]
    pub pod_uid: String,

    /// The comma-separated IPs of the worker pod.
    #[arg(long, env = "HK_POD_IPS", default_value = "")]
    pub pod_ips: String,

    /// Address for the HTTP /readyz and /healthz probes.
    #[arg(long, default_value = "0.0.0.0:8080")]
    pub readiness_listen_address: String,

    /// How many Actors this worker hosts at once.
    #[arg(long, default_value_t = 1000)]
    pub max_actors: usize,

    /// How often Actor usage is sampled; each sample writes a usage record
    /// and refreshes what GetActiveWorkloadStats serves.
    #[arg(long, default_value = "1m", value_parser = humantime::parse_duration)]
    pub usage_sample_interval: Duration,

    /// Minimum log level: trace, debug, info, warn or error.
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Preview gates to enable.
    #[arg(long, value_delimiter = ',')]
    pub preview: Vec<String>,

    /// The JSON runsc flag configuration. Defaults to `internal_flags.json`,
    /// under `$HK_DATA_PATH` when set.
    #[arg(long, env = "HK_RUNSC_FLAGS")]
    pub runsc_flags: Option<PathBuf>,

    /// The pod cgroup scope sandbox leaves are created under.
    #[arg(long, default_value = crate::cgroup::DEFAULT_CGROUP_ROOT)]
    pub cgroup_root: PathBuf,

    /// The worker pool's namespace, for usage records.
    #[arg(long, env = "POD_NAMESPACE", default_value = "")]
    pub pool_namespace: String,

    /// The worker pool's name, for usage records.
    #[arg(long, env = "WORKER_POOL_NAME", default_value = "")]
    pub pool_name: String,

    /// Serve with this epoch instead of registering with the keeper. For
    /// running the worker without a keeper.
    #[arg(long, env = "HK_STANDALONE_EPOCH")]
    pub standalone_epoch: Option<i64>,

    /// The tunnel, keeper connection and pod capacity.
    #[command(flatten)]
    pub node: crate::node_flags::NodeFlags,
}

impl Config {
    /// Checks flag values that clap cannot.
    pub fn validate(&self) -> Result<(), String> {
        if self.usage_sample_interval < MIN_USAGE_SAMPLE_INTERVAL {
            return Err(format!(
                "--usage-sample-interval must be at least {:?}, got {:?}",
                MIN_USAGE_SAMPLE_INTERVAL, self.usage_sample_interval
            ));
        }
        let msgs = hivekeep_core::names::check_dns_label(&self.pod_uid);
        if !msgs.is_empty() {
            return Err(format!(
                "invalid --pod-uid {:?}: {}",
                self.pod_uid,
                msgs.join("; ")
            ));
        }
        Ok(())
    }

    /// Where the runsc flag configuration is read from.
    pub fn runsc_flags_path(&self) -> PathBuf {
        if let Some(p) = &self.runsc_flags {
            return p.clone();
        }
        let name = PathBuf::from("internal_flags.json");
        match std::env::var_os("HK_DATA_PATH") {
            Some(d) => PathBuf::from(d).join(name),
            None => name,
        }
    }

    /// The pod's IPs.
    pub fn pod_ip_list(&self) -> Vec<String> {
        self.pod_ips
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_validates() {
        let c = Config::try_parse_from([
            "w",
            "--pod-uid",
            "pod-1",
            "--pod-ips",
            "10.0.0.1, fd00::1",
            "--usage-sample-interval",
            "2m",
        ])
        .unwrap();
        assert!(c.validate().is_ok());
        assert_eq!(c.pod_ip_list(), vec!["10.0.0.1", "fd00::1"]);
        assert_eq!(c.usage_sample_interval, Duration::from_secs(120));
        let short =
            Config::try_parse_from(["w", "--pod-uid", "pod-1", "--usage-sample-interval", "10s"])
                .unwrap();
        assert!(short.validate().is_err());
        let bad = Config::try_parse_from(["w", "--pod-uid", "../x"]).unwrap();
        assert!(bad.validate().is_err());
    }
}

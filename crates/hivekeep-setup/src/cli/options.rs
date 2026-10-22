//! Reading command-scoped settings into step options, naming the channel a
//! rejected value came from.

use crate::config::{Config, ConfigError, SANDBOX_CLASS_GVISOR, SANDBOX_CLASS_MICROVM};
use crate::steps::benchmarks::BenchmarkOptions;
use crate::steps::csi::validate_csi_driver;
use crate::steps::deploy::DeployOptions;
use crate::steps::publish::PublishReleaseOptions;

/// The `deploy hivekeep-system` options. A driver from the environment or a
/// file cannot be checked while arguments are validated, so it is checked
/// here, after loading but before anything is installed.
pub fn deploy_options(cfg: &Config) -> Result<DeployOptions, ConfigError> {
    let r = cfg.resolved();
    let opts = DeployOptions {
        setup_csi: r.string("csi.setup"),
    };
    if validate_csi_driver(&opts.setup_csi).is_err() {
        return Err(ConfigError::invalid(
            &r.value_or_default("csi.setup"),
            "nfs, hostpath, both or none",
        ));
    }
    Ok(opts)
}

/// The benchmark options. Each setting reaches three channels, so a
/// rejection names the one that supplied the value.
pub fn benchmark_options(cfg: &Config) -> Result<BenchmarkOptions, ConfigError> {
    let r = cfg.resolved();
    let opts = BenchmarkOptions {
        worker_count: r.int("benchmark.workerCount"),
        sandbox_class: r.string("benchmark.sandboxClass"),
    };
    if opts.worker_count < 1 {
        return Err(ConfigError::invalid(
            &r.value_or_default("benchmark.workerCount"),
            "at least 1",
        ));
    }
    match opts.sandbox_class.as_str() {
        SANDBOX_CLASS_GVISOR | SANDBOX_CLASS_MICROVM => Ok(opts),
        _ => Err(ConfigError::invalid(
            &r.value_or_default("benchmark.sandboxClass"),
            format!("{SANDBOX_CLASS_GVISOR} or {SANDBOX_CLASS_MICROVM}"),
        )),
    }
}

pub fn publish_release_options(cfg: &Config) -> PublishReleaseOptions {
    PublishReleaseOptions {
        overwrite: cfg.resolved().bool("publish.overwrite"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{resolve, ResolveOptions};
    use std::collections::BTreeMap;

    fn cfg_from_env(pairs: &[(&str, &str)]) -> Config {
        super::super::register_settings();
        let env: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let r = resolve(
            None,
            ResolveOptions {
                file: None,
                env: Some(&env),
            },
        )
        .unwrap();
        let mut cfg = Config::default();
        cfg.set_resolved(r);
        cfg
    }

    #[test]
    fn benchmark_options_name_the_supplying_channel() {
        let cases: [(&str, (&str, &str), &[&str]); 3] = [
            ("valid", ("HK_BENCHMARK_SANDBOX_CLASS", "microvm"), &[]),
            (
                "sandbox class from the environment",
                ("HK_BENCHMARK_SANDBOX_CLASS", "nonsense"),
                &[
                    "benchmark.sandboxClass",
                    "nonsense",
                    "HK_BENCHMARK_SANDBOX_CLASS",
                ],
            ),
            (
                "worker count from the environment",
                ("HK_BENCHMARK_WORKER_COUNT", "0"),
                &[
                    "benchmark.workerCount",
                    "at least 1",
                    "HK_BENCHMARK_WORKER_COUNT",
                ],
            ),
        ];
        for (name, var, want_in) in cases {
            let res = benchmark_options(&cfg_from_env(&[var]));
            if want_in.is_empty() {
                assert!(res.is_ok(), "{name}: {res:?}");
                continue;
            }
            let err = res.expect_err(name).to_string();
            for want in want_in {
                assert!(err.contains(want), "{name}: {err:?} missing {want:?}");
            }
        }
    }

    #[test]
    fn csi_driver_from_the_environment_is_still_rejected() {
        assert!(deploy_options(&cfg_from_env(&[("SETUP_CSI", "nfs")])).is_ok());
        let err = deploy_options(&cfg_from_env(&[("SETUP_CSI", "hostpaht")]))
            .unwrap_err()
            .to_string();
        // The value came from the environment, so the message names the
        // variable rather than --setup-csi.
        for want in ["csi.setup", "hostpaht", "SETUP_CSI"] {
            assert!(err.contains(want), "{err:?} missing {want:?}");
        }
    }
}

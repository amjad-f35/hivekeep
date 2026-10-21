//! The benchmark and micro-VM stacks, still driven by repository scripts
//! that orchestrate image builds, asset assembly and object-store staging;
//! the installer translates its settings into their argument lists.

use super::{Env, Result, StepError};
use crate::config::{SANDBOX_CLASS_GVISOR, SANDBOX_CLASS_MICROVM};
use crate::log;

/// Repository scripts, relative to the repository root.
pub const DEPLOY_LOCUST_SCRIPT: &str = "scripts/benchmarking/deploy_locust.sh";
pub const INSTALL_MICROVM_DEP_SCRIPT: &str = "scripts/install-microvm-deps.sh";

/// Shapes the benchmark WorkerPool.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BenchmarkOptions {
    /// WorkerPool replicas.
    pub worker_count: i64,
    /// gvisor or microvm.
    pub sandbox_class: String,
}

impl BenchmarkOptions {
    /// A precondition on the exported methods. The commands check the same
    /// settings first and name the channel; these messages name the field,
    /// for callers that built the options themselves.
    pub fn validate(&self) -> Result<()> {
        if self.worker_count < 1 {
            return Err(StepError::msg(format!(
                "BenchmarkOptions.worker_count must be at least 1, got {}",
                self.worker_count
            )));
        }
        match self.sandbox_class.as_str() {
            SANDBOX_CLASS_GVISOR | SANDBOX_CLASS_MICROVM => Ok(()),
            other => Err(StepError::msg(format!(
                "BenchmarkOptions.sandbox_class must be {SANDBOX_CLASS_GVISOR} or {SANDBOX_CLASS_MICROVM}, got {other:?}"
            ))),
        }
    }
}

/// The deploy_locust.sh arguments. The script reads these only as flags;
/// the trailing two are omitted when unset, since it rejects an empty
/// endpoint and an empty memory would override the workload default.
pub(crate) fn deploy_locust_args(
    opts: &BenchmarkOptions,
    otlp_endpoint: &str,
    actor_memory: &str,
) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "--deploy".into(),
        "--worker-count".into(),
        opts.worker_count.to_string(),
        "--sandbox-class".into(),
        opts.sandbox_class.clone(),
    ];
    // Actor telemetry goes where the control plane's does.
    if !otlp_endpoint.is_empty() {
        args.extend(["--otlp-endpoint".into(), otlp_endpoint.into()]);
    }
    if !actor_memory.is_empty() {
        args.extend(["--actor-memory".into(), actor_memory.into()]);
    }
    args
}

impl Env {
    /// Installs the benchmark workloads and the locust load test stack.
    pub async fn deploy_benchmarks(&self, opts: &BenchmarkOptions) -> Result<()> {
        opts.validate()?;
        self.require_control_plane().await?;
        log::step(&format!(
            "deploy_benchmarks (worker_count={}, sandbox_class={})",
            opts.worker_count, opts.sandbox_class
        ));
        // The microvm SandboxConfig is outside the default set; the workloads
        // reference it by name.
        if opts.sandbox_class == SANDBOX_CLASS_MICROVM {
            self.run_script(INSTALL_MICROVM_DEP_SCRIPT, &["--install"])
                .await?;
        }
        let args = deploy_locust_args(
            opts,
            &self.cfg.otlp_endpoint,
            &self.cfg.benchmark_actor_memory,
        );
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        self.run_script(DEPLOY_LOCUST_SCRIPT, &args).await
    }

    /// Removes the locust stack and the benchmark workloads. The cluster-wide
    /// microvm SandboxConfig goes only when microvm was chosen, after the
    /// pool; the script keeps it if another pool still uses it.
    pub async fn delete_benchmarks(&self, opts: &BenchmarkOptions) -> Result<()> {
        opts.validate()?;
        log::step(&format!(
            "delete_benchmarks (sandbox_class={})",
            opts.sandbox_class
        ));
        self.run_script(DEPLOY_LOCUST_SCRIPT, &["--delete"]).await?;
        if opts.sandbox_class == SANDBOX_CLASS_MICROVM {
            return self
                .run_script(INSTALL_MICROVM_DEP_SCRIPT, &["--delete"])
                .await;
        }
        Ok(())
    }

    /// Runs a repository script from the repository root with the
    /// environment the installer resolved, and only that environment.
    pub(crate) async fn run_script(&self, rel_path: &str, args: &[&str]) -> Result<()> {
        let mut cmd = tokio::process::Command::new(self.cfg.path(&[rel_path]));
        cmd.args(args).current_dir(&self.cfg.root).env_clear();
        for kv in self.cfg.script_env() {
            if let Some((k, v)) = kv.split_once('=') {
                cmd.env(k, v);
            }
        }
        let status = cmd
            .status()
            .await
            .map_err(|e| StepError::msg(format!("while running {rel_path}: {e}")))?;
        if !status.success() {
            return Err(StepError::msg(format!(
                "while running {rel_path}: {status}"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deploy_locust_args_cases() {
        let opts = BenchmarkOptions {
            worker_count: 4,
            sandbox_class: SANDBOX_CLASS_GVISOR.into(),
        };
        let base = [
            "--deploy",
            "--worker-count",
            "4",
            "--sandbox-class",
            "gvisor",
        ];
        let otlp = "http://otel-collector.hivekeep-system.svc:4317";
        for (name, endpoint, memory, extra) in [
            ("neither configured", "", "", vec![]),
            (
                "otlp endpoint only",
                otlp,
                "",
                vec!["--otlp-endpoint", otlp],
            ),
            (
                "actor memory only",
                "",
                "256Mi",
                vec!["--actor-memory", "256Mi"],
            ),
            (
                "both configured",
                otlp,
                "256Mi",
                vec!["--otlp-endpoint", otlp, "--actor-memory", "256Mi"],
            ),
        ] {
            let mut want: Vec<&str> = base.to_vec();
            want.extend(extra);
            assert_eq!(deploy_locust_args(&opts, endpoint, memory), want, "{name}");
        }
    }

    #[test]
    fn validate_options() {
        assert!(BenchmarkOptions {
            worker_count: 0,
            sandbox_class: "gvisor".into()
        }
        .validate()
        .is_err());
        assert!(BenchmarkOptions {
            worker_count: 1,
            sandbox_class: "runc".into()
        }
        .validate()
        .is_err());
        assert!(BenchmarkOptions {
            worker_count: 1,
            sandbox_class: "microvm".into()
        }
        .validate()
        .is_ok());
    }
}

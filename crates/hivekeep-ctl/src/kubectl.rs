//! Running `kubectl` for the few commands that talk to the cluster itself
//! (pod logs, pod metrics, Secrets), so the user's kubeconfig, context and
//! credential plugins apply exactly as they do for kubectl.

use std::io::Write;
use std::process::{Command, Stdio};

use crate::verbosity::{Logger, FLOW};

/// Environment variable overriding the kubectl binary.
pub const KUBECTL_ENV: &str = "HK_KUBECTL";

/// A failed kubectl invocation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct KubectlError {
    /// kubectl's error output, or why it could not run.
    pub message: String,
}

impl KubectlError {
    /// Whether kubectl reported that the object does not exist.
    pub fn is_not_found(&self) -> bool {
        self.message.contains("NotFound") || self.message.contains("not found")
    }

    /// Whether kubectl reported a resourceVersion conflict.
    pub fn is_conflict(&self) -> bool {
        self.message.contains("Conflict") || self.message.contains("the object has been modified")
    }
}

/// A kubectl bound to a kubeconfig and context.
#[derive(Clone)]
pub struct Kubectl {
    kubeconfig: Option<String>,
    context: Option<String>,
    log: Logger,
}

impl Kubectl {
    /// A kubectl using `kubeconfig` and `context` when given.
    pub fn new(kubeconfig: Option<String>, context: Option<String>, log: Logger) -> Self {
        Kubectl {
            kubeconfig,
            context,
            log,
        }
    }

    fn program() -> String {
        std::env::var(KUBECTL_ENV).unwrap_or_else(|_| "kubectl".into())
    }

    /// The full argument list for `args`, with the connection flags first.
    pub fn args(&self, args: &[&str]) -> Vec<String> {
        let mut all = Vec::new();
        if let Some(k) = &self.kubeconfig {
            all.push(format!("--kubeconfig={k}"));
        }
        if let Some(c) = &self.context {
            all.push(format!("--context={c}"));
        }
        all.extend(args.iter().map(|s| s.to_string()));
        all
    }

    /// An async command for `args`, for streaming output.
    pub fn tokio_command(&self, args: &[&str]) -> tokio::process::Command {
        let full = self.args(args);
        self.log
            .log(FLOW, format!("Running kubectl {}", full.join(" ")));
        let mut cmd = tokio::process::Command::new(Self::program());
        cmd.args(full)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        cmd
    }

    /// Runs `args` to completion, feeding `stdin`, and returns stdout.
    pub fn run(&self, args: &[&str], stdin: Option<&[u8]>) -> Result<Vec<u8>, KubectlError> {
        let full = self.args(args);
        self.log
            .log(FLOW, format!("Running kubectl {}", full.join(" ")));
        let mut child = Command::new(Self::program())
            .args(&full)
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| KubectlError {
                message: format!("running kubectl: {e}"),
            })?;
        if let (Some(data), Some(mut pipe)) = (stdin, child.stdin.take()) {
            pipe.write_all(data).map_err(|e| KubectlError {
                message: format!("writing to kubectl: {e}"),
            })?;
        }
        let out = child.wait_with_output().map_err(|e| KubectlError {
            message: format!("running kubectl: {e}"),
        })?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
            return Err(KubectlError {
                message: if stderr.is_empty() {
                    format!("kubectl exited with {}", out.status)
                } else {
                    stderr
                },
            });
        }
        Ok(out.stdout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_flags_come_first() {
        let k = Kubectl::new(Some("/k".into()), Some("dev".into()), Logger::silent());
        assert_eq!(
            k.args(&["get", "pods"]),
            ["--kubeconfig=/k", "--context=dev", "get", "pods"]
        );
        let k = Kubectl::new(None, None, Logger::silent());
        assert_eq!(k.args(&["version"]), ["version"]);
    }

    #[test]
    fn error_classes() {
        let e = KubectlError {
            message: "Error from server (NotFound): secrets \"x\" not found".into(),
        };
        assert!(e.is_not_found() && !e.is_conflict());
        let e = KubectlError {
            message: "Operation cannot be fulfilled on secrets \"x\": the object has been modified"
                .into(),
        };
        assert!(e.is_conflict());
    }
}

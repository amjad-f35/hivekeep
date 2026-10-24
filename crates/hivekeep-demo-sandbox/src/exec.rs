//! Running one requested command to completion.

use crate::wire::{ProcessRequest, ProcessResponse};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

/// Runs `req.command` with captured output. With a `timeout`, the process is
/// killed once it elapses. Never fails: problems land in `error` with exit
/// code -1, next to whatever output was produced.
pub async fn run(req: &ProcessRequest, timeout: Option<Duration>) -> ProcessResponse {
    let Some((program, args)) = req.command.split_first() else {
        return ProcessResponse {
            exit_code: -1,
            error: "command required".into(),
            ..Default::default()
        };
    };
    let mut cmd = Command::new(program);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if !req.cwd.is_empty() {
        cmd.current_dir(&req.cwd);
    }
    cmd.envs(&req.env_vars);

    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return ProcessResponse {
                exit_code: -1,
                error: format!("starting {program}: {e}"),
                ..Default::default()
            }
        }
    };
    let waited = match timeout {
        Some(t) => match tokio::time::timeout(t, child.wait_with_output()).await {
            Ok(r) => r,
            // Dropping the future drops the child, which kills it.
            Err(_) => {
                return ProcessResponse {
                    exit_code: -1,
                    error: format!("killed after the {t:?} timeout"),
                    ..Default::default()
                }
            }
        },
        None => child.wait_with_output().await,
    };
    match waited {
        Ok(out) => {
            let mut resp = ProcessResponse {
                stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
                exit_code: out.status.code().unwrap_or(-1),
                error: String::new(),
            };
            if !out.status.success() {
                resp.error = out.status.to_string();
            }
            resp
        }
        Err(e) => ProcessResponse {
            exit_code: -1,
            error: format!("waiting for {program}: {e}"),
            ..Default::default()
        },
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn req(cmd: &[&str]) -> ProcessRequest {
        ProcessRequest {
            command: cmd.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn captures_output_and_status() {
        let r = run(&req(&["sh", "-c", "echo out; echo err >&2; exit 3"]), None).await;
        assert_eq!(r.stdout, "out\n");
        assert_eq!(r.stderr, "err\n");
        assert_eq!(r.exit_code, 3);
        assert!(!r.error.is_empty());
    }

    #[tokio::test]
    async fn success_has_no_error() {
        let r = run(&req(&["true"]), None).await;
        assert_eq!((r.exit_code, r.error.as_str()), (0, ""));
    }

    #[tokio::test]
    async fn env_and_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let mut q = req(&["sh", "-c", "echo $GREETING; pwd -P"]);
        q.env_vars.insert("GREETING".into(), "hi".into());
        q.cwd = dir.path().to_string_lossy().into_owned();
        let r = run(&q, None).await;
        let canon = std::fs::canonicalize(dir.path()).unwrap();
        assert_eq!(r.stdout, format!("hi\n{}\n", canon.display()));
    }

    #[tokio::test]
    async fn missing_program() {
        let r = run(&req(&["definitely-not-a-real-program-xyz"]), None).await;
        assert_eq!(r.exit_code, -1);
        assert!(r.error.starts_with("starting "), "{}", r.error);
    }

    #[tokio::test]
    async fn timeout_kills() {
        let start = std::time::Instant::now();
        let r = run(&req(&["sleep", "5"]), Some(Duration::from_millis(100))).await;
        assert!(start.elapsed() < Duration::from_secs(3));
        assert_eq!(r.exit_code, -1);
        assert!(r.error.contains("timeout"));
    }
}

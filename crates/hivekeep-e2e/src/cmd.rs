//! Running external commands (kubectl, mostly) from a test, with their
//! output streamed indented and colored under the test's log.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::harness::T;
use crate::pretty::{ColorWriter, IndentWriter, ANSI_CYAN, ANSI_RED};

/// Walks up from `start` to the workspace root: the first directory whose
/// `Cargo.toml` declares `[workspace]`.
pub fn find_repo_root_from(start: &Path) -> Result<PathBuf, String> {
    let mut dir = start.to_path_buf();
    loop {
        let manifest = dir.join("Cargo.toml");
        if let Ok(text) = std::fs::read_to_string(&manifest) {
            if text.lines().any(|l| l.trim() == "[workspace]") {
                return Ok(dir);
            }
        }
        if !dir.pop() {
            return Err("workspace root (a Cargo.toml with [workspace]) not found".into());
        }
    }
}

/// The workspace root above this crate.
pub fn find_repo_root() -> Result<PathBuf, String> {
    find_repo_root_from(Path::new(env!("CARGO_MANIFEST_DIR")))
}

const INDENT: &str = "        ";

fn stream(
    reader: impl Read + Send + 'static,
    ansi: &'static str,
    to_stderr: bool,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let reader = BufReader::new(reader);
        for line in reader.lines().map_while(Result::ok) {
            let line = format!("{line}\n");
            if to_stderr {
                let mut w = IndentWriter::new(ColorWriter::new(std::io::stderr(), ansi), INDENT);
                let _ = w.write_all(line.as_bytes());
            } else {
                let mut w = IndentWriter::new(ColorWriter::new(std::io::stdout(), ansi), INDENT);
                let _ = w.write_all(line.as_bytes());
            }
        }
    })
}

fn render(name: &str, args: &[String]) -> String {
    std::iter::once(name.to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Runs `name args...` with `env` added to the process environment,
/// streaming stdout and stderr, and fails `t` if it fails.
pub fn run_cmd_with_env(t: &T, env: &[(&str, &str)], name: &str, args: &[String]) {
    t.log(format!("Running command: {}", render(name, args)));
    let mut cmd = Command::new(name);
    cmd.args(args)
        .envs(env.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .unwrap_or_else(|e| t.fatal(format!("Command failed: {}: {e}", render(name, args))));
    let out = stream(child.stdout.take().expect("piped"), ANSI_CYAN, false);
    let err = stream(child.stderr.take().expect("piped"), ANSI_RED, true);
    let status = child.wait();
    let _ = out.join();
    let _ = err.join();
    match status {
        Ok(s) if s.success() => {}
        Ok(s) => t.fatal(format!("Command failed: {}: {s}", render(name, args))),
        Err(e) => t.fatal(format!("Command failed: {}: {e}", render(name, args))),
    }
}

/// Runs `name args...`, streaming its output, and fails `t` if it fails.
pub fn run_cmd(t: &T, name: &str, args: &[String]) {
    run_cmd_with_env(t, &[], name, args)
}

/// Runs `name args...` with `env` added, streaming stderr, and returns its
/// stdout. Fails `t` if it fails.
pub fn run_cmd_output(t: &T, env: &[(&str, &str)], name: &str, args: &[String]) -> Vec<u8> {
    t.log(format!("Running command: {}", render(name, args)));
    let mut cmd = Command::new(name);
    cmd.args(args)
        .envs(env.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .unwrap_or_else(|e| t.fatal(format!("Command failed: {}: {e}", render(name, args))));
    let err = stream(child.stderr.take().expect("piped"), ANSI_RED, true);
    let mut stdout = Vec::new();
    let _ = child.stdout.take().expect("piped").read_to_end(&mut stdout);
    let status = child.wait();
    let _ = err.join();
    match status {
        Ok(s) if s.success() => stdout,
        Ok(s) => t.fatal(format!("Command failed: {}: {s}", render(name, args))),
        Err(e) => t.fatal(format!("Command failed: {}: {e}", render(name, args))),
    }
}

/// Runs kubectl against the cluster under test (its kubeconfig and
/// context), failing `t` if it fails.
pub fn kubectl(t: &T, args: &[&str]) {
    let mut all = crate::settings::kubectl_context_args();
    all.extend(args.iter().map(|s| s.to_string()));
    run_cmd(t, "kubectl", &all)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repo_root_is_the_workspace() {
        let root = find_repo_root().unwrap();
        assert!(
            root.join("crates/hivekeep-e2e/Cargo.toml").exists(),
            "{}",
            root.display()
        );
        let dir = tempfile::tempdir().unwrap();
        assert!(
            find_repo_root_from(dir.path()).is_err()
                || find_repo_root_from(dir.path()).unwrap() != root
        );
    }

    #[tokio::test]
    async fn commands_report_failure() {
        use crate::harness::{run_outcome, Outcome};
        let ok = run_outcome("Ok", |t| async move {
            assert_eq!(
                run_cmd_output(
                    &t,
                    &[("HK_E2E_X", "1")],
                    "sh",
                    &["-c".into(), "printf $HK_E2E_X".into()]
                ),
                b"1"
            );
            run_cmd(&t, "true", &[]);
        })
        .await;
        assert_eq!(ok, Outcome::Passed);
        let bad = run_outcome("Bad", |t| async move { run_cmd(&t, "false", &[]) }).await;
        assert!(matches!(bad, Outcome::Failed(_)));
    }
}

//! Running `gcloud`, behind a trait so the provisioning logic can be tested
//! against a recording fake.

use super::GcpError;
use parking_lot::Mutex;
use std::process::Command;
use std::sync::Arc;

/// What a command printed, and whether it succeeded.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Output {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

impl Output {
    pub fn ok(stdout: impl Into<String>) -> Self {
        Self {
            success: true,
            stdout: stdout.into(),
            stderr: String::new(),
        }
    }

    pub fn fail(stderr: impl Into<String>) -> Self {
        Self {
            success: false,
            stdout: String::new(),
            stderr: stderr.into(),
        }
    }

    /// Whether the failure is the API's "not found".
    pub fn is_not_found(&self) -> bool {
        let e = self.stderr.to_lowercase();
        !self.success
            && (e.contains("not_found")
                || e.contains("not found")
                || e.contains("404")
                || e.contains("does not exist"))
    }
}

/// Runs a program with arguments and collects its output. A non-zero exit is
/// an [`Output`] with `success` false, not an error; an error means the
/// program could not be run at all.
pub trait Runner: Send + Sync {
    fn run(&self, program: &str, args: &[String]) -> Result<Output, GcpError>;
}

/// Runs real processes.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessRunner;

impl Runner for ProcessRunner {
    fn run(&self, program: &str, args: &[String]) -> Result<Output, GcpError> {
        let out = Command::new(program)
            .args(args)
            .output()
            .map_err(|e| GcpError::Other(format!("running {program}: {e}")))?;
        Ok(Output {
            success: out.status.success(),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }
}

type Responder = dyn Fn(&[String]) -> Option<Output> + Send + Sync;
type Responders = Vec<(Vec<String>, Arc<Responder>)>;

/// Records every invocation and answers from registered responders: the first
/// responder whose argument prefix matches wins; unmatched invocations
/// succeed with empty output.
#[derive(Clone, Default)]
pub struct FakeRunner {
    calls: Arc<Mutex<Vec<Vec<String>>>>,
    responders: Arc<Mutex<Responders>>,
}

impl FakeRunner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Answers invocations whose arguments start with `prefix` with `out`.
    pub fn respond(&self, prefix: &[&str], out: Output) {
        self.respond_with(prefix, move |_| Some(out.clone()));
    }

    /// Answers invocations whose arguments start with `prefix` with `f`; a
    /// `None` passes the call on to later responders.
    pub fn respond_with(
        &self,
        prefix: &[&str],
        f: impl Fn(&[String]) -> Option<Output> + Send + Sync + 'static,
    ) {
        self.responders
            .lock()
            .push((prefix.iter().map(|s| s.to_string()).collect(), Arc::new(f)));
    }

    /// Every invocation, program first.
    pub fn calls(&self) -> Vec<Vec<String>> {
        self.calls.lock().clone()
    }

    /// The invocations joined with spaces, for readable assertions.
    pub fn command_lines(&self) -> Vec<String> {
        self.calls().iter().map(|c| c.join(" ")).collect()
    }
}

impl Runner for FakeRunner {
    fn run(&self, program: &str, args: &[String]) -> Result<Output, GcpError> {
        let mut full = vec![program.to_string()];
        full.extend(args.iter().cloned());
        self.calls.lock().push(full);
        let responders = self.responders.lock().clone();
        for (prefix, f) in responders {
            if args.len() >= prefix.len() && args.iter().zip(&prefix).all(|(a, p)| a == p) {
                if let Some(out) = f(args) {
                    return Ok(out);
                }
            }
        }
        Ok(Output::ok(""))
    }
}

/// Helpers over a runner for `gcloud`.
pub struct Gcloud<'a> {
    pub runner: &'a dyn Runner,
}

impl Gcloud<'_> {
    /// Runs gcloud, returning its output whatever the exit status.
    pub fn raw(&self, args: &[String]) -> Result<Output, GcpError> {
        self.runner.run("gcloud", args)
    }

    /// Runs gcloud and fails, naming `what`, on a non-zero exit.
    pub fn run(&self, what: &str, args: &[String]) -> Result<String, GcpError> {
        let out = self.raw(args)?;
        if !out.success {
            return Err(GcpError::Command {
                what: what.to_string(),
                stderr: out.stderr.trim().to_string(),
            });
        }
        Ok(out.stdout)
    }
}

/// Builds an argument vector from string slices.
pub fn args(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| s.to_string()).collect()
}

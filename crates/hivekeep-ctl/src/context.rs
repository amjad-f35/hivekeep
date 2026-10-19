//! What every command runs with: the global options, output streams, a
//! printer, a logger, and the means to reach the control plane and the
//! cluster.

use std::io::{Read, Write};

use crate::cli::GlobalArgs;
use crate::client::Api;
use crate::connection::{warn_anonymous, ConnectionConfig, TokenSource};
use crate::error::{status_text, CliError, Result};
use crate::io::{Io, Sink};
use crate::kubectl::Kubectl;
use crate::output::Printer;
use crate::verbosity::Logger;

/// The state a command runs against.
pub struct Ctx {
    /// The global options.
    pub global: GlobalArgs,
    /// Renders results in the `-o` format.
    pub printer: Printer,
    /// `-v` diagnostics.
    pub log: Logger,
    /// Output streams and stdin.
    pub io: Io,
}

impl Ctx {
    /// A context for `global` over `io`.
    pub fn new(global: GlobalArgs, io: Io) -> Self {
        Ctx {
            printer: Printer::new(global.output),
            log: Logger::new(global.verbosity(), io.err.clone()),
            global,
            io,
        }
    }

    /// The command-output stream.
    pub fn out(&self) -> Sink {
        self.io.out.clone()
    }

    /// The diagnostics stream.
    pub fn err(&self) -> Sink {
        self.io.err.clone()
    }

    /// Writes `line` to the command-output stream.
    pub fn println(&self, line: &str) -> Result<()> {
        let mut out = self.out();
        writeln!(out, "{line}")?;
        Ok(())
    }

    /// The connection settings from the global options. Reads the token
    /// from stdin when `--token-file -` was given.
    pub fn connection_config(&mut self) -> Result<ConnectionConfig> {
        let token = TokenSource::from_flag(self.global.token_file.as_deref(), &mut self.io.stdin)?;
        Ok(ConnectionConfig {
            endpoint: self.global.endpoint.clone(),
            ca_file: self.global.ca_file.clone(),
            server_name: self.global.tls_server_name.clone(),
            plaintext: self.global.plaintext,
            token,
            preview: self.global.preview,
            trace: self.global.trace,
        })
    }

    /// Connects to `hivekeep-api`.
    pub async fn api(&mut self) -> Result<Api> {
        let cfg = self.connection_config()?;
        // Settle a bad endpoint before saying anything about credentials.
        cfg.tonic_endpoint()?;
        warn_anonymous(&self.io.err, &cfg.token);
        let channel = cfg.connect(&self.log).await?;
        Ok(Api::new(
            channel,
            cfg.decorator(&self.io.err),
            self.log.clone(),
        ))
    }

    /// Runs `kubectl` against the configured kubeconfig and context.
    pub fn kubectl(&self) -> Kubectl {
        Kubectl::new(
            self.global.kubeconfig.clone(),
            self.global.kube_context.clone(),
            self.log.clone(),
        )
    }

    /// Reads a manifest from `path`, or from stdin when it is `-`.
    pub fn read_manifest(&mut self, path: &str) -> Result<String> {
        if path == "-" {
            let mut s = String::new();
            self.io
                .stdin
                .read_to_string(&mut s)
                .map_err(|e| CliError::failed(format!("read manifest from stdin: {e}")))?;
            return Ok(s);
        }
        std::fs::read_to_string(path).map_err(|e| CliError::failed(format!("open {path}: {e}")))
    }
}

/// A failed RPC, as `"{context}: rpc error: ..."`.
pub fn rpc_error(context: impl std::fmt::Display, status: &tonic::Status) -> CliError {
    CliError::failed(format!("{context}: {}", status_text(status)))
}

/// Checks the scope flags of a `get` for a hive-scoped resource before
/// connecting: by name needs `-H` and forbids `-A`; a list needs exactly one
/// of them. Returns the hive to use, empty for all hives.
pub fn hive_scope(
    resource_plural: &str,
    names: &[String],
    hive: Option<&str>,
    all_hives: bool,
) -> Result<String> {
    let hive = hive.unwrap_or_default();
    if !names.is_empty() {
        if all_hives {
            return Err(CliError::usage(format!(
                "-A/--all-hives cannot be used when getting {resource_plural} by name"
            )));
        }
        if hive.is_empty() {
            return Err(CliError::usage(format!(
                "-H/--hive is required when getting {resource_plural} by name"
            )));
        }
        return Ok(hive.to_string());
    }
    if all_hives && !hive.is_empty() {
        return Err(CliError::usage(
            "-H/--hive and -A/--all-hives are mutually exclusive",
        ));
    }
    if !all_hives && hive.is_empty() {
        return Err(CliError::usage(
            "-H/--hive <name> or -A/--all-hives is required",
        ));
    }
    Ok(hive.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes() {
        let names = ["counter".to_string()];
        for (names, hive, all, want) in [
            (
                &names[..],
                None,
                false,
                Err("-H/--hive is required when getting"),
            ),
            (
                &names[..],
                None,
                true,
                Err("-A/--all-hives cannot be used when getting"),
            ),
            (
                &names[..],
                Some("demo"),
                true,
                Err("-A/--all-hives cannot be used when getting"),
            ),
            (
                &[][..],
                None,
                false,
                Err("-H/--hive <name> or -A/--all-hives is required"),
            ),
            (
                &[][..],
                Some("demo"),
                true,
                Err("-H/--hive and -A/--all-hives are mutually exclusive"),
            ),
            (&names[..], Some("demo"), false, Ok("demo")),
            (&[][..], Some("demo"), false, Ok("demo")),
            (&[][..], None, true, Ok("")),
        ] {
            match (hive_scope("actors", names, hive, all), want) {
                (Ok(got), Ok(w)) => assert_eq!(got, w),
                (Err(e), Err(w)) => {
                    assert!(e.is_usage());
                    assert!(e.message().contains(w), "{e} vs {w}");
                }
                (got, want) => panic!("{names:?} {hive:?} {all}: got {got:?}, want {want:?}"),
            }
        }
    }
}

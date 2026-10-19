//! The CLI's error type.
//!
//! Errors fall in two classes: a usage error means the invocation itself was
//! wrong (a bad flag combination, a malformed reference) and is reported with
//! a pointer to `--help`; any other failure is reported as is.

use std::fmt;

/// A failed command.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CliError {
    /// The command line was invalid.
    #[error("{0}")]
    Usage(String),
    /// The command was valid but failed.
    #[error("{0}")]
    Failed(String),
}

/// Result alias for CLI operations.
pub type Result<T, E = CliError> = std::result::Result<T, E>;

impl CliError {
    /// A usage error with `msg`.
    pub fn usage(msg: impl fmt::Display) -> Self {
        CliError::Usage(msg.to_string())
    }

    /// A runtime failure with `msg`.
    pub fn failed(msg: impl fmt::Display) -> Self {
        CliError::Failed(msg.to_string())
    }

    /// Whether this is a usage error.
    pub fn is_usage(&self) -> bool {
        matches!(self, CliError::Usage(_))
    }

    /// The error message.
    pub fn message(&self) -> &str {
        match self {
            CliError::Usage(m) | CliError::Failed(m) => m,
        }
    }

    /// Re-classifies a failure as a usage error, keeping its message.
    pub fn into_usage(self) -> Self {
        CliError::Usage(self.message().to_string())
    }
}

/// Adds a `context: ` prefix to errors, as a failure.
pub trait Context<T> {
    /// Wraps the error as `"{context}: {err}"`.
    fn context(self, context: impl fmt::Display) -> Result<T>;
}

impl<T, E: fmt::Display> Context<T> for std::result::Result<T, E> {
    fn context(self, context: impl fmt::Display) -> Result<T> {
        self.map_err(|e| CliError::Failed(format!("{context}: {e}")))
    }
}

impl From<std::io::Error> for CliError {
    fn from(e: std::io::Error) -> Self {
        CliError::Failed(e.to_string())
    }
}

/// Renders a gRPC status the way the CLI reports it: code and message.
pub fn status_text(status: &tonic::Status) -> String {
    format!(
        "rpc error: code = {:?} desc = {}",
        status.code(),
        status.message()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_prefixes_failure() {
        let r: std::result::Result<(), &str> = Err("boom");
        assert_eq!(
            r.context("failed to x").unwrap_err(),
            CliError::Failed("failed to x: boom".into())
        );
    }

    #[test]
    fn into_usage_keeps_message() {
        let e = CliError::failed("bad").into_usage();
        assert!(e.is_usage());
        assert_eq!(e.message(), "bad");
    }

    #[test]
    fn status_text_names_code() {
        let s = tonic::Status::not_found("nope");
        assert_eq!(status_text(&s), "rpc error: code = NotFound desc = nope");
    }
}

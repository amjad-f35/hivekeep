//! The worker's error type and its mapping onto gRPC status codes.

use std::io;

/// An error from a worker operation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error that already carries its gRPC status.
    #[error("{}", .0.message())]
    Status(tonic::Status),
    /// A filesystem or process error.
    #[error("{context}: {source}")]
    Io {
        /// What was being done.
        context: String,
        /// The cause.
        #[source]
        source: io::Error,
    },
    /// Any other failure.
    #[error("{0}")]
    Msg(String),
    /// Another error with what was being done when it happened.
    #[error("{context}: {source}")]
    Context {
        /// What was being done.
        context: String,
        /// The cause.
        #[source]
        source: Box<Error>,
    },
    /// Several independent failures.
    #[error("{}", join(.0))]
    Many(Vec<Error>),
    /// The operation was cancelled.
    #[error("context canceled")]
    Cancelled,
    /// The operation ran out of time.
    #[error("context deadline exceeded")]
    DeadlineExceeded,
}

fn join(errs: &[Error]) -> String {
    errs.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

/// The result of a worker operation.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// A plain message.
    pub fn msg(m: impl Into<String>) -> Self {
        Error::Msg(m.into())
    }

    /// An I/O error with context.
    pub fn io(context: impl Into<String>, source: io::Error) -> Self {
        Error::Io {
            context: context.into(),
            source,
        }
    }

    /// Joins errors, `None` when there are none.
    pub fn join(mut errs: Vec<Error>) -> Option<Error> {
        match errs.len() {
            0 => None,
            1 => errs.pop(),
            _ => Some(Error::Many(errs)),
        }
    }

    /// The gRPC code this error answers with: the code of the innermost
    /// status, cancellation and deadlines as themselves, otherwise INTERNAL.
    pub fn code(&self) -> tonic::Code {
        match self {
            Error::Status(s) => s.code(),
            Error::Context { source, .. } => source.code(),
            Error::Cancelled => tonic::Code::Cancelled,
            Error::DeadlineExceeded => tonic::Code::DeadlineExceeded,
            Error::Many(errs) => errs.first().map_or(tonic::Code::Internal, Error::code),
            Error::Io { .. } | Error::Msg(_) => tonic::Code::Internal,
        }
    }

    /// Whether the error is, or wraps, a cancellation or deadline.
    pub fn is_context_error(&self) -> bool {
        matches!(
            self.code(),
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded
        )
    }
}

impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self {
        Error::Status(s)
    }
}

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        match e {
            Error::Status(s) => s,
            other => tonic::Status::new(other.code(), other.to_string()),
        }
    }
}

/// Adds context to errors.
pub trait Context<T> {
    /// Wraps the error with `context`.
    fn context(self, context: impl Into<String>) -> Result<T>;
    /// Wraps the error with a lazily built context.
    fn with_context<F: FnOnce() -> String>(self, f: F) -> Result<T>;
}

impl<T> Context<T> for Result<T> {
    fn context(self, context: impl Into<String>) -> Result<T> {
        self.map_err(|e| Error::Context {
            context: context.into(),
            source: Box::new(e),
        })
    }

    fn with_context<F: FnOnce() -> String>(self, f: F) -> Result<T> {
        self.map_err(|e| Error::Context {
            context: f(),
            source: Box::new(e),
        })
    }
}

impl<T> Context<T> for std::result::Result<T, io::Error> {
    fn context(self, context: impl Into<String>) -> Result<T> {
        self.map_err(|e| Error::io(context, e))
    }

    fn with_context<F: FnOnce() -> String>(self, f: F) -> Result<T> {
        self.map_err(|e| Error::io(f(), e))
    }
}

/// Shorthand for the gRPC statuses handlers return.
pub mod status {
    /// UNAVAILABLE.
    pub fn unavailable(m: impl Into<String>) -> tonic::Status {
        tonic::Status::unavailable(m)
    }
    /// NOT_FOUND.
    pub fn not_found(m: impl Into<String>) -> tonic::Status {
        tonic::Status::not_found(m)
    }
    /// FAILED_PRECONDITION.
    pub fn failed_precondition(m: impl Into<String>) -> tonic::Status {
        tonic::Status::failed_precondition(m)
    }
    /// RESOURCE_EXHAUSTED.
    pub fn resource_exhausted(m: impl Into<String>) -> tonic::Status {
        tonic::Status::resource_exhausted(m)
    }
    /// INTERNAL.
    pub fn internal(m: impl Into<String>) -> tonic::Status {
        tonic::Status::internal(m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_keeps_the_inner_code() {
        let e: Result<()> = Err(tonic::Status::unavailable("busy").into());
        let e = e.context("while starting").unwrap_err();
        assert_eq!(e.code(), tonic::Code::Unavailable);
        assert_eq!(e.to_string(), "while starting: busy");
        let s: tonic::Status = e.into();
        assert_eq!(s.code(), tonic::Code::Unavailable);
        assert_eq!(s.message(), "while starting: busy");
        assert_eq!(Error::msg("x").code(), tonic::Code::Internal);
        assert!(Error::Cancelled.is_context_error());
    }
}

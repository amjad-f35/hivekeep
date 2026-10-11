//! Storage errors and what of a service's response is safe to log.

use std::fmt;
use std::io;

/// A storage service's failed response in terms its backend judged safe to
/// log: never a URI, header or credential.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServiceError {
    /// The response's HTTP status.
    pub http_status: u16,
    /// The service's error code, such as `PreconditionFailed`, if the backend
    /// reports one.
    pub code: String,
    /// The service's own description, if the backend reports one it knows
    /// names no caller, resource or credential.
    pub message: String,
}

/// Which API a failed response came from, with what it said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceDetail {
    /// GCS JSON API: the first error item's reason. Its message quotes the
    /// caller and resource, so it is not kept.
    GcsJson {
        /// The `errors[0].reason`.
        reason: String,
    },
    /// GCS XML API: the error document's generic code and message. Its
    /// `Details` element names the caller and is not kept.
    GcsXml {
        /// `<Code>`.
        code: String,
        /// `<Message>`.
        message: String,
    },
    /// S3: the error code. Only the HTTP status is described.
    S3 {
        /// `<Code>`, or empty when the response had no error document.
        code: String,
    },
    /// Any other service: only the status is known.
    Other,
}

/// A service's failed response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceResponse {
    /// The HTTP status.
    pub status: u16,
    /// What the response said.
    pub detail: ServiceDetail,
}

impl ServiceResponse {
    /// What may be logged of the response.
    pub fn describe(&self) -> ServiceError {
        let mut desc = ServiceError {
            http_status: self.status,
            ..Default::default()
        };
        match &self.detail {
            ServiceDetail::GcsJson { reason } => desc.code = reason.clone(),
            ServiceDetail::GcsXml { code, message } => {
                desc.code = code.clone();
                desc.message = message.clone();
            }
            ServiceDetail::S3 { .. } | ServiceDetail::Other => {}
        }
        desc
    }
}

/// What kind of failure an [`Error`] is.
#[derive(Debug, Clone)]
pub enum ErrorKind {
    /// The object or its bucket is absent, as opposed to a transient fetch
    /// failure.
    NotFound,
    /// A listing found the bucket itself missing.
    BucketNotFound,
    /// A prefix copy found no objects to copy.
    EmptySource,
    /// The caller gave up.
    Canceled,
    /// The caller's deadline passed.
    DeadlineExceeded,
    /// The storage service refused or failed the request.
    Service(ServiceResponse),
    /// A gRPC status, from a credential source, passed through as it is.
    Status(tonic::Status),
    /// A local I/O failure.
    Io(io::ErrorKind),
    /// The request never got a response.
    Transport,
    /// Anything else.
    Other,
}

/// A storage error: a kind and a message carrying its context.
#[derive(Clone)]
pub struct Error {
    kind: ErrorKind,
    message: String,
}

/// A storage result.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// An error of `kind`.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Error {
            kind,
            message: message.into(),
        }
    }

    /// An error of kind [`ErrorKind::Other`].
    pub fn other(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Other, message)
    }

    /// A not-found error.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
    }

    /// A canceled error.
    pub fn canceled() -> Self {
        Self::new(ErrorKind::Canceled, "context canceled")
    }

    /// A deadline error.
    pub fn deadline_exceeded() -> Self {
        Self::new(ErrorKind::DeadlineExceeded, "context deadline exceeded")
    }

    /// A service response error.
    pub fn service(resp: ServiceResponse, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Service(resp), message)
    }

    /// The kind.
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    /// The message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Prefixes the message with `ctx`, keeping the kind.
    pub fn context(mut self, ctx: impl fmt::Display) -> Self {
        self.message = format!("{ctx}: {}", self.message);
        self
    }

    /// Whether the object or bucket is absent.
    pub fn is_not_found(&self) -> bool {
        matches!(self.kind, ErrorKind::NotFound)
    }

    /// The service's HTTP status, if a service answered.
    pub fn http_status(&self) -> Option<u16> {
        match &self.kind {
            ErrorKind::Service(r) => Some(r.status),
            _ => None,
        }
    }

    /// The S3 error code, if S3 answered with one.
    pub fn s3_code(&self) -> Option<&str> {
        match &self.kind {
            ErrorKind::Service(ServiceResponse {
                detail: ServiceDetail::S3 { code },
                ..
            }) if !code.is_empty() => Some(code),
            _ => None,
        }
    }

    /// What of the service's response is safe to log, if a service answered.
    pub fn describe(&self) -> Option<ServiceError> {
        match &self.kind {
            ErrorKind::Service(r) => Some(r.describe()),
            _ => None,
        }
    }

    /// A short name for the kind, for logs that must not carry the message.
    pub fn kind_name(&self) -> &'static str {
        match &self.kind {
            ErrorKind::NotFound => "not_found",
            ErrorKind::BucketNotFound => "bucket_not_found",
            ErrorKind::EmptySource => "empty_source",
            ErrorKind::Canceled => "canceled",
            ErrorKind::DeadlineExceeded => "deadline_exceeded",
            ErrorKind::Service(_) => "service",
            ErrorKind::Status(_) => "status",
            ErrorKind::Io(_) => "io",
            ErrorKind::Transport => "transport",
            ErrorKind::Other => "other",
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message, self.kind_name())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        // An I/O error may carry a storage error that crossed a sync bridge.
        if let Some(inner) = e.get_ref().and_then(|i| i.downcast_ref::<Error>()) {
            return inner.clone();
        }
        Error::new(ErrorKind::Io(e.kind()), e.to_string())
    }
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        let kind = match e.kind {
            ErrorKind::Io(k) => k,
            ErrorKind::NotFound => io::ErrorKind::NotFound,
            _ => io::ErrorKind::Other,
        };
        io::Error::new(kind, e)
    }
}

impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self {
        let message = s.message().to_string();
        Error::new(ErrorKind::Status(s), message)
    }
}

impl From<crate::http::TransportError> for Error {
    fn from(e: crate::http::TransportError) -> Self {
        Error::new(ErrorKind::Transport, e.to_string())
    }
}

/// Describes the storage service response `err` carries, if any.
pub fn describe_service_error(err: &Error) -> Option<ServiceError> {
    err.describe()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_keeps_kind_and_io_roundtrips() {
        let e = Error::not_found("gone").context("while reading");
        assert!(e.is_not_found());
        assert_eq!(e.to_string(), "while reading: gone");
        let io: io::Error = e.clone().into();
        let back: Error = io.into();
        assert!(back.is_not_found());
    }
}

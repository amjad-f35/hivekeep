//! How a handler's error becomes the status a caller sees.
//!
//! A handler signals a deliberate API outcome with [`ApiError`] (or a
//! validation [`ValidationError`]); anything else, including a
//! [`tonic::Status`] received from an upstream service, is an internal
//! failure. The marker is found anywhere in the error's source chain, so a
//! handler may add context around it. The outermost [`ApiError`] wins over
//! anything it wraps.

use std::error::Error;
use std::fmt;

use hivekeep_core::field::ValidationError;
use tonic::{Code, Status};

/// A boxed handler error.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// An error meant to reach the caller with its own code and message.
#[derive(Debug)]
pub struct ApiError {
    code: Code,
    message: String,
    source: Option<BoxError>,
}

impl ApiError {
    /// An error with `code` and `message`.
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        ApiError {
            code,
            message: message.into(),
            source: None,
        }
    }

    /// Keeps `source` as the cause, for logs; the caller sees only this
    /// error's own message.
    pub fn with_source(mut self, source: impl Into<BoxError>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// The code the caller sees.
    pub fn code(&self) -> Code {
        self.code
    }

    /// `NotFound`.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(Code::NotFound, message)
    }

    /// `InvalidArgument`.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(Code::InvalidArgument, message)
    }

    /// `FailedPrecondition`.
    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self::new(Code::FailedPrecondition, message)
    }

    /// `PermissionDenied`.
    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self::new(Code::PermissionDenied, message)
    }

    /// `Internal`, with a message the caller may see.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(Code::Internal, message)
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for ApiError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn Error + 'static))
    }
}

/// A request's own cancellation or deadline, which keeps its code through
/// any context a handler adds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ContextError {
    /// The caller went away.
    #[error("context canceled")]
    Canceled,
    /// The request ran out of time.
    #[error("context deadline exceeded")]
    DeadlineExceeded,
}

impl From<tokio::time::error::Elapsed> for ContextError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        ContextError::DeadlineExceeded
    }
}

/// An error with added context, rendering as `context: source`.
#[derive(Debug)]
pub struct Wrapped {
    context: String,
    source: BoxError,
}

/// Adds `context` in front of `err`.
pub fn wrap(context: impl Into<String>, err: impl Into<BoxError>) -> BoxError {
    Box::new(Wrapped {
        context: context.into(),
        source: err.into(),
    })
}

impl fmt::Display for Wrapped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.context)
    }
}

impl Error for Wrapped {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&*self.source)
    }
}

/// The error and every source below it, joined with `: `.
pub fn chain_text(err: &(dyn Error + 'static)) -> String {
    let mut out = err.to_string();
    let mut cur = err.source();
    while let Some(e) = cur {
        let s = e.to_string();
        if !s.is_empty() && !out.ends_with(&s) {
            out.push_str(": ");
            out.push_str(&s);
        }
        cur = e.source();
    }
    out
}

/// The status for `err`, and whether it was a deliberate API outcome. An
/// [`ApiError`] keeps its code and message; a [`ValidationError`] is
/// `InvalidArgument`; a [`ContextError`] or an elapsed timeout keeps its
/// code with the full message; anything else is `Internal` with the full
/// message and `false`.
pub fn from_error(err: &(dyn Error + 'static)) -> (Status, bool) {
    let mut cur: Option<&(dyn Error + 'static)> = Some(err);
    while let Some(e) = cur {
        if let Some(api) = e.downcast_ref::<ApiError>() {
            return (Status::new(api.code, api.message.clone()), true);
        }
        if let Some(v) = e.downcast_ref::<ValidationError>() {
            return (Status::invalid_argument(v.to_string()), true);
        }
        let ctx = e.downcast_ref::<ContextError>().copied().or_else(|| {
            e.downcast_ref::<tokio::time::error::Elapsed>()
                .map(|_| ContextError::DeadlineExceeded)
        });
        if let Some(ctx) = ctx {
            let code = match ctx {
                ContextError::Canceled => Code::Cancelled,
                ContextError::DeadlineExceeded => Code::DeadlineExceeded,
            };
            return (Status::new(code, chain_text(err)), true);
        }
        cur = e.source();
    }
    (Status::internal(chain_text(err)), false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_text_joins_sources() {
        let e = wrap("outer", wrap("middle", "inner"));
        assert_eq!(chain_text(&*e), "outer: middle: inner");
    }

    #[test]
    fn outermost_api_error_wins() {
        let e = wrap(
            "ctx",
            ApiError::internal("while calling keeper: down").with_source(ApiError::not_found("x")),
        );
        let (st, ok) = from_error(&*e);
        assert!(ok);
        assert_eq!(st.code(), Code::Internal);
        assert_eq!(st.message(), "while calling keeper: down");
    }

    #[test]
    fn validation_errors_are_invalid_argument() {
        use hivekeep_core::field::{ErrorList, FieldError, Path};
        let v = ErrorList::from(FieldError::required(&Path::new(["a"]), ""))
            .into_result()
            .unwrap_err();
        let (st, ok) = from_error(&v);
        assert!(ok);
        assert_eq!(st.code(), Code::InvalidArgument);
    }
}

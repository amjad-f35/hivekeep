//! The image cache's error type.
//!
//! Callers branch on a few failure classes (a layer that lies about its
//! content, a registry status, a caller that gave up, an eviction pass that
//! could not see everything) while the message carries the context chain.
//! An [`Error`] is cheap to clone, so concurrent waiters on one pull can all
//! receive it.

use std::fmt;
use std::io;
use std::sync::Arc;

use hivekeep_node::context::CtxError;

/// A failure class callers can test for with [`Error::is`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Layer content does not hash to the diffID the image config claims.
    /// It is a property of the image, so retrying cannot fix it.
    DiffIdMismatch,
    /// A registry answered with this HTTP status.
    Registry(u16),
    /// The caller's context was cancelled.
    Canceled,
    /// The caller's (or the pull's) deadline passed.
    DeadlineExceeded,
    /// The image reference does not parse.
    InvalidReference,
    /// An eviction pass did nothing because the image records or bundle
    /// specs could not all be read.
    IncompleteEnumeration,
}

/// An image cache error: a message plus the failure classes it carries.
#[derive(Clone)]
pub struct Error {
    msg: Arc<str>,
    kinds: Arc<[ErrorKind]>,
}

impl Error {
    /// An error with no class.
    pub fn msg(msg: impl fmt::Display) -> Self {
        Error {
            msg: msg.to_string().into(),
            kinds: Arc::new([]),
        }
    }

    /// An error of class `kind`.
    pub fn new(kind: ErrorKind, msg: impl fmt::Display) -> Self {
        Error {
            msg: msg.to_string().into(),
            kinds: Arc::new([kind]),
        }
    }

    /// The same error with `ctx` prefixed to its message.
    pub fn context(self, ctx: impl fmt::Display) -> Self {
        Error {
            msg: format!("{ctx}: {}", self.msg).into(),
            kinds: self.kinds,
        }
    }

    /// Reports whether this error carries `kind`.
    pub fn is(&self, kind: ErrorKind) -> bool {
        self.kinds.contains(&kind)
    }

    /// The first registry status this error carries.
    pub fn registry_status(&self) -> Option<u16> {
        self.kinds.iter().find_map(|k| match k {
            ErrorKind::Registry(s) => Some(*s),
            _ => None,
        })
    }

    /// Joins several errors into one carrying every class, messages on
    /// separate lines. `None` when `errs` is empty.
    pub fn join(errs: impl IntoIterator<Item = Error>) -> Option<Error> {
        let errs: Vec<Error> = errs.into_iter().collect();
        match errs.len() {
            0 => None,
            1 => errs.into_iter().next(),
            _ => {
                let msg = errs
                    .iter()
                    .map(|e| e.msg.as_ref())
                    .collect::<Vec<_>>()
                    .join("\n");
                let kinds: Vec<ErrorKind> =
                    errs.iter().flat_map(|e| e.kinds.iter().copied()).collect();
                Some(Error {
                    msg: msg.into(),
                    kinds: kinds.into(),
                })
            }
        }
    }

    /// An I/O error with `ctx` prefixed.
    pub fn io(ctx: impl fmt::Display, err: io::Error) -> Self {
        Error::msg(format!("{ctx}: {err}"))
    }
}

impl From<CtxError> for Error {
    fn from(e: CtxError) -> Self {
        match e {
            CtxError::Canceled => Error::new(ErrorKind::Canceled, e),
            CtxError::DeadlineExceeded => Error::new(ErrorKind::DeadlineExceeded, e),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} {:?}", self.msg, self.kinds)
    }
}

impl std::error::Error for Error {}

/// The crate's result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Attaches context to I/O results.
pub(crate) trait IoContext<T> {
    fn ctx(self, ctx: impl fmt::Display) -> Result<T>;
}

impl<T> IoContext<T> for io::Result<T> {
    fn ctx(self, ctx: impl fmt::Display) -> Result<T> {
        self.map_err(|e| Error::io(ctx, e))
    }
}

/// Attaches context to crate results.
pub(crate) trait ResultContext<T> {
    fn context(self, ctx: impl fmt::Display) -> Result<T>;
}

impl<T> ResultContext<T> for Result<T> {
    fn context(self, ctx: impl fmt::Display) -> Result<T> {
        self.map_err(|e| e.context(ctx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_keeps_kinds() {
        let e = Error::new(ErrorKind::Registry(404), "not found").context("in pull");
        assert_eq!(e.to_string(), "in pull: not found");
        assert_eq!(e.registry_status(), Some(404));
        let j = Error::join([Error::msg("a"), Error::new(ErrorKind::DiffIdMismatch, "b")]).unwrap();
        assert!(j.is(ErrorKind::DiffIdMismatch));
        assert_eq!(j.to_string(), "a\nb");
        assert!(Error::join([]).is_none());
    }
}

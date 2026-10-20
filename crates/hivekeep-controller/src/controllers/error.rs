//! The error a reconcile returns. Any error requeues the key with backoff.

/// A reconcile failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An API request failed.
    #[error("{context}: {source}")]
    Api {
        /// What was being done.
        context: String,
        /// The API error.
        #[source]
        source: Box<kube::Error>,
    },
    /// The controller refused to act, e.g. to take over an object it does
    /// not own.
    #[error("{0}")]
    Refused(String),
    /// An object the controller reads from is malformed or inconsistent.
    #[error("{0}")]
    Invalid(String),
}

impl Error {
    /// The API error underneath, if any.
    pub fn api_error(&self) -> Option<&kube::Error> {
        match self {
            Error::Api { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }

    /// Whether the API server rejected a write on a stale resourceVersion.
    pub fn is_conflict(&self) -> bool {
        self.api_error()
            .is_some_and(hivekeep_k8s::api_errors::is_conflict)
    }
}

/// Wraps a [`kube::Error`] with `context`.
pub(crate) fn api_err(context: impl Into<String>) -> impl FnOnce(kube::Error) -> Error {
    let context = context.into();
    move |source| Error::Api {
        context,
        source: Box::new(source),
    }
}

/// The result of a reconcile.
pub type Result<T, E = Error> = std::result::Result<T, E>;

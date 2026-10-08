//! Errors returned by every [`Store`](crate::Store) backend.

/// A boxed error carried through the store unchanged.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Result alias used throughout the store.
pub type Result<T, E = StoreError> = std::result::Result<T, E>;

/// Why a store operation failed.
///
/// The sentinel variants are part of the contract: callers branch on them
/// (for example, a [`VersionConflict`](Self::VersionConflict) is retryable, a
/// [`UidConflict`](Self::UidConflict) never is). Errors produced by a caller's
/// own `mutate` or `admit` closure travel back as [`Rejected`](Self::Rejected)
/// or as whatever variant the closure chose to return.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The addressed object does not exist.
    #[error("store: not found")]
    NotFound,

    /// An object with the same key already exists.
    #[error("store: already exists")]
    AlreadyExists,

    /// The write was guarded on a version the stored object is no longer at,
    /// or it lost a race with a concurrent write.
    #[error("store: version conflict")]
    VersionConflict,

    /// The object, or its parent, is not in the state the operation needs
    /// (a missing parent Hive, a non-empty Hive being deleted, ...).
    #[error("store: failed precondition")]
    FailedPrecondition,

    /// A lease is already held by another holder.
    #[error("store: lease conflict")]
    LeaseConflict,

    /// A list page token is malformed or was issued for another listing.
    #[error("store: invalid page token: {0}")]
    InvalidPageToken(String),

    /// A negative page size was supplied.
    #[error("store: invalid page size")]
    InvalidPageSize,

    /// The write was guarded on a uid the stored object does not carry: the
    /// name now addresses a different incarnation. Retrying cannot help.
    #[error("store: uid conflict")]
    UidConflict,

    /// An update was called without one of its required guards. The payload
    /// names the missing guard (`"uid"` or `"version"`).
    #[error("store: precondition required: {0}")]
    PreconditionRequired(&'static str),

    /// A mutation changed a field that is immutable for the object's lifetime.
    #[error("store: immutable field: {0}")]
    ImmutableField(String),

    /// The object handed to the store is malformed (for example, it carries a
    /// snapshot URI that does not follow the storage layout).
    #[error("store: invalid argument: {0}")]
    InvalidArgument(String),

    /// A caller-supplied `mutate` or `admit` closure refused the write. The
    /// closure's error is carried verbatim.
    #[error(transparent)]
    Rejected(BoxError),

    /// A stored record could not be decoded or is inconsistent with its key.
    #[error("store: corrupt record: {0}")]
    Corrupt(String),

    /// The backend could not be reached.
    #[error("store: backend unavailable: {0}")]
    Unavailable(String),

    /// Any other backend failure.
    #[error("store: backend error: {0}")]
    Backend(BoxError),
}

impl StoreError {
    /// Wraps a caller's error so it passes through the store unchanged.
    pub fn rejected(err: impl Into<BoxError>) -> Self {
        StoreError::Rejected(err.into())
    }

    /// Wraps a backend failure.
    pub fn backend(err: impl Into<BoxError>) -> Self {
        StoreError::Backend(err.into())
    }

    /// The caller error carried by [`Rejected`](Self::Rejected), if it is a `T`.
    pub fn rejection<T: std::error::Error + 'static>(&self) -> Option<&T> {
        match self {
            StoreError::Rejected(e) => e.downcast_ref::<T>(),
            _ => None,
        }
    }

    /// Whether this is [`NotFound`](Self::NotFound).
    pub fn is_not_found(&self) -> bool {
        matches!(self, StoreError::NotFound)
    }

    /// Whether a fresh read-modify-write may succeed where this one failed.
    pub fn is_retryable(&self) -> bool {
        matches!(self, StoreError::VersionConflict)
    }
}

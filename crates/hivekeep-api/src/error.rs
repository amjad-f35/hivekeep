//! The error type shared by the RPC handlers and the workflows behind them.
//!
//! Handlers answer with a [`tonic::Status`]; workflows need to tell a few
//! retryable conditions apart first (a lost version race, a worker that
//! filled up under us, an Actor busy with another operation), so they carry
//! [`Error`] and convert at the edge.

use hivekeep_store::StoreError;
use tonic::{Code, Status};

/// Result alias of this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why an operation failed.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A final answer for the caller.
    #[error("{}", .0.message())]
    Status(Status),
    /// A store failure not yet translated for the caller.
    #[error(transparent)]
    Store(StoreError),
    /// The worker the scheduler picked would not take the Actor once asked
    /// under its row lock. Retryable.
    #[error("picked worker no longer has room")]
    WorkerFilledUp,
    /// No worker has room for the Actor under its constraints.
    #[error("no worker has room for the actor")]
    NoWorkerRoom,
    /// Another operation holds the Actor's lease.
    #[error("actor has an operation in progress")]
    ActorBusy,
    /// A mark-for-deletion found the resource already marked.
    #[error("already marked for deletion")]
    AlreadyMarked,
    /// A keeper could not be reached because none runs on the node.
    #[error("no keeper found on node {0:?}")]
    NoKeeperOnNode(String),
}

impl Error {
    /// The gRPC code the caller would see.
    pub fn code(&self) -> Code {
        match self {
            Error::Status(s) => s.code(),
            Error::Store(e) => store_code(e),
            Error::WorkerFilledUp => Code::Aborted,
            Error::NoWorkerRoom => Code::ResourceExhausted,
            Error::ActorBusy => Code::Aborted,
            Error::AlreadyMarked => Code::Internal,
            Error::NoKeeperOnNode(_) => Code::Unavailable,
        }
    }

    /// Whether this is the store's version conflict.
    pub fn is_version_conflict(&self) -> bool {
        matches!(self, Error::Store(StoreError::VersionConflict))
    }

    /// Whether this is the store's uid conflict.
    pub fn is_uid_conflict(&self) -> bool {
        matches!(self, Error::Store(StoreError::UidConflict))
    }

    /// Whether this is the store's not-found.
    pub fn is_store_not_found(&self) -> bool {
        matches!(self, Error::Store(StoreError::NotFound))
    }

    /// Whether this is the store's lease conflict.
    pub fn is_lease_conflict(&self) -> bool {
        matches!(self, Error::Store(StoreError::LeaseConflict))
    }

    /// Wraps the message with context, keeping the code.
    pub fn context(self, what: impl std::fmt::Display) -> Error {
        match self {
            Error::Status(s) => {
                Error::Status(Status::new(s.code(), format!("{what}: {}", s.message())))
            }
            other => {
                let status = Status::from(other);
                Error::Status(Status::new(
                    status.code(),
                    format!("{what}: {}", status.message()),
                ))
            }
        }
    }

    /// Wraps the message with context, keeping a store error matchable.
    pub fn store_context(self, what: impl std::fmt::Display) -> Error {
        match self {
            Error::Store(_) => self,
            other => other.context(what),
        }
    }
}

fn store_code(e: &StoreError) -> Code {
    match e {
        StoreError::NotFound => Code::NotFound,
        StoreError::AlreadyExists => Code::AlreadyExists,
        StoreError::VersionConflict | StoreError::UidConflict | StoreError::LeaseConflict => {
            Code::Aborted
        }
        StoreError::FailedPrecondition => Code::FailedPrecondition,
        StoreError::InvalidPageToken(_)
        | StoreError::InvalidPageSize
        | StoreError::PreconditionRequired(_)
        | StoreError::ImmutableField(_)
        | StoreError::InvalidArgument(_) => Code::InvalidArgument,
        StoreError::Unavailable(_) => Code::Unavailable,
        StoreError::Rejected(_) | StoreError::Corrupt(_) | StoreError::Backend(_) => Code::Internal,
    }
}

impl From<Status> for Error {
    fn from(s: Status) -> Self {
        Error::Status(s)
    }
}

impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::Rejected(boxed) => match boxed.downcast::<Error>() {
                Ok(err) => *err,
                Err(boxed) => match boxed.downcast::<Status>() {
                    Ok(status) => Error::Status(*status),
                    Err(boxed) => Error::Store(StoreError::Rejected(boxed)),
                },
            },
            other => Error::Store(other),
        }
    }
}

impl From<hivekeep_core::field::ValidationError> for Error {
    fn from(e: hivekeep_core::field::ValidationError) -> Self {
        Error::Status(e.into())
    }
}

impl From<Error> for Status {
    fn from(e: Error) -> Self {
        match e {
            Error::Status(s) => s,
            Error::Store(e) => store_status(e),
            Error::WorkerFilledUp => Status::aborted("picked worker no longer has room"),
            Error::NoWorkerRoom => Status::resource_exhausted("no worker has room for the actor"),
            Error::ActorBusy => Status::aborted("actor has an operation in progress"),
            Error::AlreadyMarked => Status::internal("already marked for deletion"),
            Error::NoKeeperOnNode(node) => {
                Status::unavailable(format!("no keeper found on node {node:?}"))
            }
        }
    }
}

fn store_status(e: StoreError) -> Status {
    match e {
        StoreError::NotFound => Status::not_found("not found"),
        StoreError::AlreadyExists => Status::already_exists("already exists"),
        StoreError::VersionConflict | StoreError::UidConflict => {
            Status::aborted("concurrent update conflict, please retry")
        }
        StoreError::LeaseConflict => Status::aborted("another operation is in progress"),
        StoreError::FailedPrecondition => Status::failed_precondition("failed precondition"),
        StoreError::InvalidPageToken(_) => Status::invalid_argument("invalid page_token"),
        StoreError::InvalidPageSize => Status::invalid_argument("invalid page_size"),
        e @ (StoreError::PreconditionRequired(_)
        | StoreError::ImmutableField(_)
        | StoreError::InvalidArgument(_)) => Status::invalid_argument(e.to_string()),
        StoreError::Unavailable(m) => Status::unavailable(format!("store unavailable: {m}")),
        e => Status::internal(e.to_string()),
    }
}

/// `ABORTED` for a lost optimistic-concurrency race.
pub fn conflict() -> Error {
    Error::Status(Status::aborted("concurrent update conflict, please retry"))
}

/// Shorthand constructors returning [`Error`].
pub mod status {
    use super::Error;
    use tonic::Status;

    macro_rules! ctor {
        ($($name:ident),*) => {
            $(
                #[doc = concat!("An `", stringify!($name), "` error.")]
                pub fn $name(msg: impl Into<String>) -> Error {
                    Error::Status(Status::$name(msg.into()))
                }
            )*
        };
    }

    ctor!(
        not_found,
        already_exists,
        failed_precondition,
        aborted,
        invalid_argument,
        internal,
        unavailable,
        resource_exhausted,
        permission_denied,
        unauthenticated,
        data_loss
    );
}

/// Converts a mutate-closure refusal into the store's carrier.
pub fn reject(e: impl Into<Error>) -> StoreError {
    StoreError::rejected(e.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejected_errors_round_trip() {
        let e: Error = reject(status::failed_precondition("nope")).into();
        assert_eq!(e.code(), Code::FailedPrecondition);
        let e: Error = reject(Error::WorkerFilledUp).into();
        assert!(matches!(e, Error::WorkerFilledUp));
        let e: Error = StoreError::rejected(Status::not_found("x")).into();
        assert_eq!(e.code(), Code::NotFound);
    }

    #[test]
    fn store_errors_map_to_codes() {
        let s: Status = Error::from(StoreError::VersionConflict).into();
        assert_eq!(s.code(), Code::Aborted);
        let s: Status = Error::from(StoreError::InvalidPageToken("x".into())).into();
        assert_eq!(s.code(), Code::InvalidArgument);
        assert_eq!(s.message(), "invalid page_token");
        assert!(Error::from(StoreError::VersionConflict).is_version_conflict());
    }

    #[test]
    fn context_keeps_code() {
        let e = status::unavailable("down").context("while calling keeper Run");
        assert_eq!(e.code(), Code::Unavailable);
        assert_eq!(Status::from(e).message(), "while calling keeper Run: down");
    }
}

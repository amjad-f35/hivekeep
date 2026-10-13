//! gRPC status helpers shared by the keeper's RPC paths.
//!
//! Every error that can reach the control plane is a [`tonic::Status`]: the
//! code tells the control plane whether to retry (Unavailable, Canceled,
//! DeadlineExceeded), refuse (FailedPrecondition, InvalidArgument, Aborted,
//! NotFound), or crash the actor (Internal).

use tonic::{Code, Status};

/// A failure the keeper cannot classify, reported as Internal.
pub fn internal(msg: impl Into<String>) -> Status {
    Status::internal(msg)
}

/// Prefixes `what` to `err`'s message, keeping its code.
pub fn wrap(what: impl std::fmt::Display, err: Status) -> Status {
    Status::new(err.code(), format!("{what}: {}", err.message()))
}

/// A filesystem failure while doing `what`. A missing file keeps NotFound
/// so callers can tell it apart; anything else is Internal.
pub fn fs_err(what: impl std::fmt::Display, err: &std::io::Error) -> Status {
    let code = match err.kind() {
        std::io::ErrorKind::NotFound => Code::NotFound,
        _ => Code::Internal,
    };
    Status::new(code, format!("{what}: {err}"))
}

/// Whether `err` is a filesystem "not found" turned into a status by
/// [`fs_err`].
pub fn is_not_found(err: &Status) -> bool {
    err.code() == Code::NotFound
}

/// The status of an I/O error from a blocking task that panicked or was
/// cancelled.
pub fn join_err(err: tokio::task::JoinError) -> Status {
    Status::internal(format!("background task failed: {err}"))
}

/// Runs a blocking closure on the blocking pool.
pub async fn blocking<T, F>(f: F) -> Result<T, Status>
where
    F: FnOnce() -> Result<T, Status> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(join_err)?
}

/// A gRPC code's canonical name, as other gRPC implementations spell it in
/// metric labels ("Canceled", not "Cancelled").
pub fn code_name(code: tonic::Code) -> &'static str {
    use tonic::Code::*;
    match code {
        Ok => "OK",
        Cancelled => "Canceled",
        Unknown => "Unknown",
        InvalidArgument => "InvalidArgument",
        DeadlineExceeded => "DeadlineExceeded",
        NotFound => "NotFound",
        AlreadyExists => "AlreadyExists",
        PermissionDenied => "PermissionDenied",
        ResourceExhausted => "ResourceExhausted",
        FailedPrecondition => "FailedPrecondition",
        Aborted => "Aborted",
        OutOfRange => "OutOfRange",
        Unimplemented => "Unimplemented",
        Internal => "Internal",
        Unavailable => "Unavailable",
        DataLoss => "DataLoss",
        Unauthenticated => "Unauthenticated",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_keeps_code() {
        let e = wrap("while doing x", Status::unavailable("down"));
        assert_eq!(e.code(), Code::Unavailable);
        assert_eq!(e.message(), "while doing x: down");
    }

    #[test]
    fn fs_err_keeps_not_found() {
        let e = fs_err(
            "reading",
            &std::io::Error::from(std::io::ErrorKind::NotFound),
        );
        assert!(is_not_found(&e));
        let e = fs_err(
            "reading",
            &std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        );
        assert_eq!(e.code(), Code::Internal);
    }
}

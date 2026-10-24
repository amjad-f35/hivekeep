//! Canonical gRPC status names (`OK`, `Unavailable`, ...), as reported in
//! the `/grpc` endpoint's `code` field.

use tonic::Code;

/// The canonical CamelCase name of `code`.
pub fn name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "Canceled",
        Code::Unknown => "Unknown",
        Code::InvalidArgument => "InvalidArgument",
        Code::DeadlineExceeded => "DeadlineExceeded",
        Code::NotFound => "NotFound",
        Code::AlreadyExists => "AlreadyExists",
        Code::PermissionDenied => "PermissionDenied",
        Code::ResourceExhausted => "ResourceExhausted",
        Code::FailedPrecondition => "FailedPrecondition",
        Code::Aborted => "Aborted",
        Code::OutOfRange => "OutOfRange",
        Code::Unimplemented => "Unimplemented",
        Code::Internal => "Internal",
        Code::Unavailable => "Unavailable",
        Code::DataLoss => "DataLoss",
        Code::Unauthenticated => "Unauthenticated",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(name(Code::Ok), "OK");
        assert_eq!(name(Code::Unavailable), "Unavailable");
        assert_eq!(name(Code::Cancelled), "Canceled");
    }
}

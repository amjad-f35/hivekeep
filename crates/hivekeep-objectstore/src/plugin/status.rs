//! Mapping plugin errors to gRPC statuses.

use tonic::Status;

use crate::error::{Error, ErrorKind};

/// Maps a plugin error to a status, keeping NotFound distinguishable so
/// callers can probe for a snapshot. A status from the credential source, or
/// one the plugin already classified, passes through as it is.
pub fn to_status(err: Error) -> Status {
    match err.kind() {
        ErrorKind::Status(s) => Status::new(s.code(), s.message().to_string()),
        ErrorKind::NotFound => Status::not_found(err.to_string()),
        ErrorKind::EmptySource => Status::failed_precondition(err.to_string()),
        ErrorKind::Canceled => Status::cancelled(err.to_string()),
        ErrorKind::DeadlineExceeded => Status::deadline_exceeded(err.to_string()),
        _ => {
            // The caller sees only Internal, and the keeper drops a plugin's
            // status text, so this line is the one record of why the plugin
            // failed. It carries what the backend judged safe to log, never
            // the error's text, which may quote a URI, header or credential.
            log_internal(&err);
            Status::internal(err.to_string())
        }
    }
}

fn log_internal(err: &Error) {
    match err.describe() {
        Some(d) => tracing::warn!(
            http_status = d.http_status,
            service_code = %d.code,
            service_message = %d.message,
            error_type = err.kind_name(),
            "Object-store plugin failed with an unrecognized error; returning Internal"
        ),
        None => tracing::warn!(
            error_type = err.kind_name(),
            "Object-store plugin failed with an unrecognized error; returning Internal"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ServiceDetail, ServiceResponse};
    use crate::testing::LogCapture;

    #[test]
    fn logs_unrecognized_errors_without_their_text() {
        const SECRET: &str = "SECRET-SIG";
        let cases: Vec<(&str, Error, tonic::Code, Vec<&str>)> = vec![
            (
                "storage response error",
                Error::service(
                    ServiceResponse {
                        status: 412,
                        detail: ServiceDetail::Other,
                    },
                    format!("precondition failed for gs://b/o {SECRET}"),
                )
                .context(format!("while reading gs://b/o?sig={SECRET}")),
                tonic::Code::Internal,
                vec![
                    r#""level":"WARN""#,
                    r#""http_status":412"#,
                    r#""error_type":"service""#,
                ],
            ),
            (
                "other error",
                Error::other("boom").context(format!("while reading gs://b/o?sig={SECRET}")),
                tonic::Code::Internal,
                vec![r#""level":"WARN""#, r#""error_type":"other""#],
            ),
            (
                "not found",
                Error::not_found("object not found").context("while reading"),
                tonic::Code::NotFound,
                vec![],
            ),
            (
                "canceled",
                Error::canceled(),
                tonic::Code::Cancelled,
                vec![],
            ),
        ];
        for (name, err, code, want_logs) in cases {
            let logs = LogCapture::default();
            let _g = logs.install();
            assert_eq!(to_status(err).code(), code, "{name}");
            let out = logs.contents();
            if want_logs.is_empty() {
                assert!(out.is_empty(), "{name}: logged a recognized error: {out}");
            }
            for w in want_logs {
                assert!(out.contains(w), "{name}: logs missing {w}: {out}");
            }
            for leak in [SECRET, "gs://b/o", "boom", "precondition failed"] {
                assert!(!out.contains(leak), "{name}: logs carry {leak:?}: {out}");
            }
        }
    }
}

//! Routing errors and the immediate responses Envoy sends for them.

use std::fmt;

use envoy_types::pb::envoy::config::core::v3::{HeaderValue, HeaderValueOption};
use envoy_types::pb::envoy::r#type::v3::HttpStatus;
use envoy_types::pb::envoy::service::ext_proc::v3::{
    processing_response, HeaderMutation, ImmediateResponse, ProcessingResponse,
};

/// How a routing step failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// The request (or its caller) was canceled.
    Canceled,
    /// A deadline passed.
    DeadlineExceeded,
    /// Anything else.
    Other,
}

/// A failed routing decision.
///
/// `status` is the HTTP status for the client (500 when unset). `code` is
/// the gRPC code of an underlying control-plane failure, which takes
/// precedence when classifying the outcome for metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteError {
    /// What the client is told.
    pub message: String,
    /// HTTP status to answer with.
    pub status: Option<u16>,
    /// gRPC code of the cause, if it came from a gRPC call.
    pub code: Option<tonic::Code>,
    /// Cancellation or timeout.
    pub kind: ErrorKind,
    /// The cause's message, for logs.
    pub cause: Option<String>,
    /// The gRPC status message of the cause.
    pub grpc_message: Option<String>,
}

impl RouteError {
    /// An error answered with HTTP `status`.
    pub fn req(status: u16, message: impl Into<String>) -> Self {
        RouteError {
            message: message.into(),
            status: Some(status),
            code: None,
            kind: ErrorKind::Other,
            cause: None,
            grpc_message: None,
        }
    }

    /// An error answered with HTTP `status`, caused by `cause`.
    pub fn wrap(status: u16, cause: &RouteError, message: impl Into<String>) -> Self {
        RouteError {
            message: message.into(),
            status: Some(status),
            code: cause.code,
            kind: cause.kind,
            cause: Some(cause.to_string()),
            grpc_message: cause.grpc_message.clone(),
        }
    }

    /// An error with no HTTP mapping.
    pub fn other(message: impl Into<String>) -> Self {
        RouteError {
            message: message.into(),
            status: None,
            code: None,
            kind: ErrorKind::Other,
            cause: None,
            grpc_message: None,
        }
    }

    /// Cancellation.
    pub fn canceled() -> Self {
        RouteError {
            kind: ErrorKind::Canceled,
            ..RouteError::other("context canceled")
        }
    }

    /// A passed deadline.
    pub fn deadline_exceeded() -> Self {
        RouteError {
            kind: ErrorKind::DeadlineExceeded,
            ..RouteError::other("context deadline exceeded")
        }
    }

    /// Drops the gRPC code (the error then classifies by HTTP status).
    pub fn without_code(mut self) -> Self {
        self.code = None;
        self
    }

    /// The HTTP status to answer with.
    pub fn http_status(&self) -> u16 {
        self.status.unwrap_or(500)
    }
}

impl fmt::Display for RouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RouteError {}

impl From<tonic::Status> for RouteError {
    fn from(s: tonic::Status) -> Self {
        let kind = match s.code() {
            tonic::Code::Cancelled => ErrorKind::Canceled,
            tonic::Code::DeadlineExceeded => ErrorKind::DeadlineExceeded,
            _ => ErrorKind::Other,
        };
        RouteError {
            message: format!("rpc error: code = {:?} desc = {}", s.code(), s.message()),
            status: None,
            code: Some(s.code()),
            kind,
            cause: None,
            grpc_message: Some(s.message().to_string()),
        }
    }
}

/// An immediate response with `status` and a plain-text `message`. The
/// content type travels in `raw_value`, which Envoy requires for header
/// mutations.
pub fn immediate_response(status: u16, message: &str) -> ProcessingResponse {
    ProcessingResponse {
        response: Some(processing_response::Response::ImmediateResponse(
            ImmediateResponse {
                status: Some(HttpStatus {
                    code: i32::from(status),
                }),
                body: message.as_bytes().to_vec(),
                headers: Some(HeaderMutation {
                    set_headers: vec![HeaderValueOption {
                        header: Some(HeaderValue {
                            key: "content-type".into(),
                            raw_value: b"text/plain".to_vec(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_req_error() {
        let err = RouteError::req(400, format!("actor {:?} is {}", "abc", "bad"));
        assert_eq!(err.http_status(), 400);
        assert_eq!(err.to_string(), r#"actor "abc" is bad"#);
    }

    #[test]
    fn immediate_response_header_encoding() {
        let resp = immediate_response(503, "body");
        let Some(processing_response::Response::ImmediateResponse(ir)) = resp.response else {
            panic!("not an immediate response");
        };
        let set = ir.headers.unwrap().set_headers;
        assert_eq!(set.len(), 1);
        let h = set[0].header.as_ref().unwrap();
        assert_eq!(
            (h.key.as_str(), h.raw_value.as_slice()),
            ("content-type", &b"text/plain"[..])
        );
        assert!(h.value.is_empty(), "must use raw_value only");
        assert_eq!(ir.status.unwrap().code, 503);
        assert_eq!(ir.body, b"body".to_vec());
    }
}

//! Errors from cluster access, classified the way the installer decides
//! whether to retry, ignore or report them.

use std::io;

/// How a request failed below the HTTP layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportKind {
    ConnectionRefused,
    ConnectionReset,
    ConnectionLost,
    UnexpectedEof,
    Timeout,
    Other,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum KubeError {
    /// The API server answered with a failure status.
    #[error("{message}")]
    Api {
        code: u16,
        reason: String,
        message: String,
    },
    /// No resource serves this group/version/kind (CRD not installed yet).
    #[error("no matches for kind {kind:?} in version {api_version:?}")]
    NoMatch { api_version: String, kind: String },
    /// The request never got an answer.
    #[error("{message}")]
    Transport {
        kind: TransportKind,
        message: String,
    },
    /// A manifest or local file could not be read.
    #[error("{message}")]
    Io {
        kind: io::ErrorKind,
        message: String,
    },
    /// A manifest or response could not be decoded.
    #[error("{0}")]
    Decode(String),
    /// The client could not be configured.
    #[error("{0}")]
    Config(String),
    /// A wait ran out of time.
    #[error("{0}")]
    Deadline(String),
    /// The operation was cancelled.
    #[error("operation cancelled")]
    Cancelled,
    /// A lower error with the operation it interrupted.
    #[error("{context}: {source}")]
    Context {
        context: String,
        source: Box<KubeError>,
    },
    #[error("{0}")]
    Other(String),
}

impl KubeError {
    pub fn api(code: u16, reason: &str, message: impl Into<String>) -> Self {
        Self::Api {
            code,
            reason: reason.to_string(),
            message: message.into(),
        }
    }
    pub fn not_found(resource: &str, name: &str) -> Self {
        Self::api(404, "NotFound", format!("{resource} {name:?} not found"))
    }
    pub fn conflict(resource: &str, name: &str, why: &str) -> Self {
        Self::api(
            409,
            "Conflict",
            format!("Operation cannot be fulfilled on {resource} {name:?}: {why}"),
        )
    }
    pub fn internal(why: &str) -> Self {
        Self::api(
            500,
            "InternalError",
            format!("Internal error occurred: {why}"),
        )
    }
    pub fn service_unavailable(why: &str) -> Self {
        Self::api(503, "ServiceUnavailable", why)
    }
    pub fn too_many_requests(why: &str) -> Self {
        Self::api(429, "TooManyRequests", why)
    }
    pub fn server_timeout(resource: &str, op: &str) -> Self {
        Self::api(
            504,
            "ServerTimeout",
            format!("The {op} operation against {resource} could not be completed at this time"),
        )
    }
    pub fn forbidden(resource: &str, name: &str, why: &str) -> Self {
        Self::api(
            403,
            "Forbidden",
            format!("{resource} {name:?} is forbidden: {why}"),
        )
    }
    pub fn bad_request(why: &str) -> Self {
        Self::api(400, "BadRequest", why)
    }
    pub fn invalid(kind: &str, name: &str, why: &str) -> Self {
        Self::api(422, "Invalid", format!("{kind} {name:?} is invalid: {why}"))
    }
    pub fn no_match(gvk: &super::Gvk) -> Self {
        Self::NoMatch {
            api_version: gvk.api_version(),
            kind: gvk.kind.clone(),
        }
    }
    pub fn transport(kind: TransportKind, message: impl Into<String>) -> Self {
        Self::Transport {
            kind,
            message: message.into(),
        }
    }
    pub fn io(err: &io::Error, context: impl Into<String>) -> Self {
        Self::Io {
            kind: err.kind(),
            message: format!("{}: {err}", context.into()),
        }
    }

    /// Wraps the error with the operation it interrupted.
    pub fn context(self, context: impl Into<String>) -> Self {
        Self::Context {
            context: context.into(),
            source: Box::new(self),
        }
    }

    /// The innermost error.
    pub fn root(&self) -> &KubeError {
        match self {
            Self::Context { source, .. } => source.root(),
            other => other,
        }
    }

    fn api_status(&self) -> Option<(u16, &str)> {
        match self.root() {
            Self::Api { code, reason, .. } => Some((*code, reason.as_str())),
            _ => None,
        }
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self.api_status(), Some((404, _)))
    }
    pub fn is_conflict(&self) -> bool {
        matches!(self.api_status(), Some((409, _)))
    }
    pub fn is_forbidden(&self) -> bool {
        matches!(self.api_status(), Some((403, _)))
    }
    pub fn is_invalid(&self) -> bool {
        matches!(self.api_status(), Some((422, _)))
    }
    pub fn is_already_exists(&self) -> bool {
        matches!(self.api_status(), Some((409, "AlreadyExists")))
    }
    pub fn is_no_match(&self) -> bool {
        matches!(self.root(), Self::NoMatch { .. })
    }
    /// A local file named by the operation does not exist.
    pub fn is_not_exist(&self) -> bool {
        matches!(
            self.root(),
            Self::Io {
                kind: io::ErrorKind::NotFound,
                ..
            }
        )
    }
    pub fn is_cancelled(&self) -> bool {
        matches!(self.root(), Self::Cancelled)
    }

    /// Whether the failure describes the connection to the cluster, or a
    /// control plane still coming up, rather than the cluster's answer. The
    /// install runs bursts of requests against control planes that are often
    /// still starting, which is exactly when these appear.
    pub fn is_retryable(&self) -> bool {
        match self.root() {
            Self::Api { code, reason, .. } => {
                matches!(*code, 429 | 500 | 503 | 504)
                    || matches!(
                        reason.as_str(),
                        "TooManyRequests"
                            | "ServiceUnavailable"
                            | "InternalError"
                            | "Timeout"
                            | "ServerTimeout"
                    )
            }
            Self::Transport { kind, .. } => *kind != TransportKind::Other,
            _ => false,
        }
    }
}

impl From<kube::Error> for KubeError {
    fn from(err: kube::Error) -> Self {
        match err {
            kube::Error::Api(status) => KubeError::Api {
                code: status.code,
                reason: status.reason.clone(),
                message: status.message.clone(),
            },
            other => classify_transport(&other.to_string()),
        }
    }
}

/// Classifies a transport failure by its rendered chain, which is all the
/// HTTP stack reliably exposes.
pub fn classify_transport(message: &str) -> KubeError {
    let lower = message.to_lowercase();
    let kind = if lower.contains("connection refused") {
        TransportKind::ConnectionRefused
    } else if lower.contains("connection reset") || lower.contains("broken pipe") {
        TransportKind::ConnectionReset
    } else if lower.contains("connection closed") || lower.contains("goaway") {
        TransportKind::ConnectionLost
    } else if lower.contains("unexpected eof") || lower.contains("unexpected end of file") {
        TransportKind::UnexpectedEof
    } else if lower.contains("timed out") || lower.contains("timeout") {
        TransportKind::Timeout
    } else {
        TransportKind::Other
    };
    KubeError::Transport {
        kind,
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_classification() {
        let cases: Vec<(&str, KubeError, bool)> = vec![
            ("throttled", KubeError::too_many_requests("slow down"), true),
            (
                "apiserver unavailable",
                KubeError::service_unavailable("apiserver is starting"),
                true,
            ),
            (
                "webhook not serving",
                KubeError::internal("failed calling webhook"),
                true,
            ),
            (
                "server timeout",
                KubeError::server_timeout("deployments.apps", "get"),
                true,
            ),
            (
                "connection reset",
                classify_transport("Get \"https://k8s/api\": connection reset by peer"),
                true,
            ),
            (
                "connection refused",
                classify_transport("Get \"https://k8s/api\": connection refused"),
                true,
            ),
            (
                "not found",
                KubeError::not_found("deployments.apps", "api"),
                false,
            ),
            (
                "forbidden",
                KubeError::forbidden("deployments.apps", "api", "nope"),
                false,
            ),
            ("invalid", KubeError::bad_request("malformed"), false),
            (
                "plain error",
                KubeError::Other("deployment exceeded its progress deadline".into()),
                false,
            ),
        ];
        for (name, err, want) in cases {
            assert_eq!(err.is_retryable(), want, "{name}: {err}");
            // Wrapping keeps the classification.
            assert_eq!(
                err.clone().context("while waiting").is_retryable(),
                want,
                "{name} wrapped"
            );
        }
    }
}

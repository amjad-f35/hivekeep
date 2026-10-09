//! Wrappers applied around gRPC handlers: logging with redaction, status
//! mapping, a deadline ceiling, and refusal of unknown fields.

pub mod deadline;
pub mod error;
pub mod log_layer;
pub mod reject_unknown;
pub mod unary;

pub use deadline::{with_max_deadline, Deadline};
pub use error::{from_error, wrap, ApiError, BoxError, ContextError};
pub use log_layer::{GrpcLogLayer, LogMode};
pub use reject_unknown::{find_unknown_fields, RejectUnknownFieldsLayer};
pub use unary::{internal_server_unary, server_unary, LoggedPrincipal, SERVER_ELAPSED_TRAILER};

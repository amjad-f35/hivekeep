//! The router's Envoy external processor and the names it shares with the
//! Envoy configuration.

pub mod attributes;
pub mod dispatch;
pub mod error;
pub mod metadata;
pub mod metrics;
pub mod record;
pub mod server;

pub use attributes::*;
pub use dispatch::{
    direction_of, is_egress_filter_chain, Direction, EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
    EGRESS_FILTER_CHAIN_NAME, EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
};
pub use error::{immediate_response, ErrorKind, RouteError};
pub use metadata::RequestMetadata;
pub use metrics::{classify_outcome, new_route_duration_histogram, SERVICE_NAME};
pub use record::{QueryRecorder, RecordedQuery};
pub use server::{Handler, HandlerResult, Handlers, Server, SharedServer};

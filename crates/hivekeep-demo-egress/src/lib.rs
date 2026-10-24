//! The egress demo workload.
//!
//! An Actor running this server takes instructions over HTTP and performs
//! outbound work on the caller's behalf — plain fetches, slow or delayed
//! uploads, many parallel connections, WebSockets, gRPC calls and HTTP
//! CONNECTs — then reports what came back. Whatever the gate's egress policy
//! allowed, rewrote or refused is visible in those reports.

pub mod proto;

pub mod address;
pub mod app;
pub mod connect;
pub mod dial;
pub mod fetch;
pub mod grpc;
pub mod grpc_code;
pub mod http1;
pub mod limits;
pub mod mux;
pub mod post;
pub mod slow_headers;
pub mod tls;
pub mod websocket;
pub mod wire;

pub use app::{router, AppState, Delays};
pub use fetch::{Fetch, Fetched, HttpFetcher, Outbound};

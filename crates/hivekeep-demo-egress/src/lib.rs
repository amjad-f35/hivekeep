//! The egress demo workload.
//!
//! An Actor running this server takes instructions over HTTP and performs
//! outbound work on the caller's behalf — plain fetches, slow or delayed
//! uploads, many parallel connections, WebSockets, gRPC calls and HTTP
//! CONNECTs — then reports what came back. Whatever the gate's egress policy
//! allowed, rewrote or refused is visible in those reports.

pub mod proto;

pub mod address;
pub mod dial;
pub mod grpc_code;
pub mod http1;
pub mod limits;
pub mod tls;

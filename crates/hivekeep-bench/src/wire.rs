//! Wire-visible names shared by the benchmark tools and the services they
//! drive.

/// Response header (HTTP) or trailer (gRPC) carrying the server's own
/// handling time in microseconds, so a load generator can tell server time
/// from client-observed latency.
pub const SERVER_ELAPSED_HEADER: &str = "x-server-elapsed-us";

/// Request header naming the Actor (`<hive>/<name>`) the gate router should
/// deliver a request to.
pub const TARGET_ACTOR_HEADER: &str = "hk-target-actor";

/// Port the keeper serves its gRPC API on; the fake keeper listens here so
/// hivekeep-api dials it unchanged.
pub const KEEPER_DEFAULT_PORT: u16 = 7443;

/// The server-elapsed value for a handler that started at `start`.
pub fn server_elapsed_value(start: std::time::Instant) -> String {
    start.elapsed().as_micros().to_string()
}

//! Sizes and timeouts shared by every endpoint.

use std::time::Duration;

/// Address the server listens on.
pub const LISTEN_ADDRESS: &str = "0.0.0.0:80";
/// Largest JSON instruction accepted.
pub const MAX_REQUEST_BODY: usize = 64 << 10;
/// Largest upstream body relayed back.
pub const MAX_RESPONSE_BODY: usize = 1 << 20;
/// Budget for one outbound operation.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// Most WebSocket messages one instruction may send.
pub const MAX_WEBSOCKET_MESSAGES: usize = 16;
/// Largest WebSocket message sent or received.
pub const MAX_WEBSOCKET_MESSAGE_SIZE: usize = 64 << 10;
/// Largest body kept from a refused WebSocket handshake.
pub const MAX_WEBSOCKET_HANDSHAKE_BODY: usize = 4 << 10;
/// Largest body kept from a refused CONNECT.
pub const MAX_CONNECT_ANSWER_BODY: usize = 4 << 10;
/// Parallel connections the `/mux` endpoint opens.
pub const MUX_REQUEST_COUNT: usize = 250;
/// Body size of each `/mux` and `/post` upload.
pub const MUX_BODY_SIZE: usize = 64;
/// How long `/post` holds its body back after the headers. Longer than the
/// gate's external-processor cache lifetime (10s), so the connection outlives
/// a cached policy decision.
pub const POST_BODY_DELAY: Duration = Duration::from_secs(15);
/// How long `/slow-headers` pauses before finishing its header block.
pub const SLOW_HEADER_DELAY: Duration = Duration::from_secs(5);

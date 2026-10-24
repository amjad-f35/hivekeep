//! JSON instructions the endpoints accept and the reports they return.
//!
//! Field names are part of the demo's contract with the scripts and tests
//! that drive it; missing fields take their zero value and unknown ones are
//! ignored.

use serde::{Deserialize, Serialize};

fn is_zero_u16(v: &u16) -> bool {
    *v == 0
}

/// Instruction for `/`, `/post`, `/slow-headers` and `/mux`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct FetchRequest {
    pub url: String,
    #[serde(rename = "rootCA", skip_serializing_if = "String::is_empty")]
    pub root_ca: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub http1: bool,
}

/// Report for the fetch-like endpoints.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct FetchResponse {
    #[serde(rename = "statusCode", skip_serializing_if = "is_zero_u16")]
    pub status_code: u16,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub body: String,
    #[serde(rename = "serverCert", skip_serializing_if = "String::is_empty")]
    pub server_cert: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub protocol: String,
    pub tls: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error: String,
}

impl FetchResponse {
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            ..Default::default()
        }
    }
}

/// Instruction for `/websocket`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct WebSocketRequest {
    pub url: String,
    #[serde(rename = "rootCA", skip_serializing_if = "String::is_empty")]
    pub root_ca: String,
    pub messages: Vec<String>,
}

/// Opcode numbers reported for received messages.
pub const TEXT_MESSAGE: u8 = 1;
pub const BINARY_MESSAGE: u8 = 2;

/// One message received over the WebSocket.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct WebSocketMessage {
    #[serde(rename = "type")]
    pub kind: u8,
    pub message: String,
}

/// Report for `/websocket`. `messages` is always present, even when empty.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct WebSocketResponse {
    #[serde(rename = "statusCode", skip_serializing_if = "is_zero_u16")]
    pub status_code: u16,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub protocol: String,
    #[serde(rename = "handshakeBody", skip_serializing_if = "String::is_empty")]
    pub handshake_body: String,
    pub tls: bool,
    pub messages: Vec<WebSocketMessage>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error: String,
}

impl WebSocketResponse {
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            ..Default::default()
        }
    }
}

/// Instruction for `/grpc`: one unary Echo against `target`, plus a server
/// stream and/or a bidirectional stream when the matching count is positive.
/// The connection is cleartext HTTP/2; carrying it is the egress path's job.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct GrpcRequest {
    /// `host:port` of the gRPC server.
    pub target: String,
    pub message: String,
    #[serde(rename = "streamCount", skip_serializing_if = "is_zero_i32")]
    pub stream_count: i32,
    /// Messages sent over a bidirectional stream, each awaiting its answer
    /// before the next goes out.
    #[serde(rename = "bidiCount", skip_serializing_if = "is_zero_i32")]
    pub bidi_count: i32,
}

fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}

/// One numbered response from a stream.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct StreamedMessage {
    pub message: String,
    pub index: i32,
}

/// Report for `/grpc`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct GrpcResponse {
    /// What the unary Echo returned.
    pub message: String,
    /// The server stream, in arrival order; absent unless asked for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<Vec<StreamedMessage>>,
    /// The bidirectional stream, in arrival order; absent unless asked for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bidi: Option<Vec<StreamedMessage>>,
    /// gRPC status name of the last RPC attempted. Statuses travel in
    /// trailers, so a path that loses trailers or falls back to HTTP/1.1
    /// cannot produce one.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub code: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error: String,
}

impl GrpcResponse {
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            ..Default::default()
        }
    }
}

/// Instruction for `/connect`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConnectRequest {
    /// `host:port` the CONNECT is sent to.
    pub proxy: String,
    /// `host:port` named in the CONNECT request line.
    pub target: String,
    /// Speak TLS to the proxy first, as with an HTTPS proxy. Its certificate
    /// is checked against `rootCA` when given, else the system roots.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub tls: bool,
    #[serde(rename = "rootCA", skip_serializing_if = "String::is_empty")]
    pub root_ca: String,
}

/// The proxy's answer to the CONNECT.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConnectResponse {
    #[serde(rename = "statusCode", skip_serializing_if = "is_zero_u16")]
    pub status_code: u16,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub protocol: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub body: String,
    /// Whether the CONNECT went over a verified TLS session.
    pub tls: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error: String,
}

impl ConnectResponse {
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_fields_are_omitted_but_tls_and_messages_stay() {
        let fetch = serde_json::to_string(&FetchResponse::default()).unwrap();
        assert_eq!(fetch, r#"{"tls":false}"#);
        let ws = serde_json::to_string(&WebSocketResponse::default()).unwrap();
        assert_eq!(ws, r#"{"tls":false,"messages":[]}"#);
        let grpc = serde_json::to_string(&GrpcResponse::default()).unwrap();
        assert_eq!(grpc, r#"{"message":""}"#);
    }

    #[test]
    fn camel_case_names_round_trip() {
        let req: GrpcRequest =
            serde_json::from_str(r#"{"target":"a:1","streamCount":2,"bidiCount":3,"x":1}"#)
                .unwrap();
        assert_eq!((req.stream_count, req.bidi_count), (2, 3));
        let req: FetchRequest =
            serde_json::from_str(r#"{"url":"u","rootCA":"pem","http1":true}"#).unwrap();
        assert_eq!(
            req,
            FetchRequest {
                url: "u".into(),
                root_ca: "pem".into(),
                http1: true
            }
        );
    }
}

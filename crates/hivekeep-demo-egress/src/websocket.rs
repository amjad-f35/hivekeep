//! `/websocket`: opens a WebSocket, sends each message in turn and records
//! the reply to each.
//!
//! The opening handshake is written and read here rather than by a client
//! library so that a refused upgrade is fully observable: its status line,
//! protocol and (bounded) body all land in the report, which is how a test
//! tells a policy denial from a broken origin.

use futures::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::address::parse_url;
use crate::dial::{self, BoxedIo, DEADLINE_EXCEEDED};
use crate::http1::{read_body, read_head};
use crate::limits::{
    MAX_WEBSOCKET_HANDSHAKE_BODY, MAX_WEBSOCKET_MESSAGES, MAX_WEBSOCKET_MESSAGE_SIZE,
    REQUEST_TIMEOUT,
};
use crate::tls;
use crate::wire::{
    WebSocketMessage, WebSocketRequest, WebSocketResponse, BINARY_MESSAGE, TEXT_MESSAGE,
};

/// Checks the instruction before anything is dialed.
pub fn validate(input: &WebSocketRequest) -> Result<(), String> {
    let parts = parse_url(&input.url).map_err(|e| format!("invalid WebSocket URL: {e}"))?;
    if parts.scheme != "ws" && parts.scheme != "wss" {
        return Err("WebSocket URL scheme must be ws or wss".into());
    }
    if parts.hostname.is_empty() {
        return Err("WebSocket URL must include a hostname".into());
    }
    if input.messages.is_empty() {
        return Err("messages must contain at least one message".into());
    }
    if input.messages.len() > MAX_WEBSOCKET_MESSAGES {
        return Err(format!(
            "messages contains {} items, maximum is {MAX_WEBSOCKET_MESSAGES}",
            input.messages.len()
        ));
    }
    for (index, message) in input.messages.iter().enumerate() {
        if message.len() > MAX_WEBSOCKET_MESSAGE_SIZE {
            return Err(format!(
                "message {index} is {} bytes, maximum is {MAX_WEBSOCKET_MESSAGE_SIZE}",
                message.len()
            ));
        }
    }
    Ok(())
}

/// Runs the instruction and returns the HTTP status and report.
pub async fn run(input: WebSocketRequest) -> (u16, WebSocketResponse) {
    if let Err(e) = validate(&input) {
        return (400, WebSocketResponse::error(e));
    }
    let mut response = WebSocketResponse::default();
    let outcome = tokio::time::timeout(REQUEST_TIMEOUT, perform(&input, &mut response)).await;
    let failure = match outcome {
        Err(_) => Some(DEADLINE_EXCEEDED.to_string()),
        Ok(Err(e)) => Some(e),
        Ok(Ok(())) => None,
    };
    match failure {
        Some(e) => {
            response.error = e;
            (502, response)
        }
        None => (200, response),
    }
}

/// Handshakes and exchanges the messages, recording observations into
/// `response` as they happen so a later failure keeps them.
async fn perform(input: &WebSocketRequest, response: &mut WebSocketResponse) -> Result<(), String> {
    let parts = parse_url(&input.url).map_err(|e| format!("invalid WebSocket URL: {e}"))?;
    let secure = parts.scheme == "wss";
    let tls_config = if secure || !input.root_ca.is_empty() {
        let roots = tls::roots(&input.root_ca).map_err(|e| format!("configuring TLS: {e}"))?;
        Some(tls::client_config(roots, tls::ALPN_HTTP1))
    } else {
        None
    };
    let address = parts.dial_address(if secure { "443" } else { "80" });
    let mut conn: BoxedIo = dial::open(
        &address,
        if secure {
            tls_config.map(|c| (parts.hostname.as_str(), c))
        } else {
            None
        },
    )
    .await
    .map_err(|e| format!("WebSocket handshake failed: {e}"))?;

    let key = generate_key();
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n",
        parts.request_uri, parts.host
    );
    conn.write_all(request.as_bytes())
        .await
        .map_err(|e| format!("WebSocket handshake failed: {e}"))?;
    conn.flush()
        .await
        .map_err(|e| format!("WebSocket handshake failed: {e}"))?;

    let mut leftover = Vec::new();
    let head = read_head(&mut conn, &mut leftover)
        .await
        .map_err(|e| format!("WebSocket handshake failed: {e}"))?;
    response.status_code = head.status;
    response.protocol = head.protocol.clone();
    if head.status != 101 {
        let body = read_body(
            &mut conn,
            leftover,
            &head,
            "GET",
            MAX_WEBSOCKET_HANDSHAKE_BODY,
        )
        .await
        .unwrap_or_default();
        response.handshake_body = String::from_utf8_lossy(&body).into_owned();
        return Err(format!(
            "WebSocket handshake failed: bad handshake (HTTP {})",
            head.status
        ));
    }
    let upgraded = head
        .header("upgrade")
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    let accepted =
        head.header("sec-websocket-accept") == Some(derive_accept_key(key.as_bytes()).as_str());
    if !upgraded || !accepted {
        return Err("WebSocket handshake failed: bad handshake (invalid upgrade response)".into());
    }
    response.tls = secure;

    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_WEBSOCKET_MESSAGE_SIZE))
        .max_frame_size(Some(MAX_WEBSOCKET_MESSAGE_SIZE));
    let mut ws =
        WebSocketStream::from_partially_read(conn, leftover, Role::Client, Some(config)).await;

    for (index, message) in input.messages.iter().enumerate() {
        match tokio::time::timeout(REQUEST_TIMEOUT, ws.send(Message::text(message.clone()))).await {
            Err(_) => return Err(format!("writing message {index}: i/o timeout")),
            Ok(Err(e)) => return Err(format!("writing message {index}: {e}")),
            Ok(Ok(())) => {}
        }
        let received = loop {
            let next = tokio::time::timeout(REQUEST_TIMEOUT, ws.next())
                .await
                .map_err(|_| format!("reading response to message {index}: i/o timeout"))?;
            match next {
                None => {
                    return Err(format!(
                        "reading response to message {index}: connection closed"
                    ))
                }
                Some(Err(e)) => return Err(format!("reading response to message {index}: {e}")),
                Some(Ok(Message::Text(text))) => {
                    break WebSocketMessage {
                        kind: TEXT_MESSAGE,
                        message: text.as_str().to_string(),
                    }
                }
                Some(Ok(Message::Binary(bytes))) => {
                    break WebSocketMessage {
                        kind: BINARY_MESSAGE,
                        message: String::from_utf8_lossy(&bytes).into_owned(),
                    }
                }
                Some(Ok(Message::Close(frame))) => {
                    let why = frame
                        .map(|f| format!("{} {}", u16::from(f.code), f.reason))
                        .unwrap_or_default();
                    return Err(format!(
                        "reading response to message {index}: close received {why}"
                    )
                    .trim_end()
                    .to_string());
                }
                // Control frames are answered by the protocol layer.
                Some(Ok(_)) => continue,
            }
        };
        response.messages.push(received);
    }
    let _ = ws.close(None).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(url: &str, messages: &[&str]) -> WebSocketRequest {
        WebSocketRequest {
            url: url.into(),
            messages: messages.iter().map(|m| m.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn validation() {
        assert!(validate(&req("ws://h/", &["a"])).is_ok());
        assert!(validate(&req("http://h/", &["a"]))
            .unwrap_err()
            .contains("ws or wss"));
        assert!(validate(&req("ws:///x", &["a"]))
            .unwrap_err()
            .contains("hostname"));
        assert!(validate(&req("ws://h/", &[]))
            .unwrap_err()
            .contains("at least one"));
        let many = vec!["m"; MAX_WEBSOCKET_MESSAGES + 1];
        assert!(validate(&req("ws://h/", &many))
            .unwrap_err()
            .contains("maximum is 16"));
        let big = "x".repeat(MAX_WEBSOCKET_MESSAGE_SIZE + 1);
        assert!(validate(&req("ws://h/", &[&big]))
            .unwrap_err()
            .starts_with("message 0 is"));
    }
}

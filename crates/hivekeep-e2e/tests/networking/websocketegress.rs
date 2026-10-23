//! WebSocket upgrades from an Actor are refused by the egress gate, in the
//! clear and inside TLS.

use hivekeep_e2e::images::Image;
use hivekeep_e2e::serverpod::ServerPod;
use hivekeep_e2e::suite::e2e_test;
use serde::Deserialize;

use crate::networking::{post_egress_with_status, prepare_protocol_actor, prepare_protocol_origin};

/// The handshake body the gate refuses an upgrade with.
pub const WEBSOCKET_DENIED_BODY: &str = "WebSocket egress is not supported";

/// The Actor's observation; the outer status is checked first.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct EgressWebSocketResponse {
    #[serde(rename = "statusCode")]
    status_code: u16,
    protocol: String,
    #[serde(rename = "handshakeBody")]
    handshake_body: String,
    tls: bool,
    messages: Vec<EgressWebSocketMessage>,
    error: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct EgressWebSocketMessage {
    #[serde(rename = "type")]
    kind: i32,
    message: String,
}

fn ws_origin(name: &str, tls_port: u16) -> ServerPod {
    ServerPod {
        name: name.into(),
        image: Image::Testserver,
        args: vec!["websocket".into(), "--echo".into()],
        port: 80,
        target_port: 8080,
        tls_port,
        health_path: "/readyz".into(),
        ..Default::default()
    }
}

async fn check_refused(
    t: &hivekeep_e2e::harness::T,
    scheme: &str,
    encrypted: bool,
    prefix: &str,
    message: &str,
    what: &str,
) {
    let name = if encrypted { "wss-origin" } else { "ws-origin" };
    let (_, address, _) = prepare_protocol_origin(
        t,
        ws_origin(name, if encrypted { 443 } else { 0 }),
        encrypted,
    )
    .await;
    let (_, router, aref) = prepare_protocol_actor(t, prefix).await;
    let raw = post_egress_with_status(
        t,
        &router,
        &aref,
        "/websocket",
        &serde_json::json!({ "url": format!("{scheme}://{address}/ws"), "messages": [message] }),
        502,
    )
    .await;
    let got: EgressWebSocketResponse = serde_json::from_slice(&raw)
        .unwrap_or_else(|e| t.fatal(format!("decoding {what} observation: {e}")));
    if got.status_code != 403 {
        t.error(format!(
            "{what} handshake status = {}, want 403 ({got:?})",
            got.status_code
        ));
    }
    if got.handshake_body != WEBSOCKET_DENIED_BODY {
        t.error(format!(
            "{what} handshake body = {:?}, want {WEBSOCKET_DENIED_BODY:?}",
            got.handshake_body
        ));
    }
    if got.error.is_empty() {
        t.error(format!("{what} handshake error is empty"));
    }
    if !got.messages.is_empty() {
        let shown: Vec<(i32, &str)> = got
            .messages
            .iter()
            .map(|m| (m.kind, m.message.as_str()))
            .collect();
        t.error(format!(
            "{what} messages = {shown:?}, want no echoes (protocol {:?}, tls {})",
            got.protocol, got.tls
        ));
    }
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_websocket() {
    e2e_test("TestActorEgressWebSocket", |t| async move {
        check_refused(&t, "ws", false, "ws", "ws-message", "WebSocket").await
    });
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_secure_websocket() {
    e2e_test("TestActorEgressSecureWebSocket", |t| async move {
        check_refused(&t, "wss", true, "wss", "wss-message", "secure WebSocket").await
    });
}

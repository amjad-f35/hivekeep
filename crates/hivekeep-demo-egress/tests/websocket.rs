//! The `/websocket` endpoint.

mod support;

use hivekeep_demo_egress::limits::MAX_WEBSOCKET_HANDSHAKE_BODY;
use hivekeep_demo_egress::wire::{WebSocketRequest, WebSocketResponse, TEXT_MESSAGE};
use support::*;

fn req(url: &str, messages: &[&str]) -> WebSocketRequest {
    WebSocketRequest {
        url: url.into(),
        messages: messages.iter().map(|m| m.to_string()).collect(),
        ..Default::default()
    }
}

#[tokio::test]
async fn plaintext_exchange() {
    init();
    let url = serve_websocket(None, false).await;
    let messages = ["one", "two", "three"];
    let (status, got): (u16, WebSocketResponse) =
        post_json(app(), "/websocket", &req(&url, &messages)).await;
    assert_eq!(status, 200, "{got:?}");
    assert_eq!(
        (
            got.status_code,
            got.protocol.as_str(),
            got.tls,
            got.error.as_str()
        ),
        (101, "HTTP/1.1", false, "")
    );
    assert_eq!(got.messages.len(), messages.len());
    for (i, m) in got.messages.iter().enumerate() {
        assert_eq!(
            (m.kind, m.message.as_str()),
            (TEXT_MESSAGE, messages[i]),
            "messages[{i}]"
        );
    }
}

#[tokio::test]
async fn tls_exchange_and_verification_failure() {
    init();
    let tls = loopback_tls();
    let url = serve_websocket(Some(tls.server.clone()), false).await;
    let messages = ["tls-one", "tls-two", "tls-three"];
    let mut input = req(&url, &messages);
    input.root_ca = tls.root_pem.clone();
    let (status, got): (u16, WebSocketResponse) = post_json(app(), "/websocket", &input).await;
    assert_eq!(status, 200, "{got:?}");
    assert!(
        got.status_code == 101 && got.protocol == "HTTP/1.1" && got.tls && got.messages.len() == 3,
        "{got:?}"
    );

    let (status, _): (u16, WebSocketResponse) =
        post_json(app(), "/websocket", &req(&url, &messages)).await;
    assert_eq!(status, 502, "untrusted origin");
}

#[tokio::test]
async fn rejects_non_upgrade() {
    init();
    let url = serve(handler(|_| async { reply(200, "not websocket") }), None).await;
    let (status, got): (u16, WebSocketResponse) = post_json(
        app(),
        "/websocket",
        &req(&ws_url(&url), &["one", "two", "three"]),
    )
    .await;
    assert_eq!(status, 502);
    assert!(
        got.status_code == 200 && got.protocol == "HTTP/1.1" && !got.tls && !got.error.is_empty(),
        "{got:?}"
    );
}

#[tokio::test]
async fn preserves_bounded_handshake_body() {
    init();
    let oversized = "x".repeat(MAX_WEBSOCKET_HANDSHAKE_BODY + 1);
    for (name, body, prefix_only) in [
        (
            "exact denial body",
            "WebSocket egress is not supported".to_string(),
            false,
        ),
        ("oversized body", oversized, true),
    ] {
        let served = body.clone();
        let url = serve(
            handler(move |_| {
                let served = served.clone();
                async move { reply(403, served) }
            }),
            None,
        )
        .await;
        let (status, got): (u16, WebSocketResponse) =
            post_json(app(), "/websocket", &req(&ws_url(&url), &["probe"])).await;
        assert_eq!(status, 502, "{name}");
        assert_eq!(got.status_code, 403, "{name}");
        if prefix_only {
            let n = got.handshake_body.len();
            assert!(
                n > 0 && n <= MAX_WEBSOCKET_HANDSHAKE_BODY && body.starts_with(&got.handshake_body),
                "{name}: {n} bytes"
            );
        } else {
            assert_eq!(got.handshake_body, body, "{name}");
        }
    }
}

#[tokio::test]
async fn preserves_partial_observations_on_bad_response() {
    init();
    let url = serve_websocket(None, true).await;
    let (status, got): (u16, WebSocketResponse) =
        post_json(app(), "/websocket", &req(&url, &["one", "two", "three"])).await;
    assert_eq!(status, 502);
    assert_eq!(got.messages.len(), 1, "{got:?}");
    assert_eq!(
        (got.messages[0].kind, got.messages[0].message.as_str()),
        (TEXT_MESSAGE, "one")
    );
    assert!(!got.error.is_empty());
}

#[tokio::test]
async fn invalid_requests() {
    for (name, method, body, want) in [
        ("method", "GET", "{}", 405),
        ("malformed JSON", "POST", "{", 400),
        (
            "http scheme",
            "POST",
            r#"{"url":"http://h/","messages":["a"]}"#,
            400,
        ),
        (
            "no messages",
            "POST",
            r#"{"url":"ws://h/","messages":[]}"#,
            400,
        ),
    ] {
        let (status, body) = status_of(app(), method, "/websocket", body).await;
        assert_eq!(status, want, "{name}: {body}");
        let got: WebSocketResponse = serde_json::from_str(&body).unwrap();
        assert!(!got.error.is_empty(), "{name}");
    }
    let (_, body) = status_of(app(), "GET", "/websocket", "{}").await;
    assert!(
        body.contains(r#""messages":[]"#),
        "messages must always be present: {body}"
    );
}

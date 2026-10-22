//! The `websocket` subcommand: a WebSocket server on `/ws` that answers
//! `PING` with `PONG`, or with `--echo` returns every message as sent.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use http::{header, Request, Response, StatusCode};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use super::serve::{handler, text, Body, Handler};

fn is_upgrade(req: &Request<Incoming>) -> bool {
    let has_token = |name: header::HeaderName, token: &str| {
        req.headers().get_all(name).iter().any(|v| {
            v.to_str()
                .map(|s| s.split(',').any(|t| t.trim().eq_ignore_ascii_case(token)))
                .unwrap_or(false)
        })
    };
    has_token(header::CONNECTION, "upgrade") && has_token(header::UPGRADE, "websocket")
}

/// What the server sends back for a data message, or `None` for nothing.
fn reply(message: Message, echo: bool) -> Option<Message> {
    let payload: Bytes = match &message {
        Message::Text(t) => Bytes::copy_from_slice(t.as_bytes()),
        Message::Binary(b) => b.clone(),
        _ => return None,
    };
    if echo {
        return Some(message);
    }
    if payload.as_ref() != b"PING" {
        return None;
    }
    Some(match message {
        Message::Text(_) => Message::text("PONG"),
        _ => Message::binary(Bytes::from_static(b"PONG")),
    })
}

async fn converse(req: Request<Incoming>, echo: bool) {
    let upgraded = match hyper::upgrade::on(req).await {
        Ok(u) => u,
        Err(e) => {
            eprintln!("upgrade: {e}");
            return;
        }
    };
    let mut ws = WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, None).await;
    while let Some(next) = ws.next().await {
        let message = match next {
            Ok(m) => m,
            Err(e) => {
                eprintln!("read: {e}");
                break;
            }
        };
        if message.is_close() {
            break;
        }
        if let Some(response) = reply(message, echo) {
            if let Err(e) = ws.send(response).await {
                eprintln!("write: {e}");
                break;
            }
        }
    }
}

fn upgrade(req: Request<Incoming>, echo: bool) -> Response<Body> {
    if !is_upgrade(&req) {
        return text(StatusCode::BAD_REQUEST, "Bad Request\n");
    }
    let Some(key) = req.headers().get("sec-websocket-key") else {
        return text(StatusCode::BAD_REQUEST, "Bad Request\n");
    };
    let accept = derive_accept_key(key.as_bytes());
    tokio::spawn(converse(req, echo));
    let mut response = text(StatusCode::SWITCHING_PROTOCOLS, "");
    let headers = response.headers_mut();
    headers.insert(
        header::CONNECTION,
        http::HeaderValue::from_static("Upgrade"),
    );
    headers.insert(header::UPGRADE, http::HeaderValue::from_static("websocket"));
    headers.insert(
        header::SEC_WEBSOCKET_ACCEPT,
        http::HeaderValue::from_str(&accept).expect("accept key is base64"),
    );
    response
}

/// The server's routes: `/readyz` and `/ws`. Any origin is accepted.
pub fn websocket_handler(echo: bool) -> Handler {
    handler(move |req: Request<Incoming>| async move {
        match req.uri().path() {
            "/readyz" => text(StatusCode::OK, "ok"),
            "/ws" => upgrade(req, echo),
            _ => text(StatusCode::NOT_FOUND, "404 page not found\n"),
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::net::TcpStream;
    use tokio::time::timeout;

    use super::super::serve::testing::{start_origin_server, write_origin_certificate};
    use super::*;

    const DEADLINE: Duration = Duration::from_secs(2);

    #[tokio::test]
    async fn default_responds_to_ping_with_pong() {
        let server = start_origin_server(websocket_handler(false), None).await;
        let host = server.ws_url.trim_start_matches("ws://").to_string();
        let tcp = TcpStream::connect(&host).await.unwrap();
        let (mut ws, response) = timeout(
            DEADLINE,
            tokio_tungstenite::client_async(format!("{}/ws", server.ws_url), tcp),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        timeout(DEADLINE, ws.send(Message::text("PING")))
            .await
            .unwrap()
            .unwrap();
        let got = timeout(DEADLINE, ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(got, Message::text("PONG"), "PING response");
    }

    #[tokio::test]
    async fn echo_preserves_message_type_and_payload() {
        let cert = write_origin_certificate();
        let server = start_origin_server(websocket_handler(true), Some(&cert)).await;
        let mut roots = rustls::RootCertStore::empty();
        for c in rustls_pemfile::certs(&mut cert.ca_pem.as_slice()) {
            roots.add(c.unwrap()).unwrap();
        }
        let config =
            rustls::ClientConfig::builder_with_provider(super::super::tls::crypto_provider())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
        let host = server.wss_url.trim_start_matches("wss://").to_string();
        let tcp = TcpStream::connect(&host).await.unwrap();
        let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(
                rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap(),
                tcp,
            )
            .await
            .expect("dial secure websocket");
        let (mut ws, response) = timeout(
            DEADLINE,
            tokio_tungstenite::client_async(format!("{}/ws", server.wss_url), tls),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        for want in [
            Message::text("first message"),
            Message::binary(Bytes::from_static(&[0, 1, 2, 255])),
            Message::text("PING"),
        ] {
            timeout(DEADLINE, ws.send(want.clone()))
                .await
                .unwrap()
                .unwrap();
            let got = timeout(DEADLINE, ws.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(got, want, "echo keeps type and payload");
        }
    }
}

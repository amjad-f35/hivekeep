//! The `/connect` endpoint against a proxy stand-in.

mod support;

use std::time::Instant;

use hivekeep_demo_egress::limits::REQUEST_TIMEOUT;
use hivekeep_demo_egress::wire::{ConnectRequest, ConnectResponse};
use support::*;

fn req(proxy: &str, target: &str) -> ConnectRequest {
    ConnectRequest {
        proxy: proxy.into(),
        target: target.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn reports_refusal() {
    init();
    let (proxy, received) = proxy_stub(
        "HTTP/1.1 403 Forbidden\r\nContent-Length: 6\r\n\r\ndenied",
        None,
    )
    .await;
    let (status, got): (u16, ConnectResponse) =
        post_json(app(), "/connect", &req(&proxy, "example.com:443")).await;
    assert_eq!(status, 200, "{got:?}");
    assert_eq!(
        (
            got.status_code,
            got.body.as_str(),
            got.protocol.as_str(),
            got.tls,
            got.error.as_str()
        ),
        (403, "denied", "HTTP/1.1", false, "")
    );
    assert_eq!(
        *received.lock().unwrap(),
        vec!["CONNECT example.com:443".to_string()]
    );
}

/// A 2xx has no body: the connection is the tunnel, and the stub sends
/// nothing more, so a handler that waited for one would hang.
#[tokio::test]
async fn reports_opened_tunnel_without_waiting_for_body() {
    init();
    let (proxy, _) = proxy_stub("HTTP/1.1 200 Connection established\r\n\r\n", None).await;
    let start = Instant::now();
    let (status, got): (u16, ConnectResponse) =
        post_json(app(), "/connect", &req(&proxy, "example.com:443")).await;
    assert!(
        start.elapsed() < REQUEST_TIMEOUT / 2,
        "answering a 200 took {:?}",
        start.elapsed()
    );
    assert_eq!(status, 200);
    assert!(
        got.status_code == 200 && got.body.is_empty() && got.error.is_empty(),
        "{got:?}"
    );
}

#[tokio::test]
async fn over_tls() {
    init();
    let tls = loopback_tls();
    let (proxy, received) = proxy_stub(
        "HTTP/1.1 403 Forbidden\r\nContent-Length: 16\r\n\r\nno tunnels here\n",
        Some(tls.server.clone()),
    )
    .await;

    let mut input = req(&proxy, "example.com:443");
    input.tls = true;
    input.root_ca = tls.root_pem.clone();
    let (status, got): (u16, ConnectResponse) = post_json(app(), "/connect", &input).await;
    assert_eq!(status, 200, "{got:?}");
    assert!(
        got.status_code == 403
            && got.tls
            && got.body.trim() == "no tunnels here"
            && got.error.is_empty(),
        "{got:?}"
    );
    assert_eq!(
        received.lock().unwrap().first().map(String::as_str),
        Some("CONNECT example.com:443")
    );

    // Without the root the handshake fails: no answer, not an answer.
    input.root_ca.clear();
    let (status, got): (u16, ConnectResponse) = post_json(app(), "/connect", &input).await;
    assert_eq!(status, 502);
    assert!(
        got.status_code == 0 && got.error.contains("TLS handshake"),
        "{got:?}"
    );
}

/// A rootCA that is not a certificate is refused before dialing: the proxy
/// address refuses connections, so dialing first would report that instead.
#[tokio::test]
async fn rejects_bad_root_ca_before_dialing() {
    let proxy = closed_port().await;
    let input = ConnectRequest {
        proxy,
        target: "example.com:443".into(),
        tls: true,
        root_ca: "not-a-pem".into(),
    };
    let (status, got): (u16, ConnectResponse) = post_json(app(), "/connect", &input).await;
    assert_eq!(status, 400);
    assert!(
        got.error.contains("rootCA") && !got.error.contains("dialing"),
        "{}",
        got.error
    );
}

#[tokio::test]
async fn reports_unreachable_proxy() {
    let proxy = closed_port().await;
    let (status, got): (u16, ConnectResponse) =
        post_json(app(), "/connect", &req(&proxy, "example.com:443")).await;
    assert_eq!(status, 502);
    assert!(
        got.error.contains(&format!("dialing proxy {proxy}")),
        "{}",
        got.error
    );
}

#[tokio::test]
async fn rejects_invalid_requests() {
    for (name, method, body, want) in [
        (
            "GET",
            "GET",
            r#"{"proxy":"127.0.0.1:3128","target":"example.com:443"}"#,
            405,
        ),
        ("not JSON", "POST", "proxy", 400),
        ("no proxy", "POST", r#"{"target":"example.com:443"}"#, 400),
        (
            "proxy without port",
            "POST",
            r#"{"proxy":"127.0.0.1","target":"example.com:443"}"#,
            400,
        ),
        ("no target", "POST", r#"{"proxy":"127.0.0.1:3128"}"#, 400),
        (
            "target with scheme",
            "POST",
            r#"{"proxy":"127.0.0.1:3128","target":"https://example.com:443"}"#,
            400,
        ),
    ] {
        let (status, body) = status_of(app(), method, "/connect", body).await;
        assert_eq!(status, want, "{name}: {body}");
    }
}

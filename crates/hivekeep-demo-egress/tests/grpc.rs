//! The `/grpc` endpoint against an in-process Echo server, where there is
//! no tunnel to blame.

mod support;

use hivekeep_demo_egress::wire::GrpcResponse;
use support::*;

async fn post_grpc(body: String) -> (u16, GrpcResponse) {
    let (status, _, raw) = call(app(), "POST", "/grpc", body, &[]).await;
    let decoded =
        serde_json::from_slice(&raw).unwrap_or_else(|e| panic!("decoding (HTTP {status}): {e}"));
    (status, decoded)
}

#[tokio::test]
async fn unary() {
    init();
    let target = start_echo_server().await;
    let (status, got) = post_grpc(format!(r#"{{"target":"{target}","message":"hello"}}"#)).await;
    assert_eq!(status, 200, "{got:?}");
    assert_eq!(got.message, "hello");
    assert_eq!(got.code, "OK");
    // No stream was asked for, so none may be reported.
    assert!(got.stream.is_none(), "{got:?}");
}

#[tokio::test]
async fn server_stream() {
    init();
    let target = start_echo_server().await;
    let (status, got) = post_grpc(format!(
        r#"{{"target":"{target}","message":"streamed","streamCount":3}}"#
    ))
    .await;
    assert_eq!(status, 200, "{got:?}");
    let stream = got.stream.expect("stream");
    assert_eq!(stream.len(), 3);
    for (i, m) in stream.iter().enumerate() {
        assert_eq!(
            (m.message.as_str(), m.index),
            ("streamed", i as i32),
            "stream[{i}]"
        );
    }
}

#[tokio::test]
async fn bidi_stream() {
    init();
    let target = start_echo_server().await;
    let (status, got) = post_grpc(format!(
        r#"{{"target":"{target}","message":"duplex","bidiCount":3}}"#
    ))
    .await;
    assert_eq!(status, 200, "{got:?}");
    let bidi = got.bidi.clone().expect("bidi");
    assert_eq!(bidi.len(), 3);
    // The handler numbers its own messages, so each echo is pinned to the
    // request it answered.
    for (i, m) in bidi.iter().enumerate() {
        assert_eq!(
            (m.message.clone(), m.index),
            (format!("duplex-{i}"), i as i32),
            "bidi[{i}]"
        );
    }
    assert!(got.stream.is_none(), "{got:?}");
}

/// A failed RPC still reports its gRPC code; that code is the proof that
/// trailers arrived and must not collapse into the HTTP status.
#[tokio::test]
async fn failure_reports_status_code() {
    init();
    let target = closed_port().await;
    let (status, got) = post_grpc(format!(r#"{{"target":"{target}","message":"hello"}}"#)).await;
    assert_eq!(status, 502);
    assert_eq!(got.code, "Unavailable", "error = {}", got.error);
}

#[tokio::test]
async fn invalid_requests() {
    for (name, method, body, want) in [
        ("method", "GET", "{}", 405),
        ("malformed JSON", "POST", "{", 400),
        ("missing target", "POST", r#"{"message":"hi"}"#, 400),
        ("no port", "POST", r#"{"target":"grpcecho"}"#, 400),
        ("no host", "POST", r#"{"target":":50051"}"#, 400),
        (
            "non-numeric port",
            "POST",
            r#"{"target":"grpcecho:grpc"}"#,
            400,
        ),
        (
            "URL not host:port",
            "POST",
            r#"{"target":"http://grpcecho:50051"}"#,
            400,
        ),
    ] {
        let (status, body) = status_of(app(), method, "/grpc", body).await;
        assert_eq!(status, want, "{name}: {body}");
    }
}

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::test_pki::TestCa;
use crate::tunnel::egress::test_support::*;

fn h(
    f: impl Fn(H2Request) -> futures::future::BoxFuture<'static, ()> + Send + Sync + 'static,
) -> H2Handler {
    Arc::new(f)
}

async fn send_all(s: &mut h2::SendStream<Bytes>, data: &[u8]) {
    s.send_data(Bytes::copy_from_slice(data), false).unwrap();
}

#[tokio::test]
async fn dial_context_http1() {
    let ca = TestCa::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let addr = serve_connect_gateway(&ca, move |mut conn, req| {
        Box::pin(async move {
            tx.send(req).unwrap();
            conn.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\nhello")
                .await
                .unwrap();
            let mut p = [0u8; 4];
            conn.read_exact(&mut p).await.unwrap();
            assert_eq!(&p, b"ping");
        })
    })
    .await;
    let (client, _, _d) = new_test_client(&ca, dial_fixed(addr));
    let mut conn = client.dial("192.0.2.10:443").await.unwrap();
    let req = recv_within(&mut rx, "CONNECT request").await;
    assert_eq!(req.method, "CONNECT");
    assert_eq!(req.target, "192.0.2.10:443");
    assert_eq!(req.host(), Some("192.0.2.10:443"));
    assert!(req
        .headers
        .iter()
        .all(|(k, _)| !k.starts_with("x-hk-") && k != "authorization"));
    let mut hello = [0u8; 5];
    conn.read_exact(&mut hello).await.unwrap();
    assert_eq!(&hello, b"hello");
    conn.write_all(b"ping").await.unwrap();
}

#[tokio::test]
async fn dial_context_http2() {
    let ca = TestCa::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let tx = Arc::new(tx);
    let (addr, _) = serve_h2_gateway(
        &ca,
        h(move |mut r| {
            let tx = tx.clone();
            Box::pin(async move {
                let _ = tx.send((
                    r.head.method.clone(),
                    r.authority(),
                    r.head.headers.clone(),
                    r.head.version,
                ));
                let mut s = r.ok();
                send_all(&mut s, b"hello").await;
                assert_eq!(read_body(&mut r.body).await, b"ping");
                s.send_data(Bytes::from_static(b"pong"), true).unwrap();
            })
        }),
    )
    .await;
    let (client, _, _d) = new_test_client(&ca, dial_fixed(addr));
    let mut conn = client.dial("192.0.2.10:443").await.unwrap();
    let (method, authority, headers, version) = recv_within(&mut rx, "HTTP/2 CONNECT").await;
    assert_eq!(version, http::Version::HTTP_2);
    assert_eq!(method, http::Method::CONNECT);
    assert_eq!(authority, "192.0.2.10:443");
    assert!(headers.get("authorization").is_none());
    assert!(headers.keys().all(|k| !k.as_str().starts_with("x-hk-")));
    let mut hello = [0u8; 5];
    conn.read_exact(&mut hello).await.unwrap();
    assert_eq!(&hello, b"hello");
    conn.write_all(b"ping").await.unwrap();
    conn.shutdown().await.unwrap();
    let mut reply = Vec::new();
    conn.read_to_end(&mut reply).await.unwrap();
    assert_eq!(reply, b"pong");
}

#[tokio::test]
async fn http2_multiplexes_streams() {
    let ca = TestCa::new();
    const N: u32 = 4;
    let opened = Arc::new(AtomicU32::new(0));
    let all_open = Arc::new(tokio::sync::Notify::new());
    let (addr, _) = serve_h2_gateway(
        &ca,
        h(move |mut r| {
            let (opened, all_open) = (opened.clone(), all_open.clone());
            Box::pin(async move {
                let mut s = r.ok();
                if opened.fetch_add(1, Ordering::SeqCst) + 1 == N {
                    all_open.notify_waiters();
                } else {
                    let notified = all_open.notified();
                    if opened.load(Ordering::SeqCst) < N {
                        notified.await;
                    }
                }
                let data = read_body(&mut r.body).await;
                let msg = format!("{}:{}", r.authority(), String::from_utf8_lossy(&data));
                s.send_data(Bytes::from(msg), true).unwrap();
            })
        }),
    )
    .await;
    let dials = Arc::new(AtomicU32::new(0));
    let (ctx, _crx) = tokio::sync::mpsc::unbounded_channel();
    let (client, _, _d) = new_test_client(&ca, dial_counting(addr, dials.clone(), u32::MAX, ctx));
    let client = Arc::new(client);
    let mut tasks = Vec::new();
    for i in 0..N {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            let dest = format!("192.0.2.{}:443", 10 + i);
            let mut c = client.dial(&dest).await.unwrap();
            let msg = format!("msg-{i}");
            c.write_all(msg.as_bytes()).await.unwrap();
            c.shutdown().await.unwrap();
            let mut got = Vec::new();
            c.read_to_end(&mut got).await.unwrap();
            assert_eq!(String::from_utf8(got).unwrap(), format!("{dest}:{msg}"));
        }));
    }
    for t in tasks {
        tokio::time::timeout(Duration::from_secs(10), t)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(
        dials.load(Ordering::SeqCst),
        1,
        "one shared HTTP/2 connection"
    );
}

fn echo_handler() -> H2Handler {
    h(|mut r| {
        Box::pin(async move {
            let mut s = r.ok();
            let data = read_body(&mut r.body).await;
            let _ = s.send_data(Bytes::from(data), true);
        })
    })
}

#[tokio::test]
async fn http2_reconnects_after_connection_close() {
    let ca = TestCa::new();
    let (addr, close_active) = serve_h2_gateway(&ca, echo_handler()).await;
    let dials = Arc::new(AtomicU32::new(0));
    let (ctx, _crx) = tokio::sync::mpsc::unbounded_channel();
    let (client, _, _d) = new_test_client(&ca, dial_counting(addr, dials.clone(), u32::MAX, ctx));
    drop(client.dial("192.0.2.10:443").await.unwrap());
    close_active();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut c = client
        .dial("192.0.2.10:443")
        .await
        .expect("second dial after the gateway closed");
    c.write_all(b"after-reconnect").await.unwrap();
    c.shutdown().await.unwrap();
    let mut got = Vec::new();
    c.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, b"after-reconnect");
    assert_eq!(dials.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn http2_retires_connection_after_renewal() {
    let ca = TestCa::new();
    let (stx, mut srx) = tokio::sync::mpsc::unbounded_channel();
    let stx = Arc::new(stx);
    let (addr, _) = serve_h2_gateway(
        &ca,
        h(move |mut r| {
            let stx = stx.clone();
            Box::pin(async move {
                let _ = stx.send(r.peer_leaf.clone());
                let mut s = r.ok();
                let data = read_body(&mut r.body).await;
                let _ = s.send_data(Bytes::from(data), true);
            })
        }),
    )
    .await;
    let dials = Arc::new(AtomicU32::new(0));
    let (ctx, mut crx) = tokio::sync::mpsc::unbounded_channel();
    let (client, cert, _d) = new_test_client(&ca, dial_counting(addr, dials.clone(), 1, ctx));
    let mut before = client.dial("192.0.2.10:443").await.unwrap();
    let first = recv_within(&mut srx, "CONNECT before the renewal").await;
    *cert.0.lock() = actor_leaf(&ca);
    let _after = client.dial("192.0.2.11:443").await.unwrap();
    let second = recv_within(&mut srx, "CONNECT after the renewal").await;
    assert_ne!(first, second, "the renewed certificate is presented");
    assert_eq!(dials.load(Ordering::SeqCst), 2);
    assert!(
        crx.try_recv().is_err(),
        "first connection closed while its stream was open"
    );
    before.write_all(b"still-open").await.unwrap();
    before.shutdown().await.unwrap();
    let mut got = Vec::new();
    before.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, b"still-open");
    drop(before);
    recv_within(
        &mut crx,
        "retired connection to close after its last stream",
    )
    .await;
}

#[tokio::test]
async fn http2_read_timeout_keeps_the_stream() {
    let ca = TestCa::new();
    let release = Arc::new(tokio::sync::Notify::new());
    let rel = release.clone();
    let (addr, _) = serve_h2_gateway(
        &ca,
        h(move |mut r| {
            let rel = rel.clone();
            Box::pin(async move {
                let mut s = r.ok();
                rel.notified().await;
                let _ = s.send_data(Bytes::from_static(b"delayed"), false);
                let _ = read_body(&mut r.body).await;
            })
        }),
    )
    .await;
    let (client, _, _d) = new_test_client(&ca, dial_fixed(addr));
    let mut conn = client.dial("192.0.2.10:443").await.unwrap();
    let mut buf = [0u8; 7];
    assert!(
        tokio::time::timeout(Duration::from_millis(20), conn.read(&mut buf))
            .await
            .is_err(),
        "read must time out"
    );
    release.notify_one();
    conn.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"delayed");
}

#[tokio::test]
async fn dial_context_rejected_http1() {
    let ca = TestCa::new();
    let addr = serve_connect_gateway(&ca, |mut conn, _| {
        Box::pin(async move {
            let body = "denied by policy";
            let _ = conn
                .write_all(
                    format!(
                        "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await;
        })
    })
    .await;
    let (client, _, _d) = new_test_client(&ca, dial_fixed(addr));
    let err = client.dial("192.0.2.10:443").await.err().unwrap();
    assert!(err.to_string().contains("denied by policy"), "{err}");
}

#[tokio::test]
async fn dial_context_rejected_http2() {
    let ca = TestCa::new();
    let (addr, _) = serve_h2_gateway(
        &ca,
        h(|mut r| {
            Box::pin(async move {
                if r.authority() == "192.0.2.10:443" {
                    r.error(403, "denied by policy");
                    return;
                }
                let _s = r.ok();
                let _ = read_body(&mut r.body).await;
            })
        }),
    )
    .await;
    let dials = Arc::new(AtomicU32::new(0));
    let (ctx, _crx) = tokio::sync::mpsc::unbounded_channel();
    let (client, _, _d) = new_test_client(&ca, dial_counting(addr, dials.clone(), u32::MAX, ctx));
    match client.dial("192.0.2.10:443").await.err().unwrap() {
        TunnelError::ConnectRejected {
            status, message, ..
        } => {
            assert_eq!(status, 403);
            assert_eq!(message, "denied by policy");
        }
        other => panic!("want ConnectRejected, got {other}"),
    }
    drop(
        client
            .dial("192.0.2.11:443")
            .await
            .expect("subsequent dial"),
    );
    assert_eq!(dials.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn gateway_refuses_client_certificate() {
    use rustls::version::{TLS12, TLS13};
    let cases: [(&str, &'static rustls::SupportedProtocolVersion, &[&[u8]]); 4] = [
        (
            "TLS 1.3 HTTP/1.1 refuses after the client handshake completes",
            &TLS13,
            &[],
        ),
        ("TLS 1.2 HTTP/1.1 refuses during the handshake", &TLS12, &[]),
        (
            "TLS 1.3 HTTP/2 refuses after the client handshake completes",
            &TLS13,
            &[b"h2"],
        ),
        (
            "TLS 1.2 HTTP/2 refuses during the handshake",
            &TLS12,
            &[b"h2"],
        ),
    ];
    for (name, version, alpn) in cases {
        let ca = TestCa::new();
        let addr = serve_refusing_gateway(&ca, &TestCa::new(), &[version], alpn).await;
        let (client, _, _d) = new_test_client(&ca, dial_fixed(addr));
        let err = client
            .dial("192.0.2.10:443")
            .await
            .err()
            .unwrap_or_else(|| panic!("{name}: dial succeeded"));
        assert!(err.is_gateway_handshake(), "{name}: {err:?}");
        assert!(err.rejected_status().is_none(), "{name}");
    }
}

#[tokio::test]
async fn gateway_hangs_up_before_responding_http1() {
    let ca = TestCa::new();
    let addr = serve_connect_gateway(&ca, |conn, _| Box::pin(async move { drop(conn) })).await;
    let (client, _, _d) = new_test_client(&ca, dial_fixed(addr));
    let err = client.dial("192.0.2.10:443").await.err().unwrap();
    assert!(err.is_gateway_handshake(), "{err:?}");
}

#[tokio::test]
async fn gateway_hangs_up_before_responding_http2() {
    let ca = TestCa::new();
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let cfg = crate::test_pki::server_config(
        &ca.server("egress.test"),
        Some(&ca),
        &[b"h2"],
        crate::test_pki::ALL_VERSIONS,
    );
    tokio::spawn(async move {
        let (tcp, _) = lis.accept().await.unwrap();
        if let Ok(mut tls) = tokio_rustls::TlsAcceptor::from(cfg).accept(tcp).await {
            let _ = tls.shutdown().await;
        }
    });
    let (client, _, _d) = new_test_client(&ca, dial_fixed(addr));
    let err = client.dial("192.0.2.10:443").await.err().unwrap();
    assert!(err.is_gateway_handshake(), "{err:?}");
}

use tokio::net::TcpListener;

#[tokio::test]
async fn dial_context_validates_input() {
    let ca = TestCa::new();
    let (client, _, _d) = new_test_client(&ca, dial_fixed("127.0.0.1:1".into()));
    for dest in ["192.0.2.10", "example.com:443"] {
        assert!(
            matches!(
                client.dial(dest).await,
                Err(TunnelError::InvalidDestination { .. })
            ),
            "{dest}"
        );
    }
}

#[tokio::test]
async fn client_close() {
    let ca = TestCa::new();
    let (addr, _) = serve_h2_gateway(&ca, echo_handler()).await;
    let dials = Arc::new(AtomicU32::new(0));
    let (ctx, mut crx) = tokio::sync::mpsc::unbounded_channel();
    let (client, _, _d) = new_test_client(&ca, dial_counting(addr, dials, 0, ctx));
    drop(client.dial("192.0.2.10:443").await.unwrap());
    client.close();
    recv_within(&mut crx, "idle HTTP/2 connection closed on close").await;
    assert!(matches!(
        client.dial("192.0.2.10:443").await,
        Err(TunnelError::Closed)
    ));
}

#[tokio::test]
async fn copy_both_ways_http2_stream() {
    let ca = TestCa::new();
    let (client, _, _d) = new_test_client(&ca, dial_fixed(serve_h2_echo_gateway(&ca).await));
    let upstream = client.dial("192.0.2.10:443").await.unwrap();
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut app = tokio::net::TcpStream::connect(lis.local_addr().unwrap())
        .await
        .unwrap();
    let (downstream, _) = lis.accept().await.unwrap();
    let copied = tokio::spawn(crate::tunnel::egress::proxy::copy_both_ways(
        Box::new(downstream),
        upstream,
    ));
    let mut sent = Vec::new();
    use crate::tunnel::egress::stream_copy::{BUF_SIZE, SMALL_BUF_SIZE};
    for (i, size) in [1, SMALL_BUF_SIZE, SMALL_BUF_SIZE + 1, BUF_SIZE, 3 << 20]
        .into_iter()
        .enumerate()
    {
        sent.extend((0..size).map(|j| (i + j) as u8));
    }
    let (mut ar, mut aw) = app.split();
    let want = sent.clone();
    let (got, wrote) = tokio::join!(
        async {
            let mut got = Vec::new();
            ar.read_to_end(&mut got).await.unwrap();
            got
        },
        async {
            aw.write_all(&sent).await.unwrap();
            aw.shutdown().await
        }
    );
    wrote.unwrap();
    assert_eq!(got.len(), want.len());
    assert!(got == want, "echo differs");
    tokio::time::timeout(Duration::from_secs(5), copied)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn h2_stream_copy_honors_a_timeout() {
    let ca = TestCa::new();
    let release = Arc::new(tokio::sync::Notify::new());
    let rel = release.clone();
    let (addr, _) = serve_h2_gateway(
        &ca,
        h(move |mut r| {
            let rel = rel.clone();
            Box::pin(async move {
                let mut s = r.ok();
                rel.notified().await;
                let _ = s.send_data(Bytes::from_static(b"delayed"), true);
                let _ = read_body(&mut r.body).await;
            })
        }),
    )
    .await;
    let (client, _, _d) = new_test_client(&ca, dial_fixed(addr));
    let mut conn = client.dial("192.0.2.10:443").await.unwrap();
    let mut sink = Vec::new();
    let r = tokio::time::timeout(
        Duration::from_millis(20),
        crate::tunnel::egress::stream_copy::stream_copy(&mut conn, &mut sink),
    )
    .await;
    assert!(r.is_err(), "copy must time out");
    release.notify_one();
    crate::tunnel::egress::stream_copy::stream_copy(&mut conn, &mut sink)
        .await
        .unwrap();
    assert_eq!(sink, b"delayed");
}

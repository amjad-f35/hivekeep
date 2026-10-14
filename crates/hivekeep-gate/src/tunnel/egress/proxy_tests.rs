use std::sync::atomic::{AtomicU32, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::{mpsc, Notify};

use super::*;
use crate::tunnel::egress::test_support::*;
use crate::tunnel::egress::{MintError, TunnelError};

const UID: &str = "actor-uid-1";

struct FnDialer<F>(F);

impl<F> TunnelDialer for FnDialer<F>
where
    F: Fn(&str) -> Result<BoxIo, TunnelError> + Send + Sync + 'static,
{
    fn dial<'a>(&'a self, destination: &'a str) -> BoxFuture<'a, Result<BoxIo, TunnelError>> {
        let r = (self.0)(destination);
        Box::pin(async move { r })
    }
    fn close(&self) {}
}

fn failing_dialer(
    tx: mpsc::UnboundedSender<&'static str>,
    generation: &'static str,
) -> Arc<dyn TunnelDialer> {
    Arc::new(FnDialer(move |_: &str| {
        let _ = tx.send(generation);
        Err(TunnelError::Canceled)
    }))
}

#[derive(Clone, Default)]
struct FakeSource {
    expires_at: Option<SystemTime>,
    err: Option<MintError>,
    calls: Arc<AtomicU32>,
    called: Option<mpsc::UnboundedSender<()>>,
    release: Option<Arc<Notify>>,
}

impl ActorCertificateSource for FakeSource {
    fn mint(&self) -> BoxFuture<'_, Result<SystemTime, MintError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(c) = &self.called {
                let _ = c.send(());
            }
            if let Some(r) = &self.release {
                r.notified().await;
            }
            match &self.err {
                Some(e) => Err(e.clone()),
                None => Ok(self.expires_at.unwrap_or(SystemTime::UNIX_EPOCH)),
            }
        })
    }
}

fn hour() -> SystemTime {
    SystemTime::now() + Duration::from_secs(3600)
}

fn fixed_destination(dest: &'static str) -> OriginalDestination {
    Arc::new(move |_| Ok(dest.to_string()))
}

fn pipe() -> (DuplexStream, BoxIo) {
    let (a, b) = tokio::io::duplex(64 << 10);
    (a, Box::new(b))
}

async fn closed_within(conn: &mut (impl AsyncReadExt + Unpin)) -> bool {
    let mut b = [0u8; 1];
    matches!(
        tokio::time::timeout(Duration::from_secs(1), conn.read(&mut b)).await,
        Ok(Ok(0)) | Ok(Err(_))
    )
}

/// An acceptor that signals each accept and waits for a release before
/// handing the connection over.
struct DelayedAccept {
    inner: tokio::net::TcpListener,
    accepted: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
}

impl Accept for DelayedAccept {
    fn accept(&mut self) -> BoxFuture<'_, io::Result<Option<BoxIo>>> {
        Box::pin(async move {
            let (c, _) = self.inner.accept().await?;
            let _ = self.accepted.send(());
            self.release.notified().await;
            Ok(Some(Box::new(c) as BoxIo))
        })
    }
}

#[tokio::test]
async fn binding_does_not_outlive_its_activation() {
    let proxy = Proxy::new(fixed_destination("192.0.2.10:443"));
    let (dtx, mut dialed) = mpsc::unbounded_channel();
    let old_serve = proxy.bind(UID).unwrap();
    proxy
        .activate(
            UID,
            failing_dialer(dtx.clone(), "old"),
            Arc::new(FakeSource::default()),
            hour(),
        )
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (atx, mut accepted) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let old_listener = DelayedAccept {
        inner: listener,
        accepted: atx,
        release: release.clone(),
    };
    let old_task = tokio::spawn(old_serve.serve(old_listener, CancellationToken::new()));
    let mut old_conn = tokio::net::TcpStream::connect(addr).await.unwrap();
    recv_within(&mut accepted, "old socket accepted").await;
    proxy.deactivate(UID).await;
    let new_serve = proxy.bind(UID).unwrap();
    proxy
        .activate(
            UID,
            failing_dialer(dtx, "new"),
            Arc::new(FakeSource::default()),
            hour(),
        )
        .unwrap();
    release.notify_one();
    assert!(
        closed_within(&mut old_conn).await,
        "old connection was not closed"
    );
    tokio::time::timeout(Duration::from_secs(5), old_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(dialed.try_recv().is_err(), "old listener used credentials");
    let new_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let new_addr = new_listener.local_addr().unwrap();
    let new_task = tokio::spawn(new_serve.serve(new_listener, CancellationToken::new()));
    let _c = tokio::net::TcpStream::connect(new_addr).await.unwrap();
    assert_eq!(recv_within(&mut dialed, "new activation dial").await, "new");
    proxy.deactivate(UID).await;
    tokio::time::timeout(Duration::from_secs(5), new_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn late_serve_of_a_deactivated_binding_closes() {
    let proxy = Proxy::new(fixed_destination("192.0.2.10:443"));
    let (dtx, mut dialed) = mpsc::unbounded_channel();
    let old_serve = proxy.bind(UID).unwrap();
    proxy
        .activate(
            UID,
            failing_dialer(dtx.clone(), "old"),
            Arc::new(FakeSource::default()),
            hour(),
        )
        .unwrap();
    proxy.deactivate(UID).await;
    let _new = proxy.bind(UID).unwrap();
    proxy
        .activate(
            UID,
            failing_dialer(dtx, "new"),
            Arc::new(FakeSource::default()),
            hour(),
        )
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut conn = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    old_serve
        .serve(listener, CancellationToken::new())
        .await
        .unwrap();
    assert!(
        closed_within(&mut conn).await,
        "old connection was not closed"
    );
    assert!(dialed.try_recv().is_err(), "old listener used credentials");
    proxy.deactivate(UID).await;
}

struct ClosedListener;
impl Accept for ClosedListener {
    fn accept(&mut self) -> BoxFuture<'_, io::Result<Option<BoxIo>>> {
        Box::pin(async { Ok(None) })
    }
}

#[tokio::test]
async fn unactivated_binding_closes() {
    let proxy = Proxy::new(fixed_destination(""));
    assert!(proxy.bind("").is_err());
    let serve = proxy.bind(UID).unwrap();
    serve
        .serve(ClosedListener, CancellationToken::new())
        .await
        .unwrap();
    assert!(
        !proxy.is_armed(UID),
        "closed listener retained an unactivated binding"
    );
}

#[tokio::test]
async fn activation_fails_closed() {
    let proxy = Proxy::new(fixed_destination(""));
    let dialer: Arc<dyn TunnelDialer> =
        Arc::new(FnDialer(|_: &str| -> Result<BoxIo, TunnelError> {
            panic!("dialed after failed activation")
        }));
    let src = FakeSource {
        err: Some(MintError::other("renewal failed")),
        ..Default::default()
    };
    assert!(proxy
        .activate(UID, dialer, Arc::new(src), SystemTime::UNIX_EPOCH)
        .is_err());
    let (mut actor, p) = pipe();
    proxy.handle_for(p, UID);
    assert!(
        closed_within(&mut actor).await,
        "failed activation admitted egress"
    );
}

#[tokio::test]
async fn expiry_rejects_new_but_preserves_established() {
    let (up_proxy, mut up_gateway) = tokio::io::duplex(1024);
    let up = Arc::new(parking_lot::Mutex::new(Some(up_proxy)));
    let dials = Arc::new(AtomicU32::new(0));
    let d2 = dials.clone();
    let dialer: Arc<dyn TunnelDialer> = Arc::new(FnDialer(move |_: &str| {
        d2.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(up.lock().take().unwrap()) as BoxIo)
    }));
    let proxy = Proxy::new(fixed_destination("192.0.2.10:443"));
    let src = FakeSource {
        err: Some(MintError::other("renewal failed")),
        ..Default::default()
    };
    let mints = src.calls.clone();
    proxy
        .activate(
            UID,
            dialer,
            Arc::new(src),
            SystemTime::now() + Duration::from_millis(50),
        )
        .unwrap();
    let (mut actor, p) = pipe();
    proxy.handle_for(p, UID);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while dials.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(dials.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    actor.write_all(b"still-open").await.unwrap();
    let mut buf = [0u8; 10];
    up_gateway
        .read_exact(&mut buf)
        .await
        .expect("established tunnel closed after expiry");
    let (mut new_actor, np) = pipe();
    proxy.handle_for(np, UID);
    assert!(
        closed_within(&mut new_actor).await,
        "new tunnel admitted after expiry"
    );
    assert_eq!(dials.load(Ordering::SeqCst), 1);
    assert!(
        mints.load(Ordering::SeqCst) <= 3,
        "retry loop spun near expiry"
    );
    proxy.deactivate(UID).await;
}

#[tokio::test]
async fn renews_before_expiry() {
    let (tx, mut renewed) = mpsc::unbounded_channel();
    let renewed_expiry = hour();
    let src = FakeSource {
        expires_at: Some(renewed_expiry),
        called: Some(tx),
        ..Default::default()
    };
    let dialer: Arc<dyn TunnelDialer> = Arc::new(FnDialer(|_: &str| Err(TunnelError::Canceled)));
    let proxy = Proxy::new(fixed_destination("192.0.2.10:443"));
    proxy
        .activate(
            UID,
            dialer,
            Arc::new(src),
            SystemTime::now() + Duration::from_millis(80),
        )
        .unwrap();
    recv_within(&mut renewed, "renewal").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while proxy.expires_at(UID) != Some(renewed_expiry) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "renewed expiry was not installed"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    proxy.deactivate(UID).await;
}

#[test]
fn retries_renewal_after_expiry() {
    for _ in 0..100 {
        let got = retry_after(SystemTime::now() - Duration::from_secs(1));
        assert!(
            got >= Duration::from_secs(25) && got < Duration::from_secs(35),
            "{got:?}"
        );
    }
}

#[tokio::test]
async fn stops_after_terminal_renewal_failure() {
    for code in [
        tonic::Code::Aborted,
        tonic::Code::FailedPrecondition,
        tonic::Code::PermissionDenied,
    ] {
        let (tx, mut called) = mpsc::unbounded_channel();
        let src = FakeSource {
            err: Some(MintError {
                code: Some(code),
                message: "stale activation".into(),
            }),
            called: Some(tx),
            ..Default::default()
        };
        let dialer: Arc<dyn TunnelDialer> =
            Arc::new(FnDialer(|_: &str| -> Result<BoxIo, TunnelError> {
                panic!("dialed after renewal was denied")
            }));
        let proxy = Proxy::new(fixed_destination(""));
        proxy
            .activate(
                UID,
                dialer,
                Arc::new(src),
                SystemTime::now() + Duration::from_millis(50),
            )
            .unwrap();
        recv_within(&mut called, "renewal start").await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while proxy.expires_at(UID).is_some() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{code:?}: terminal failure did not block new egress"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        proxy.deactivate(UID).await;
    }
}

#[tokio::test]
async fn deactivation_drops_concurrent_renewal() {
    let (tx, mut started) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let src = FakeSource {
        expires_at: Some(hour()),
        called: Some(tx),
        release: Some(release.clone()),
        ..Default::default()
    };
    let dialer: Arc<dyn TunnelDialer> = Arc::new(FnDialer(|_: &str| Err(TunnelError::Canceled)));
    let proxy = Proxy::new(fixed_destination(""));
    proxy
        .activate(
            UID,
            dialer,
            Arc::new(src),
            SystemTime::now() + Duration::from_millis(50),
        )
        .unwrap();
    let active = proxy.lookup(UID).unwrap();
    recv_within(&mut started, "renewal start").await;
    let p2 = proxy.clone();
    let done = tokio::spawn(async move { p2.deactivate(UID).await });
    tokio::time::timeout(Duration::from_secs(5), active.cancel.cancelled())
        .await
        .unwrap();
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), done)
        .await
        .unwrap()
        .unwrap();
    assert!(active.state.lock().expires_at.is_none());
}

#[tokio::test]
async fn end_to_end() {
    let ca = crate::test_pki::TestCa::new();
    let (rtx, mut requests) = mpsc::unbounded_channel();
    let (gtx, mut gateway_done) = mpsc::unbounded_channel();
    let addr = serve_connect_gateway(&ca, move |mut conn, req| {
        Box::pin(async move {
            let uris = crate::tunnel::keeper_dial::uri_sans(&req.peer_leaf);
            assert_eq!(uris, vec![TEST_ACTOR_ID.to_string()]);
            rtx.send(req).unwrap();
            conn.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            let mut p = [0u8; 10];
            conn.read_exact(&mut p).await.unwrap();
            assert_eq!(&p, b"from actor");
            conn.write_all(b"from gateway").await.unwrap();
            let _ = gtx.send(());
        })
    })
    .await;
    let (client, _, _d) = new_test_client(&ca, dial_fixed(addr));
    let proxy = Proxy::new(fixed_destination("192.0.2.10:443"));
    proxy
        .activate(
            UID,
            Arc::new(client),
            Arc::new(FakeSource {
                expires_at: Some(hour()),
                ..Default::default()
            }),
            hour(),
        )
        .unwrap();
    let (mut actor, p) = pipe();
    proxy.handle_for(p, UID);
    let req = recv_within(&mut requests, "gateway CONNECT request").await;
    assert_eq!(
        (req.method.as_str(), req.host()),
        ("CONNECT", Some("192.0.2.10:443"))
    );
    assert!(req
        .headers
        .iter()
        .all(|(k, _)| k != "authorization" && !k.starts_with("x-hk-")));
    actor.write_all(b"from actor").await.unwrap();
    let mut got = [0u8; 12];
    actor.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, b"from gateway");
    recv_within(&mut gateway_done, "gateway completion").await;
    proxy.deactivate(UID).await;
}

#[tokio::test]
async fn rejects_inactive_connection() {
    let proxy = Proxy::new(Arc::new(|_| panic!("inactive egress resolved destination")));
    let (mut actor, p) = pipe();
    proxy.handle_for(p, UID);
    assert!(
        closed_within(&mut actor).await,
        "inactive connection remained open"
    );
}

#[tokio::test]
async fn bounds_how_long_a_tunnel_waits_to_open() {
    let ca = crate::test_pki::TestCa::new();
    let (wtx, mut withdrawn) = mpsc::unbounded_channel();
    let wtx = Arc::new(wtx);
    let (addr, _) = serve_h2_gateway(
        &ca,
        Arc::new(move |mut r: H2Request| {
            let wtx = wtx.clone();
            Box::pin(async move {
                // Never answer; report when the client resets the stream.
                futures::future::poll_fn(|cx| r.respond.poll_reset(cx))
                    .await
                    .ok();
                let _ = wtx.send(());
            })
        }),
    )
    .await;
    let (client, _, _d) = new_test_client(&ca, dial_fixed(addr));
    let proxy = Proxy::new(fixed_destination("192.0.2.10:443"));
    proxy.set_tunnel_open_timeout(Duration::from_millis(100));
    proxy
        .activate(
            UID,
            Arc::new(client),
            Arc::new(FakeSource {
                expires_at: Some(hour()),
                ..Default::default()
            }),
            hour(),
        )
        .unwrap();
    let (mut actor, p) = pipe();
    proxy.handle_for(p, UID);
    recv_within(&mut withdrawn, "the gateway to see the CONNECT withdrawn").await;
    assert!(
        closed_within(&mut actor).await,
        "the Actor connection stayed open"
    );
    proxy.deactivate(UID).await;
}

#[tokio::test]
async fn is_armed_per_actor() {
    let proxy = Proxy::new(fixed_destination("example.com:443"));
    let src: Arc<dyn ActorCertificateSource> = Arc::new(FakeSource {
        expires_at: Some(hour()),
        ..Default::default()
    });
    let dialer: Arc<dyn TunnelDialer> = Arc::new(FnDialer(|_: &str| Err(TunnelError::Canceled)));
    for uid in ["actor-uid-1", "actor-uid-2"] {
        proxy
            .activate(uid, dialer.clone(), src.clone(), hour())
            .unwrap();
    }
    assert!(
        proxy.activate("actor-uid-1", dialer, src, hour()).is_err(),
        "activated the same Actor twice"
    );
    proxy.deactivate("actor-uid-1").await;
    assert!(!proxy.is_armed("actor-uid-1"));
    assert!(proxy.is_armed("actor-uid-2"));
}

#[tokio::test]
async fn deactivate_closes_idle_h2_connection() {
    let ca = crate::test_pki::TestCa::new();
    let (addr, _) = serve_h2_gateway(
        &ca,
        Arc::new(|mut r: H2Request| {
            Box::pin(async move {
                let _s = r.ok();
                let _ = read_body(&mut r.body).await;
            })
        }),
    )
    .await;
    let (ctx, mut closed) = mpsc::unbounded_channel();
    let (client, _, _d) = new_test_client(&ca, dial_counting(addr, Arc::default(), 0, ctx));
    let client = Arc::new(client);
    let proxy = Proxy::new(fixed_destination("192.0.2.10:443"));
    proxy
        .activate(
            UID,
            client.clone(),
            Arc::new(FakeSource {
                expires_at: Some(hour()),
                ..Default::default()
            }),
            hour(),
        )
        .unwrap();
    drop(client.dial("192.0.2.10:443").await.unwrap());
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        closed.try_recv().is_err(),
        "HTTP/2 connection closed while the Actor was active"
    );
    proxy.deactivate(UID).await;
    recv_within(&mut closed, "idle HTTP/2 connection closed on deactivate").await;
}

#[test]
fn port_from_listen_addr_cases() {
    assert_eq!(
        super::super::port_from_listen_addr("0.0.0.0:15001").unwrap(),
        15001
    );
    assert!(super::super::port_from_listen_addr("0.0.0.0").is_err());
    assert!(super::super::port_from_listen_addr("0.0.0.0:0").is_err());
}

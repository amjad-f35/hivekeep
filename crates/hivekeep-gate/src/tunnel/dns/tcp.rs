//! The TCP side of the relay: each Actor connection is paired with one
//! upstream connection, queries are pipelined in both directions, and the
//! pair is closed after an idle period.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::handler::{Action, DnsHandler};
use super::limiter::{Limiter, SlotGuard};
use super::protocol::{read_tcp_frame, write_tcp_frame};
use super::{DEFAULT_CONNECTION_TIMEOUT, DNS_EXCHANGE_TIMEOUT};

/// Serves one Actor's TCP DNS listener.
pub struct TcpHandler {
    upstreams: Vec<String>,
    listener: TcpListener,
    dns: Arc<DnsHandler>,
    limiter: Arc<Limiter>,
    /// Upstream dial timeout.
    pub dial_timeout: Duration,
    /// Idle timeout of a connection pair, refreshed by every message.
    pub tcp_timeout: Duration,
}

/// A sliding deadline shared by both directions of a connection pair.
struct Deadline {
    at: Mutex<Instant>,
    timeout: Duration,
}

impl Deadline {
    fn refresh(&self) {
        *self.at.lock() = Instant::now() + self.timeout;
    }

    /// Cancels `conn` once the deadline passes without a refresh.
    async fn watch(&self, conn: &CancellationToken) {
        loop {
            let at = *self.at.lock();
            tokio::select! {
                _ = tokio::time::sleep_until(at) => {
                    if *self.at.lock() <= Instant::now() {
                        conn.cancel();
                        return;
                    }
                }
                _ = conn.cancelled() => return,
            }
        }
    }
}

type ClientWriter = Arc<tokio::sync::Mutex<OwnedWriteHalf>>;

impl TcpHandler {
    /// A handler accepting on `listener` and pairing connections with the
    /// first reachable of `upstreams`.
    pub fn new(
        upstreams: Vec<String>,
        listener: TcpListener,
        dns: Arc<DnsHandler>,
        limiter: Arc<Limiter>,
    ) -> Self {
        TcpHandler {
            upstreams,
            listener,
            dns,
            limiter,
            dial_timeout: DNS_EXCHANGE_TIMEOUT,
            tcp_timeout: DEFAULT_CONNECTION_TIMEOUT,
        }
    }

    /// Accepts until `cancel` fires, then waits for every connection to
    /// close. Connections beyond the limit are closed at once.
    pub async fn serve(self: Arc<Self>, cancel: CancellationToken) {
        let tracker = TaskTracker::new();
        loop {
            let conn = tokio::select! {
                r = self.listener.accept() => match r {
                    Ok((conn, _)) => conn,
                    Err(err) => {
                        tracing::warn!(%err, "dns: accepting Actor DNS connection");
                        break;
                    }
                },
                _ = cancel.cancelled() => break,
            };
            let Some(slot) = SlotGuard::acquire(&self.limiter.connections) else {
                tracing::debug!("dns relay: refusing connection, at limit");
                drop(conn);
                continue;
            };
            let this = self.clone();
            let cancel = cancel.child_token();
            tracker.spawn(async move {
                this.on_connect(conn, cancel).await;
                drop(slot);
            });
        }
        tracker.close();
        tracker.wait().await;
    }

    async fn dial_upstream(&self, cancel: &CancellationToken) -> Option<TcpStream> {
        let mut errs = Vec::new();
        for address in &self.upstreams {
            let dial =
                tokio::time::timeout(self.dial_timeout, TcpStream::connect(address.as_str()));
            let r = tokio::select! {
                r = dial => r,
                _ = cancel.cancelled() => return None,
            };
            match r {
                Ok(Ok(conn)) => return Some(conn),
                Ok(Err(e)) => errs.push(format!("{address}: {e}")),
                Err(_) => errs.push(format!("{address}: dial timed out")),
            }
        }
        tracing::warn!(errors = ?errs, "dns relay: could not reach any upstream");
        None
    }

    async fn on_connect(&self, downstream: TcpStream, conn_cancel: CancellationToken) {
        let Some(upstream) = self.dial_upstream(&conn_cancel).await else {
            return;
        };
        let deadline = Deadline {
            at: Mutex::new(Instant::now() + self.tcp_timeout),
            timeout: self.tcp_timeout,
        };
        let (client_r, client_w) = downstream.into_split();
        let (up_r, up_w) = upstream.into_split();
        let client_w: ClientWriter = Arc::new(tokio::sync::Mutex::new(client_w));
        tokio::join!(
            self.process_requests(client_r, up_w, client_w.clone(), &deadline, &conn_cancel),
            self.process_responses(up_r, client_w, &deadline, &conn_cancel),
            deadline.watch(&conn_cancel),
        );
    }

    async fn process_requests(
        &self,
        mut client: OwnedReadHalf,
        mut upstream: OwnedWriteHalf,
        client_w: ClientWriter,
        deadline: &Deadline,
        conn: &CancellationToken,
    ) {
        loop {
            let frame = tokio::select! {
                r = read_tcp_frame(&mut client) => r,
                _ = conn.cancelled() => return,
            };
            let raw = match frame {
                Ok(Some(raw)) => raw,
                Ok(None) => {
                    // The Actor is done sending: pass the half-close on and
                    // let the response side finish.
                    let _ = upstream.shutdown().await;
                    return;
                }
                Err(_) => {
                    conn.cancel();
                    return;
                }
            };
            deadline.refresh();
            let act = tokio::select! {
                act = self.dns.on_tcp_request(&raw) => act,
                _ = conn.cancelled() => return,
            };
            let result = match act {
                Action::Reply(payload) => {
                    write_tcp_frame(&mut *client_w.lock().await, &payload).await
                }
                Action::Forward { payload, .. } => write_tcp_frame(&mut upstream, &payload).await,
                _ => {
                    tracing::debug!("dns relay: dropping request and closing stream");
                    conn.cancel();
                    return;
                }
            };
            if let Err(err) = result {
                tracing::debug!(%err, "dns relay: error writing on TCP");
                conn.cancel();
                return;
            }
            deadline.refresh();
        }
    }

    async fn process_responses(
        &self,
        mut upstream: OwnedReadHalf,
        client_w: ClientWriter,
        deadline: &Deadline,
        conn: &CancellationToken,
    ) {
        loop {
            let frame = tokio::select! {
                r = read_tcp_frame(&mut upstream) => r,
                _ = conn.cancelled() => return,
            };
            let raw = match frame {
                Ok(Some(raw)) => raw,
                Ok(None) => {
                    let _ = client_w.lock().await.shutdown().await;
                    conn.cancel();
                    return;
                }
                Err(_) => {
                    conn.cancel();
                    return;
                }
            };
            deadline.refresh();
            let Action::Deliver { payload, .. } = self.dns.on_tcp_response(&raw) else {
                conn.cancel();
                return;
            };
            if let Err(err) = write_tcp_frame(&mut *client_w.lock().await, &payload).await {
                tracing::warn!(%err, "dns relay could not return TCP DNS answer");
                conn.cancel();
                return;
            }
            deadline.refresh();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tunnel::dns::pending::PendingRequests;
    use crate::tunnel::dns::policy::{FnPolicy, QueryPolicy};
    use crate::tunnel::dns::protocol::{rcode, rtype, Header, Question, CLASS_INET};
    use crate::tunnel::dns::test_support::*;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use tokio::io::AsyncReadExt;

    struct Started {
        addr: SocketAddr,
        cancel: CancellationToken,
    }

    impl Drop for Started {
        fn drop(&mut self) {
            self.cancel.cancel();
        }
    }

    async fn start(
        upstreams: Vec<String>,
        allow: Option<Arc<dyn QueryPolicy>>,
        idle: Option<Duration>,
    ) -> Started {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap();
        let lim = Arc::new(Limiter::new());
        let dns = Arc::new(DnsHandler::new(
            Arc::new(PendingRequests::new(lim.clone())),
            lim.clone(),
            allow,
        ));
        let mut h = TcpHandler::new(upstreams, lis, dns, lim);
        if let Some(t) = idle {
            h.tcp_timeout = t;
        }
        let cancel = CancellationToken::new();
        tokio::spawn(Arc::new(h).serve(cancel.clone()));
        Started { addr, cancel }
    }

    async fn out_of_order_resolver() -> String {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut c, _)) = lis.accept().await {
                tokio::spawn(async move {
                    let Ok(Some(q1)) = read_tcp_frame(&mut c).await else {
                        return;
                    };
                    let Ok(Some(q2)) = read_tcp_frame(&mut c).await else {
                        return;
                    };
                    let _ = write_tcp_frame(&mut c, &dns_answer(&q2, 0)).await;
                    let _ = write_tcp_frame(&mut c, &dns_answer(&q1, 0)).await;
                });
            }
        });
        addr
    }

    fn id(b: &[u8]) -> u16 {
        u16::from_be_bytes([b[0], b[1]])
    }

    async fn read_frame(c: &mut TcpStream) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(5), read_tcp_frame(c))
            .await
            .expect("timed out")
            .unwrap()
            .expect("frame")
    }

    #[tokio::test]
    async fn pipelining_and_out_of_order_responses() {
        let s = start(vec![out_of_order_resolver().await], None, None).await;
        let mut c = TcpStream::connect(s.addr).await.unwrap();
        write_tcp_frame(&mut c, &dns_query(0x1111)).await.unwrap();
        write_tcp_frame(&mut c, &dns_query(0x2222)).await.unwrap();
        assert_eq!(id(&read_frame(&mut c).await), 0x2222);
        assert_eq!(id(&read_frame(&mut c).await), 0x1111);
    }

    #[tokio::test]
    async fn synthesized_reply_and_half_close() {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up = lis.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut c, _) = lis.accept().await.unwrap();
            if let Ok(Some(q)) = read_tcp_frame(&mut c).await {
                let _ = write_tcp_frame(&mut c, &dns_answer(&q, 0)).await;
            }
        });
        let allow: Arc<dyn QueryPolicy> =
            Arc::new(FnPolicy(|q: &Question| q.name != b"blocked.example.com."));
        let s = start(vec![up], Some(allow), None).await;
        let mut c = TcpStream::connect(s.addr).await.unwrap();
        let blocked = build_message(
            &Header {
                id: 0xbad0,
                recursion_desired: true,
                ..Default::default()
            },
            &[Question::new("blocked.example.com.", rtype::A, CLASS_INET)],
        );
        write_tcp_frame(&mut c, &blocked).await.unwrap();
        write_tcp_frame(&mut c, &dns_query(0x1234)).await.unwrap();
        c.shutdown().await.unwrap();
        let r1 = read_frame(&mut c).await;
        let r2 = read_frame(&mut c).await;
        let got: HashMap<u16, u8> = [(id(&r1), r1[3] & 0x0f), (id(&r2), r2[3] & 0x0f)].into();
        assert_eq!(got[&0xbad0], rcode::NAME_ERROR);
        assert_eq!(got[&0x1234], rcode::SUCCESS);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn sliding_idle_timeout() {
        let (up, _) = tcp_resolver(|q| dns_answer(q, 0)).await;
        let idle = Duration::from_millis(80);
        let s = start(vec![up], None, Some(idle)).await;
        let mut c = TcpStream::connect(s.addr).await.unwrap();
        for i in 0..3u16 {
            if i > 0 {
                tokio::time::sleep(idle / 2).await;
            }
            write_tcp_frame(&mut c, &dns_query(0x3000 + i))
                .await
                .unwrap();
            assert_eq!(id(&read_frame(&mut c).await), 0x3000 + i);
        }
        let mut b = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(5), c.read(&mut b))
            .await
            .unwrap();
        assert!(matches!(n, Ok(0) | Err(_)), "idle connection still open");
    }

    #[tokio::test]
    async fn closes_on_dropped_frame() {
        let (up, _) = held_tcp_resolver().await;
        let s = start(vec![up], None, None).await;
        let mut c = TcpStream::connect(s.addr).await.unwrap();
        write_tcp_frame(&mut c, &[1, 2, 3]).await.unwrap();
        let mut b = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(5), c.read(&mut b))
            .await
            .unwrap();
        assert!(
            matches!(n, Ok(0) | Err(_)),
            "connection still open after a dropped frame"
        );
    }
}

//! The UDP side of the relay: Actor queries in, upstream answers back,
//! and a sweeper retrying silent upstreams.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

use super::handler::{Action, DnsHandler};
use super::pending::PendingRequests;
use super::relay::normalize_addr;
use super::{DEFAULT_SWEEP_INTERVAL, MAX_DATAGRAM_SIZE};

/// Serves one Actor's UDP DNS socket.
pub struct UdpHandler {
    actor_sock: Arc<UdpSocket>,
    upstream_sock: Arc<UdpSocket>,
    upstreams: Vec<SocketAddr>,
    upstream_strs: Vec<String>,
    dns: Arc<DnsHandler>,
    pending: Arc<PendingRequests>,
    /// How often expired attempts are swept.
    pub sweep_interval: Duration,
}

impl UdpHandler {
    /// A handler reading queries on `actor_sock` and talking to `upstreams`
    /// over `upstream_sock`.
    pub fn new(
        actor_sock: Arc<UdpSocket>,
        upstream_sock: Arc<UdpSocket>,
        upstreams: Vec<SocketAddr>,
        dns: Arc<DnsHandler>,
        pending: Arc<PendingRequests>,
    ) -> Self {
        let upstream_strs = upstreams.iter().map(normalize_addr).collect();
        UdpHandler {
            actor_sock,
            upstream_sock,
            upstreams,
            upstream_strs,
            dns,
            pending,
            sweep_interval: DEFAULT_SWEEP_INTERVAL,
        }
    }

    /// Serves until `cancel` fires; pending queries are then dropped.
    pub async fn serve(self: Arc<Self>, cancel: CancellationToken) {
        tokio::join!(
            self.process_requests(&cancel),
            self.process_responses(&cancel),
            self.sweep_loop(&cancel),
        );
        self.pending.clear_all();
    }

    async fn process_requests(&self, cancel: &CancellationToken) {
        let mut buf = vec![0; MAX_DATAGRAM_SIZE];
        loop {
            let (n, from) = tokio::select! {
                r = self.actor_sock.recv_from(&mut buf) => match r {
                    Ok(r) => r,
                    Err(err) => {
                        tracing::warn!(%err, "dns relay: reading Actor DNS query");
                        return;
                    }
                },
                _ = cancel.cancelled() => return,
            };
            let raw = buf[..n].to_vec();
            let act = tokio::select! {
                act = self.dns.on_udp_request(raw, Some(from), &self.upstream_strs) => act,
                _ = cancel.cancelled() => return,
            };
            match act {
                Action::Reply(payload) => {
                    if let Err(err) = self.actor_sock.send_to(&payload, from).await {
                        tracing::warn!(%err, "dns relay: could not return a synthesized DNS reply");
                    }
                }
                Action::Forward {
                    payload,
                    upstream_id,
                } => {
                    self.send_to_upstream(upstream_id, 0, payload).await;
                }
                _ => {}
            }
        }
    }

    async fn send_to_upstream(&self, upstream_id: u16, mut idx: usize, mut query: Vec<u8>) {
        while idx < self.upstreams.len() {
            let Err(err) = self
                .upstream_sock
                .send_to(&query, self.upstreams[idx])
                .await
            else {
                return;
            };
            tracing::warn!(upstream = %self.upstream_strs[idx], %err, "dns relay: could not send query to upstream");
            let Some((next, next_query)) = self.pending.fail_over_on_send_error(upstream_id, idx)
            else {
                return;
            };
            idx = next;
            query = next_query;
        }
    }

    async fn process_responses(&self, cancel: &CancellationToken) {
        let mut buf = vec![0; MAX_DATAGRAM_SIZE];
        loop {
            let (n, from) = tokio::select! {
                r = self.upstream_sock.recv_from(&mut buf) => match r {
                    Ok(r) => r,
                    Err(err) => {
                        tracing::debug!(%err, "dns relay: upstream socket read error");
                        continue;
                    }
                },
                _ = cancel.cancelled() => return,
            };
            match self.dns.on_udp_response(&buf[..n], from) {
                Action::Failover {
                    payload,
                    upstream_idx,
                } if upstream_idx < self.upstreams.len() && payload.len() >= 2 => {
                    let id = u16::from_be_bytes([payload[0], payload[1]]);
                    self.send_to_upstream(id, upstream_idx, payload).await;
                }
                Action::Deliver {
                    payload,
                    client_addr: Some(client),
                } => {
                    if let Err(err) = self.actor_sock.send_to(&payload, client).await {
                        tracing::warn!(%err, "dns relay: could not return a DNS answer");
                    }
                }
                _ => {}
            }
        }
    }

    async fn sweep_loop(&self, cancel: &CancellationToken) {
        let interval = if self.sweep_interval.is_zero() {
            DEFAULT_SWEEP_INTERVAL
        } else {
            self.sweep_interval
        };
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = cancel.cancelled() => return,
            }
            let (failovers, deliveries) = self.pending.sweep(std::time::Instant::now());
            for f in failovers {
                self.send_to_upstream(f.upstream_id, f.upstream_idx, f.query)
                    .await;
            }
            for d in deliveries {
                if let Err(err) = self.actor_sock.send_to(&d.payload, d.client_addr).await {
                    tracing::warn!(%err, "dns relay: could not return a deferred DNS answer");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tunnel::dns::limiter::Limiter;
    use crate::tunnel::dns::policy::{FnPolicy, QueryPolicy};
    use crate::tunnel::dns::protocol::rcode;
    use crate::tunnel::dns::test_support::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct Started {
        client: UdpSocket,
        cancel: CancellationToken,
    }

    impl Drop for Started {
        fn drop(&mut self) {
            self.cancel.cancel();
        }
    }

    async fn start(
        upstreams: &[String],
        allow: Option<Arc<dyn QueryPolicy>>,
        attempt: Option<Duration>,
        sweep: Option<Duration>,
    ) -> Started {
        let ingress = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let egress = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addrs = upstreams.iter().map(|u| u.parse().unwrap()).collect();
        let lim = Arc::new(Limiter::new());
        let mut pending = PendingRequests::new(lim.clone());
        pending.attempt_timeout = attempt;
        let pending = Arc::new(pending);
        let dns = Arc::new(DnsHandler::new(pending.clone(), lim, allow));
        let mut h = UdpHandler::new(ingress.clone(), egress, addrs, dns, pending);
        if let Some(s) = sweep {
            h.sweep_interval = s;
        }
        let cancel = CancellationToken::new();
        tokio::spawn(Arc::new(h).serve(cancel.clone()));
        Started {
            client: udp_client(ingress.local_addr().unwrap()).await,
            cancel,
        }
    }

    #[tokio::test]
    async fn forwards_and_restores_id() {
        let seen = Arc::new(AtomicU32::new(0));
        let s2 = seen.clone();
        let (up, _t) = fake_resolver(move |q| {
            s2.store(
                u32::from(u16::from_be_bytes([q[0], q[1]])),
                Ordering::SeqCst,
            );
            dns_answer(q, 0)
        })
        .await;
        let s = start(&[up], None, None, None).await;
        s.client.send(&dns_query(0x4242)).await.unwrap();
        let got = read_within(&s.client).await;
        assert_eq!(u16::from_be_bytes([got[0], got[1]]), 0x4242);
        assert_ne!(got[2] & 0x80, 0);
    }

    #[tokio::test]
    async fn synthesized_reply() {
        let calls = Arc::new(AtomicU32::new(0));
        let c2 = calls.clone();
        let (up, _t) = fake_resolver(move |q| {
            c2.fetch_add(1, Ordering::SeqCst);
            dns_answer(q, 0)
        })
        .await;
        let deny: Arc<dyn QueryPolicy> =
            Arc::new(FnPolicy(|_: &crate::tunnel::dns::protocol::Question| false));
        let s = start(&[up], Some(deny), None, None).await;
        s.client.send(&dns_query(0x9999)).await.unwrap();
        let got = read_within(&s.client).await;
        assert_eq!(got[3] & 0x0f, rcode::NAME_ERROR);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn failover_on_sweep_timeout_and_deferred_delivery() {
        let ms = Duration::from_millis;
        let (silent1, _rx1, _t1) = silent_resolver().await;
        let (healthy, _t2) = fake_resolver(|q| dns_answer(q, 0)).await;
        let s = start(&[silent1, healthy], None, Some(ms(40)), Some(ms(10))).await;
        s.client.send(&dns_query(0x7777)).await.unwrap();
        let got = read_within(&s.client).await;
        assert_eq!(u16::from_be_bytes([got[0], got[1]]), 0x7777);
        assert_eq!(got[3] & 0x0f, 0, "answer after timeout failover");

        let (sick, _t3) = fake_resolver(|q| dns_answer(q, rcode::SERVER_FAILURE)).await;
        let (silent2, _rx2, _t4) = silent_resolver().await;
        let s2 = start(&[sick, silent2], None, Some(ms(40)), Some(ms(10))).await;
        s2.client.send(&dns_query(0x8888)).await.unwrap();
        let got = read_within(&s2.client).await;
        assert_eq!(u16::from_be_bytes([got[0], got[1]]), 0x8888);
        assert_eq!(got[3] & 0x0f, rcode::SERVER_FAILURE, "deferred answer");
    }
}

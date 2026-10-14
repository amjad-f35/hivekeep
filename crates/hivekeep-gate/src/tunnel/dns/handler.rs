//! Transport-independent decisions for each message the relay sees.

use std::net::SocketAddr;
use std::sync::Arc;

use super::limiter::{Limiter, SlotGuard};
use super::pending::{PendingRequests, ResponseOutcome};
use super::policy::{AllowAll, QueryPolicy};
use super::protocol::{
    error_packet, first_question, nxdomain_packet, qd_count, rcode, request_sanity_check,
    response_sanity_check, rtype, Header, Question,
};

/// What the transport should do with a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Discard it.
    Drop,
    /// Answer the Actor with this synthesized reply.
    Reply(Vec<u8>),
    /// Send this query upstream (UDP: under `upstream_id`).
    Forward {
        /// The query.
        payload: Vec<u8>,
        /// The upstream transaction ID (UDP only, 0 for TCP).
        upstream_id: u16,
    },
    /// Re-send the query to upstream `upstream_idx`.
    Failover {
        /// The query.
        payload: Vec<u8>,
        /// The upstream to ask.
        upstream_idx: usize,
    },
    /// Return this answer to the Actor.
    Deliver {
        /// The answer.
        payload: Vec<u8>,
        /// Where to send it (UDP only).
        client_addr: Option<SocketAddr>,
    },
}

/// Applies the relay's rules to queries and answers.
pub struct DnsHandler {
    pub(crate) pending: Arc<PendingRequests>,
    pub(crate) limiter: Arc<Limiter>,
    pub(crate) allow: Arc<dyn QueryPolicy>,
}

impl DnsHandler {
    /// A handler; without a policy every query may be forwarded.
    pub fn new(
        pending: Arc<PendingRequests>,
        limiter: Arc<Limiter>,
        allow: Option<Arc<dyn QueryPolicy>>,
    ) -> Self {
        DnsHandler {
            pending,
            limiter,
            allow: allow.unwrap_or_else(|| Arc::new(AllowAll)),
        }
    }

    /// Validates a query: a forward carries the canonical question.
    async fn on_request_common(&self, raw: &[u8]) -> (Header, Option<Question>, Action) {
        if !request_sanity_check(raw) {
            return (Header::default(), None, Action::Drop);
        }
        let hdr = Header::parse(raw).expect("sane header");
        let qd = qd_count(raw);
        let reply = |code, q: Option<&Question>| Action::Reply(error_packet(&hdr, code, q));
        if hdr.opcode != 0 {
            let q = if qd == 1 { first_question(raw) } else { None };
            return (hdr, None, reply(rcode::NOT_IMPLEMENTED, q.as_ref()));
        }
        if qd != 1 {
            return (hdr, None, reply(rcode::FORMAT_ERROR, None));
        }
        let Some(q) = first_question(raw) else {
            return (hdr, None, reply(rcode::FORMAT_ERROR, None));
        };
        if q.qtype == rtype::AXFR || q.qtype == rtype::IXFR {
            return (hdr, None, reply(rcode::NOT_IMPLEMENTED, Some(&q)));
        }
        let cq = q.canonical();
        if !self.allow.allow(&cq).await {
            return (hdr, None, Action::Reply(nxdomain_packet(&hdr, &q)));
        }
        (
            hdr,
            Some(cq),
            Action::Forward {
                payload: raw.to_vec(),
                upstream_id: 0,
            },
        )
    }

    /// A UDP query from the Actor at `client`. Forwarded queries hold an
    /// in-flight slot until answered or abandoned.
    pub async fn on_udp_request(
        &self,
        mut raw: Vec<u8>,
        client: Option<SocketAddr>,
        upstreams: &[String],
    ) -> Action {
        let Some(slot) = SlotGuard::acquire(&self.limiter.in_flight) else {
            tracing::debug!("dns relay dropped query; too many in flight");
            return Action::Drop;
        };
        let (hdr, cq, act) = self.on_request_common(&raw).await;
        let Some(cq) = cq.filter(|_| matches!(act, Action::Forward { .. })) else {
            return act;
        };
        match self
            .pending
            .record(&mut raw, slot, hdr.id, cq, client, upstreams.to_vec())
        {
            Some(upstream_id) => Action::Forward {
                payload: raw,
                upstream_id,
            },
            None => Action::Drop,
        }
    }

    /// An answer from upstream `from`.
    pub fn on_udp_response(&self, raw: &[u8], from: SocketAddr) -> Action {
        if !response_sanity_check(raw) {
            return Action::Drop;
        }
        let hdr = Header::parse(raw).expect("sane header");
        let Some(q) = first_question(raw) else {
            return Action::Drop;
        };
        match self
            .pending
            .on_response(hdr.id, from, &q.canonical(), hdr.rcode, raw)
        {
            ResponseOutcome::Drop => Action::Drop,
            ResponseOutcome::Failover {
                upstream_idx,
                query,
            } => Action::Failover {
                payload: query,
                upstream_idx,
            },
            ResponseOutcome::Deliver {
                client_addr,
                payload,
            } => Action::Deliver {
                payload,
                client_addr,
            },
        }
    }

    /// A TCP query; TCP keeps one upstream connection per Actor connection,
    /// so nothing is recorded.
    pub async fn on_tcp_request(&self, raw: &[u8]) -> Action {
        self.on_request_common(raw).await.2
    }

    /// A TCP answer from upstream.
    pub fn on_tcp_response(&self, raw: &[u8]) -> Action {
        if !response_sanity_check(raw) || first_question(raw).is_none() {
            return Action::Drop;
        }
        Action::Deliver {
            payload: raw.to_vec(),
            client_addr: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tunnel::dns::policy::FnPolicy;
    use crate::tunnel::dns::protocol::{query_packet, Message, CLASS_INET};
    use crate::tunnel::dns::test_support::{build_message, dns_answer, dns_query};

    fn handler(lim: Arc<Limiter>, allow: Option<Arc<dyn QueryPolicy>>) -> DnsHandler {
        DnsHandler::new(Arc::new(PendingRequests::new(lim.clone())), lim, allow)
    }

    fn check_uncacheable_negative_answer(payload: &[u8]) {
        let m = Message::parse(payload).unwrap();
        assert_eq!(m.authorities.len(), 1, "one SOA record");
        let soa = &m.authorities[0];
        assert_eq!(soa.rtype, rtype::SOA);
        assert_eq!(soa.ttl, 0);
        assert_eq!(&soa.rdata[soa.rdata.len() - 4..], &[0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn on_request_drop() {
        let lim = Arc::new(Limiter::with_limits(1, 1));
        let h = handler(lim.clone(), None);
        let ups = vec!["10.96.0.10:53".to_string()];
        assert_eq!(
            h.on_udp_request(vec![1, 2, 3, 4], None, &ups).await,
            Action::Drop
        );
        assert_eq!(h.on_tcp_request(&[1, 2, 3, 4]).await, Action::Drop);
        let resp = dns_answer(&dns_query(0x1234), 0);
        assert_eq!(
            h.on_udp_request(resp.clone(), None, &ups).await,
            Action::Drop
        );
        assert_eq!(h.on_tcp_request(&resp).await, Action::Drop);
        let held = SlotGuard::acquire(&lim.in_flight).unwrap();
        assert_eq!(
            h.on_udp_request(dns_query(0x1234), None, &ups).await,
            Action::Drop,
            "rate-limited"
        );
        drop(held);
    }

    #[tokio::test]
    async fn on_request_synthesized_replies() {
        let lim = Arc::new(Limiter::new());
        let allow: Arc<dyn QueryPolicy> = Arc::new(FnPolicy(|q: &Question| {
            q.name != b"blocked.example.com." && q.qtype != 16
        }));
        let h = handler(lim.clone(), Some(allow));
        let ups = vec!["10.96.0.10:53".to_string()];
        let q_example = Question::new("example.com.", rtype::A, CLASS_INET);
        let hd = |id, opcode| Header {
            id,
            opcode,
            recursion_desired: true,
            ..Default::default()
        };
        let cases: Vec<(&str, Vec<u8>, u8)> = vec![
            (
                "QDCOUNT == 0 -> FORMERR",
                build_message(&hd(0x1001, 0), &[]),
                rcode::FORMAT_ERROR,
            ),
            (
                "QDCOUNT == 2 -> FORMERR",
                build_message(&hd(0x1002, 0), &[q_example.clone(), q_example.clone()]),
                rcode::FORMAT_ERROR,
            ),
            (
                "malformed question name -> FORMERR",
                vec![
                    0x10, 0x03, 0x01, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0, 0x10, b'a', b'b',
                ],
                rcode::FORMAT_ERROR,
            ),
            (
                "non-zero OpCode -> NOTIMP",
                build_message(&hd(0x1004, 1), std::slice::from_ref(&q_example)),
                rcode::NOT_IMPLEMENTED,
            ),
            (
                "QTYPE AXFR -> NOTIMP",
                build_message(
                    &hd(0x1005, 0),
                    &[Question::new("example.com.", rtype::AXFR, CLASS_INET)],
                ),
                rcode::NOT_IMPLEMENTED,
            ),
            (
                "QTYPE IXFR -> NOTIMP",
                build_message(
                    &hd(0x1006, 0),
                    &[Question::new("example.com.", rtype::IXFR, CLASS_INET)],
                ),
                rcode::NOT_IMPLEMENTED,
            ),
            (
                "disallowed domain -> NXDOMAIN",
                build_message(
                    &hd(0x1007, 0),
                    &[Question::new("Blocked.Example.COM.", rtype::A, CLASS_INET)],
                ),
                rcode::NAME_ERROR,
            ),
            (
                "disallowed record type -> NXDOMAIN",
                build_message(
                    &hd(0x1008, 0),
                    &[Question::new("example.com.", 16, CLASS_INET)],
                ),
                rcode::NAME_ERROR,
            ),
        ];
        for (name, raw, want) in cases {
            let want_id = u16::from_be_bytes([raw[0], raw[1]]);
            for act in [
                h.on_udp_request(raw.clone(), None, &ups).await,
                h.on_tcp_request(&raw).await,
            ] {
                let Action::Reply(payload) = act else {
                    panic!("{name}: want a reply, got {act:?}");
                };
                let hdr = Header::parse(&payload).unwrap();
                assert_eq!(hdr.id, want_id, "{name}");
                assert!(hdr.response, "{name}");
                assert_eq!(hdr.rcode, want, "{name}");
                if hdr.rcode == rcode::NAME_ERROR {
                    check_uncacheable_negative_answer(&payload);
                }
                assert_eq!(lim.in_flight.occupied(), 0, "{name}");
            }
        }
    }

    #[tokio::test]
    async fn udp_forward_and_response_validation() {
        let lim = Arc::new(Limiter::new());
        let h = handler(lim.clone(), None);
        let up1: SocketAddr = "10.96.0.10:53".parse().unwrap();
        let up2: SocketAddr = "10.96.0.11:53".parse().unwrap();
        let wrong: SocketAddr = "10.96.0.99:53".parse().unwrap();
        let client: SocketAddr = "169.254.0.2:54321".parse().unwrap();
        let query = query_packet(
            0xbeef,
            &Question::new("ExAmPlE.CoM.", rtype::A, CLASS_INET),
            true,
        );
        let act = h
            .on_udp_request(query, Some(client), &[up1.to_string(), up2.to_string()])
            .await;
        let Action::Forward {
            payload: rewritten,
            upstream_id,
        } = act
        else {
            panic!("want forward, got {act:?}");
        };
        assert_eq!(
            u16::from_be_bytes([rewritten[0], rewritten[1]]),
            upstream_id
        );
        assert_eq!(h.on_udp_response(&rewritten, up1), Action::Drop, "QR=0");
        let valid = dns_answer(&rewritten, 0);
        assert_eq!(
            h.on_udp_response(&valid, wrong),
            Action::Drop,
            "wrong upstream"
        );
        let mut wrong_id = valid.clone();
        wrong_id[0..2].copy_from_slice(&(upstream_id ^ 0xffff).to_be_bytes());
        assert_eq!(h.on_udp_response(&wrong_id, up1), Action::Drop, "wrong id");
        let resp_hdr = Header {
            id: upstream_id,
            response: true,
            ..Default::default()
        };
        let wrong_name = build_message(
            &resp_hdr,
            &[Question::new("other.com.", rtype::A, CLASS_INET)],
        );
        assert_eq!(
            h.on_udp_response(&wrong_name, up1),
            Action::Drop,
            "wrong qname"
        );
        let wrong_type = build_message(
            &resp_hdr,
            &[Question::new("example.com.", rtype::AAAA, CLASS_INET)],
        );
        assert_eq!(
            h.on_udp_response(&wrong_type, up1),
            Action::Drop,
            "wrong qtype"
        );
        let servfail = dns_answer(&rewritten, rcode::SERVER_FAILURE);
        match h.on_udp_response(&servfail, up1) {
            Action::Failover { upstream_idx, .. } => assert_eq!(upstream_idx, 1),
            other => panic!("want failover, got {other:?}"),
        }
        assert_eq!(lim.in_flight.occupied(), 1);
        let lower = build_message(
            &resp_hdr,
            &[Question::new("example.com.", rtype::A, CLASS_INET)],
        );
        match h.on_udp_response(&lower, up2) {
            Action::Deliver {
                payload,
                client_addr,
            } => {
                assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 0xbeef);
                assert_eq!(client_addr, Some(client));
            }
            other => panic!("want deliver, got {other:?}"),
        }
        assert_eq!(lim.in_flight.occupied(), 0);
    }

    #[tokio::test]
    async fn tcp_forward_and_response_validation() {
        let lim = Arc::new(Limiter::new());
        let h = handler(lim.clone(), None);
        let query = dns_query(0xbeef);
        assert_eq!(
            h.on_tcp_request(&query).await,
            Action::Forward {
                payload: query.clone(),
                upstream_id: 0
            }
        );
        assert!(h.pending.is_empty());
        assert_eq!(lim.in_flight.occupied(), 0);
        assert_eq!(h.on_tcp_response(&[1, 2, 3]), Action::Drop);
        assert_eq!(h.on_tcp_response(&query), Action::Drop);
        let resp = dns_answer(&query, 0);
        assert_eq!(
            h.on_tcp_response(&resp),
            Action::Deliver {
                payload: resp.clone(),
                client_addr: None
            }
        );
    }
}

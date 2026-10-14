//! UDP queries forwarded upstream and not yet answered, keyed by the
//! transaction ID the relay chose for the upstream leg.
//!
//! Upstream IDs are drawn from a random starting point so they cannot be
//! predicted from the Actor's own IDs, and each is unique while in flight.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::freemap::Map64k;
use super::limiter::{Limiter, SlotGuard};
use super::protocol::{set_txn_id, Question};
use super::relay::normalize_addr_str;
use super::{DEFAULT_UPSTREAM_ATTEMPT_TIMEOUT, DNS_EXCHANGE_TIMEOUT};

/// One forwarded query.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingRequest {
    /// The ID the Actor used, restored on the answer.
    pub client_request_id: u16,
    /// Where the answer goes.
    pub client_addr: Option<SocketAddr>,
    /// The canonical question, which the answer must echo.
    pub question: Question,
    /// The query as sent upstream (with the upstream ID), for failover.
    pub raw_query: Vec<u8>,
    /// Upstreams in the order they are tried.
    pub upstreams: Vec<String>,
    /// The upstream currently asked.
    pub upstream_idx: usize,
    /// A failure answer kept in case every later upstream stays silent.
    pub deferred_resp: Option<Vec<u8>>,
    /// When the current attempt times out.
    pub expiry: Option<Instant>,
}

/// A query to re-send to the next upstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failover {
    /// The upstream ID of the query.
    pub upstream_id: u16,
    /// The upstream to ask now.
    pub upstream_idx: usize,
    /// The query bytes.
    pub query: Vec<u8>,
}

/// An answer to return to the Actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// The Actor's address.
    pub client_addr: SocketAddr,
    /// The answer, with the Actor's ID.
    pub payload: Vec<u8>,
}

/// What to do with an upstream answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseOutcome {
    /// Not an answer to anything pending from that upstream.
    Drop,
    /// A failure with upstreams left: ask the next one.
    Failover {
        /// The upstream to ask.
        upstream_idx: usize,
        /// The query to send.
        query: Vec<u8>,
    },
    /// The answer for the Actor.
    Deliver {
        /// The Actor's address.
        client_addr: Option<SocketAddr>,
        /// The answer, with the Actor's ID.
        payload: Vec<u8>,
    },
}

#[derive(Debug, Default)]
pub(crate) struct Inner {
    pub(crate) entries: HashMap<u16, PendingRequest>,
    pub(crate) in_use: Map64k,
}

/// The table of pending queries.
pub struct PendingRequests {
    pub(crate) inner: Mutex<Inner>,
    limiter: Arc<Limiter>,
    /// Budget for the whole exchange across upstreams.
    pub exchange_timeout: Duration,
    /// Fixed per-attempt timeout overriding the computed one (tests).
    pub attempt_timeout: Option<Duration>,
}

/// Whether an answer with this code should be retried on another upstream.
pub fn is_failover_rcode(rcode: u8) -> bool {
    use super::protocol::rcode::*;
    matches!(rcode, SERVER_FAILURE | NOT_IMPLEMENTED | REFUSED)
}

impl PendingRequests {
    /// An empty table releasing in-flight slots of `limiter`.
    pub fn new(limiter: Arc<Limiter>) -> Self {
        PendingRequests {
            inner: Mutex::new(Inner::default()),
            limiter,
            exchange_timeout: DNS_EXCHANGE_TIMEOUT,
            attempt_timeout: None,
        }
    }

    /// How long attempt `idx` of `n` waits. Attempts with fallbacks left get
    /// a share of the exchange budget, capped; the last gets all of it.
    pub fn timeout_for_attempt(&self, idx: usize, n: usize) -> Duration {
        if let Some(t) = self.attempt_timeout.filter(|t| !t.is_zero()) {
            return t;
        }
        let timeout = if self.exchange_timeout.is_zero() {
            DNS_EXCHANGE_TIMEOUT
        } else {
            self.exchange_timeout
        };
        if n > 1 && idx + 1 < n {
            return (timeout / n as u32).min(DEFAULT_UPSTREAM_ATTEMPT_TIMEOUT);
        }
        timeout
    }

    /// Records a query, rewriting its ID in `raw` to a free upstream ID.
    /// Takes over `slot`; when every ID is in use the slot is released and
    /// `None` returned.
    pub fn record(
        &self,
        raw: &mut [u8],
        slot: SlotGuard,
        client_request_id: u16,
        question: Question,
        client_addr: Option<SocketAddr>,
        upstreams: Vec<String>,
    ) -> Option<u16> {
        let start: u16 = rand::random();
        let mut inner = self.inner.lock();
        let id = inner.in_use.find_first_unset_from(start)?;
        set_txn_id(raw, id);
        inner.in_use.set(id);
        let expiry = Instant::now() + self.timeout_for_attempt(0, upstreams.len());
        inner.entries.insert(
            id,
            PendingRequest {
                client_request_id,
                client_addr,
                question,
                raw_query: raw.to_vec(),
                upstreams,
                upstream_idx: 0,
                deferred_resp: None,
                expiry: Some(expiry),
            },
        );
        slot.keep();
        Some(id)
    }

    fn delete_locked(&self, inner: &mut Inner, id: u16) -> Option<PendingRequest> {
        let entry = inner.entries.remove(&id)?;
        inner.in_use.clear(id);
        self.limiter.in_flight.release();
        Some(entry)
    }

    /// Removes an entry, releasing its slot once however often it is called.
    pub fn delete(&self, id: u16) {
        let mut inner = self.inner.lock();
        self.delete_locked(&mut inner, id);
    }

    /// Advances to the next upstream after sending attempt `failed_idx`
    /// failed. Returns the upstream index and query to send, or `None` when
    /// the entry is gone, the error is stale, or no upstream is left (the
    /// entry is then dropped).
    pub fn fail_over_on_send_error(&self, id: u16, failed_idx: usize) -> Option<(usize, Vec<u8>)> {
        let mut inner = self.inner.lock();
        let entry = inner.entries.get(&id)?;
        if entry.upstream_idx != failed_idx {
            return None;
        }
        if entry.upstream_idx + 1 >= entry.upstreams.len() {
            self.delete_locked(&mut inner, id);
            return None;
        }
        let n = entry.upstreams.len();
        let timeout = self.timeout_for_attempt(failed_idx + 1, n);
        let entry = inner.entries.get_mut(&id)?;
        entry.upstream_idx += 1;
        entry.expiry = Some(Instant::now() + timeout);
        Some((entry.upstream_idx, entry.raw_query.clone()))
    }

    /// Matches an upstream answer to its pending query.
    pub fn on_response(
        &self,
        id: u16,
        from: SocketAddr,
        question: &Question,
        rcode: u8,
        raw: &[u8],
    ) -> ResponseOutcome {
        let mut inner = self.inner.lock();
        let Some(entry) = inner.entries.get(&id) else {
            return ResponseOutcome::Drop;
        };
        let expected = normalize_addr_str(&entry.upstreams[entry.upstream_idx]);
        if expected != super::relay::normalize_addr(&from) || &entry.question != question {
            return ResponseOutcome::Drop;
        }
        if is_failover_rcode(rcode) && entry.upstream_idx + 1 < entry.upstreams.len() {
            let timeout = self.timeout_for_attempt(entry.upstream_idx + 1, entry.upstreams.len());
            let entry = inner.entries.get_mut(&id).expect("entry present");
            entry.deferred_resp = Some(raw.to_vec());
            entry.upstream_idx += 1;
            entry.expiry = Some(Instant::now() + timeout);
            return ResponseOutcome::Failover {
                upstream_idx: entry.upstream_idx,
                query: entry.raw_query.clone(),
            };
        }
        let entry = self.delete_locked(&mut inner, id).expect("entry present");
        let mut payload = raw.to_vec();
        set_txn_id(&mut payload, entry.client_request_id);
        ResponseOutcome::Deliver {
            client_addr: entry.client_addr,
            payload,
        }
    }

    /// Drops every entry, releasing their slots.
    pub fn clear_all(&self) {
        let mut inner = self.inner.lock();
        let ids: Vec<u16> = inner.entries.keys().copied().collect();
        for id in ids {
            self.delete_locked(&mut inner, id);
        }
    }

    /// Handles attempts whose time has passed at `now`: the next upstream
    /// is tried, or after the last one the entry is dropped, delivering a
    /// deferred failure answer if one was kept.
    pub fn sweep(&self, now: Instant) -> (Vec<Failover>, Vec<Delivery>) {
        let mut inner = self.inner.lock();
        let mut failovers = Vec::new();
        let mut deliveries = Vec::new();
        let expired: Vec<u16> = inner
            .entries
            .iter()
            .filter(|(_, e)| e.expiry.is_some_and(|x| now > x))
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            let entry = inner.entries.get_mut(&id).expect("entry present");
            if entry.upstream_idx + 1 < entry.upstreams.len() {
                entry.upstream_idx += 1;
                let (idx, n) = (entry.upstream_idx, entry.upstreams.len());
                let query = entry.raw_query.clone();
                let timeout = self.timeout_for_attempt(idx, n);
                inner.entries.get_mut(&id).expect("entry present").expiry = Some(now + timeout);
                failovers.push(Failover {
                    upstream_id: id,
                    upstream_idx: idx,
                    query,
                });
                continue;
            }
            let entry = self.delete_locked(&mut inner, id).expect("entry present");
            if let (Some(mut resp), Some(client_addr)) = (entry.deferred_resp, entry.client_addr) {
                set_txn_id(&mut resp, entry.client_request_id);
                deliveries.push(Delivery {
                    client_addr,
                    payload: resp,
                });
            }
        }
        (failovers, deliveries)
    }

    /// Number of pending entries.
    pub fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }

    /// Whether nothing is pending.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tunnel::dns::protocol::rcode;
    use crate::tunnel::dns::test_support::{dns_answer, dns_query, example_question};

    fn record_query(
        p: &PendingRequests,
        lim: &Arc<Limiter>,
        client_id: u16,
        client: Option<SocketAddr>,
        upstreams: &[&str],
    ) -> (u16, Vec<u8>) {
        let slot = SlotGuard::acquire(&lim.in_flight).unwrap();
        let mut raw = dns_query(client_id);
        let id = p
            .record(
                &mut raw,
                slot,
                client_id,
                example_question(),
                client,
                upstreams.iter().map(|s| s.to_string()).collect(),
            )
            .expect("record");
        (id, raw)
    }

    fn check_expiry(expiry: Instant, before: Instant, after: Instant, timeout: Duration) {
        assert!(
            expiry >= before + timeout && expiry <= after + timeout,
            "expiry off"
        );
    }

    #[test]
    fn timeout_for_attempt_cases() {
        let exchange = DEFAULT_UPSTREAM_ATTEMPT_TIMEOUT * 10;
        let ms40 = Duration::from_millis(40);
        let cases = [
            (
                "single upstream gets the whole exchange",
                exchange,
                None,
                0,
                1,
                exchange,
            ),
            (
                "attempt with fallbacks left is capped",
                exchange,
                None,
                0,
                2,
                DEFAULT_UPSTREAM_ATTEMPT_TIMEOUT,
            ),
            (
                "attempt with fallbacks left gets its share",
                DEFAULT_UPSTREAM_ATTEMPT_TIMEOUT,
                None,
                1,
                4,
                DEFAULT_UPSTREAM_ATTEMPT_TIMEOUT / 4,
            ),
            (
                "last upstream gets the whole exchange",
                exchange,
                None,
                1,
                2,
                exchange,
            ),
            (
                "unset exchange timeout uses the default",
                Duration::ZERO,
                None,
                0,
                1,
                DNS_EXCHANGE_TIMEOUT,
            ),
            (
                "attempt timeout overrides an attempt with fallbacks left",
                exchange,
                Some(ms40),
                0,
                2,
                ms40,
            ),
            (
                "attempt timeout overrides the last attempt",
                exchange,
                Some(ms40),
                1,
                2,
                ms40,
            ),
        ];
        for (name, ex, at, idx, n, want) in cases {
            let mut p = PendingRequests::new(Arc::new(Limiter::new()));
            p.exchange_timeout = ex;
            p.attempt_timeout = at;
            assert_eq!(p.timeout_for_attempt(idx, n), want, "{name}");
        }
    }

    #[test]
    fn record() {
        let lim = Arc::new(Limiter::new());
        let p = PendingRequests::new(lim.clone());
        let client: SocketAddr = "169.254.0.2:54321".parse().unwrap();
        let upstreams = vec![
            "[::ffff:10.96.0.10]:53".to_string(),
            "10.96.0.11:53".to_string(),
        ];
        let slot = SlotGuard::acquire(&lim.in_flight).unwrap();
        let mut raw = dns_query(0xbeef);
        let before = Instant::now();
        let id = p
            .record(
                &mut raw,
                slot,
                0xbeef,
                example_question(),
                Some(client),
                upstreams.clone(),
            )
            .unwrap();
        let after = Instant::now();
        assert_eq!(u16::from_be_bytes([raw[0], raw[1]]), id);
        let inner = p.inner.lock();
        let entry = inner
            .entries
            .get(&id)
            .expect("entry keyed by the upstream ID");
        let want = PendingRequest {
            client_request_id: 0xbeef,
            client_addr: Some(client),
            question: example_question(),
            raw_query: raw.clone(),
            upstreams: upstreams.clone(),
            upstream_idx: 0,
            deferred_resp: None,
            expiry: entry.expiry,
        };
        assert_eq!(entry, &want);
        check_expiry(
            entry.expiry.unwrap(),
            before,
            after,
            p.timeout_for_attempt(0, 2),
        );
        assert!(inner.in_use.get(id));
        assert_eq!(lim.in_flight.occupied(), 1);
        let last = raw.len() - 1;
        raw[last] ^= 0xff;
        assert_ne!(entry.raw_query, raw, "entry shares the caller's buffer");
    }

    #[test]
    fn record_allocates_free_id() {
        let lim = Arc::new(Limiter::new());
        let p = PendingRequests::new(lim.clone());
        const FREE: u16 = 0xabcd;
        {
            let mut inner = p.inner.lock();
            for id in 0..=u16::MAX {
                if id != FREE {
                    inner.in_use.set(id);
                }
            }
        }
        let (id, _) = record_query(&p, &lim, 0x1234, None, &["10.96.0.10:53"]);
        assert_eq!(id, FREE);
        let slot = SlotGuard::acquire(&lim.in_flight).unwrap();
        let mut raw = dns_query(0x5678);
        assert!(p
            .record(
                &mut raw,
                slot,
                0x5678,
                example_question(),
                None,
                vec!["10.96.0.10:53".into()]
            )
            .is_none());
        assert_eq!(lim.in_flight.occupied(), 1);
    }

    #[test]
    fn delete_entry_releases_slot_once() {
        let lim = Arc::new(Limiter::new());
        let p = PendingRequests::new(lim.clone());
        SlotGuard::acquire(&lim.in_flight).unwrap().keep();
        let (id, _) = record_query(&p, &lim, 0x1234, None, &["10.96.0.10:53"]);
        p.delete(id);
        p.delete(id);
        assert!(p.is_empty());
        assert_eq!(p.inner.lock().in_use.count(), 0);
        assert_eq!(lim.in_flight.occupied(), 1);
    }

    #[test]
    fn fail_over_on_send_error() {
        let lim = Arc::new(Limiter::new());
        let p = PendingRequests::new(lim.clone());
        let ups = ["10.96.0.10:53", "10.96.0.11:53"];
        let (id, raw) = record_query(&p, &lim, 0x1234, None, &ups);
        assert!(p.fail_over_on_send_error(id.wrapping_add(1), 0).is_none());
        assert!(p.fail_over_on_send_error(id, 1).is_none());
        let before = Instant::now();
        let (next, query) = p.fail_over_on_send_error(id, 0).unwrap();
        let after = Instant::now();
        assert_eq!(next, 1);
        assert_eq!(query, raw);
        {
            let inner = p.inner.lock();
            let e = &inner.entries[&id];
            assert_eq!(e.upstream_idx, 1);
            check_expiry(
                e.expiry.unwrap(),
                before,
                after,
                p.timeout_for_attempt(1, 2),
            );
        }
        assert_eq!(lim.in_flight.occupied(), 1);
        assert!(p.fail_over_on_send_error(id, 0).is_none());
        assert!(p.fail_over_on_send_error(id, 1).is_none());
        assert!(p.is_empty());
        assert_eq!(p.inner.lock().in_use.count(), 0);
        assert_eq!(lim.in_flight.occupied(), 0);
    }

    #[test]
    fn clear_all() {
        let lim = Arc::new(Limiter::new());
        let p = PendingRequests::new(lim.clone());
        for i in 0..3 {
            record_query(&p, &lim, i, None, &["10.96.0.10:53", "10.96.0.11:53"]);
        }
        p.clear_all();
        assert!(p.is_empty());
        assert_eq!(p.inner.lock().in_use.count(), 0);
        assert_eq!(lim.in_flight.occupied(), 0);
    }

    #[test]
    fn sweep() {
        let lim = Arc::new(Limiter::new());
        let p = PendingRequests::new(lim.clone());
        let (id, raw) = record_query(&p, &lim, 0x1234, None, &["10.96.0.10:53", "10.96.0.11:53"]);
        let expiry = p.inner.lock().entries[&id].expiry.unwrap();
        let (f, d) = p.sweep(expiry);
        assert!(
            f.is_empty() && d.is_empty(),
            "nothing until the expiry has passed"
        );
        let now = expiry + Duration::from_nanos(1);
        let (f, d) = p.sweep(now);
        assert_eq!(
            f,
            vec![Failover {
                upstream_id: id,
                upstream_idx: 1,
                query: raw
            }]
        );
        assert!(d.is_empty());
        let expiry = {
            let inner = p.inner.lock();
            let e = &inner.entries[&id];
            assert_eq!(e.upstream_idx, 1);
            assert_eq!(e.expiry, Some(now + p.timeout_for_attempt(1, 2)));
            e.expiry.unwrap()
        };
        let (f, d) = p.sweep(expiry + Duration::from_nanos(1));
        assert!(f.is_empty() && d.is_empty());
        assert!(p.is_empty());
        assert_eq!(lim.in_flight.occupied(), 0);
    }

    #[test]
    fn sweep_delivers_deferred_answer() {
        let lim = Arc::new(Limiter::new());
        let p = PendingRequests::new(lim.clone());
        let client: SocketAddr = "169.254.0.2:54321".parse().unwrap();
        let (id, raw) = record_query(
            &p,
            &lim,
            0x1234,
            Some(client),
            &["10.96.0.10:53", "10.96.0.11:53"],
        );
        p.fail_over_on_send_error(id, 0).unwrap();
        let expiry = {
            let mut inner = p.inner.lock();
            let e = inner.entries.get_mut(&id).unwrap();
            e.deferred_resp = Some(dns_answer(&raw, rcode::SERVER_FAILURE));
            e.expiry.unwrap()
        };
        let (f, d) = p.sweep(expiry + Duration::from_nanos(1));
        assert!(f.is_empty());
        assert_eq!(
            d,
            vec![Delivery {
                client_addr: client,
                payload: dns_answer(&dns_query(0x1234), rcode::SERVER_FAILURE)
            }]
        );
        assert!(p.is_empty());
        assert_eq!(lim.in_flight.occupied(), 0);
    }
}

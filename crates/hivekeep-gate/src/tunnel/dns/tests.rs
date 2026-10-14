//! End-to-end relay tests over loopback sockets.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hivekeep_proto::keeper::v1 as pb;
use parking_lot::Mutex;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::Notify;

use super::evaluator::tests::http_policy;
use super::limiter::{MAX_CONNECTIONS, MAX_IN_FLIGHT};
use super::protocol::{
    query_packet, rcode, read_tcp_frame, rtype, write_tcp_frame, Header, Question, CLASS_INET,
};
use super::test_support::*;
use super::*;

struct StaticPolicy(Result<Option<Arc<pb::EgressPolicy>>, &'static str>);

#[async_trait]
impl PolicySource for StaticPolicy {
    async fn egress_policy(&self) -> Result<Option<Arc<pb::EgressPolicy>>, PolicyError> {
        self.0.clone().map_err(Into::into)
    }
}

fn with_policy() -> Option<Arc<dyn PolicySource>> {
    Some(Arc::new(StaticPolicy(Ok(Some(Arc::new(http_policy(&[
        "*",
    ])))))))
}

#[derive(Default)]
struct SwitchablePolicy(Mutex<Option<Arc<pb::EgressPolicy>>>);

#[async_trait]
impl PolicySource for SwitchablePolicy {
    async fn egress_policy(&self) -> Result<Option<Arc<pb::EgressPolicy>>, PolicyError> {
        Ok(self.0.lock().clone())
    }
}

struct BlockingPolicy(Arc<Notify>);

#[async_trait]
impl PolicySource for BlockingPolicy {
    async fn egress_policy(&self) -> Result<Option<Arc<pb::EgressPolicy>>, PolicyError> {
        self.0.notify_one();
        std::future::pending().await
    }
}

struct Served {
    srv: Server,
    udp: std::net::SocketAddr,
    tcp: std::net::SocketAddr,
}

async fn serve_loopback(relay: &Relay, policy: Option<Arc<dyn PolicySource>>) -> Served {
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let egress = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (u, t) = (udp.local_addr().unwrap(), tcp.local_addr().unwrap());
    let srv = relay.serve_on(
        ServerSockets {
            udp,
            tcp,
            egress_udp: egress,
        },
        policy,
    );
    Served {
        srv,
        udp: u,
        tcp: t,
    }
}

async fn ask_udp(c: &UdpSocket, q: &[u8]) -> Vec<u8> {
    c.send(q).await.unwrap();
    read_within(c).await
}

async fn ask_tcp(c: &mut TcpStream, q: &[u8]) -> Vec<u8> {
    write_tcp_frame(c, q).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), read_tcp_frame(c))
        .await
        .expect("timeout")
        .unwrap()
        .expect("answer")
}

fn check_answer_for(answer: &[u8], id: u16, q: &Question, code: u8) {
    let h = Header::parse(answer).unwrap();
    assert!(
        h.id == id && h.response && h.rcode == code,
        "answer header {h:?}, want {id:#x} rcode {code}"
    );
    assert_eq!(&super::protocol::first_question(answer).unwrap(), q);
}

fn check_answer(answer: &[u8], id: u16, code: u8) {
    check_answer_for(answer, id, &example_question(), code);
}

fn policy_sources() -> Vec<(&'static str, Option<Arc<dyn PolicySource>>)> {
    vec![
        ("no policy", Some(Arc::new(StaticPolicy(Ok(None))))),
        (
            "policy unavailable",
            Some(Arc::new(StaticPolicy(Err("keeper is unreachable")))),
        ),
        ("no policy source", None),
    ]
}

#[tokio::test]
async fn relay_denies_udp_queries_without_policy() {
    for (name, policy) in policy_sources() {
        let asked = Arc::new(AtomicI64::new(0));
        let a = asked.clone();
        let (up, _t) = fake_resolver(move |q| {
            a.fetch_add(1, Ordering::SeqCst);
            dns_answer(q, 0)
        })
        .await;
        let relay = Relay::for_upstreams(vec![up]).unwrap();
        let s = serve_loopback(&relay, policy).await;
        let c = udp_client(s.udp).await;
        check_answer(
            &ask_udp(&c, &dns_query(0x1111)).await,
            0x1111,
            rcode::NAME_ERROR,
        );
        assert_eq!(asked.load(Ordering::SeqCst), 0, "{name}");
    }
}

#[tokio::test]
async fn relay_denies_tcp_queries_without_policy() {
    for (name, policy) in policy_sources() {
        let (up, asked) = tcp_resolver(|q| dns_answer(q, 0)).await;
        let relay = Relay::for_upstreams(vec![up]).unwrap();
        let s = serve_loopback(&relay, policy).await;
        let mut c = TcpStream::connect(s.tcp).await.unwrap();
        check_answer(
            &ask_tcp(&mut c, &dns_query(0x1111)).await,
            0x1111,
            rcode::NAME_ERROR,
        );
        check_answer(
            &ask_tcp(&mut c, &dns_query(0x2222)).await,
            0x2222,
            rcode::NAME_ERROR,
        );
        assert_eq!(asked.load(Ordering::SeqCst), 0, "{name}");
    }
}

#[tokio::test]
async fn relay_follows_policy_changes() {
    for network in ["udp", "tcp"] {
        let up = if network == "udp" {
            fake_resolver(|q| dns_answer(q, 0)).await.0
        } else {
            tcp_resolver(|q| dns_answer(q, 0)).await.0
        };
        let relay = Relay::for_upstreams(vec![up]).unwrap();
        let policy = Arc::new(SwitchablePolicy::default());
        let s = serve_loopback(&relay, Some(policy.clone())).await;
        let udp = udp_client(s.udp).await;
        let mut tcp = TcpStream::connect(s.tcp).await.unwrap();
        macro_rules! ask {
            ($q:expr) => {
                if network == "udp" {
                    ask_udp(&udp, &$q).await
                } else {
                    ask_tcp(&mut tcp, &$q).await
                }
            };
        }
        check_answer(&ask!(dns_query(1)), 1, rcode::NAME_ERROR);
        *policy.0.lock() = Some(Arc::new(http_policy(&["example.com", "*.wild.example"])));
        check_answer(&ask!(dns_query(2)), 2, rcode::SUCCESS);
        let wild = Question::new("api.wild.example.", rtype::A, CLASS_INET);
        check_answer_for(
            &ask!(query_packet(3, &wild, true)),
            3,
            &wild,
            rcode::SUCCESS,
        );
        let unlisted = Question::new("other.example.com.", rtype::A, CLASS_INET);
        check_answer_for(
            &ask!(query_packet(4, &unlisted, true)),
            4,
            &unlisted,
            rcode::NAME_ERROR,
        );
        *policy.0.lock() = None;
        check_answer(&ask!(dns_query(5)), 5, rcode::NAME_ERROR);
    }
}

#[tokio::test]
async fn stop_cancels_a_policy_fetch_in_progress() {
    let (up, _t) = fake_resolver(|q| dns_answer(q, 0)).await;
    let relay = Relay::for_upstreams(vec![up]).unwrap();
    let called = Arc::new(Notify::new());
    let s = serve_loopback(&relay, Some(Arc::new(BlockingPolicy(called.clone())))).await;
    let c = udp_client(s.udp).await;
    c.send(&dns_query(0x1111)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("the relay never fetched the Actor's policy");
    tokio::time::timeout(Duration::from_secs(1), s.srv.stop())
        .await
        .expect("stop");
    assert_eq!(s.srv.limiter().in_flight.occupied(), 0);
}

#[tokio::test]
async fn relay_cancels_udp_exchange() {
    let (silent, mut asked, _t) = silent_resolver().await;
    let second_asked = Arc::new(AtomicBool::new(false));
    let sa = second_asked.clone();
    let (second, _t2) = fake_resolver(move |q| {
        sa.store(true, Ordering::SeqCst);
        dns_answer(q, 0)
    })
    .await;
    let relay = Relay::for_upstreams(vec![silent, second]).unwrap();
    let s = serve_loopback(&relay, with_policy()).await;
    let c = udp_client(s.udp).await;
    c.send(&dns_query(0x1234)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), asked.recv())
        .await
        .expect("resolver never saw the query");
    tokio::time::timeout(Duration::from_secs(5), s.srv.stop())
        .await
        .expect("stop");
    assert!(
        !second_asked.load(Ordering::SeqCst),
        "stopping moved on to the second upstream"
    );
}

#[tokio::test]
async fn relay_forwards_udp_verbatim() {
    let (up, _t) = fake_resolver(|q| dns_answer(q, 0)).await;
    let relay = Relay::for_upstreams(vec![up]).unwrap();
    let s = serve_loopback(&relay, with_policy()).await;
    let c = udp_client(s.udp).await;
    let q = dns_query(0xabcd);
    assert_eq!(ask_udp(&c, &q).await, dns_answer(&q, 0));
}

#[tokio::test]
async fn relay_falls_back_to_the_next_resolver() {
    let dead = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead.local_addr().unwrap().to_string();
    drop(dead);
    let (live, _t) = fake_resolver(|q| dns_answer(q, 0)).await;
    let relay = Relay::for_upstreams(vec![dead_addr, live]).unwrap();
    let s = serve_loopback(&relay, with_policy()).await;
    let c = udp_client(s.udp).await;
    let q = dns_query(0x2468);
    assert_eq!(ask_udp(&c, &q).await, dns_answer(&q, 0));
}

#[test]
fn new_relay_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("resolv.conf");
    std::fs::write(&empty, "").unwrap();
    assert!(Relay::new(&empty).is_err(), "empty resolv.conf");
    assert!(Relay::for_upstreams(vec![]).is_err(), "no upstream");
    assert!(
        Relay::for_upstreams(vec!["10.96.0.10".into()]).is_err(),
        "no port"
    );
}

#[tokio::test]
async fn relay_forwards_answers_larger_than_the_common_buffer() {
    let q = dns_query(0x1357);
    let base = dns_answer(&q, 0);
    let padding: Vec<u8> = (0..MAX_DATAGRAM_SIZE - base.len())
        .map(|i| i as u8)
        .collect();
    let pad = padding.clone();
    let (up, _t) = fake_resolver(move |q| {
        let mut a = dns_answer(q, 0);
        a.extend_from_slice(&pad);
        a
    })
    .await;
    let relay = Relay::for_upstreams(vec![up]).unwrap();
    let s = serve_loopback(&relay, with_policy()).await;
    let c = udp_client(s.udp).await;
    let got = ask_udp(&c, &q).await;
    assert_eq!(
        got.len(),
        MAX_DATAGRAM_SIZE,
        "answer was cut down in the relay"
    );
    let mut want = base;
    want.extend_from_slice(&padding);
    assert_eq!(got, want);
}

#[tokio::test]
async fn relay_drops_queries_beyond_its_in_flight_limit() {
    let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let up = sock.local_addr().unwrap().to_string();
    let in_flight = Arc::new(AtomicI64::new(0));
    let counter = in_flight.clone();
    let _resolver = tokio::spawn(async move {
        let mut buf = vec![0; MAX_DATAGRAM_SIZE];
        // Hold every query: never answer.
        while sock.recv_from(&mut buf).await.is_ok() {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    });
    let relay = Relay::for_upstreams(vec![up]).unwrap();
    let s = serve_loopback(&relay, with_policy()).await;
    let c = udp_client(s.udp).await;
    for i in 0..(MAX_IN_FLIGHT * 4) as u16 {
        c.send(&dns_query(i)).await.unwrap();
    }
    let mut last = -1;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let n = in_flight.load(Ordering::SeqCst);
        if n == last {
            break;
        }
        last = n;
    }
    assert!(in_flight.load(Ordering::SeqCst) <= i64::from(MAX_IN_FLIGHT));
}

async fn read_eof(c: &mut TcpStream) -> bool {
    let mut b = [0u8; 1];
    matches!(
        tokio::time::timeout(Duration::from_secs(5), c.read(&mut b)).await,
        Ok(Ok(0)) | Ok(Err(_))
    )
}

#[tokio::test]
async fn relay_refuses_tcp_connections_beyond_its_limit() {
    let (up, held) = held_tcp_resolver().await;
    let relay = Relay::for_upstreams(vec![up]).unwrap();
    let s = serve_loopback(&relay, with_policy()).await;
    let mut conns = Vec::new();
    for _ in 0..MAX_CONNECTIONS {
        conns.push(TcpStream::connect(s.tcp).await.unwrap());
    }
    wait_for("the relay to fill up", || {
        held.load(Ordering::SeqCst) == i64::from(MAX_CONNECTIONS)
    })
    .await;
    let mut extra = TcpStream::connect(s.tcp).await.unwrap();
    assert!(read_eof(&mut extra).await, "refused connection not closed");
    assert_eq!(held.load(Ordering::SeqCst), i64::from(MAX_CONNECTIONS));
}

#[tokio::test]
async fn relay_closes_tcp_connections_when_serving_ends() {
    let (up, held) = held_tcp_resolver().await;
    let relay = Relay::for_upstreams(vec![up]).unwrap();
    let s = serve_loopback(&relay, with_policy()).await;
    let mut c = TcpStream::connect(s.tcp).await.unwrap();
    wait_for("the connection to reach an upstream", || {
        held.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(s.srv.limiter().connections.occupied(), 1);
    tokio::time::timeout(Duration::from_secs(5), s.srv.stop())
        .await
        .expect("stop");
    assert_eq!(s.srv.limiter().connections.occupied(), 0);
    assert!(read_eof(&mut c).await, "the connection outlived the Actor");
    tokio::time::timeout(Duration::from_secs(5), s.srv.stop())
        .await
        .expect("second stop");
}

#[tokio::test]
async fn stop_cancels_udp_queries_in_flight() {
    let (silent, mut asked, _t) = silent_resolver().await;
    let relay = Relay::for_upstreams(vec![silent]).unwrap();
    let s = serve_loopback(&relay, with_policy()).await;
    let c = udp_client(s.udp).await;
    c.send(&dns_query(0x4567)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), asked.recv())
        .await
        .expect("never asked");
    assert_eq!(s.srv.limiter().in_flight.occupied(), 1);
    tokio::time::timeout(DNS_EXCHANGE_TIMEOUT / 5, s.srv.stop())
        .await
        .expect("stop");
    assert_eq!(s.srv.limiter().in_flight.occupied(), 0);
}

#[tokio::test]
async fn relay_limits_are_per_actor() {
    let (up, held) = held_tcp_resolver().await;
    let relay = Relay::for_upstreams(vec![up]).unwrap();
    let a = serve_loopback(&relay, with_policy()).await;
    let b = serve_loopback(&relay, with_policy()).await;
    let mut conns = Vec::new();
    for _ in 0..MAX_CONNECTIONS {
        conns.push(TcpStream::connect(a.tcp).await.unwrap());
    }
    let max = i64::from(MAX_CONNECTIONS);
    wait_for("actor A to fill up", || held.load(Ordering::SeqCst) == max).await;
    assert_eq!(a.srv.limiter().connections.occupied(), MAX_CONNECTIONS);
    assert_eq!(b.srv.limiter().connections.occupied(), 0);
    let _cb = TcpStream::connect(b.tcp).await.unwrap();
    wait_for("actor B connection to reach upstream", || {
        held.load(Ordering::SeqCst) == max + 1
    })
    .await;
    assert_eq!(b.srv.limiter().connections.occupied(), 1);
}

#[tokio::test]
async fn relay_fails_over_on_server_failure() {
    let healthy_calls = Arc::new(AtomicU32::new(0));
    let hc = healthy_calls.clone();
    let (sick, _t1) = fake_resolver(|q| dns_answer(q, rcode::SERVER_FAILURE)).await;
    let (healthy, _t2) = fake_resolver(move |q| {
        hc.fetch_add(1, Ordering::SeqCst);
        dns_answer(q, 0)
    })
    .await;
    let relay = Relay::for_upstreams(vec![sick, healthy]).unwrap();
    let s = serve_loopback(&relay, with_policy()).await;
    let c = udp_client(s.udp).await;
    let answer = ask_udp(&c, &dns_query(0x1234)).await;
    assert_eq!(
        answer[3] & 0x0f,
        0,
        "returned the failing resolver's answer"
    );
    assert_eq!(healthy_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn relay_returns_server_failure_when_all_fail() {
    let (first, _t1) = fake_resolver(|q| dns_answer(q, rcode::SERVER_FAILURE)).await;
    let (second, _t2) = fake_resolver(|q| dns_answer(q, rcode::REFUSED)).await;
    let relay = Relay::for_upstreams(vec![first, second]).unwrap();
    let s = serve_loopback(&relay, with_policy()).await;
    let c = udp_client(s.udp).await;
    assert_eq!(
        ask_udp(&c, &dns_query(0x2345)).await[3] & 0x0f,
        rcode::REFUSED
    );
}

#[tokio::test]
async fn relay_returns_nxdomain_without_failover() {
    let second_calls = Arc::new(AtomicU32::new(0));
    let sc = second_calls.clone();
    let (first, _t1) = fake_resolver(|q| dns_answer(q, rcode::NAME_ERROR)).await;
    let (second, _t2) = fake_resolver(move |q| {
        sc.fetch_add(1, Ordering::SeqCst);
        dns_answer(q, 0)
    })
    .await;
    let relay = Relay::for_upstreams(vec![first, second]).unwrap();
    let s = serve_loopback(&relay, with_policy()).await;
    let c = udp_client(s.udp).await;
    assert_eq!(
        ask_udp(&c, &dns_query(0x3456)).await[3] & 0x0f,
        rcode::NAME_ERROR
    );
    assert_eq!(
        second_calls.load(Ordering::SeqCst),
        0,
        "failed over on NXDOMAIN"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "needs root: creates network namespaces"]
async fn closing_sandbox_dns_stops_serving() {
    let name = "dns-teardown-test";
    let ns = crate::netns::create_named(name).unwrap();
    let relay = Relay::for_upstreams(vec!["127.0.0.1:53".into()]).unwrap();
    let srv = relay.serve(&ns, None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), srv.stop())
        .await
        .expect("stop");
    drop(ns);
    let _ = crate::netns::remove_named(name);
}

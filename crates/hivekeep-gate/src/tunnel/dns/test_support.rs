//! Test helpers: canned queries and answers and fake resolvers.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;

use super::protocol::{
    qd_count, read_tcp_frame, rtype, write_tcp_frame, Header, Question, CLASS_INET,
};
use super::MAX_DATAGRAM_SIZE;

/// The question every canned query asks.
pub fn example_question() -> Question {
    Question::new("example.com.", rtype::A, CLASS_INET)
}

/// A recursive A query for example.com with transaction ID `id`.
pub fn dns_query(id: u16) -> Vec<u8> {
    let mut msg = vec![
        (id >> 8) as u8,
        id as u8,
        0x01,
        0x00,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
    ];
    for label in ["example", "com"] {
        msg.push(label.len() as u8);
        msg.extend_from_slice(label.as_bytes());
    }
    msg.extend_from_slice(&[0, 0, 1, 0, 1]);
    msg
}

/// `query` turned into an answer with `rcode`.
pub fn dns_answer(query: &[u8], rcode: u8) -> Vec<u8> {
    let mut resp = query.to_vec();
    resp[2] |= 0x80;
    resp[3] = (resp[3] & 0xf0) | rcode;
    resp
}

/// A message with header `h` and `questions`.
pub fn build_message(h: &Header, questions: &[Question]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&h.id.to_be_bytes());
    let mut b2 = (h.opcode & 0x0f) << 3;
    if h.response {
        b2 |= 0x80;
    }
    if h.recursion_desired {
        b2 |= 0x01;
    }
    let mut b3 = h.rcode & 0x0f;
    if h.recursion_available {
        b3 |= 0x80;
    }
    out.extend_from_slice(&[b2, b3]);
    out.extend_from_slice(&(questions.len() as u16).to_be_bytes());
    out.extend_from_slice(&[0; 6]);
    for q in questions {
        let name = std::str::from_utf8(&q.name).unwrap();
        for label in name.trim_end_matches('.').split('.') {
            out.push(label.len() as u8);
            out.extend_from_slice(label.as_bytes());
        }
        out.push(0);
        out.extend_from_slice(&q.qtype.to_be_bytes());
        out.extend_from_slice(&q.qclass.to_be_bytes());
    }
    debug_assert_eq!(qd_count(&out) as usize, questions.len());
    out
}

/// A UDP resolver answering every query with `respond`.
pub async fn fake_resolver(
    respond: impl Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
) -> (String, tokio::task::JoinHandle<()>) {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let mut buf = vec![0; MAX_DATAGRAM_SIZE];
        while let Ok((n, from)) = sock.recv_from(&mut buf).await {
            let _ = sock.send_to(&respond(&buf[..n]), from).await;
        }
    });
    (addr, task)
}

/// A UDP resolver that never answers; the receiver yields once per query.
pub async fn silent_resolver() -> (
    String,
    mpsc::UnboundedReceiver<()>,
    tokio::task::JoinHandle<()>,
) {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap().to_string();
    let (tx, rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let mut buf = vec![0; MAX_DATAGRAM_SIZE];
        while sock.recv_from(&mut buf).await.is_ok() {
            let _ = tx.send(());
        }
    });
    (addr, rx, task)
}

/// A TCP resolver answering every framed query with `respond`; counts
/// queries asked.
pub async fn tcp_resolver(
    respond: impl Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
) -> (String, Arc<AtomicI64>) {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let asked = Arc::new(AtomicI64::new(0));
    let respond = Arc::new(respond);
    let counter = asked.clone();
    tokio::spawn(async move {
        while let Ok((mut conn, _)) = lis.accept().await {
            let (respond, counter) = (respond.clone(), counter.clone());
            tokio::spawn(async move {
                while let Ok(Some(q)) = read_tcp_frame(&mut conn).await {
                    counter.fetch_add(1, Ordering::SeqCst);
                    if write_tcp_frame(&mut conn, &respond(&q)).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (addr, asked)
}

/// A TCP resolver that accepts connections and holds them silently.
pub async fn held_tcp_resolver() -> (String, Arc<AtomicI64>) {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let held = Arc::new(AtomicI64::new(0));
    let counter = held.clone();
    tokio::spawn(async move {
        let mut conns = Vec::new();
        while let Ok((conn, _)) = lis.accept().await {
            conns.push(conn);
            counter.fetch_add(1, Ordering::SeqCst);
        }
    });
    (addr, held)
}

/// Polls `cond` for up to five seconds.
pub async fn wait_for(what: &str, cond: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Receives one datagram within five seconds.
pub async fn read_within(sock: &UdpSocket) -> Vec<u8> {
    let mut buf = vec![0; MAX_DATAGRAM_SIZE * 2];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), sock.recv(&mut buf))
        .await
        .expect("timed out reading the relay's answer")
        .expect("reading the relay's answer");
    buf.truncate(n);
    buf
}

/// A UDP client connected to `addr`.
pub async fn udp_client(addr: SocketAddr) -> UdpSocket {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sock.connect(addr).await.unwrap();
    sock
}

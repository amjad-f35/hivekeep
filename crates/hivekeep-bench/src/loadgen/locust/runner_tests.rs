use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use parking_lot::Mutex;

use super::super::message::text;
use super::super::transport::{ChannelTransport, MasterEnd, ZmqTransport};
use super::*;

/// Counts iterations per user.
#[derive(Default)]
struct Counting {
    seen: Mutex<Vec<VuId>>,
    shutdowns: AtomicUsize,
}

#[async_trait]
impl UserTask for Counting {
    async fn iterate(&self, vu: VuId) {
        self.seen.lock().push(vu);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    async fn shutdown(&self) {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
    }
}

fn fast() -> Timing {
    Timing {
        heartbeat: Duration::from_millis(20),
        stats: Duration::from_millis(30),
    }
}

/// The next worker message of `kind`, skipping heartbeats and stats unless
/// asked for.
async fn next_of(master: &mut MasterEnd, kind: &str) -> Message {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(5), master.from_worker.recv())
            .await
            .unwrap_or_else(|_| panic!("no {kind} message"))
            .expect("worker gone");
        if m.kind == kind {
            return m;
        }
    }
}

fn spawn_msg(class: &str, n: u64) -> Message {
    Message::new(
        "spawn",
        map([
            ("user_classes_count", map([(class, Value::from(n))])),
            ("spawn_rate", Value::from(1.0)),
        ]),
        "",
    )
}

#[tokio::test]
async fn follows_spawn_stop_and_quit() {
    let task = Arc::new(Counting::default());
    let events = Events::new();
    let spawns = Arc::new(Mutex::new(Vec::new()));
    let s = spawns.clone();
    events.subscribe_spawn(Arc::new(move |n, rate| {
        s.lock().push((n, rate));
        Box::pin(async {})
    }));
    let runner = Runner::new("TestUser", task.clone(), events)
        .with_node_id("w1")
        .with_timing(fast());
    let (mut transport, mut master) = ChannelTransport::pair();
    let run = tokio::spawn(async move { runner.run(&mut transport, std::future::pending()).await });

    let ready = next_of(&mut master, "client_ready").await;
    assert_eq!(ready.node_id, "w1");
    assert_eq!(number(&ready.data), Some(-1.0));
    let hb = next_of(&mut master, "heartbeat").await;
    assert_eq!(hb.get("state").and_then(text).as_deref(), Some("ready"));

    master.to_worker.send(spawn_msg("TestUser", 3)).unwrap();
    next_of(&mut master, "spawning").await;
    let done = next_of(&mut master, "spawning_complete").await;
    assert_eq!(done.get("user_count").and_then(number), Some(3.0));
    assert_eq!(
        *spawns.lock(),
        vec![(3, 1.0)],
        "the spawn event carries the message"
    );

    // Every user iterates, and stats carry the user count.
    let st = next_of(&mut master, "stats").await;
    assert_eq!(st.get("user_count").and_then(number), Some(3.0));
    let users: BTreeSet<VuId> = task.seen.lock().iter().copied().collect();
    assert_eq!(users, BTreeSet::from([0, 1, 2]));

    // Scaling down stops the newest users.
    master.to_worker.send(spawn_msg("TestUser", 1)).unwrap();
    next_of(&mut master, "spawning_complete").await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    task.seen.lock().clear();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let users: BTreeSet<VuId> = task.seen.lock().iter().copied().collect();
    assert_eq!(users, BTreeSet::from([0]));

    master
        .to_worker
        .send(Message::new("stop", Value::Nil, ""))
        .unwrap();
    next_of(&mut master, "client_stopped").await;
    next_of(&mut master, "client_ready").await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    task.seen.lock().clear();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        task.seen.lock().is_empty(),
        "users still iterate after stop"
    );

    // A spawn for another class starts nothing here.
    master.to_worker.send(spawn_msg("OtherUser", 5)).unwrap();
    let done = next_of(&mut master, "spawning_complete").await;
    assert_eq!(done.get("user_count").and_then(number), Some(0.0));

    master
        .to_worker
        .send(Message::new("quit", Value::Nil, ""))
        .unwrap();
    assert_eq!(run.await.unwrap().unwrap(), Exit::MasterQuit);
    assert_eq!(
        task.shutdowns.load(Ordering::SeqCst),
        0,
        "shutdown is the caller's"
    );
}

#[tokio::test]
async fn a_signal_tells_the_master_and_ends_the_run() {
    let task = Arc::new(Counting::default());
    let runner = Runner::new("TestUser", task, Events::new()).with_timing(fast());
    let (mut transport, mut master) = ChannelTransport::pair();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let run = tokio::spawn(async move {
        runner
            .run(&mut transport, async {
                let _ = rx.await;
            })
            .await
    });
    next_of(&mut master, "client_ready").await;
    tx.send(()).unwrap();
    next_of(&mut master, "quit").await;
    assert_eq!(run.await.unwrap().unwrap(), Exit::Signal);
}

#[tokio::test]
async fn a_closed_connection_is_an_error() {
    let runner =
        Runner::new("TestUser", Arc::new(Counting::default()), Events::new()).with_timing(fast());
    let (mut transport, master) = ChannelTransport::pair();
    drop(master);
    assert!(runner
        .run(&mut transport, std::future::pending())
        .await
        .is_err());
}

#[test]
fn legacy_spawn_counts_are_read() {
    let runner = Runner::new("TestUser", Arc::new(Counting::default()), Events::new());
    let m = Message::new("spawn", map([("num_users", Value::from(4))]), "");
    assert_eq!(runner.wanted_users(&m), 4);
    assert!(runner.node_id().contains('_'));
}

async fn zrecv(master: &mut zeromq::RouterSocket) -> Message {
    use zeromq::SocketRecv;
    let frames = tokio::time::timeout(Duration::from_secs(5), master.recv())
        .await
        .unwrap()
        .unwrap()
        .into_vec();
    assert_eq!(
        frames[0].as_ref(),
        b"zmq-worker",
        "routing frame is the node ID"
    );
    Message::decode(frames.last().unwrap()).unwrap()
}

/// End to end over ZeroMQ: a ROUTER socket stands in for the master.
#[tokio::test]
async fn speaks_the_protocol_over_zmq() {
    use zeromq::{Socket, SocketSend, ZmqMessage};
    let mut master = zeromq::RouterSocket::new();
    let endpoint = master.bind("tcp://127.0.0.1:0").await.unwrap();
    let port: u16 = endpoint
        .to_string()
        .rsplit(':')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let task = Arc::new(Counting::default());
    let runner = Runner::new("TestUser", task.clone(), Events::new())
        .with_node_id("zmq-worker")
        .with_timing(fast());
    let run = tokio::spawn(async move {
        let mut t = ZmqTransport::connect("127.0.0.1", port, "zmq-worker")
            .await
            .unwrap();
        runner.run(&mut t, std::future::pending()).await
    });
    let first = zrecv(&mut master).await;
    assert_eq!(first.kind, "client_ready");
    let send = |m: Message| {
        let mut z = ZmqMessage::from(b"zmq-worker".to_vec());
        z.push_back(m.encode().into());
        z
    };
    master.send(send(spawn_msg("TestUser", 2))).await.unwrap();
    loop {
        let m = zrecv(&mut master).await;
        if m.kind == "spawning_complete" {
            assert_eq!(m.get("user_count").and_then(number), Some(2.0));
            break;
        }
    }
    master
        .send(send(Message::new("quit", Value::Nil, "")))
        .await
        .unwrap();
    assert_eq!(run.await.unwrap().unwrap(), Exit::MasterQuit);
    assert!(!task.seen.lock().is_empty());
}

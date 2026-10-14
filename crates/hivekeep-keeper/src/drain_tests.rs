use std::sync::Arc;
use std::time::{Duration, Instant};

use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::keeper::v1::keeper_client::KeeperClient;
use hivekeep_proto::keeper::v1::keeper_server::KeeperServer;
use tokio::sync::Notify;
use tonic::transport::{Channel, Server};

use super::*;
use crate::service_stubs::{loopback, StubBehaviour, StubKeeper};

/// A server whose Run blocks until `block` is notified, the shutdown
/// context stopping it, and a client of it.
async fn blocking_server(
    block: Arc<Notify>,
) -> (
    JoinHandle<Result<(), tonic::transport::Error>>,
    Ctx,
    CancellationToken,
    KeeperClient<Channel>,
) {
    let (incoming, addr) = loopback().await;
    let force = CancellationToken::new();
    let incoming = force_closable(incoming, force.clone());
    let shutdown = Ctx::background();
    let svc = KeeperServer::new(StubKeeper(StubBehaviour { block: Some(block) }));
    let server = tokio::spawn(
        Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(incoming, shutdown.clone().done_owned()),
    );
    let client = KeeperClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    (server, shutdown, force, client)
}

fn call_run(mut client: KeeperClient<Channel>) -> JoinHandle<Result<(), tonic::Status>> {
    tokio::spawn(async move { client.run(pb::RunRequest::default()).await.map(|_| ()) })
}

/// An RPC in flight when the drain starts finishes, and readiness is off
/// afterwards.
#[tokio::test]
async fn drain_on_shutdown_in_flight_finishes() {
    let block = Arc::new(Notify::new());
    let (server, shutdown, force, client) = blocking_server(block.clone()).await;
    let rpc = call_run(client);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let readiness = Arc::new(Readiness::default());
    readiness.mark_serving();
    let r2 = readiness.clone();
    let drain = tokio::spawn(async move {
        drain_on_shutdown(
            &r2,
            Duration::ZERO,
            Duration::from_secs(5),
            &shutdown,
            &force,
            server,
        )
        .await
    });
    // Released shortly after the drain begins: a graceful drain waits.
    tokio::time::sleep(Duration::from_millis(100)).await;
    block.notify_waiters();

    let res = tokio::time::timeout(Duration::from_secs(3), rpc)
        .await
        .expect("in-flight RPC did not complete")
        .unwrap();
    assert!(
        res.is_ok(),
        "in-flight RPC failed during a graceful drain: {res:?}"
    );
    let outcome = tokio::time::timeout(Duration::from_secs(3), drain)
        .await
        .expect("drain did not complete")
        .unwrap();
    assert_eq!(outcome, DrainOutcome::Graceful);
    assert!(readiness.check().is_err(), "ready after the drain");
}

/// An RPC still running past the drain timeout is stopped by force.
#[tokio::test]
async fn drain_on_shutdown_force_stops_after_timeout() {
    let block = Arc::new(Notify::new()); // never notified
    let (server, shutdown, force, client) = blocking_server(block).await;
    let rpc = call_run(client);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let readiness = Readiness::default();
    let start = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        drain_on_shutdown(
            &readiness,
            Duration::ZERO,
            Duration::from_millis(200),
            &shutdown,
            &force,
            server,
        ),
    )
    .await
    .expect("drain did not force-stop within the deadline");
    assert_eq!(outcome, DrainOutcome::Forced);
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "force stop took {:?}, want about the drain timeout",
        start.elapsed()
    );
    let res = tokio::time::timeout(Duration::from_secs(2), rpc)
        .await
        .expect("in-flight RPC did not return after the force stop")
        .unwrap();
    assert!(res.is_err(), "in-flight RPC survived the force stop");
    assert!(readiness.check().is_err(), "ready after the drain");
}

/// /readyz fails until the gRPC port is open, whatever else is up.
#[test]
fn serving_check() {
    let r = Readiness::default();
    assert!(r.check().is_err(), "ready before the port was opened");
    r.mark_serving();
    assert!(r.check().is_ok(), "not ready after the port opened");
    assert_eq!(
        crate::http_endpoints::respond("/readyz", None, Some(&r)).0,
        200
    );
    r.mark_not_ready();
    assert_eq!(
        crate::http_endpoints::respond("/readyz", None, Some(&r)).0,
        503
    );
}

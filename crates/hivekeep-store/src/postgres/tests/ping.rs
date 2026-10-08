//! Health checks over the dedicated probe connection.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use super::super::PROBE_POOL_MAX;
use super::*;
use crate::api::HealthCheck;

/// A saturated request pool cannot fail a health check.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn ping_ignores_saturated_request_pool() {
    let schema = unique_schema("ping_saturated");
    let store = PgStore::connect(&PgConfig {
        max_connections: Some(1),
        ..config(&schema)
    })
    .await
    .unwrap();
    assert_eq!(
        store.probe_pool.options().get_max_connections(),
        PROBE_POOL_MAX
    );
    assert_eq!(store.probe_pool.options().get_min_connections(), 0);

    let held = store.pool.acquire().await.unwrap();
    let budget = Duration::from_millis(500);
    let saturated = tokio::time::timeout(budget, store.pool.acquire()).await;
    assert!(
        saturated.is_err(),
        "a request connection was free; the pool is not saturated"
    );
    tokio::time::timeout(budget, store.ping())
        .await
        .expect("ping waited on the request pool")
        .expect("ping with a saturated request pool");
    drop(held);
    store.close().await;
    drop_schema(&schema).await;
}

#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn ping_fails_after_close() {
    let db = fresh("ping_closed").await;
    db.store.ping().await.unwrap();
    db.store.close().await;
    assert!(db.store.ping().await.is_err());
    drop_schema(&db.schema).await;
}

/// Forwards TCP connections on `addr` (port 0 for a fresh one) to `target`
/// until the returned token is cancelled, which also cuts every forwarded
/// connection. Returns the bound address.
async fn forward(addr: SocketAddr, target: SocketAddr) -> (SocketAddr, CancellationToken) {
    let listener = TcpListener::bind(addr).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let token = stop.clone();
    tokio::spawn(async move {
        loop {
            let mut client = tokio::select! {
                _ = stop.cancelled() => return,
                accepted = listener.accept() => match accepted {
                    Ok((c, _)) => c,
                    Err(_) => return,
                },
            };
            let stop = stop.clone();
            tokio::spawn(async move {
                let Ok(mut upstream) = TcpStream::connect(target).await else {
                    return;
                };
                tokio::select! {
                    _ = stop.cancelled() => {}
                    _ = tokio::io::copy_bidirectional(&mut client, &mut upstream) => {}
                }
            });
        }
    });
    (bound, token)
}

/// A health check reports the database unreachable while it is, and
/// healthy again once it is back.
#[tokio::test]
#[ignore = "needs HK_TEST_DATABASE_URL"]
async fn ping_follows_store_reachability() {
    let direct = url();
    let opts: sqlx::postgres::PgConnectOptions = direct.parse().unwrap();
    let host_port = format!("{}:{}", opts.get_host(), opts.get_port());
    let target = tokio::net::lookup_host(host_port.as_str())
        .await
        .unwrap()
        .next()
        .unwrap();
    let (addr, up) = forward("127.0.0.1:0".parse().unwrap(), target).await;
    let proxied = direct.replacen(&host_port, &addr.to_string(), 1);
    assert_ne!(
        proxied, direct,
        "HK_TEST_DATABASE_URL must spell out host:port"
    );

    let schema = unique_schema("ping_reach");
    let store = PgStore::connect(&PgConfig {
        read_write_url: proxied,
        owner_url: Some(direct.clone()),
        ..config(&schema)
    })
    .await
    .unwrap();
    let ping = || async {
        matches!(
            tokio::time::timeout(Duration::from_secs(2), store.ping()).await,
            Ok(Ok(()))
        )
    };
    assert!(ping().await, "ping with the store reachable");
    up.cancel();
    assert!(!ping().await, "ping succeeded with the store unreachable");
    let (_, up) = forward(addr, target).await;
    assert!(ping().await, "ping after the store became reachable again");

    up.cancel();
    store.close().await;
    drop_schema(&schema).await;
}

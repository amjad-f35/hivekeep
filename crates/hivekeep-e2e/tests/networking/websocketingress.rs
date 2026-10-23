//! WebSocket ingress through the router to an Actor running the test
//! server's WebSocket echo.

use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use hivekeep_e2e::fixture::{deploy_hive_fixture, FixtureManifests, FixtureOptions};
use hivekeep_e2e::harness::T;
use hivekeep_e2e::install;
use hivekeep_e2e::sandbox::HiveFixture;
use hivekeep_e2e::suite::{clients, e2e_test};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use crate::httpwire::router_tls;
use crate::networking::{actor_ref, bucket, create_and_resume_hive_actor, must_router_client};

/// The WebSocket fixture's manifests.
pub const WEBSOCKET_FIXTURE: FixtureManifests = FixtureManifests {
    pool: "fixtures/testserver/websocket.yaml.tmpl",
    template: "fixtures/testserver/websocket-template.yaml.tmpl",
};

/// Installs the WebSocket fixture for the sandbox class under test and
/// waits for its golden snapshot.
async fn deploy_websocket_fixture(t: &T) -> HiveFixture {
    let bucket = bucket(t);
    let (hive, _) = deploy_hive_fixture(
        t,
        &clients(),
        WEBSOCKET_FIXTURE,
        &bucket,
        "websocket",
        &[],
        FixtureOptions::default(),
    )
    .await;
    HiveFixture {
        pool_namespace: hive.clone(),
        hive,
        name: "websocket".into(),
        pool_name: "websocket".into(),
        deploy_with: "the networking suite itself (see deploy_websocket_fixture)".into(),
    }
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn websocket_ingress_ping() {
    e2e_test("TestWebsocketIngressPing", |t| async move {
        let fixture = deploy_websocket_fixture(&t).await;
        let (_, actor_name, _) = create_and_resume_hive_actor(&t, "websocket", &fixture).await;
        let router = must_router_client(&t).await;
        let url = format!("wss://{}/ws", router.address());
        let aref = actor_ref(&actor_name);
        // Ride out the route's propagation to the router (up to ~30 s).
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut ws = loop {
            let mut req = url
                .as_str()
                .into_client_request()
                .unwrap_or_else(|e| t.fatal(format!("parse ws URL: {e}")));
            req.headers_mut().insert(
                install::TARGET_ACTOR_HEADER,
                aref.to_string().parse().expect("header"),
            );
            let tls = router_tls(&t, &router, router.address(), &[b"http/1.1"]).await;
            match tokio_tungstenite::client_async(req, tls).await {
                Ok((ws, _)) => break ws,
                Err(e) if Instant::now() > deadline => {
                    t.fatal(format!("websocket dial {url} failed after 30s: {e}"))
                }
                Err(e) => {
                    t.log(format!("websocket dial failed ({e}), retrying..."));
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };
        ws.send(Message::text("PING"))
            .await
            .unwrap_or_else(|e| t.fatal(format!("write: {e}")));
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap_or_else(|_| t.fatal("read message: timed out"))
            .unwrap_or_else(|| t.fatal("read message: connection closed"))
            .unwrap_or_else(|e| t.fatal(format!("read message: {e}")));
        let Message::Text(text) = msg else {
            t.fatal(format!("expected text message, got {msg:?}"))
        };
        if text.as_str() != "PONG" {
            t.fatal(format!("expected PONG, got {}", text.as_str()));
        }
        let _ = ws.close(None).await;
        t.log("Websocket PingPong succeeded");
    });
}

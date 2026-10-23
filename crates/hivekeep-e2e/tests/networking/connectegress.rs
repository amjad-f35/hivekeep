//! The only CONNECT the egress gate tunnels is the one an Actor's tunnel
//! wraps its traffic in, authenticated by the Actor's certificate and
//! addressed to what the Actor dialed. A CONNECT the Actor sends itself, as
//! a client configured with a forward proxy does, arrives as a request
//! inside that tunnel and is refused with a 403: otherwise it would be
//! policed as its authority's host on the dialed port while the tunnel went
//! to the port the authority names.
//!
//! Each test sends the CONNECT to two proxies: an origin that answers any
//! CONNECT 200 (so a 403 cannot be its doing), and the gate's own Service.

use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::env::{resource_name, system_namespace};
use hivekeep_e2e::harness::T;
use hivekeep_e2e::images::Image;
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::serverpod::ServerPod;
use hivekeep_e2e::suite::e2e_test;
use serde::Deserialize;

use crate::networking::{
    lossy, post_through_egress_actor_until, prepare_protocol_actor, prepare_protocol_origin,
    reached,
};

/// The body the gate refuses the CONNECT with.
pub const CONNECT_DENIED_BODY: &str = "CONNECT egress is not supported";
/// What the Actor asks for a tunnel to; it is refused unresolved, so it
/// need not exist.
pub const CONNECT_TUNNEL_TARGET: &str = "example.com:443";

/// The egress demo's `/connect` observation: the proxy's answer.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct EgressConnectResponse {
    #[serde(rename = "statusCode")]
    status_code: u16,
    body: String,
    tls: bool,
}

/// The egress gate's CONNECT front door, the address the control plane
/// hands every keeper.
pub fn egress_gateway_address() -> String {
    format!(
        "{}.{}.svc:443",
        resource_name("gate-egress"),
        system_namespace()
    )
}

/// The test server answering CONNECT the way a forward proxy does.
fn connect_proxy_spec(port: u16, tls_port: u16) -> ServerPod {
    ServerPod {
        name: "connect-proxy".into(),
        image: Image::Testserver,
        args: vec!["http".into(), "--accept-connect".into()],
        port,
        target_port: 8080,
        tls_port,
        ..Default::default()
    }
}

/// A cleartext CONNECT, as a client with an `http://` proxy sends, to port
/// 3128 (neither 80 nor 443), so the refusal does not depend on the port.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_connect() {
    e2e_test("TestActorEgressConnect", |t| async move {
        let (_, proxy, _) = prepare_protocol_origin(&t, connect_proxy_spec(3128, 0), false).await;
        let (_, router, aref) = prepare_protocol_actor(&t, "connect").await;
        let (r, a, p) = (&router, &aref, &proxy);
        t.run("to a proxy", |t| async move {
            assert_actor_connect_refused(&t, r, a, p, false).await
        })
        .await;
        t.run("to the gateway", |t| async move {
            assert_actor_connect_refused(&t, r, a, &egress_gateway_address(), false).await
        })
        .await;
    });
}

/// The CONNECT inside TLS, as a client with an `https://` proxy sends. The
/// gate terminates that TLS under the Actor's https rule, so the CONNECT is
/// read and refused on the decrypted leg.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_secure_connect() {
    e2e_test("TestActorEgressSecureConnect", |t| async move {
        let (_, proxy, _) = prepare_protocol_origin(&t, connect_proxy_spec(80, 443), true).await;
        let (_, router, aref) = prepare_protocol_actor(&t, "connect-tls").await;
        let (r, a, p) = (&router, &aref, &proxy);
        t.run("to a proxy", |t| async move {
            assert_actor_connect_refused(&t, r, a, p, true).await
        })
        .await;
        t.run("to the gateway", |t| async move {
            assert_actor_connect_refused(&t, r, a, &egress_gateway_address(), true).await
        })
        .await;
    });
}

/// Has the Actor send `proxy` a CONNECT (over TLS when `encrypted`) and
/// checks the gate answered 403. Retried until the Actor has an answer: a
/// failed handshake is a 502 from the demo, and the first handshake with a
/// minted leaf can fail until the gate's CA reaches the Actor.
async fn assert_actor_connect_refused(
    t: &T,
    router: &RouterClient,
    aref: &ActorRef,
    proxy: &str,
    encrypted: bool,
) {
    let payload = serde_json::to_vec(
        &serde_json::json!({ "proxy": proxy, "target": CONNECT_TUNNEL_TARGET, "tls": encrypted }),
    )
    .unwrap_or_default();
    let (status, raw) =
        post_through_egress_actor_until(t, router, aref, "/connect", payload, reached).await;
    if status != 200 {
        t.fatal(format!("the actor got no answer to its CONNECT {CONNECT_TUNNEL_TARGET} via {proxy}: HTTP {status}; body: {}", lossy(&raw)));
    }
    let got: EgressConnectResponse = serde_json::from_slice(&raw).unwrap_or_else(|e| {
        t.fatal(format!(
            "decoding the CONNECT observation: {e} (body {:?})",
            lossy(&raw)
        ))
    });
    if got.status_code / 100 == 2 {
        t.fatal(format!("CONNECT {CONNECT_TUNNEL_TARGET} via {proxy} was answered {}: the gateway opened a tunnel for the actor's own CONNECT: {got:?}", got.status_code));
    }
    if got.status_code != 403 || got.body != CONNECT_DENIED_BODY {
        t.fatal(format!(
            "CONNECT {CONNECT_TUNNEL_TARGET} via {proxy} was answered {} {:?}, want 403 {CONNECT_DENIED_BODY:?}: {got:?}",
            got.status_code, got.body
        ));
    }
    if got.tls != encrypted {
        t.error(format!("CONNECT {CONNECT_TUNNEL_TARGET} via {proxy} went over verified TLS = {}, want {encrypted}: {got:?}", got.tls));
    }
    t.log(format!(
        "the gateway refused the actor's CONNECT {CONNECT_TUNNEL_TARGET} via {proxy}: {got:?}"
    ));
}

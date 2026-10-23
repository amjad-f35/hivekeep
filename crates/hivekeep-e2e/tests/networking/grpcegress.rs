//! Egress with gRPC, which fails in ways the HTTP tests cannot see: the
//! gate decides the cleartext requests inside the Actor's CONNECT one by
//! one, so HTTP/2 framing must survive that parsing and the gRPC status must
//! arrive in trailers after the body. A path that downgraded to HTTP/1.1 or
//! dropped trailers would pass the HTTP tests and fail here.

use hivekeep_e2e::egresspolicy::egress_allow_all;
use hivekeep_e2e::images::Image;
use hivekeep_e2e::sandbox::egress_fixture;
use hivekeep_e2e::serverpod::{deploy_server_pod, ServerPod};
use hivekeep_e2e::suite::e2e_test;
use serde::Deserialize;

use crate::networking::{
    assert_egress_gateway_connect, create_and_resume_actor_with_egress, log_scan_start, lossy,
    must_router_client, post_through_egress_actor,
};
use hivekeep_core::resource::ActorRef;

/// The origin: a cleartext-HTTP/2 gRPC server in its own namespace, so
/// nothing depends on the internet. Readiness goes through the health
/// service, since a gRPC server answers an HTTP GET with a protocol error.
pub fn grpc_echo() -> ServerPod {
    ServerPod {
        name: "grpcecho".into(),
        image: Image::Testserver,
        args: vec!["grpc".into()],
        port: 50051,
        grpc_probe: true,
        ..Default::default()
    }
}

/// The egress demo's `/grpc` answer, kept local: the wire shape between the
/// two is what this pins.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct GrpcEchoResponse {
    message: String,
    stream: Vec<GrpcEchoStreamed>,
    bidi: Vec<GrpcEchoStreamed>,
    code: String,
    error: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct GrpcEchoStreamed {
    message: String,
    index: i32,
}

/// All three streaming shapes in one request, because each fails
/// differently: unary is a status in trailers, a server stream is many
/// frames over a held-open connection, and a bidirectional stream has both
/// directions carrying frames at once before one half-closes.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_grpc() {
    e2e_test("TestActorEgressGRPC", |t| async move {
        let spec = grpc_echo();
        let target = deploy_server_pod(&t, &spec).await.address();
        let (hive, actor_name, _) = create_and_resume_actor_with_egress(
            &t,
            "egress-grpc",
            &egress_fixture(),
            &egress_allow_all(),
        )
        .await;
        let router = must_router_client(&t).await;
        let since = log_scan_start();
        const MESSAGE: &str = "hello over grpc";
        const STREAM_COUNT: usize = 3;
        const BIDI_COUNT: usize = 3;
        let payload = serde_json::to_vec(&serde_json::json!({ "target": target, "message": MESSAGE, "streamCount": STREAM_COUNT, "bidiCount": BIDI_COUNT })).unwrap_or_default();
        let aref = ActorRef::new(hive.clone(), actor_name.clone());
        let (status, body) = post_through_egress_actor(&t, &router, &aref, "/grpc", payload).await;
        if status != 200 {
            t.fatal(format!(
                "Actor gRPC egress to {target} returned HTTP {status}, want 200; body: {}",
                lossy(&body)
            ));
        }
        let got: GrpcEchoResponse = serde_json::from_slice(&body).unwrap_or_else(|e| {
            t.fatal(format!(
                "decoding the Actor's gRPC response: {e}; body: {}",
                lossy(&body)
            ))
        });
        if got.message != MESSAGE {
            t.error(format!(
                "unary Echo returned {:?}, want {MESSAGE:?}",
                got.message
            ));
        }
        // The status travels after the body: this is the trailer assertion.
        if got.code != "OK" {
            t.error(format!(
                "gRPC status = {:?}, want OK; error: {}",
                got.code, got.error
            ));
        }
        if got.stream.len() != STREAM_COUNT {
            t.fatal(format!(
                "EchoStream returned {} responses, want {STREAM_COUNT}: {:?}",
                got.stream.len(),
                got.stream
            ));
        }
        for (i, r) in got.stream.iter().enumerate() {
            if r.message != MESSAGE {
                t.error(format!(
                    "stream response {i} message = {:?}, want {MESSAGE:?}",
                    r.message
                ));
            }
            if r.index as usize != i {
                t.error(format!("stream response {i} index = {}, want {i}", r.index));
            }
        }
        // The Actor sends each bidi message only after reading the previous
        // answer, so a one-direction-at-a-time path would hang until the
        // Actor's own timeout and come back as a 502, not a short answer.
        if got.bidi.len() != BIDI_COUNT {
            t.fatal(format!(
                "EchoBidi returned {} responses, want {BIDI_COUNT}: {:?}",
                got.bidi.len(),
                got.bidi
            ));
        }
        for (i, r) in got.bidi.iter().enumerate() {
            let want = format!("{MESSAGE}-{i}");
            if r.message != want {
                t.error(format!(
                    "bidi response {i} message = {:?}, want {want:?}",
                    r.message
                ));
            }
            if r.index as usize != i {
                t.error(format!("bidi response {i} index = {}, want {i}", r.index));
            }
        }
        t.log(format!(
            "Actor gRPC egress to {target} succeeded; body: {}",
            lossy(&body)
        ));
        // All of the above would pass over a masqueraded path too; this is
        // what says it went through the gate on this Actor's certificate.
        assert_egress_gateway_connect(&t, since, &hive, &actor_name, &spec.port.to_string()).await;
    });
}

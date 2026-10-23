//! The ingress protocol contract: non-gRPC HTTP/2 is downgraded for an
//! HTTP/1.1-only Actor, gRPC is carried as real HTTP/2 and fails loudly
//! against such an Actor, and real gRPC to a gRPC Actor works in all three
//! streaming shapes.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::fixture::{deploy_hive_fixture, FixtureManifests, FixtureOptions};
use hivekeep_e2e::harness::T;
use hivekeep_e2e::install;
use hivekeep_e2e::proto::grpcecho::echo_client::EchoClient;
use hivekeep_e2e::proto::grpcecho::{EchoRequest, EchoStreamRequest};
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::sandbox::{counter_fixture, HiveFixture};
use hivekeep_e2e::suite::{clients, e2e_test};
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::TokioIo;
use tonic::codegen::Service;
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint, Uri};

use crate::httpwire::{h2_handshake, router_tls};
use crate::networking::{
    actor_ref, bucket, create_and_resume_hive_actor, must_router_client, wait_for_route_ready,
};

/// The fixture providing a gRPC-speaking Actor: the same `testserver grpc`
/// echo the gRPC egress test deploys as a plain pod.
pub const GRPC_ECHO_FIXTURE: FixtureManifests = FixtureManifests {
    pool: "fixtures/testserver/grpcecho.yaml.tmpl",
    template: "fixtures/testserver/grpcecho-template.yaml.tmpl",
};

/// An h2 client reaches the HTTP/1.1-only counter (non-gRPC HTTP/2 is
/// downgraded), while a gRPC-shaped request is carried as HTTP/2 and fails
/// with gRPC Unavailable rather than being silently downgraded, which would
/// strip the trailers gRPC needs.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn ingress_protocol_downgrade() {
    e2e_test("TestIngressProtocolDowngrade", |t| async move {
        let (_, actor_name, _) =
            create_and_resume_hive_actor(&t, "protodowngrade", &counter_fixture()).await;
        let aref = actor_ref(&actor_name);
        let rc = must_router_client(&t).await;
        // Wait for the route over HTTP/1.1 first, so the protocol assertions
        // never race readiness.
        wait_for_route_ready(&t, "HTTP/1.1 access through ingress", || {
            rc.get(&aref, "/readyz")
        })
        .await;
        let (r, a) = (&rc, &aref);

        t.run("h2 client reaches h1-only actor", |t| async move {
            let resp = h2_request(&t, r, a, http::Method::GET, "/readyz", None).await;
            if resp.version != http::Version::HTTP_2 {
                t.error(format!("downstream proto = {:?}, want HTTP/2.0 (the client really negotiated h2)", resp.version));
            }
            if resp.status != 200 {
                t.fatal(format!("h2 GET = {} (body {:?}), want 200: non-gRPC HTTP/2 must be downgraded for HTTP/1.1-only actors", resp.status, resp.text()));
            }
            if !resp.text().contains("ok") {
                t.error(format!("h2 GET body = {:?}, want the actor's health payload", resp.text()));
            }
        })
        .await;

        t.run("grpc to non-grpc actor fails loudly", |t| async move {
            let resp = h2_request(&t, r, a, http::Method::POST, "/count", Some("application/grpc")).await;
            // The counter cannot speak h2c. The dataplane reaches the Actor's
            // tunnel through CONNECT, where the failure is gRPC Unavailable; an
            // HTTP 502 would mean a different path was taken.
            let grpc_status = resp.header("grpc-status").or_else(|| resp.trailer("grpc-status")).unwrap_or_default();
            if resp.status != 200 || grpc_status != "14" {
                t.fatal(format!(
                    "gRPC-shaped POST = {}, grpc-status {grpc_status:?} (body {:?}), want gRPC Unavailable: gRPC must not be silently downgraded to HTTP/1.1",
                    resp.status,
                    resp.text()
                ));
            }
        })
        .await;
    });
}

/// A response read over HTTP/2.
struct H2Response {
    status: u16,
    version: http::Version,
    headers: http::HeaderMap,
    trailers: http::HeaderMap,
    body: Bytes,
}

impl H2Response {
    fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    }
    fn trailer(&self, name: &str) -> Option<String> {
        self.trailers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    }
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// One request to `aref` over a fresh HTTP/2 connection to the router,
/// negotiated by ALPN on its HTTPS listener.
async fn h2_request(
    t: &T,
    rc: &RouterClient,
    aref: &ActorRef,
    method: http::Method,
    path: &str,
    content_type: Option<&str>,
) -> H2Response {
    let work = async {
        let tls = router_tls(t, rc, rc.address(), &[b"h2"]).await;
        let mut sender = h2_handshake(t, tls).await;
        let mut req = http::Request::builder()
            .method(method)
            .uri(format!("https://{}{path}", rc.address()))
            .header(install::TARGET_ACTOR_HEADER, aref.to_string());
        if let Some(ct) = content_type {
            req = req.header(http::header::CONTENT_TYPE, ct);
        }
        let req = req
            .body(Empty::<Bytes>::new())
            .unwrap_or_else(|e| t.fatal(format!("building request: {e}")));
        let resp = sender
            .send_request(req)
            .await
            .unwrap_or_else(|e| t.fatal(format!("h2 request through ingress: {e}")));
        let (status, version, headers) = (
            resp.status().as_u16(),
            resp.version(),
            resp.headers().clone(),
        );
        let collected = resp
            .into_body()
            .collect()
            .await
            .unwrap_or_else(|e| t.fatal(format!("reading the h2 response: {e}")));
        let trailers = collected.trailers().cloned().unwrap_or_default();
        H2Response {
            status,
            version,
            headers,
            trailers,
            body: collected.to_bytes(),
        }
    };
    tokio::time::timeout(Duration::from_secs(30), work)
        .await
        .unwrap_or_else(|_| t.fatal("h2 request timed out"))
}

/// Dials the router over TLS with ALPN h2 for tonic, which then speaks
/// plain HTTP/2 over the connection.
#[derive(Clone)]
struct RouterConnector {
    config: std::sync::Arc<rustls::ClientConfig>,
    addr: String,
}

impl Service<Uri> for RouterConnector {
    type Response = TokioIo<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Uri) -> Self::Future {
        let (config, addr) = (self.config.clone(), self.addr.clone());
        Box::pin(async move {
            let tcp = tokio::net::TcpStream::connect(&addr).await?;
            let tls = tokio_rustls::TlsConnector::from(config)
                .connect(hivekeep_e2e::ingressauth::router_rustls_server_name(), tcp)
                .await?;
            Ok(TokioIo::new(tls))
        })
    }
}

/// A real gRPC client reaching a real gRPC Actor through the router: unary
/// (status in trailers), server stream (many frames over a held-open
/// connection) and bidi (both directions at once, then a half-close).
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn ingress_grpc() {
    e2e_test("TestIngressGRPC", |t| async move {
        let bucket = bucket(&t);
        let fixture = deploy_grpc_echo_template(&t, &bucket).await;
        let (_, actor_name, _) = create_and_resume_hive_actor(&t, "grpcingress", &fixture).await;
        let aref = actor_ref(&actor_name);
        let rc = must_router_client(&t).await;
        let mut config = rc
            .tls_config()
            .unwrap_or_else(|| t.fatal("router client has no TLS config"));
        config.alpn_protocols = vec![b"h2".to_vec()];
        let connector = RouterConnector {
            config: std::sync::Arc::new(config),
            addr: rc.address().to_string(),
        };
        let channel = Endpoint::from_shared(format!("http://{}", rc.address()))
            .unwrap_or_else(|e| t.fatal(format!("creating the gRPC client for {aref}: {e}")))
            .connect_with_connector(connector)
            .await
            .unwrap_or_else(|e| t.fatal(format!("creating the gRPC client for {aref}: {e}")));
        // Explicit metadata names the Actor on every call.
        let target: MetadataValue<_> = aref
            .to_string()
            .parse()
            .unwrap_or_else(|e| t.fatal(format!("target metadata: {e}")));
        let client = EchoClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
            req.metadata_mut()
                .insert(install::TARGET_ACTOR_HEADER, target.clone());
            Ok(req)
        });
        const MESSAGE: &str = "hello over grpc ingress";
        // An RPC, not a GET: the Actor serves only gRPC on port 80.
        wait_for_grpc_route_ready(&t, client.clone(), MESSAGE).await;
        let c = &client;

        t.run("unary", |t| async move {
            // An error here is itself the trailer assertion: a missing or
            // malformed status surfaces as an error.
            let resp = tokio::time::timeout(
                Duration::from_secs(30),
                c.clone().echo(EchoRequest {
                    message: MESSAGE.into(),
                }),
            )
            .await
            .unwrap_or_else(|_| t.fatal("unary Echo through ingress: deadline exceeded"))
            .unwrap_or_else(|e| t.fatal(format!("unary Echo through ingress: {e}")))
            .into_inner();
            if resp.message != MESSAGE {
                t.error(format!(
                    "unary Echo returned {:?}, want {MESSAGE:?}",
                    resp.message
                ));
            }
        })
        .await;

        t.run("server stream", |t| async move {
            const COUNT: usize = 3;
            let work = async {
                let mut stream = c
                    .clone()
                    .echo_stream(EchoStreamRequest {
                        message: MESSAGE.into(),
                        count: COUNT as i32,
                    })
                    .await
                    .unwrap_or_else(|e| t.fatal(format!("EchoStream through ingress: {e}")))
                    .into_inner();
                let mut got = Vec::new();
                while let Some(r) = stream.message().await.unwrap_or_else(|e| {
                    t.fatal(format!(
                        "EchoStream Recv after {} responses: {e}",
                        got.len()
                    ))
                }) {
                    got.push(r);
                }
                got
            };
            let got = tokio::time::timeout(Duration::from_secs(30), work)
                .await
                .unwrap_or_else(|_| t.fatal("EchoStream: deadline exceeded"));
            if got.len() != COUNT {
                t.fatal(format!(
                    "EchoStream returned {} responses, want {COUNT}",
                    got.len()
                ));
            }
            // Indexes separate an intact stream from a reordered or
            // deduplicated one.
            for (i, r) in got.iter().enumerate() {
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
        })
        .await;

        t.run("bidi stream", |t| async move {
            let work = async {
                let (tx, rx) = tokio::sync::mpsc::channel::<EchoRequest>(1);
                let mut responses = c
                    .clone()
                    .echo_bidi(tokio_stream::wrappers::ReceiverStream::new(rx))
                    .await
                    .unwrap_or_else(|e| t.fatal(format!("EchoBidi through ingress: {e}")))
                    .into_inner();
                // One message at a time, each waiting for its answer: a path
                // carrying one direction at a time would hang here.
                const COUNT: usize = 3;
                for i in 0..COUNT {
                    let want = format!("{MESSAGE}-{i}");
                    tx.send(EchoRequest {
                        message: want.clone(),
                    })
                    .await
                    .unwrap_or_else(|e| t.fatal(format!("EchoBidi Send {i}: {e}")));
                    let r = responses
                        .message()
                        .await
                        .unwrap_or_else(|e| t.fatal(format!("EchoBidi Recv {i}: {e}")))
                        .unwrap_or_else(|| t.fatal(format!("EchoBidi Recv {i}: stream ended")));
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
                // Half-close: the server must still end the call with OK.
                drop(tx);
                match responses.message().await {
                    Ok(None) => {}
                    other => t.error(format!(
                        "EchoBidi Recv after CloseSend = {other:?}, want the end of the stream"
                    )),
                }
            };
            tokio::time::timeout(Duration::from_secs(30), work)
                .await
                .unwrap_or_else(|_| t.fatal("EchoBidi: deadline exceeded"));
        })
        .await;
    });
}

/// Installs the gRPC Actor fixture (the suite's own copy) and returns it.
async fn deploy_grpc_echo_template(t: &T, bucket: &str) -> HiveFixture {
    let (hive, _) = deploy_hive_fixture(
        t,
        &clients(),
        GRPC_ECHO_FIXTURE,
        bucket,
        "networking",
        &[],
        FixtureOptions::default(),
    )
    .await;
    HiveFixture {
        pool_namespace: hive.clone(),
        hive,
        name: "grpcecho".into(),
        pool_name: "grpcecho".into(),
        deploy_with: "the networking suite itself (see deploy_grpc_echo_template)".into(),
    }
}

/// Retries a unary Echo (60 s) until it succeeds, riding out the window
/// before the route reaches the router, where calls come back Unavailable.
async fn wait_for_grpc_route_ready<F>(
    t: &T,
    client: EchoClient<InterceptedService<Channel, F>>,
    message: &str,
) where
    F: tonic::service::Interceptor + Clone,
{
    let timeout = Duration::from_secs(60);
    let deadline = Instant::now() + timeout;
    loop {
        let err = match tokio::time::timeout(
            Duration::from_secs(10),
            client.clone().echo(EchoRequest {
                message: message.into(),
            }),
        )
        .await
        {
            Ok(Ok(_)) => return,
            Ok(Err(e)) => e.to_string(),
            Err(_) => "deadline exceeded".to_string(),
        };
        if Instant::now() > deadline {
            t.fatal(format!(
                "gRPC through ingress did not become ready within {timeout:?}: {err}"
            ));
        }
        t.log(format!("gRPC through ingress failed: {err}; retrying..."));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

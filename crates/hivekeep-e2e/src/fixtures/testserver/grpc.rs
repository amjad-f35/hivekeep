//! The `grpc` subcommand: the cleartext HTTP/2 gRPC echo origin both
//! networking directions use.
//!
//! - Egress: deployed as a plain pod, it is the origin an Actor dials through
//!   the egress gateway.
//! - Ingress: deployed as an Actor, it is the origin a client reaches through
//!   the gate router.
//!
//! One server for both legs, so they cannot drift apart in what they consider
//! a working RPC. No TLS anywhere: the leg under test is the tunnel, not the
//! origin's identity, and the egress gateway relays a terminated CONNECT as
//! opaque TCP. Besides Echo it serves the gRPC health service, which the
//! egress pod's readiness probe checks; `--health-listen` adds a separate
//! HTTP/1.1 `/readyz` for the ingress Actor, whose wakeup probe can only be an
//! HTTP GET.

use std::pin::Pin;

use futures::Stream;
use http::{Request, StatusCode};
use hyper::body::Incoming;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Response, Status, Streaming};

use super::serve::{handler, text, Handler};
use crate::proto::grpcecho::echo_server::{Echo, EchoServer};
use crate::proto::grpcecho::{EchoRequest, EchoResponse, EchoStreamRequest};

/// Upper bound on `EchoStream`'s count. A test asks for a handful; a request
/// for millions is a bug in the caller, and answering it would look like a
/// hung tunnel.
pub const MAX_STREAM_COUNT: i32 = 1000;

type ResponseStream = Pin<Box<dyn Stream<Item = Result<EchoResponse, Status>> + Send>>;

/// The Echo service.
#[derive(Debug, Default, Clone, Copy)]
pub struct EchoService;

#[tonic::async_trait]
impl Echo for EchoService {
    async fn echo(
        &self,
        request: tonic::Request<EchoRequest>,
    ) -> Result<Response<EchoResponse>, Status> {
        Ok(Response::new(EchoResponse {
            message: request.into_inner().message,
            index: 0,
        }))
    }

    type EchoStreamStream = ResponseStream;

    async fn echo_stream(
        &self,
        request: tonic::Request<EchoStreamRequest>,
    ) -> Result<Response<Self::EchoStreamStream>, Status> {
        let req = request.into_inner();
        if req.count <= 0 {
            return Err(Status::invalid_argument(format!(
                "count must be positive, got {}",
                req.count
            )));
        }
        if req.count > MAX_STREAM_COUNT {
            return Err(Status::invalid_argument(format!(
                "count must be at most {MAX_STREAM_COUNT}, got {}",
                req.count
            )));
        }
        let message = req.message;
        let stream = tokio_stream::iter((0..req.count).map(move |index| {
            Ok(EchoResponse {
                message: message.clone(),
                index,
            })
        }));
        Ok(Response::new(Box::pin(stream)))
    }

    type EchoBidiStream = ResponseStream;

    /// Answers each request as it arrives rather than draining the request
    /// direction first: batching would deadlock a caller that waits for each
    /// response before sending the next, and would test nothing a server
    /// stream does not already cover.
    async fn echo_bidi(
        &self,
        request: tonic::Request<Streaming<EchoRequest>>,
    ) -> Result<Response<Self::EchoBidiStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut index = 0;
            // The end of the inbound stream is the client's half-close; the
            // response direction then ends with an OK status.
            while let Some(next) = inbound.next().await {
                let item = next.map(|req| EchoResponse {
                    message: req.message,
                    index,
                });
                let failed = item.is_err();
                if tx.send(item).await.is_err() || failed {
                    return;
                }
                index += 1;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// The server with everything the pod serves registered on it, so the unit
/// tests exercise the same registrations.
pub fn new_server() -> tonic::transport::server::Router {
    let (_reporter, health) = tonic_health::server::health_reporter();
    tonic::transport::Server::builder()
        .add_service(EchoServer::new(EchoService))
        .add_service(health)
}

/// Answers the HTTP readiness probe. Deliberately trivial: the gRPC listener
/// is bound before this one, so a 200 here means the RPC port is accepting.
pub fn health_handler() -> Handler {
    handler(|req: Request<Incoming>| async move {
        if req.uri().path() == "/readyz" {
            text(StatusCode::OK, "ok")
        } else {
            text(StatusCode::NOT_FOUND, "404 page not found\n")
        }
    })
}

/// Runs the `grpc` subcommand.
pub async fn run(listen: &str, health_listen: &str) -> anyhow::Result<()> {
    use super::serve::{bind_address, serve_listeners};
    let listener = tokio::net::TcpListener::bind(bind_address(listen))
        .await
        .map_err(|e| anyhow::anyhow!("listening on {listen}: {e}"))?;
    eprintln!("testserver grpc: serving on {}", listener.local_addr()?);
    // Bound before the gRPC server starts serving, so a readiness 200 can
    // never precede the gRPC port being open.
    if !health_listen.is_empty() {
        let health = tokio::net::TcpListener::bind(bind_address(health_listen))
            .await
            .map_err(|e| anyhow::anyhow!("listening for health on {health_listen}: {e}"))?;
        eprintln!(
            "testserver grpc: serving /readyz on {}",
            health.local_addr()?
        );
        tokio::spawn(async move {
            if let Err(e) = serve_listeners(health_handler(), health, None).await {
                eprintln!("testserver grpc: health listener: {e}");
                std::process::exit(1);
            }
        });
    }
    new_server()
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tonic::transport::Channel;
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::{health_check_response::ServingStatus, HealthCheckRequest};

    use super::super::serve::testing::start_origin_server;
    use super::*;
    use crate::proto::grpcecho::echo_client::EchoClient;

    /// Starts the server on a real loopback listener, so the registrations
    /// and HTTP/2 framing are the pod's.
    async fn dial_local() -> (Channel, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let _ = new_server()
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
        let channel = Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .timeout(Duration::from_secs(10))
            .connect()
            .await
            .unwrap();
        (channel, task)
    }

    #[tokio::test]
    async fn echo() {
        let (channel, task) = dial_local().await;
        let mut client = EchoClient::new(channel);
        for message in ["hello", "", "unicode: é世界", "with spaces and\nnewline"] {
            let response = client
                .echo(EchoRequest {
                    message: message.into(),
                })
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.message, message, "Echo({message:?})");
        }
        task.abort();
    }

    #[tokio::test]
    async fn echo_stream() {
        let (channel, task) = dial_local().await;
        let mut client = EchoClient::new(channel);
        let mut stream = client
            .echo_stream(EchoStreamRequest {
                message: "streamed".into(),
                count: 3,
            })
            .await
            .unwrap()
            .into_inner();
        let mut got = Vec::new();
        while let Some(response) = stream.message().await.unwrap() {
            got.push(response);
        }
        assert_eq!(got.len(), 3);
        // The indexes tell a reordered or deduplicated stream from an intact one.
        for (i, response) in got.iter().enumerate() {
            assert_eq!(response.message, "streamed");
            assert_eq!(response.index as usize, i);
        }
        task.abort();
    }

    /// Sends one message at a time and blocks on its response before sending
    /// the next, which only a server that answers as requests arrive passes.
    #[tokio::test]
    async fn echo_bidi() {
        let (channel, task) = dial_local().await;
        let mut client = EchoClient::new(channel);
        let (tx, rx) = mpsc::channel(1);
        let mut responses = client
            .echo_bidi(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        for i in 0..3 {
            let message = format!("message-{i}");
            tx.send(EchoRequest {
                message: message.clone(),
            })
            .await
            .unwrap();
            let response = responses.message().await.unwrap().expect("response");
            assert_eq!(response.message, message);
            assert_eq!(response.index, i);
        }
        // Half-close: the server must end with OK, not an error.
        drop(tx);
        assert!(
            responses.message().await.unwrap().is_none(),
            "want a clean end of stream"
        );
        task.abort();
    }

    /// A non-positive or oversized count fails loudly; an empty stream would
    /// make a caller that forgot to set count look like a working one.
    #[tokio::test]
    async fn echo_stream_rejects_bad_count() {
        let (channel, task) = dial_local().await;
        let mut client = EchoClient::new(channel);
        for count in [0, -1, MAX_STREAM_COUNT + 1] {
            let result = client
                .echo_stream(EchoStreamRequest {
                    message: "x".into(),
                    count,
                })
                .await;
            let code = match result {
                Err(status) => status.code(),
                Ok(stream) => match stream.into_inner().message().await {
                    Err(status) => status.code(),
                    Ok(_) => tonic::Code::Ok,
                },
            };
            assert_eq!(
                code,
                tonic::Code::InvalidArgument,
                "EchoStream(count={count})"
            );
        }
        task.abort();
    }

    /// The pod's readiness probe checks the health service's empty service
    /// name; nothing else would notice that registration disappearing.
    #[tokio::test]
    async fn health_service_is_serving() {
        let (channel, task) = dial_local().await;
        let response = HealthClient::new(channel)
            .check(HealthCheckRequest {
                service: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.status, ServingStatus::Serving as i32);
        task.abort();
    }

    /// The ingress Actor's wakeup probe is an HTTP GET, so this is what gets
    /// it ready; a 404 from a mistyped path would look like a template that
    /// never boots.
    #[tokio::test]
    async fn readyz_answers_http_get() {
        let server = start_origin_server(health_handler(), None).await;
        let host = server.http_url.trim_start_matches("http://").to_string();
        let stream = tokio::net::TcpStream::connect(&host).await.unwrap();
        let (mut sender, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .unwrap();
        tokio::spawn(conn);
        let req = http::Request::get("/readyz")
            .header("host", host)
            .body(http_body_util::Full::<bytes::Bytes>::default())
            .unwrap();
        assert_eq!(
            sender.send_request(req).await.unwrap().status(),
            StatusCode::OK
        );
    }
}

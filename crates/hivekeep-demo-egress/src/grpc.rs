//! `/grpc`: calls the Echo service at a target and reports what came back.
//!
//! An e2e uses this to show gRPC survives the egress path: HTTP/2 framing
//! end to end, streams in both directions, and a status delivered in
//! trailers.

use std::future::Future;

use tokio::sync::mpsc;
use tokio::time::{timeout_at, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::address::validate_target;
use crate::fetch::error_chain;
use crate::grpc_code;
use crate::limits::REQUEST_TIMEOUT;
use crate::proto::echo_client::EchoClient;
use crate::proto::{EchoRequest, EchoStreamRequest};
use crate::wire::{GrpcRequest, GrpcResponse, StreamedMessage};

/// Runs `fut` until `deadline`, turning a timeout into DeadlineExceeded.
async fn by<T>(
    deadline: Instant,
    fut: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    timeout_at(deadline, fut)
        .await
        .unwrap_or_else(|_| Err(Status::deadline_exceeded("context deadline exceeded")))
}

/// Renders a status the way gRPC clients conventionally print one.
fn describe(status: &Status) -> String {
    format!(
        "rpc error: code = {} desc = {}",
        grpc_code::name(status.code()),
        status.message()
    )
}

/// The report for a failed RPC. The HTTP status is 502 so a caller polling
/// through the ingress keeps retrying while the origin comes up; the gRPC
/// code rides in the body where an HTTP status cannot flatten it.
fn failure(rpc: &str, status: &Status) -> (u16, GrpcResponse) {
    (
        502,
        GrpcResponse {
            code: grpc_code::name(status.code()).to_string(),
            error: format!("{rpc} failed: {}", describe(status)),
            ..Default::default()
        },
    )
}

/// Runs the instruction and returns the HTTP status and report.
pub async fn run(input: GrpcRequest) -> (u16, GrpcResponse) {
    if let Err(e) = validate_target(&input.target, "target") {
        return (400, GrpcResponse::error(e));
    }
    let deadline = Instant::now() + REQUEST_TIMEOUT;
    let endpoint = match Endpoint::from_shared(format!("http://{}", input.target)) {
        Ok(e) => e,
        Err(e) => {
            return (
                400,
                GrpcResponse::error(format!("dialing {}: {e}", input.target)),
            )
        }
    };
    // A fresh connection per instruction: the Actor is checkpointed and
    // restored, and a connection from before a snapshot is dead on resume.
    let channel = match by(deadline, async {
        endpoint
            .connect()
            .await
            .map_err(|e| Status::new(Code::Unavailable, error_chain(&e)))
    })
    .await
    {
        Ok(c) => c,
        Err(status) => return failure("Echo", &status),
    };
    let mut client = EchoClient::new(channel);

    let echoed = match by(
        deadline,
        client.echo(EchoRequest {
            message: input.message.clone(),
        }),
    )
    .await
    {
        Ok(r) => r.into_inner(),
        Err(status) => return failure("Echo", &status),
    };
    let mut response = GrpcResponse {
        message: echoed.message,
        code: grpc_code::name(Code::Ok).to_string(),
        ..Default::default()
    };
    if input.stream_count > 0 {
        match echo_stream(&mut client, &input, deadline).await {
            Ok(s) => response.stream = Some(s),
            Err(status) => return failure("EchoStream", &status),
        }
    }
    if input.bidi_count > 0 {
        match echo_bidi(&mut client, &input, deadline).await {
            Ok(s) => response.bidi = Some(s),
            Err(status) => return failure("EchoBidi", &status),
        }
    }
    (200, response)
}

/// Drains a server stream.
async fn echo_stream(
    client: &mut EchoClient<Channel>,
    input: &GrpcRequest,
    deadline: Instant,
) -> Result<Vec<StreamedMessage>, Status> {
    let request = EchoStreamRequest {
        message: input.message.clone(),
        count: input.stream_count,
    };
    let mut stream = by(deadline, client.echo_stream(request))
        .await?
        .into_inner();
    let mut out = Vec::new();
    while let Some(received) = by(deadline, stream.message()).await? {
        out.push(StreamedMessage {
            message: received.message,
            index: received.index,
        });
    }
    Ok(out)
}

/// Runs a bidirectional stream one message at a time, each awaiting its
/// answer before the next is sent — sending everything up front would pass
/// over a path that only carries one direction at a time, which is exactly
/// what this must catch. Then half-closes and expects the server to finish.
async fn echo_bidi(
    client: &mut EchoClient<Channel>,
    input: &GrpcRequest,
    deadline: Instant,
) -> Result<Vec<StreamedMessage>, Status> {
    // Distinct text per message, so a replayed or stale frame shows up as a
    // mismatch rather than a pass.
    let message_at = |i: i32| EchoRequest {
        message: format!("{}-{i}", input.message),
    };
    let (tx, rx) = mpsc::channel::<EchoRequest>(1);
    // The first message is queued before the call so a server that sends
    // its headers only with its first response cannot deadlock the opening.
    tx.send(message_at(0))
        .await
        .map_err(|_| Status::internal("request stream closed"))?;
    let mut responses = by(deadline, client.echo_bidi(ReceiverStream::new(rx)))
        .await?
        .into_inner();

    let mut out = Vec::with_capacity(input.bidi_count.max(0) as usize);
    for i in 0..input.bidi_count {
        if i > 0 && tx.send(message_at(i)).await.is_err() {
            // The request side is gone; the real reason is on the response side.
            by(deadline, responses.message()).await?;
            return Err(Status::unavailable("request stream closed"));
        }
        let received = by(deadline, responses.message()).await?.ok_or_else(|| {
            Status::unavailable(format!(
                "stream ended after {i} of {} responses",
                input.bidi_count
            ))
        })?;
        out.push(StreamedMessage {
            message: received.message,
            index: received.index,
        });
    }

    // Half-close the request direction while responses may still flow, then
    // drain. A path that treats a one-sided END_STREAM as teardown fails here.
    drop(tx);
    match by(deadline, responses.message()).await? {
        None => Ok(out),
        Some(_) => Err(Status::unknown(format!(
            "server sent more responses than the {} messages requested",
            input.bidi_count
        ))),
    }
}

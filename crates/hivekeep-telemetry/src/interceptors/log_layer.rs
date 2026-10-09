//! A tower layer giving every gRPC server the per-call log line and
//! elapsed-time metadata of [`super::unary`] without wrapping each handler.
//!
//! [`GrpcLogLayer`] sees the HTTP/2 request and response. For a unary
//! request whose method is in the descriptor pool it reads the request
//! frame and logs it with `debug_redact` fields masked; streaming requests
//! pass through untouched and log only their method. The call is logged once
//! its response ends, with the gRPC status from the trailers (or from the
//! headers of a trailers-only error) and the time to the response headers,
//! which for a unary call is the handler's duration. That duration is also
//! sent as [`SERVER_ELAPSED_TRAILER`] response metadata.
//!
//! Status mapping stays with the handlers: by the time a response reaches
//! the layer it is already a gRPC status.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes};
use http::{HeaderMap, HeaderValue};
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use tower::{Layer, Service};

use super::unary::SERVER_ELAPSED_TRAILER;
use crate::contextlogging::ProtoValue;
use crate::protoredact;

/// Which server the layer logs for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogMode {
    /// The public API: status messages are logged as returned.
    Public,
    /// An internal service; the same line, tagged `internal`.
    Internal,
}

/// The layer; see the module docs.
#[derive(Debug, Clone, Copy)]
pub struct GrpcLogLayer {
    mode: LogMode,
}

impl GrpcLogLayer {
    /// For the public API.
    pub fn public() -> Self {
        GrpcLogLayer {
            mode: LogMode::Public,
        }
    }

    /// For an internal service.
    pub fn internal() -> Self {
        GrpcLogLayer {
            mode: LogMode::Internal,
        }
    }
}

impl<S> Layer<S> for GrpcLogLayer {
    type Service = GrpcLog<S>;
    fn layer(&self, inner: S) -> Self::Service {
        GrpcLog {
            inner,
            mode: self.mode,
        }
    }
}

/// The service [`GrpcLogLayer`] builds.
#[derive(Debug, Clone)]
pub struct GrpcLog<S> {
    inner: S,
    mode: LogMode,
}

/// The request of a unary call, decoded for the log against `desc`. A frame
/// that does not decode is logged by size.
fn describe_request(desc: &MessageDescriptor, body: &[u8]) -> ProtoValue {
    let mut b = body;
    if b.len() < 5 {
        return ProtoValue::text(format!("<{} bytes>", body.len()));
    }
    let compressed = b[0] == 1;
    let len = u32::from_be_bytes([b[1], b[2], b[3], b[4]]) as usize;
    b.advance(5);
    if compressed || b.len() < len {
        return ProtoValue::text(format!("<{len} bytes, not decoded>"));
    }
    match DynamicMessage::decode(desc.clone(), &b[..len]) {
        Ok(msg) => ProtoValue::of(desc, &msg),
        Err(_) => ProtoValue::text(format!("<{len} bytes, undecodable>")),
    }
}

/// What the layer knows about a call when its response ends.
struct Call {
    method: String,
    mode: LogMode,
    req: ProtoValue,
    elapsed: Duration,
    header_status: Option<(String, String)>,
}

fn status_of(h: &HeaderMap) -> Option<(String, String)> {
    let code = h.get("grpc-status")?.to_str().ok()?.to_string();
    let msg = h
        .get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .map(|m| {
            percent_encoding::percent_decode_str(m)
                .decode_utf8_lossy()
                .into_owned()
        })
        .unwrap_or_default();
    Some((code, msg))
}

impl Call {
    fn log(self, trailers: Option<&HeaderMap>) {
        let (code, message) = trailers
            .and_then(status_of)
            .or(self.header_status)
            .unwrap_or_else(|| ("0".into(), String::new()));
        let code_name = tonic::Code::from_i32(code.parse().unwrap_or(2));
        let internal = self.mode == LogMode::Internal;
        if code_name == tonic::Code::Ok {
            tracing::info!(
                method = %self.method,
                req = %self.req,
                code = ?code_name,
                elapsed_time = ?self.elapsed,
                internal,
                "Handle RPC"
            );
        } else {
            tracing::info!(
                method = %self.method,
                req = %self.req,
                code = ?code_name,
                err = %message,
                elapsed_time = ?self.elapsed,
                internal,
                "Handle RPC"
            );
        }
    }
}

/// The response body, logging the call when it ends.
pub struct LoggedBody<B> {
    inner: Pin<Box<B>>,
    call: Option<Call>,
}

impl<B> Drop for LoggedBody<B> {
    fn drop(&mut self) {
        // A response dropped before its end (the caller went away) is still
        // one call.
        if let Some(call) = self.call.take() {
            call.log(None);
        }
    }
}

impl<B> Body for LoggedBody<B>
where
    B: Body<Data = Bytes>,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, B::Error>>> {
        let polled = self.inner.as_mut().poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(t) = frame.trailers_ref() {
                    if let Some(call) = self.call.take() {
                        call.log(Some(t));
                    }
                }
            }
            Poll::Ready(None) => {
                if let Some(call) = self.call.take() {
                    call.log(None);
                }
            }
            _ => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl<S, ResBody> Service<http::Request<tonic::body::Body>> for GrpcLog<S>
where
    S: Service<http::Request<tonic::body::Body>, Response = http::Response<ResBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ResBody: Body<Data = Bytes> + Send + 'static,
    ResBody::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let mode = self.mode;
        let method = req.uri().path().to_string();
        let unary_input = protoredact::method_descriptor(&method)
            .filter(|m| !m.is_client_streaming())
            .map(|m| m.input());
        Box::pin(async move {
            let (req_log, req) = match unary_input {
                Some(desc) => {
                    let (parts, body) = req.into_parts();
                    match body.collect().await {
                        Ok(c) => {
                            let bytes = c.to_bytes();
                            let log = describe_request(&desc, &bytes);
                            let body = tonic::body::Body::new(http_body_util::Full::new(bytes));
                            (log, http::Request::from_parts(parts, body))
                        }
                        Err(e) => {
                            let status = tonic::Status::internal(format!("read request: {e}"));
                            Call {
                                method,
                                mode,
                                req: ProtoValue::none(),
                                elapsed: Duration::ZERO,
                                header_status: Some((
                                    i32::from(status.code()).to_string(),
                                    status.message().to_string(),
                                )),
                            }
                            .log(None);
                            let (parts, body) =
                                status.into_http::<tonic::body::Body>().into_parts();
                            return Ok(http::Response::from_parts(parts, body));
                        }
                    }
                }
                None => (ProtoValue::text("<stream>"), req),
            };
            let start = Instant::now();
            let resp = inner.call(req).await?;
            let elapsed = start.elapsed();
            let (mut parts, body) = resp.into_parts();
            if let Ok(v) = HeaderValue::from_str(&elapsed.as_micros().to_string()) {
                parts.headers.insert(SERVER_ELAPSED_TRAILER, v);
            }
            let call = Call {
                method,
                mode,
                req: req_log,
                elapsed,
                header_status: status_of(&parts.headers),
            };
            let body = LoggedBody {
                inner: Box::pin(body),
                call: Some(call),
            };
            Ok(http::Response::from_parts(
                parts,
                tonic::body::Body::new(body),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contextlogging::tests::capture;
    use hivekeep_proto::{api::v1 as api, keeper::v1 as keeper};
    use std::convert::Infallible;

    fn frame(msg: &impl prost::Message) -> Bytes {
        let payload = msg.encode_to_vec();
        let mut b = vec![0u8];
        b.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        b.extend_from_slice(&payload);
        Bytes::from(b)
    }

    /// A service answering every call with `status` in the trailers (OK) or
    /// a trailers-only error.
    #[derive(Clone)]
    struct Fixed(tonic::Code);

    impl Service<http::Request<tonic::body::Body>> for Fixed {
        type Response = http::Response<tonic::body::Body>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: http::Request<tonic::body::Body>) -> Self::Future {
            let code = self.0;
            Box::pin(async move {
                if code != tonic::Code::Ok {
                    return Ok(tonic::Status::new(code, "nope").into_http());
                }
                let mut trailers = HeaderMap::new();
                trailers.insert("grpc-status", HeaderValue::from_static("0"));
                let frames = tokio_stream::iter([
                    Ok::<_, Infallible>(Frame::data(frame(&api::ObjectRef::default()))),
                    Ok(Frame::trailers(trailers)),
                ]);
                Ok(http::Response::new(tonic::body::Body::new(
                    http_body_util::StreamBody::new(frames),
                )))
            })
        }
    }

    fn request(path: &str, body: Bytes) -> http::Request<tonic::body::Body> {
        http::Request::builder()
            .uri(format!("http://x{path}"))
            .body(tonic::body::Body::new(http_body_util::Full::new(body)))
            .unwrap()
    }

    fn run(code: tonic::Code, path: &str, body: Bytes) -> (http::HeaderMap, String) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut headers = None;
        let log = capture(|| {
            rt.block_on(async {
                let mut svc = GrpcLogLayer::internal().layer(Fixed(code));
                let resp = svc.call(request(path, body)).await.unwrap();
                headers = Some(resp.headers().clone());
                let _ = resp.into_body().collect().await;
            })
        });
        (headers.unwrap(), log)
    }

    #[test]
    fn logs_unary_calls_with_redacted_requests_and_elapsed_metadata() {
        let req = keeper::RunRequest {
            spec: Some(keeper::WorkloadSpec {
                containers: vec![keeper::Container {
                    name: "main".into(),
                    env: vec![keeper::EnvVar {
                        name: "API_KEY".into(),
                        value: "sk-secret".into(),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let (headers, log) = run(
            tonic::Code::Ok,
            "/hivekeep.keeper.v1.Keeper/Run",
            frame(&req),
        );
        assert!(headers.contains_key(SERVER_ELAPSED_TRAILER), "{headers:?}");
        let line = log.lines().find(|l| l.contains("Handle RPC")).expect(&log);
        assert!(
            line.contains(r#""method":"/hivekeep.keeper.v1.Keeper/Run""#),
            "{line}"
        );
        assert!(line.contains(r#""code":"Ok""#), "{line}");
        assert!(line.contains("main"), "the request is logged: {line}");
        assert!(!line.contains("sk-secret"), "{line}");
        assert_eq!(log.matches("Handle RPC").count(), 1, "{log}");
    }

    #[test]
    fn streams_and_unknown_methods_pass_through() {
        let (headers, log) = run(
            tonic::Code::Ok,
            "/unknown.Service/Method",
            frame(&api::ObjectRef::default()),
        );
        assert!(headers.contains_key(SERVER_ELAPSED_TRAILER));
        assert!(log.contains("<stream>"), "{log}");
    }

    #[test]
    fn logs_trailers_only_errors() {
        let (_, log) = run(
            tonic::Code::NotFound,
            "/unknown.Service/Method",
            Bytes::new(),
        );
        let line = log.lines().find(|l| l.contains("Handle RPC")).expect(&log);
        assert!(
            line.contains(r#""code":"NotFound""#) && line.contains(r#""err":"nope""#),
            "{line}"
        );
    }

    #[test]
    fn describe_request_masks_debug_redact_fields() {
        let desc = protoredact::descriptor_pool()
            .get_message_by_name("hivekeep.api.v1.MintActorJwtResponse")
            .expect("message in the pool");
        let req = api::MintActorJwtResponse {
            actor_jwt: "secret".into(),
            ..Default::default()
        };
        let got = describe_request(&desc, &frame(&req)).to_string();
        assert!(
            !got.contains("secret") || got.contains("[REDACTED]"),
            "{got}"
        );
        assert!(got.contains("[REDACTED]"), "{got}");
        assert!(describe_request(&desc, b"\x00\x00")
            .to_string()
            .contains("bytes"));
    }
}

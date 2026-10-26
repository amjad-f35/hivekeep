//! Call plumbing the Actor-driving classes share: where a request's
//! outcome is recorded, a traced Control API call, the Hive bootstrap, and
//! protobuf-over-HTTP requests to an Actor through the gate router.

use std::future::Future;
use std::time::{Duration, Instant};

use hivekeep_proto::api::v1 as api;
use tonic::{Code, Request, Response, Status};

use crate::loadgen::metrics;
use crate::loadgen::telemetry::{self, elapsed_from_metadata, SOURCE_CLIENT, SOURCE_SERVER};
use crate::loadgen::trace::{inject_headers, inject_metadata, SpanContext};
use crate::loadgen::userclass::Config;
use crate::wire::{SERVER_ELAPSED_HEADER, TARGET_ACTOR_HEADER};

/// Where a class books each request's outcome.
pub trait Recorder: Send + Sync {
    /// A successful request.
    fn success(&self, method: &str, name: &str, latency: Duration, bytes: i64);
    /// A failed request.
    fn failure(&self, method: &str, name: &str, latency: Duration, err: &str);
}

/// Books outcomes under one locust user class, to the master and to
/// Prometheus.
pub struct ClassRecorder(pub String);

impl Recorder for ClassRecorder {
    fn success(&self, method: &str, name: &str, latency: Duration, bytes: i64) {
        metrics::record_success(method, name, &self.0, latency, bytes);
    }
    fn failure(&self, method: &str, name: &str, latency: Duration, err: &str) {
        metrics::record_failure(method, name, &self.0, latency, err);
    }
}

/// A status as an error line: code and message.
pub fn status_text(s: &Status) -> String {
    format!("rpc error: code = {:?} desc = {}", s.code(), s.message())
}

/// A request carrying the span's trace context.
pub fn traced_request<T>(msg: T, span: &SpanContext) -> Request<T> {
    let mut req = Request::new(msg);
    inject_metadata(span, req.metadata_mut());
    req
}

/// Runs one Control API call under a span and books it under `name`. The
/// booked latency is the client's wall clock, so it covers any retries
/// inside `call`; the server-elapsed trailer, when present, only goes on
/// the span.
pub async fn traced_call<T, F, Fut>(
    cfg: &Config,
    rec: &dyn Recorder,
    name: &str,
    call: F,
) -> Result<T, Status>
where
    F: FnOnce(SpanContext) -> Fut,
    Fut: Future<Output = Result<Response<T>, Status>>,
{
    let mut span = cfg.tracer.start(name);
    let ctx = *span.context();
    let start = Instant::now();
    let res = call(ctx).await;
    let latency = start.elapsed();
    match res {
        Ok(resp) => {
            let (server, source) =
                elapsed_from_metadata(resp.metadata(), SERVER_ELAPSED_HEADER, Duration::ZERO);
            if source == SOURCE_SERVER {
                span.set_attribute("server.elapsed_ms", telemetry::ms_float(server));
            }
            telemetry::log_sampled_trace(&ctx, name, latency, SOURCE_CLIENT, None);
            rec.success("grpc", name, latency, 0);
            span.end();
            Ok(resp.into_inner())
        }
        Err(e) => {
            let text = status_text(&e);
            telemetry::log_sampled_trace(&ctx, name, latency, SOURCE_CLIENT, Some(&text));
            rec.failure("grpc", name, latency, &text);
            span.record_error(&text);
            span.end();
            Err(e)
        }
    }
}

/// An Actor reference in the configured Hive.
pub fn actor_ref(cfg: &Config, name: &str) -> api::ObjectRef {
    api::ObjectRef {
        hive: cfg.hive.clone(),
        name: name.to_string(),
    }
}

/// A CreateActor request for `name` from the template `hive/template`.
pub fn create_request(
    cfg: &Config,
    name: &str,
    template_hive: &str,
    template: &str,
) -> api::CreateActorRequest {
    api::CreateActorRequest {
        actor: Some(api::Actor {
            metadata: Some(api::ResourceMetadata {
                hive: cfg.hive.clone(),
                name: name.to_string(),
                ..Default::default()
            }),
            actor_template: Some(api::ObjectRef {
                hive: template_hive.to_string(),
                name: template.to_string(),
            }),
            ..Default::default()
        }),
    }
}

/// A SuspendActor request in the given durability mode.
pub fn suspend_request(
    cfg: &Config,
    name: &str,
    mode: api::DurabilityMode,
) -> api::SuspendActorRequest {
    api::SuspendActorRequest {
        actor: Some(actor_ref(cfg, name)),
        options: Some(api::SuspendOptions { mode: mode as i32 }),
    }
}

/// Creates the configured Hive, treating AlreadyExists as success so the
/// users racing the first creation all see it done.
pub async fn ensure_hive(cfg: &Config, rec: &dyn Recorder) -> Result<(), Status> {
    let req = api::CreateHiveRequest {
        hive: Some(api::Hive {
            metadata: Some(api::ResourceMetadata {
                name: cfg.hive.clone(),
                ..Default::default()
            }),
        }),
    };
    let res = traced_call(cfg, rec, "CreateHive", |span| async move {
        match cfg.api.create_hive(traced_request(req, &span)).await {
            Err(e) if e.code() == Code::AlreadyExists => Ok(Response::new(api::Hive::default())),
            other => other,
        }
    })
    .await;
    res.map(|_| ())
}

/// An Actor's answer through the router.
pub struct Reply {
    /// HTTP status.
    pub status: reqwest::StatusCode,
    /// Response headers.
    pub headers: http::HeaderMap,
    /// Response body.
    pub body: bytes::Bytes,
}

/// POSTs a protobuf body to `path` on Actor `actor` through the gate router,
/// with the span's trace context.
pub async fn post(
    cfg: &Config,
    actor: &str,
    path: &str,
    body: Vec<u8>,
    span: &SpanContext,
) -> Result<Reply, String> {
    let mut headers = http::HeaderMap::new();
    inject_headers(span, &mut headers);
    let target = format!("{}/{actor}", cfg.hive);
    let resp = cfg
        .http
        .post(format!("{}{path}", cfg.router_url))
        .headers(headers)
        .header(http::header::CONTENT_TYPE, "application/x-protobuf")
        .header(TARGET_ACTOR_HEADER, target)
        .body(body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp.bytes().await.map_err(|e| e.to_string())?;
    Ok(Reply {
        status,
        headers,
        body,
    })
}

/// [`post`] that decodes a protobuf answer, treating any 4xx/5xx as an
/// error naming the path and status.
pub async fn post_proto<Req: prost::Message, Resp: prost::Message + Default>(
    cfg: &Config,
    actor: &str,
    path: &str,
    req: &Req,
    span: &SpanContext,
) -> Result<Resp, String> {
    let reply = post(cfg, actor, path, req.encode_to_vec(), span).await?;
    if reply.status.as_u16() >= 400 {
        return Err(format!(
            "{path}: HTTP {}: {}",
            reply.status.as_u16(),
            String::from_utf8_lossy(&reply.body).trim()
        ));
    }
    Resp::decode(reply.body.as_ref()).map_err(|e| e.to_string())
}

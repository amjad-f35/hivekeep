//! The Envoy external processor: answers each request's headers by
//! dispatching to the ingress or egress handler.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_server::{
    ExternalProcessor, ExternalProcessorServer,
};
use envoy_types::pb::envoy::service::ext_proc::v3::{
    processing_request, processing_response, CommonResponse, HeadersResponse, HttpHeaders,
    ProcessingRequest, ProcessingResponse,
};
use envoy_types::pb::google::protobuf::Struct;
use prometheus::HistogramVec;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use super::dispatch::{direction_of, Direction};
use super::error::{immediate_response, RouteError};
use super::metadata::RequestMetadata;
use super::metrics::{classify_outcome, record_route_duration};
use super::record::{QueryRecorder, RecordedQuery};
use crate::attr;

/// What a handler decided for a request.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HandlerResult {
    /// The headers response for Envoy.
    pub response: HeadersResponse,
    /// Where the request goes (for the status page).
    pub target: String,
    /// The Actor template's hive (metrics).
    pub template_hive: String,
    /// The Actor template's name (metrics).
    pub template_name: String,
    /// How the Actor was resumed ([`attr::resume`]); empty means unknown.
    pub resume: String,
    /// Dynamic metadata for later filters.
    pub dynamic_metadata: Option<Struct>,
}

impl HandlerResult {
    /// The resume label, defaulting to unknown.
    pub fn resume(&self) -> &str {
        if self.resume.is_empty() {
            attr::resume::UNKNOWN
        } else {
            &self.resume
        }
    }

    /// A pass-through result.
    pub fn continue_request() -> Self {
        HandlerResult {
            response: HeadersResponse {
                response: Some(CommonResponse::default()),
            },
            ..Default::default()
        }
    }
}

/// Routes the requests of one direction.
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    /// The direction served.
    fn direction(&self) -> Direction;
    /// Decides one request from its headers.
    async fn handle_request_headers(
        &self,
        md: &mut RequestMetadata,
    ) -> Result<HandlerResult, RouteError>;
}

/// Handlers by direction.
pub type Handlers = HashMap<Direction, Arc<dyn Handler>>;

/// Queries kept for the status page.
const RECORDED_QUERIES: usize = 100;

/// The external processor.
pub struct Server {
    handlers: Handlers,
    recorder: Arc<QueryRecorder>,
    route_duration: Option<HistogramVec>,
}

impl Server {
    /// A server dispatching to `handlers`, recording route durations in
    /// `route_duration` when given.
    pub fn new(route_duration: Option<HistogramVec>, handlers: Handlers) -> Self {
        Server {
            handlers,
            recorder: Arc::new(QueryRecorder::new(RECORDED_QUERIES)),
            route_duration,
        }
    }

    /// The recent routing decisions, newest first.
    pub fn queries(&self) -> Vec<RecordedQuery> {
        self.recorder.get()
    }

    /// The recorder (shared with the status page).
    pub fn recorder(&self) -> Arc<QueryRecorder> {
        self.recorder.clone()
    }

    /// The gRPC service.
    pub fn into_service(self: Arc<Self>) -> ExternalProcessorServer<SharedServer> {
        ExternalProcessorServer::new(SharedServer(self))
    }

    /// Answers one request-headers message.
    pub async fn process_request_headers(
        &self,
        req: &ProcessingRequest,
        headers: &HttpHeaders,
    ) -> ProcessingResponse {
        let start = Instant::now();
        let started = chrono::Utc::now();
        let hdrs = headers
            .headers
            .as_ref()
            .map(|h| h.headers.as_slice())
            .unwrap_or_default();
        let mut md = RequestMetadata::new(hdrs, req.attributes.clone());
        let dir = direction_of(req);
        let res = match self.handlers.get(&dir) {
            Some(h) => h.handle_request_headers(&mut md).await,
            None => Err(RouteError::req(
                404,
                format!("this router does not serve {dir} traffic"),
            )),
        };
        let elapsed = start.elapsed();
        self.record(elapsed, &res);
        match res {
            Err(err) => {
                tracing::error!(direction = %dir, %err, cause = ?err.cause, "error during ext_proc RequestHeaders processing");
                self.recorder
                    .add_router_request(started, elapsed, "Error", "-", &md);
                immediate_response(err.http_status(), &err.message)
            }
            Ok(r) => {
                self.recorder
                    .add_router_request(started, elapsed, "Route ok", &r.target, &md);
                ProcessingResponse {
                    response: Some(processing_response::Response::RequestHeaders(r.response)),
                    dynamic_metadata: r.dynamic_metadata,
                    ..Default::default()
                }
            }
        }
    }

    fn record(&self, elapsed: Duration, res: &Result<HandlerResult, RouteError>) {
        let Some(h) = &self.route_duration else {
            return;
        };
        let (hive, name, resume) = match res {
            Ok(r) => (
                r.template_hive.as_str(),
                r.template_name.as_str(),
                r.resume(),
            ),
            Err(_) => ("", "", attr::resume::UNKNOWN),
        };
        record_route_duration(
            h,
            elapsed,
            hive,
            name,
            classify_outcome(res.as_ref().err()),
            resume,
        );
    }

    async fn process_one(&self, req: ProcessingRequest) -> ProcessingResponse {
        match &req.request {
            Some(processing_request::Request::RequestHeaders(h)) => {
                self.process_request_headers(&req, h).await
            }
            other => {
                tracing::error!(request = ?other.as_ref().map(std::mem::discriminant), "unexpected ext_proc request type");
                ProcessingResponse {
                    response: Some(processing_response::Response::RequestHeaders(
                        HeadersResponse {
                            response: Some(CommonResponse::default()),
                        },
                    )),
                    ..Default::default()
                }
            }
        }
    }
}

/// A handle to a shared server, so the service can be cloned into tonic.
pub struct SharedServer(pub Arc<Server>);

#[async_trait]
impl ExternalProcessor for SharedServer {
    type ProcessStream = ReceiverStream<Result<ProcessingResponse, tonic::Status>>;

    async fn process(
        &self,
        request: tonic::Request<tonic::Streaming<ProcessingRequest>>,
    ) -> Result<tonic::Response<Self::ProcessStream>, tonic::Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let server = self.0.clone();
        tokio::spawn(async move {
            loop {
                let next = tokio::select! {
                    n = stream.next() => n,
                    _ = tx.closed() => return,
                };
                let req = match next {
                    Some(Ok(r)) => r,
                    Some(Err(s)) => {
                        let _ = tx.send(Err(s)).await;
                        return;
                    }
                    None => return,
                };
                let resp = tokio::select! {
                    r = server.process_one(req) => r,
                    _ = tx.closed() => return,
                };
                if tx.send(Ok(resp)).await.is_err() {
                    return;
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::extproc::dispatch::tests::{connect_request, INGRESS_HTTP_LISTENER};
    use crate::router::extproc::dispatch::EGRESS_FILTER_CHAIN_NAME;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Stub {
        dir: Direction,
        called: AtomicBool,
    }

    #[async_trait]
    impl Handler for Stub {
        fn direction(&self) -> Direction {
            self.dir
        }
        async fn handle_request_headers(
            &self,
            _: &mut RequestMetadata,
        ) -> Result<HandlerResult, RouteError> {
            self.called.store(true, Ordering::SeqCst);
            Ok(HandlerResult::continue_request())
        }
    }

    #[tokio::test]
    async fn process_request_headers_dispatches_by_mode() {
        for (name, registered, chain, want_ran, want_status) in [
            (
                "egress-only instance serves egress traffic",
                vec![Direction::Egress],
                EGRESS_FILTER_CHAIN_NAME,
                Some(Direction::Egress),
                None,
            ),
            (
                "ingress-only instance serves ingress traffic",
                vec![Direction::Ingress],
                INGRESS_HTTP_LISTENER,
                Some(Direction::Ingress),
                None,
            ),
            (
                "egress-only instance refuses ingress traffic",
                vec![Direction::Egress],
                INGRESS_HTTP_LISTENER,
                None,
                Some(404),
            ),
            (
                "ingress-only instance refuses egress traffic",
                vec![Direction::Ingress],
                EGRESS_FILTER_CHAIN_NAME,
                None,
                Some(404),
            ),
        ] {
            let stubs: HashMap<Direction, Arc<Stub>> = registered
                .iter()
                .map(|d| {
                    (
                        *d,
                        Arc::new(Stub {
                            dir: *d,
                            called: AtomicBool::new(false),
                        }),
                    )
                })
                .collect();
            let handlers: Handlers = stubs
                .iter()
                .map(|(d, s)| (*d, s.clone() as Arc<dyn Handler>))
                .collect();
            let s = Server::new(None, handlers);
            let req = connect_request("envoy.filters.http.ext_proc", chain);
            let Some(processing_request::Request::RequestHeaders(h)) = &req.request else {
                unreachable!()
            };
            let resp = s.process_request_headers(&req, h).await;
            match want_status {
                Some(code) => {
                    let Some(processing_response::Response::ImmediateResponse(ir)) = resp.response
                    else {
                        panic!("{name}: want an immediate response");
                    };
                    assert_eq!(ir.status.unwrap().code, code, "{name}");
                    assert!(
                        String::from_utf8_lossy(&ir.body).contains("does not serve"),
                        "{name}"
                    );
                    assert!(
                        stubs.values().all(|s| !s.called.load(Ordering::SeqCst)),
                        "{name}: a handler ran"
                    );
                }
                None => {
                    assert!(
                        matches!(
                            resp.response,
                            Some(processing_response::Response::RequestHeaders(_))
                        ),
                        "{name}"
                    );
                    for (d, s) in &stubs {
                        assert_eq!(
                            s.called.load(Ordering::SeqCst),
                            Some(*d) == want_ran,
                            "{name}: {d}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn result_resume_defaults_to_unknown() {
        for (resume, want) in [
            ("", attr::resume::UNKNOWN),
            (attr::resume::NONE, attr::resume::NONE),
            (attr::resume::TRIGGERED, attr::resume::TRIGGERED),
            (attr::resume::JOINED, attr::resume::JOINED),
            (attr::resume::UNKNOWN, attr::resume::UNKNOWN),
        ] {
            let r = HandlerResult {
                resume: resume.into(),
                ..Default::default()
            };
            assert_eq!(r.resume(), want);
        }
    }
}

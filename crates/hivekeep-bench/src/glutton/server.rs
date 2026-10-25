//! Serving: the gRPC service, the protobuf-over-HTTP route table, the
//! wakeup probe, and one listener that carries gRPC and plain HTTP side by
//! side.

use std::convert::Infallible;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use tokio::net::TcpListener;
use tonic::{Code, Status};
use tower::ServiceExt;

use super::ctx::{parse_grpc_timeout, Ctx};
use super::service::Service;
use super::{
    BURN_CPU_ROUTE, INGEST_ROUTE, MODE_GRPC, MODE_HTTP, PING_ROUTE, READYZ_ROUTE, READ_DISK_ROUTE,
    READ_RAM_ROUTE, RUN_SCRIPT_ROUTE, USE_CPU_ROUTE, WRITE_DISK_ROUTE, WRITE_RAM_ROUTE,
};
use crate::pb::glutton::v1 as pb;
use crate::pb::glutton::v1::glutton_server::{Glutton, GluttonServer};
use crate::wire::{server_elapsed_value, SERVER_ELAPSED_HEADER};

/// Error type of response bodies.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// Response body every handler produces.
pub type Body = UnsyncBoxBody<Bytes, BoxError>;
type BoxFut = Pin<Box<dyn Future<Output = Response<Body>> + Send>>;

/// A request handler, cheap to clone.
#[derive(Clone)]
pub struct Handler(Arc<dyn Fn(Request<Incoming>) -> BoxFut + Send + Sync>);

impl Handler {
    /// Wraps an async function as a handler.
    pub fn new<F, Fut>(f: F) -> Handler
    where
        F: Fn(Request<Incoming>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Response<Body>> + Send + 'static,
    {
        Handler(Arc::new(move |req| Box::pin(f(req))))
    }

    /// Handles one request.
    pub async fn call(&self, req: Request<Incoming>) -> Response<Body> {
        (self.0)(req).await
    }
}

/// Why [`handler`] refused a mode.
#[derive(Debug, thiserror::Error)]
#[error("must be {MODE_GRPC} or {MODE_HTTP}: {0:?}")]
pub struct UnknownMode(pub String);

/// The request handler for a wire mode. gRPC mode serves the gRPC service
/// next to the wakeup probe on one listener; HTTP mode serves the
/// protobuf-over-HTTP route table.
pub fn handler(mode: &str, svc: Arc<Service>) -> Result<Handler, UnknownMode> {
    match mode {
        MODE_GRPC => Ok(split_grpc(
            grpc_handler(svc.clone()),
            readyz_handler(svc.readyz_dir.clone()),
        )),
        MODE_HTTP => Ok(http_handler(svc)),
        other => Err(UnknownMode(other.to_string())),
    }
}

/// Sends gRPC requests (HTTP/2 with a gRPC content type) to `grpc` and
/// everything else to `rest`. Every glutton RPC is unary.
pub fn split_grpc(grpc: Handler, rest: Handler) -> Handler {
    Handler::new(move |req: Request<Incoming>| {
        let is_grpc = req.version() == http::Version::HTTP_2
            && req
                .headers()
                .get(http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|ct| ct.starts_with("application/grpc"));
        let target = if is_grpc { grpc.clone() } else { rest.clone() };
        async move { target.call(req).await }
    })
}

/// The tonic service as a handler.
pub fn grpc_handler(svc: Arc<Service>) -> Handler {
    let server = GluttonServer::new(GrpcService { svc });
    Handler::new(move |req: Request<Incoming>| {
        let server = server.clone();
        async move {
            match server.oneshot(req).await {
                Ok(resp) => resp.map(|b| b.map_err(|e| -> BoxError { Box::new(e) }).boxed_unsync()),
                Err(never) => match never {},
            }
        }
    })
}

/// Serves `handler` on `listener` (HTTP/1.1 and cleartext HTTP/2) until
/// the returned future is dropped.
pub async fn serve(listener: TcpListener, handler: Handler) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let handler = handler.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req| {
                let handler = handler.clone();
                async move { Ok::<_, Infallible>(handler.call(req).await) }
            });
            if let Err(err) = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), svc)
                .await
            {
                tracing::debug!(%err, "connection ended with an error");
            }
        });
    }
}

/// The file the wakeup probe writes to check the data dir is writable.
pub const READYZ_CANARY: &str = ".readyz_canary";

fn full(status: StatusCode, body: impl Into<Bytes>) -> Response<Body> {
    let mut resp = Response::new(
        Full::new(body.into())
            .map_err(|never| match never {})
            .boxed_unsync(),
    );
    *resp.status_mut() = status;
    resp
}

/// A plain-text error answer, like a classic HTTP error page.
fn error_text(status: StatusCode, msg: &str) -> Response<Body> {
    let mut resp = full(status, format!("{msg}\n"));
    resp.headers_mut().insert(
        http::header::CONTENT_TYPE,
        "text/plain; charset=utf-8".parse().unwrap(),
    );
    resp
}

fn not_found() -> Response<Body> {
    error_text(StatusCode::NOT_FOUND, "404 page not found")
}

/// Answers the wakeup probe. With a writable dir set it writes a small file
/// there and fails until that works, so a resume waits for an external
/// volume to be mounted and writable.
fn readyz(writable_dir: Option<&Path>) -> Response<Body> {
    if let Some(dir) = writable_dir {
        if let Err(err) = std::fs::write(dir.join(READYZ_CANARY), b"ok") {
            return error_text(
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("data dir not writable: {err}"),
            );
        }
    }
    full(StatusCode::OK, Bytes::new())
}

/// A handler serving only the wakeup probe.
pub fn readyz_handler(writable_dir: Option<PathBuf>) -> Handler {
    Handler::new(move |req: Request<Incoming>| {
        let dir = writable_dir.clone();
        async move {
            if req.uri().path() == READYZ_ROUTE {
                readyz(dir.as_deref())
            } else {
                not_found()
            }
        }
    })
}

/// The HTTP-mode route table on top of the wakeup probe: each route is
/// POST-only, takes and returns a protobuf message, maps status codes
/// (InvalidArgument 400, NotFound 404, else 500), and stamps the
/// server-elapsed header so a load generator can split server time from
/// network time.
pub fn http_handler(svc: Arc<Service>) -> Handler {
    Handler::new(move |req: Request<Incoming>| {
        let svc = svc.clone();
        async move { http_route(&svc, req).await }
    })
}

async fn http_route(svc: &Service, req: Request<Incoming>) -> Response<Body> {
    let start = Instant::now();
    let path = req.uri().path().to_string();
    if path == READYZ_ROUTE {
        return readyz(svc.readyz_dir.as_deref());
    }
    let known = [
        PING_ROUTE,
        WRITE_DISK_ROUTE,
        READ_DISK_ROUTE,
        WRITE_RAM_ROUTE,
        READ_RAM_ROUTE,
        BURN_CPU_ROUTE,
        INGEST_ROUTE,
        USE_CPU_ROUTE,
        RUN_SCRIPT_ROUTE,
    ];
    if !known.contains(&path.as_str()) {
        return not_found();
    }
    if req.method() != Method::POST {
        return error_text(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    let body = match req.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(err) => return error_text(StatusCode::BAD_REQUEST, &err.to_string()),
    };
    // Ends background work (a burn, a script) if the caller goes away.
    let ctx = Ctx::background();
    let _guard = ctx.cancel_on_drop();
    let out = match path.as_str() {
        PING_ROUTE => run(&body, |r: pb::PingRequest| async move { Ok(svc.ping(&r)) }).await,
        WRITE_DISK_ROUTE => {
            run(&body, |r: pb::WriteDiskRequest| async move {
                svc.write_disk(&r).await
            })
            .await
        }
        READ_DISK_ROUTE => {
            run(&body, |r: pb::ReadDiskRequest| async move {
                svc.read_disk(&r).await
            })
            .await
        }
        WRITE_RAM_ROUTE => {
            run(
                &body,
                |r: pb::WriteRamRequest| async move { svc.write_ram(&r) },
            )
            .await
        }
        READ_RAM_ROUTE => {
            run(
                &body,
                |r: pb::ReadRamRequest| async move { svc.read_ram(&r) },
            )
            .await
        }
        BURN_CPU_ROUTE => {
            let ctx = &ctx;
            run(&body, |r: pb::BurnCpuRequest| async move {
                svc.burn_cpu(ctx, &r).await
            })
            .await
        }
        INGEST_ROUTE => {
            run(
                &body,
                |r: pb::IngestRequest| async move { svc.ingest(&r).await },
            )
            .await
        }
        USE_CPU_ROUTE => run(&body, |r: pb::UseCpuRequest| async move { svc.use_cpu(&r) }).await,
        _ => {
            let ctx = &ctx;
            run(&body, |r: pb::RunScriptRequest| async move {
                svc.run_script(ctx, &r).await
            })
            .await
        }
    };
    let mut resp = match out {
        Ok(bytes) => full(StatusCode::OK, bytes),
        Err((code, msg)) => return error_text(code, &msg),
    };
    let h = resp.headers_mut();
    h.insert(
        SERVER_ELAPSED_HEADER,
        server_elapsed_value(start).parse().unwrap(),
    );
    h.insert(
        http::header::CONTENT_TYPE,
        "application/x-protobuf".parse().unwrap(),
    );
    resp
}

/// Decodes the request, runs it, and encodes the answer or maps its error.
async fn run<Req, Resp, F, Fut>(body: &[u8], f: F) -> Result<Vec<u8>, (StatusCode, String)>
where
    Req: Message + Default,
    Resp: Message,
    F: FnOnce(Req) -> Fut,
    Fut: Future<Output = Result<Resp, Status>>,
{
    let req =
        Req::decode(body).map_err(|e| (StatusCode::BAD_REQUEST, format!("unmarshal: {e}")))?;
    match f(req).await {
        Ok(resp) => Ok(resp.encode_to_vec()),
        Err(st) => {
            let code = match st.code() {
                Code::InvalidArgument => StatusCode::BAD_REQUEST,
                Code::NotFound => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            Err((code, st.message().to_string()))
        }
    }
}

/// The tonic face of [`Service`].
pub struct GrpcService {
    svc: Arc<Service>,
}

impl GrpcService {
    /// Wraps a shared service.
    pub fn new(svc: Arc<Service>) -> GrpcService {
        GrpcService { svc }
    }
}

/// The lifetime of a gRPC request: its `grpc-timeout`, if any.
fn grpc_ctx<T>(req: &tonic::Request<T>) -> Ctx {
    match req
        .metadata()
        .get("grpc-timeout")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_grpc_timeout)
    {
        Some(timeout) => Ctx::with_timeout(timeout),
        None => Ctx::background(),
    }
}

type GrpcResult<T> = Result<tonic::Response<T>, Status>;

#[tonic::async_trait]
impl Glutton for GrpcService {
    async fn write_ram(
        &self,
        request: tonic::Request<pb::WriteRamRequest>,
    ) -> GrpcResult<pb::WriteRamResponse> {
        self.svc
            .write_ram(request.get_ref())
            .map(tonic::Response::new)
    }

    async fn read_ram(
        &self,
        request: tonic::Request<pb::ReadRamRequest>,
    ) -> GrpcResult<pb::ReadRamResponse> {
        self.svc
            .read_ram(request.get_ref())
            .map(tonic::Response::new)
    }

    async fn write_disk(
        &self,
        request: tonic::Request<pb::WriteDiskRequest>,
    ) -> GrpcResult<pb::WriteDiskResponse> {
        self.svc
            .write_disk(request.get_ref())
            .await
            .map(tonic::Response::new)
    }

    async fn read_disk(
        &self,
        request: tonic::Request<pb::ReadDiskRequest>,
    ) -> GrpcResult<pb::ReadDiskResponse> {
        self.svc
            .read_disk(request.get_ref())
            .await
            .map(tonic::Response::new)
    }

    async fn open_fd(
        &self,
        request: tonic::Request<pb::OpenFdRequest>,
    ) -> GrpcResult<pb::OpenFdResponse> {
        self.svc
            .open_fd(request.get_ref())
            .map(tonic::Response::new)
    }

    async fn ping(&self, request: tonic::Request<pb::PingRequest>) -> GrpcResult<pb::PingResponse> {
        Ok(tonic::Response::new(self.svc.ping(request.get_ref())))
    }

    async fn gossip(
        &self,
        request: tonic::Request<pb::GossipRequest>,
    ) -> GrpcResult<pb::GossipResponse> {
        self.svc
            .gossip(request.get_ref())
            .await
            .map(tonic::Response::new)
    }

    async fn burn_cpu(
        &self,
        request: tonic::Request<pb::BurnCpuRequest>,
    ) -> GrpcResult<pb::BurnCpuResponse> {
        let ctx = grpc_ctx(&request);
        let _guard = ctx.cancel_on_drop();
        self.svc
            .burn_cpu(&ctx, request.get_ref())
            .await
            .map(tonic::Response::new)
    }

    async fn ingest(
        &self,
        request: tonic::Request<pb::IngestRequest>,
    ) -> GrpcResult<pb::IngestResponse> {
        self.svc
            .ingest(request.get_ref())
            .await
            .map(tonic::Response::new)
    }

    async fn use_cpu(
        &self,
        request: tonic::Request<pb::UseCpuRequest>,
    ) -> GrpcResult<pb::UseCpuResponse> {
        self.svc
            .use_cpu(request.get_ref())
            .map(tonic::Response::new)
    }

    async fn run_script(
        &self,
        request: tonic::Request<pb::RunScriptRequest>,
    ) -> GrpcResult<pb::RunScriptResponse> {
        let ctx = grpc_ctx(&request);
        let _guard = ctx.cancel_on_drop();
        self.svc
            .run_script(&ctx, request.get_ref())
            .await
            .map(tonic::Response::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::glutton::script::tests::{op, read_disk, script, write_disk};
    use crate::glutton::service::Options;
    use crate::pb::glutton::v1::glutton_client::GluttonClient;

    async fn start(handler: Handler) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, handler));
        format!("http://{addr}")
    }

    fn service(dir: &Path, opts: Options) -> Arc<Service> {
        Arc::new(Service::new(dir, opts).unwrap())
    }

    async fn post(url: &str, body: Vec<u8>) -> reqwest::Response {
        reqwest::Client::new()
            .post(url)
            .header("content-type", "application/x-protobuf")
            .body(body)
            .send()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn split_grpc_serves_readyz_and_grpc_on_one_listener() {
        let dir = tempfile::tempdir().unwrap();
        let svc = service(dir.path(), Options::default());
        let url = start(split_grpc(grpc_handler(svc), readyz_handler(None))).await;
        let resp = reqwest::get(format!("{url}{READYZ_ROUTE}")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let mut client = GluttonClient::connect(url).await.unwrap();
        let pong = client
            .ping(pb::PingRequest {
                message: "hi".into(),
            })
            .await
            .unwrap();
        assert_eq!(pong.into_inner().message, "hi");
    }

    #[tokio::test]
    async fn split_grpc_routes_on_content_type() {
        let grpc_hit = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hit = grpc_hit.clone();
        let grpc = Handler::new(move |_req| {
            hit.store(true, std::sync::atomic::Ordering::SeqCst);
            async { full(StatusCode::OK, Bytes::new()) }
        });
        let rest = Handler::new(|_req| async { full(StatusCode::IM_A_TEAPOT, Bytes::new()) });
        let url = start(split_grpc(grpc, rest)).await;
        let resp = reqwest::get(format!("{url}/anything")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::IM_A_TEAPOT);
        assert!(
            !grpc_hit.load(std::sync::atomic::Ordering::SeqCst),
            "HTTP/1.1 GET reached the gRPC handler"
        );
    }

    #[tokio::test]
    async fn handler_rejects_unknown_mode() {
        let dir = tempfile::tempdir().unwrap();
        let svc = service(dir.path(), Options::default());
        for mode in [MODE_GRPC, MODE_HTTP] {
            assert!(handler(mode, svc.clone()).is_ok(), "{mode}");
        }
        assert!(handler("quic", svc).is_err());
    }

    #[tokio::test]
    async fn http_routes() {
        let dir = tempfile::tempdir().unwrap();
        let svc = service(dir.path(), Options::default());
        let url = start(http_handler(svc)).await;

        assert_eq!(
            reqwest::get(format!("{url}{READYZ_ROUTE}"))
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            reqwest::get(format!("{url}{PING_ROUTE}"))
                .await
                .unwrap()
                .status(),
            405
        );
        assert_eq!(
            post(&format!("{url}{PING_ROUTE}"), b"garbage".to_vec())
                .await
                .status(),
            400
        );

        let resp = post(
            &format!("{url}{PING_ROUTE}"),
            pb::PingRequest {
                message: "hello".into(),
            }
            .encode_to_vec(),
        )
        .await;
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.headers()["content-type"], "application/x-protobuf");
        assert!(resp.headers().contains_key(SERVER_ELAPSED_HEADER));
        let pong = pb::PingResponse::decode(resp.bytes().await.unwrap()).unwrap();
        assert_eq!(pong.message, "hello");

        let resp = post(
            &format!("{url}{WRITE_DISK_ROUTE}"),
            pb::WriteDiskRequest {
                key: "httpfile".into(),
                size: 512,
                write_mode: pb::WriteMode::Truncate as i32,
            }
            .encode_to_vec(),
        )
        .await;
        assert_eq!(resp.status(), 200);
        let written = pb::WriteDiskResponse::decode(resp.bytes().await.unwrap()).unwrap();
        assert_eq!(written.size, 512);

        let resp = post(
            &format!("{url}{READ_DISK_ROUTE}"),
            pb::ReadDiskRequest {
                key: "httpfile".into(),
                read_mode: 0,
            }
            .encode_to_vec(),
        )
        .await;
        assert_eq!(resp.status(), 200);
        let read = pb::ReadDiskResponse::decode(resp.bytes().await.unwrap()).unwrap();
        assert_eq!(read.size, 512);
        assert_eq!(read.sha256, written.sha256);

        let resp = post(
            &format!("{url}{WRITE_RAM_ROUTE}"),
            pb::WriteRamRequest {
                key: "httpram".into(),
                size: "1Ki".into(),
                write_mode: pb::WriteMode::Truncate as i32,
            }
            .encode_to_vec(),
        )
        .await;
        assert_eq!(resp.status(), 200);

        let resp = post(
            &format!("{url}{READ_RAM_ROUTE}"),
            pb::ReadRamRequest {
                key: "httpram".into(),
                size: String::new(),
            }
            .encode_to_vec(),
        )
        .await;
        assert_eq!(resp.status(), 200);
        assert_eq!(
            pb::ReadRamResponse::decode(resp.bytes().await.unwrap())
                .unwrap()
                .size,
            1024
        );

        let miss = pb::ReadDiskRequest {
            key: "nosuchfile".into(),
            read_mode: 0,
        };
        assert_eq!(
            post(&format!("{url}{READ_DISK_ROUTE}"), miss.encode_to_vec())
                .await
                .status(),
            404
        );
        let bad = pb::ReadDiskRequest {
            key: "../etc/passwd".into(),
            read_mode: 0,
        };
        assert_eq!(
            post(&format!("{url}{READ_DISK_ROUTE}"), bad.encode_to_vec())
                .await
                .status(),
            400
        );
    }

    #[tokio::test]
    async fn readyz_data_dir() {
        let writable = tempfile::tempdir().unwrap();
        let cases: Vec<(&str, Option<PathBuf>, u16, bool)> = vec![
            ("no data dir", None, 200, false),
            (
                "writable data dir",
                Some(writable.path().to_path_buf()),
                200,
                true,
            ),
            (
                "missing data dir",
                Some(writable.path().join("not-mounted")),
                503,
                false,
            ),
        ];
        for (name, dir, want, canary) in cases {
            let url = start(readyz_handler(dir.clone())).await;
            let resp = reqwest::get(format!("{url}{READYZ_ROUTE}")).await.unwrap();
            assert_eq!(resp.status().as_u16(), want, "{name}");
            if canary {
                assert!(
                    dir.unwrap().join(READYZ_CANARY).exists(),
                    "{name}: canary not written"
                );
            }
        }
    }

    #[tokio::test]
    async fn service_options() {
        let cases = [
            ("default", Options::default(), 200, false),
            (
                "writable readyz",
                Options {
                    writable_readyz: true,
                    ..Default::default()
                },
                503,
                false,
            ),
            (
                "sync writes",
                Options {
                    sync_writes: true,
                    ..Default::default()
                },
                200,
                true,
            ),
        ];
        for (name, opts, want, want_sync) in cases {
            // The data dir does not exist, like an external volume that is
            // not mounted yet.
            let tmp = tempfile::tempdir().unwrap();
            let svc = service(&tmp.path().join("not-mounted"), opts);
            assert_eq!(svc.sync_writes, want_sync, "{name}");
            let url = start(http_handler(svc)).await;
            let resp = reqwest::get(format!("{url}{READYZ_ROUTE}")).await.unwrap();
            assert_eq!(resp.status().as_u16(), want, "{name}");
        }
    }

    #[tokio::test]
    async fn run_script_http_route() {
        let dir = tempfile::tempdir().unwrap();
        let url = start(handler(MODE_HTTP, service(dir.path(), Options::default())).unwrap()).await;
        let resp = post(
            &format!("{url}{RUN_SCRIPT_ROUTE}"),
            script(vec![
                op(vec![write_disk("viahttp", 64)]),
                op(vec![read_disk("viahttp")]),
            ])
            .encode_to_vec(),
        )
        .await;
        assert_eq!(resp.status(), 200);
        let out = pb::RunScriptResponse::decode(resp.bytes().await.unwrap()).unwrap();
        assert_eq!(out.result.unwrap().stats.unwrap().requests_run, 2);
        let resp = post(
            &format!("{url}{RUN_SCRIPT_ROUTE}"),
            script(vec![op(vec![read_disk("absent")])]).encode_to_vec(),
        )
        .await;
        assert_eq!(resp.status(), 404);
        assert!(resp.text().await.unwrap().contains("(read_disk)"));
    }
}

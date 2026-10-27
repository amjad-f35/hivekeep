//! The small HTTP/1.1 admin surface every benchmark process has (`/stats`,
//! `/healthz`, and the egress actor's `/run`), served and read over TCP or
//! a Unix socket.

use std::convert::Infallible;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use http::{Method, Request, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio_util::sync::CancellationToken;

use crate::{procstats, Error, Result};

/// A response body.
pub type Body = BoxBody<Bytes, io::Error>;

/// Answers one admin request.
pub type Handler =
    Arc<dyn Fn(Request<Incoming>) -> BoxFuture<'static, Response<Body>> + Send + Sync>;

/// Extra `/stats` fields a process adds to its snapshot.
pub type Extra = Arc<dyn Fn() -> serde_json::Map<String, serde_json::Value> + Send + Sync>;

/// A complete body.
pub fn body(b: impl Into<Bytes>) -> Body {
    Full::new(b.into()).map_err(|never| match never {}).boxed()
}

/// A response with `status`, a content type and a complete body.
pub fn respond(status: StatusCode, content_type: &str, b: impl Into<Bytes>) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, content_type)
        .body(body(b))
        .unwrap_or_else(|_| Response::new(body(Bytes::new())))
}

/// `/stats` (the process snapshot plus `extra`) and `/healthz`. `/stats`
/// accepts `?gc=1` for callers written against collected runtimes; memory
/// here is freed when dropped, so it reads the same.
pub fn stats_handler(extra: Option<Extra>) -> Handler {
    Arc::new(move |req| {
        let extra = extra.clone();
        Box::pin(async move { stats_route(&req, extra.as_ref()).unwrap_or_else(not_found) })
    })
}

/// The `/stats` and `/healthz` routes, or `None` for any other path.
pub fn stats_route<B>(req: &Request<B>, extra: Option<&Extra>) -> Option<Response<Body>> {
    if req.method() != Method::GET {
        return None;
    }
    match req.uri().path() {
        "/stats" => {
            let fields = extra.map(|f| f()).unwrap_or_default();
            Some(respond(
                StatusCode::OK,
                "application/json",
                procstats::stats_body(fields),
            ))
        }
        "/healthz" => Some(respond(StatusCode::OK, "text/plain", "ok")),
        _ => None,
    }
}

/// 404.
pub fn not_found() -> Response<Body> {
    respond(StatusCode::NOT_FOUND, "text/plain", "not found\n")
}

async fn serve_conn<T>(io: T, handler: Handler)
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let svc = hyper::service::service_fn(move |req| {
        let handler = handler.clone();
        async move { Ok::<_, Infallible>(handler(req).await) }
    });
    let _ = hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(io), svc)
        .await;
}

/// Serves `handler` on TCP until `cancel`.
pub async fn serve_tcp(
    lis: TcpListener,
    handler: Handler,
    cancel: CancellationToken,
) -> io::Result<()> {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            accepted = lis.accept() => {
                let (conn, _) = accepted?;
                tokio::spawn(serve_conn(conn, handler.clone()));
            }
        }
    }
}

/// Serves `handler` on a Unix socket until `cancel`.
pub async fn serve_unix(
    lis: UnixListener,
    handler: Handler,
    cancel: CancellationToken,
) -> io::Result<()> {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            accepted = lis.accept() => {
                let (conn, _) = accepted?;
                tokio::spawn(serve_conn(conn, handler.clone()));
            }
        }
    }
}

/// Listens on a fresh Unix socket at `path` that a process running as
/// another user, in another container, may connect to.
pub fn listen_unix(path: &Path) -> io::Result<UnixListener> {
    let _ = std::fs::remove_file(path);
    let lis = UnixListener::bind(path)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o777))?;
    Ok(lis)
}

/// Binds `addr`, reading a bare `:port` as all interfaces.
pub async fn listen_tcp(addr: &str) -> io::Result<TcpListener> {
    let addr = if addr.starts_with(':') {
        format!("0.0.0.0{addr}")
    } else {
        addr.to_string()
    };
    TcpListener::bind(addr).await
}

#[derive(Debug, Clone)]
enum Endpoint {
    Tcp { authority: String },
    Unix(PathBuf),
}

/// A client of one process's admin endpoint. Each request uses its own
/// connection: admin traffic is a few requests a second.
#[derive(Debug, Clone)]
pub struct Client {
    /// The base URL, for messages.
    pub base: String,
    endpoint: Endpoint,
    timeout: Duration,
}

impl Client {
    /// A client of `http://host:port`.
    pub fn http(base: &str) -> Result<Client> {
        let rest = base
            .strip_prefix("http://")
            .ok_or_else(|| Error::msg(format!("admin URL {base:?} must start with http://")))?;
        let authority = rest.trim_end_matches('/').to_string();
        if authority.is_empty() || authority.contains('/') {
            return Err(Error::msg(format!(
                "admin URL {base:?} must be http://host:port"
            )));
        }
        Ok(Client {
            base: base.trim_end_matches('/').to_string(),
            endpoint: Endpoint::Tcp { authority },
            timeout: Duration::from_secs(5),
        })
    }

    /// A client of the endpoint served on the Unix socket at `path`.
    pub fn unix(path: impl Into<PathBuf>) -> Client {
        let path = path.into();
        Client {
            base: format!("unix:{}", path.display()),
            endpoint: Endpoint::Unix(path),
            timeout: Duration::from_secs(5),
        }
    }

    fn host(&self) -> &str {
        match &self.endpoint {
            Endpoint::Tcp { authority } => authority,
            Endpoint::Unix(_) => "localhost",
        }
    }

    /// Sends `req` and returns the response head with its streaming body,
    /// without a timeout.
    pub async fn send(
        &self,
        method: Method,
        path: &str,
        payload: Bytes,
    ) -> Result<Response<Incoming>> {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .header(http::header::HOST, self.host())
            .body(Full::new(payload))
            .map_err(|e| Error::msg(e.to_string()))?;
        match &self.endpoint {
            Endpoint::Tcp { authority } => {
                exchange(TcpStream::connect(authority.as_str()).await?, req).await
            }
            Endpoint::Unix(path) => exchange(UnixStream::connect(path).await?, req).await,
        }
    }

    /// GETs `path` and returns the status and body, within the timeout.
    pub async fn get(&self, path: &str) -> Result<(StatusCode, Bytes)> {
        let fut = async {
            let resp = self.send(Method::GET, path, Bytes::new()).await?;
            let status = resp.status();
            let b = resp
                .into_body()
                .collect()
                .await
                .map_err(|e| Error::msg(e.to_string()))?
                .to_bytes();
            Ok((status, b))
        };
        tokio::time::timeout(self.timeout, fut)
            .await
            .map_err(|_| Error::msg(format!("{}{path}: timed out", self.base)))?
    }

    /// Polls `/healthz` every 200 ms until it answers 200 or `deadline`.
    pub async fn wait_ready(&self, deadline: tokio::time::Instant) -> Result<()> {
        loop {
            if let Ok((StatusCode::OK, _)) = self.get("/healthz").await {
                return Ok(());
            }
            if tokio::time::Instant::now() + Duration::from_millis(200) > deadline {
                return Err(Error::msg(format!("{} not ready", self.base)));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

async fn exchange<T>(io: T, req: Request<Full<Bytes>>) -> Result<Response<Incoming>>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io))
        .await
        .map_err(|e| Error::msg(e.to_string()))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    sender
        .send_request(req)
        .await
        .map_err(|e| Error::msg(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn serves_stats_over_a_unix_socket() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("a.sock");
        let lis = listen_unix(&socket).unwrap();
        let cancel = CancellationToken::new();
        let extra: Extra = Arc::new(|| {
            let mut m = serde_json::Map::new();
            m.insert("x".into(), 1.into());
            m
        });
        tokio::spawn(serve_unix(lis, stats_handler(Some(extra)), cancel.clone()));
        let c = Client::unix(&socket);
        c.wait_ready(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let (status, b) = c.get("/stats").await.unwrap();
        assert_eq!(status, StatusCode::OK);
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(v["x"], 1);
        assert!(v["process"]["cpus"].as_u64().unwrap() > 0);
        assert_eq!(c.get("/nope").await.unwrap().0, StatusCode::NOT_FOUND);
        cancel.cancel();
    }

    #[test]
    fn http_urls() {
        assert!(Client::http("http://a:1").is_ok());
        assert!(Client::http("https://a:1").is_err());
        assert!(Client::http("http://a:1/x").is_err());
    }
}

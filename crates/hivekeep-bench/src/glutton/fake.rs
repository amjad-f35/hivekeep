#![allow(dead_code, clippy::result_large_err)]

//! A stand-in for a glutton Actor behind the gate, for load-class tests.
//!
//! [`Server`] holds one file: both disk routes report `len(data)` and
//! `sha256(data)`, and `/readdisk` serves `data` as its payload. Each
//! override field makes the Actor lie about exactly one property. Every
//! request is recorded so a test can assert what the client sent.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::Mutex;
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::glutton::{
    BURN_CPU_ROUTE, INGEST_ROUTE, PING_ROUTE, READ_DISK_ROUTE, READ_RAM_ROUTE, RUN_SCRIPT_ROUTE,
    USE_CPU_ROUTE, WRITE_DISK_ROUTE, WRITE_RAM_ROUTE,
};
use crate::pb::glutton::v1 as pb;
use crate::wire::SERVER_ELAPSED_HEADER;

/// What the fake Actor reports. Build it, then [`Server::start`] it.
#[derive(Debug, Default, Clone)]
pub struct Config {
    /// The file the Actor holds, driving size, digest and payload.
    pub data: Vec<u8>,
    /// Overrides the sha256 both disk routes return, leaving size and
    /// payload honest.
    pub digest: Option<Vec<u8>>,
    /// Served by `/readdisk` instead of `data`, keeping size and digest
    /// honest.
    pub corrupt_payload: Option<Vec<u8>>,
    /// Makes `/readdisk` omit the payload entirely (the digest-only wire
    /// form). Takes precedence over `corrupt_payload`.
    pub empty_payload: bool,
    /// Fails every route with this HTTP status.
    pub status: Option<u16>,
    /// Sets the server-elapsed header on every answer.
    pub elapsed_us: Option<String>,
}

/// Every request the fake has seen, by kind.
#[derive(Debug, Default, Clone)]
pub struct Recorded {
    /// Request paths, in arrival order.
    pub paths: Vec<String>,
    /// `/writedisk` sizes.
    pub write_sizes: Vec<i32>,
    /// `/readdisk` read modes.
    pub read_modes: Vec<pb::ReadMode>,
    /// `/writeram` size strings.
    pub ram_write_sizes: Vec<String>,
    /// `/writeram` write modes.
    pub ram_write_modes: Vec<pb::WriteMode>,
    /// `/readram` size strings.
    pub ram_read_sizes: Vec<String>,
    /// `/burncpu` durations.
    pub burn_millis: Vec<i64>,
    /// `/ingest` payload lengths.
    pub ingest_sizes: Vec<i64>,
    /// `/usecpu` requests.
    pub cpu_requests: Vec<pb::UseCpuRequest>,
    /// `/runscript` requests.
    pub scripts: Vec<pb::RunScriptRequest>,
    /// Request headers, one map per request in arrival order.
    pub headers: Vec<http::HeaderMap>,
}

struct Shared {
    config: Mutex<Config>,
    recorded: Mutex<Recorded>,
}

/// A running fake. Dropping it stops the listener.
pub struct Server {
    shared: Arc<Shared>,
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    /// Serves `config` on a loopback port.
    pub async fn start(config: Config) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake glutton");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(Shared {
            config: Mutex::new(config),
            recorded: Mutex::new(Recorded::default()),
        });
        let s = shared.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let s = s.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |req| {
                        let s = s.clone();
                        async move { Ok::<_, Infallible>(serve(&s, req).await) }
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });
        Server { shared, addr, task }
    }

    /// The base URL (`http://127.0.0.1:<port>`), with no trailing slash.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The listening address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Changes what the fake reports from now on.
    pub fn update(&self, f: impl FnOnce(&mut Config)) {
        f(&mut self.shared.config.lock());
    }

    /// A copy of everything recorded so far.
    pub fn recorded(&self) -> Recorded {
        self.shared.recorded.lock().clone()
    }

    /// The paths requested so far.
    pub fn recorded_paths(&self) -> Vec<String> {
        self.shared.recorded.lock().paths.clone()
    }

    /// The digest the fake reports, hex-encoded.
    pub fn hex_digest(&self) -> String {
        hex::encode(reported_digest(&self.shared.config.lock()))
    }
}

fn reported_digest(c: &Config) -> Vec<u8> {
    match &c.digest {
        Some(d) => d.clone(),
        None => Sha256::digest(&c.data).to_vec(),
    }
}

fn reported_payload(c: &Config) -> Vec<u8> {
    if c.empty_payload {
        return Vec::new();
    }
    c.corrupt_payload.clone().unwrap_or_else(|| c.data.clone())
}

fn text(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    let mut resp = Response::new(Full::new(Bytes::from(format!("{body}\n"))));
    *resp.status_mut() = status;
    resp
}

fn decode<M: Message + Default>(body: &[u8]) -> Result<M, Response<Full<Bytes>>> {
    M::decode(body).map_err(|e| text(StatusCode::BAD_REQUEST, &e.to_string()))
}

async fn serve(s: &Shared, req: Request<Incoming>) -> Response<Full<Bytes>> {
    let path = req.uri().path().to_string();
    {
        let mut r = s.recorded.lock();
        r.paths.push(path.clone());
        r.headers.push(req.headers().clone());
    }
    let config = s.config.lock().clone();
    if let Some(code) = config.status {
        let st = StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        return text(st, st.canonical_reason().unwrap_or(""));
    }
    let body = match req.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(e) => return text(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let out = match route(s, &config, &path, &body) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return text(StatusCode::NOT_FOUND, "404 page not found"),
        Err(resp) => return resp,
    };
    let mut resp = Response::new(Full::new(Bytes::from(out)));
    let h = resp.headers_mut();
    if let Some(us) = &config.elapsed_us {
        h.insert(
            SERVER_ELAPSED_HEADER,
            us.parse().expect("elapsed header value"),
        );
    }
    h.insert(
        http::header::CONTENT_TYPE,
        "application/x-protobuf".parse().unwrap(),
    );
    resp
}

fn route(
    s: &Shared,
    c: &Config,
    path: &str,
    body: &[u8],
) -> Result<Option<Vec<u8>>, Response<Full<Bytes>>> {
    let out = match path {
        WRITE_DISK_ROUTE => {
            let req: pb::WriteDiskRequest = decode(body)?;
            s.recorded.lock().write_sizes.push(req.size);
            pb::WriteDiskResponse {
                size: c.data.len() as i64,
                sha256: reported_digest(c),
            }
            .encode_to_vec()
        }
        READ_DISK_ROUTE => {
            let req: pb::ReadDiskRequest = decode(body)?;
            s.recorded.lock().read_modes.push(req.read_mode());
            pb::ReadDiskResponse {
                size: c.data.len() as i64,
                sha256: reported_digest(c),
                data: reported_payload(c),
            }
            .encode_to_vec()
        }
        WRITE_RAM_ROUTE => {
            let req: pb::WriteRamRequest = decode(body)?;
            let mut r = s.recorded.lock();
            r.ram_write_sizes.push(req.size.clone());
            r.ram_write_modes.push(req.write_mode());
            pb::WriteRamResponse {}.encode_to_vec()
        }
        READ_RAM_ROUTE => {
            let req: pb::ReadRamRequest = decode(body)?;
            s.recorded.lock().ram_read_sizes.push(req.size);
            pb::ReadRamResponse {
                size: c.data.len() as i64,
                checksum: 0,
            }
            .encode_to_vec()
        }
        BURN_CPU_ROUTE => {
            let req: pb::BurnCpuRequest = decode(body)?;
            s.recorded.lock().burn_millis.push(req.duration_ms);
            pb::BurnCpuResponse { iterations: 1 }.encode_to_vec()
        }
        INGEST_ROUTE => {
            let req: pb::IngestRequest = decode(body)?;
            s.recorded
                .lock()
                .ingest_sizes
                .push(req.payload.len() as i64);
            pb::IngestResponse {
                size: req.payload.len() as i64,
                sha256: Sha256::digest(&req.payload).to_vec(),
            }
            .encode_to_vec()
        }
        PING_ROUTE => {
            let req: pb::PingRequest = decode(body)?;
            pb::PingResponse {
                message: req.message,
            }
            .encode_to_vec()
        }
        USE_CPU_ROUTE => {
            let req: pb::UseCpuRequest = decode(body)?;
            let n = req.num_cores;
            s.recorded.lock().cpu_requests.push(req);
            pb::UseCpuResponse { num_cores: n }.encode_to_vec()
        }
        RUN_SCRIPT_ROUTE => {
            let req: pb::RunScriptRequest = decode(body)?;
            // The fake does no work, so the script is instant: every node
            // entered once, every request run.
            let result = mirror_block(req.script.as_ref());
            s.recorded.lock().scripts.push(req);
            pb::RunScriptResponse {
                result: Some(result),
            }
            .encode_to_vec()
        }
        _ => return Ok(None),
    };
    Ok(Some(out))
}

fn stats(passes: i64, requests_run: i64) -> Option<pb::Stats> {
    Some(pb::Stats {
        passes,
        requests_run,
        ..Default::default()
    })
}

/// The result tree the real glutton returns for a script that cost
/// nothing: one pass per node and one request per leaf.
pub fn mirror_block(block: Option<&pb::Block>) -> pb::StepResult {
    let mut result = pb::StepResult {
        stats: stats(1, 0),
        children: Vec::new(),
    };
    for step in block.map(|b| b.steps.as_slice()).unwrap_or_default() {
        let child = match &step.kind {
            Some(pb::step::Kind::Block(b)) => mirror_block(Some(b)),
            Some(pb::step::Kind::Operation(op)) => {
                let n = op.requests.len() as i64;
                pb::StepResult {
                    stats: stats(1, n),
                    children: (0..n)
                        .map(|_| pb::StepResult {
                            stats: stats(1, 1),
                            children: Vec::new(),
                        })
                        .collect(),
                }
            }
            None => pb::StepResult {
                stats: Some(pb::Stats::default()),
                children: Vec::new(),
            },
        };
        if let Some(st) = result.stats.as_mut() {
            st.requests_run += child.stats.as_ref().map(|s| s.requests_run).unwrap_or(0);
        }
        result.children.push(child);
    }
    result
}

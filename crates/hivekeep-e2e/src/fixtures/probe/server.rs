//! The probe's HTTP server and routing.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context as _;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde::Serialize;
use tokio::net::TcpListener;

use super::bootfetch::{boot_fetch, RestoreFetch, BOOT_FETCH_URL_ENV};
use super::{capabilities, fetch, files, resources, IDENTITY_FILE, TRUST_FILE};

/// How `/healthz` answers.
#[derive(Clone)]
enum Health {
    /// Always 200: the probe serves as soon as it listens.
    Ready,
    /// Gated on the current identity having done the boot fetch.
    Gated(Arc<RestoreFetch>),
}

fn json<T: Serialize>(v: &T) -> Response<Full<Bytes>> {
    let body = serde_json::to_vec(v).unwrap_or_default();
    let mut res = Response::new(Full::new(Bytes::from(body)));
    res.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    res
}

fn status(code: u16, body: String) -> Response<Full<Bytes>> {
    let mut res = Response::new(Full::new(Bytes::from(body)));
    *res.status_mut() = StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    res
}

fn query_pairs(req: &Request<Incoming>) -> Vec<(String, String)> {
    url::form_urlencoded::parse(req.uri().query().unwrap_or("").as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

fn first(query: &[(String, String)], key: &str) -> String {
    query
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}

async fn route(
    req: Request<Incoming>,
    health: Health,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let query = query_pairs(&req);
    let res = match req.uri().path() {
        "/whoami" => json(&files::whoami()),
        "/fetch" => json(&fetch::fetch(&query).await),
        "/readfile" => json(&files::readfile(&first(&query, "path"))),
        "/writefile" => json(&files::writefile(&first(&query, "path"))),
        "/lookup" => json(&files::lookup(&first(&query, "host")).await),
        "/resources" => json(&resources::resources()),
        "/capabilities" => json(&capabilities::capabilities()),
        "/healthz" => match &health {
            Health::Ready => status(200, String::new()),
            Health::Gated(gate) => {
                let (code, body) = gate.healthz();
                status(code, body)
            }
        },
        _ => status(404, "404 page not found\n".into()),
    };
    Ok(res)
}

/// Performs the boot fetch if one is asked for, then serves on `listen`
/// until the process ends.
pub async fn serve(listen: SocketAddr) -> anyhow::Result<()> {
    let mut health = Health::Ready;
    if let Ok(url) = std::env::var(BOOT_FETCH_URL_ENV) {
        if !url.is_empty() {
            let booted =
                std::fs::read(IDENTITY_FILE).with_context(|| format!("reading {IDENTITY_FILE}"))?;
            boot_fetch(&url, TRUST_FILE)
                .await
                .map_err(anyhow::Error::msg)?;
            eprintln!("probe: boot fetch of {url} succeeded");
            health = Health::Gated(RestoreFetch::new(
                url,
                IDENTITY_FILE,
                TRUST_FILE,
                String::from_utf8_lossy(&booted).into_owned(),
            ));
        }
    }
    // Held before serving, so every snapshot of this Actor contains an open
    // guest handle on a system-info file.
    files::hold_identity();

    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("listening on {listen}"))?;
    eprintln!("probe listening on {listen}");
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("probe: accept: {e}");
                continue;
            }
        };
        let health = health.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req| route(req, health.clone()));
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), svc)
                .await
            {
                eprintln!("probe: serving connection: {e}");
            }
        });
    }
}

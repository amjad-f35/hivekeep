//! An in-memory OCI registry for pull tests, served over plain HTTP on a
//! loopback port, with a hook in front of every request so tests can
//! throttle, authenticate, gate, count or tamper with responses.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use base64::Engine as _;
use bytes::Bytes;
use futures::future::BoxFuture;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

use super::image::{build_image, BuiltImage, MemLayer};
use crate::digest::Digest;
use crate::manifest::{Descriptor, ImageConfig, Index, Platform, MEDIA_OCI_INDEX};

/// What a hook saw of a request.
#[derive(Debug, Clone)]
pub struct ReqInfo {
    pub method: String,
    pub path: String,
    pub basic: Option<(String, String)>,
    pub authorization: Option<String>,
}

/// What a hook decides.
pub enum Action {
    /// Serve normally.
    Pass,
    /// Answer with this status and headers instead.
    Respond(u16, Vec<(String, String)>),
    /// Answer with this status, headers and body instead.
    RespondBody(u16, Vec<(String, String)>, Vec<u8>),
    /// Serve normally, rewriting the body.
    Tamper(Box<dyn FnOnce(Vec<u8>) -> Vec<u8> + Send>),
}

/// A request hook.
pub type Hook = Arc<dyn Fn(ReqInfo) -> BoxFuture<'static, Action> + Send + Sync>;

#[derive(Default)]
struct Storage {
    blobs: HashMap<String, Bytes>,
    manifests: HashMap<(String, String), (String, Bytes)>,
}

/// A running test registry.
pub struct TestRegistry {
    pub host: String,
    storage: Arc<Mutex<Storage>>,
    tasks: Arc<Mutex<JoinSet<()>>>,
}

impl TestRegistry {
    /// Starts a registry with no hook.
    pub async fn start() -> Self {
        Self::start_with(Arc::new(|_| Box::pin(async { Action::Pass }))).await
    }

    /// Starts a registry behind `hook`.
    pub async fn start_with(hook: Hook) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let storage: Arc<Mutex<Storage>> = Arc::default();
        let tasks: Arc<Mutex<JoinSet<()>>> = Arc::new(Mutex::new(JoinSet::new()));
        let (st, ts) = (storage.clone(), tasks.clone());
        tasks.lock().spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let (st, hook) = (st.clone(), hook.clone());
                let svc = service_fn(move |req| serve(st.clone(), hook.clone(), req));
                ts.lock().spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });
        TestRegistry {
            host,
            storage,
            tasks,
        }
    }

    /// Stops serving and drops every open connection.
    pub fn close(&self) {
        self.tasks.lock().abort_all();
    }

    fn put_blob(&self, data: &[u8]) -> Digest {
        let d = Digest::of(data);
        self.storage
            .lock()
            .blobs
            .insert(d.to_string(), Bytes::copy_from_slice(data));
        d
    }

    fn put_manifest(&self, repo: &str, tag: Option<&str>, media: &str, body: &[u8]) -> Digest {
        let d = Digest::of(body);
        let mut s = self.storage.lock();
        let v = (media.to_string(), Bytes::copy_from_slice(body));
        s.manifests
            .insert((repo.to_string(), d.to_string()), v.clone());
        if let Some(t) = tag {
            s.manifests.insert((repo.to_string(), t.to_string()), v);
        }
        d
    }

    /// Stores an already built image under `repo:tag`.
    pub fn push_built(&self, repo_tag: &str, img: &BuiltImage) {
        let (repo, tag) = split_repo_tag(repo_tag);
        for l in &img.layers {
            self.put_blob(&l.compressed);
        }
        self.put_blob(&img.config);
        self.put_manifest(repo, Some(tag), &img.manifest_media_type, &img.manifest);
    }

    /// Builds and stores an image of `layers` under `repo:tag` (a path
    /// without the host, e.g. `test/app:latest`).
    pub fn push(&self, repo_tag: &str, cfg: ImageConfig, layers: &[MemLayer]) -> BuiltImage {
        let img = build_image(cfg, layers, None);
        self.push_built(repo_tag, &img);
        img
    }

    /// Stores an image whose config claims `diff_ids` for `layers`.
    pub fn push_lying(
        &self,
        repo_tag: &str,
        layers: &[MemLayer],
        diff_ids: Vec<Digest>,
    ) -> BuiltImage {
        let img = build_image(ImageConfig::default(), layers, Some(diff_ids));
        self.push_built(repo_tag, &img);
        img
    }

    /// Stores a multi-platform index of `images` under `repo:tag`.
    pub fn push_index(&self, repo_tag: &str, images: &[(Platform, BuiltImage)]) -> Digest {
        let (repo, tag) = split_repo_tag(repo_tag);
        let mut manifests = Vec::new();
        for (p, img) in images {
            for l in &img.layers {
                self.put_blob(&l.compressed);
            }
            self.put_blob(&img.config);
            self.put_manifest(repo, None, &img.manifest_media_type, &img.manifest);
            manifests.push(Descriptor {
                media_type: img.manifest_media_type.clone(),
                size: img.manifest.len() as i64,
                digest: img.digest.clone(),
                platform: Some(p.clone()),
            });
        }
        let index = serde_json::to_vec(&Index {
            schema_version: 2,
            media_type: MEDIA_OCI_INDEX.to_string(),
            manifests,
        })
        .unwrap();
        self.put_manifest(repo, Some(tag), MEDIA_OCI_INDEX, &index)
    }

    /// A full reference for `path` (`repo:tag` or `repo@digest`).
    pub fn reference(&self, path: &str) -> String {
        format!("{}/{}", self.host, path)
    }
}

impl Drop for TestRegistry {
    fn drop(&mut self) {
        self.close();
    }
}

fn split_repo_tag(repo_tag: &str) -> (&str, &str) {
    match repo_tag.rsplit_once(':') {
        Some((r, t)) if !t.contains('/') => (r, t),
        _ => (repo_tag, "latest"),
    }
}

fn basic_of(req: &Request<Incoming>) -> Option<(String, String)> {
    let v = req.headers().get("authorization")?.to_str().ok()?;
    let enc = v.strip_prefix("Basic ")?;
    let raw = base64::engine::general_purpose::STANDARD.decode(enc).ok()?;
    let raw = String::from_utf8(raw).ok()?;
    let (u, p) = raw.split_once(':')?;
    Some((u.to_string(), p.to_string()))
}

fn response(status: u16, headers: Vec<(String, String)>, body: Bytes) -> Response<Full<Bytes>> {
    let mut b = Response::builder().status(status);
    for (k, v) in headers {
        b = b.header(k, v);
    }
    b.body(Full::new(body)).unwrap()
}

async fn serve(
    storage: Arc<Mutex<Storage>>,
    hook: Hook,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let info = ReqInfo {
        method: req.method().to_string(),
        path: req.uri().path().to_string(),
        basic: basic_of(&req),
        authorization: req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
    };
    let head = req.method() == hyper::Method::HEAD;
    let tamper = match hook(info.clone()).await {
        Action::Pass => None,
        Action::Respond(status, headers) => return Ok(response(status, headers, Bytes::new())),
        Action::RespondBody(status, headers, body) => {
            return Ok(response(status, headers, Bytes::from(body)))
        }
        Action::Tamper(f) => Some(f),
    };
    let not_found = || {
        response(
            404,
            vec![],
            Bytes::from_static(br#"{"errors":[{"code":"NOT_FOUND"}]}"#),
        )
    };
    let path = info.path.as_str();
    if path == "/v2/" || path == "/v2" {
        return Ok(response(200, vec![], Bytes::new()));
    }
    let Some(rest) = path.strip_prefix("/v2/") else {
        return Ok(not_found());
    };
    let (status, headers, body) = if let Some(i) = rest.rfind("/manifests/") {
        let (repo, r) = (&rest[..i], &rest[i + "/manifests/".len()..]);
        let Some((media, body)) = storage
            .lock()
            .manifests
            .get(&(repo.to_string(), r.to_string()))
            .cloned()
        else {
            return Ok(not_found());
        };
        let d = Digest::of(&body);
        (
            200,
            vec![
                ("content-type".to_string(), media),
                ("docker-content-digest".to_string(), d.to_string()),
            ],
            body,
        )
    } else if let Some(i) = rest.rfind("/blobs/") {
        let d = &rest[i + "/blobs/".len()..];
        let Some(body) = storage.lock().blobs.get(d).cloned() else {
            return Ok(not_found());
        };
        (
            200,
            vec![("docker-content-digest".to_string(), d.to_string())],
            body,
        )
    } else {
        return Ok(not_found());
    };
    let body = match tamper {
        Some(f) => Bytes::from(f(body.to_vec())),
        None => body,
    };
    let mut headers = headers;
    headers.push(("content-length".to_string(), body.len().to_string()));
    Ok(response(
        status,
        headers,
        if head { Bytes::new() } else { body },
    ))
}

/// A hook from a synchronous decision.
pub fn hook(f: impl Fn(&ReqInfo) -> Action + Send + Sync + 'static) -> Hook {
    Arc::new(move |r| {
        let a = f(&r);
        Box::pin(async move { a })
    })
}

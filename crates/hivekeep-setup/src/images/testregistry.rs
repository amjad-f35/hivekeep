//! An in-process registry for tests: serves pushed manifests and blobs over
//! the read half of the OCI distribution API, on plain HTTP.

use super::reference::Platform;
use super::registry::sha256_digest;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const INDEX_TYPE: &str = "application/vnd.oci.image.index.v1+json";
const MANIFEST_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
const CONFIG_TYPE: &str = "application/vnd.oci.image.config.v1+json";

#[derive(Default)]
struct Store {
    /// (repository, tag or digest) → (media type, body).
    manifests: HashMap<(String, String), (String, Vec<u8>)>,
    blobs: HashMap<String, Vec<u8>>,
}

pub struct TestRegistry {
    addr: std::net::SocketAddr,
    store: Arc<Mutex<Store>>,
}

/// An image config blob for `p`.
pub fn config_body(p: &Platform) -> Vec<u8> {
    let mut v = serde_json::json!({"architecture": p.architecture, "os": p.os, "rootfs": {"type": "layers", "diff_ids": []}});
    if !p.variant.is_empty() {
        v["variant"] = serde_json::json!(p.variant);
    }
    serde_json::to_vec(&v).unwrap()
}

/// An image manifest naming `config`.
pub fn manifest_body(config: &[u8]) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "mediaType": MANIFEST_TYPE,
        "config": {"mediaType": CONFIG_TYPE, "digest": sha256_digest(config), "size": config.len()},
        "layers": [],
    }))
    .unwrap()
}

/// An index with one empty image per platform.
pub fn index_body(platforms: &[Platform]) -> Vec<u8> {
    let manifests: Vec<serde_json::Value> = platforms
        .iter()
        .map(|p| {
            let m = manifest_body(&config_body(p));
            let mut platform = serde_json::json!({"os": p.os, "architecture": p.architecture});
            if !p.variant.is_empty() {
                platform["variant"] = serde_json::json!(p.variant);
            }
            serde_json::json!({"mediaType": MANIFEST_TYPE, "digest": sha256_digest(&m), "size": m.len(), "platform": platform})
        })
        .collect();
    serde_json::to_vec(
        &serde_json::json!({"schemaVersion": 2, "mediaType": INDEX_TYPE, "manifests": manifests}),
    )
    .unwrap()
}

fn respond(status: &str, headers: &[(&str, String)], body: &[u8], head: bool) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (k, v) in headers {
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    out.push_str("\r\n");
    let mut bytes = out.into_bytes();
    if !head {
        bytes.extend_from_slice(body);
    }
    bytes
}

fn handle(store: &Store, method: &str, path: &str) -> Vec<u8> {
    let head = method == "HEAD";
    let not_found = |code: &str| {
        let body = format!(r#"{{"errors":[{{"code":"{code}","message":"not found"}}]}}"#);
        respond(
            "404 Not Found",
            &[("Content-Type", "application/json".into())],
            body.as_bytes(),
            head,
        )
    };
    let Some(rest) = path.strip_prefix("/v2/") else {
        return not_found("NAME_UNKNOWN");
    };
    if let Some((repo, reference)) = rest.split_once("/manifests/") {
        return match store
            .manifests
            .get(&(repo.to_string(), reference.to_string()))
        {
            Some((media_type, body)) => respond(
                "200 OK",
                &[
                    ("Content-Type", media_type.clone()),
                    ("Docker-Content-Digest", sha256_digest(body)),
                ],
                body,
                head,
            ),
            None => not_found("MANIFEST_UNKNOWN"),
        };
    }
    if let Some((_, digest)) = rest.split_once("/blobs/") {
        return match store.blobs.get(digest) {
            Some(b) => respond(
                "200 OK",
                &[("Content-Type", "application/octet-stream".into())],
                b,
                head,
            ),
            None => not_found("BLOB_UNKNOWN"),
        };
    }
    not_found("NAME_UNKNOWN")
}

impl TestRegistry {
    async fn serve(deny: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let store: Arc<Mutex<Store>> = Arc::default();
        let s = store.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let s = s.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let text = String::from_utf8_lossy(&buf);
                    let mut first = text.lines().next().unwrap_or("").split_whitespace();
                    let method = first.next().unwrap_or("").to_string();
                    let path = first
                        .next()
                        .unwrap_or("")
                        .split('?')
                        .next()
                        .unwrap_or("")
                        .to_string();
                    let out = if deny {
                        respond("403 Forbidden", &[], b"denied", method == "HEAD")
                    } else {
                        handle(&s.lock(), &method, &path)
                    };
                    let _ = sock.write_all(&out).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        Self { addr, store }
    }

    pub async fn start() -> Self {
        Self::serve(false).await
    }

    /// A registry that refuses every request.
    pub async fn start_denying() -> Self {
        Self::serve(true).await
    }

    pub fn host(&self) -> String {
        self.addr.to_string()
    }

    fn put_manifest(&self, repo: &str, tag: &str, media_type: &str, body: Vec<u8>) {
        let mut st = self.store.lock();
        let digest = sha256_digest(&body);
        st.manifests
            .insert((repo.into(), digest), (media_type.into(), body.clone()));
        st.manifests
            .insert((repo.into(), tag.into()), (media_type.into(), body));
    }

    /// Pushes an index with one empty image per platform as `repo:tag`.
    pub fn push_index(&self, repo: &str, tag: &str, platforms: &[Platform]) {
        for p in platforms {
            let config = config_body(p);
            let m = manifest_body(&config);
            let mut st = self.store.lock();
            st.blobs.insert(sha256_digest(&config), config);
            st.manifests
                .insert((repo.into(), sha256_digest(&m)), (MANIFEST_TYPE.into(), m));
        }
        self.put_manifest(repo, tag, INDEX_TYPE, index_body(platforms));
    }

    /// Pushes a single image whose config names `platform`.
    pub fn push_image(&self, repo: &str, tag: &str, platform: &Platform) {
        let config = config_body(platform);
        let m = manifest_body(&config);
        self.store
            .lock()
            .blobs
            .insert(sha256_digest(&config), config);
        self.put_manifest(repo, tag, MANIFEST_TYPE, m);
    }
}

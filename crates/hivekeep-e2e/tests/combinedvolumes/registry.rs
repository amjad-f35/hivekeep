//! Building a small OCI image in memory and pushing it to a registry over
//! the distribution API, for the image-volume fixture.
//!
//! Layers are uncompressed tars. Authentication covers anonymous access and
//! the `auths` entries of the local Docker config (basic credentials, also
//! used to fetch a bearer token when the registry asks for one); credential
//! helpers are not consulted.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use reqwest::{Method, StatusCode};
use sha2::{Digest, Sha256};

const LAYER_MEDIA_TYPE: &str = "application/vnd.oci.image.layer.v1.tar";
const CONFIG_MEDIA_TYPE: &str = "application/vnd.oci.image.config.v1+json";
const MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";

/// A content-addressed blob.
pub struct Blob {
    pub media_type: &'static str,
    pub data: Vec<u8>,
}

impl Blob {
    pub fn digest(&self) -> String {
        sha256(&self.data)
    }
}

fn sha256(data: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(data)))
}

/// A layer holding `files` (path to contents), each read-only. A path whose
/// base name starts with `.wh.` is an OCI whiteout for the lower path of
/// the same name.
pub fn tar_layer(files: &[(&str, &str)]) -> Result<Blob, String> {
    let mut builder = tar::Builder::new(Vec::new());
    for (name, body) in files {
        let mut header = tar::Header::new_ustar();
        header.set_mode(0o444);
        header.set_size(body.len() as u64);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_cksum();
        builder
            .append_data(&mut header, name, body.as_bytes())
            .map_err(|e| format!("writing tar entry {name:?}: {e}"))?;
    }
    let data = builder
        .into_inner()
        .map_err(|e| format!("closing tar: {e}"))?;
    Ok(Blob {
        media_type: LAYER_MEDIA_TYPE,
        data,
    })
}

/// The config and manifest of an image made of `layers`, bottom first.
pub fn image(layers: &[Blob]) -> (Blob, Blob) {
    let diff_ids: Vec<String> = layers.iter().map(Blob::digest).collect();
    let config =
        serde_json::json!({ "rootfs": { "type": "layers", "diff_ids": diff_ids }, "config": {} });
    let config = Blob {
        media_type: CONFIG_MEDIA_TYPE,
        data: serde_json::to_vec(&config).expect("json"),
    };
    let descriptor = |b: &Blob| serde_json::json!({ "mediaType": b.media_type, "digest": b.digest(), "size": b.data.len() });
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": MANIFEST_MEDIA_TYPE,
        "config": descriptor(&config),
        "layers": layers.iter().map(descriptor).collect::<Vec<_>>(),
    });
    (
        config,
        Blob {
            media_type: MANIFEST_MEDIA_TYPE,
            data: serde_json::to_vec(&manifest).expect("json"),
        },
    )
}

/// Splits `host/path/name:tag` into registry host, repository and tag.
pub fn parse_reference(reference: &str) -> Result<(String, String, String), String> {
    let (host, rest) = reference
        .split_once('/')
        .ok_or_else(|| format!("{reference:?} names no registry"))?;
    let (repo, tag) = rest
        .rsplit_once(':')
        .filter(|(_, t)| !t.contains('/'))
        .ok_or_else(|| format!("{reference:?} has no tag"))?;
    Ok((host.to_string(), repo.to_string(), tag.to_string()))
}

fn http_client() -> reqwest::Client {
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(cert);
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("TLS versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    reqwest::Client::builder()
        .tls_backend_preconfigured(config)
        .timeout(Duration::from_secs(120))
        .build()
        .expect("HTTP client")
}

/// The basic credentials the Docker config holds for `host`, if any.
fn docker_basic_auth(host: &str) -> Option<String> {
    let dir = std::env::var_os("DOCKER_CONFIG")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|h| std::path::PathBuf::from(h).join(".docker"))
        })?;
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("config.json")).ok()?).ok()?;
    let auths = config.get("auths")?.as_object()?;
    auths
        .iter()
        .find(|(k, _)| {
            k.trim_start_matches("https://")
                .trim_start_matches("http://")
                .trim_end_matches('/')
                == host
        })
        .and_then(|(_, v)| v.get("auth")?.as_str().map(str::to_string))
}

/// Parses `Bearer realm="..",service="..",scope=".."`.
fn bearer_params(challenge: &str) -> Option<Vec<(String, String)>> {
    let rest = challenge.strip_prefix("Bearer ")?;
    let mut out = Vec::new();
    for part in rest.split(',') {
        let (k, v) = part.trim().split_once('=')?;
        out.push((k.to_string(), v.trim_matches('"').to_string()));
    }
    Some(out)
}

/// One registry session: scheme, repository and the authorization header
/// that works.
pub struct Pusher {
    client: reqwest::Client,
    base: String,
    host: String,
    repo: String,
    auth: Option<String>,
}

impl Pusher {
    /// Connects to `host` over HTTPS, falling back to cleartext for a
    /// registry that serves none (a local development registry).
    pub async fn new(host: &str, repo: &str) -> Result<Pusher, String> {
        let client = http_client();
        let mut last = String::new();
        for scheme in ["https", "http"] {
            let base = format!("{scheme}://{host}");
            match client.get(format!("{base}/v2/")).send().await {
                Ok(_) => {
                    return Ok(Pusher {
                        client,
                        base,
                        host: host.into(),
                        repo: repo.into(),
                        auth: None,
                    })
                }
                Err(e) => last = e.to_string(),
            }
        }
        Err(format!("reaching registry {host}: {last}"))
    }

    async fn authorize(&mut self, challenge: &str) -> Result<(), String> {
        let basic = docker_basic_auth(&self.host);
        if challenge.starts_with("Basic") {
            self.auth = Some(format!(
                "Basic {}",
                basic.ok_or("registry wants basic credentials and the Docker config has none")?
            ));
            return Ok(());
        }
        let params = bearer_params(challenge)
            .ok_or_else(|| format!("unsupported registry challenge {challenge:?}"))?;
        let get = |k: &str| {
            params
                .iter()
                .find(|(pk, _)| pk == k)
                .map(|(_, v)| v.clone())
        };
        let realm = get("realm").ok_or("bearer challenge without realm")?;
        let mut url = url::Url::parse(&realm).map_err(|e| format!("token realm {realm:?}: {e}"))?;
        url.query_pairs_mut()
            .append_pair("scope", &format!("repository:{}:pull,push", self.repo));
        if let Some(service) = get("service") {
            url.query_pairs_mut().append_pair("service", &service);
        }
        let mut req = self.client.get(url.as_str());
        if let Some(b) = basic {
            req = req.header("authorization", format!("Basic {b}"));
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("fetching a registry token: {e}"))?;
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("decoding the registry token: {e}"))?;
        let token = body
            .get("token")
            .or_else(|| body.get("access_token"))
            .and_then(|v| v.as_str())
            .ok_or("registry token response holds no token")?;
        self.auth = Some(format!("Bearer {token}"));
        Ok(())
    }

    /// Sends a request, authorizing once on a 401 challenge.
    async fn send(
        &mut self,
        method: Method,
        url: &str,
        content_type: Option<&str>,
        body: Option<&[u8]>,
    ) -> Result<reqwest::Response, String> {
        for _ in 0..2 {
            let mut req = self.client.request(method.clone(), url);
            if let Some(a) = &self.auth {
                req = req.header("authorization", a);
            }
            if let Some(ct) = content_type {
                req = req.header("content-type", ct);
            }
            if let Some(b) = body {
                req = req.body(b.to_vec());
            }
            let resp = req
                .send()
                .await
                .map_err(|e| format!("{method} {url}: {e}"))?;
            if resp.status() == StatusCode::UNAUTHORIZED && self.auth.is_none() {
                let challenge = resp
                    .headers()
                    .get("www-authenticate")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                self.authorize(&challenge).await?;
                continue;
            }
            return Ok(resp);
        }
        Err(format!("{method} {url}: still unauthorized"))
    }

    /// Uploads `blob` unless the registry already has it.
    pub async fn push_blob(&mut self, blob: &Blob) -> Result<(), String> {
        let digest = blob.digest();
        let url = format!("{}/v2/{}/blobs/{digest}", self.base, self.repo);
        if self
            .send(Method::HEAD, &url, None, None)
            .await?
            .status()
            .is_success()
        {
            return Ok(());
        }
        let start = format!("{}/v2/{}/blobs/uploads/", self.base, self.repo);
        let resp = self.send(Method::POST, &start, None, None).await?;
        if resp.status() != StatusCode::ACCEPTED {
            return Err(format!(
                "starting the upload of {digest}: {}",
                resp.status()
            ));
        }
        let location = resp
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .ok_or("upload has no location")?
            .to_string();
        let location = if location.starts_with('/') {
            format!("{}{location}", self.base)
        } else {
            location
        };
        let sep = if location.contains('?') { '&' } else { '?' };
        let put = format!(
            "{location}{sep}digest={}",
            url::form_urlencoded::byte_serialize(digest.as_bytes()).collect::<String>()
        );
        let resp = self
            .send(
                Method::PUT,
                &put,
                Some("application/octet-stream"),
                Some(&blob.data),
            )
            .await?;
        if resp.status() != StatusCode::CREATED {
            return Err(format!(
                "finishing the upload of {digest}: {}",
                resp.status()
            ));
        }
        Ok(())
    }

    /// Puts `manifest` under `tag`.
    pub async fn push_manifest(&mut self, tag: &str, manifest: &Blob) -> Result<(), String> {
        let url = format!("{}/v2/{}/manifests/{tag}", self.base, self.repo);
        let resp = self
            .send(
                Method::PUT,
                &url,
                Some(manifest.media_type),
                Some(&manifest.data),
            )
            .await?;
        if !resp.status().is_success() {
            return Err(format!(
                "putting the manifest: {} {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            ));
        }
        Ok(())
    }
}

/// Pushes an image of `layers` as `reference` (host/repo:tag) and returns
/// its digest-pinned reference.
pub async fn push(reference: &str, layers: Vec<Blob>) -> Result<String, String> {
    let (host, repo, tag) = parse_reference(reference)?;
    let (config, manifest) = image(&layers);
    let mut pusher = Pusher::new(&host, &repo).await?;
    for blob in layers.iter().chain(std::iter::once(&config)) {
        pusher.push_blob(blob).await?;
    }
    pusher.push_manifest(&tag, &manifest).await?;
    Ok(format!("{host}/{repo}@{}", manifest.digest()))
}

/// Base64 helper for tests of the Docker config format.
#[allow(dead_code)]
pub fn basic(user: &str, pass: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"))
}

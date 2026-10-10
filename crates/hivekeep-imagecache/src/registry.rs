//! A minimal OCI distribution client: everything a pull needs and nothing
//! else (no pushes, no catalog).
//!
//! A [`Session`] is one repository with one credential. Every request
//! retries transient failures (429, 5xx, connection errors) with the
//! registry's backoff, answers auth challenges once, and observes the
//! caller's [`Ctx`]. Rejections surface as [`ErrorKind::Registry`] errors
//! carrying the status.

use std::io;
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use hivekeep_node::context::Ctx;
use parking_lot::Mutex;
use reqwest::header::{ACCEPT, AUTHORIZATION, WWW_AUTHENTICATE};
use reqwest::{Method, StatusCode};
use serde::Deserialize;

use crate::auth::{parse_challenge, pull_scope, Challenge, Credential};
use crate::backoff::{retryable_status, Backoff};
use crate::digest::Digest;
use crate::error::{Error, ErrorKind, Result};
use crate::manifest::{is_index, ConfigFile, Index, Manifest, Platform, ACCEPTED_MANIFESTS};
use crate::reference::Reference;

/// The largest manifest or config accepted.
const MAX_DOCUMENT_BYTES: usize = 8 << 20;
/// Bounds connecting and each request's response headers. A blob body is
/// bounded only by the caller's context.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Builds the HTTP client pulls share.
pub fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(REQUEST_TIMEOUT)
        .read_timeout(REQUEST_TIMEOUT)
        .user_agent(concat!("hivekeep-imagecache/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| Error::msg(format!("while building the registry HTTP client: {e}")))
}

#[derive(Debug, Clone)]
enum AuthState {
    /// No challenge seen yet.
    Unknown,
    /// Send the credential as Basic auth.
    Basic,
    /// Send this bearer token.
    Bearer(String),
}

/// One repository on one registry, with one credential.
pub struct Session {
    http: reqwest::Client,
    reference: Reference,
    credential: Option<Credential>,
    backoff: Backoff,
    auth: Mutex<AuthState>,
}

/// A resolved manifest.
#[derive(Debug, Clone)]
pub struct ResolvedImage {
    /// The image manifest (the platform child's, for an index).
    pub manifest: Manifest,
    /// The digest of `manifest` itself.
    pub manifest_digest: Digest,
    /// The image config.
    pub config: ConfigFile,
}

#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default)]
    token: String,
    #[serde(default)]
    access_token: String,
}

impl Session {
    /// A session for `reference`'s repository.
    pub fn new(
        http: reqwest::Client,
        reference: Reference,
        credential: Option<Credential>,
        backoff: Backoff,
    ) -> Self {
        Session {
            http,
            reference,
            credential,
            backoff,
            auth: Mutex::new(AuthState::Unknown),
        }
    }

    fn url(&self, kind: &str, r: &str) -> String {
        format!(
            "{}/v2/{}/{kind}/{r}",
            self.reference.base_url(),
            self.reference.repository
        )
    }

    /// Sends one request, retrying transient failures and answering one
    /// auth challenge. Returns the successful response.
    async fn send(
        &self,
        ctx: &Ctx,
        method: Method,
        url: &str,
        accept: Option<&str>,
    ) -> Result<reqwest::Response> {
        let mut attempt = 0u32;
        let mut challenged = false;
        loop {
            if let Some(e) = ctx.err() {
                return Err(Error::from(e).context(format!("{method} {url}")));
            }
            let mut req = self.http.request(method.clone(), url);
            if let Some(a) = accept {
                req = req.header(ACCEPT, a);
            }
            match &*self.auth.lock() {
                AuthState::Unknown => {}
                AuthState::Basic => {
                    if let Some(c) = &self.credential {
                        req = req.header(AUTHORIZATION, c.basic_header());
                    }
                }
                AuthState::Bearer(t) => req = req.header(AUTHORIZATION, format!("Bearer {t}")),
            }
            let res = ctx
                .run(req.send())
                .await
                .map_err(|e| Error::from(e).context(format!("{method} {url}")))?;
            let retry = match res {
                Ok(resp) if resp.status().is_success() => return Ok(resp),
                Ok(resp) if resp.status() == StatusCode::UNAUTHORIZED && !challenged => {
                    challenged = true;
                    let challenge = resp
                        .headers()
                        .get(WWW_AUTHENTICATE)
                        .and_then(|v| v.to_str().ok())
                        .and_then(parse_challenge);
                    match challenge {
                        Some(Challenge::Basic) if self.credential.is_some() => {
                            *self.auth.lock() = AuthState::Basic;
                            continue;
                        }
                        Some(Challenge::Bearer {
                            realm,
                            service,
                            scope,
                        }) => {
                            let token = self
                                .fetch_token(ctx, &realm, service.as_deref(), scope.as_deref())
                                .await?;
                            *self.auth.lock() = AuthState::Bearer(token);
                            continue;
                        }
                        _ => return Err(status_error(&method, url, resp.status())),
                    }
                }
                Ok(resp) if retryable_status(resp.status().as_u16()) => {
                    status_error(&method, url, resp.status())
                }
                Ok(resp) => return Err(status_error(&method, url, resp.status())),
                Err(e) => Error::msg(format!("{method} {url}: {}", error_chain(&e))),
            };
            attempt += 1;
            if attempt >= self.backoff.steps.max(1) {
                return Err(retry);
            }
            tracing::debug!(%url, attempt, err = %retry, "Retrying registry request");
            ctx.sleep(self.backoff.delay(attempt - 1))
                .await
                .map_err(|e| Error::from(e).context(format!("{method} {url}")))?;
        }
    }

    async fn fetch_token(
        &self,
        ctx: &Ctx,
        realm: &str,
        service: Option<&str>,
        scope: Option<&str>,
    ) -> Result<String> {
        let mut url = url::Url::parse(realm)
            .map_err(|e| Error::msg(format!("invalid token realm {realm:?}: {e}")))?;
        {
            let mut q = url.query_pairs_mut();
            if let Some(s) = service {
                q.append_pair("service", s);
            }
            q.append_pair(
                "scope",
                &scope
                    .map(str::to_string)
                    .unwrap_or_else(|| pull_scope(&self.reference.repository)),
            );
        }
        let mut attempt = 0u32;
        loop {
            let mut req = self.http.get(url.clone());
            if let Some(c) = &self.credential {
                req = req.header(AUTHORIZATION, c.basic_header());
            }
            let res = ctx
                .run(req.send())
                .await
                .map_err(|e| Error::from(e).context("while fetching a registry token"))?;
            let err = match res {
                Ok(resp) if resp.status().is_success() => {
                    let body = read_limited(ctx, resp).await?;
                    let t: TokenResponse = serde_json::from_slice(&body).map_err(|e| {
                        Error::msg(format!("while decoding the registry token: {e}"))
                    })?;
                    let token = if t.token.is_empty() {
                        t.access_token
                    } else {
                        t.token
                    };
                    if token.is_empty() {
                        return Err(Error::msg("registry token service returned no token"));
                    }
                    return Ok(token);
                }
                Ok(resp) if retryable_status(resp.status().as_u16()) => {
                    status_error(&Method::GET, realm, resp.status())
                }
                Ok(resp) => return Err(status_error(&Method::GET, realm, resp.status())),
                Err(e) => Error::msg(format!("GET {realm}: {}", error_chain(&e))),
            };
            attempt += 1;
            if attempt >= self.backoff.steps.max(1) {
                return Err(err);
            }
            ctx.sleep(self.backoff.delay(attempt - 1)).await?;
        }
    }

    /// Resolves the reference's tag to a manifest digest with one HEAD (a
    /// GET when the registry omits the digest header).
    pub async fn head_digest(&self, ctx: &Ctx) -> Result<Digest> {
        let accept = ACCEPTED_MANIFESTS.join(", ");
        let url = self.url("manifests", &self.reference.api_reference());
        let resp = self.send(ctx, Method::HEAD, &url, Some(&accept)).await?;
        if let Some(d) = resp
            .headers()
            .get("docker-content-digest")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| Digest::parse(v).ok())
        {
            return Ok(d);
        }
        let resp = self.send(ctx, Method::GET, &url, Some(&accept)).await?;
        Ok(Digest::of(&read_limited(ctx, resp).await?))
    }

    /// Fetches the manifest `digest`, checking its bytes hash to it.
    async fn manifest_bytes(&self, ctx: &Ctx, digest: &Digest) -> Result<(String, Bytes)> {
        let accept = ACCEPTED_MANIFESTS.join(", ");
        let url = self.url("manifests", &digest.to_string());
        let resp = self.send(ctx, Method::GET, &url, Some(&accept)).await?;
        let media = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = read_limited(ctx, resp).await?;
        let got = Digest::of(&body);
        if &got != digest {
            return Err(Error::msg(format!(
                "manifest digest mismatch: got {got}, want {digest}"
            )));
        }
        Ok((media, body))
    }

    /// Fetches the image `digest` names, resolving an index to `platform`'s
    /// manifest, and its config.
    pub async fn image(
        &self,
        ctx: &Ctx,
        digest: &Digest,
        platform: &Platform,
    ) -> Result<ResolvedImage> {
        let (mut media, mut body) = self.manifest_bytes(ctx, digest).await?;
        let mut manifest_digest = digest.clone();
        if media.is_empty() {
            media = sniff_media_type(&body);
        }
        if is_index(&media) {
            let index: Index = serde_json::from_slice(&body)
                .map_err(|e| Error::msg(format!("while decoding image index: {e}")))?;
            let child = index
                .manifests
                .iter()
                .find(|d| d.platform.as_ref().is_some_and(|p| platform.matches(p)))
                .ok_or_else(|| {
                    Error::msg(format!(
                        "no child with platform {}/{} in index {digest}",
                        platform.os, platform.architecture
                    ))
                })?;
            manifest_digest = child.digest.clone();
            (media, body) = self.manifest_bytes(ctx, &manifest_digest).await?;
            if media.is_empty() {
                media = sniff_media_type(&body);
            }
            if is_index(&media) {
                return Err(Error::msg(format!(
                    "nested image index {manifest_digest} is not supported"
                )));
            }
        }
        let manifest: Manifest = serde_json::from_slice(&body)
            .map_err(|e| Error::msg(format!("while decoding image manifest: {e}")))?;
        let cfg_bytes = self.blob_bytes(ctx, &manifest.config.digest).await?;
        let config: ConfigFile = serde_json::from_slice(&cfg_bytes)
            .map_err(|e| Error::msg(format!("while decoding image config: {e}")))?;
        Ok(ResolvedImage {
            manifest,
            manifest_digest,
            config,
        })
    }

    /// Fetches a small blob whole, checking its digest.
    async fn blob_bytes(&self, ctx: &Ctx, digest: &Digest) -> Result<Bytes> {
        let resp = self
            .send(
                ctx,
                Method::GET,
                &self.url("blobs", &digest.to_string()),
                None,
            )
            .await?;
        let body = read_limited(ctx, resp).await?;
        let got = Digest::of(&body);
        if &got != digest {
            return Err(Error::msg(format!(
                "blob digest mismatch: got {got}, want {digest}"
            )));
        }
        Ok(body)
    }

    /// Opens a blob as a byte stream that fails (rather than ending) once
    /// `cancel` is cancelled. The caller verifies the digest.
    pub async fn blob_stream(
        &self,
        ctx: &Ctx,
        digest: &Digest,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<impl Stream<Item = io::Result<Bytes>> + Send + Unpin + 'static> {
        let resp = self
            .send(
                ctx,
                Method::GET,
                &self.url("blobs", &digest.to_string()),
                None,
            )
            .await?;
        let body = resp.bytes_stream().map_err(io::Error::other);
        let watch = cancel.clone();
        let tail = futures::stream::once(async move { watch.is_cancelled() }).filter_map(|c| {
            futures::future::ready(c.then(|| Err(io::Error::other("context canceled"))))
        });
        Ok(Box::pin(
            body.take_until(cancel.cancelled_owned()).chain(tail),
        ))
    }
}

fn sniff_media_type(body: &[u8]) -> String {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Probe {
        #[serde(default)]
        media_type: String,
        #[serde(default)]
        manifests: Option<serde_json::Value>,
    }
    match serde_json::from_slice::<Probe>(body) {
        Ok(p) if !p.media_type.is_empty() => p.media_type,
        Ok(p) if p.manifests.is_some() => crate::manifest::MEDIA_OCI_INDEX.to_string(),
        _ => crate::manifest::MEDIA_OCI_MANIFEST.to_string(),
    }
}

async fn read_limited(ctx: &Ctx, resp: reqwest::Response) -> Result<Bytes> {
    let mut out = Vec::new();
    let mut stream = resp.bytes_stream();
    loop {
        let next = ctx.run(stream.next()).await?;
        match next {
            None => break,
            Some(Ok(chunk)) => {
                out.extend_from_slice(&chunk);
                if out.len() > MAX_DOCUMENT_BYTES {
                    return Err(Error::msg("registry document exceeds the size limit"));
                }
            }
            Some(Err(e)) => {
                return Err(Error::msg(format!(
                    "while reading registry response: {}",
                    error_chain(&e)
                )))
            }
        }
    }
    Ok(out.into())
}

fn status_error(method: &Method, url: &str, status: StatusCode) -> Error {
    Error::new(
        ErrorKind::Registry(status.as_u16()),
        format!("{method} {url}: unexpected status code {status}"),
    )
}

fn error_chain(e: &dyn std::error::Error) -> String {
    let mut s = e.to_string();
    let mut src = e.source();
    while let Some(c) = src {
        s.push_str(": ");
        s.push_str(&c.to_string());
        src = c.source();
    }
    s
}

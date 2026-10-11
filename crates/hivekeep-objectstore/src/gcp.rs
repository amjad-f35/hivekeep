//! Google OAuth access tokens: the metadata server, service-account keys,
//! fixed tokens, and a cache that refreshes ahead of expiry.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use http::Method;
use parking_lot::Mutex;
use serde::Deserialize;
use url::Url;

use crate::error::{Error, Result};
use crate::http::{Request, ReqwestTransport, Transport};

/// The scope storage and IAM calls are made with.
pub const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
/// The default metadata server.
pub const DEFAULT_METADATA_HOST: &str = "169.254.169.254";

/// An access token and when it expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessToken {
    /// The bearer token.
    pub token: String,
    /// When it expires; `None` for a token without a known expiry.
    pub expiry: Option<SystemTime>,
}

/// Hands out access tokens.
#[async_trait]
pub trait TokenSource: Send + Sync + 'static {
    /// A token, or `None` when requests go unauthenticated.
    async fn token(&self) -> Result<Option<AccessToken>>;
}

/// Sends no credentials, for public buckets and emulators.
#[derive(Debug, Clone, Copy, Default)]
pub struct Anonymous;

#[async_trait]
impl TokenSource for Anonymous {
    async fn token(&self) -> Result<Option<AccessToken>> {
        Ok(None)
    }
}

/// Always the same token.
#[derive(Debug, Clone)]
pub struct StaticToken(pub AccessToken);

#[async_trait]
impl TokenSource for StaticToken {
    async fn token(&self) -> Result<Option<AccessToken>> {
        Ok(Some(self.0.clone()))
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

/// Tokens of the instance's (or pod's workload identity's) service account
/// from the metadata server.
pub struct MetadataServer {
    base: Url,
    transport: Arc<dyn Transport>,
}

impl MetadataServer {
    /// A source asking `host` (`GCE_METADATA_HOST`, or the default).
    pub fn new(host: Option<String>) -> Result<Self> {
        let host = host.unwrap_or_else(|| DEFAULT_METADATA_HOST.to_string());
        let base = Url::parse(&format!("http://{host}/"))
            .map_err(|e| Error::other(format!("metadata host {host:?}: {e}")))?;
        let transport = Arc::new(ReqwestTransport::new(Some(Duration::from_secs(5)))?);
        Ok(MetadataServer { base, transport })
    }
}

#[async_trait]
impl TokenSource for MetadataServer {
    async fn token(&self) -> Result<Option<AccessToken>> {
        let url = self
            .base
            .join("computeMetadata/v1/instance/service-accounts/default/token")
            .map_err(|e| Error::other(e.to_string()))?;
        let resp = self
            .transport
            .send(Request::new(Method::GET, url).header("metadata-flavor", "Google"))
            .await?;
        let status = resp.status.as_u16();
        let body = resp.bytes().await?;
        if status != 200 {
            return Err(Error::other(format!(
                "metadata server answered HTTP {status}"
            )));
        }
        let t: TokenResponse = serde_json::from_slice(&body)
            .map_err(|e| Error::other(format!("metadata server token: {e}")))?;
        Ok(Some(AccessToken {
            token: t.access_token,
            expiry: t
                .expires_in
                .map(|s| SystemTime::now() + Duration::from_secs(s)),
        }))
    }
}

#[derive(Deserialize)]
struct ServiceAccountFile {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    client_email: String,
    #[serde(default)]
    private_key: String,
    #[serde(default)]
    private_key_id: String,
    #[serde(default)]
    token_uri: String,
}

/// Tokens for a service account from its JSON key, by signed assertion.
pub struct ServiceAccountKey {
    email: String,
    key: jsonwebtoken::EncodingKey,
    key_id: String,
    token_uri: Url,
    transport: Arc<dyn Transport>,
}

impl ServiceAccountKey {
    /// Reads a key file.
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let raw = std::fs::read(path)
            .map_err(|e| Error::from(e).context(format!("reading {}", path.display())))?;
        let f: ServiceAccountFile = serde_json::from_slice(&raw)
            .map_err(|e| Error::other(format!("parsing {}: {e}", path.display())))?;
        if f.kind != "service_account" {
            return Err(Error::other(format!(
                "credentials file type {:?} is not supported; use a service account key or the metadata server",
                f.kind
            )));
        }
        let key = jsonwebtoken::EncodingKey::from_rsa_pem(f.private_key.as_bytes())
            .map_err(|e| Error::other(format!("service account key: {e}")))?;
        let token_uri = if f.token_uri.is_empty() {
            "https://oauth2.googleapis.com/token".to_string()
        } else {
            f.token_uri
        };
        Ok(ServiceAccountKey {
            email: f.client_email,
            key,
            key_id: f.private_key_id,
            token_uri: Url::parse(&token_uri).map_err(|e| Error::other(e.to_string()))?,
            transport: Arc::new(ReqwestTransport::new(Some(Duration::from_secs(30)))?),
        })
    }
}

#[async_trait]
impl TokenSource for ServiceAccountKey {
    async fn token(&self) -> Result<Option<AccessToken>> {
        #[derive(serde::Serialize)]
        struct Claims<'a> {
            iss: &'a str,
            scope: &'a str,
            aud: &'a str,
            iat: u64,
            exp: u64,
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        if !self.key_id.is_empty() {
            header.kid = Some(self.key_id.clone());
        }
        let assertion = jsonwebtoken::encode(
            &header,
            &Claims {
                iss: &self.email,
                scope: CLOUD_PLATFORM_SCOPE,
                aud: self.token_uri.as_str(),
                iat: now,
                exp: now + 3600,
            },
            &self.key,
        )
        .map_err(|e| Error::other(format!("signing the token assertion: {e}")))?;
        let body = form_urlencoded::Serializer::new(String::new())
            .append_pair("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
            .append_pair("assertion", &assertion)
            .finish();
        let resp = self
            .transport
            .send(
                Request::new(Method::POST, self.token_uri.clone())
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(body),
            )
            .await?;
        let status = resp.status.as_u16();
        let body = resp.bytes().await?;
        if status != 200 {
            return Err(Error::other(format!(
                "token endpoint answered HTTP {status}"
            )));
        }
        let t: TokenResponse = serde_json::from_slice(&body)
            .map_err(|e| Error::other(format!("token response: {e}")))?;
        Ok(Some(AccessToken {
            token: t.access_token,
            expiry: t
                .expires_in
                .map(|s| SystemTime::now() + Duration::from_secs(s)),
        }))
    }
}

/// Reuses a token until less than `margin` of its life is left.
pub struct Cached {
    inner: Arc<dyn TokenSource>,
    margin: Duration,
    cur: Mutex<Option<AccessToken>>,
}

impl Cached {
    /// Caches `inner`'s tokens, refreshing `margin` before expiry.
    pub fn new(inner: Arc<dyn TokenSource>, margin: Duration) -> Self {
        Cached {
            inner,
            margin,
            cur: Mutex::new(None),
        }
    }
}

#[async_trait]
impl TokenSource for Cached {
    async fn token(&self) -> Result<Option<AccessToken>> {
        if let Some(t) = self.cur.lock().clone() {
            let fresh = match t.expiry {
                None => true,
                Some(exp) => exp
                    .duration_since(SystemTime::now())
                    .map(|left| left > self.margin)
                    .unwrap_or(false),
            };
            if fresh {
                return Ok(Some(t));
            }
        }
        let t = self.inner.token().await?;
        *self.cur.lock() = t.clone();
        Ok(t)
    }
}

/// The ambient credential: a service-account key file named by
/// `GOOGLE_APPLICATION_CREDENTIALS`, otherwise the metadata server. Tokens are
/// reused until five minutes before expiry.
pub fn default_credentials() -> Result<Arc<dyn TokenSource>> {
    let inner: Arc<dyn TokenSource> = match std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS") {
        Some(p) if !p.is_empty() => {
            Arc::new(ServiceAccountKey::from_file(std::path::Path::new(&p))?)
        }
        _ => Arc::new(MetadataServer::new(
            std::env::var("GCE_METADATA_HOST").ok(),
        )?),
    };
    Ok(Arc::new(Cached::new(inner, Duration::from_secs(300))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct Counting(AtomicU32, Duration);

    #[async_trait]
    impl TokenSource for Counting {
        async fn token(&self) -> Result<Option<AccessToken>> {
            let n = self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Some(AccessToken {
                token: format!("t{n}"),
                expiry: Some(SystemTime::now() + self.1),
            }))
        }
    }

    #[tokio::test]
    async fn cached_refreshes_inside_the_margin() {
        let long = Arc::new(Counting(AtomicU32::new(0), Duration::from_secs(3600)));
        let c = Cached::new(long.clone(), Duration::from_secs(300));
        c.token().await.unwrap();
        c.token().await.unwrap();
        assert_eq!(long.0.load(Ordering::SeqCst), 1);
        let short = Arc::new(Counting(AtomicU32::new(0), Duration::from_secs(60)));
        let c = Cached::new(short.clone(), Duration::from_secs(300));
        c.token().await.unwrap();
        c.token().await.unwrap();
        assert_eq!(short.0.load(Ordering::SeqCst), 2);
    }
}

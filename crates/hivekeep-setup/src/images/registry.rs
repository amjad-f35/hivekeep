//! A minimal OCI distribution API client: manifest HEAD/GET and blob GET,
//! with the bearer-token challenge flow and credentials from the docker
//! config file.

use super::reference::ImageRef;
use super::ImageError;
use base64::Engine;
use reqwest::{header, Method, StatusCode};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Media types a manifest request accepts: indexes and single images, OCI
/// and Docker flavours.
pub const MANIFEST_ACCEPT: &[&str] = &[
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
];

/// Whether a manifest media type is an index of per-platform manifests.
pub fn is_index(media_type: &str) -> bool {
    media_type == "application/vnd.oci.image.index.v1+json"
        || media_type == "application/vnd.docker.distribution.manifest.list.v2+json"
}

/// A registry error, keeping what decides whether the image is absent.
#[derive(Clone, Debug)]
pub struct RegistryFailure {
    pub status: u16,
    /// Error codes from the registry's JSON error body.
    pub codes: Vec<String>,
    pub message: String,
}

impl RegistryFailure {
    /// The repository or the tag does not exist.
    pub fn is_not_found(&self) -> bool {
        self.status == 404
            || self
                .codes
                .iter()
                .any(|c| c == "MANIFEST_UNKNOWN" || c == "NAME_UNKNOWN")
    }
}

impl std::fmt::Display for RegistryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.codes.is_empty() {
            write!(f, "{} (HTTP {})", self.message, self.status)
        } else {
            write!(
                f,
                "{}: {} (HTTP {})",
                self.codes.join(", "),
                self.message,
                self.status
            )
        }
    }
}

/// A fetched manifest.
#[derive(Clone, Debug)]
pub struct Manifest {
    pub media_type: String,
    pub digest: String,
    pub body: Vec<u8>,
}

/// `sha256:<hex>` of `data`.
pub fn sha256_digest(data: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(data)))
}

/// Basic credentials for `host` from `~/.docker/config.json` (or
/// `$DOCKER_CONFIG/config.json`). Credential helpers are not consulted.
pub fn docker_config_auth(host: &str) -> Option<(String, String)> {
    let dir = std::env::var("DOCKER_CONFIG")
        .ok()
        .filter(|d| !d.is_empty())
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(|h| std::path::Path::new(&h).join(".docker"))
        })?;
    let raw = std::fs::read(dir.join("config.json")).ok()?;
    auth_from_config(&raw, host)
}

/// The `auths` entry for `host` in a docker config document.
pub fn auth_from_config(raw: &[u8], host: &str) -> Option<(String, String)> {
    let doc: serde_json::Value = serde_json::from_slice(raw).ok()?;
    let auths = doc.get("auths")?.as_object()?;
    let candidates = [
        host.to_string(),
        format!("https://{host}"),
        format!("http://{host}"),
    ];
    let hub = host == "index.docker.io" || host == "registry-1.docker.io";
    let entry = auths.iter().find(|(k, _)| {
        let k = k.trim_end_matches('/');
        candidates.iter().any(|c| c == k) || (hub && k.contains("index.docker.io"))
    })?;
    let encoded = entry.1.get("auth")?.as_str()?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let (user, pass) = text.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

/// Parses a `WWW-Authenticate` challenge into its scheme and parameters.
pub fn parse_challenge(value: &str) -> (String, HashMap<String, String>) {
    let (scheme, rest) = value.split_once(' ').unwrap_or((value, ""));
    let mut params = HashMap::new();
    let mut rest = rest.trim();
    while !rest.is_empty() {
        let Some((key, after)) = rest.split_once('=') else {
            break;
        };
        let key = key.trim().trim_start_matches(',').trim().to_lowercase();
        let (val, remaining) = if let Some(quoted) = after.strip_prefix('"') {
            match quoted.split_once('"') {
                Some((v, r)) => (v.to_string(), r),
                None => (quoted.to_string(), ""),
            }
        } else {
            match after.split_once(',') {
                Some((v, r)) => (v.trim().to_string(), r),
                None => (after.trim().to_string(), ""),
            }
        };
        params.insert(key, val);
        rest = remaining.trim_start_matches(',').trim();
    }
    (scheme.to_lowercase(), params)
}

/// A registry client for one repository.
pub struct RegistryClient {
    http: reqwest::Client,
    reference: ImageRef,
    auth: Option<String>,
}

fn http_client() -> Result<reqwest::Client, ImageError> {
    // The workspace links more than one rustls provider; pick one before the
    // client builds its TLS config.
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .build()
        .map_err(|e| ImageError::Registry(format!("while creating the registry client: {e}")))
}

impl RegistryClient {
    pub fn new(reference: ImageRef) -> Result<Self, ImageError> {
        Ok(Self {
            http: http_client()?,
            reference,
            auth: None,
        })
    }

    fn base(&self) -> String {
        format!(
            "{}://{}/v2/{}",
            self.reference.scheme(),
            self.reference.api_host(),
            self.reference.repository
        )
    }

    async fn failure(resp: reqwest::Response) -> RegistryFailure {
        let status = resp.status().as_u16();
        let body = resp.bytes().await.unwrap_or_default();
        let mut codes = Vec::new();
        let mut message = String::from_utf8_lossy(&body).trim().to_string();
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&body) {
            if let Some(errs) = v.get("errors").and_then(|e| e.as_array()) {
                let mut msgs = Vec::new();
                for e in errs {
                    if let Some(c) = e.get("code").and_then(|c| c.as_str()) {
                        codes.push(c.to_string());
                    }
                    if let Some(m) = e.get("message").and_then(|m| m.as_str()) {
                        msgs.push(m.to_string());
                    }
                }
                message = msgs.join("; ");
            }
        }
        if message.is_empty() {
            message = StatusCode::from_u16(status)
                .map(|s| s.to_string())
                .unwrap_or_default();
        }
        RegistryFailure {
            status,
            codes,
            message,
        }
    }

    /// Answers a 401 challenge, storing the Authorization header to retry
    /// with.
    async fn authenticate(&mut self, challenge: &str) -> Result<(), RegistryFailure> {
        let (scheme, params) = parse_challenge(challenge);
        let creds = docker_config_auth(&self.reference.registry);
        let basic = |(u, p): &(String, String)| {
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"))
            )
        };
        if scheme == "basic" {
            self.auth = creds.as_ref().map(basic);
            return match self.auth {
                Some(_) => Ok(()),
                None => Err(RegistryFailure {
                    status: 401,
                    codes: vec!["UNAUTHORIZED".into()],
                    message: "the registry requires credentials".into(),
                }),
            };
        }
        let realm = params.get("realm").cloned().unwrap_or_default();
        let mut url = url::Url::parse(&realm).map_err(|e| RegistryFailure {
            status: 401,
            codes: vec![],
            message: format!("the registry named an invalid token realm {realm:?}: {e}"),
        })?;
        {
            let mut q = url.query_pairs_mut();
            if let Some(s) = params.get("service") {
                q.append_pair("service", s);
            }
            q.append_pair(
                "scope",
                &format!("repository:{}:pull", self.reference.repository),
            );
        }
        let mut req = self.http.get(url);
        if let Some(c) = &creds {
            req = req.header(header::AUTHORIZATION, basic(c));
        }
        let resp = req.send().await.map_err(|e| RegistryFailure {
            status: 0,
            codes: vec![],
            message: format!("while fetching a registry token: {e}"),
        })?;
        if !resp.status().is_success() {
            return Err(Self::failure(resp).await);
        }
        let body: serde_json::Value = resp.json().await.map_err(|e| RegistryFailure {
            status: 0,
            codes: vec![],
            message: format!("while reading a registry token: {e}"),
        })?;
        let token = body
            .get("token")
            .or_else(|| body.get("access_token"))
            .and_then(|t| t.as_str())
            .unwrap_or("");
        self.auth = Some(format!("Bearer {token}"));
        Ok(())
    }

    async fn send(
        &mut self,
        method: Method,
        url: &str,
        accept: Option<&str>,
    ) -> Result<reqwest::Response, RegistryFailure> {
        for attempt in 0..2 {
            let mut req = self.http.request(method.clone(), url);
            if let Some(a) = accept {
                req = req.header(header::ACCEPT, a);
            }
            if let Some(auth) = &self.auth {
                req = req.header(header::AUTHORIZATION, auth);
            }
            let resp = req.send().await.map_err(|e| RegistryFailure {
                status: 0,
                codes: vec![],
                message: e.to_string(),
            })?;
            if resp.status() == StatusCode::UNAUTHORIZED && attempt == 0 {
                if let Some(ch) = resp
                    .headers()
                    .get(header::WWW_AUTHENTICATE)
                    .and_then(|v| v.to_str().ok())
                {
                    let ch = ch.to_string();
                    self.authenticate(&ch).await?;
                    continue;
                }
            }
            if !resp.status().is_success() {
                return Err(Self::failure(resp).await);
            }
            return Ok(resp);
        }
        unreachable!("the loop returns on its second attempt")
    }

    /// The digest the reference's tag (or digest) names, by HEAD, falling
    /// back to hashing the manifest when the registry omits the header.
    pub async fn head_digest(&mut self) -> Result<String, RegistryFailure> {
        let url = format!("{}/manifests/{}", self.base(), self.reference.identifier());
        let accept = MANIFEST_ACCEPT.join(", ");
        let resp = self.send(Method::HEAD, &url, Some(&accept)).await?;
        if let Some(d) = resp
            .headers()
            .get("docker-content-digest")
            .and_then(|v| v.to_str().ok())
        {
            return Ok(d.to_string());
        }
        Ok(self.manifest().await?.digest)
    }

    /// The manifest the reference names.
    pub async fn manifest(&mut self) -> Result<Manifest, RegistryFailure> {
        let id = String::from(self.reference.identifier());
        self.manifest_at(&id).await
    }

    pub async fn manifest_at(&mut self, identifier: &str) -> Result<Manifest, RegistryFailure> {
        let url = format!("{}/manifests/{identifier}", self.base());
        let accept = MANIFEST_ACCEPT.join(", ");
        let resp = self.send(Method::GET, &url, Some(&accept)).await?;
        let header_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(';').next().unwrap_or("").trim().to_string());
        let header_digest = resp
            .headers()
            .get("docker-content-digest")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = resp
            .bytes()
            .await
            .map_err(|e| RegistryFailure {
                status: 0,
                codes: vec![],
                message: e.to_string(),
            })?
            .to_vec();
        let body_type = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| {
                v.get("mediaType")
                    .and_then(|m| m.as_str())
                    .map(str::to_string)
            });
        let media_type = body_type.or(header_type).unwrap_or_default();
        let digest = header_digest.unwrap_or_else(|| sha256_digest(&body));
        Ok(Manifest {
            media_type,
            digest,
            body,
        })
    }

    /// A blob of the repository.
    pub async fn blob(&mut self, digest: &str) -> Result<Vec<u8>, RegistryFailure> {
        let url = format!("{}/blobs/{digest}", self.base());
        let resp = self.send(Method::GET, &url, None).await?;
        Ok(resp
            .bytes()
            .await
            .map_err(|e| RegistryFailure {
                status: 0,
                codes: vec![],
                message: e.to_string(),
            })?
            .to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn challenge_parsing() {
        let (scheme, p) = parse_challenge(
            r#"Bearer realm="https://auth.example.com/token",service="registry.example.com",scope="repository:a:pull""#,
        );
        assert_eq!(scheme, "bearer");
        assert_eq!(p["realm"], "https://auth.example.com/token");
        assert_eq!(p["service"], "registry.example.com");
        assert_eq!(p["scope"], "repository:a:pull");
    }

    #[test]
    fn docker_config_lookup() {
        let auth = base64::engine::general_purpose::STANDARD.encode("user:secret");
        let doc = format!(r#"{{"auths":{{"https://registry.example.com":{{"auth":"{auth}"}}}}}}"#);
        assert_eq!(
            auth_from_config(doc.as_bytes(), "registry.example.com"),
            Some(("user".into(), "secret".into()))
        );
        assert_eq!(auth_from_config(doc.as_bytes(), "other.example.com"), None);
    }

    #[test]
    fn not_found_classification() {
        let f = RegistryFailure {
            status: 400,
            codes: vec!["MANIFEST_UNKNOWN".into()],
            message: String::new(),
        };
        assert!(f.is_not_found());
        let f = RegistryFailure {
            status: 403,
            codes: vec!["DENIED".into()],
            message: String::new(),
        };
        assert!(!f.is_not_found());
    }
}

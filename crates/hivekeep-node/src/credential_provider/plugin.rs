//! One credential provider plugin: when it claims an image, running its
//! executable, and caching its answer as it asks.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use chrono::{DateTime, Utc};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use super::config::{parse_duration, SUPPORTED_API_VERSION};
use super::matcher::{matches_image, registry_of};
use crate::clock::{self, SharedClock, SystemClock};
use crate::context::Ctx;

/// Bounds one plugin run.
const EXEC_TIMEOUT: Duration = Duration::from_secs(60);
/// The largest response accepted.
const MAX_RESPONSE_BYTES: usize = 1 << 20;
/// The cache key of a response valid for every image.
const GLOBAL_CACHE_KEY: &str = "global";
/// The shortest a response is cached.
const MIN_CACHE_DURATION: Duration = Duration::from_secs(60);

/// A username and password for a registry.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct AuthConfig {
    /// The username.
    #[serde(default)]
    pub username: String,
    /// The password or token.
    #[serde(default)]
    pub password: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecRequest<'a> {
    kind: &'static str,
    api_version: &'static str,
    image: &'a str,
}

/// A plugin's answer.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecResponse {
    /// The API version it spoke.
    #[serde(default)]
    pub api_version: String,
    /// What the answer is valid for: Image, Registry, or Global.
    #[serde(default)]
    pub cache_key_type: String,
    /// How long it is valid.
    #[serde(default)]
    pub cache_duration: Option<String>,
    /// Credentials by image pattern.
    #[serde(default)]
    pub auth: BTreeMap<String, AuthConfig>,
}

struct CacheEntry {
    auth: BTreeMap<String, AuthConfig>,
    expires_at: DateTime<Utc>,
}

/// A configured plugin.
pub struct Plugin {
    /// Its name, which is also its executable's.
    pub name: String,
    /// Its executable.
    pub path: PathBuf,
    args: Vec<String>,
    env: Vec<(String, String)>,
    match_images: Vec<String>,
    default_cache_duration: Duration,
    clock: SharedClock,
    cache: Mutex<HashMap<String, CacheEntry>>,
}

impl Plugin {
    pub(super) fn new(
        name: String,
        path: PathBuf,
        args: Vec<String>,
        env: Vec<(String, String)>,
        match_images: Vec<String>,
        default_cache_duration: Duration,
    ) -> Self {
        Plugin {
            name,
            path,
            args,
            env,
            match_images,
            default_cache_duration,
            clock: std::sync::Arc::new(SystemClock),
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Reads cache expiry against `clock` instead of the system clock.
    pub(super) fn set_clock(&mut self, clock: SharedClock) {
        self.clock = clock;
    }

    /// The executable's arguments.
    pub fn args(&self) -> &[String] {
        &self.args
    }

    /// How long a response is cached when it names no duration.
    pub fn default_cache_duration(&self) -> Duration {
        self.default_cache_duration
    }

    #[cfg(test)]
    pub(super) fn cache_len(&self) -> usize {
        self.cache.lock().len()
    }

    /// Whether the plugin claims `image`.
    pub fn claims(&self, image: &str) -> Result<bool, String> {
        for m in &self.match_images {
            if matches_image(m, image)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Credentials for `image`, from the cache or a run of the plugin, and
    /// whether they came from the cache. A run is abandoned (and its
    /// process killed) when `ctx` ends.
    pub async fn provide(
        &self,
        ctx: &Ctx,
        image: &str,
    ) -> Result<(BTreeMap<String, AuthConfig>, bool), String> {
        if let Some(a) = self.lookup(image) {
            return Ok((a, true));
        }
        let resp = ctx
            .run(self.exec(image))
            .await
            .map_err(|e| format!("credential provider {:?}: {e}", self.name))??;
        let auth = resp.auth.clone();
        self.store(image, resp);
        Ok((auth, false))
    }

    fn lookup(&self, image: &str) -> Option<BTreeMap<String, AuthConfig>> {
        let mut c = self.cache.lock();
        let now = self.clock.now();
        for key in [image, registry_of(image), GLOBAL_CACHE_KEY] {
            match c.get(key) {
                Some(e) if now < e.expires_at => return Some(e.auth.clone()),
                Some(_) => {
                    c.remove(key);
                }
                None => {}
            }
        }
        None
    }

    pub(super) fn store(&self, image: &str, resp: ExecResponse) {
        let key = match resp.cache_key_type.as_str() {
            "Image" => image.to_string(),
            "Registry" => registry_of(image).to_string(),
            "Global" => GLOBAL_CACHE_KEY.to_string(),
            other => {
                tracing::warn!(provider = %self.name, cache_key_type = other, "Image credential provider returned an unknown cacheKeyType; not caching");
                return;
            }
        };
        let mut d = resp
            .cache_duration
            .as_deref()
            .and_then(|s| parse_duration(s).ok())
            .unwrap_or(self.default_cache_duration);
        if d.is_zero() {
            d = MIN_CACHE_DURATION;
        }
        let mut c = self.cache.lock();
        let now = self.clock.now();
        c.retain(|_, e| now < e.expires_at);
        c.insert(
            key,
            CacheEntry {
                auth: resp.auth,
                expires_at: clock::add(now, d),
            },
        );
    }

    async fn exec(&self, image: &str) -> Result<ExecResponse, String> {
        let req = serde_json::to_vec(&ExecRequest {
            kind: "CredentialProviderRequest",
            api_version: SUPPORTED_API_VERSION,
            image,
        })
        .map_err(|e| format!("while encoding credential provider request: {e}"))?;
        let mut cmd = tokio::process::Command::new(&self.path);
        cmd.args(&self.args)
            .envs(self.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let run = async {
            let mut child = cmd
                .spawn()
                .map_err(|e| format!("while running credential provider {:?}: {e}", self.name))?;
            if let Some(mut stdin) = child.stdin.take() {
                // A plugin that exits without reading its request still
                // gets its exit status and stderr reported.
                if let Err(e) = stdin.write_all(&req).await {
                    if e.kind() != std::io::ErrorKind::BrokenPipe {
                        return Err(e.to_string());
                    }
                }
            }
            child.wait_with_output().await.map_err(|e| e.to_string())
        };
        let out = tokio::time::timeout(EXEC_TIMEOUT, run)
            .await
            .map_err(|_| format!("credential provider {:?} timed out", self.name))??;
        let stderr = truncate(&String::from_utf8_lossy(&out.stderr));
        if !out.status.success() {
            return Err(format!(
                "while running credential provider {:?}: {} (stderr: {stderr})",
                self.name, out.status
            ));
        }
        if out.stdout.len() > MAX_RESPONSE_BYTES {
            return Err(format!(
                "credential provider {:?} returned {} bytes, over the {MAX_RESPONSE_BYTES} byte limit",
                self.name,
                out.stdout.len()
            ));
        }
        let resp: ExecResponse = serde_json::from_slice(&out.stdout).map_err(|e| {
            format!(
                "while decoding response from credential provider {:?}: {e} (stderr: {stderr})",
                self.name
            )
        })?;
        if resp.api_version != SUPPORTED_API_VERSION {
            return Err(format!(
                "credential provider {:?} responded with apiVersion {:?}, want {SUPPORTED_API_VERSION:?}",
                self.name, resp.api_version
            ));
        }
        Ok(resp)
    }
}

fn truncate(s: &str) -> String {
    const LIMIT: usize = 2048;
    let s = s.trim();
    if s.len() <= LIMIT {
        return s.to_string();
    }
    let mut end = LIMIT;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}... (truncated)", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plugin(path: PathBuf) -> Plugin {
        Plugin::new(
            "p".into(),
            path,
            vec![],
            vec![],
            vec!["*.example.io".into()],
            Duration::from_secs(600),
        )
    }

    #[test]
    fn caches_by_key_type() {
        let p = plugin(PathBuf::from("/nonexistent"));
        let mut auth = BTreeMap::new();
        auth.insert(
            "*.example.io".into(),
            AuthConfig {
                username: "u".into(),
                password: "p".into(),
            },
        );
        p.store(
            "a.example.io/img",
            ExecResponse {
                api_version: SUPPORTED_API_VERSION.into(),
                cache_key_type: "Registry".into(),
                cache_duration: None,
                auth: auth.clone(),
            },
        );
        assert_eq!(
            p.lookup("a.example.io/other").unwrap(),
            auth,
            "registry-wide"
        );
        assert!(p.lookup("b.example.io/img").is_none());
        p.store(
            "x",
            ExecResponse {
                api_version: SUPPORTED_API_VERSION.into(),
                cache_key_type: "Bogus".into(),
                cache_duration: None,
                auth: BTreeMap::new(),
            },
        );
        assert!(p.claims("c.example.io/img").unwrap());
        assert!(!p.claims("quay.io/img").unwrap());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runs_the_executable() {
        let d = tempfile::tempdir().unwrap();
        let script = d.path().join("p");
        std::fs::write(
            &script,
            "#!/bin/sh\ncat >/dev/null\necho '{\"apiVersion\":\"credentialprovider.kubelet.k8s.io/v1\",\"cacheKeyType\":\"Image\",\"auth\":{\"a.example.io\":{\"username\":\"u\",\"password\":\"s\"}}}'\n",
        )
        .unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let p = plugin(script);
        let ctx = Ctx::background();
        let (auth, cached) = p.provide(&ctx, "a.example.io/img").await.unwrap();
        assert!(!cached);
        assert_eq!(auth["a.example.io"].password, "s");
        let (_, cached) = p.provide(&ctx, "a.example.io/img").await.unwrap();
        assert!(cached);
    }

    #[tokio::test]
    async fn a_missing_executable_fails() {
        let p = plugin(PathBuf::from("/nonexistent/provider"));
        assert!(p
            .provide(&Ctx::background(), "a.example.io/img")
            .await
            .is_err());
    }
}

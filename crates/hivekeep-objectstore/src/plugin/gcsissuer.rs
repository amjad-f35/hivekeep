//! GCS tokens downscoped to an actor's grants: the GCS half of the control
//! plugin's credential exchange.
//!
//! The source credential is a token for the snapshot transfer service
//! account, which the pod's own identity impersonates through the IAM
//! Credentials API, so audit logs name that account. STS exchanges it with
//! a Credential Access Boundary for a token GCS limits to the boundary. STS
//! reports no lifetime for that token, so it expires with its source.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use http::Method;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::json;
use url::Url;

use super::actorscope::Grant;
use super::credentials::{lifetime_left, Token, MIN_SERVE_LIFETIME};
use super::gcsboundary::{from_grants, MAX_RULES};
use crate::error::{Error, ErrorKind, Result};
use crate::gcp::{AccessToken, Cached, TokenSource, CLOUD_PLATFORM_SCOPE};
use crate::http::{Request, ReqwestTransport, Transport};
use crate::lru::LruCache;

/// The STS token endpoint.
pub const STS_ENDPOINT: &str = "https://sts.googleapis.com/v1/token";
/// The IAM Credentials endpoint.
pub const IAM_CREDENTIALS_ENDPOINT: &str = "https://iamcredentials.googleapis.com/";
/// The per-actor cache's bound; a miss costs an exchange, never correctness.
pub const DEFAULT_CACHE_SIZE: usize = 100_000;
/// Bounds one STS exchange.
pub const STS_TIMEOUT: Duration = Duration::from_secs(30);
/// The source token's requested lifetime, the longest the IAM Credentials
/// API grants without an org policy change.
pub const TRANSFER_TOKEN_LIFETIME: Duration = Duration::from_secs(3600);
/// How much life a source token has left when the next is fetched; minted
/// tokens start with at least this long.
pub const SOURCE_REFRESH_MARGIN: Duration = Duration::from_secs(40 * 60);
/// Bounds one IAM Credentials call.
pub const IAM_TIMEOUT: Duration = Duration::from_secs(5);

/// Mints a credential limited to `grants` for the actor. `force` skips any
/// cache. A status error other than UNAVAILABLE is a failure the same
/// request would hit again; a cancellation keeps the caller's code; anything
/// else is reported as UNAVAILABLE.
#[async_trait]
pub trait Issuer: Send + Sync + 'static {
    /// Issues the token.
    async fn issue(&self, actor_uid: &str, grants: &[Grant], force: bool) -> Result<Token>;
}

#[derive(Clone)]
struct Entry {
    token: Token,
    grants: Vec<Grant>,
}

/// Exchanges the transfer account's token for per-actor downscoped tokens,
/// cached by actor UID.
pub struct GcsIssuer {
    source: Arc<dyn TokenSource>,
    sts: Url,
    transport: Arc<dyn Transport>,
    now: Arc<dyn Fn() -> SystemTime + Send + Sync>,
    cache: Mutex<LruCache<String, Entry>>,
}

/// An endpoint, the credential calling it and the transport to it.
pub type Endpoint = (Url, Arc<dyn TokenSource>, Arc<dyn Transport>);

/// How a [`GcsIssuer`] is built; tests replace the parts.
pub struct IssuerOptions {
    /// The transfer account's token; `None` impersonates the account.
    pub source: Option<Arc<dyn TokenSource>>,
    /// The IAM Credentials endpoint and the pod identity calling it.
    pub iam: Option<Endpoint>,
    /// The STS endpoint and transport.
    pub sts: Option<(Url, Arc<dyn Transport>)>,
    /// The clock.
    pub now: Option<Arc<dyn Fn() -> SystemTime + Send + Sync>>,
    /// The cache bound; below one is one.
    pub cache_size: usize,
}

impl Default for IssuerOptions {
    fn default() -> Self {
        IssuerOptions {
            source: None,
            iam: None,
            sts: None,
            now: None,
            cache_size: DEFAULT_CACHE_SIZE,
        }
    }
}

impl GcsIssuer {
    /// An issuer whose source is a token for the service account `account`,
    /// impersonated with the pod's credential. Tests that replace the
    /// source pass an empty account.
    pub fn new(account: &str, opts: IssuerOptions) -> Result<Self> {
        let source = match opts.source {
            Some(s) => s,
            None => {
                let (base, auth, transport) = match opts.iam {
                    Some(i) => i,
                    None => (
                        Url::parse(IAM_CREDENTIALS_ENDPOINT).expect("valid URL"),
                        crate::gcp::default_credentials()?,
                        Arc::new(ReqwestTransport::new(Some(IAM_TIMEOUT))?) as Arc<dyn Transport>,
                    ),
                };
                transfer_account_source(account, base, auth, transport)?
            }
        };
        let (sts, transport) = match opts.sts {
            Some(s) => s,
            None => (
                Url::parse(STS_ENDPOINT).expect("valid URL"),
                Arc::new(ReqwestTransport::new(Some(STS_TIMEOUT))?) as Arc<dyn Transport>,
            ),
        };
        Ok(GcsIssuer {
            source,
            sts,
            transport,
            now: opts.now.unwrap_or_else(|| Arc::new(SystemTime::now)),
            cache: Mutex::new(LruCache::new(opts.cache_size)),
        })
    }

    /// Cached actors, for tests.
    pub fn cache_len(&self) -> usize {
        self.cache.lock().len()
    }

    async fn mint(&self, grants: &[Grant]) -> Result<Token> {
        let rules = from_grants(grants).map_err(|e| {
            Error::from(tonic::Status::internal(format!(
                "while building the access boundary: {e}"
            )))
        })?;
        if rules.is_empty() || rules.len() > MAX_RULES {
            return Err(tonic::Status::internal(format!(
                "while building the access boundary: the number of rules ({}) must be between 1 and {MAX_RULES}; greater than {MAX_RULES} is refused",
                rules.len()
            ))
            .into());
        }
        let src = self
            .source
            .token()
            .await
            .map_err(|e| e.context("while getting the source token"))?
            .ok_or_else(|| Error::other("the source credential gave no token"))?;
        let options = json!({ "accessBoundary": { "accessBoundaryRules": rules } }).to_string();
        let body = form_urlencoded::Serializer::new(String::new())
            .append_pair(
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            )
            .append_pair(
                "subject_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            )
            .append_pair(
                "requested_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            )
            .append_pair("subject_token", &src.token)
            .append_pair("options", &options)
            .finish();
        let req = Request::new(Method::POST, self.sts.clone())
            .header("content-type", "application/x-www-form-urlencoded")
            .body(body);
        let resp = self
            .transport
            .send(req)
            .await
            .map_err(|e| Error::other(format!("while exchanging at STS: {e}")))?;
        let status = resp.status.as_u16();
        let raw = resp
            .bytes()
            .await
            .map_err(|e| Error::other(format!("while exchanging at STS: {e}")))?;
        if status != 200 {
            return Err(Error::other(format!(
                "while exchanging at STS: status code {status}: {}",
                String::from_utf8_lossy(&raw[..raw.len().min(512)])
            )));
        }
        #[derive(Deserialize)]
        struct StsResponse {
            #[serde(default)]
            access_token: String,
            #[serde(default)]
            expires_in: Option<u64>,
        }
        let r: StsResponse = serde_json::from_slice(&raw)
            .map_err(|e| Error::other(format!("while exchanging at STS: {e}")))?;
        if r.access_token.is_empty() {
            return Err(Error::other("STS returned no access token"));
        }
        let expiry = match (r.expires_in, src.expiry) {
            (Some(s), _) => Some((self.now)() + Duration::from_secs(s)),
            (None, e) => e,
        };
        let Some(expiry) = expiry else {
            // Without an expiry the node plugin could not know when to stop
            // using the token.
            return Err(Error::other("downscoped token has no expiry"));
        };
        Ok(Token {
            access_token: r.access_token,
            expiry,
        })
    }
}

/// Whether a token minted for `cached` serves a request for `requested`:
/// each requested prefix must be there with the same or higher access.
fn covers(cached: &[Grant], requested: &[Grant]) -> bool {
    requested.iter().all(|r| {
        cached
            .iter()
            .any(|c| c.prefix == r.prefix && c.access >= r.access)
    })
}

#[async_trait]
impl Issuer for GcsIssuer {
    /// A cached token is returned while it covers the grants and has at
    /// least [`MIN_SERVE_LIFETIME`] left, unless `force`. Grants narrow when
    /// the actor's tag drops out after its first suspend (still covered, a
    /// hit) and widen when a never-suspended actor is repointed (not
    /// covered, a mint), so the common path costs nothing and a repoint
    /// never gets a token GCS would refuse. A minted token shorter than the
    /// floor is handed out but not cached.
    async fn issue(&self, actor_uid: &str, grants: &[Grant], force: bool) -> Result<Token> {
        if grants.is_empty() {
            // Before the cache: any cached token covers nothing, but an actor
            // with no grants gets nothing.
            return Err(tonic::Status::internal("actor has no storage grants").into());
        }
        if !force {
            if let Some(e) = self.cache.lock().get(&actor_uid.to_string()) {
                if covers(&e.grants, grants)
                    && lifetime_left(&e.token, (self.now)()) >= MIN_SERVE_LIFETIME
                {
                    return Ok(e.token);
                }
            }
        }
        let tok = self.mint(grants).await?;
        if lifetime_left(&tok, (self.now)()) >= MIN_SERVE_LIFETIME {
            self.cache.lock().add(
                actor_uid.to_string(),
                Entry {
                    token: tok.clone(),
                    grants: grants.to_vec(),
                },
            );
        }
        Ok(tok)
    }
}

/// Tokens for the transfer account from the IAM Credentials API on every
/// call; the caller is the pod's identity, which needs the token creator
/// role on the account.
struct TransferAccount {
    base: Url,
    name: String,
    auth: Arc<dyn TokenSource>,
    transport: Arc<dyn Transport>,
}

#[async_trait]
impl TokenSource for TransferAccount {
    async fn token(&self) -> Result<Option<AccessToken>> {
        let url = self
            .base
            .join(&format!("v1/{}:generateAccessToken", self.name))
            .map_err(|e| Error::other(e.to_string()))?;
        let body = json!({
            "scope": [CLOUD_PLATFORM_SCOPE],
            "lifetime": format!("{}s", TRANSFER_TOKEN_LIFETIME.as_secs()),
        })
        .to_string();
        let mut req = Request::new(Method::POST, url)
            .header("content-type", "application/json")
            .body(body);
        if let Some(t) = self.auth.token().await? {
            req = req.header("authorization", format!("Bearer {}", t.token));
        }
        let call = async {
            let resp = self.transport.send(req).await?;
            let status = resp.status.as_u16();
            let raw = resp.bytes().await?;
            Ok::<_, Error>((status, raw))
        };
        let (status, raw) = tokio::time::timeout(IAM_TIMEOUT, call)
            .await
            .map_err(|_| Error::deadline_exceeded())
            .and_then(|r| r)
            .map_err(|e| e.context(format!("while getting a token for {}", self.name)))?;
        if status != 200 {
            return Err(Error::other(format!(
                "while getting a token for {}: the IAM Credentials API answered HTTP {status}",
                self.name
            )));
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Generated {
            #[serde(default)]
            access_token: String,
            #[serde(default)]
            expire_time: String,
        }
        let g: Generated = serde_json::from_slice(&raw)
            .map_err(|e| Error::other(format!("while getting a token for {}: {e}", self.name)))?;
        if g.access_token.is_empty() {
            return Err(Error::other(format!(
                "the IAM Credentials API returned no token for {}",
                self.name
            )));
        }
        let expiry = chrono::DateTime::parse_from_rfc3339(&g.expire_time).map_err(|e| {
            Error::other(format!(
                "the IAM Credentials API returned expiry {:?} for {}: {e}",
                g.expire_time, self.name
            ))
        })?;
        Ok(Some(AccessToken {
            token: g.access_token,
            expiry: Some(SystemTime::from(expiry)),
        }))
    }
}

/// Returns the last token its source gave while the source fails and that
/// token outlasts an STS call, so a brief IAM Credentials outage does not
/// fail exchanges.
struct Fallback {
    src: Arc<dyn TokenSource>,
    last: Mutex<Option<AccessToken>>,
}

#[async_trait]
impl TokenSource for Fallback {
    async fn token(&self) -> Result<Option<AccessToken>> {
        match self.src.token().await {
            Ok(t) => {
                *self.last.lock() = t.clone();
                Ok(t)
            }
            Err(e) => {
                let last = self.last.lock().clone();
                if let Some(t) = last {
                    let left = t
                        .expiry
                        .and_then(|x| x.duration_since(SystemTime::now()).ok())
                        .unwrap_or(Duration::ZERO);
                    if left > STS_TIMEOUT {
                        return Ok(Some(t));
                    }
                }
                Err(e)
            }
        }
    }
}

/// The source credential: tokens for `account`, fetched again once less
/// than [`SOURCE_REFRESH_MARGIN`] is left, with the last one kept through a
/// failed fetch.
fn transfer_account_source(
    account: &str,
    base: Url,
    auth: Arc<dyn TokenSource>,
    transport: Arc<dyn Transport>,
) -> Result<Arc<dyn TokenSource>> {
    if account.is_empty() {
        return Err(Error::other("no transfer service account to impersonate"));
    }
    let src: Arc<dyn TokenSource> = Arc::new(TransferAccount {
        base,
        name: format!("projects/-/serviceAccounts/{account}"),
        auth,
        transport,
    });
    Ok(Arc::new(Fallback {
        src: Arc::new(Cached::new(src, SOURCE_REFRESH_MARGIN)),
        last: Mutex::new(None),
    }))
}

/// Whether an issuer error should keep its code.
pub fn issue_code(err: &Error) -> tonic::Code {
    match err.kind() {
        ErrorKind::Canceled => tonic::Code::Cancelled,
        ErrorKind::DeadlineExceeded => tonic::Code::DeadlineExceeded,
        ErrorKind::Status(s) if s.code() != tonic::Code::Unknown => s.code(),
        _ => tonic::Code::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcp::{Anonymous, StaticToken};
    use crate::http::{transport_fn, Response};
    use crate::plugin::actorscope::tests::prefix;
    use crate::plugin::actorscope::Access;
    use crate::plugin::gcsboundary::AccessBoundaryRule;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn rw() -> Grant {
        Grant {
            prefix: prefix("gs://snaps/root/hives/team-a/actors/9f1e"),
            access: Access::ReadWrite,
        }
    }
    fn ro() -> Grant {
        Grant {
            prefix: prefix("gs://snaps/root/hives/hivekeep-golden/tags/7a3c"),
            access: Access::ReadOnly,
        }
    }
    fn grants() -> Vec<Grant> {
        vec![rw(), ro()]
    }

    fn t0() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_791_885_600)
    }

    /// Records the last exchange and answers with a token.
    #[derive(Default)]
    struct FakeSts {
        calls: AtomicUsize,
        status: u16,
        body: String,
        last_url: Mutex<String>,
        last: Mutex<HashMap<String, String>>,
    }

    fn sts_transport(sts: Arc<FakeSts>) -> Arc<dyn Transport> {
        transport_fn(move |req: Request| {
            let sts = sts.clone();
            Box::pin(async move {
                sts.calls.fetch_add(1, Ordering::SeqCst);
                *sts.last_url.lock() = req.url.to_string();
                *sts.last.lock() = form_urlencoded::parse(&req.body).into_owned().collect();
                let body = if sts.body.is_empty() {
                    r#"{"access_token":"downscoped-xxxxxxxx","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer"}"#.to_string()
                } else {
                    sts.body.clone()
                };
                Ok(Response::new(
                    if sts.status == 0 { 200 } else { sts.status },
                    body,
                ))
            })
        })
    }

    fn static_source(expiry: Option<SystemTime>) -> Arc<dyn TokenSource> {
        Arc::new(StaticToken(AccessToken {
            token: "source-token".into(),
            expiry,
        }))
    }

    fn issuer(
        sts: Arc<FakeSts>,
        src: Arc<dyn TokenSource>,
        now: Arc<dyn Fn() -> SystemTime + Send + Sync>,
    ) -> GcsIssuer {
        GcsIssuer::new(
            "",
            IssuerOptions {
                source: Some(src),
                sts: Some((Url::parse(STS_ENDPOINT).unwrap(), sts_transport(sts))),
                now: Some(now),
                cache_size: 2,
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn fixed(t: SystemTime) -> Arc<dyn Fn() -> SystemTime + Send + Sync> {
        Arc::new(move || t)
    }

    #[tokio::test]
    async fn issue_exchanges_with_the_boundary() {
        let expiry = t0() + Duration::from_secs(3600);
        let sts = Arc::new(FakeSts::default());
        let i = issuer(sts.clone(), static_source(Some(expiry)), fixed(t0()));
        let tok = i.issue("9f1e", &grants(), false).await.unwrap();
        assert!(tok.access_token.starts_with("downscoped-"));
        assert_eq!(tok.expiry, expiry, "expires with the source token");
        assert_eq!(*sts.last_url.lock(), STS_ENDPOINT);
        let last = sts.last.lock().clone();
        for (k, v) in [
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            (
                "subject_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            (
                "requested_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            ("subject_token", "source-token"),
        ] {
            assert_eq!(last.get(k).map(String::as_str), Some(v), "form {k}");
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Boundary {
            access_boundary_rules: Vec<AccessBoundaryRule>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Options {
            access_boundary: Boundary,
        }
        let o: Options = serde_json::from_str(&last["options"]).unwrap();
        assert_eq!(
            o.access_boundary.access_boundary_rules,
            from_grants(&grants()).unwrap()
        );
    }

    #[tokio::test]
    async fn issue_caches_per_actor_until_near_expiry() {
        let clock = Arc::new(Mutex::new(t0()));
        let c = clock.clone();
        let sts = Arc::new(FakeSts::default());
        let i = issuer(
            sts.clone(),
            static_source(Some(t0() + Duration::from_secs(3600))),
            Arc::new(move || *c.lock()),
        );
        let first = i.issue("9f1e", &grants(), false).await.unwrap();
        let second = i.issue("9f1e", &grants(), false).await.unwrap();
        assert_eq!(sts.calls.load(Ordering::SeqCst), 1);
        assert_eq!(first, second);
        i.issue("other", &grants(), false).await.unwrap();
        assert_eq!(sts.calls.load(Ordering::SeqCst), 2);
        *clock.lock() =
            t0() + Duration::from_secs(3600) - MIN_SERVE_LIFETIME + Duration::from_secs(1);
        i.issue("9f1e", &grants(), false).await.unwrap();
        assert_eq!(sts.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn issue_force_bypasses_the_cache() {
        let sts = Arc::new(FakeSts::default());
        let i = issuer(
            sts.clone(),
            static_source(Some(t0() + Duration::from_secs(3600))),
            fixed(t0()),
        );
        for force in [false, true] {
            i.issue("9f1e", &grants(), force).await.unwrap();
        }
        assert_eq!(sts.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn issue_cache_is_bounded() {
        let sts = Arc::new(FakeSts::default());
        let i = issuer(
            sts.clone(),
            static_source(Some(t0() + Duration::from_secs(3600))),
            fixed(t0()),
        );
        for uid in ["a", "b", "c", "a"] {
            i.issue(uid, &grants(), false).await.unwrap();
        }
        assert_eq!((sts.calls.load(Ordering::SeqCst), i.cache_len()), (4, 2));
    }

    #[tokio::test]
    async fn issue_serves_the_cached_token_when_it_covers_the_grants() {
        let rw_other = Grant {
            prefix: prefix("gs://other/root/hives/team-a/actors/9f1e"),
            access: Access::ReadWrite,
        };
        let ro_own = Grant {
            prefix: rw().prefix,
            access: Access::ReadOnly,
        };
        for (name, first, second, mints) in [
            ("same grants", grants(), grants(), 1),
            (
                "tag dropped after the first suspend",
                grants(),
                vec![rw()],
                1,
            ),
            (
                "read-only covered by read-write",
                grants(),
                vec![ro_own.clone()],
                1,
            ),
            (
                "repoint adds a location",
                grants(),
                vec![rw(), rw_other.clone(), ro()],
                2,
            ),
            (
                "read-write not covered by read-only",
                vec![ro_own.clone()],
                vec![rw()],
                2,
            ),
        ] {
            let sts = Arc::new(FakeSts::default());
            let i = issuer(
                sts.clone(),
                static_source(Some(t0() + Duration::from_secs(3600))),
                fixed(t0()),
            );
            i.issue("9f1e", &first, false).await.unwrap();
            i.issue("9f1e", &second, false).await.unwrap();
            assert_eq!(sts.calls.load(Ordering::SeqCst), mints, "{name}");
        }
    }

    #[tokio::test]
    async fn issue_refuses_empty_grants_even_when_cached() {
        let sts = Arc::new(FakeSts::default());
        let i = issuer(
            sts.clone(),
            static_source(Some(t0() + Duration::from_secs(3600))),
            fixed(t0()),
        );
        i.issue("9f1e", &grants(), false).await.unwrap();
        assert!(i.issue("9f1e", &[], false).await.is_err());
        assert_eq!(sts.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn issue_stops_when_the_caller_gives_up() {
        let hang = transport_fn(|_req: Request| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(3)).await;
                Ok(Response::new(200, "{}"))
            })
        });
        let i = GcsIssuer::new(
            "",
            IssuerOptions {
                source: Some(static_source(Some(t0() + Duration::from_secs(3600)))),
                sts: Some((Url::parse(STS_ENDPOINT).unwrap(), hang)),
                now: Some(fixed(t0())),
                ..Default::default()
            },
        )
        .unwrap();
        let start = std::time::Instant::now();
        let r = tokio::time::timeout(
            Duration::from_millis(100),
            i.issue("9f1e", &grants(), false),
        )
        .await;
        assert!(r.is_err(), "issue succeeded against a hanging STS");
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn cache_size_zero_is_bounded() {
        let sts = Arc::new(FakeSts::default());
        let i = GcsIssuer::new(
            "",
            IssuerOptions {
                source: Some(static_source(Some(t0() + Duration::from_secs(3600)))),
                sts: Some((Url::parse(STS_ENDPOINT).unwrap(), sts_transport(sts))),
                now: Some(fixed(t0())),
                cache_size: 0,
                ..Default::default()
            },
        )
        .unwrap();
        for uid in ["a", "b"] {
            i.issue(uid, &grants(), false).await.unwrap();
        }
        assert_eq!(i.cache_len(), 1);
    }

    #[tokio::test]
    async fn issue_does_not_cache_a_short_lived_token() {
        let sts = Arc::new(FakeSts::default());
        let i = issuer(
            sts.clone(),
            static_source(Some(t0() + MIN_SERVE_LIFETIME - Duration::from_secs(60))),
            fixed(t0()),
        );
        for _ in 0..2 {
            assert!(!i
                .issue("9f1e", &grants(), false)
                .await
                .unwrap()
                .access_token
                .is_empty());
        }
        assert_eq!((sts.calls.load(Ordering::SeqCst), i.cache_len()), (2, 0));
    }

    fn many_grants(n: usize) -> Vec<Grant> {
        (0..n)
            .map(|i| Grant {
                prefix: prefix(&format!("gs://b/p{i}")),
                access: Access::ReadOnly,
            })
            .collect()
    }

    #[tokio::test]
    async fn issue_errors() {
        let ok = Some(t0() + Duration::from_secs(3600));
        for (name, status, body, expiry, gs, want, code) in [
            (
                "source without expiry",
                0,
                "",
                None,
                grants(),
                "no expiry",
                tonic::Code::Unavailable,
            ),
            (
                "STS refuses",
                400,
                r#"{"error":"invalid_grant"}"#,
                ok,
                grants(),
                "400",
                tonic::Code::Unavailable,
            ),
            (
                "STS answers without a token",
                0,
                "{}",
                ok,
                grants(),
                "no access token",
                tonic::Code::Unavailable,
            ),
            (
                "no grants",
                0,
                "",
                ok,
                vec![],
                "no storage grants",
                tonic::Code::Internal,
            ),
            (
                "too many grants",
                0,
                "",
                ok,
                many_grants(11),
                "greater than 10",
                tonic::Code::Internal,
            ),
        ] {
            let sts = Arc::new(FakeSts {
                status,
                body: body.into(),
                ..Default::default()
            });
            let i = issuer(sts, static_source(expiry), fixed(t0()));
            let e = i.issue("9f1e", &gs, false).await.unwrap_err();
            assert!(e.to_string().contains(want), "{name}: {e}");
            assert_eq!(issue_code(&e), code, "{name}");
        }
    }

    const ACCOUNT: &str = "hivekeep-snapshot-transfer@proj.iam.gserviceaccount.com";

    /// Answers generateAccessToken with a token that has `left` remaining.
    struct FakeIam {
        calls: AtomicUsize,
        left: Mutex<i64>,
        status: Mutex<u16>,
    }

    fn iam(left: i64) -> (Arc<FakeIam>, Endpoint) {
        let f = Arc::new(FakeIam {
            calls: AtomicUsize::new(0),
            left: Mutex::new(left),
            status: Mutex::new(0),
        });
        let f2 = f.clone();
        let t = transport_fn(move |req: Request| {
            let f = f2.clone();
            Box::pin(async move {
                f.calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    req.url.path(),
                    format!("/v1/projects/-/serviceAccounts/{ACCOUNT}:generateAccessToken")
                );
                let v: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                assert_eq!(v["scope"], json!([CLOUD_PLATFORM_SCOPE]));
                assert_eq!(v["lifetime"], "3600s");
                let status = *f.status.lock();
                if status != 0 {
                    return Ok(Response::new(
                        status,
                        r#"{"error":{"code":403,"message":"denied"}}"#,
                    ));
                }
                let exp = chrono::Utc::now() + chrono::Duration::seconds(*f.left.lock());
                Ok(Response::new(
                    200,
                    json!({"accessToken": "transfer-token", "expireTime": exp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)}).to_string(),
                ))
            })
        });
        (
            f,
            (
                Url::parse("http://iam.test/").unwrap(),
                Arc::new(Anonymous),
                t,
            ),
        )
    }

    #[tokio::test]
    async fn transfer_account_fetches_again_inside_the_margin() {
        for (name, left, want) in [
            ("45 minutes left is reused", 45 * 60, 1),
            ("39 minutes left is fetched again", 39 * 60, 2),
        ] {
            let (f, (base, auth, t)) = iam(left);
            let src = transfer_account_source(ACCOUNT, base, auth, t).unwrap();
            for _ in 0..2 {
                assert_eq!(src.token().await.unwrap().unwrap().token, "transfer-token");
            }
            assert_eq!(f.calls.load(Ordering::SeqCst), want, "{name}");
        }
    }

    #[tokio::test]
    async fn transfer_account_names_the_account_when_refused() {
        let (f, (base, auth, t)) = iam(3600);
        *f.status.lock() = 403;
        let src = transfer_account_source(ACCOUNT, base, auth, t).unwrap();
        let e = src.token().await.unwrap_err();
        assert!(e.to_string().contains(ACCOUNT), "{e}");
    }

    #[tokio::test]
    async fn transfer_account_keeps_the_last_token_when_a_fetch_fails() {
        for (name, left, want_err) in [
            ("an unexpired token is returned", 39 * 60, false),
            (
                "a token that would expire during the STS call is not",
                20,
                true,
            ),
            ("an expired token is not", -60, true),
        ] {
            let (f, (base, auth, t)) = iam(left);
            let src = transfer_account_source(ACCOUNT, base, auth, t).unwrap();
            src.token().await.unwrap();
            *f.status.lock() = 503;
            let r = src.token().await;
            assert_eq!(r.is_err(), want_err, "{name}: {r:?}");
            if !want_err {
                assert_eq!(r.unwrap().unwrap().token, "transfer-token");
            }
            assert_eq!(f.calls.load(Ordering::SeqCst), 2, "{name}");
        }
    }

    #[test]
    fn new_needs_a_transfer_account() {
        let (_, iam) = iam(3600);
        let e = GcsIssuer::new(
            "",
            IssuerOptions {
                iam: Some(iam),
                ..Default::default()
            },
        )
        .err()
        .unwrap();
        assert!(e.to_string().contains("transfer service account"), "{e}");
    }

    #[tokio::test]
    async fn issue_downscopes_the_transfer_accounts_token() {
        let (f, iam) = iam(3600);
        let sts = Arc::new(FakeSts::default());
        let i = GcsIssuer::new(
            ACCOUNT,
            IssuerOptions {
                iam: Some(iam),
                sts: Some((
                    Url::parse(STS_ENDPOINT).unwrap(),
                    sts_transport(sts.clone()),
                )),
                ..Default::default()
            },
        )
        .unwrap();
        let tok = i.issue("9f1e", &grants(), false).await.unwrap();
        assert_eq!(sts.last.lock()["subject_token"], "transfer-token");
        let left = tok.expiry.duration_since(SystemTime::now()).unwrap();
        assert!(
            left >= SOURCE_REFRESH_MARGIN,
            "downscoped token has {left:?} left"
        );
        assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    }
}

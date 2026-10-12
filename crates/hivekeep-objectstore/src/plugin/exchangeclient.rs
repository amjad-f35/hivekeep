//! The GCS node plugin's side of the credential exchange: for each transfer
//! it trades the actor JWT for a GCS token scoped to the actor's snapshot
//! prefixes, caches the token and the storage client built on it under that
//! JWT, and hands the client to the transfer.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use parking_lot::Mutex;
use tonic::transport::Channel;
use tonic::Status;

use super::credentials::{lifetime_left, Credentials, Token, MIN_SERVE_LIFETIME};
use crate::error::{Error, Result};
use crate::gcp::{AccessToken, StaticToken};
use crate::gcs::{GcsClient, GcsOptions};
use crate::lru::LruCache;
use crate::proto::exchange::v1::credential_exchange_client::CredentialExchangeClient;
use crate::proto::exchange::v1::{ExchangeActorJwtRequest, ExchangeActorJwtResponse};
use crate::storage::ObjectStorage;
use crate::tls::{channel, ClientFiles};

/// The cache bound: one entry per JWT the keeper holds, which is one per
/// actor the node transferred for within a JWT lifetime. Eviction costs only
/// an exchange and a new client.
pub const DEFAULT_CACHE_SIZE: usize = 1_000;

/// How the exchange is reached.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// The exchange's host and port: the hivekeep-api Service.
    pub address: String,
    /// The CA bundle verifying the exchange's certificate.
    pub ca_file: PathBuf,
    /// The DNS name expected on that certificate.
    pub server_name: String,
}

/// Calls the exchange.
#[async_trait]
pub trait ExchangeApi: Send + Sync + 'static {
    /// One exchange.
    async fn exchange(
        &self,
        req: ExchangeActorJwtRequest,
    ) -> Result<ExchangeActorJwtResponse, Status>;
}

#[async_trait]
impl ExchangeApi for CredentialExchangeClient<Channel> {
    async fn exchange(
        &self,
        req: ExchangeActorJwtRequest,
    ) -> Result<ExchangeActorJwtResponse, Status> {
        self.clone()
            .exchange_actor_jwt(req)
            .await
            .map(|r| r.into_inner())
    }
}

/// Builds the storage client a token is used through.
pub type NewClient = dyn Fn(&Token) -> Result<Arc<dyn ObjectStorage>> + Send + Sync;

#[derive(Clone)]
struct Entry {
    token: Token,
    client: Arc<dyn ObjectStorage>,
}

/// [`Credentials`] for GCS through the exchange. Nothing closes a client
/// that leaves the cache: a transfer still using it finishes on it, and its
/// connections close once idle.
pub struct Source {
    exchange: Arc<dyn ExchangeApi>,
    new_client: Arc<NewClient>,
    now: Mutex<Arc<dyn Fn() -> SystemTime + Send + Sync>>,
    cache: Mutex<LruCache<String, Entry>>,
}

/// A GCS client authenticating with `tok` alone.
pub fn new_gcs_client(tok: &Token) -> Result<Arc<dyn ObjectStorage>> {
    let token = AccessToken {
        token: tok.access_token.clone(),
        expiry: Some(tok.expiry),
    };
    // An emulator is reached anonymously; anything else with the token.
    let opts = GcsOptions::from_env(|| Ok(Arc::new(StaticToken(token))))?;
    Ok(Arc::new(GcsClient::new(&opts)?))
}

impl Source {
    /// Dials the exchange over TLS; nothing is sent until the first
    /// transfer. The CA bundle is checked now.
    pub fn new(cfg: Config) -> Result<Self> {
        if cfg.address.is_empty() {
            return Err(Error::other("exchangeclient: address is required"));
        }
        hivekeep_identity::cred_bundle::load_trust_anchors(&cfg.ca_file).map_err(|e| {
            Error::other(format!(
                "exchangeclient: while loading the exchange CA bundle: {e}"
            ))
        })?;
        let ch = channel(
            &cfg.address,
            ClientFiles {
                ca_file: cfg.ca_file.clone(),
                client_bundle: None,
                server_name: cfg.server_name.clone(),
            },
        )
        .map_err(|e| {
            Error::other(format!(
                "exchangeclient: while creating the exchange client: {e}"
            ))
        })?;
        Ok(Self::with_exchange(
            Arc::new(CredentialExchangeClient::new(ch)),
            Arc::new(new_gcs_client),
        ))
    }

    /// A source over any exchange and client factory.
    pub fn with_exchange(exchange: Arc<dyn ExchangeApi>, new_client: Arc<NewClient>) -> Self {
        Source {
            exchange,
            new_client,
            now: Mutex::new(Arc::new(SystemTime::now)),
            cache: Mutex::new(LruCache::new(DEFAULT_CACHE_SIZE)),
        }
    }

    /// Sets the clock, for tests.
    pub fn set_clock(&self, now: Arc<dyn Fn() -> SystemTime + Send + Sync>) {
        *self.now.lock() = now;
    }

    /// Bounds the cache, for tests.
    pub fn set_cache_size(&self, n: usize) {
        *self.cache.lock() = LruCache::new(n);
    }

    fn now(&self) -> SystemTime {
        (self.now.lock())()
    }
}

#[async_trait]
impl Credentials for Source {
    /// A client scoped to the actor: the cached one for this JWT while its
    /// token has at least [`MIN_SERVE_LIFETIME`] left, otherwise one built on
    /// a fresh exchange. `force` skips the cache after GCS refused the cached
    /// token, and asks the exchange to skip its own. A refused or nearly
    /// expired entry is dropped first, so it is never served again even if
    /// the exchange fails. The cache is keyed by the JWT the exchange
    /// verified, never by a claim read out of it.
    async fn client(&self, actor_jwt: &str, force: bool) -> Result<Arc<dyn ObjectStorage>, Status> {
        if actor_jwt.is_empty() {
            return Err(Status::invalid_argument("actor_jwt is required on GCS"));
        }
        let key = actor_jwt.to_string();
        if !force {
            if let Some(e) = self.cache.lock().get(&key) {
                if lifetime_left(&e.token, self.now()) >= MIN_SERVE_LIFETIME {
                    return Ok(e.client);
                }
            }
        }
        self.cache.lock().remove(&key);
        let resp = self
            .exchange
            .exchange(ExchangeActorJwtRequest {
                actor_jwt: key.clone(),
                force,
            })
            .await?;
        let expiry = resp
            .expire_time
            .and_then(|t| SystemTime::try_from(t).ok())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let tok = Token {
            access_token: resp.access_token,
            expiry,
        };
        if tok.access_token.is_empty() {
            return Err(Status::internal("the exchange returned no access token"));
        }
        let client = (self.new_client)(&tok).map_err(|e| {
            Status::internal(format!("while building the actor's storage client: {e}"))
        })?;
        // A token shorter than the floor serves this transfer and is not kept.
        if lifetime_left(&tok, self.now()) >= MIN_SERVE_LIFETIME {
            self.cache.lock().add(
                key,
                Entry {
                    token: tok,
                    client: client.clone(),
                },
            );
        }
        Ok(client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{BackendKind, ObjectReader, UploadBody};
    use std::time::Duration;

    #[derive(Default)]
    struct FakeExchange {
        token: Mutex<String>,
        expiry: Mutex<Option<SystemTime>>,
        err: Mutex<Option<Status>>,
        calls: Mutex<Vec<ExchangeActorJwtRequest>>,
    }

    #[async_trait]
    impl ExchangeApi for FakeExchange {
        async fn exchange(
            &self,
            req: ExchangeActorJwtRequest,
        ) -> Result<ExchangeActorJwtResponse, Status> {
            self.calls.lock().push(req);
            if let Some(e) = self.err.lock().clone() {
                return Err(e);
            }
            Ok(ExchangeActorJwtResponse {
                access_token: self.token.lock().clone(),
                expire_time: self.expiry.lock().map(prost_types::Timestamp::from),
            })
        }
    }

    /// The storage client built for a token.
    struct FakeClient;

    #[async_trait]
    impl ObjectStorage for FakeClient {
        async fn get_object(&self, _: &str, _: &str) -> Result<ObjectReader> {
            Err(Error::other("unused"))
        }
        async fn put_object(&self, _: &str, _: &str, _: UploadBody) -> Result<()> {
            Err(Error::other("unused"))
        }
        fn backend(&self) -> BackendKind {
            BackendKind::Other
        }
    }

    fn t0() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_792_490_400)
    }

    const HOUR: Duration = Duration::from_secs(3600);

    fn exchange(token: &str, expiry: SystemTime) -> Arc<FakeExchange> {
        let e = FakeExchange::default();
        *e.token.lock() = token.into();
        *e.expiry.lock() = Some(expiry);
        Arc::new(e)
    }

    type Built = (Source, Arc<Mutex<usize>>, Arc<Mutex<Vec<String>>>);

    fn source(ex: Arc<FakeExchange>, now: SystemTime) -> Built {
        let built = Arc::new(Mutex::new(0));
        let tokens = Arc::new(Mutex::new(Vec::new()));
        let (b, t) = (built.clone(), tokens.clone());
        let s = Source::with_exchange(
            ex,
            Arc::new(move |tok: &Token| {
                *b.lock() += 1;
                t.lock().push(tok.access_token.clone());
                Ok(Arc::new(FakeClient) as Arc<dyn ObjectStorage>)
            }),
        );
        s.set_clock(Arc::new(move || now));
        (s, built, tokens)
    }

    /// Which token a returned client was built on: the clients are opaque,
    /// so the factory records tokens in build order and pointer identity
    /// tells which build a client is.
    fn same(a: &Arc<dyn ObjectStorage>, b: &Arc<dyn ObjectStorage>) -> bool {
        Arc::ptr_eq(a, b)
    }

    #[tokio::test]
    async fn caches_the_token_and_client_per_jwt_until_near_expiry() {
        let ex = exchange("tok-1", t0() + HOUR);
        let (s, built, tokens) = source(ex.clone(), t0());
        let first = s.client("jwt-a", false).await.unwrap();
        let again = s.client("jwt-a", false).await.unwrap();
        assert!(
            same(&first, &again),
            "second transfer got a different client"
        );
        assert_eq!(ex.calls.lock().len(), 1);
        assert!(!ex.calls.lock()[0].force && ex.calls.lock()[0].actor_jwt == "jwt-a");
        assert_eq!(*built.lock(), 1);
        // Another JWT is a miss, even one naming the same actor.
        s.client("jwt-b-same-actor", false).await.unwrap();
        assert_eq!(ex.calls.lock().len(), 2);
        // Under the floor the cache is not served and the entry is replaced.
        s.set_clock(Arc::new(|| {
            t0() + HOUR - MIN_SERVE_LIFETIME + Duration::from_secs(1)
        }));
        *ex.token.lock() = "tok-2".into();
        *ex.expiry.lock() = Some(t0() + 2 * HOUR);
        let c = s.client("jwt-a", false).await.unwrap();
        assert_eq!(tokens.lock().last().unwrap(), "tok-2");
        let c2 = s.client("jwt-a", false).await.unwrap();
        assert!(same(&c, &c2));
        assert_eq!(ex.calls.lock().len(), 3);
    }

    #[tokio::test]
    async fn force_skips_both_caches() {
        let ex = exchange("tok-1", t0() + HOUR);
        let (s, built, tokens) = source(ex.clone(), t0());
        s.client("jwt", false).await.unwrap();
        *ex.token.lock() = "tok-2".into();
        let forced = s.client("jwt", true).await.unwrap();
        assert_eq!(tokens.lock().last().unwrap(), "tok-2");
        assert!(ex.calls.lock().len() == 2 && ex.calls.lock()[1].force);
        // The forced token replaces the cached one.
        let after = s.client("jwt", false).await.unwrap();
        assert!(same(&forced, &after));
        assert_eq!(ex.calls.lock().len(), 2);
        assert_eq!(*built.lock(), 2);
    }

    #[tokio::test]
    async fn drops_the_refused_entry_even_when_the_forced_exchange_fails() {
        let ex = exchange("tok-1", t0() + HOUR);
        let (s, _, _) = source(ex.clone(), t0());
        s.client("jwt", false).await.unwrap();
        *ex.err.lock() = Some(Status::not_found("actor not found"));
        assert_eq!(
            s.client("jwt", true).await.err().unwrap().code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            s.client("jwt", false).await.err().unwrap().code(),
            tonic::Code::NotFound
        );
        assert_eq!(ex.calls.lock().len(), 3);
    }

    #[tokio::test]
    async fn drops_the_refused_entry_when_the_forced_token_is_short() {
        let ex = exchange("tok-1", t0() + HOUR);
        let (s, _, tokens) = source(ex.clone(), t0());
        s.client("jwt", false).await.unwrap();
        *ex.token.lock() = "short".into();
        *ex.expiry.lock() = Some(t0() + MIN_SERVE_LIFETIME - Duration::from_secs(60));
        s.client("jwt", true).await.unwrap();
        assert_eq!(tokens.lock().last().unwrap(), "short");
        s.client("jwt", false).await.unwrap();
        assert_eq!(tokens.lock().last().unwrap(), "short");
        assert_eq!(
            ex.calls.lock().len(),
            3,
            "neither the refused nor the short token is cached"
        );
    }

    #[tokio::test]
    async fn evicts_the_oldest_jwt_when_full() {
        let ex = exchange("tok", t0() + HOUR);
        let (s, _, _) = source(ex.clone(), t0());
        s.set_cache_size(1);
        for jwt in ["jwt-a", "jwt-b", "jwt-a"] {
            s.client(jwt, false).await.unwrap();
        }
        assert_eq!(ex.calls.lock().len(), 3);
    }

    #[tokio::test]
    async fn uses_but_does_not_cache_a_short_token() {
        let ex = exchange("short", t0() + MIN_SERVE_LIFETIME - Duration::from_secs(60));
        let (s, _, _) = source(ex.clone(), t0());
        for _ in 0..2 {
            s.client("jwt", false).await.unwrap();
        }
        assert_eq!(ex.calls.lock().len(), 2);
    }

    #[tokio::test]
    async fn client_errors() {
        use tonic::Code::*;
        for (name, jwt, token, err, want) in [
            ("empty jwt", "", "t", None, InvalidArgument),
            (
                "exchange refuses",
                "jwt",
                "",
                Some(Status::permission_denied(
                    "actor is no longer on the token's node",
                )),
                PermissionDenied,
            ),
            (
                "exchange unavailable",
                "jwt",
                "",
                Some(Status::unavailable("down")),
                Unavailable,
            ),
            ("exchange returns no token", "jwt", "", None, Internal),
        ] {
            let ex = exchange(token, t0() + HOUR);
            *ex.err.lock() = err;
            let (s, _, _) = source(ex.clone(), t0());
            let e = s.client(jwt, false).await.err().unwrap();
            assert_eq!(e.code(), want, "{name}: {e:?}");
            if jwt.is_empty() {
                assert!(
                    ex.calls.lock().is_empty(),
                    "the exchange was called for an empty token"
                );
            }
        }
    }

    #[tokio::test]
    async fn reports_a_client_build_failure() {
        let s = Source::with_exchange(
            exchange("t", t0() + HOUR),
            Arc::new(|_: &Token| Err(Error::other("no transport"))),
        );
        s.set_clock(Arc::new(t0));
        assert_eq!(
            s.client("jwt", false).await.err().unwrap().code(),
            tonic::Code::Internal
        );
    }

    #[test]
    fn new_checks_its_config() {
        assert!(Source::new(Config {
            ca_file: "x".into(),
            ..Default::default()
        })
        .is_err());
        let dir = tempfile::tempdir().unwrap();
        assert!(Source::new(Config {
            address: "hivekeep-api:8445".into(),
            ca_file: dir.path().join("absent.pem"),
            ..Default::default()
        })
        .is_err());
    }
}

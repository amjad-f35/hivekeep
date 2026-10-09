//! Verifying tokens from one trusted issuer.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use jsonwebtoken::{Algorithm, DecodingKey};
use parking_lot::Mutex;
use serde::Deserialize;

use super::authority::JwtSigner;
use super::claims::{ActorClaims, TokenKind, WireClaims, ACTOR_CLAIM, WORKER_FOR_ACTOR_CLAIM};
use super::jwks::{key_set_json, parse_key_set, Curve, JwkKey, KeyWithId};
use crate::error::{IdentityError, Result};

/// Clock skew tolerated on `exp`, `nbf` and `iat`.
pub const PERMITTED_SKEW: Duration = Duration::from_secs(5 * 60);
/// How long fetched keys are trusted before they are fetched again.
pub const KEY_REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// Minimum spacing of refetches after a failure or an unknown key id.
pub const KEY_REFRESH_RETRY_INTERVAL: Duration = Duration::from_secs(60);

/// Supplies an issuer's verification keys. Called on first use, every
/// [`KEY_REFRESH_INTERVAL`], and at a bounded rate when a token names an
/// unknown key, so a source may read its backing store on every call.
pub trait KeySource: Send + Sync {
    /// The issuer's current keys.
    fn keys(&self) -> Result<Vec<KeyWithId>>;
}

/// Reads a JWK set file on every call, for verifiers handed the issuer's
/// public keys directly (e.g. a projected volume). The file must be
/// replaced atomically so a rotation is never read half-written.
#[derive(Debug, Clone)]
pub struct JwksFileSource {
    path: PathBuf,
}

impl JwksFileSource {
    /// A source reading `path`.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        JwksFileSource { path: path.into() }
    }
}

impl KeySource for JwksFileSource {
    fn keys(&self) -> Result<Vec<KeyWithId>> {
        let bytes = std::fs::read(&self.path).map_err(|source| IdentityError::Io {
            path: self.path.display().to_string(),
            source,
        })?;
        parse_key_set(&bytes, &format!("file {}", self.path.display()))
    }
}

/// Verifies against the keys of a local signer, as a process that both mints
/// and checks tokens does.
pub struct SignerKeySource(pub Arc<dyn JwtSigner>);

impl KeySource for SignerKeySource {
    fn keys(&self) -> Result<Vec<KeyWithId>> {
        parse_key_set(&key_set_json(&self.0.verification_keys()?)?, "local signer")
    }
}

/// A fixed set of keys.
#[derive(Debug, Clone)]
pub struct StaticKeySource(pub Vec<KeyWithId>);

impl KeySource for StaticKeySource {
    fn keys(&self) -> Result<Vec<KeyWithId>> {
        Ok(self.0.clone())
    }
}

/// The claims of a verified token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedToken {
    /// `iss`.
    pub issuer: String,
    /// `sub`.
    pub subject: String,
    /// `aud`, as a list whichever form the token used.
    pub audiences: Vec<String>,
    /// `exp`.
    pub expiration: SystemTime,
    /// `nbf`.
    pub not_before: SystemTime,
    /// `iat`.
    pub issued_at: SystemTime,
    /// `jti`.
    pub jti: String,
    /// The Hivekeep claim and which kind it was, when the verifier decodes
    /// them and the token carries one.
    pub actor: Option<(TokenKind, ActorClaims)>,
    /// The bound claims of a Kubernetes service-account token; empty for
    /// tokens without them.
    pub kubernetes: KubernetesClaims,
}

/// What a Kubernetes service-account token says about its holder (the
/// `kubernetes.io` claim). Fields the token does not carry are empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KubernetesClaims {
    /// Namespace of the service account.
    pub namespace: String,
    /// Service account name.
    pub service_account_name: String,
    /// Service account UID.
    pub service_account_uid: String,
    /// Pod the token is bound to.
    pub pod_name: String,
    /// UID of that Pod.
    pub pod_uid: String,
    /// Secret the token is bound to.
    pub secret_name: String,
    /// UID of that Secret.
    pub secret_uid: String,
    /// Node the token is bound to.
    pub node_name: String,
    /// UID of that Node.
    pub node_uid: String,
    /// When the kubelet should have refreshed the token, if stated.
    pub warn_after: Option<SystemTime>,
}

#[derive(Deserialize, Default)]
struct BoundClaimsOnly {
    #[serde(rename = "kubernetes.io", default)]
    kubernetes: BoundClaims,
}

#[derive(Deserialize, Default)]
struct BoundClaims {
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    pod: BoundObject,
    #[serde(default)]
    serviceaccount: BoundObject,
    #[serde(default)]
    secret: BoundObject,
    #[serde(default)]
    node: BoundObject,
    #[serde(default)]
    warnafter: f64,
}

#[derive(Deserialize, Default)]
struct BoundObject {
    #[serde(default)]
    name: String,
    #[serde(default)]
    uid: String,
}

impl From<BoundClaims> for KubernetesClaims {
    fn from(b: BoundClaims) -> Self {
        KubernetesClaims {
            namespace: b.namespace,
            service_account_name: b.serviceaccount.name,
            service_account_uid: b.serviceaccount.uid,
            pod_name: b.pod.name,
            pod_uid: b.pod.uid,
            secret_name: b.secret.name,
            secret_uid: b.secret.uid,
            node_name: b.node.name,
            node_uid: b.node.uid,
            warn_after: (b.warnafter > 0.0)
                .then(|| UNIX_EPOCH + Duration::from_secs(b.warnafter as u64)),
        }
    }
}

#[derive(Deserialize)]
struct Header {
    #[serde(default)]
    typ: String,
    #[serde(default)]
    alg: String,
    #[serde(default)]
    kid: String,
}

#[derive(Deserialize)]
struct IssuerOnly {
    #[serde(rename = "iss", default)]
    issuer: String,
}

#[derive(Default)]
struct Cache {
    keys: Vec<KeyWithId>,
    last_refresh: Option<SystemTime>,
    last_failed_refresh: Option<SystemTime>,
    last_unknown_key_refresh: Option<SystemTime>,
}

fn within(now: SystemTime, then: Option<SystemTime>, window: Duration) -> bool {
    then.is_some_and(|t| now.duration_since(t).map(|d| d < window).unwrap_or(true))
}

/// Verifies JWTs from one issuer, caching its keys.
pub struct Verifier {
    issuer: String,
    audiences: Vec<String>,
    source: Arc<dyn KeySource>,
    actor_claims: bool,
    cache: Mutex<Cache>,
}

impl Verifier {
    /// A verifier for `issuer` accepting tokens that name at least one of
    /// `audiences`.
    pub fn new(
        issuer: impl Into<String>,
        audiences: Vec<String>,
        source: Arc<dyn KeySource>,
    ) -> Self {
        Verifier {
            issuer: issuer.into(),
            audiences,
            source,
            actor_claims: false,
            cache: Mutex::new(Cache::default()),
        }
    }

    /// Also decodes the Hivekeep actor claims, refusing a token that carries
    /// both kinds or a malformed one. Only meaningful for the Actor token
    /// issuer: any issuer can put those claim names in a token.
    pub fn with_actor_claims(mut self) -> Self {
        self.actor_claims = true;
        self
    }

    fn key(&self, key_id: &str, now: SystemTime) -> Result<JwkKey> {
        let mut c = self.cache.lock();
        let cached = c
            .keys
            .iter()
            .find(|k| k.key_id == key_id)
            .map(|k| k.key.clone());
        if let Some(k) = &cached {
            if within(now, c.last_refresh, KEY_REFRESH_INTERVAL) {
                return Ok(k.clone());
            }
        }
        if within(now, c.last_failed_refresh, KEY_REFRESH_RETRY_INTERVAL) {
            return cached.ok_or_else(|| {
                IdentityError::Keys("signing key refresh is temporarily unavailable".into())
            });
        }
        if cached.is_none() && !c.keys.is_empty() {
            if within(now, c.last_unknown_key_refresh, KEY_REFRESH_RETRY_INTERVAL) {
                return Err(IdentityError::InvalidToken(format!(
                    "unknown key ID {key_id:?}"
                )));
            }
            c.last_unknown_key_refresh = Some(now);
        }
        match self.source.keys() {
            Ok(keys) => {
                c.keys = keys;
                c.last_refresh = Some(now);
                c.last_failed_refresh = None;
                c.keys
                    .iter()
                    .find(|k| k.key_id == key_id)
                    .map(|k| k.key.clone())
                    .ok_or_else(|| {
                        IdentityError::InvalidToken(format!("unknown key ID {key_id:?}"))
                    })
            }
            Err(e) => {
                c.last_failed_refresh = Some(now);
                match cached {
                    Some(k) => {
                        tracing::warn!(issuer = self.issuer, error = %e, "could not refresh JWT keys; using cached key");
                        Ok(k)
                    }
                    None => Err(IdentityError::Keys(format!(
                        "loading signing keys for issuer: {e}"
                    ))),
                }
            }
        }
    }

    /// Verifies `token` at time `now`.
    pub fn verify(&self, token: &str, now: SystemTime) -> Result<VerifiedToken> {
        let bad = |m: String| IdentityError::InvalidToken(m);
        let mut parts = token.split('.');
        let (Some(h64), Some(p64), Some(s64), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(bad("malformed JWT".into()));
        };
        let header: Header = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(h64)
                .map_err(|e| bad(format!("decoding header: {e}")))?,
        )
        .map_err(|e| bad(format!("parsing header: {e}")))?;
        if !matches!(header.typ.as_str(), "" | "JWT") {
            return Err(bad("unexpected value in type header".into()));
        }
        let payload = URL_SAFE_NO_PAD
            .decode(p64)
            .map_err(|e| bad(format!("decoding payload: {e}")))?;

        // Until the signature checks out, the payload is consulted only for
        // the issuer, to pick the keys.
        let iss: IssuerOnly =
            serde_json::from_slice(&payload).map_err(|e| bad(format!("parsing payload: {e}")))?;
        if iss.issuer != self.issuer {
            return Err(bad(format!("unexpected issuer {:?}", iss.issuer)));
        }
        if header.kid.is_empty() {
            return Err(bad("key ID is required".into()));
        }
        let key = self.key(&header.kid, now)?;
        verify_signature(&header.alg, &key, format!("{h64}.{p64}").as_bytes(), s64)?;

        // The payload is authentic; its bindings still need checking.
        let claims: WireClaims =
            serde_json::from_slice(&payload).map_err(|e| bad(format!("parsing payload: {e}")))?;
        let audiences = match &claims.audiences {
            Some(serde_json::Value::String(s)) => vec![s.clone()],
            Some(v @ serde_json::Value::Array(_)) => serde_json::from_value(v.clone())
                .map_err(|_| bad("unable to parse audiences".into()))?,
            _ => return Err(bad("unable to parse audiences".into())),
        };
        if !self.audiences.iter().any(|want| audiences.contains(want)) {
            return Err(bad("token is not issued for expected audience".into()));
        }
        if claims.subject.is_empty() {
            return Err(bad("token subject is required".into()));
        }
        let at = |secs: f64| UNIX_EPOCH + Duration::from_secs(secs.max(0.0) as u64);
        let (expiration, not_before, issued_at) = (
            at(claims.expiration),
            at(claims.not_before),
            at(claims.issued_at),
        );
        if expiration + PERMITTED_SKEW < now {
            return Err(bad("jwt has expired".into()));
        }
        if not_before > now + PERMITTED_SKEW {
            return Err(bad("jwt is not valid yet".into()));
        }
        if issued_at > now + PERMITTED_SKEW {
            return Err(bad("jwt claims to have been issued in the future".into()));
        }
        let actor = if self.actor_claims {
            decode_actor_claims(&claims)?
        } else {
            None
        };
        let bound: BoundClaimsOnly = serde_json::from_slice(&payload)
            .map_err(|e| bad(format!("parsing the kubernetes.io claim: {e}")))?;
        Ok(VerifiedToken {
            issuer: claims.issuer,
            subject: claims.subject,
            audiences,
            expiration,
            not_before,
            issued_at,
            jti: claims.jti,
            actor,
            kubernetes: bound.kubernetes.into(),
        })
    }
}

fn decode_actor_claims(claims: &WireClaims) -> Result<Option<(TokenKind, ActorClaims)>> {
    let decode = |name: &str, v: &Option<serde_json::Value>| -> Result<Option<ActorClaims>> {
        match v {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(v @ serde_json::Value::Object(_)) => serde_json::from_value(v.clone())
                .map(Some)
                .map_err(|e| IdentityError::InvalidToken(format!("parsing the {name} claim: {e}"))),
            Some(_) => Err(IdentityError::InvalidToken(format!(
                "the {name} claim is not an object"
            ))),
        }
    };
    match (
        decode(ACTOR_CLAIM, &claims.actor)?,
        decode(WORKER_FOR_ACTOR_CLAIM, &claims.worker_for_actor)?,
    ) {
        (None, None) => Ok(None),
        (Some(a), None) => Ok(Some((TokenKind::Actor, a))),
        (None, Some(w)) => Ok(Some((TokenKind::WorkerForActor, w))),
        (Some(_), Some(_)) => Err(IdentityError::InvalidToken(format!(
            "token carries both {ACTOR_CLAIM} and {WORKER_FOR_ACTOR_CLAIM}"
        ))),
    }
}

fn verify_signature(alg: &str, key: &JwkKey, message: &[u8], signature: &str) -> Result<()> {
    let bad = |m: &str| IdentityError::InvalidToken(m.to_string());
    let (algorithm, decoding) = match (alg, key) {
        ("RS256" | "RS384" | "RS512", JwkKey::Rsa { n, e }) => (
            match alg {
                "RS256" => Algorithm::RS256,
                "RS384" => Algorithm::RS384,
                _ => Algorithm::RS512,
            },
            DecodingKey::from_rsa_raw_components(n, e),
        ),
        ("RS256" | "RS384" | "RS512", _) => return Err(bad("requested key ID is not an RSA key")),
        (
            "ES256",
            JwkKey::Ec {
                curve: Curve::P256,
                x,
                y,
            },
        )
        | (
            "ES384",
            JwkKey::Ec {
                curve: Curve::P384,
                x,
                y,
            },
        ) => (
            if alg == "ES256" {
                Algorithm::ES256
            } else {
                Algorithm::ES384
            },
            DecodingKey::from_ec_components(x, y)
                .map_err(|e| IdentityError::InvalidToken(e.to_string()))?,
        ),
        (
            "ES512",
            JwkKey::Ec {
                curve: Curve::P521,
                x,
                y,
            },
        ) => return verify_p521(x, y, message, signature),
        ("ES256" | "ES384" | "ES512", _) => {
            return Err(bad(
                "requested key ID is not an ECDSA key on the algorithm's curve",
            ))
        }
        (other, _) => {
            return Err(IdentityError::InvalidToken(format!(
                "unsupported algorithm {other:?}"
            )))
        }
    };
    match jsonwebtoken::crypto::verify(signature, message, &decoding, algorithm) {
        Ok(true) => Ok(()),
        Ok(false) => Err(bad("invalid signature")),
        Err(e) => Err(IdentityError::InvalidToken(format!(
            "verifying signature: {e}"
        ))),
    }
}

/// ES512, which jsonwebtoken does not implement: a fixed-width `r || s`
/// signature over SHA-512 on P-521.
fn verify_p521(x: &str, y: &str, message: &[u8], signature: &str) -> Result<()> {
    use aws_lc_rs::signature::{UnparsedPublicKey, ECDSA_P521_SHA512_FIXED};
    let bad = |m: String| IdentityError::InvalidToken(m);
    let decode = |what: &str, v: &str| {
        URL_SAFE_NO_PAD
            .decode(v)
            .map_err(|e| bad(format!("decoding {what}: {e}")))
    };
    let mut point = vec![0x04];
    point.extend(decode("x", x)?);
    point.extend(decode("y", y)?);
    let signature = decode("signature", signature)?;
    if signature.len() != 2 * 66 {
        return Err(bad("invalid signature: wrong length for ES512".into()));
    }
    UnparsedPublicKey::new(&ECDSA_P521_SHA512_FIXED, &point)
        .verify(message, &signature)
        .map_err(|_| bad("invalid signature".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jwt::authority::{JwtAlgorithm, JwtAuthority, JwtPool};
    use crate::jwt::claims::Claims;
    use crate::key::{KeyAlgorithm, PrivateKey};
    use std::sync::atomic::{AtomicUsize, Ordering};

    const ISSUER: &str = "https://idp.example.com";

    fn pool(alg: JwtAlgorithm) -> Arc<JwtPool> {
        let a = match alg {
            JwtAlgorithm::Es256 => JwtAuthority::generate(alg, Some("k1")).unwrap(),
            _ => JwtAuthority::new(
                "k1",
                alg,
                PrivateKey::generate(KeyAlgorithm::Rsa2048).unwrap(),
            )
            .unwrap(),
        };
        Arc::new(JwtPool::new(vec![a], "k1"))
    }

    fn verifier(p: &Arc<JwtPool>) -> Verifier {
        Verifier::new(
            ISSUER,
            vec!["aud-1".into()],
            Arc::new(SignerKeySource(p.clone())),
        )
        .with_actor_claims()
    }

    fn actor_claims(now: SystemTime) -> Claims {
        Claims::for_actor(
            ISSUER,
            "team-a",
            "agent",
            "uid-1",
            vec!["aud-1".into()],
            Duration::from_secs(600),
            now,
        )
        .unwrap()
    }

    #[test]
    fn round_trips_every_algorithm() {
        let now = SystemTime::now();
        for alg in [
            JwtAlgorithm::Es256,
            JwtAlgorithm::Rs256,
            JwtAlgorithm::Rs384,
            JwtAlgorithm::Rs512,
        ] {
            let p = pool(alg);
            let token = p.sign_jwt(&actor_claims(now)).unwrap();
            let got = verifier(&p).verify(&token, now).unwrap();
            assert_eq!(got.subject, "actor/team-a/agent", "{alg}");
            assert_eq!(got.audiences, vec!["aud-1"]);
            let (kind, claims) = got.actor.unwrap();
            assert_eq!(kind, TokenKind::Actor);
            assert_eq!(claims.actor_uid, "uid-1");
        }
    }

    #[test]
    fn worker_for_actor_tokens_carry_the_node() {
        let now = SystemTime::now();
        let p = pool(JwtAlgorithm::Es256);
        let claims = Claims::for_worker_for_actor(
            ISSUER,
            "team-a",
            "agent",
            "uid-1",
            "node-7",
            vec!["aud-1".into()],
            now,
        )
        .unwrap();
        let got = verifier(&p)
            .verify(&p.sign_jwt(&claims).unwrap(), now)
            .unwrap();
        let (kind, c) = got.actor.unwrap();
        assert_eq!(
            (kind, c.node.as_str()),
            (TokenKind::WorkerForActor, "node-7")
        );
    }

    #[test]
    fn rejects_bad_tokens() {
        let now = SystemTime::now();
        let p = pool(JwtAlgorithm::Es256);
        let v = verifier(&p);
        let token = p.sign_jwt(&actor_claims(now)).unwrap();

        // Tampered payload.
        let parts: Vec<&str> = token.split('.').collect();
        let mut claims: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        claims["sub"] = "actor/team-a/other".into();
        let forged = format!(
            "{}.{}.{}",
            parts[0],
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap()),
            parts[2]
        );
        assert!(v.verify(&forged, now).is_err());

        // Wrong audience, issuer, timing.
        let other_aud = Verifier::new(
            ISSUER,
            vec!["other".into()],
            Arc::new(SignerKeySource(p.clone())),
        );
        assert!(other_aud.verify(&token, now).is_err());
        let other_iss = Verifier::new(
            "https://elsewhere",
            vec!["aud-1".into()],
            Arc::new(SignerKeySource(p.clone())),
        );
        assert!(other_iss.verify(&token, now).is_err());
        assert!(v
            .verify(
                &token,
                now + Duration::from_secs(600) + PERMITTED_SKEW + Duration::from_secs(2)
            )
            .is_err());
        let future = p
            .sign_jwt(&actor_claims(now + Duration::from_secs(3600)))
            .unwrap();
        assert!(v.verify(&future, now).is_err());

        // Signed by a key the issuer does not publish.
        let stranger = pool(JwtAlgorithm::Es256);
        assert!(v
            .verify(&stranger.sign_jwt(&actor_claims(now)).unwrap(), now)
            .is_err());

        for junk in ["", "a.b", "a.b.c.d", "!!.!!.!!"] {
            assert!(v.verify(junk, now).is_err(), "{junk:?}");
        }
    }

    #[test]
    fn refuses_algorithm_confusion() {
        let now = SystemTime::now();
        let p = pool(JwtAlgorithm::Es256);
        let token = p.sign_jwt(&actor_claims(now)).unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        let header = URL_SAFE_NO_PAD.encode(br#"{"typ":"JWT","alg":"RS256","kid":"k1"}"#);
        let swapped = format!("{header}.{}.{}", parts[1], parts[2]);
        let err = verifier(&p).verify(&swapped, now).unwrap_err().to_string();
        assert!(err.contains("not an RSA key"), "{err}");
    }

    struct Counting {
        inner: SignerKeySource,
        calls: AtomicUsize,
    }

    impl KeySource for Counting {
        fn keys(&self) -> Result<Vec<KeyWithId>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.keys()
        }
    }

    #[test]
    fn caches_keys_and_rate_limits_unknown_ids() {
        let now = SystemTime::now();
        let p = pool(JwtAlgorithm::Es256);
        let source = Arc::new(Counting {
            inner: SignerKeySource(p.clone()),
            calls: AtomicUsize::new(0),
        });
        let v = Verifier::new(ISSUER, vec!["aud-1".into()], source.clone());
        let token = p.sign_jwt(&actor_claims(now)).unwrap();
        v.verify(&token, now).unwrap();
        v.verify(&token, now + Duration::from_secs(10)).unwrap();
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);

        // An unknown key id refetches once, then is refused without asking
        // again until the retry interval passes.
        let stranger = pool(JwtAlgorithm::Es256);
        let mut stranger_pool = (*stranger).clone();
        stranger_pool
            .add(JwtAuthority::generate(JwtAlgorithm::Es256, Some("k2")).unwrap())
            .unwrap();
        stranger_pool.activate("k2").unwrap();
        let unknown = stranger_pool.sign_jwt(&actor_claims(now)).unwrap();
        assert!(v.verify(&unknown, now).is_err());
        assert!(v.verify(&unknown, now).is_err());
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);
        assert!(v
            .verify(&unknown, now + KEY_REFRESH_RETRY_INTERVAL)
            .is_err());
        assert_eq!(source.calls.load(Ordering::SeqCst), 3);

        // Keys are refetched once the refresh interval passes.
        v.verify(
            &token,
            now + KEY_REFRESH_INTERVAL + KEY_REFRESH_RETRY_INTERVAL + Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(source.calls.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn reads_key_files() {
        let now = SystemTime::now();
        let p = pool(JwtAlgorithm::Es256);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jwks.json");
        std::fs::write(
            &path,
            key_set_json(&p.verification_keys().unwrap()).unwrap(),
        )
        .unwrap();
        let v = Verifier::new(
            ISSUER,
            vec!["aud-1".into()],
            Arc::new(JwksFileSource::new(&path)),
        );
        let got = v
            .verify(&p.sign_jwt(&actor_claims(now)).unwrap(), now)
            .unwrap();
        assert!(
            got.actor.is_none(),
            "actor claims decoded without opting in"
        );
    }

    #[test]
    fn wire_claims_actor_kind() {
        let actor = serde_json::json!({"hive": "a", "actorName": "n", "actorUID": "u"});
        let worker = serde_json::json!({"hive": "a2", "actorName": "n2", "actorUID": "u2"});
        let parsed =
            |v: &serde_json::Value| -> ActorClaims { serde_json::from_value(v.clone()).unwrap() };
        let cases = [
            ("neither claim", None, None, Ok(None)),
            ("both claims", Some(&actor), Some(&worker), Err(())),
            (
                "actor",
                Some(&actor),
                None,
                Ok(Some((TokenKind::Actor, parsed(&actor)))),
            ),
            (
                "worker-for-actor",
                None,
                Some(&worker),
                Ok(Some((TokenKind::WorkerForActor, parsed(&worker)))),
            ),
        ];
        for (name, a, w, want) in cases {
            let wire = WireClaims {
                actor: a.cloned(),
                worker_for_actor: w.cloned(),
                ..Default::default()
            };
            assert_eq!(decode_actor_claims(&wire).map_err(|_| ()), want, "{name}");
        }
    }

    #[test]
    fn refuses_tokens_with_both_actor_claims() {
        let claims = WireClaims {
            actor: Some(serde_json::json!({})),
            worker_for_actor: Some(serde_json::json!({})),
            ..Default::default()
        };
        assert!(decode_actor_claims(&claims).is_err());
        let not_object = WireClaims {
            actor: Some(serde_json::json!("x")),
            ..Default::default()
        };
        assert!(decode_actor_claims(&not_object).is_err());
        assert!(decode_actor_claims(&WireClaims::default())
            .unwrap()
            .is_none());
    }
}

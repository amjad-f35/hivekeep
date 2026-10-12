//! The credential exchange the control plugin serves: the node plugin
//! presents the worker-for-actor JWT the keeper gave it and gets back a GCS
//! token scoped to that actor's snapshot prefixes.
//!
//! The handler verifies the JWT, looks the actor up, derives its grants and
//! hands them to an [`Issuer`]. Verification, lookup and grants know no
//! storage provider; the issuer is the seam between backends.

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use hivekeep_core::actor_state::ActorState;
use hivekeep_identity::jwt::claims::{ActorClaims, TokenKind};
use hivekeep_proto::api::v1 as pb;
use tonic::{Request, Response, Status};

use super::actorscope::from_actor;
use super::gcsissuer::{issue_code, Issuer};
use crate::proto::exchange::v1::credential_exchange_server::CredentialExchange;
use crate::proto::exchange::v1::{ExchangeActorJwtRequest, ExchangeActorJwtResponse};

/// The port the control sidecar serves the exchange on.
pub const DEFAULT_PORT: u16 = 8445;
/// The audience of the object-store plugin API. The keeper requests it on
/// the worker-for-actor JWT it hands the node plugin, and every exchange
/// serving this API requires it.
pub const ACTOR_JWT_AUDIENCE: &str = "objectstore.hivekeep.io";

/// Why a lookup failed.
#[derive(Debug, Clone)]
pub enum LookupError {
    /// The actor at the JWT's name has another UID: it was deleted and
    /// recreated since the JWT was minted.
    Stale,
    /// hivekeep-api answered with a status.
    Status(Status),
    /// Anything else, such as claims without an actor.
    Other(String),
}

impl std::fmt::Display for LookupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LookupError::Stale => f.write_str("actor UID does not match the JWT"),
            LookupError::Status(s) => write!(f, "{}: {}", s.code(), s.message()),
            LookupError::Other(m) => f.write_str(m),
        }
    }
}

/// Resolves the actor a verified JWT names.
#[async_trait]
pub trait Lookup: Send + Sync + 'static {
    /// The actor the claims name.
    async fn actor(&self, claims: &ActorClaims) -> Result<pb::Actor, LookupError>;
}

/// What a verified token says.
#[derive(Debug, Clone)]
pub struct Verified {
    /// The Hivekeep claim and its kind, if any.
    pub actor: Option<(TokenKind, ActorClaims)>,
}

/// Checks a JWT.
pub trait Verifier: Send + Sync + 'static {
    /// The token's claims, or why it is refused.
    fn verify(&self, jwt: &str, now: SystemTime) -> Result<Verified, String>;
}

impl Verifier for hivekeep_identity::jwt::verifier::Verifier {
    fn verify(&self, jwt: &str, now: SystemTime) -> Result<Verified, String> {
        hivekeep_identity::jwt::verifier::Verifier::verify(self, jwt, now)
            .map(|t| Verified { actor: t.actor })
            .map_err(|e| e.to_string())
    }
}

/// Serves CredentialExchange.
pub struct ExchangeServer {
    verifier: Arc<dyn Verifier>,
    lookup: Arc<dyn Lookup>,
    issuer: Arc<dyn Issuer>,
    now: Arc<dyn Fn() -> SystemTime + Send + Sync>,
}

impl ExchangeServer {
    /// A server; the verifier must accept [`ACTOR_JWT_AUDIENCE`].
    pub fn new(
        verifier: Arc<dyn Verifier>,
        lookup: Arc<dyn Lookup>,
        issuer: Arc<dyn Issuer>,
    ) -> Self {
        ExchangeServer {
            verifier,
            lookup,
            issuer,
            now: Arc::new(SystemTime::now),
        }
    }

    /// The same server on another clock, for tests.
    pub fn with_clock(mut self, now: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> Self {
        self.now = now;
        self
    }

    /// The exchange. Only a worker-for-actor token is accepted: an actor's
    /// own token has the same shape and can be minted for this audience, and
    /// actor code must not obtain credentials for its own snapshots.
    pub async fn exchange(
        &self,
        req: &ExchangeActorJwtRequest,
    ) -> Result<ExchangeActorJwtResponse, Status> {
        if req.actor_jwt.is_empty() {
            return Err(Status::invalid_argument("actor_jwt is required"));
        }
        let verified = self
            .verifier
            .verify(&req.actor_jwt, (self.now)())
            .map_err(|e| Status::unauthenticated(format!("actor_jwt is not valid: {e}")))?;
        let claims = match verified.actor {
            Some((TokenKind::WorkerForActor, c)) => c,
            _ => {
                return Err(Status::permission_denied(
                    "only a worker-for-actor token may be exchanged",
                ))
            }
        };
        if claims.hive.is_empty()
            || claims.actor_name.is_empty()
            || claims.actor_uid.is_empty()
            || claims.node.is_empty()
        {
            return Err(Status::unauthenticated(
                "actor_jwt must carry the actor's hive, name and UID and the minting node, and one of them is empty",
            ));
        }
        let actor = match self.lookup.actor(&claims).await {
            Ok(a) => a,
            Err(LookupError::Stale) => return Err(Status::not_found("actor not found")),
            Err(LookupError::Status(s)) => {
                return Err(match s.code() {
                    tonic::Code::NotFound => Status::not_found("actor not found"),
                    // hivekeep-api refused the actor reference the token carries.
                    tonic::Code::InvalidArgument => Status::unauthenticated(format!(
                        "actor_jwt names an invalid actor: {}",
                        s.message()
                    )),
                    // The exchange's own identity lacks access; retrying cannot help.
                    tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
                        Status::failed_precondition(format!(
                            "the exchange may not read actors: {}",
                            s.message()
                        ))
                    }
                    // The caller gave up, which says nothing about hivekeep-api.
                    tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => s,
                    _ => {
                        Status::unavailable(format!("while looking up the actor: {}", s.message()))
                    }
                });
            }
            Err(e) => {
                return Err(Status::unavailable(format!(
                    "while looking up the actor: {e}"
                )))
            }
        };
        actor_still_on_node(&actor, &claims.node)?;
        let grants = from_actor(&actor).map_err(|e| {
            Status::internal(format!("while deriving the actor's storage grants: {e}"))
        })?;
        if grants.is_empty() {
            return Err(Status::permission_denied(
                "actor has no snapshot location to grant",
            ));
        }
        let uid = actor
            .metadata
            .as_ref()
            .map(|m| m.uid.clone())
            .unwrap_or_default();
        let tok = self
            .issuer
            .issue(&uid, &grants, req.force)
            .await
            .map_err(|e| {
                Status::new(issue_code(&e), format!("while issuing the credential: {e}"))
            })?;
        Ok(ExchangeActorJwtResponse {
            access_token: tok.access_token,
            expire_time: Some(prost_types::Timestamp::from(tok.expiry)),
        })
    }
}

/// Refuses a token minted for a node the actor has since left, or for an
/// actor in a state with no transfer. The token outlives the mint-time check
/// by up to an hour; the actor's assigned node names the node holding it
/// (set on resume, kept while suspended with its local snapshot there,
/// cleared on detach), so a token from a node it moved away from no longer
/// matches.
fn actor_still_on_node(actor: &pb::Actor, node: &str) -> Result<(), Status> {
    let st = actor.status.clone().unwrap_or_default();
    let state = ActorState::from_proto(st.state);
    if !state.is_some_and(|s| s.is_transfer_state()) {
        return Err(Status::permission_denied(format!(
            "actor is {:?}, no transfer is due",
            pb::ActorState::try_from(st.state).unwrap_or(pb::ActorState::Unspecified)
        )));
    }
    if st.assigned_node != node {
        return Err(Status::permission_denied(
            "actor is no longer on the token's node",
        ));
    }
    Ok(())
}

#[tonic::async_trait]
impl CredentialExchange for ExchangeServer {
    async fn exchange_actor_jwt(
        &self,
        req: Request<ExchangeActorJwtRequest>,
    ) -> Result<Response<ExchangeActorJwtResponse>, Status> {
        self.exchange(req.get_ref()).await.map(Response::new)
    }
}

/// The exchange for a backend without per-actor credentials: S3 node
/// plugins never call it.
pub struct UnimplementedExchange;

#[tonic::async_trait]
impl CredentialExchange for UnimplementedExchange {
    async fn exchange_actor_jwt(
        &self,
        _: Request<ExchangeActorJwtRequest>,
    ) -> Result<Response<ExchangeActorJwtResponse>, Status> {
        Err(Status::unimplemented(
            "this object-store plugin has no credential exchange",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, Result};
    use crate::plugin::actorscope::tests::prefix;
    use crate::plugin::actorscope::{Access, Grant};
    use crate::plugin::credentials::Token;
    use base64::Engine;
    use hivekeep_identity::jwt::authority::{JwtAlgorithm, JwtAuthority};
    use hivekeep_identity::jwt::claims::Claims;
    use hivekeep_identity::jwt::verifier::JwksFileSource;
    use parking_lot::Mutex;
    use std::time::Duration;

    const ISSUER: &str = "https://idp.hivekeep-system.svc";
    const HIVE: &str = "team-a";
    const NAME: &str = "bot";
    const UID: &str = "9f1e0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a";

    fn tag_uri() -> String {
        "gs://snaps/root/hives/hivekeep-golden/tags/7a3c0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a".into()
    }

    /// Mints real JWTs whose public keys the verifier reads from a file.
    struct Signer {
        auth: JwtAuthority,
        pkcs8: Vec<u8>,
        now: SystemTime,
        node: Mutex<String>,
        _dir: tempfile::TempDir,
    }

    fn signer() -> (Arc<Signer>, Arc<hivekeep_identity::jwt::verifier::Verifier>) {
        let pkcs8 = hivekeep_identity::key::PrivateKey::generate(
            hivekeep_identity::key::KeyAlgorithm::EcdsaP256,
        )
        .unwrap()
        .pkcs8_der()
        .to_vec();
        let key = hivekeep_identity::key::PrivateKey::from_pkcs8_der(&pkcs8).unwrap();
        let auth = JwtAuthority::new("k1", JwtAlgorithm::Es256, key).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jwks.json");
        let jwks = hivekeep_identity::jwt::jwks::key_set_json(&[auth.verification_key()]).unwrap();
        std::fs::write(&path, jwks).unwrap();
        let v = hivekeep_identity::jwt::verifier::Verifier::new(
            ISSUER,
            vec![ACTOR_JWT_AUDIENCE.into()],
            Arc::new(JwksFileSource::new(&path)),
        )
        .with_actor_claims();
        let now = SystemTime::UNIX_EPOCH
            + Duration::from_secs(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
        (
            Arc::new(Signer {
                auth,
                pkcs8,
                now,
                node: Mutex::new("node-1".into()),
                _dir: dir,
            }),
            Arc::new(v),
        )
    }

    impl Signer {
        fn jwt(&self, kind: TokenKind, audience: &str, uid: &str) -> String {
            let node = if kind == TokenKind::WorkerForActor {
                self.node.lock().clone()
            } else {
                String::new()
            };
            self.auth
                .sign(&Claims {
                    kind,
                    issuer: ISSUER.into(),
                    subject: kind.subject(HIVE, NAME),
                    audiences: vec![audience.into()],
                    expiration: self.now + Duration::from_secs(3600),
                    not_before: self.now - Duration::from_secs(60),
                    issued_at: self.now,
                    jti: "j".into(),
                    actor: ActorClaims {
                        hive: HIVE.into(),
                        actor_name: NAME.into(),
                        actor_uid: uid.into(),
                        node,
                    },
                })
                .unwrap()
        }

        /// Signs an arbitrary claim set, for tokens the claims builder
        /// refuses to make.
        fn raw(&self, claims: serde_json::Value) -> String {
            let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
            let header = b64.encode(br#"{"alg":"ES256","typ":"JWT","kid":"k1"}"#);
            let payload = b64.encode(claims.to_string());
            let input = format!("{header}.{payload}");
            let sig = jsonwebtoken::crypto::sign(
                input.as_bytes(),
                &jsonwebtoken::EncodingKey::from_ec_der(&self.pkcs8),
                jsonwebtoken::Algorithm::ES256,
            )
            .unwrap();
            format!("{input}.{sig}")
        }

        fn raw_claims(&self) -> serde_json::Value {
            let secs = |t: SystemTime| t.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
            serde_json::json!({
                "iss": ISSUER, "sub": format!("worker-for-actor/{HIVE}/{NAME}"), "aud": ACTOR_JWT_AUDIENCE,
                "exp": secs(self.now + Duration::from_secs(3600)), "nbf": secs(self.now - Duration::from_secs(60)), "iat": secs(self.now),
                "worker-for-actor.hivekeep.io": {"hive": HIVE, "actorName": NAME, "actorUID": UID, "node": *self.node.lock()},
            })
        }
    }

    struct FakeLookup {
        actor: Option<pb::Actor>,
        err: Option<LookupError>,
    }

    #[async_trait]
    impl Lookup for FakeLookup {
        async fn actor(&self, claims: &ActorClaims) -> Result<pb::Actor, LookupError> {
            if let Some(e) = &self.err {
                return Err(e.clone());
            }
            let a = self.actor.clone().unwrap();
            if claims.actor_uid != a.metadata.as_ref().unwrap().uid {
                return Err(LookupError::Stale);
            }
            Ok(a)
        }
    }

    #[derive(Default)]
    struct FakeIssuer {
        err: Option<Error>,
        expiry: Option<SystemTime>,
        calls: Mutex<Vec<(String, Vec<Grant>, bool)>>,
    }

    #[async_trait]
    impl Issuer for FakeIssuer {
        async fn issue(&self, uid: &str, grants: &[Grant], force: bool) -> Result<Token> {
            self.calls.lock().push((uid.into(), grants.to_vec(), force));
            if let Some(e) = &self.err {
                return Err(e.clone());
            }
            Ok(Token {
                access_token: "downscoped".into(),
                expiry: self.expiry.unwrap_or(SystemTime::UNIX_EPOCH),
            })
        }
    }

    fn actor_with_tag() -> pb::Actor {
        pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: HIVE.into(),
                name: NAME.into(),
                uid: UID.into(),
                ..Default::default()
            }),
            status: Some(pb::ActorStatus {
                state: pb::ActorState::Running as i32,
                assigned_node: "node-1".into(),
                snapshots: vec![pb::Snapshot {
                    generation: 1,
                    owner: pb::SnapshotOwner::Tag as i32,
                    durable_snapshot: Some(pb::SnapshotStorage {
                        state: pb::SnapshotStorageState::Completed as i32,
                        object: Some(pb::ObjectSnapshot {
                            snapshot_uri: tag_uri(),
                        }),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn actor_local_only() -> pb::Actor {
        let mut a = actor_with_tag();
        let st = a.status.as_mut().unwrap();
        st.state = pb::ActorState::Suspended as i32;
        st.snapshots = vec![pb::Snapshot {
            generation: 1,
            owner: pb::SnapshotOwner::Actor as i32,
            locality: "node-1".into(),
            ..Default::default()
        }];
        a
    }

    fn actor_in(state: pb::ActorState, node: &str) -> pb::Actor {
        let mut a = actor_with_tag();
        let st = a.status.as_mut().unwrap();
        st.state = state as i32;
        st.assigned_node = node.into();
        a
    }

    fn server(
        s: &Signer,
        v: Arc<dyn Verifier>,
        lookup: FakeLookup,
        issuer: Arc<FakeIssuer>,
    ) -> ExchangeServer {
        let now = s.now;
        ExchangeServer::new(v, Arc::new(lookup), issuer).with_clock(Arc::new(move || now))
    }

    fn req(jwt: String, force: bool) -> ExchangeActorJwtRequest {
        ExchangeActorJwtRequest {
            actor_jwt: jwt,
            force,
        }
    }

    #[tokio::test]
    async fn node_binding() {
        let (s, v) = signer();
        use pb::ActorState::*;
        for (name, actor, ok) in [
            (
                "resuming on the token's node",
                actor_in(Resuming, "node-1"),
                true,
            ),
            (
                "running on the token's node",
                actor_in(Running, "node-1"),
                true,
            ),
            (
                "suspending on the token's node",
                actor_in(Suspending, "node-1"),
                true,
            ),
            (
                "suspended with its snapshot on the token's node",
                actor_in(Suspended, "node-1"),
                true,
            ),
            (
                "resumed on another node",
                actor_in(Running, "node-2"),
                false,
            ),
            ("detached from the node", actor_in(Suspended, ""), false),
            (
                "crashed on the token's node",
                actor_in(Crashed, "node-1"),
                false,
            ),
            (
                "deleting on the token's node",
                actor_in(Deleting, "node-1"),
                false,
            ),
            (
                "reverting on the token's node",
                actor_in(Reverting, "node-1"),
                false,
            ),
        ] {
            let issuer = Arc::new(FakeIssuer {
                expiry: Some(s.now + Duration::from_secs(3600)),
                ..Default::default()
            });
            let srv = server(
                &s,
                v.clone(),
                FakeLookup {
                    actor: Some(actor),
                    err: None,
                },
                issuer.clone(),
            );
            let r = srv
                .exchange(&req(
                    s.jwt(TokenKind::WorkerForActor, ACTOR_JWT_AUDIENCE, UID),
                    false,
                ))
                .await;
            match ok {
                true => {
                    r.unwrap_or_else(|e| panic!("{name}: {e:?}"));
                }
                false => assert_eq!(
                    r.unwrap_err().code(),
                    tonic::Code::PermissionDenied,
                    "{name}"
                ),
            }
            assert_eq!(
                issuer.calls.lock().len(),
                usize::from(ok),
                "{name}: issuer calls"
            );
        }
        // A token without a node is refused before the lookup.
        *s.node.lock() = String::new();
        let srv = server(
            &s,
            v.clone(),
            FakeLookup {
                actor: None,
                err: Some(LookupError::Status(Status::not_found("no such actor"))),
            },
            Arc::new(FakeIssuer::default()),
        );
        let e = srv
            .exchange(&req(
                s.jwt(TokenKind::WorkerForActor, ACTOR_JWT_AUDIENCE, UID),
                false,
            ))
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn exchanges_for_the_actors_grants() {
        let (s, v) = signer();
        let expiry = s.now + Duration::from_secs(50 * 60);
        let issuer = Arc::new(FakeIssuer {
            expiry: Some(expiry),
            ..Default::default()
        });
        let srv = server(
            &s,
            v,
            FakeLookup {
                actor: Some(actor_with_tag()),
                err: None,
            },
            issuer.clone(),
        );
        for force in [false, true] {
            let resp = srv
                .exchange(&req(
                    s.jwt(TokenKind::WorkerForActor, ACTOR_JWT_AUDIENCE, UID),
                    force,
                ))
                .await
                .unwrap();
            assert_eq!(resp.access_token, "downscoped");
            assert_eq!(resp.expire_time, Some(prost_types::Timestamp::from(expiry)));
            let (uid, grants, f) = issuer.calls.lock().last().cloned().unwrap();
            assert_eq!((uid.as_str(), f), (UID, force));
            assert_eq!(
                grants,
                vec![
                    Grant {
                        prefix: prefix(&format!("gs://snaps/root/hives/{HIVE}/actors/{UID}")),
                        access: Access::ReadWrite
                    },
                    Grant {
                        prefix: prefix(&tag_uri()),
                        access: Access::ReadOnly
                    },
                ]
            );
        }
        assert_eq!(issuer.calls.lock().len(), 2);
    }

    #[tokio::test]
    async fn exchange_errors() {
        let (s, v) = signer();
        let (stranger, _) = signer();
        let good = s.jwt(TokenKind::WorkerForActor, ACTOR_JWT_AUDIENCE, UID);
        let mut no_claim = s.raw_claims();
        no_claim
            .as_object_mut()
            .unwrap()
            .remove("worker-for-actor.hivekeep.io");
        let mut both = s.raw_claims();
        both["actor.hivekeep.io"] = both["worker-for-actor.hivekeep.io"].clone();
        let mut no_uid = s.raw_claims();
        no_uid["worker-for-actor.hivekeep.io"] =
            serde_json::json!({"hive": HIVE, "actorName": NAME});
        let ok_lookup = || FakeLookup {
            actor: Some(actor_with_tag()),
            err: None,
        };
        let failing = |st: Status| FakeLookup {
            actor: None,
            err: Some(LookupError::Status(st)),
        };
        let issuer_err = |e: Error| FakeIssuer {
            err: Some(e),
            ..Default::default()
        };
        use tonic::Code::*;
        let cases: Vec<(&str, String, FakeLookup, FakeIssuer, tonic::Code)> = vec![
            (
                "empty jwt",
                String::new(),
                ok_lookup(),
                FakeIssuer::default(),
                InvalidArgument,
            ),
            (
                "bad signature",
                stranger.jwt(TokenKind::WorkerForActor, ACTOR_JWT_AUDIENCE, UID),
                ok_lookup(),
                FakeIssuer::default(),
                Unauthenticated,
            ),
            (
                "wrong audience",
                s.jwt(TokenKind::WorkerForActor, "other", UID),
                ok_lookup(),
                FakeIssuer::default(),
                Unauthenticated,
            ),
            (
                "actor's own token",
                s.jwt(TokenKind::Actor, ACTOR_JWT_AUDIENCE, UID),
                ok_lookup(),
                FakeIssuer::default(),
                PermissionDenied,
            ),
            (
                "no hivekeep claim",
                s.raw(no_claim),
                ok_lookup(),
                FakeIssuer::default(),
                PermissionDenied,
            ),
            (
                "both hivekeep claims",
                s.raw(both),
                ok_lookup(),
                FakeIssuer::default(),
                Unauthenticated,
            ),
            (
                "claim without the actor uid",
                s.raw(no_uid),
                ok_lookup(),
                FakeIssuer::default(),
                Unauthenticated,
            ),
            (
                "hivekeep-api rejects the actor ref",
                good.clone(),
                failing(Status::invalid_argument("bad name")),
                FakeIssuer::default(),
                Unauthenticated,
            ),
            (
                "caller gave up during the lookup",
                good.clone(),
                failing(Status::cancelled("canceled")),
                FakeIssuer::default(),
                Cancelled,
            ),
            (
                "caller's deadline passed during the lookup",
                good.clone(),
                failing(Status::deadline_exceeded("deadline")),
                FakeIssuer::default(),
                DeadlineExceeded,
            ),
            (
                "recreated actor",
                s.jwt(TokenKind::WorkerForActor, ACTOR_JWT_AUDIENCE, "old-uid"),
                ok_lookup(),
                FakeIssuer::default(),
                NotFound,
            ),
            (
                "actor gone",
                good.clone(),
                failing(Status::not_found("no such actor")),
                FakeIssuer::default(),
                NotFound,
            ),
            (
                "hivekeep-api down",
                good.clone(),
                failing(Status::unavailable("connection refused")),
                FakeIssuer::default(),
                Unavailable,
            ),
            (
                "exchange lacks the actor read role",
                good.clone(),
                failing(Status::permission_denied("not allowed")),
                FakeIssuer::default(),
                FailedPrecondition,
            ),
            (
                "no snapshot location",
                good.clone(),
                FakeLookup {
                    actor: Some(actor_local_only()),
                    err: None,
                },
                FakeIssuer::default(),
                PermissionDenied,
            ),
            (
                "issuer failing",
                good.clone(),
                ok_lookup(),
                issuer_err(Error::other("STS 503")),
                Unavailable,
            ),
            (
                "issuer cannot express the grants",
                good.clone(),
                ok_lookup(),
                issuer_err(Status::internal("bad boundary").into()),
                Internal,
            ),
            (
                "caller gave up at STS",
                good.clone(),
                ok_lookup(),
                issuer_err(Error::canceled().context("Post sts")),
                Cancelled,
            ),
            (
                "caller's deadline passed at STS",
                good.clone(),
                ok_lookup(),
                issuer_err(Error::deadline_exceeded().context("Post sts")),
                DeadlineExceeded,
            ),
        ];
        for (name, jwt, lookup, issuer, want) in cases {
            let want_calls = usize::from(issuer.err.is_some());
            let issuer = Arc::new(issuer);
            let srv = server(&s, v.clone(), lookup, issuer.clone());
            let e = srv.exchange(&req(jwt, false)).await.unwrap_err();
            assert_eq!(e.code(), want, "{name}: {e:?}");
            assert_eq!(
                issuer.calls.lock().len(),
                want_calls,
                "{name}: issuer calls"
            );
        }
    }
}
